function ozonPriceResultErrorMessage(result = {}) {
  const errors = Array.isArray(result.errors) ? result.errors : [];
  if (!errors.length) return "";
  return errors
    .map((error) => cleanText(error.message || error.error || error.code || JSON.stringify(error)))
    .filter(Boolean)
    .join("; ");
}

function extractOzonPriceResponseFailures(response = {}, payloads = []) {
  const results = Array.isArray(response?.result) ? response.result : [];
  if (!results.length) return [];
  const payloadByOffer = new Map(payloads.map((payload) => [String(payload.offer_id || ""), payload]));
  const payloadByProduct = new Map(payloads.map((payload) => [String(payload.product_id || ""), payload]));
  const failed = [];
  for (const result of results) {
    const offerId = String(result.offer_id || "");
    const productId = String(result.product_id || "");
    const payload = payloadByOffer.get(offerId) || payloadByProduct.get(productId);
    if (!payload) continue;
    const detail = ozonPriceResultErrorMessage(result);
    if (result.updated === false || detail) {
      const error = new Error(detail || "Ozon price update was not applied");
      error.ozon = result;
      failed.push({ payload, error });
    }
  }
  return failed;
}

async function sendOzonPricePayloadChunks(account, prices) {
  const results = [];
  const failed = [];
  for (const chunk of chunkArray(prices, getOzonPriceBatchSize())) {
    try {
      const response = await sendOzonPriceBatch(account, chunk);
      const responseFailures = extractOzonPriceResponseFailures(response, chunk);
      if (responseFailures.length) failed.push(...responseFailures);
      results.push({ response, count: chunk.length - responseFailures.length });
    } catch (error) {
      if (!isOzonResourceExhaustedError(error) || chunk.length <= 1) {
        failed.push(...chunk.map((payload) => ({ payload, error })));
        continue;
      }
      logger.warn("ozon price batch limit exceeded, falling back to single-item sends", {
        account: account?.id || account?.name || "ozon",
        items: chunk.length,
        detail: error?.message || String(error),
      });
      for (const payload of chunk) {
        try {
          results.push({ response: await sendOzonPriceBatch(account, [payload]), count: 1 });
        } catch (singleError) {
          failed.push({ payload, error: singleError });
        }
      }
    }
  }
  return { results, failed };
}

async function verifyOzonPriceApplied(account, entries = [], { priceIntentId = "", sentAt = new Date().toISOString(), delayMs = ozonPriceVerifyDelayMs, attempts = ozonPriceVerifyAttempts } = {}) {
  const pending = new Map((Array.isArray(entries) ? entries : [])
    .filter((entry) => entry?.payload?.offer_id && Number(entry.payload.price) > 0)
    .map((entry) => [String(entry.payload.offer_id), entry]));
  const verified = [];
  const failed = [];
  if (!pending.size) return { verified, failed };
  const maxAttempts = Math.max(1, Number(attempts || 1) || 1);
  const waitMs = Math.max(0, Number(delayMs || 0) || 0);
  for (let attempt = 1; attempt <= maxAttempts && pending.size; attempt += 1) {
    if (waitMs > 0) await sleep(waitMs);
    const offerIds = Array.from(pending.keys());
    let priceMap = new Map();
    try {
      priceMap = await getOzonPriceMap(offerIds, account);
    } catch (error) {
      if (attempt >= maxAttempts) {
        for (const entry of pending.values()) {
          const verifyError = new Error(error?.message || "ozon_price_verify_failed");
          verifyError.code = "ozon_price_verify_failed";
          failed.push({ ...entry, error: verifyError, verificationAttempts: attempt });
        }
        pending.clear();
      }
      continue;
    }
    for (const [offerId, entry] of Array.from(pending.entries())) {
      const details = normalizeOzonPriceDetails(getOzonOfferMapValue(priceMap, offerId) || priceMap.get(offerId) || {});
      const cabinetPrice = roundPrice(pickOzonCabinetListedPrice(details) || details.currentPrice || details.marketingSellerPrice || 0);
      const requestedPrice = roundPrice(entry.payload.price);
      const applied = requestedPrice > 0 && cabinetPrice > 0 && Math.abs(cabinetPrice - requestedPrice) <= ozonPriceVerifyToleranceRub;
      if (!applied) continue;
      verified.push({
        ...entry,
        verifiedPrice: cabinetPrice,
        verifiedAt: new Date().toISOString(),
        verificationAttempts: attempt,
        priceIntentId,
        sentAt,
      });
      pending.delete(offerId);
    }
  }
  for (const entry of pending.values()) {
    const error = new Error("ozon_price_not_applied");
    error.code = "ozon_price_not_applied";
    failed.push({ ...entry, error, verificationAttempts: maxAttempts });
  }
  return { verified, failed };
}
