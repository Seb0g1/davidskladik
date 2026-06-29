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

// Deferred (background) price verification: the send pipeline no longer blocks 10-30s per
// batch waiting for Ozon to apply prices. Runs after a delay, updates sku states, and
// force-requeues prices that did not apply so they are pushed again automatically.
let deferredOzonVerifyInFlight = 0;
function scheduleDeferredOzonPriceVerification(account, entries = [], { priceIntentId = "", sentAt = new Date().toISOString() } = {}) {
  if (!entries.length) return;
  const maxInFlight = Math.max(1, Number(process.env.OZON_PRICE_VERIFY_MAX_INFLIGHT || 15) || 15);
  if (deferredOzonVerifyInFlight >= maxInFlight) {
    logger.warn("deferred ozon price verification skipped: too many in flight", {
      inFlight: deferredOzonVerifyInFlight,
      items: entries.length,
    });
    return;
  }
  deferredOzonVerifyInFlight += 1;
  const timer = setTimeout(async () => {
    try {
      const { verified, failed } = await verifyOzonPriceApplied(account, entries, { priceIntentId, sentAt });
      if (verified.length) {
        await upsertSalesAutomationSkuStates(verified.map((entry) => ({
          ...entry.item,
          productId: entry.item.productId || entry.item.id,
          priceStatus: "success",
          priceApplyStatus: "verified",
          lastVerifiedPrice: entry.verifiedPrice,
          lastPriceVerifiedAt: entry.verifiedAt,
          priceIntentId,
        }))).catch((error) => logger.warn("deferred verify state update failed", { detail: error?.message || String(error) }));
      }
      if (failed.length) {
        const failedIds = failed.map((entry) => entry.item?.id).filter(Boolean);
        logger.warn("deferred ozon price verification: prices not applied, requeueing", {
          account: account?.id || "ozon",
          failed: failed.length,
          sample: failedIds.slice(0, 10),
        });
        await queueAuthoritativePriceReprice({
          productIds: failedIds,
          marketplace: "ozon",
          reason: "ozon_price_verify_retry",
          sourceEvent: "ozon_price_verify_retry",
          force: true,
          onlyChanged: false,
          refreshMarketplacePrices: true,
          livePriceMaster: true,
          verify: true,
          priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
        }).catch((error) => logger.warn("deferred verify requeue failed", { detail: error?.message || String(error) }));
      }
    } catch (error) {
      logger.warn("deferred ozon price verification failed", { detail: error?.message || String(error) });
    } finally {
      deferredOzonVerifyInFlight -= 1;
    }
  }, Math.max(1000, ozonPriceVerifyDelayMs));
  timer.unref?.();
}
