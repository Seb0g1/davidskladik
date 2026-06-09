async function processPriceRetryQueue({ queueKeys = [], limit = 1000, respectNextRetryAt = false, trigger = "manual" } = {}) {
  if (priceRetryRunning) return { ok: true, skipped: true, reason: "already_running", processed: 0, retried: 0, failed: 0, remaining: 0 };
  priceRetryRunning = true;
  try {
    const queue = await readPriceRetryQueue();
    if (!queue.items.length) return { ok: true, processed: 0, retried: 0, failed: 0, remaining: 0, results: [] };
    const requestedKeys = new Set((Array.isArray(queueKeys) ? queueKeys : []).map(String));
    const now = new Date();
    const selected = (requestedKeys.size
      ? queue.items.filter((item) => requestedKeys.has(String(priceRetryQueueKey(item))))
      : queue.items.filter((item) => !respectNextRetryAt || !item.nextRetryAt || new Date(item.nextRetryAt).getTime() <= now.getTime()))
      .slice(0, Math.max(1, Number(limit || 1000) || 1000));
    if (!selected.length) return { ok: true, processed: 0, retried: 0, failed: 0, remaining: queue.items.length, results: [] };

    const results = [];
  const failed = [];
  const historyRows = [];

  for (const account of getOzonAccounts()) {
      const targetItems = selected.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
      const ozonItems = targetItems.map((item) => ({ item, payload: buildOzonPricePayload(item) }))
        .filter((entry) => entry.payload.offer_id && Number(entry.payload.price) > 0);
      if (!ozonItems.length) continue;
      const sent = await sendOzonPricePayloadChunks(account, ozonItems.map((entry) => entry.payload));
      results.push(...sent.results.map((entry) => ({ target: account.id, response: entry.response, count: entry.count })));
      const failedOfferIds = new Map(sent.failed.map((entry) => [String(entry.payload.offer_id), entry.error]));
      const acceptedOzonItems = ozonItems.filter((entry) => !failedOfferIds.has(String(entry.payload.offer_id)));
      const verification = acceptedOzonItems.length
        ? await verifyOzonPriceApplied(account, acceptedOzonItems, { priceIntentId: `retry-${now.getTime()}`, sentAt: now.toISOString() })
        : { verified: [], failed: [] };
      const verificationFailedOfferIds = new Map(verification.failed.map((entry) => [String(entry.payload.offer_id), entry.error]));
      const failedOfferIdSet = new Set(failedOfferIds.keys());
      const verificationFailedOfferIdSet = new Set(verificationFailedOfferIds.keys());
      for (const entry of ozonItems) {
        const error = failedOfferIds.get(String(entry.payload.offer_id)) || verificationFailedOfferIds.get(String(entry.payload.offer_id));
        const delayed = error ? isOzonPerItemPriceLimitError(error) : false;
        const oldPriceAdjusted = error ? needsOzonOldPriceEscalation(error) : false;
        historyRows.push({
          productId: entry.item.productId || entry.item.id,
          marketplace: "ozon",
          target: entry.item.target,
          offerId: entry.item.offerId,
          oldPrice: entry.item.oldPrice,
          newPrice: entry.item.price,
          status: error ? (delayed ? "delayed" : (oldPriceAdjusted ? "pending" : "failed")) : "success",
          error: error?.message || "",
          at: now.toISOString(),
        });
      }
      failed.push(...ozonItems
        .filter((entry) => failedOfferIdSet.has(String(entry.payload.offer_id)))
        .map((entry) => buildPriceRetryItem(entry.item, failedOfferIds.get(String(entry.payload.offer_id)), now)));
      failed.push(...ozonItems
        .filter((entry) => verificationFailedOfferIdSet.has(String(entry.payload.offer_id)))
        .map((entry) => buildPriceRetryItem(entry.item, verificationFailedOfferIds.get(String(entry.payload.offer_id)), now)));
      const stagedFollowUps = [];
      for (const entry of ozonItems) {
        const offerId = String(entry.payload.offer_id || "");
        if (failedOfferIdSet.has(offerId) || verificationFailedOfferIdSet.has(offerId)) continue;
        const finalTarget = roundPrice(entry.item.finalTargetPrice ?? entry.item.targetPrice ?? 0);
        const sentPrice = roundPrice(entry.item.price);
        if (!finalTarget || sentPrice <= finalTarget) continue;
        const nextStep = computeOzonQuarantineNextPrice(sentPrice, finalTarget);
        stagedFollowUps.push({
          ...entry.item,
          price: nextStep,
          finalTargetPrice: finalTarget,
          cabinetPrice: sentPrice,
          oldPrice: resolveOzonOldPrice(nextStep, {}),
          forceOldPrice: true,
          status: "pending",
          attempts: 0,
          queuedAt: entry.item.queuedAt || now.toISOString(),
          nextRetryAt: new Date(now.getTime() + priceRetryDelayMs(1, { message: "скидк" })).toISOString(),
          retryReason: "ozon_quarantine_step",
          error: "",
        });
      }
      if (stagedFollowUps.length) failed.push(...stagedFollowUps);
    }

    for (const shop of getYandexShops()) {
      const targetItems = selected.filter((item) => item.marketplace === "yandex" && matchesYandexTarget(item.target, shop.id));
      const yandexItems = targetItems.map((item) => ({ offerId: String(item.offerId || "").trim(), price: { value: roundPrice(item.price), currencyId: "RUR" } }))
        .filter((item) => item.offerId && item.price.value > 0);
      if (!yandexItems.length) continue;
      try {
        for (const chunk of chunkArray(yandexItems, 500)) {
          results.push({ target: shop.id, response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }) });
        }
        historyRows.push(...targetItems.map((item) => ({
          productId: item.productId || item.id,
          marketplace: "yandex",
          target: item.target,
          offerId: item.offerId,
          oldPrice: item.oldPrice,
          newPrice: item.price,
          status: "success",
          error: "",
          at: now.toISOString(),
        })));
      } catch (error) {
        historyRows.push(...targetItems.map((item) => ({
          productId: item.productId || item.id,
          marketplace: "yandex",
          target: item.target,
          offerId: item.offerId,
          oldPrice: item.oldPrice,
          newPrice: item.price,
          status: "failed",
          error: error?.message || "send_failed",
          at: now.toISOString(),
        })));
        failed.push(...targetItems.map((item) => buildPriceRetryItem(item, error, now)));
      }
    }

    const processedKeys = new Set(selected.map((item) => String(priceRetryQueueKey(item))));
    const untouched = queue.items.filter((item) => !processedKeys.has(String(priceRetryQueueKey(item))));
    const remaining = [...failed, ...untouched];
    await writePriceRetryQueue({ items: remaining.slice(0, 5000) });
    appendPriceHistoryRows(historyRows).catch((error) => logger.warn("retry price history append failed", { detail: error?.message || String(error) }));
    schedulePriceRetryItems(remaining);
    return {
      ok: true,
      trigger,
      processed: selected.length,
      retried: selected.length - failed.length,
      failed: failed.length,
      remaining: remaining.length,
      results,
    };
  } finally {
    priceRetryRunning = false;
  }
}


