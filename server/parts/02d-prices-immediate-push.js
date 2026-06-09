function hasPendingImmediateAutoPricePush() {
  return immediateAutoPushAll || immediateAutoPushIds.size > 0;
}

function mergeImmediateAutoPricePushOptions(reason = "price_change_detected", options = {}) {
  immediateAutoPushReasons.add(cleanText(reason) || "price_change_detected");
  if (options.force === true) {
    immediateAutoPushForce = true;
    immediateAutoPushOnlyChanged = false;
  } else if (options.onlyChanged !== undefined && options.onlyChanged === false) {
    immediateAutoPushOnlyChanged = false;
  }
  const marketplace = cleanText(options.marketplace || "all").toLowerCase();
  if (marketplace === "ozon" || marketplace === "yandex") {
    if (!immediateAutoPushMarketplaceLocked) {
      immediateAutoPushMarketplace = marketplace;
      immediateAutoPushMarketplaceLocked = true;
    } else if (immediateAutoPushMarketplace !== marketplace) {
      immediateAutoPushMarketplace = "all";
    }
  } else {
    immediateAutoPushMarketplace = "all";
    immediateAutoPushMarketplaceLocked = true;
  }
  if (options.refreshMarketplacePrices === false) {
    immediateAutoPushRefreshMarketplacePrices = false;
  }
  if (options.livePriceMaster === false) {
    immediateAutoPushLivePriceMaster = false;
  }
  if (options.limit !== undefined) {
    const limit = Number(options.limit);
    if (Number.isFinite(limit) && limit > 0) {
      immediateAutoPushLimit = immediateAutoPushLimit === undefined ? limit : Math.max(immediateAutoPushLimit, limit);
    }
  }
}

function takeImmediateAutoPricePushBatch() {
  const ids = immediateAutoPushAll ? undefined : Array.from(immediateAutoPushIds);
  const reasons = Array.from(immediateAutoPushReasons).filter(Boolean);
  const batch = {
    ids,
    force: immediateAutoPushForce,
    reason: reasons.length > 1 ? reasons.slice(0, 5).join(",") : (reasons[0] || "price_change_detected"),
    marketplace: immediateAutoPushMarketplace || "all",
    onlyChanged: immediateAutoPushForce ? false : immediateAutoPushOnlyChanged,
    refreshMarketplacePrices: immediateAutoPushRefreshMarketplacePrices,
    livePriceMaster: immediateAutoPushLivePriceMaster,
    limit: immediateAutoPushLimit,
  };
  immediateAutoPushAll = false;
  immediateAutoPushForce = false;
  immediateAutoPushIds.clear();
  immediateAutoPushReasons.clear();
  immediateAutoPushMarketplace = "all";
  immediateAutoPushMarketplaceLocked = false;
  immediateAutoPushOnlyChanged = true;
  immediateAutoPushRefreshMarketplacePrices = true;
  immediateAutoPushLivePriceMaster = true;
  immediateAutoPushLimit = undefined;
  return batch;
}

function scheduleImmediateAutoPricePushFlush(delayMs = immediateAutoPushDelayMs) {
  if (immediateAutoPushTimer || immediateAutoPushRunning || !hasPendingImmediateAutoPricePush()) return;
  immediateAutoPushTimer = setTimeout(() => {
    immediateAutoPushTimer = null;
    flushImmediateAutoPricePush().catch((error) => {
      logger.warn("immediate auto price push failed", { detail: error?.message || String(error) });
    });
  }, Math.max(0, Number(delayMs) || 0));
  immediateAutoPushTimer.unref?.();
}

async function flushImmediateAutoPricePush() {
  if (immediateAutoPushRunning || !hasPendingImmediateAutoPricePush()) return;
  const batch = takeImmediateAutoPricePushBatch();
  immediateAutoPushRunning = true;
  const queuedMode = Boolean(marketplaceQueue && marketplaceQueueAutoPricePushEnabled);
  let handedToQueue = false;
  try {
    logger.info("immediate auto price push queued", {
      reason: batch.reason,
      scope: batch.ids ? batch.ids.length : "all",
      mode: queuedMode ? "bullmq" : "inline",
    });
    const result = batch.livePriceMaster
      ? await queueAuthoritativePriceReprice({
        productIds: batch.ids,
        marketplace: batch.marketplace,
        reason: batch.reason,
        sourceEvent: batch.reason,
        force: true,
        onlyChanged: batch.onlyChanged,
        refreshMarketplacePrices: batch.refreshMarketplacePrices,
        livePriceMaster: true,
        verify: true,
        limit: batch.limit,
        priority: 1,
      })
      : await enqueueMarketplaceJobAccepted(
        "auto-price-push",
        {
          productIds: batch.ids,
          priceIntentId: crypto.randomUUID(),
          usdRate: undefined,
          minDiffRub: 0,
          minDiffPct: 0,
          force: batch.force,
          reason: batch.reason,
          sourceEvent: batch.reason,
          marketplace: batch.marketplace,
          onlyChanged: batch.onlyChanged,
          refreshMarketplacePrices: batch.refreshMarketplacePrices,
          livePriceMaster: batch.livePriceMaster,
          verify: true,
          limit: batch.limit,
        },
        { priority: 1 },
      );
    handedToQueue = queuedMode && result && typeof result === "object" && !("sent" in result);
    if (result && typeof result === "object" && "sent" in result) {
      const skippedReasons = Array.isArray(result.skipped)
        ? result.skipped.reduce((acc, item) => {
          const reason = item.reason || "unknown";
          acc[reason] = (acc[reason] || 0) + 1;
          return acc;
        }, {})
        : {};
      logger.info("immediate auto price push complete", {
        reason: batch.reason,
        scope: batch.ids ? batch.ids.length : "all",
        force: batch.force,
        sent: result.sent,
        failed: result.failed,
        stockSent: result.stockSent,
        stockFailed: result.stockFailed,
        ozonSent: result.ozonSent || 0,
        ozonFailed: result.ozonFailed || 0,
        ozonSkipped: result.ozonSkipped || 0,
        yandexSent: result.yandexSent || 0,
        yandexFailed: result.yandexFailed || 0,
        yandexSkipped: result.yandexSkipped || 0,
        skipped: Array.isArray(result.skipped) ? result.skipped.length : 0,
        skippedReasons,
      });
    }
  } catch (error) {
    logger.warn("immediate auto price push failed", { reason: batch.reason, detail: error?.message || String(error) });
  } finally {
    if (!handedToQueue) {
      immediateAutoPushRunning = false;
      if (hasPendingImmediateAutoPricePush()) scheduleImmediateAutoPricePushFlush(immediateAutoPushFollowupDelayMs);
    }
  }
}

function queueImmediateAutoPricePush(productIds = [], reason = "price_change_detected", options = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return;
  mergeImmediateAutoPricePushOptions(reason, options);
  if (Array.isArray(productIds) && productIds.length) {
    productIds.forEach((id) => immediateAutoPushIds.add(String(id)));
  } else {
    immediateAutoPushAll = true;
  }
  scheduleImmediateAutoPricePushFlush();
}

function queueChangedWarehousePrices(products = [], reason = "warehouse_changed_prices_detected") {
  const isPassiveWarehouseView = String(reason || "").startsWith("warehouse_page_")
    || String(reason || "").startsWith("warehouse_view_");
  if (isPassiveWarehouseView && process.env.AUTO_PRICE_FROM_WAREHOUSE_PAGE_ENABLED !== "true") return 0;
  const now = Date.now();
  const cooldownMs = Math.max(
    10_000,
    Number(process.env.AUTO_PRICE_CHANGED_COOLDOWN_MS || detectedPriceAutoPushDefaultCooldownMs)
      || detectedPriceAutoPushDefaultCooldownMs,
  );
  const batchCooldownMs = Math.max(
    1_000,
    Number(process.env.AUTO_PRICE_CHANGED_BATCH_COOLDOWN_MS || detectedPriceAutoPushDefaultBatchCooldownMs)
      || detectedPriceAutoPushDefaultBatchCooldownMs,
  );
  if (changedPriceAutoPushLastBatchAt && now - changedPriceAutoPushLastBatchAt < batchCooldownMs) return 0;
  if (changedPriceAutoPushAt.size > 20_000) {
    for (const [id, last] of changedPriceAutoPushAt.entries()) {
      if (!last || now - Number(last) > cooldownMs * 2) changedPriceAutoPushAt.delete(id);
    }
  }
  const ids = (Array.isArray(products) ? products : [])
    .filter((product) => {
      if (!product?.hasLinks) return false;
      if (product.changed && Number(product.nextPrice || 0) > 0) return true;
      return shouldSendTargetStockForProduct(product);
    })
    .map((product) => product.id)
    .filter(Boolean)
    .filter((id) => {
      const last = Number(changedPriceAutoPushAt.get(String(id)) || 0);
      if (last && now - last < cooldownMs) return false;
      changedPriceAutoPushAt.set(String(id), now);
      return true;
    });
  if (!ids.length) return 0;
  changedPriceAutoPushLastBatchAt = now;
  queueImmediateAutoPricePush(ids, reason);
  return ids.length;
}
