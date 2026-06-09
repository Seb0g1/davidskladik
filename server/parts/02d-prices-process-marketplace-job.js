async function processMarketplaceJob(name, data = {}) {
  if (name === "auto-price-push") {
    return sendWarehousePrices({
      productIds: Array.isArray(data.productIds) ? data.productIds : undefined,
      usdRate: data.usdRate,
      minDiffRub: 0,
      minDiffPct: 0,
      force: data.force === true,
      dryRun: false,
      marketplace: data.marketplace || "all",
      onlyChanged: data.onlyChanged === true,
      refreshMarketplacePrices: data.refreshMarketplacePrices === true,
      livePriceMaster: data.livePriceMaster === true,
      reason: data.reason || "auto-price-push",
      limit: data.limit,
      verify: data.verify !== false,
      priceIntentId: cleanText(data.priceIntentId || ""),
      sourceEvent: data.sourceEvent || data.reason || "auto-price-push",
      repriceAttempt: Number(data.repriceAttempt || 0) || 0,
      priceMasterTimeoutMs: Number(data.priceMasterTimeoutMs || 0) || undefined,
    });
  }
  if (name === "no-supplier-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds);
      return runNoSupplierMarketplaceAutomation({ products }, {
        productIds,
        includeNoLinks: true,
        source: "targeted",
      });
    }
    const preview = await buildWarehouseView({ sync: true });
    return runNoSupplierMarketplaceAutomation(preview, { source: "full_sync" });
  }
  if (name === "supplier-recovery-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    const source = cleanText(data.source || data.sourceEvent) || (productIds.length ? "targeted" : "full");
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds, {
        refreshPrices: data.refreshPrices !== false,
        livePriceMaster: data.livePriceMaster !== false,
        batchPriceMaster: data.livePriceMaster !== false,
        priceMasterTimeoutMs: Number(data.priceMasterTimeoutMs || 0) || autoPricePmTimeoutMs,
      });
      return runSupplierRecoveryAutomation({ products }, {
        productIds,
        source,
        sourceEvent: data.sourceEvent || source,
        force: data.force === true,
        forceOzonDailyLimit: data.forceOzonDailyLimit === true,
      });
    }
    const preview = await buildWarehouseView({ sync: false });
    return runSupplierRecoveryAutomation(preview, { source });
  }
  if (name === "ozon-unarchive-queue-process") {
    return processOzonUnarchiveQueue({
      source: data.source || "ozon_unarchive_queue_auto",
      limit: data.limit || ozonUnarchiveQueueBatchLimit,
      force: data.force === true,
      queueRunId: cleanText(data.queueRunId || data.runId),
    });
  }
  if (name === "yandex-unarchive-queue-process") {
    return processYandexUnarchiveQueue({
      source: data.source || "yandex_unarchive_queue_auto",
      limit: data.limit || yandexUnarchiveQueueBatchLimit,
      queueRunId: cleanText(data.queueRunId || data.runId),
    });
  }
  return null;
}

