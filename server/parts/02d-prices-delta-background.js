async function sendPriceMasterDeltaWarehousePrices(priceMaster = {}, warehouse = {}, options = {}) {
  const usdRate = options.usdRate;
  if (!priceMasterDeltaPricePushEnabled) {
    return {
      ok: true,
      sent: 0,
      failed: 0,
      skipped: [],
      delta: { productIds: [], skipped: true, reason: "disabled" },
    };
  }
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  invalidateWarehouseViewCache();
  const delta = priceMasterChangeImpactProductIds(warehouse, priceMaster.changedRows || [], {
    maxChanges: options.maxChanges || priceMasterDeltaMaxChanges,
    maxProducts: options.maxProducts || priceMasterDeltaMaxProducts,
    fullReconcileOnTooMany: options.fullReconcileOnTooMany !== false,
    fullReconcileMaxProducts: options.fullReconcileMaxProducts || autoPriceReconcileMaxProducts,
  });
  if (delta.skipped) {
    logger.warn("PriceMaster delta price push limited", {
      reason: delta.reason,
      scannedChanges: delta.scannedChanges,
      impactedProducts: delta.productIds.length,
    });
  }
  if (!delta.productIds.length) {
    return {
      ok: true,
      sent: 0,
      failed: 0,
      skipped: [],
      delta,
    };
  }
  if (delta.fallbackFullReconcile) {
    const queued = await queueAuthoritativePriceReprice({
      productIds: delta.productIds,
      marketplace: "all",
      reason: delta.reason,
      sourceEvent: "pricemaster_delta_full_reconcile",
      force: false,
      onlyChanged: true,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
    });
    logger.info("PriceMaster delta price push queued full reconcile", {
      reason: delta.reason,
      scannedChanges: delta.scannedChanges,
      products: queued.queued,
      batches: queued.queuedBatches,
      batchSize: authoritativeRepriceBatchSize,
    });
    return {
      ok: true,
      sent: 0,
      failed: 0,
      queued: queued.queued,
      queuedBatches: queued.queuedBatches,
      priceIntentId: queued.priceIntentId,
      skipped: [],
      delta,
    };
  }
  const result = await queueAuthoritativePriceReprice({
    productIds: delta.productIds,
    marketplace: "all",
    reason: delta.reason,
    sourceEvent: "pricemaster_delta",
    force: false,
    onlyChanged: options.onlyChanged !== false,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
    verify: true,
    priority: QUEUE_PRIORITY.PRICE_IMMEDIATE,
  });
  return {
    sent: 0,
    failed: 0,
    skipped: [],
    ...result,
    delta,
  };
}

function emptyNoSupplierAutomationResult(reason = "no_changed_products") {
  return {
    zeroStockSent: 0,
    archived: 0,
    errors: [],
    productStatuses: [],
    skipped: true,
    reason,
    source: "targeted",
  };
}

function emptySupplierRecoveryResult(reason = "no_changed_products") {
  return {
    recovered: 0,
    restoredStocks: 0,
    unarchived: 0,
    errors: [],
    productStatuses: [],
    skipped: true,
    reason,
    source: "targeted",
  };
}

async function runTargetedBackgroundSupplierAutomations(priceMaster = {}, warehouse = {}, options = {}) {
  const scope = backgroundAutomationProductIds(priceMaster, warehouse, options);
  if (!scope.productIds.length) {
    return {
      automation: emptyNoSupplierAutomationResult("no_changed_products"),
      recovery: emptySupplierRecoveryResult("no_changed_products"),
      scope,
    };
  }

  const ids = new Set(scope.productIds.map(String));
  const products = (Array.isArray(warehouse.products) ? warehouse.products : [])
    .filter((product) => ids.has(String(product.id)));
  if (!products.length) {
    return {
      automation: emptyNoSupplierAutomationResult("changed_products_not_in_view"),
      recovery: emptySupplierRecoveryResult("changed_products_not_in_view"),
      scope,
    };
  }

  const automation = await runNoSupplierMarketplaceAutomation(
    { products },
    { productIds: scope.productIds, includeNoLinks: true, source: "targeted" },
  );
  const recovery = await runSupplierRecoveryAutomation(
    { products },
    { productIds: scope.productIds, source: "targeted" },
  );
  return { automation, recovery, scope };
}
