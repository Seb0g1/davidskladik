async function runSupplierRecoveryAutomation(preview, options = {}) {
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const source = options.source || (Array.isArray(options.productIds) && options.productIds.length ? "targeted" : "full");
  logger.info("supplier_recovery_run", {
    source,
    enabled: autoRestoreOnSupplierReturn,
    inputProducts: products.length,
    targetedIds: Array.isArray(options.productIds) ? options.productIds.length : 0,
  });
  if (!autoRestoreOnSupplierReturn) {
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [] };
  }
  let recovered = pickSupplierRecoveryCandidates(products, options);
  const targetedIdSet = Array.isArray(options.productIds) && options.productIds.length
    ? new Set(options.productIds.map((id) => String(id || "").trim()).filter(Boolean))
    : null;
  let queuedOzonProducts = [];
  try {
    const queueState = await readOzonUnarchiveQueue();
    const nowMs = Date.now();
    const perTargetTaken = new Map();
    const queuedIds = [];
    for (const item of queueState.items || []) {
      if (item.status === "done") continue;
      const id = cleanText(item.id);
      if (!id) continue;
      if (targetedIdSet && !targetedIdSet.has(id)) continue;
      const retryAtMs = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
      if (retryAtMs && Number.isFinite(retryAtMs) && retryAtMs > nowMs) continue;
      const target = cleanText(item.target) || "default";
      const taken = perTargetTaken.get(target) || 0;
      if (taken >= ozonUnarchiveQueueBatchLimit) continue;
      queuedIds.push(id);
      perTargetTaken.set(target, taken + 1);
      if (queuedIds.length >= ozonUnarchiveQueueBatchLimit) break;
    }
    const missingQueuedIds = queuedIds.filter((id) => !recovered.some((product) => String(product.id) === id));
    if (missingQueuedIds.length) {
      queuedOzonProducts = await buildFreshWarehouseProducts(missingQueuedIds);
      const existingIds = new Set(recovered.map((product) => String(product.id)));
      recovered = [
        ...recovered,
        ...queuedOzonProducts.filter((product) => product?.marketplace === "ozon" && !existingIds.has(String(product.id))),
      ];
    }
  } catch (error) {
    logger.warn("ozon unarchive queue load failed during supplier recovery", { detail: error?.message || String(error) });
  }
  if (!recovered.length) {
    logger.info("supplier recovery automation complete", {
      source,
      products: products.length,
      recovered: 0,
      restoredStocks: 0,
      unarchived: 0,
      errors: 0,
    });
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [], source };
  }
  const yandexRecovered = recovered.filter((product) => product.marketplace === "yandex");
  let ozonRecovered = recovered.filter((product) => product.marketplace !== "yandex");
  let ozonQueuedUnarchiveActions = [];
  if (options.deferOzonUnarchive === true && ozonRecovered.length) {
    const archivedOzon = ozonRecovered.filter((product) => productLooksArchived(product));
    const activeOzon = ozonRecovered.filter((product) => !productLooksArchived(product));
    if (archivedOzon.length) {
      ozonQueuedUnarchiveActions = await queueOzonUnarchiveForLinkedProducts(archivedOzon, {
        source: options.source || options.sourceEvent || "linked_activation",
        warning: "linked_activation_queued",
      });
    }
    ozonRecovered = activeOzon;
  }
  const ozonFirstStockActions = ozonRecovered.length ? await restoreStocksOnMarketplaces(ozonRecovered) : [];
  const ozonUnarchiveActions = ozonQueuedUnarchiveActions.length
    ? ozonQueuedUnarchiveActions
    : (ozonRecovered.length
      ? await verifyMarketplaceUnarchiveActions(
        ozonRecovered,
        await unarchiveProductsOnMarketplaces(ozonRecovered, { forceOzonDailyLimit: options.forceOzonDailyLimit === true }),
      )
      : []);
  const ozonSecondStockActions = ozonRecovered.length ? await restoreStocksOnMarketplaces(ozonRecovered) : [];
  const yandexUnarchiveActions = yandexRecovered.length
    ? await verifyYandexUnarchiveActions(
      yandexRecovered,
      await unarchiveProductsOnMarketplaces(yandexRecovered, { forceOzonDailyLimit: options.forceOzonDailyLimit === true }),
    )
    : [];
  const yandexStockActions = yandexRecovered.length ? await restoreStocksOnMarketplaces(yandexRecovered) : [];
  const stockActions = [...ozonFirstStockActions, ...ozonSecondStockActions, ...yandexStockActions];
  const unarchiveActions = [...ozonUnarchiveActions, ...yandexUnarchiveActions];
  const productStatuses = summarizeSupplierRecoveryProducts(recovered, stockActions, unarchiveActions);
  await hydrateWarehouseProductsForIds(recovered.map((item) => item.id), { expandGroups: false });
  const warehouse = await readWarehouse();
  const now = new Date().toISOString();
  const recoveredIds = new Set(recovered.map((item) => String(item.id)));
  const sellableIds = new Set(productStatuses.filter((item) => item.sellable).map((item) => String(item.id)));
  const statusById = new Map(productStatuses.map((item) => [String(item.id), item]));
  const stockActionsById = new Map();
  const unarchiveActionsById = new Map();
  for (const action of stockActions) {
    const id = String(action.id || "");
    if (id) stockActionsById.set(id, action);
  }
  for (const action of unarchiveActions) {
    const id = String(action.id || "");
    if (id) unarchiveActionsById.set(id, action);
  }
  const restoredStockById = new Map(stockActions
    .filter((item) => item.ok)
    .map((item) => [String(item.id), Math.max(1, Math.round(Number(item.stock || 1)))]));
  const changedProducts = [];
  for (const product of warehouse.products) {
    const productId = String(product.id);
    if (!recoveredIds.has(productId)) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    const status = statusById.get(productId);
    const stockAction = stockActionsById.get(productId);
    const unarchiveAction = unarchiveActionsById.get(productId);
    if (stockAction) product.lastStockSend = marketplaceCommandFromAction(stockAction, product, now);
    if (unarchiveAction) product.lastArchiveSend = marketplaceCommandFromAction(unarchiveAction, product, now);
    if (status?.sellable) {
      product.noSupplierAutomation.recoveredAt = now;
      product.noSupplierAutomation.manualSellableAt = now;
      product.noSupplierAutomation.stockZeroAt = null;
      product.noSupplierAutomation.archivedAt = null;
      product.noSupplierAutomation.lastError = null;
    } else if (status?.error) {
      product.noSupplierAutomation.lastError = status.error;
    }
    if (sellableIds.has(productId)) {
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        code: "active",
        status: "active",
        archived: false,
        stock: restoredStockById.get(productId) || Math.max(1, Math.round(Number(product.marketplaceState?.stock || 1))),
      };
      product.status = "active";
      product.archived = false;
    }
    product.updatedAt = now;
    changedProducts.push(product);
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "supplier_recovery_automation" });
  } else if ((warehouse.products || []).length) {
    await writeWarehouse(warehouse);
  }
  const recoveryPriceIds = recovered.filter((item) => !warehouseProductUsesStockOnlyPricing(item)).map((item) => item.id);
  const recoveryPriceEvent = cleanText(options.sourceEvent || source) || "supplier_recovery_price_refresh";
  const priceRefresh = backgroundMarketplaceJobsBlocked()
    ? await sendWarehousePrices({
      productIds: recoveryPriceIds,
      marketplace: "all",
      reason: recoveryPriceEvent,
      sourceEvent: recoveryPriceEvent,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
    }).catch((error) => {
      logger.warn("supplier recovery inline price refresh failed", {
        source,
        detail: error?.message || String(error),
        products: recovered.length,
      });
      return { ok: false, accepted: false, sent: 0, failed: 0, skipped: [], priceIntentId: null, error: error?.message || String(error) };
    })
    : await queueAuthoritativePriceReprice({
      productIds: recoveryPriceIds,
      marketplace: "all",
      reason: recoveryPriceEvent,
      sourceEvent: recoveryPriceEvent,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: 2,
    }).catch((error) => {
      logger.warn("supplier recovery price refresh queue failed", {
        source,
        detail: error?.message || String(error),
        products: recovered.length,
      });
      return { ok: false, accepted: false, queued: 0, queuedBatches: 0, priceIntentId: null, error: error?.message || String(error) };
    });

  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, type: item.type, error: item.error }));
  const unarchivePending = unarchiveActions.filter((item) => item.pending).length;
  const queuedByDailyLimit = unarchiveActions.filter((item) => item.ok && item.queuedByDailyLimit).length;
  const queuedNextRetryAt = unarchiveActions
    .filter((item) => item.queuedByDailyLimit && item.nextRetryAt)
    .map((item) => item.nextRetryAt)
    .sort()[0] || null;
  const ozonQueueState = await readOzonUnarchiveQueue().catch(() => ({ items: [] }));
  const productStatusesTotal = productStatuses.length;
  logger.info("supplier recovery automation complete", {
    source,
    products: products.length,
    recovered: recovered.length,
    queuedPulled: queuedOzonProducts.length,
    sellableRecovered: sellableIds.size,
    restoredStocks: stockActions.filter((item) => item.ok).length,
    unarchived: unarchiveActions.filter((item) => item.ok).length,
    unarchivePending,
    queuedByDailyLimit,
    stockFailed: stockActions.filter((item) => !item.ok).length,
    unarchiveFailed: unarchiveActions.filter((item) => !item.ok).length,
    errors: errors.length,
    priceIntentId: priceRefresh.priceIntentId || null,
    priceQueued: priceRefresh.queued || 0,
  });
  return {
    recovered: recovered.length,
    sellableRecovered: sellableIds.size,
    restoredStocks: stockActions.filter((item) => item.ok).length,
    unarchived: unarchiveActions.filter((item) => item.ok).length,
    unarchivePending,
    queuedByDailyLimit,
    nextRetryAt: queuedNextRetryAt,
    queueSize: Array.isArray(ozonQueueState.items) ? ozonQueueState.items.length : 0,
    priceIntentId: priceRefresh.priceIntentId || null,
    priceQueued: priceRefresh.queued || 0,
    priceQueuedBatches: priceRefresh.queuedBatches || 0,
    queuedProcessedThisRun: queuedOzonProducts.length,
    queuedSamples: unarchiveActions
      .filter((item) => item.queuedByDailyLimit)
      .slice(0, 20)
      .map((item) => ({ id: item.id, offerId: item.offerId, target: item.target, nextRetryAt: item.nextRetryAt })),
    stockFailed: stockActions.filter((item) => !item.ok).length,
    unarchiveFailed: unarchiveActions.filter((item) => !item.ok).length,
    errors,
    productStatuses: productStatuses.slice(0, 200),
    productStatusesTotal,
    source,
  };
}

