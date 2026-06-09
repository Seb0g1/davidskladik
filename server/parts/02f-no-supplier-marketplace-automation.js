async function runNoSupplierMarketplaceAutomation(preview, options = {}) {
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const now = new Date().toISOString();
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates(products, {
    includeNoLinks: Boolean(options.includeNoLinks),
    now,
    skipLinkedGrace: options.skipLinkedGrace === true,
  });
  const source = options.source || (Array.isArray(options.productIds) && options.productIds.length ? "targeted" : "full");

  if (!toZeroStock.length && !toArchive.length) {
    const productStatuses = summarizeNoSupplierAutomationProducts(products, []);
    logger.info("no-supplier automation complete", {
      source,
      products: products.length,
      zeroStockSent: 0,
      archived: 0,
      errors: 0,
      statuses: productStatuses.slice(0, 10),
    });
    return { zeroStockSent: 0, archived: 0, errors: [], productStatuses };
  }

  const stockActions = await sendZeroStocksToMarketplace(toZeroStock);
  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => item.id));
  const archiveMap = new Map();
  for (const product of toArchive) archiveMap.set(product.id, product);
  for (const product of toZeroStock) {
    if (stockOkIds.has(product.id) && !product.hasLinks) archiveMap.set(product.id, product);
  }
  const archiveActions = await archiveProductsOnMarketplaces(Array.from(archiveMap.values()));
  const allActions = [...stockActions, ...archiveActions];
  if (!allActions.length) {
    const productStatuses = summarizeNoSupplierAutomationProducts(products, []);
    return { zeroStockSent: 0, archived: 0, errors: [], productStatuses };
  }

  const hydratedProducts = await hydrateWarehouseProductsForIds(
    [...toZeroStock, ...toArchive].map((product) => product.id),
    { expandGroups: false },
  );
  const productSourceById = new Map();
  for (const product of [...products, ...toZeroStock, ...toArchive, ...hydratedProducts]) {
    if (!product?.id) continue;
    productSourceById.set(String(product.id), normalizeWarehouseProduct(product));
  }
  if (shouldUsePostgresStorage()) {
    const missingIds = [...new Set([...toZeroStock, ...toArchive].map((product) => String(product.id)))]
      .filter((id) => !productSourceById.has(id));
    if (missingIds.length) {
      const loaded = await readWarehouseProductsFromPostgresByIds(missingIds);
      for (const row of loaded) {
        const product = productFromPostgres(row);
        productSourceById.set(String(product.id), product);
      }
    }
  }
  const warehouse = await readWarehouse();
  const changedById = new Map();
  for (const action of allActions) {
    const product = productSourceById.get(String(action.id))
      || warehouse.products.find((item) => item.id === action.id);
    if (!product) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || { stockZeroAt: null, archivedAt: null, lastError: null };
    if (action.type === "zero_stock") product.lastStockSend = marketplaceCommandFromAction(action, product, now);
    if (action.type === "archive") product.lastArchiveSend = marketplaceCommandFromAction(action, product, now);
    if (action.ok && action.type === "zero_stock") {
      product.noSupplierAutomation.stockZeroAt = now;
      product.targetStock = 0;
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        stock: 0,
        code: product.marketplaceState?.code || "out_of_stock",
      };
    }
    if (action.ok && action.type === "archive") product.noSupplierAutomation.archivedAt = now;
    product.noSupplierAutomation.lastError = action.ok || action.skipped ? null : action.error;
    product.updatedAt = now;
    changedById.set(product.id, product);
  }
  const changedProducts = Array.from(changedById.values());
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "no_supplier_automation" });
  } else if ((warehouse.products || []).length) {
    await writeWarehouse(warehouse);
  }

  const errors = allActions
    .filter((item) => !item.ok && !item.skipped)
    .map((item) => ({ id: item.id, type: item.type, error: item.error }));
  const skipped = allActions
    .filter((item) => item.skipped)
    .map((item) => ({ id: item.id, type: item.type, reason: item.reason, error: item.error }));
  const productStatuses = summarizeNoSupplierAutomationProducts(products, allActions);
  logger.info("no-supplier automation complete", {
    source,
    products: products.length,
    zeroStockSent: stockActions.filter((item) => item.ok).length,
    archived: archiveActions.filter((item) => item.ok).length,
    skipped: skipped.length,
    errors: errors.length,
    statuses: productStatuses.slice(0, 10),
  });

  return {
    zeroStockSent: stockActions.filter((item) => item.ok).length,
    archived: archiveActions.filter((item) => item.ok).length,
    errors,
    skipped,
    productStatuses,
  };
}

