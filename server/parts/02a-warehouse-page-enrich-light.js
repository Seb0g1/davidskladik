function warehousePageShouldUseLightEnrich(filters = {}) {
  const linked = cleanText(filters.linked || "all");
  if (linked === "ready" || linked === "changed" || linked === "linked_archived") return false;
  return true;
}

function enrichWarehousePageProductsLight(pageWarehouse = {}) {
  const warehouse = {
    ...(pageWarehouse || {}),
    products: applyGroupLinkInheritanceForPage(Array.isArray(pageWarehouse?.products) ? pageWarehouse.products : []),
    suppliers: Array.isArray(pageWarehouse?.suppliers) ? pageWarehouse.suppliers : [],
  };
  const propagated = propagateGroupSupplierContextForPage(warehouse.products);
  return propagated.map((product) => mapWarehousePageItemFromProduct(product));
}

async function enrichWarehousePageProductsForDisplay(pageWarehouse = {}, { usdRate = 95, traceStartedAt = 0, traceLabel = "postgres:after-build" } = {}) {
  const warehouse = {
    ...(pageWarehouse || {}),
    products: applyGroupLinkInheritanceForPage(Array.isArray(pageWarehouse?.products) ? pageWarehouse.products : []),
    suppliers: Array.isArray(pageWarehouse?.suppliers) ? pageWarehouse.suppliers : [],
  };
  const linkedIds = warehouse.products
    .filter((product) => compactWarehouseLinks(product.links || []).length > 0)
    .map((product) => product.id);
  let builtMap = new Map();
  if (linkedIds.length) {
    const built = await buildFreshWarehouseProductsForWarehouse(
      warehouse,
      linkedIds,
      {
        refreshPrices: false,
        persistMutations: false,
        livePriceMaster: false,
        batchPriceMaster: false,
        usdRate,
      },
    );
    if (traceStartedAt) pageTrace(traceLabel, traceStartedAt);
    builtMap = new Map(built.map((product) => [product.id, product]));
    queueLinkedUnavailableSupplierZeroStock(built, { source: "warehouse_page" });
  }
  const enrichedProducts = propagateGroupSupplierContextForPage(
    warehouse.products.map((product) => builtMap.get(product.id) || product),
  );
  return enrichedProducts.map((product) => mapWarehousePageItemFromProduct(product));
}

