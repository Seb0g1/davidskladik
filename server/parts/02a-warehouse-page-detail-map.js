async function buildWarehouseDetailProductsFromPageWarehouse(pageWarehouse = {}, { refreshPrices = false, usdRate = 95 } = {}) {
  const warehouse = {
    ...(pageWarehouse || {}),
    products: applyGroupLinkInheritanceForPage(Array.isArray(pageWarehouse?.products) ? pageWarehouse.products : []),
    suppliers: Array.isArray(pageWarehouse?.suppliers) ? pageWarehouse.suppliers : [],
  };
  let pageProducts = warehouse.products;
  if (refreshPrices) {
    pageProducts = await enrichWeakOzonProductsForPage(warehouse.products);
  }
  const linkedIds = pageProducts
    .filter((product) => compactWarehouseLinks(product.links || []).length > 0)
    .map((product) => product.id);
  let builtMap = new Map();
  if (linkedIds.length) {
    const built = await buildFreshWarehouseProductsForWarehouse(
      { products: pageProducts, suppliers: warehouse.suppliers },
      linkedIds,
      {
        refreshPrices,
        persistMutations: false,
        livePriceMaster: refreshPrices,
        batchPriceMaster: refreshPrices,
        usdRate,
        priceMasterTimeoutMs: refreshPrices ? autoPricePmTimeoutMs : undefined,
      },
    );
    builtMap = new Map(built.map((product) => [product.id, product]));
    queueLinkedUnavailableSupplierZeroStock(built, { source: "warehouse_group_detail" });
  }
  const enrichedProducts = propagateGroupSupplierContextForPage(
    pageProducts.map((product) => builtMap.get(product.id) || product),
  );
  return enrichedProducts.map((product) => normalizeWarehouseDetailProduct(product));
}

function mapServerWarehousePageGroup(group = {}) {
  const products = Array.isArray(group.products) ? group.products.map(mapWarehousePageItemFromProduct) : [];
  const primary = products.find((product) => product.selectedSupplier)
    || products.find((product) => Number(product.nextPrice || product.targetPrice || 0) > 0)
    || products.find((product) => cleanText(product.marketplace).toLowerCase() === "ozon")
    || products.find((product) => cleanText(product.marketplace).toLowerCase() === "yandex")
    || products[0]
    || null;
  return {
    ...group,
    groupKey: group.groupKey || "",
    primary,
    products,
    links: Array.isArray(group.links) ? group.links : [],
    marketplaces: Array.isArray(group.marketplaces) ? group.marketplaces : [],
    statusSummary: group.statusSummary || {
      total: products.length,
      linked: 0,
      archived: 0,
      ready: 0,
      changed: 0,
      withoutSupplier: 0,
    },
  };
}

