function propagateGroupSupplierContextForPage(products = []) {
  const originals = Array.isArray(products) ? products.map((product) => ({ ...product })) : [];
  if (!originals.length) return originals;
  const normalized = originals.map(normalizeWarehouseProduct);
  const groupContext = buildWarehouseCatalogGroupContext(normalized);
  const donorsByKey = new Map();
  const stockDonorsByKey = new Map();
  const donorScore = (product = {}) => {
    if (warehouseProductUsesStockOnlyPricing(product)) return 0;
    if (product.selectedSupplier) return 3;
    if (displaySupplierFromProductLinks(product)) return 2;
    if (Number(product.nextPrice || product.targetPrice || 0) > 0) return 2;
    if (compactWarehouseLinks(product.links || []).length) return 1;
    return 0;
  };
  const stockDonorScore = (product = {}) => {
    const stock = Number(product.targetStock ?? product.priceFormula?.targetStock ?? 0);
    if (stock <= 0) return 0;
    if (warehouseProductUsesStockOnlyPricing(product)) return 2;
    if (product.selectedSupplier || displaySupplierFromProductLinks(product)) return 3;
    return 1;
  };
  for (const product of originals) {
    const key = warehouseProductPageGroupKey(normalizeWarehouseProduct(product), groupContext) || `id:${product.id}`;
    const score = donorScore(product);
    if (score) {
      const prev = donorsByKey.get(key);
      if (!prev || donorScore(prev) < score) donorsByKey.set(key, product);
    }
    const stockScore = stockDonorScore(product);
    if (stockScore) {
      const prevStock = stockDonorsByKey.get(key);
      if (!prevStock || stockDonorScore(prevStock) < stockScore) stockDonorsByKey.set(key, product);
    }
  }
  return originals.map((product) => {
    if (product.selectedSupplier || product.stockOnlyFallbackActive) return product;
    const links = compactWarehouseLinks(product.links || []);
    if (!links.length) return product;
    const key = warehouseProductPageGroupKey(normalizeWarehouseProduct(product), groupContext) || `id:${product.id}`;
    const donor = donorsByKey.get(key);
    const stockDonor = stockDonorsByKey.get(key);
    const stockDonorTargetStock = Number(stockDonor?.targetStock ?? stockDonor?.priceFormula?.targetStock ?? 0);
    const productTargetStock = Number(product.targetStock ?? product.priceFormula?.targetStock ?? 0);
    if (!donor || !donorScore(donor)) {
      if (!stockDonor || !stockDonorScore(stockDonor)) return product;
      return {
        ...product,
        targetStock: productTargetStock > 0 ? productTargetStock : (stockDonorTargetStock > 0 ? stockDonorTargetStock : product.targetStock),
      };
    }
    const donorTargetStock = Number(donor.targetStock ?? donor.priceFormula?.targetStock ?? 0);
    if (warehouseProductUsesStockOnlyPricing(donor)) {
      return {
        ...product,
        targetStock: productTargetStock > 0 ? productTargetStock : (donorTargetStock > 0 ? donorTargetStock : product.targetStock),
      };
    }
    return {
      ...product,
      selectedSupplier: donor.selectedSupplier || displaySupplierFromProductLinks(donor) || null,
      stockOnlyFallbackActive: Boolean(donor.stockOnlyFallbackActive),
      nextPrice: Number(product.nextPrice || donor.nextPrice || 0) || product.nextPrice,
      targetPrice: Number(product.targetPrice || donor.targetPrice || 0) || product.targetPrice,
      targetStock: productTargetStock > 0 ? productTargetStock : (donorTargetStock > 0 ? donorTargetStock : (stockDonorTargetStock > 0 ? stockDonorTargetStock : product.targetStock)),
      priceFormula: product.priceFormula || donor.priceFormula,
      supplierCount: Number(donor.supplierCount || product.supplierCount || 0),
      availableSupplierCount: Number(donor.availableSupplierCount || product.availableSupplierCount || 0),
      priceSource: product.priceSource || donor.priceSource,
      priceSelectionReason: product.priceSelectionReason || donor.priceSelectionReason,
      status: product.status || donor.status,
      ready: Boolean(product.ready || donor.ready || (donor.selectedSupplier && donorTargetStock > 0)),
    };
  });
}


