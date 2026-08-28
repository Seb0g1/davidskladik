function mapWarehousePageItemFromProduct(product = {}) {
  const item = normalizeWarehouseProduct(product);
  const links = Array.isArray(item.links) ? item.links : [];
  const selectedSupplier = product.selectedSupplier || null;
  const stockOnlyFallbackActive = Boolean(product.stockOnlyFallbackActive);
  const hasSupplier = Boolean(selectedSupplier) || stockOnlyFallbackActive;
  return {
    ...item,
    autoPriceEnabled: item.autoPriceEnabled !== false,
    links,
    suppliers: Array.isArray(product.suppliers) ? product.suppliers : [],
    selectedSupplier,
    stockOnlyFallbackActive,
    nextPrice: Number(product.nextPrice ?? product.newPrice ?? item.targetPrice ?? 0) || null,
    newPrice: Number(product.newPrice ?? product.nextPrice ?? 0) || null,
    targetPrice: Number(product.targetPrice ?? item.targetPrice ?? 0) || null,
    ready: Boolean(product.ready),
    changed: Boolean(product.changed),
    supplierCount: Number(product.supplierCount ?? item.supplierCount ?? 0),
    availableSupplierCount: Number(product.availableSupplierCount ?? item.availableSupplierCount ?? 0),
    priceSource: product.priceSource || null,
    priceSelectionReason: product.priceSelectionReason || null,
    noSupplierAutomation: item.noSupplierAutomation || {},
    marketplaceState: item.marketplaceState || {},
    targetStock: Number.isFinite(Number(product.targetStock))
      ? Number(product.targetStock)
      : (Number.isFinite(Number(product.priceFormula?.targetStock)) ? Number(product.priceFormula.targetStock) : item.targetStock),
    partial: Boolean(product.partial) || (links.length > 0 && !hasSupplier),
  };
}

function mapWarehousePageProductsLight(pageProducts = [], normalizedSuppliers = []) {
  return (Array.isArray(pageProducts) ? pageProducts : []).map((product) => {
    const item = mapWarehousePageItemFromProduct(product);
    return {
      ...item,
      suppliers: Array.isArray(item.suppliers) && item.suppliers.length ? item.suppliers : normalizedSuppliers,
    };
  });
}

function applyGroupLinkInheritanceForPage(products = []) {
  const normalized = (Array.isArray(products) ? products : []).map(normalizeWarehouseProduct);
  if (!normalized.length) return normalized;
  const groupContext = buildWarehouseCatalogGroupContext(normalized);
  const donorsByKey = new Map();
  for (const product of normalized) {
    const links = compactWarehouseLinks(product.links || []);
    if (!links.length) continue;
    const key = warehouseProductPageGroupKey(product, groupContext) || `id:${product.id}`;
    const existing = donorsByKey.get(key);
    if (!existing || compactWarehouseLinks(existing.links || []).length < links.length) {
      donorsByKey.set(key, normalizeWarehouseProduct({ ...product, links }));
    }
  }
  return normalized.map((product) => {
    const links = compactWarehouseLinks(product.links || []);
    if (links.length) return product;
    const key = warehouseProductPageGroupKey(product, groupContext) || `id:${product.id}`;
    const donor = donorsByKey.get(key);
    if (!donor?.links?.length) return product;
    return normalizeWarehouseProduct({ ...product, links: donor.links });
  });
}

function displaySupplierFromProductLinks(product = {}) {
  if (product?.selectedSupplier) {
    if (warehouseProductUsesStockOnlyPricing(product)) return null;
    return product.selectedSupplier;
  }
  const link = compactWarehouseLinks(product?.links || [])[0];
  if (!link) return null;
  const supplierName = cleanText(link.supplierName || link.name || "");
  const article = cleanText(link.article || link.supplierArticle || "");
  if (!supplierName && !article) return null;
  const candidate = {
    supplierName,
    article,
    partnerId: link.partnerId || "",
    price: link.price ?? link.calculatedPrice ?? null,
    displayOnly: true,
  };
  if (supplierUsesStockOnlyPricing(null, candidate)) return null;
  return candidate;
}


