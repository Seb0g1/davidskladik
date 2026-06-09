function buildCommonWarehouseGroupLinks(products = [], incomingLinks = [], { now = new Date().toISOString(), username = "system" } = {}) {
  const byKey = new Map();
  const addLink = (input = {}) => {
    const normalized = normalizeWarehouseLink(input);
    if (!warehouseLinkHasMatchTarget(normalized)) return;
    const key = warehouseLinkTargetKey(normalized);
    const next = normalizeWarehouseLink({
      ...normalized,
      createdAt: normalized.createdAt || now,
      updatedAt: normalized.updatedAt || now,
      createdBy: normalized.createdBy || username,
      updatedBy: normalized.updatedBy || username,
    });
    const existing = byKey.get(key);
    byKey.set(key, existing ? mergeWarehouseLinkForSave(existing, next, { now, username }) : next);
  };
  for (const product of Array.isArray(products) ? products : []) {
    for (const link of Array.isArray(product.links) ? product.links : []) addLink(link);
  }
  for (const link of Array.isArray(incomingLinks) ? incomingLinks : []) addLink(link);
  return compactWarehouseLinks(Array.from(byKey.values()));
}

function marketplacePriceBreakdown(products = []) {
  return (Array.isArray(products) ? products : []).map((product) => {
    const normalized = normalizeWarehouseProduct(product);
    const supplier = product.selectedSupplier || {};
    const formula = product.priceFormula || {};
    const state = product.marketplaceState && typeof product.marketplaceState === "object" ? product.marketplaceState : {};
    const lastPriceSend = product.marketplace === "ozon"
      ? product.lastOzonPriceSend
      : product.lastYandexPriceSend;
    return {
      productId: product.id || "",
      offerId: product.offerId || "",
      marketplace: normalized.marketplace || "",
      target: product.target || "",
      targetName: resolveWarehouseProductTargetName(product),
      supplierName: supplier.supplierName || supplier.partnerName || supplier.name || "",
      supplierArticle: supplier.article || supplier.supplierArticle || "",
      markupCoefficient: Number(product.markupCoefficient || supplier.markupCoefficient || formula.markupCoefficient || 0) || null,
      baseMarkupCoefficient: Number(supplier.baseMarkupCoefficient || formula.baseMarkupCoefficient || 0) || null,
      usdRate: Number(product.usdRate || formula.usdRate || 0) || null,
      selectedSupplierPrice: Number(supplier.price ?? formula.selectedSupplierPrice ?? 0) || null,
      selectedSupplierCurrency: supplier.priceCurrency || supplier.currency || formula.selectedSupplierCurrency || "",
      calculatedPrice: Number(supplier.calculatedPrice || formula.calculatedPrice || 0) || null,
      targetPrice: Number(product.targetPrice || product.nextPrice || formula.targetPrice || 0) || null,
      currentPrice: Number(product.currentPrice || formula.currentPrice || 0) || null,
      targetStock: Number(product.targetStock || 0) || null,
      statusCode: cleanText(state.code || product.status || state.state).toLowerCase(),
      statusLabel: cleanText(state.label || state.stateName || product.status || ""),
      archived: Boolean(product.archived || state.archived || state.code === "archived"),
      changed: Boolean(product.changed),
      ready: Boolean(product.ready),
      stockOnlyFallback: Boolean(product.stockOnlyFallbackActive),
      ozonMinPrice: product.marketplace === "ozon" ? (Number(product.ozonMinPrice || 0) || null) : null,
      lastPriceSendAt: cleanText(lastPriceSend?.at || lastPriceSend?.requestedAt || lastPriceSend?.lastPriceRequestAt || ""),
      lastPriceVerifiedAt: cleanText(lastPriceSend?.verifiedAt || lastPriceSend?.lastPriceVerifiedAt || ""),
      ozonUnarchiveQueued: Boolean(product.lastArchiveSend?.queuedByDailyLimit || product.lastArchiveSend?.pending),
      ozonUnarchiveNextRetryAt: cleanText(product.lastArchiveSend?.nextRetryAt || ""),
    };
  });
}


