async function buildWarehouseSkuDiagnostics(sku = "", { limit = 50, auditLimit = 30, usdRate } = {}) {
  const query = cleanText(sku);
  if (!query) {
    const error = new Error("Укажите sku.");
    error.statusCode = 400;
    throw error;
  }
  const normalizedQuery = normalizeWarehouseSearchToken(query);
  const warehouse = await readWarehouse();
  const products = Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [];
  const strictMatches = products.filter((product) => warehouseProductMatchesSearchQuery(product, query));
  const focusedMatches = preferWarehousePrimaryIdentityMatches(strictMatches, { q: query });
  const hiddenSupplierOnlyMatches = Math.max(0, strictMatches.length - focusedMatches.length);
  const primaryOfferIds = new Set(
    focusedMatches
      .filter((product) => warehouseProductSearchRank(product, query) <= 2)
      .map((product) => cleanText(product.offerId).toLowerCase())
      .filter(Boolean),
  );
  const productIds = new Set(focusedMatches.map((product) => String(product.id)));
  if (primaryOfferIds.size) {
    for (const product of products) {
      const offerId = cleanText(product.offerId).toLowerCase();
      if (offerId && primaryOfferIds.has(offerId)) productIds.add(String(product.id));
    }
  }
  const matchedGroupKeys = new Set(
    products
      .filter((product) => productIds.has(String(product.id)))
      .map(warehouseProductPageGroupKey)
      .filter(Boolean),
  );
  if (matchedGroupKeys.size) {
    for (const product of products) {
      if (matchedGroupKeys.has(warehouseProductPageGroupKey(product))) productIds.add(String(product.id));
    }
  }
  const matchedProducts = products
    .filter((product) => productIds.has(String(product.id)))
    .slice(0, Math.max(1, Math.min(100, Math.round(Number(limit || 50) || 50))));
  const built = matchedProducts.length
    ? await buildFreshWarehouseProductsFromKnownProducts(
      warehouse,
      matchedProducts,
      { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate },
    )
    : [];
  const builtById = new Map(built.map((product) => [String(product.id), normalizeWarehouseProduct(product)]));
  const diagnosticProducts = matchedProducts.map((product) => {
    const builtProduct = builtById.get(String(product.id));
    if (!builtProduct) return normalizeWarehouseDetailProduct(product);
    return normalizeWarehouseDetailProduct({
      ...builtProduct,
      selectedSupplier: builtProduct.selectedSupplier || product.selectedSupplier || null,
      supplierCount: Number(builtProduct.supplierCount || product.supplierCount || 0),
      availableSupplierCount: Number(builtProduct.availableSupplierCount || product.availableSupplierCount || 0),
      targetStock: Number(builtProduct.targetStock || 0) > 0 ? builtProduct.targetStock : product.targetStock,
      links: Array.isArray(builtProduct.links) && builtProduct.links.length ? builtProduct.links : product.links,
    });
  });
  const latestAudit = await readAuditFiltered({ q: query }, Math.max(1, Math.min(100, Math.round(Number(auditLimit || 30) || 30))));
  const warnings = [];
  if (!diagnosticProducts.length) warnings.push("sku_not_found");
  if (hiddenSupplierOnlyMatches > 0) warnings.push("supplier_only_matches_hidden_by_primary_identity");
  const firstGroupKey = diagnosticProducts.map(warehouseProductPageGroupKey).find(Boolean) || "";
  const groupProducts = firstGroupKey
    ? diagnosticProducts.filter((product) => warehouseProductPageGroupKey(product) === firstGroupKey)
    : diagnosticProducts;
  const groupSummary = buildWarehouseDiagnosticsGroupSummary(groupProducts);
  return {
    sku: query,
    normalizedSku: normalizedQuery,
    groupKey: firstGroupKey,
    group: {
      groupKey: firstGroupKey,
      offerId: cleanText(groupProducts[0]?.offerId || query),
      manualGroupId: cleanText(groupProducts[0]?.manualGroupId || groupProducts[0]?.raw?.manualGroupId || ""),
      name: cleanText(groupProducts[0]?.name || groupProducts[0]?.offerId || query),
      marketplaces: groupSummary.marketplaces,
      statusSummary: groupSummary,
    },
    statusSummary: groupSummary,
    matched: diagnosticProducts.length,
    hiddenSupplierOnlyMatches,
    warnings,
    products: diagnosticProducts.map((product) => publicWarehouseDiagnosticProduct(product, diagnosticProducts)),
    audit: latestAudit.map((entry) => ({
      at: entry.at,
      user: entry.user,
      action: entry.action,
      productIds: auditEntryProductIds(entry),
      details: entry.details || {},
    })),
  };
}
