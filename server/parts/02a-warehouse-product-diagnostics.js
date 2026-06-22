function buildWarehouseProductAutomationDiagnostics(product = {}, contextProducts = []) {
  const candidates = Array.isArray(contextProducts) && contextProducts.length ? contextProducts : [product];
  const noSupplier = pickNoSupplierAutomationCandidates(candidates, { includeNoLinks: true });
  const toZero = new Set(noSupplier.toZeroStock.map((item) => String(item.id)));
  const toArchive = new Set(noSupplier.toArchive.map((item) => String(item.id)));
  const hasDirectLinks = product.hasLinks || (Array.isArray(product.links) && product.links.length > 0);
  return {
    wouldSendTargetStock: shouldSendTargetStockForProduct(product),
    wouldRecoverSupplier: pickSupplierRecoveryCandidates(candidates, { productIds: [product.id] }).some((item) => String(item.id) === String(product.id)),
    wouldZeroStockAsNoSupplier: toZero.has(String(product.id)),
    wouldArchiveAsNoSupplier: toArchive.has(String(product.id)),
    protectedFromNoSupplierArchive: Boolean(hasDirectLinks || product.noSupplierAutomation?.manualSellableAt),
    sameOfferGroupProtected: Boolean(
      marketplaceOfferAutomationKey(product)
      && candidates.some((item) =>
        String(item.id) !== String(product.id)
        && marketplaceOfferAutomationKey(item) === marketplaceOfferAutomationKey(product)
        && (item.hasLinks || (Array.isArray(item.links) && item.links.length > 0) || item.noSupplierAutomation?.manualSellableAt)
      )
    ),
    needsSalesRecovery: marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true }),
  };
}

function marketplaceCommandHasError(command = {}) {
  const normalized = command && typeof command === "object" ? command : {};
  const status = cleanText(normalized.status || "").toLowerCase();
  return Boolean(status === "error" || status === "failed" || status.includes("fail") || normalized.error);
}

function marketplaceCommandIsPending(command = {}) {
  const normalized = command && typeof command === "object" ? command : {};
  const status = cleanText(normalized.status || "").toLowerCase();
  const detail = cleanText(normalized.detail || normalized.warning || normalized.error || "").toLowerCase();
  return Boolean(
    status === "pending"
      || status === "queued"
      || status === "accepted"
      || status === "processing"
      || detail.includes("pending")
      || detail.includes("not_visible")
  );
}

function warehouseProductDiagnosticSaleState(product = {}, contextProducts = []) {
  const state = product.marketplaceState || {};
  const lastStockSend = latestProductCommand(product, "stock");
  const lastArchiveSend = latestProductCommand(product, "archive");
  const lastPriceSend = normalizeWarehouseProduct(product).marketplace === "yandex"
    ? normalizeLastPriceSend(product.lastYandexPriceSend || {})
    : normalizeLastPriceSend(product.lastOzonPriceSend || product.lastYandexPriceSend || {});
  const hasLinks = Boolean(product.hasLinks || (Array.isArray(product.links) && product.links.length));
  const selectedSupplier = Boolean(product.selectedSupplier || Number(product.availableSupplierCount || 0) > 0);
  const archived = Boolean(productLooksArchived(product));
  const stock = Number(product.targetStock ?? product.stock ?? state.stock ?? state.availableStock ?? 0);
  const stateCode = cleanText(state.code || product.status || "").toLowerCase();
  const marketplaceStock = Number(state.stock ?? state.present ?? state.availableStock ?? NaN);
  const lastError = cleanText(
    product.noSupplierAutomation?.lastError
      || lastStockSend?.error
      || lastArchiveSend?.error
      || lastPriceSend?.detail
      || "",
  );

  if (marketplaceCommandHasError(lastStockSend) || marketplaceCommandHasError(lastArchiveSend) || marketplaceCommandHasError(lastPriceSend) || lastError) {
    return { code: "api_error", label: "API error", reason: lastError || "last_marketplace_command_failed" };
  }
  if (marketplaceCommandIsPending(lastStockSend) || marketplaceCommandIsPending(lastArchiveSend) || marketplaceCommandIsPending(lastPriceSend)) {
    return { code: "api_pending", label: "API pending", reason: "marketplace_status_not_visible_yet" };
  }
  if (archived) return { code: "archived", label: "Архив", reason: "marketplace_card_archived" };
  if (!hasLinks) return { code: "no_links", label: "Нет привязки", reason: "no_pricemaster_links" };
  if (!selectedSupplier) {
    if (marketplaceHasPositiveStock(product)) {
      return { code: "no_supplier", label: "Нет поставщика", reason: "linked_without_supplier_marketplace_stock_positive" };
    }
    return { code: "no_supplier", label: "Нет поставщика", reason: "linked_but_supplier_not_selected" };
  }
  if (!Number.isFinite(stock) || stock <= 0) return { code: "no_stock", label: "Нет остатка", reason: "target_stock_is_zero" };
  if (
    stock > 0
    && (stateCode === "out_of_stock" || stateCode === "inactive" || stateCode.includes("out_of_stock"))
    && (!Number.isFinite(marketplaceStock) || marketplaceStock <= 0)
  ) {
    return { code: "stock_stale", label: "Stock push needed", reason: "linked_supplier_target_stock_positive_but_marketplace_stock_is_zero" };
  }
  return { code: "ready", label: "Готов к продаже", reason: "linked_supplier_stock_and_not_archived" };
}

function buildWarehouseDiagnosticsGroupSummary(products = []) {
  const summary = {
    total: 0,
    linked: 0,
    ready: 0,
    archived: 0,
    noLinks: 0,
    noSupplier: 0,
    noStock: 0,
    stockStale: 0,
    apiPending: 0,
    apiError: 0,
    marketplaces: [],
  };
  const marketplaces = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const saleState = warehouseProductDiagnosticSaleState(normalized, products);
    summary.total += 1;
    if (normalized.hasLinks || (Array.isArray(normalized.links) && normalized.links.length)) summary.linked += 1;
    if (saleState.code === "ready") summary.ready += 1;
    if (saleState.code === "archived") summary.archived += 1;
    if (saleState.code === "no_links") summary.noLinks += 1;
    if (saleState.code === "no_supplier") summary.noSupplier += 1;
    if (saleState.code === "no_stock") summary.noStock += 1;
    if (saleState.code === "stock_stale") summary.stockStale += 1;
    if (saleState.code === "api_pending") summary.apiPending += 1;
    if (saleState.code === "api_error") summary.apiError += 1;
    const marketplace = cleanText(normalized.marketplace || normalized.target || "");
    if (marketplace) marketplaces.add(marketplace);
  }
  summary.marketplaces = Array.from(marketplaces).sort();
  return summary;
}

function publicWarehouseDiagnosticProduct(product = {}, contextProducts = []) {
  const state = product.marketplaceState || {};
  const saleState = warehouseProductDiagnosticSaleState(product, contextProducts);
  return {
    id: product.id,
    groupKey: warehouseProductPageGroupKey(product) || "",
    marketplace: product.marketplace || "",
    target: product.target || "",
    targetName: product.targetName || "",
    offerId: product.offerId || "",
    productId: product.productId || "",
    sku: product.sku || product.ozon?.sku || product.yandex?.sku || "",
    barcode: product.barcode || product.ozon?.barcode || product.yandex?.barcode || "",
    name: product.name || "",
    brand: resolveWarehouseBrand(product),
    hasLinks: Boolean(product.hasLinks || (Array.isArray(product.links) && product.links.length)),
    ready: Boolean(product.ready),
    changed: Boolean(product.changed),
    supplierCount: Number(product.supplierCount || 0),
    availableSupplierCount: Number(product.availableSupplierCount || 0),
    selectedSupplier: publicSelectedSupplierDiagnostics(product.selectedSupplier),
    markupCoefficient: product.markupCoefficient ?? null,
    usdRate: product.usdRate ?? product.priceFormula?.usdRate ?? null,
    priceFormula: product.priceFormula || null,
    currentPrice: product.currentPrice ?? null,
    targetPrice: product.targetPrice ?? null,
    targetStock: product.targetStock ?? null,
    lastOzonPriceSend: product.lastOzonPriceSend || null,
    lastYandexPriceSend: product.lastYandexPriceSend || null,
    lastStockSend: latestProductCommand(product, "stock"),
    lastArchiveSend: latestProductCommand(product, "archive"),
    automation: buildWarehouseProductAutomationDiagnostics(product, contextProducts),
    autoPriceEnabled: product.autoPriceEnabled !== false,
    status: product.status || state.code || "",
    archived: Boolean(product.archived || state.archived || cleanText(state.code).toLowerCase() === "archived"),
    saleState,
    saleStateCode: saleState.code,
    saleStateLabel: saleState.label,
    saleReason: saleState.reason,
    marketplaceState: state,
    noSupplierAutomation: product.noSupplierAutomation || {},
    links: (Array.isArray(product.links) ? product.links : []).map((link) => ({
      id: link.id || "",
      article: link.article || "",
      supplierName: link.supplierName || "",
      partnerId: link.partnerId || "",
      keyword: link.keyword || "",
      matchType: link.matchType || "",
      sourceRowId: link.sourceRowId || "",
      updatedAt: link.updatedAt || "",
      updatedBy: link.updatedBy || "",
      matchedCount: Number(link.matchedCount ?? 0),
      availableCount: Number(link.availableCount ?? 0),
      available: link.availableCount > 0,
      missingInPriceMaster: Boolean(link.missingInPriceMaster),
      unavailableInPriceMaster: Boolean(link.unavailableInPriceMaster),
    })),
  };
}
