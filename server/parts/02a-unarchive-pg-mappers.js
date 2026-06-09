function productToPostgresData(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const images = compactObject({
    imageUrl: normalized.imageUrl || null,
    images: normalized.ozon?.images || normalized.yandex?.pictures || [],
  });
  return {
    id: normalized.id,
    marketplace: normalizeMarketplaceEnum(normalized.marketplace),
    target: normalized.target || normalized.marketplace || null,
    offerId: normalized.offerId || normalized.sku || normalized.id,
    productId: normalized.productId || null,
    name: normalized.name || normalized.offerId || normalized.id,
    brand: resolveWarehouseBrand(normalized) || null,
    images: cloneAuditValue(images) || {},
    marketplaceState: cloneAuditValue(normalized.marketplaceState) || {},
    currentPrice: roundPrice(normalized.marketplacePrice || 0) || null,
    targetPrice: roundPrice(normalized.nextPrice || normalized.targetPrice || normalized.calculatedPrice || 0) || null,
    targetStock: Number.isFinite(Number(normalized.targetStock)) ? Number(normalized.targetStock) : null,
    status: normalized.marketplaceState?.code || normalized.marketplaceState?.state || normalized.status || null,
    archived: Boolean(normalized.marketplaceState?.archived || normalized.archived),
    everHadLinks: Boolean(normalized.everHadLinks),
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
}

function supplierToPostgresData(supplier = {}) {
  const normalized = normalizeManagedSupplier(supplier);
  return {
    partnerId: normalized.partnerId || normalized.id || normalizeSupplierName(normalized.name),
    name: normalized.name || normalized.partnerId || normalized.id,
    active: !normalized.stopped,
    defaultCurrency: normalized.priceCurrency === "RUB" ? "RUB" : "USD",
    stopReason: normalized.stopReason || null,
    note: normalized.note || normalized.inactiveComment || null,
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
}

function linkToPostgresData(product, link = {}) {
  const normalized = normalizeWarehouseLink(link);
  const supplierArticle = normalized.article || normalized.sourceRowId || normalized.exactName;
  const data = {
    id: "",
    productId: product.id,
    supplierArticle,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    priceCurrency: normalized.priceCurrency === "RUB" ? "RUB" : "USD",
    keyword: normalized.keyword || null,
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
  data.id = crypto.createHash("sha1").update(productLinkPostgresIdentityKey(data)).digest("hex");
  return data;
}

function productLinkPostgresIdentityKey(data = {}) {
  return [
    cleanText(data.productId),
    cleanText(data.supplierArticle).toLowerCase(),
    cleanText(data.partnerId).toLowerCase(),
    normalizeSupplierName(data.supplierName),
    cleanText(data.keyword).toLowerCase(),
    cleanText(data.priceCurrency || "USD").toUpperCase(),
  ].join("|");
}

function dedupeProductLinkRows(rows = []) {
  const byIdentity = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || !row.productId || !row.supplierArticle) continue;
    const key = productLinkPostgresIdentityKey(row);
    const existing = byIdentity.get(key);
    if (!existing) {
      byIdentity.set(key, row);
      continue;
    }
    const existingUpdatedAt = toDateOrNull(existing.updatedAt)?.getTime() || 0;
    const rowUpdatedAt = toDateOrNull(row.updatedAt)?.getTime() || 0;
    byIdentity.set(key, rowUpdatedAt >= existingUpdatedAt ? row : existing);
  }
  return Array.from(byIdentity.values());
}

