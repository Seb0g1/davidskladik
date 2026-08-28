function warehouseGroupCountCacheKey(filters = {}) {
  return JSON.stringify({
    filters: {
      q: cleanText(filters.q || "").toLowerCase(),
      autoOnly: Boolean(filters.autoOnly),
      linked: cleanText(filters.linked || "all"),
      marketplace: cleanText(filters.marketplace || "all"),
      state: cleanText(filters.state || "all"),
      brand: cleanText(filters.brand || "").toLowerCase(),
    },
    storage: shouldUsePostgresStorage() ? "postgres" : "json",
  });
}

function countWarehouseProductGroups(products = [], groupContext = null) {
  const context = groupContext || buildWarehouseCatalogGroupContext(products);
  const keys = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const groupKey = warehouseProductPageGroupKey(normalized, context) || `id:${normalized.id}`;
    keys.add(groupKey);
  }
  return keys.size;
}

async function getWarehouseCatalogPairingGroupContext(prisma) {
  if (!prisma) return buildWarehouseCatalogGroupContext([]);
  const now = Date.now();
  if (
    warehouseCatalogPairingContextCache.groupContext
    && now - warehouseCatalogPairingContextCache.at < warehouseCatalogPairingContextCacheTtlMs
  ) {
    return warehouseCatalogPairingContextCache.groupContext;
  }
  const select = {
    id: true,
    offerId: true,
    marketplace: true,
    target: true,
    raw: true,
    brand: true,
  };
  const pairingRows = await loadWarehouseCatalogPairingRows(prisma, select);
  const groupContext = buildWarehouseCatalogGroupContext(pairingRows.map((row) => productFromPostgres({ ...row, links: [] })));
  warehouseCatalogPairingContextCache = { at: now, groupContext, pairingRows };
  return groupContext;
}

function warehouseGroupCountUsesFullCatalogScan(filters = {}) {
  if (cleanText(filters.q || "")) return false;
  if (Boolean(filters.autoOnly)) return false;
  if (cleanText(filters.brand || "")) return false;
  if (cleanText(filters.marketplace || "all") !== "all") return false;
  if (cleanText(filters.state || "all") !== "all") return false;
  return cleanText(filters.linked || "all") === "all";
}

function warehouseGroupCountUsesSqlFastPath(filters = {}) {
  const linked = cleanText(filters.linked || "all");
  if (linked !== "linked" && linked !== "unlinked") return false;
  if (cleanText(filters.q || "")) return false;
  if (Boolean(filters.autoOnly)) return false;
  if (cleanText(filters.brand || "")) return false;
  if (cleanText(filters.marketplace || "all") !== "all") return false;
  if (cleanText(filters.state || "all") !== "all") return false;
  return true;
}

