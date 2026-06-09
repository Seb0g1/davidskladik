async function countWarehouseFilteredGroupTotal(prisma, filters = {}) {
  if (!prisma) return 0;
  if (cleanText(filters.q || "")) return 0;
  if (serverUnderMemoryPressure() || serverUnderHttpLoad()) {
    const cachedEstimate = getCachedWarehouseGroupTotal(filters);
    if (cachedEstimate > 0) return cachedEstimate;
    return 0;
  }
  const cacheKey = warehouseGroupCountCacheKey(filters);
  const cached = warehouseGroupCountCache.get(cacheKey);
  if (cached && Date.now() - cached.at < warehouseGroupCountCacheTtlMs) return cached.value;

  if (warehouseGroupCountUsesSqlFastPath(filters)) {
    const value = await countWarehouseGroupTotalPostgresSql(prisma, filters);
    warehouseGroupCountCache.set(cacheKey, { at: Date.now(), value });
    if (warehouseGroupCountCache.size > warehouseGroupCountCacheMax) {
      const oldestKey = warehouseGroupCountCache.keys().next().value;
      if (oldestKey) warehouseGroupCountCache.delete(oldestKey);
    }
    return value;
  }

  const linkedFilter = cleanText(filters.linked || "all");
  const needsComputedLinkFilter = linkedFilter === "ready" || linkedFilter === "changed" || linkedFilter === "linked_archived";
  const brandFilter = cleanText(filters.brand || "");
  let brandIndexProductIds = [];
  if (brandFilter) {
    await ensureWarehousePostgresBrandsBackfilled(prisma);
    brandIndexProductIds = await brandIndexProductIdsForFilterPostgres(prisma, brandFilter).catch(() => []);
  }
  const postgresFilters = {
    ...(needsComputedLinkFilter ? { ...filters, state: "all" } : filters),
    ...(brandIndexProductIds.length ? { brand: "" } : {}),
  };
  const where = warehousePagePostgresWhere(postgresFilters);
  if (brandIndexProductIds.length) where.AND.push({ id: { in: brandIndexProductIds } });

  const select = {
    id: true,
    offerId: true,
    marketplace: true,
    target: true,
    raw: true,
    brand: true,
  };
  let groupContext;
  let filteredRows;
  if (warehouseGroupCountUsesFullCatalogScan(filters)) {
    groupContext = await getWarehouseCatalogPairingGroupContext(prisma);
    const pairingCache = warehouseCatalogPairingContextCache;
    filteredRows = Array.isArray(pairingCache.pairingRows)
      ? pairingCache.pairingRows
      : await loadWarehouseCatalogPairingRows(prisma, select, where);
  } else {
    filteredRows = await loadWarehouseCatalogPairingRows(prisma, select, where);
    groupContext = buildWarehouseCatalogGroupContext(
      filteredRows.map((row) => productFromPostgres({ ...row, links: [] })),
    );
  }
  let products = filteredRows.map((row) => productFromPostgres({ ...row, links: [] }));
  if (needsComputedLinkFilter || isWarehouseStrictIdentitySearch(filters)) {
    products = products.filter((product) => warehousePageProductMatches(product, filters));
  }
  const value = countWarehouseProductGroups(products, groupContext);
  warehouseGroupCountCache.set(cacheKey, { at: Date.now(), value });
  if (warehouseGroupCountCache.size > warehouseGroupCountCacheMax) {
    const oldestKey = warehouseGroupCountCache.keys().next().value;
    if (oldestKey) warehouseGroupCountCache.delete(oldestKey);
  }
  return value;
}

