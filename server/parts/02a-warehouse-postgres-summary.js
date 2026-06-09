function marketplaceStateCodeFromPostgresRow(row = {}) {
  const state = row.marketplaceState && typeof row.marketplaceState === "object" && !Array.isArray(row.marketplaceState)
    ? row.marketplaceState
    : {};
  return cleanText(state.code || row.status || state.state).toLowerCase();
}

async function getMarketplaceStateCountsFromPostgres(prisma, marketplace = "ozon") {
  return warehouseStateCounterFromPostgres(prisma, marketplace);
}

async function getOzonStateCountsFromPostgres(prisma) {
  return getMarketplaceStateCountsFromPostgres(prisma, "ozon");
}

async function getWarehousePostgresSummary(prisma, rate) {
  const cacheKey = Number(rate || 0).toFixed(4);
  if (
    warehousePostgresSummaryCache
    && warehousePostgresSummaryCache.key === cacheKey
    && Date.now() - warehousePostgresSummaryCache.at < warehousePostgresSummaryCacheTtlMs
    && !warehousePostgresSummaryCache.value?.lightweight
  ) {
    return warehousePostgresSummaryCache.value;
  }
  if (process.env.WAREHOUSE_FULL_SUMMARY_ENABLED !== "true") {
    const light = await getWarehousePostgresSummaryLight(prisma, rate);
    const value = { ...light, lightweight: false };
    warehousePostgresSummaryCache = { key: cacheKey, at: Date.now(), value };
    return value;
  }
  const scanLimit = Math.max(500, Math.min(20000, Number(process.env.WAREHOUSE_SUMMARY_LINKED_SCAN_LIMIT || 5000) || 5000));
  const [totalAll, ozonStateCounts, yandexStateCounts, linkedRows, suppliers] = await Promise.all([
    prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
    getOzonStateCountsFromPostgres(prisma),
    getMarketplaceStateCountsFromPostgres(prisma, "yandex"),
    prisma.warehouseProduct.findMany({
      where: { AND: [enabledWarehouseTargetWhere(), { links: { some: {} } }] },
      include: { links: true },
      orderBy: { updatedAt: "desc" },
      take: scanLimit,
    }),
    getWarehousePostgresSuppliers(prisma),
  ]);
  const normalizedSuppliers = suppliers;
  const linkedProducts = linkedRows.map(productFromPostgres);
  const counterStats = await buildWarehouseCounterStatsFromLinkedProducts(
    linkedProducts,
    normalizedSuppliers,
    { totalProducts: totalAll, usdRate: rate },
  );
  const value = {
    totalAll,
    ozonStateCounts,
    yandexStateCounts,
    normalizedSuppliers,
    counterStats,
    linkedArchived: linkedProducts
      .filter((product) => product.marketplace === "ozon" && Array.isArray(product.links) && product.links.length && product.marketplaceState?.code === "archived").length,
    lightweight: false,
    summarySampled: linkedRows.length >= scanLimit,
  };
  warehousePostgresSummaryCache = { key: cacheKey, at: Date.now(), value };
  return value;
}

async function getWarehousePostgresSummaryLight(prisma, rate) {
  const cacheKey = Number(rate || 0).toFixed(4);
  if (
    warehousePostgresSummaryCache
    && warehousePostgresSummaryCache.key === cacheKey
    && Date.now() - warehousePostgresSummaryCache.at < warehousePostgresSummaryCacheTtlMs
  ) {
    return warehousePostgresSummaryCache.value;
  }
  const [totalAll, ozonStateCounts, yandexStateCounts, linkedProducts, normalizedSuppliers] = await Promise.all([
    prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
    getOzonStateCountsFromPostgres(prisma),
    getMarketplaceStateCountsFromPostgres(prisma, "yandex"),
    prisma.warehouseProduct.count({ where: { AND: [enabledWarehouseTargetWhere(), { links: { some: {} } }] } }).catch(() => 0),
    getWarehousePostgresSuppliers(prisma),
  ]);
  const value = {
    totalAll,
    ozonStateCounts,
    yandexStateCounts,
    normalizedSuppliers,
    counterStats: {
      ready: 0,
      changed: 0,
      withoutSupplier: 0,
      linkedProducts,
      linkedNotReady: 0,
    },
    linkedArchived: 0,
    lightweight: true,
  };
  const existing = warehousePostgresSummaryCache;
  if (!existing || existing.key !== cacheKey || existing.value?.lightweight) {
    warehousePostgresSummaryCache = { key: cacheKey, at: Date.now(), value };
  }
  return value;
}

function getCachedWarehousePostgresSummary(rate) {
  const cacheKey = Number(rate || 0).toFixed(4);
  if (
    warehousePostgresSummaryCache
    && warehousePostgresSummaryCache.key === cacheKey
    && Date.now() - warehousePostgresSummaryCache.at < warehousePostgresSummaryCacheTtlMs
  ) {
    return warehousePostgresSummaryCache.value;
  }
  return null;
}

