async function buildFastWarehousePageFromPostgres({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  const traceStartedAt = Date.now();
  if (filters.autoOnly) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  pageTrace("postgres:start", traceStartedAt);
  kickWarehousePostgresLinksBackfill(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const linkedFilter = cleanText(filters.linked || "all");
  const needsComputedLinkFilter = linkedFilter === "ready" || linkedFilter === "changed" || linkedFilter === "linked_archived";
  const brandFilter = cleanText(filters.brand || "");
  let brandIndexProductIds = [];
  if (brandFilter) {
    await ensureWarehousePostgresBrandsBackfilled(prisma);
    brandIndexProductIds = await brandIndexProductIdsForFilterPostgres(prisma, brandFilter).catch((error) => {
      logger.warn("warehouse brand index filter failed, using product brand fallback", { detail: error?.message || String(error) });
      return [];
    });
  }
  const strictIdentitySearch = isWarehouseStrictIdentitySearch(filters);
  const needsInMemoryPage = needsComputedLinkFilter || strictIdentitySearch;
  const preferLightPage = !needsComputedLinkFilter;
  const postgresFilters = {
    ...(needsComputedLinkFilter ? { ...filters, state: "all" } : filters),
    ...(brandIndexProductIds.length ? { brand: "" } : {}),
  };
  const where = warehousePagePostgresWhere(postgresFilters);
  if (brandIndexProductIds.length) {
    // Union brand index IDs with direct brand/name column match so products not yet
    // re-indexed (or with brand only in their name) are still found.
    where.AND.push({ OR: [{ id: { in: brandIndexProductIds } }, { brand: { contains: brandFilter, mode: "insensitive" } }, { name: { contains: brandFilter, mode: "insensitive" } }] });
  }
  const strictPrimaryWhere = strictIdentitySearch
    ? warehousePagePostgresPrimaryIdentityWhere(postgresFilters)
    : null;
  const offset = (page - 1) * pageSize;
  pageTrace("postgres:before-query", traceStartedAt);
  const [summary, dbTotal, initialDbRows] = await Promise.all([
    resolveWarehousePostgresSummaryForPage(prisma, rate, { preferLight: preferLightPage }),
    needsInMemoryPage ? Promise.resolve(0) : prisma.warehouseProduct.count({ where }),
    prisma.warehouseProduct.findMany({
      where: strictPrimaryWhere || where,
      include: { links: true },
      orderBy: warehousePagePostgresOrderBy(filters.sort),
      skip: needsInMemoryPage ? 0 : offset,
      take: needsInMemoryPage ? warehouseInMemoryPageMax : pageSize,
    }),
  ]);
  pageTrace("postgres:after-query", traceStartedAt);
  let dbRows = initialDbRows;
  if (strictIdentitySearch && dbRows.length === 0) {
    dbRows = await prisma.warehouseProduct.findMany({
      where,
      include: { links: true },
      orderBy: warehousePagePostgresOrderBy(filters.sort),
      take: warehouseInMemoryPageMax,
    });
    pageTrace("postgres:after-strict-fallback-query", traceStartedAt);
  }
  let pageBaseCount = dbRows.length;
  if (!needsComputedLinkFilter) {
    // Always add cross-marketplace siblings (Ozon↔Yandex pairing) — including for
    // strict identity searches so that searching by article shows both marketplaces.
    dbRows = await addWarehousePostgresPageGroupSiblings(prisma, where, dbRows);
  }
  const normalizedSuppliers = summary.normalizedSuppliers;
  const counterStats = summary.counterStats;
  const siblingSourceProducts = dbRows.map(productFromPostgres);
  let allProducts = sortWarehouseProductsForSearch(siblingSourceProducts, filters);
  if (needsComputedLinkFilter) {
    if (!serverUnderHttpLoad() && !serverUnderMemoryPressure()) {
      allProducts = await buildFreshWarehouseProductsForWarehouse(
        { products: allProducts, suppliers: normalizedSuppliers },
        allProducts.map((product) => product.id),
        { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
      );
    }
  }
  if (needsInMemoryPage) {
    allProducts = sortWarehouseProductsForSearch(
      preferWarehousePrimaryIdentityMatches(
        allProducts.filter((product) => warehousePageProductMatches(product, filters)),
        filters,
      ),
      filters,
    );
  }
  const total = needsInMemoryPage ? allProducts.length : dbTotal;
  let visibleProducts = allProducts;
  if (needsInMemoryPage) {
    const pageSlice = allProducts.slice(offset, offset + pageSize);
    pageBaseCount = pageSlice.length;
    visibleProducts = addWarehousePageGroupSiblings(siblingSourceProducts, pageSlice);
  }
  // When WAREHOUSE_PAGE_AUTO_ENRICH_BLOCKING=false: fire-and-forget the Ozon refresh
  // so we return the snapshot immediately and save the 300-2000ms Ozon API latency.
  // The DB is updated in the background; next page load sees the refreshed data.
  const enrichBlocking = process.env.WAREHOUSE_PAGE_AUTO_ENRICH_BLOCKING !== "false";
  const enrichedForPage = enrichBlocking
    ? await enrichWeakOzonProductsForPage(visibleProducts)
    : (enrichWeakOzonProductsForPage(visibleProducts).catch((err) => {
      logger.warn("non-blocking page Ozon enrich failed", { detail: err?.message || String(err) });
    }), visibleProducts);
  const pageProducts = Array.from(new Map(
    enrichedForPage.map((product) => [product.id, product]),
  ).values());
  const pageWarehouse = {
    createdAt: dbRows[0]?.createdAt?.toISOString() || null,
    updatedAt: dbRows[0]?.updatedAt?.toISOString() || null,
    products: pageProducts,
    suppliers: normalizedSuppliers,
  };
  const items = warehousePageShouldUseLightEnrich(filters)
    ? enrichWarehousePageProductsLight(pageWarehouse)
    : await enrichWarehousePageProductsForDisplay(pageWarehouse, {
      usdRate: rate,
      traceStartedAt,
      traceLabel: warehousePageShouldUseLightBuild() ? "postgres:after-light-enrich" : "postgres:after-build",
    });
  return {
    createdAt: pageWarehouse.createdAt,
    updatedAt: pageWarehouse.updatedAt,
    totalAll: summary.totalAll,
    ready: counterStats.ready,
    changed: counterStats.changed,
    withoutSupplier: counterStats.withoutSupplier,
    linkedProducts: counterStats.linkedProducts,
    linkedNotReady: counterStats.linkedNotReady,
    linkedArchived: summary.linkedArchived,
    ozonArchived: summary.ozonStateCounts.archived,
    ozonInactive: summary.ozonStateCounts.inactive,
    ozonOutOfStock: summary.ozonStateCounts.outOfStock,
    yandexArchived: summary.yandexStateCounts.archived,
    yandexInactive: summary.yandexStateCounts.inactive,
    yandexOutOfStock: summary.yandexStateCounts.outOfStock,
    usdRate: rate,
    priceMaster: await getPriceMasterSnapshotMetaFast(),
    sourceError: "",
    partial: items.some((item) => item.partial)
      || (needsInMemoryPage && siblingSourceProducts.length >= warehouseInMemoryPageMax)
      || (needsComputedLinkFilter && (serverUnderHttpLoad() || serverUnderMemoryPressure())),
    inMemoryScanCapped: needsInMemoryPage && siblingSourceProducts.length >= warehouseInMemoryPageMax,
    noSupplierAlerts: [],
    page,
    pageSize,
    total,
    hasMore: offset + pageBaseCount < total,
    items,
  };
}

