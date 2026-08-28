async function buildFastWarehousePage({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  const cacheParams = { page, pageSize, usdRate, filters };
  const cached = getWarehouseFastPageCache(cacheParams);
  if (cached.value) return cached.value;
  const inflight = warehouseFastPageInflight.get(cached.key);
  if (inflight) {
    try {
      return await Promise.race([
        inflight,
        promiseTimeout(warehouseFastPageBuildTimeoutMs, "warehouse_fast_page_build_timeout"),
      ]);
    } catch (error) {
      if (error?.message === "warehouse_fast_page_build_timeout" && cached.value) {
        return { ...cached.value, partial: true, stale: true, sourceError: "warehouse_fast_page_build_timeout" };
      }
      throw error;
    }
  }
  let result = null;
  const buildCore = async () => {
    if (shouldUsePostgresStorage()) {
      const postgresPage = filters.groupPage
        ? await buildFastWarehouseGroupPageFromPostgres({ page, pageSize, usdRate, filters })
        : await buildFastWarehousePageFromPostgres({ page, pageSize, usdRate, filters });
      if (postgresPage) return postgresPage;
    }
    const warehouse = await readWarehouse();
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const sourceProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
  const enabledProducts = sourceProducts.filter(isWarehouseProductTargetEnabled);
  const siblingSourceProducts = enabledProducts.map(normalizeWarehouseProduct);
  const filtered = sortWarehouseProductsForSearch(
    enabledProducts.filter((product) => warehousePageProductMatches(product, filters)),
    filters,
  );
  const total = filtered.length;
  const offset = (page - 1) * pageSize;
  const pageSlice = filtered.slice(offset, offset + pageSize);
  const strictIdentitySearch = isWarehouseStrictIdentitySearch(filters);
  const pageProducts = await enrichWeakOzonProductsForPage(
    strictIdentitySearch ? pageSlice : addWarehousePageGroupSiblings(siblingSourceProducts, pageSlice),
  );
  const built = await buildFreshWarehouseProductsForWarehouse(
    { ...warehouse, products: pageProducts },
    pageProducts.map((product) => product.id),
    { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
  );
  const builtMap = new Map(built.map((product) => [product.id, product]));
  const items = pageProducts.map((product) => {
    const item = builtMap.get(product.id) || normalizeWarehouseProduct(product);
    return {
      ...item,
      autoPriceEnabled: item.autoPriceEnabled !== false,
      links: Array.isArray(item.links) ? item.links : [],
      suppliers: Array.isArray(item.suppliers) ? item.suppliers : [],
      selectedSupplier: item.selectedSupplier || null,
      noSupplierAutomation: item.noSupplierAutomation || {},
      marketplaceState: item.marketplaceState || {},
      partial: false,
    };
  });
  const counterStats = await buildWarehouseCounterStatsFromLinkedProducts(
    enabledProducts,
    warehouse.suppliers,
    { totalProducts: enabledProducts.length, usdRate: rate },
  );
  const ozonStateCounts = warehouseStateCounter(enabledProducts, "ozon");
  const yandexStateCounts = warehouseStateCounter(enabledProducts, "yandex");
    return {
      createdAt: warehouse.createdAt || null,
      updatedAt: warehouse.updatedAt || null,
      totalAll: enabledProducts.length,
      ready: counterStats.ready,
      changed: counterStats.changed,
      withoutSupplier: counterStats.withoutSupplier,
      linkedProducts: counterStats.linkedProducts,
      linkedNotReady: counterStats.linkedNotReady,
      linkedArchived: enabledProducts.filter((product) => Array.isArray(product.links) && product.links.length && productLooksArchived(product)).length,
      ozonArchived: ozonStateCounts.archived,
      ozonInactive: ozonStateCounts.inactive,
      ozonOutOfStock: ozonStateCounts.outOfStock,
      yandexArchived: yandexStateCounts.archived,
      yandexInactive: yandexStateCounts.inactive,
      yandexOutOfStock: yandexStateCounts.outOfStock,
      usdRate: rate,
      priceMaster: await getPriceMasterSnapshotMetaFast(),
      sourceError: "",
      noSupplierAlerts: [],
      page,
      pageSize,
      total,
      hasMore: offset + pageSlice.length < total,
      items,
    };
  };

  const buildPromise = (async () => {
    const slotGranted = await acquireWarehousePageBuildSlot();
    if (!slotGranted) {
      const staleCached = getWarehouseFastPageCache(cacheParams);
      if (staleCached.value) {
        return { ...staleCached.value, partial: true, stale: true, sourceError: "warehouse_page_build_slot_timeout" };
      }
      return {
        partial: true,
        stale: false,
        sourceError: "warehouse_page_build_slot_timeout",
        page,
        pageSize,
        total: 0,
        hasMore: false,
        items: [],
      };
    }
    try {
      result = await Promise.race([
        buildCore(),
        promiseTimeout(warehouseFastPageBuildTimeoutMs, "warehouse_fast_page_build_timeout"),
      ]);
      // Never cache partial/empty fallbacks: a cached empty page makes the catalog look
      // dead (instant 0-item responses) long after the load spike has passed.
      if (result && !result.partial && !result.sourceError) setWarehouseFastPageCache(cached.key, result);
      return result;
    } finally {
      releaseWarehousePageBuildSlot();
      warehouseFastPageInflight.delete(cached.key);
    }
  })();
  warehouseFastPageInflight.set(cached.key, buildPromise);
  try {
    return await buildPromise;
  } catch (error) {
    if (error?.message === "warehouse_fast_page_build_timeout") {
      logger.warn("warehouse fast page build timed out", {
        timeoutMs: warehouseFastPageBuildTimeoutMs,
        page,
        pageSize,
        q: cleanText(filters.q || "").slice(0, 32),
        grouped: Boolean(filters.groupPage),
      });
      const staleCached = getWarehouseFastPageCache(cacheParams);
      if (staleCached.value) {
        return { ...staleCached.value, partial: true, stale: true, sourceError: "warehouse_fast_page_build_timeout" };
      }
      return {
        partial: true,
        stale: false,
        sourceError: "warehouse_fast_page_build_timeout",
        page,
        pageSize,
        total: 0,
        hasMore: false,
        items: [],
      };
    }
    throw error;
  }
}
