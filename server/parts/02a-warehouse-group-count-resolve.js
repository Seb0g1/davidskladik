function resolveWarehouseGroupTotalEstimate(prisma, filters = {}, { estimatedGroupTotal = 0 } = {}) {
  if (cleanText(filters.q || "")) {
    return { groupTotal: estimatedGroupTotal, groupCountPending: false };
  }
  const cached = getCachedWarehouseGroupTotal(filters);
  if (cached > 0) {
    return { groupTotal: cached, groupCountPending: false };
  }
  if (prisma && !warehouseGroupCountInflight.has(warehouseGroupCountCacheKey(filters))) {
    kickWarehouseGroupCountRefresh(prisma, filters);
  }
  return {
    groupTotal: estimatedGroupTotal > 0 ? estimatedGroupTotal : 0,
    groupCountPending: Boolean(prisma),
  };
}

async function acquireWarehousePageBuildSlot() {
  if (warehousePageBuildActive < warehousePageBuildMaxConcurrent) {
    warehousePageBuildActive += 1;
    return true;
  }
  return new Promise((resolve) => {
    const entry = { resolve: null };
    const timer = setTimeout(() => {
      const index = warehousePageBuildWaiters.indexOf(entry);
      if (index >= 0) warehousePageBuildWaiters.splice(index, 1);
      entry.resolve(false);
    }, warehousePageBuildAcquireTimeoutMs);
    entry.resolve = (granted) => {
      clearTimeout(timer);
      resolve(granted);
    };
    warehousePageBuildWaiters.push(entry);
  });
}

function releaseWarehousePageBuildSlot() {
  warehousePageBuildActive = Math.max(0, warehousePageBuildActive - 1);
  while (warehousePageBuildWaiters.length && warehousePageBuildActive < warehousePageBuildMaxConcurrent) {
    const next = warehousePageBuildWaiters.shift();
    warehousePageBuildActive += 1;
    next.resolve(true);
  }
}

function getCachedWarehouseGroupTotal(filters = {}) {
  const cacheKey = warehouseGroupCountCacheKey(filters);
  const cached = warehouseGroupCountCache.get(cacheKey);
  if (cached && Date.now() - cached.at < warehouseGroupCountCacheTtlMs) return cached.value;
  return cached?.value > 0 ? cached.value : 0;
}

function kickWarehouseGroupCountRefresh(prisma, filters = {}) {
  if (!prisma || cleanText(filters.q || "")) return;
  if (serverUnderMemoryPressure()) return;
  const cacheKey = warehouseGroupCountCacheKey(filters);
  if (warehouseGroupCountInflight.has(cacheKey)) return;
  const refreshPromise = countWarehouseFilteredGroupTotal(prisma, filters)
    .catch((error) => {
      logger.warn("warehouse grouped total background refresh failed", { detail: error?.message || String(error) });
      return 0;
    })
    .finally(() => {
      warehouseGroupCountInflight.delete(cacheKey);
    });
  warehouseGroupCountInflight.set(cacheKey, refreshPromise);
}

async function resolveWarehouseGroupTotalForPage(prisma, filters = {}, { estimatedGroupTotal = 0 } = {}) {
  if (!prisma || cleanText(filters.q || "")) return estimatedGroupTotal;
  const cacheKey = warehouseGroupCountCacheKey(filters);
  const cached = warehouseGroupCountCache.get(cacheKey);
  if (cached && Date.now() - cached.at < warehouseGroupCountCacheTtlMs) return cached.value;
  if (serverUnderHttpLoad() || serverUnderMemoryPressure()) {
    const fallback = estimatedGroupTotal > 0
      ? estimatedGroupTotal
      : getCachedWarehouseGroupTotal(filters);
    if (fallback > 0) return fallback;
    if (!warehouseGroupCountInflight.has(cacheKey)) kickWarehouseGroupCountRefresh(prisma, filters);
    return estimatedGroupTotal > 0 ? estimatedGroupTotal : 0;
  }
  if (cached?.value > 0) {
    kickWarehouseGroupCountRefresh(prisma, filters);
    return cached.value;
  }
  let inflight = warehouseGroupCountInflight.get(cacheKey);
  if (!inflight) {
    inflight = countWarehouseFilteredGroupTotal(prisma, filters)
      .catch((error) => {
        logger.warn("warehouse grouped total count failed, using estimate", { detail: error?.message || String(error) });
        return 0;
      })
      .finally(() => {
        warehouseGroupCountInflight.delete(cacheKey);
      });
    warehouseGroupCountInflight.set(cacheKey, inflight);
  }
  try {
    const value = await Promise.race([
      inflight,
      promiseTimeout(warehouseGroupCountWaitMs, "warehouse_group_count_wait_timeout"),
    ]);
    return value > 0 ? value : (estimatedGroupTotal > 0 ? estimatedGroupTotal : getCachedWarehouseGroupTotal(filters));
  } catch (error) {
    if (error?.message === "warehouse_group_count_wait_timeout") {
      kickWarehouseGroupCountRefresh(prisma, filters);
      return estimatedGroupTotal > 0 ? estimatedGroupTotal : getCachedWarehouseGroupTotal(filters);
    }
    throw error;
  }
}

