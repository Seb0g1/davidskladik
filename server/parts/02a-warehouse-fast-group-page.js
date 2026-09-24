async function buildFastWarehouseGroupPageFromPostgres({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  const prisma = getPrisma();
  const hasSearchQuery = Boolean(cleanText(filters.q || ""));
  const basePage = await buildFastWarehousePageFromPostgres({
    page,
    pageSize,
    usdRate,
    filters: { ...filters, groupPage: false },
  });
  if (!basePage) return basePage;
  const rawItems = Array.isArray(basePage.items) ? basePage.items : [];
  const groups = rawItems.length ? buildWarehousePageProductGroups(rawItems).map(mapServerWarehousePageGroup) : [];
  const rowTotal = Number(basePage.total || 0);
  const estimatedGroupTotal = Math.max(groups.length, Math.ceil(rowTotal / 1.6));
  if (!rawItems.length) {
    const { groupTotal, groupCountPending } = hasSearchQuery
      ? { groupTotal: 0, groupCountPending: false }
      : resolveWarehouseGroupTotalEstimate(prisma, filters, { estimatedGroupTotal: 0 });
    return {
      ...basePage,
      grouped: true,
      rowTotal,
      groupTotal: groupTotal > 0 ? groupTotal : 0,
      total: groupTotal > 0 ? groupTotal : 0,
      items: [],
      groupCountPending,
    };
  }
  const { groupTotal, groupCountPending } = hasSearchQuery
    ? { groupTotal: estimatedGroupTotal, groupCountPending: false }
    : resolveWarehouseGroupTotalEstimate(prisma, filters, { estimatedGroupTotal });
  const total = hasSearchQuery ? estimatedGroupTotal : (groupTotal > 0 ? groupTotal : estimatedGroupTotal);
  return {
    ...basePage,
    grouped: true,
    rowTotal,
    groupTotal: total,
    total,
    // Only `items`: a duplicate `groups` array doubled the JSON (~0.9 MB per 100-card page)
    // and every JSON clone of the page cache.
    items: groups,
    hasMore: (page * pageSize) < rowTotal,
    partial: Boolean(basePage.partial),
    groupCountPending,
  };
}

function warehouseFastPageCacheKey({ page = 1, pageSize = 60, usdRate, filters = {} } = {}) {
  return JSON.stringify({
    page: Number(page) || 1,
    pageSize: Number(pageSize) || 60,
    usdRate: Number.isFinite(Number(usdRate)) && Number(usdRate) > 0 ? Number(usdRate) : "default",
    filters: {
      q: cleanText(filters.q || "").toLowerCase(),
      autoOnly: Boolean(filters.autoOnly),
      linked: cleanText(filters.linked || "all"),
      marketplace: cleanText(filters.marketplace || "all"),
      state: cleanText(filters.state || "all"),
      brand: cleanText(filters.brand || "").toLowerCase(),
      sort: cleanText(filters.sort || ""),
      groupPage: Boolean(filters.groupPage),
    },
    storage: shouldUsePostgresStorage() ? "postgres" : "json",
  });
}

// Fresh within warehouseFastPageCacheTtlMs. After that the entry is kept as "stale" for
// warehouseFastPageStaleTtlMs so buildFastWarehousePage can answer instantly and rebuild in
// the background (stale-while-revalidate) — before, the first visitor after every 45 s idle
// gap waited the full ~3 s rebuild. Mutations still clear the whole cache
// (invalidateWarehouseViewCache), so edits never come back as stale pages.
function getWarehouseFastPageCache(params = {}) {
  const key = warehouseFastPageCacheKey(params);
  const cached = warehouseFastPageCache.get(key);
  const age = cached ? Date.now() - cached.at : Infinity;
  if (!cached || age > warehouseFastPageCacheTtlMs + warehouseFastPageStaleTtlMs) {
    warehouseFastPageCache.delete(key);
    return { key, value: null, stale: false };
  }
  return { key, value: cloneAuditValue(cached.value), stale: age > warehouseFastPageCacheTtlMs };
}

function setWarehouseFastPageCache(key, value) {
  if (!key || !value) return;
  warehouseFastPageCache.set(key, { at: Date.now(), value: cloneAuditValue(value) });
  if (warehouseFastPageCache.size > warehouseFastPageCacheMax) {
    const oldestKey = warehouseFastPageCache.keys().next().value;
    if (oldestKey) warehouseFastPageCache.delete(oldestKey);
  }
}

