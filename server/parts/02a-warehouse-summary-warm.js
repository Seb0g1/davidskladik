async function resolveWarehousePostgresSummaryForPage(prisma, rate, { preferLight = false } = {}) {
  const cached = getCachedWarehousePostgresSummary(rate);
  if (cached && !cached.lightweight) return cached;
  if (preferLight) {
    const light = await getWarehousePostgresSummaryLight(prisma, rate);
    if (cached?.counterStats && !cached.lightweight) {
      return {
        ...light,
        counterStats: cached.counterStats,
        linkedArchived: cached.linkedArchived ?? light.linkedArchived,
      };
    }
    return light;
  }
  return getWarehousePostgresSummary(prisma, rate);
}

let warehousePostgresSummaryWarmTimer = null;

function scheduleWarehousePostgresSummaryWarm() {
  if (process.env.WAREHOUSE_SUMMARY_WARM_ENABLED !== "true") return;
  if (!shouldUsePostgresStorage()) return;
  const intervalMs = Math.max(warehousePostgresSummaryCacheTtlMs, 60_000);
  const warm = async () => {
    try {
      const prisma = getPrisma();
      if (!prisma) return;
      const startedAt = Date.now();
      const appSettings = await readAppSettings();
      const rate = Number(appSettings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);
      await getWarehousePostgresSummary(prisma, rate);
      logger.info("warehouse postgres summary warmed", { elapsedMs: Date.now() - startedAt });
    } catch (error) {
      logger.warn("warehouse postgres summary warm failed", { detail: error?.message || String(error) });
    }
  };
  setTimeout(() => { void warm(); }, Math.max(1000, Number(process.env.WAREHOUSE_SUMMARY_WARM_INITIAL_DELAY_MS || 8000)));
  if (warehousePostgresSummaryWarmTimer) clearInterval(warehousePostgresSummaryWarmTimer);
  warehousePostgresSummaryWarmTimer = setInterval(() => { void warm(); }, intervalMs);
}

let warehouseGroupCountWarmTimer = null;

function scheduleWarehouseGroupCountWarm() {
  if (process.env.WAREHOUSE_GROUP_COUNT_WARM_ENABLED === "false") return;
  if (isMonolithServer && process.env.WAREHOUSE_GROUP_COUNT_WARM_ENABLED !== "true") return;
  if (!shouldUsePostgresStorage()) return;
  const intervalMs = Math.max(warehouseGroupCountCacheTtlMs, 60_000);
  const warm = async () => {
    try {
      if (heavyBackgroundWorkShouldDefer("warehouse_group_count_warm")) return;
      const prisma = getPrisma();
      if (!prisma) return;
      const startedAt = Date.now();
      await countWarehouseFilteredGroupTotal(prisma, {
        q: "",
        autoOnly: false,
        linked: "all",
        marketplace: "all",
        state: "all",
        brand: "",
      });
      logger.info("warehouse group count warmed", { elapsedMs: Date.now() - startedAt });
    } catch (error) {
      logger.warn("warehouse group count warm failed", { detail: error?.message || String(error) });
    }
  };
  setTimeout(() => { void warm(); }, Math.max(1000, Number(process.env.WAREHOUSE_GROUP_COUNT_WARM_INITIAL_DELAY_MS || 5000)));
  if (warehouseGroupCountWarmTimer) clearInterval(warehouseGroupCountWarmTimer);
  warehouseGroupCountWarmTimer = setInterval(() => { void warm(); }, intervalMs);
}

