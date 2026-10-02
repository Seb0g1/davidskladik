// Сторож «нет поставщика — нет продажи» (worker, каждые LIVE_STOCK_GUARD_SWEEP_MINUTES, деф. 10).
//
// Берёт все карточки, у которых на маркетплейсе СЕЙЧАС есть остаток (marketplace_state.stock > 0), и
// проверяет их тем же guardPositiveStockItems, что стоит перед каждой отправкой: активная строка в последнем
// прайсе живого PriceMaster, иначе — полный расчёт склада. Без поставщика: привязанные — через обычную
// автоматику «нет поставщика» (она же ставит отметки), без привязок (и не «продаётся вручную») — сразу 0.
// Предохранитель: больше LIVE_STOCK_GUARD_MAX_ZERO (деф. 300) за проход — ничего не делаем, тревога в Telegram.

const liveStockGuardSweepEnabled = process.env.LIVE_STOCK_GUARD_SWEEP !== "false";
const liveStockGuardSweepMs = Math.max(2, Number(process.env.LIVE_STOCK_GUARD_SWEEP_MINUTES || 10) || 10) * 60_000;
const liveStockGuardMaxZero = Math.max(10, Number(process.env.LIVE_STOCK_GUARD_MAX_ZERO || 300) || 300);
let liveStockGuardSweepRunning = false;

async function runLiveStockGuardSweep({ dryRun = false } = {}) {
  if (liveStockGuardSweepRunning) return { status: "already_running" };
  if (!liveSupplierGuardEnabled) return { status: "disabled" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  liveStockGuardSweepRunning = true;
  const startedAt = Date.now();
  try {
    try {
      await getLiveSupplierIndex();
    } catch (error) {
      logger.warn("live_stock_guard_pm_unavailable", { detail: error?.message || String(error) });
      return { status: "pm_unavailable" };
    }
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id, p.marketplace::text AS marketplace, p.target, p.offer_id AS "offerId", p.raw -> 'noSupplierAutomation' AS nsa
      FROM warehouse_products p
      WHERE p.archived = false
        AND COALESCE(NULLIF(p.marketplace_state ->> 'stock', '')::numeric, 0) > 0`);
    if (!rows.length) return { status: "ok", stocked: 0 };
    const links = await prisma.productLink.findMany({
      where: { productId: { in: rows.map((r) => r.id) } },
      select: { productId: true, supplierArticle: true, supplierName: true, partnerId: true, sourceRowId: true, exactName: true },
    });
    const linksBy = new Map();
    for (const l of links) {
      if (!linksBy.has(l.productId)) linksBy.set(l.productId, []);
      linksBy.get(l.productId).push({ ...l, article: l.supplierArticle });
    }
    const products = rows.map((r) => ({ id: r.id, marketplace: r.marketplace, target: r.target, offerId: r.offerId, links: linksBy.get(r.id) || [], noSupplierAutomation: r.nsa || {} }));
    const guard = await guardPositiveStockItems(products, { getStock: () => 1, context: "stock_guard_sweep" });
    const blocked = [...guard.blocked];
    const result = { status: "ok", stocked: products.length, blocked: blocked.length, skipped: guard.skipped.size, ms: Date.now() - startedAt };
    if (!blocked.length) return result;
    const sample = blocked.slice(0, 15).map((p) => `${p.marketplace}:${p.target}:${p.offerId}`);
    if (blocked.length > liveStockGuardMaxZero) {
      logger.warn("live_stock_guard_too_many", { ...result, sample });
      if (typeof sendHealthAlertTelegram === "function") {
        sendHealthAlertTelegram(`⚠️ DavidSklad: сторож остатков нашёл ${blocked.length} карточек с остатком без активного поставщика — больше порога ${liveStockGuardMaxZero}, ничего не обнулено. Проверьте PriceMaster.\nНапр.: ${sample.slice(0, 5).join(", ")}`).catch(() => {});
      }
      return { ...result, status: "too_many" };
    }
    if (dryRun) return { ...result, status: "dry_run", sample };
    const linked = blocked.filter((p) => p.links.length);
    const unlinked = blocked.filter((p) => !p.links.length);
    let zeroed = 0;
    if (linked.length) {
      const fresh = await buildFreshWarehouseProducts(linked.map((p) => p.id), { livePriceMaster: true, batchPriceMaster: true }).catch(() => []);
      const auto = await runNoSupplierMarketplaceAutomation({ products: fresh }, { productIds: fresh.map((p) => p.id), includeNoLinks: false, source: "live_stock_guard" })
        .catch((error) => ({ zeroStockSent: 0, error: error?.message }));
      zeroed += Number(auto?.zeroStockSent || 0);
    }
    if (unlinked.length) {
      const actions = await sendZeroStocksToMarketplace(unlinked).catch(() => []);
      zeroed += actions.filter((a) => a.ok).length;
    }
    logger.warn("live_stock_guard_zeroed", { ...result, zeroed, linked: linked.length, unlinked: unlinked.length, sample });
    return { ...result, zeroed };
  } finally {
    liveStockGuardSweepRunning = false;
  }
}

function scheduleLiveStockGuardSweep(delayMs = liveStockGuardSweepMs) {
  if (!liveStockGuardSweepEnabled) return;
  setTimeout(async () => {
    try {
      await runLiveStockGuardSweep();
    } catch (error) {
      logger.warn("live stock guard sweep failed", { detail: error?.message || String(error) });
    } finally {
      scheduleLiveStockGuardSweep(liveStockGuardSweepMs);
    }
  }, Math.max(30_000, Number(delayMs) || liveStockGuardSweepMs)).unref?.();
}

// Manual run / dry run: POST /api/stock-guard/sweep { dryRun: true }
app.post("/api/stock-guard/sweep", requireAdmin, async (request, response, next) => {
  try {
    response.json({ ok: true, ...(await runLiveStockGuardSweep({ dryRun: request.body?.dryRun !== false })) });
  } catch (error) {
    next(error);
  }
});
