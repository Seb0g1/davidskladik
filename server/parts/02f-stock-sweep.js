// Fast stock sweep: keep marketplace stock at the target level for linked products.
//
// A sale depletes the marketplace stock the same day; if the product still has a live
// supplier, the stock must come back to the target (default 5) within minutes. Every
// STOCK_SWEEP_INTERVAL_SECONDS (180s) this sweep:
//   1) gives linked products with a supplier but targetStock<=0 the default target (5);
//   2) finds linked products whose live marketplace stock is below target and re-sends it.
// Yandex stock limit is 100k offers/min, Ozon 100 items/request — a sweep batch is tiny.

const stockSweepEnabled = process.env.STOCK_SWEEP_ENABLED !== "false";
const stockSweepIntervalMs = Math.max(60_000, Number(process.env.STOCK_SWEEP_INTERVAL_SECONDS || 180) * 1000 || 180_000);
const stockSweepBatchLimit = Math.max(50, Math.min(2000, Number(process.env.STOCK_SWEEP_BATCH_LIMIT || 500) || 500));
const linkedDefaultTargetStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
let stockSweepTimer = null;
let stockSweepRunning = false;
let stockSweepNextRunAt = null;
// Marketplace stock readbacks lag a few minutes — don't resend the same value endlessly.
const stockSweepRecentlySent = new Map(); // id -> { stock, at }
const stockSweepResendCooldownMs = Math.max(5 * 60_000, Number(process.env.STOCK_SWEEP_RESEND_COOLDOWN_MS || 20 * 60_000) || 20 * 60_000);

async function runStockSweep({ source = "schedule" } = {}) {
  if (stockSweepRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  stockSweepRunning = true;
  try {
    // Candidates: linked, not archived, and either no target yet or live stock below target.
    // NOTE: `selectedSupplier` is a computed price-build field — it is NEVER persisted to
    // raw, so we must NOT filter on `raw->'selectedSupplier'` here (that predicate matched
    // zero rows and silently disabled this sweep entirely). The supplier is resolved below
    // by a fresh build; products whose supplier vanished get filtered out there (and are
    // zeroed by the no-supplier automation instead).
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id
      FROM warehouse_products p
      WHERE p.archived = false
        AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
        AND (
          COALESCE(p.target_stock, 0) <= 0
          OR COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) < p.target_stock
        )
      ORDER BY p.updated_at DESC
      LIMIT ${stockSweepBatchLimit * 3}
    `);
    if (!rows.length) return { status: "ok", candidates: 0, sent: 0 };

    const nowMs = Date.now();
    for (const [id, entry] of stockSweepRecentlySent.entries()) {
      if (nowMs - entry.at > stockSweepResendCooldownMs) stockSweepRecentlySent.delete(id);
    }
    const candidateIds = [];
    for (const row of rows) {
      const id = String(row.id);
      if (stockSweepRecentlySent.has(id)) continue;
      candidateIds.push(id);
      if (candidateIds.length >= stockSweepBatchLimit) break;
    }
    if (!candidateIds.length) return { status: "ok", candidates: rows.length, sent: 0, cooldown: true };

    // Resolve the supplier with a fresh build (snapshot pricing, not live) — productFromPostgres
    // alone does NOT populate selectedSupplier. Products whose supplier disappeared come back
    // without selectedSupplier and are dropped here (no-supplier automation zeroes those).
    const builtProducts = await buildFreshWarehouseProducts(candidateIds, { livePriceMaster: false })
      .catch((error) => {
        logger.warn("stock sweep build failed", { detail: error?.message || String(error) });
        return [];
      });
    const now = new Date().toISOString();
    const products = builtProducts
      .filter((product) => product?.selectedSupplier && product.hasLinks)
      .map((product) => ({
        ...product,
        targetStock: Math.max(linkedDefaultTargetStock, Math.round(Number(product.targetStock || 0)) || 0),
        updatedAt: now,
      }));
    if (!products.length) return { status: "ok", candidates: rows.length, sent: 0 };

    // Persist bumped targets first so future sweeps/reconciler agree on the value.
    await writeWarehouseProductPatch(products, { reason: "stock_sweep_target", writeLinks: false })
      .catch((error) => logger.warn("stock sweep persist failed", { detail: error?.message || String(error) }));

    const actions = await restoreStocksOnMarketplaces(products)
      .catch((error) => {
        logger.warn("stock sweep send failed", { detail: error?.message || String(error) });
        return [];
      });
    const sentOk = actions.filter((item) => item.ok).length;
    for (const product of products) {
      stockSweepRecentlySent.set(String(product.id), { stock: product.targetStock, at: nowMs });
    }
    logger.info("stock_sweep_complete", {
      source,
      candidates: rows.length,
      selected: products.length,
      sent: sentOk,
      failed: actions.length - sentOk,
      defaultTarget: linkedDefaultTargetStock,
    });
    return { status: "ok", candidates: rows.length, selected: products.length, sent: sentOk };
  } catch (error) {
    logger.warn("stock sweep failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    stockSweepRunning = false;
  }
}

function scheduleStockSweep(delayMs = stockSweepIntervalMs) {
  if (!stockSweepEnabled) {
    stockSweepNextRunAt = null;
    return;
  }
  if (stockSweepTimer) clearTimeout(stockSweepTimer);
  const normalizedDelay = Math.max(15_000, Number(delayMs) || stockSweepIntervalMs);
  stockSweepNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  stockSweepTimer = setTimeout(async () => {
    try {
      await runStockSweep({ source: "schedule" });
    } catch (error) {
      logger.warn("stock sweep tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleStockSweep(stockSweepIntervalMs);
    }
  }, normalizedDelay);
  stockSweepTimer.unref?.();
}
