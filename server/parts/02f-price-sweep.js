// Fast changed-price sweep.
//
// Every PRICE_SWEEP_INTERVAL_SECONDS (120s) it selects linked, non-archived products whose
// computed target price differs from the marketplace price (plain indexed DB columns) and
// queues a normal onlyChanged price push for them. This removes the "Цена ждет ~1 ч" lag:
// a price change reaches Yandex/Ozon within a couple of minutes instead of waiting for the
// rolling reconciler cursor. Yandex limit is 10k offers/min — one sweep batch is nothing.

const priceSweepEnabled = process.env.PRICE_SWEEP_ENABLED !== "false";
const priceSweepIntervalMs = Math.max(30_000, Number(process.env.PRICE_SWEEP_INTERVAL_SECONDS || 120) * 1000 || 120_000);
const priceSweepBatchLimit = Math.max(50, Math.min(2000, Number(process.env.PRICE_SWEEP_BATCH_LIMIT || 500) || 500));
let priceSweepTimer = null;
let priceSweepRunning = false;
let priceSweepNextRunAt = null;
// Don't requeue the same product+price endlessly (e.g. Yandex price quarantine keeps the
// cabinet price different): remember what we already swept recently.
const priceSweepRecentlyQueued = new Map(); // id -> { price, at }
const priceSweepRequeueCooldownMs = Math.max(5 * 60_000, Number(process.env.PRICE_SWEEP_REQUEUE_COOLDOWN_MS || 45 * 60_000) || 45 * 60_000);
// Skip SKUs reconciled as "unchanged_verified" within this window even if current_price/
// target_price cosmetically diverge again before the next reconciler pass — without this,
// a SKU whose live recompute oscillates between two nextPrice values gets re-queued by
// every sweep tick forever (see priceVerifiedAt write in 02d-prices-send-warehouse-select.js).
const priceSweepVerifiedCooldownHours = Math.max(0, Number(process.env.PRICE_SWEEP_VERIFIED_COOLDOWN_HOURS || 6) || 6);

async function runChangedPriceSweep({ source = "schedule" } = {}) {
  if (priceSweepRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  priceSweepRunning = true;
  try {
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id, p.target_price AS "targetPrice"
      FROM warehouse_products p
      WHERE p.archived = false
        AND p.target_price IS NOT NULL AND p.target_price > 0
        AND (p.current_price IS NULL OR p.current_price <> p.target_price)
        AND (
          p.raw->>'priceVerifiedAt' IS NULL
          OR (p.raw->>'priceVerifiedAt')::timestamptz < now() - interval '${priceSweepVerifiedCooldownHours} hours'
        )
        AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
      ORDER BY p.updated_at DESC
      LIMIT ${priceSweepBatchLimit * 3}
    `);
    const nowMs = Date.now();
    for (const [id, entry] of priceSweepRecentlyQueued.entries()) {
      if (nowMs - entry.at > priceSweepRequeueCooldownMs) priceSweepRecentlyQueued.delete(id);
    }
    const productIds = [];
    for (const row of rows) {
      const id = String(row.id);
      const price = Number(row.targetPrice || 0);
      const recent = priceSweepRecentlyQueued.get(id);
      if (recent && recent.price === price) continue;
      productIds.push(id);
      priceSweepRecentlyQueued.set(id, { price, at: nowMs });
      if (productIds.length >= priceSweepBatchLimit) break;
    }
    if (!productIds.length) return { status: "ok", queued: 0, candidates: rows.length };

    const refresh = await queueAuthoritativePriceReprice({
      productIds,
      marketplace: "all",
      reason: "price_sweep",
      sourceEvent: "price_sweep",
      force: false,
      onlyChanged: true,
      refreshMarketplacePrices: false,
      livePriceMaster: true,
      verify: true,
      priority: 2,
    }).catch((error) => {
      logger.warn("price sweep queue failed", { detail: error?.message || String(error) });
      return { queued: 0 };
    });
    logger.info("price_sweep_complete", {
      source,
      candidates: rows.length,
      selected: productIds.length,
      queued: refresh.queued || 0,
    });
    return { status: "ok", candidates: rows.length, selected: productIds.length, queued: refresh.queued || 0 };
  } catch (error) {
    logger.warn("price sweep failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    priceSweepRunning = false;
  }
}

function schedulePriceSweep(delayMs = priceSweepIntervalMs) {
  if (!priceSweepEnabled) {
    priceSweepNextRunAt = null;
    return;
  }
  if (priceSweepTimer) clearTimeout(priceSweepTimer);
  const normalizedDelay = Math.max(10_000, Number(delayMs) || priceSweepIntervalMs);
  priceSweepNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  priceSweepTimer = setTimeout(async () => {
    try {
      await runChangedPriceSweep({ source: "schedule" });
    } catch (error) {
      logger.warn("price sweep tick failed", { detail: error?.message || String(error) });
    } finally {
      schedulePriceSweep(priceSweepIntervalMs);
    }
  }, normalizedDelay);
  priceSweepTimer.unref?.();
}
