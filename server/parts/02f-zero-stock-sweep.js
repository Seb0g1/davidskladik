// Fast zero-stock sweep: the mirror image of the restock sweep (02f-stock-sweep.js).
//
// When a product's supplier disappears (stopped / sold out at PriceMaster / link removed),
// it must NOT keep selling marketplace stock we can no longer fulfil — overselling risk.
// The linked-reconciler already zeroes such products, but a full pass over ~14k linked
// products takes ~2h. This sweep gives the zero-stock direction a fast lane: every
// ZERO_STOCK_SWEEP_INTERVAL_SECONDS (180s) it scans a rotating batch of linked products
// that still show positive marketplace stock, rebuilds them (snapshot pricing) to resolve
// the live supplier, and hands them to runNoSupplierMarketplaceAutomation. That automation
// keeps its own safety grace window (linkedNoSupplierZeroGraceMs) so a brief supplier blip
// does not zero a product — we only feed it candidates.

const zeroStockSweepEnabled = process.env.ZERO_STOCK_SWEEP_ENABLED !== "false";
const zeroStockSweepIntervalMs = Math.max(60_000, Number(process.env.ZERO_STOCK_SWEEP_INTERVAL_SECONDS || 180) * 1000 || 180_000);
const zeroStockSweepBatchLimit = Math.max(50, Math.min(2000, Number(process.env.ZERO_STOCK_SWEEP_BATCH_LIMIT || 500) || 500));
let zeroStockSweepTimer = null;
let zeroStockSweepRunning = false;
let zeroStockSweepNextRunAt = null;
// Rotate through the catalog: skip ids we checked within this window so successive ticks
// advance instead of rebuilding the same newest rows forever.
const zeroStockSweepRecentlyChecked = new Map(); // id -> at
const zeroStockSweepCheckedCooldownMs = Math.max(5 * 60_000, Number(process.env.ZERO_STOCK_SWEEP_CHECKED_COOLDOWN_MS || 30 * 60_000) || 30 * 60_000);

async function runZeroStockSweep({ source = "schedule" } = {}) {
  if (zeroStockSweepRunning) return { status: "already_running" };
  if (!autoZeroStockOnNoSupplier) return { status: "disabled" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  zeroStockSweepRunning = true;
  try {
    // Candidates: linked, not archived, still holding positive marketplace stock. We cannot
    // tell from SQL whether the supplier is gone (selectedSupplier is a computed field, never
    // persisted) — the rebuild below resolves that and the automation zeroes only the ones
    // that truly have no supplier.
    // Candidate carries stock if EITHER the live marketplace stock OR the target stock (what
    // the catalog shows as «Остаток» and what we push) is > 0. Keying only on marketplaceState
    // missed no-supplier products whose marketplace stock was already zeroed but whose
    // target_stock was never reset — they kept showing «Остаток N» in the catalog forever.
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id
      FROM warehouse_products p
      WHERE p.archived = false
        AND ${duplicateNameSqlExclusion("p")}
        AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
        AND (
          COALESCE(p.target_stock, 0) > 0
          OR COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) > 0
        )
      ORDER BY p.updated_at DESC
      LIMIT ${zeroStockSweepBatchLimit * 4}
    `);
    if (!rows.length) return { status: "ok", candidates: 0, zeroed: 0 };

    const nowMs = Date.now();
    for (const [id, at] of zeroStockSweepRecentlyChecked.entries()) {
      if (nowMs - at > zeroStockSweepCheckedCooldownMs) zeroStockSweepRecentlyChecked.delete(id);
    }
    const candidateIds = [];
    for (const row of rows) {
      const id = String(row.id);
      if (zeroStockSweepRecentlyChecked.has(id)) continue;
      candidateIds.push(id);
      zeroStockSweepRecentlyChecked.set(id, nowMs);
      if (candidateIds.length >= zeroStockSweepBatchLimit) break;
    }
    if (!candidateIds.length) return { status: "ok", candidates: rows.length, zeroed: 0, cooldown: true };

    const products = await buildFreshWarehouseProducts(candidateIds, { livePriceMaster: false })
      .catch((error) => {
        logger.warn("zero stock sweep build failed", { detail: error?.message || String(error) });
        return [];
      });
    // Only products that lost their supplier but still carry stock are worth handing over.
    // "Carries stock" = live marketplace stock OR a positive target_stock (catalog «Остаток»),
    // so a product whose marketplace stock is already 0 but whose target_stock is still > 0
    // gets its target reset to 0 too.
    const noSupplierWithStock = products.filter((product) =>
      product?.hasLinks
      && !product.selectedSupplier
      && !product.noSupplierAutomation?.manualSellableAt
      && (marketplaceHasPositiveStock(product) || Number(product.targetStock || 0) > 0));
    if (!noSupplierWithStock.length) {
      return { status: "ok", candidates: rows.length, selected: candidateIds.length, zeroed: 0 };
    }

    const result = await runNoSupplierMarketplaceAutomation(
      { products: noSupplierWithStock },
      { productIds: noSupplierWithStock.map((product) => product.id), includeNoLinks: false, source: `zero_stock_sweep_${source}` },
    ).catch((error) => {
      logger.warn("zero stock sweep automation failed", { detail: error?.message || String(error) });
      return { zeroStockSent: 0, archived: 0 };
    });
    logger.info("zero_stock_sweep_complete", {
      source,
      candidates: rows.length,
      selected: candidateIds.length,
      noSupplierWithStock: noSupplierWithStock.length,
      zeroStockSent: result.zeroStockSent || 0,
      archived: result.archived || 0,
    });
    return {
      status: "ok",
      candidates: rows.length,
      selected: candidateIds.length,
      noSupplierWithStock: noSupplierWithStock.length,
      zeroed: result.zeroStockSent || 0,
    };
  } catch (error) {
    logger.warn("zero stock sweep failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    zeroStockSweepRunning = false;
  }
}

function scheduleZeroStockSweep(delayMs = zeroStockSweepIntervalMs) {
  if (!zeroStockSweepEnabled) {
    zeroStockSweepNextRunAt = null;
    return;
  }
  if (zeroStockSweepTimer) clearTimeout(zeroStockSweepTimer);
  const normalizedDelay = Math.max(15_000, Number(delayMs) || zeroStockSweepIntervalMs);
  zeroStockSweepNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  zeroStockSweepTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runZeroStockSweep({ source: "schedule" });
    } catch (error) {
      logger.warn("zero stock sweep tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("zero_stock_sweep", { status: result?.status || "unknown", intervalMs: zeroStockSweepIntervalMs, detail: result || {} }).catch(() => {});
      scheduleZeroStockSweep(zeroStockSweepIntervalMs);
    }
  }, normalizedDelay);
  zeroStockSweepTimer.unref?.();
}
