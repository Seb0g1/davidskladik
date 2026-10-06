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
const zeroStockSweepLivePm = process.env.ZERO_STOCK_SWEEP_LIVE_PM !== "false";
const zeroStockSweepIntervalMs = Math.max(60_000, Number(process.env.ZERO_STOCK_SWEEP_INTERVAL_SECONDS || 180) * 1000 || 180_000);
const zeroStockSweepBatchLimit = Math.max(50, Math.min(2000, Number(process.env.ZERO_STOCK_SWEEP_BATCH_LIMIT || 500) || 500));
let zeroStockSweepTimer = null;
let zeroStockSweepRunning = false;
let zeroStockSweepNextRunAt = null;
// Rotation: the cards checked longest ago (or never) go first. The time of the last check is kept in
// zero_stock_sweep_checks, so a restart does not start over. Before, the batch was the most recently
// updated cards minus an in-memory 3 h cooldown: busy cards took every slot, the cooldown was lost on
// each deploy, and a quiet card could wait for weeks (06.10: ASK47 on Маркет, its supplier gone, never
// reached — 24k never-zeroed cards were in the pool).
let zeroStockSweepTableReady = null;

function ensureZeroStockSweepTable(prisma) {
  if (!zeroStockSweepTableReady) {
    zeroStockSweepTableReady = prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS zero_stock_sweep_checks (
        product_id TEXT PRIMARY KEY,
        checked_at TIMESTAMPTZ NOT NULL
      )
    `).catch((error) => {
      zeroStockSweepTableReady = null;
      throw error;
    });
  }
  return zeroStockSweepTableReady;
}

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
    // The batch: the cards checked longest ago first, never-checked ones before all.
    await ensureZeroStockSweepTable(prisma);
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id, c.checked_at
      FROM warehouse_products p
      LEFT JOIN zero_stock_sweep_checks c ON c.product_id = p.id
      WHERE p.archived = false
        AND ${duplicateNameSqlExclusion("p")}
        AND (
          EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
          OR (p.marketplace = 'yandex' AND jsonb_array_length(COALESCE(p.raw->'links', '[]'::jsonb)) > 0)
        )
        AND (
          COALESCE(p.target_stock, 0) > 0
          OR COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) > 0
          OR (p.raw -> 'noSupplierAutomation' ->> 'stockZeroAt') IS NULL
        )
      ORDER BY c.checked_at ASC NULLS FIRST, p.updated_at DESC
      LIMIT $1
    `, zeroStockSweepBatchLimit);
    if (!rows.length) return { status: "ok", candidates: 0, zeroed: 0 };

    const nowMs = Date.now();
    const candidateIds = rows.map((row) => String(row.id));
    // Stamped before the rebuild: a card whose rebuild keeps failing must not hold the head of the line.
    await prisma.$executeRawUnsafe(`
      INSERT INTO zero_stock_sweep_checks (product_id, checked_at)
      SELECT id, NOW() FROM UNNEST($1::text[]) AS id
      ON CONFLICT (product_id) DO UPDATE SET checked_at = EXCLUDED.checked_at
    `, candidateIds);

    const products = await buildFreshWarehouseProducts(candidateIds, { livePriceMaster: zeroStockSweepLivePm, batchPriceMaster: zeroStockSweepLivePm })
      .catch((error) => {
        logger.warn("zero stock sweep build failed", { detail: error?.message || String(error) });
        return [];
      });
    // Only products that lost their supplier but still carry stock are worth handing over.
    // "Carries stock" = live marketplace stock OR a positive target_stock (catalog «Остаток»),
    // so a product whose marketplace stock is already 0 but whose target_stock is still > 0
    // gets its target reset to 0 too.
    // Also includes products never zeroed before (stockZeroAt IS NULL): they may have stock
    // on the marketplace that our DB never tracked (e.g. link supplier name mismatch).
    // manualSellableAt expires after 48 h — long enough for unarchive/recovery to settle,
    // but short enough that a product whose supplier truly vanished again gets zeroed.
    const manualSellableTtlMs = 48 * 60 * 60 * 1000;
    const noSupplierWithStock = products.filter((product) => {
      if (!product?.hasLinks || product.selectedSupplier) return false;
      const neverZeroed = !product.noSupplierAutomation?.stockZeroAt;
      if (!neverZeroed && !marketplaceHasPositiveStock(product) && Number(product.targetStock || 0) <= 0) return false;
      const manualAt = product.noSupplierAutomation?.manualSellableAt;
      if (manualAt && nowMs - new Date(manualAt).getTime() < manualSellableTtlMs) {
        if (!marketplaceHasPositiveStock(product) && Number(product.targetStock || 0) <= 0) return false;
      }
      return true;
    });
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
      // when the head of this batch was checked before (null = never): how far behind the rotation is
      headLastCheckedAt: rows[0]?.checked_at || null,
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

// No-link archive retry sweep: products whose links were deleted and stockZeroAt was set
// by the no-supplier-automation BullMQ job, but archival never completed (job failed /
// worker restart). Re-runs the archive step for stuck candidates on every sweep tick so
// archival eventually succeeds even when BullMQ jobs transiently fail.
async function runNoLinkArchiveSweep({ source = "schedule" } = {}) {
  if (process.env.AUTO_ARCHIVE_ON_NO_LINKS === "false") return { status: "disabled" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  try {
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id
      FROM warehouse_products p
      WHERE p.archived = false
        AND p.ever_had_links = true
        AND NOT EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
        AND (p.marketplace <> 'yandex' OR jsonb_array_length(COALESCE(p.raw->'links', '[]'::jsonb)) = 0)
        AND p.raw -> 'noSupplierAutomation' ->> 'stockZeroAt' IS NOT NULL
        AND (p.raw -> 'noSupplierAutomation' ->> 'archivedAt') IS NULL
      LIMIT 50
    `).catch((error) => {
      logger.warn("no-link archive sweep query failed", { detail: error?.message || String(error) });
      return [];
    });
    if (!rows.length) return { status: "ok", candidates: 0, archived: 0 };
    const ids = rows.map((row) => String(row.id));
    const products = await buildFreshWarehouseProducts(ids, { livePriceMaster: false }).catch((error) => {
      logger.warn("no-link archive sweep build failed", { detail: error?.message || String(error) });
      return [];
    });
    if (!products.length) return { status: "ok", candidates: rows.length, archived: 0 };
    const result = await runNoSupplierMarketplaceAutomation(
      { products },
      { productIds: ids, includeNoLinks: true, source: `no_link_archive_sweep_${source}` },
    ).catch((error) => {
      logger.warn("no-link archive sweep automation failed", { detail: error?.message || String(error) });
      return { archived: 0, zeroStockSent: 0 };
    });
    logger.info("no_link_archive_sweep_complete", {
      source,
      candidates: rows.length,
      archived: result.archived || 0,
      zeroStockSent: result.zeroStockSent || 0,
    });
    return { status: "ok", candidates: rows.length, archived: result.archived || 0 };
  } catch (error) {
    logger.warn("no-link archive sweep failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
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
      // Archive sweep runs fire-and-forget so it cannot delay the heartbeat write or
      // the next schedule tick — marketplace API calls inside can take several minutes
      // and would make the sweep appear stale to the heartbeat monitor otherwise.
      runNoLinkArchiveSweep({ source: "schedule" }).catch((error) => {
        logger.warn("no-link archive sweep tick failed", { detail: error?.message || String(error) });
      });
      await recordSweepHeartbeat("zero_stock_sweep", { status: result?.status || "unknown", intervalMs: zeroStockSweepIntervalMs, detail: result || {} }).catch(() => {});
      scheduleZeroStockSweep(zeroStockSweepIntervalMs);
    }
  }, normalizedDelay);
  zeroStockSweepTimer.unref?.();
}
