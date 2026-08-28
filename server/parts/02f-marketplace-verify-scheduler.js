// Marketplace stock verification: every 2 hours query Ozon API for actual FBS stock
// and correct any product whose actual stock is below targetStock even though our DB
// believes it is synced (e.g. API rejected the push silently, race with a sale, etc.).
//
// The regular stock sweep (02f-stock-sweep.js) only trusts DB flags — this job adds
// a real API readback loop. Ozon FBS only; YM can be added later.

const marketplaceVerifyEnabled = process.env.MARKETPLACE_VERIFY_ENABLED !== "false";
const marketplaceVerifyIntervalMs = Math.max(
  30 * 60_000,
  Number(process.env.MARKETPLACE_VERIFY_INTERVAL_MINUTES || 120) * 60_000 || 120 * 60_000,
);
const marketplaceVerifyBatchLimit = Math.max(
  50,
  Math.min(1000, Number(process.env.MARKETPLACE_VERIFY_BATCH_LIMIT || 500) || 500),
);

let marketplaceVerifyTimer = null;
let marketplaceVerifyRunning = false;
let marketplaceVerifyNextRunAt = null;

async function runMarketplaceVerify({ source = "schedule" } = {}) {
  if (marketplaceVerifyRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };

  marketplaceVerifyRunning = true;
  try {
    const rows = await prisma.$queryRawUnsafe(`
      SELECT p.id::text AS id,
             p.offer_id   AS "offerId",
             p.target,
             p.target_stock::int AS "targetStock"
      FROM warehouse_products p
      WHERE p.archived = false
        AND p.marketplace = 'ozon'
        AND p.target_stock > 0
        AND p.offer_id IS NOT NULL AND p.offer_id <> ''
        AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
      ORDER BY p.updated_at ASC
      LIMIT ${marketplaceVerifyBatchLimit}
    `);

    if (!rows.length) return { status: "ok", checked: 0, mismatches: 0, corrected: 0 };

    let checked = 0;
    let mismatches = 0;
    const toCorrect = [];

    for (const account of getOzonAccounts()) {
      const accountRows = rows.filter((r) => matchesOzonTarget(cleanText(r.target || "ozon"), account.id));
      if (!accountRows.length) continue;

      const offerIds = accountRows.map((r) => r.offerId).filter(Boolean);
      let stockMap;
      try {
        stockMap = await getOzonStockMap(offerIds, account, { continueOnError: true });
      } catch (err) {
        logger.warn("marketplace verify: ozon stock map failed", {
          account: account.id,
          detail: err?.message || String(err),
        });
        continue;
      }

      for (const row of accountRows) {
        checked++;
        const key = cleanText(row.offerId || "").toLowerCase();
        const actual = stockMap.get(key) || stockMap.get(row.offerId);
        if (!actual) continue; // offer not found in Ozon at all — skip (not our concern here)
        const actualStock = Number.isFinite(Number(actual.stock)) ? Number(actual.stock) : 0;
        const targetStock = Math.round(Number(row.targetStock || 0));
        if (actualStock >= targetStock) continue;

        mismatches++;
        toCorrect.push({
          id: row.id,
          offerId: row.offerId,
          target: cleanText(row.target || "ozon"),
          targetStock,
          marketplace: "ozon",
          _forceStock: true,
        });
      }
    }

    let corrected = 0;
    if (toCorrect.length) {
      const actions = await sendTargetStocksToMarketplace(toCorrect).catch((err) => {
        logger.warn("marketplace verify: correction send failed", { detail: err?.message || String(err) });
        return [];
      });
      corrected = actions.filter((a) => a.ok).length;
    }

    logger.info("marketplace_verify_complete", { source, checked, mismatches, corrected });
    return { status: "ok", checked, mismatches, corrected };
  } catch (error) {
    logger.warn("marketplace verify failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    marketplaceVerifyRunning = false;
  }
}

function scheduleMarketplaceVerify(delayMs = marketplaceVerifyIntervalMs) {
  if (!marketplaceVerifyEnabled) {
    marketplaceVerifyNextRunAt = null;
    return;
  }
  if (marketplaceVerifyTimer) clearTimeout(marketplaceVerifyTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || marketplaceVerifyIntervalMs);
  marketplaceVerifyNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  marketplaceVerifyTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runMarketplaceVerify({ source: "schedule" });
    } catch (error) {
      logger.warn("marketplace verify tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("marketplace_verify", {
        status: result?.status || "unknown",
        intervalMs: marketplaceVerifyIntervalMs,
        detail: result || {},
      }).catch(() => {});
      scheduleMarketplaceVerify(marketplaceVerifyIntervalMs);
    }
  }, normalizedDelay);
  marketplaceVerifyTimer.unref?.();
}

function startMarketplaceVerifyScheduler() {
  if (!marketplaceVerifyEnabled) return;
  if (!isWorkerServer && !isMonolithServer) return;
  // Stagger first run by 10 minutes so it doesn't pile on top of startup sweeps.
  scheduleMarketplaceVerify(10 * 60_000);
  logger.info("marketplace verify scheduler started", {
    intervalMinutes: Math.round(marketplaceVerifyIntervalMs / 60_000),
    batch: marketplaceVerifyBatchLimit,
    nextRunAt: marketplaceVerifyNextRunAt,
  });
}
