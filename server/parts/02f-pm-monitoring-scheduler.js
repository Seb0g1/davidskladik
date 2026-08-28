// Background PriceMaster monitoring: detect supplier price changes and disappearances.
//
// Two periodic sweeps run on the worker process only:
//   1. PM Price Change Monitor (every 15 min): compares each linked product's stored
//      PM USD price (resolvedPriceMasterRow.price at link save time) against the current
//      snapshot price. A >5% delta means the supplier changed their price and a reprice
//      cycle should be triggered by the normal price sweep.
//
//   2. Supplier Disappearance Monitor (every 30 min): checks whether each pinned link's
//      sourceRowId still exists in the PM snapshot. Missing rowId + missing article means
//      the supplier removed or renamed the product — stock should be zeroed.

const pmPriceMonitorEnabled = process.env.PM_PRICE_MONITOR_ENABLED !== "false";
const pmPriceMonitorIntervalMs = Math.max(
  60_000,
  Number(process.env.PM_PRICE_MONITOR_INTERVAL_SECONDS || 900) * 1000 || 900_000,
);
const pmPriceMonitorChangeThreshold = Math.max(
  0.01,
  Number(process.env.PM_PRICE_MONITOR_CHANGE_THRESHOLD || 0.05) || 0.05,
);
const pmPriceMonitorBatchLimit = Math.max(200, Math.min(5000, Number(process.env.PM_PRICE_MONITOR_BATCH_LIMIT || 2000) || 2000));

const pmDisappearMonitorEnabled = process.env.PM_DISAPPEAR_MONITOR_ENABLED !== "false";
const pmDisappearMonitorIntervalMs = Math.max(
  60_000,
  Number(process.env.PM_DISAPPEAR_MONITOR_INTERVAL_SECONDS || 1800) * 1000 || 1_800_000,
);
const pmDisappearMonitorBatchLimit = Math.max(200, Math.min(5000, Number(process.env.PM_DISAPPEAR_MONITOR_BATCH_LIMIT || 2000) || 2000));

let pmPriceMonitorTimer = null;
let pmPriceMonitorRunning = false;
let pmDisappearMonitorTimer = null;
let pmDisappearMonitorRunning = false;

async function runPmPriceChangeMonitor({ source = "schedule" } = {}) {
  if (pmPriceMonitorRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  pmPriceMonitorRunning = true;
  try {
    // Batch-fetch selected_row links that have a stored resolvedPriceMasterRow.price.
    // We only compare pinned rows — article-type links float to whatever PM returns.
    const rows = await prisma.$queryRawUnsafe(`
      SELECT
        pl.id AS link_id,
        p.id AS product_id,
        p.offer_id AS offer_id,
        p.marketplace,
        pl.supplier_article AS article,
        pl.partner_id AS partner_id,
        pl.supplier_name,
        pl.source_row_id,
        pl.exact_name,
        pl.raw
      FROM product_links pl
      JOIN warehouse_products p ON p.id = pl.product_id
      WHERE p.archived = false
        AND pl.raw->>'matchType' = 'selected_row'
        AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') IS NOT NULL
        AND pl.raw->'resolvedPriceMasterRow'->>'price' IS NOT NULL
      ORDER BY pl.updated_at DESC
      LIMIT ${pmPriceMonitorBatchLimit}
    `);

    if (!rows.length) return { status: "ok", checked: 0, changed: 0 };

    const pmIndexes = await getPriceMasterSnapshotIndexes();
    let changed = 0;
    let missing = 0;

    for (const row of rows) {
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      const sourceRowId = cleanText(row.source_row_id || raw.sourceRowId || "");
      if (!sourceRowId) continue;

      const storedPrice = Number(raw.resolvedPriceMasterRow?.price ?? NaN);
      if (!Number.isFinite(storedPrice) || storedPrice <= 0) continue;

      const snapshotRows = pmIndexes.byRowId.get(sourceRowId) || [];
      if (!snapshotRows.length) {
        // rowId no longer in snapshot — will be caught by disappearance monitor
        missing += 1;
        continue;
      }

      const currentFields = priceMasterSnapshotRowFields(snapshotRows[0]);
      const currentPrice = Number(currentFields.price ?? NaN);
      if (!Number.isFinite(currentPrice) || currentPrice <= 0) continue;

      const delta = Math.abs(currentPrice - storedPrice) / storedPrice;
      if (delta >= pmPriceMonitorChangeThreshold) {
        changed += 1;
        logger.warn("pm_price_changed", {
          offerId: cleanText(row.offer_id || ""),
          marketplace: cleanText(row.marketplace || ""),
          supplierName: cleanText(row.supplier_name || currentFields.partnerName || ""),
          article: cleanText(row.article || currentFields.article || ""),
          sourceRowId,
          storedPriceUsd: storedPrice,
          currentPriceUsd: currentPrice,
          deltaPct: Math.round(delta * 1000) / 10,
        });
      }
    }

    logger.info("pm_price_monitor_complete", {
      source,
      checked: rows.length,
      changed,
      missingRowId: missing,
    });
    return { status: "ok", checked: rows.length, changed, missingRowId: missing };
  } catch (error) {
    logger.warn("pm price monitor failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    pmPriceMonitorRunning = false;
  }
}

async function runPmDisappearanceMonitor({ source = "schedule" } = {}) {
  if (pmDisappearMonitorRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  pmDisappearMonitorRunning = true;
  try {
    // Fetch all pinned selected_row links to check if their rowId/article still exists.
    const rows = await prisma.$queryRawUnsafe(`
      SELECT
        pl.id AS link_id,
        p.id AS product_id,
        p.offer_id AS offer_id,
        p.marketplace,
        pl.supplier_article AS article,
        pl.partner_id,
        pl.supplier_name,
        pl.source_row_id,
        pl.exact_name,
        pl.raw
      FROM product_links pl
      JOIN warehouse_products p ON p.id = pl.product_id
      WHERE p.archived = false
        AND pl.raw->>'matchType' = 'selected_row'
        AND COALESCE(pl.source_row_id, pl.raw->>'sourceRowId') IS NOT NULL
      ORDER BY pl.updated_at DESC
      LIMIT ${pmDisappearMonitorBatchLimit}
    `);

    if (!rows.length) return { status: "ok", checked: 0, disappeared: 0 };

    const pmIndexes = await getPriceMasterSnapshotIndexes();
    let disappeared = 0;
    let activeOk = 0;

    for (const row of rows) {
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      const sourceRowId = cleanText(row.source_row_id || raw.sourceRowId || "");
      if (!sourceRowId) continue;

      const article = cleanText(raw.article || row.article || "");
      const exactName = cleanText(row.exact_name || raw.exactName || "");

      // Check if rowId still exists anywhere in the snapshot
      const byRowId = pmIndexes.byRowId.get(sourceRowId) || [];
      if (byRowId.length) {
        activeOk += 1;
        continue;
      }

      // rowId gone — check if article still exists under same partner
      const byArticle = article ? (pmIndexes.byArticle.get(article) || []) : [];
      const partnerId = cleanText(raw.partnerId || row.partner_id || "");
      const articleMatch = partnerId
        ? byArticle.some((r) => {
          const f = priceMasterSnapshotRowFields(r);
          return cleanText(f.partnerId) === partnerId;
        })
        : byArticle.length > 0;

      // Check by name if article also missing
      const nameMatch = exactName && (pmIndexes.byName.get(exactName.toLowerCase()) || []).some((r) => {
        if (!partnerId) return true;
        const f = priceMasterSnapshotRowFields(r);
        return cleanText(f.partnerId) === partnerId;
      });

      if (!articleMatch && !nameMatch) {
        disappeared += 1;
        logger.warn("pm_product_disappeared", {
          offerId: cleanText(row.offer_id || ""),
          marketplace: cleanText(row.marketplace || ""),
          supplierName: cleanText(row.supplier_name || ""),
          article,
          sourceRowId,
          exactName,
          partnerId,
          linkId: cleanText(row.link_id || ""),
        });
      } else {
        // rowId moved (article renumbered or re-uploaded) — price change monitor handles reprice
        activeOk += 1;
        logger.info("pm_rowid_moved", {
          offerId: cleanText(row.offer_id || ""),
          sourceRowId,
          article,
          articleFound: articleMatch,
          nameFound: nameMatch,
        });
      }
    }

    logger.info("pm_disappear_monitor_complete", {
      source,
      checked: rows.length,
      disappeared,
      activeOk,
    });
    return { status: "ok", checked: rows.length, disappeared, activeOk };
  } catch (error) {
    logger.warn("pm disappear monitor failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    pmDisappearMonitorRunning = false;
  }
}

function schedulePmPriceMonitor(delayMs = pmPriceMonitorIntervalMs) {
  if (!pmPriceMonitorEnabled || !backgroundJobsEnabled || isApiServer) return;
  if (pmPriceMonitorTimer) clearTimeout(pmPriceMonitorTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || pmPriceMonitorIntervalMs);
  pmPriceMonitorTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runPmPriceChangeMonitor({ source: "schedule" });
    } catch (error) {
      logger.warn("pm price monitor tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("pm_price_monitor", {
        status: result?.status || "unknown",
        intervalMs: pmPriceMonitorIntervalMs,
        detail: result || {},
      }).catch(() => {});
      schedulePmPriceMonitor(pmPriceMonitorIntervalMs);
    }
  }, normalizedDelay);
  pmPriceMonitorTimer.unref?.();
}

function schedulePmDisappearMonitor(delayMs = pmDisappearMonitorIntervalMs) {
  if (!pmDisappearMonitorEnabled || !backgroundJobsEnabled || isApiServer) return;
  if (pmDisappearMonitorTimer) clearTimeout(pmDisappearMonitorTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || pmDisappearMonitorIntervalMs);
  pmDisappearMonitorTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runPmDisappearanceMonitor({ source: "schedule" });
    } catch (error) {
      logger.warn("pm disappear monitor tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("pm_disappear_monitor", {
        status: result?.status || "unknown",
        intervalMs: pmDisappearMonitorIntervalMs,
        detail: result || {},
      }).catch(() => {});
      schedulePmDisappearMonitor(pmDisappearMonitorIntervalMs);
    }
  }, normalizedDelay);
  pmDisappearMonitorTimer.unref?.();
}

// Start both monitors with a staggered initial delay to avoid snapshot load at startup.
if (backgroundJobsEnabled && !isApiServer) {
  schedulePmPriceMonitor(Math.min(pmPriceMonitorIntervalMs, 5 * 60_000));
  schedulePmDisappearMonitor(Math.min(pmDisappearMonitorIntervalMs, 8 * 60_000));
}
