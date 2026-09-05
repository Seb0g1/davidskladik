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

    // Query live MySQL directly — PM is on the same server (local socket, ~1 ms).
    // This avoids loading/indexing the 77 MB snapshot just for the monitor pass.
    // Falls back to snapshot indexes if the pool is unavailable.
    let changed = 0;
    let missing = 0;
    let usedLive = true;

    // Collect unique rowIds to batch-fetch from MySQL in one query.
    const rowIdToLinks = new Map();
    for (const row of rows) {
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      const sourceRowId = cleanText(row.source_row_id || raw.sourceRowId || "");
      if (!sourceRowId) continue;
      const storedPrice = Number(raw.resolvedPriceMasterRow?.price ?? NaN);
      if (!Number.isFinite(storedPrice) || storedPrice <= 0) continue;
      if (!rowIdToLinks.has(sourceRowId)) rowIdToLinks.set(sourceRowId, []);
      rowIdToLinks.get(sourceRowId).push({ row, raw, storedPrice });
    }

    if (!rowIdToLinks.size) {
      logger.info("pm_price_monitor_complete", { source, checked: rows.length, changed: 0, missingRowId: 0, pmSource: "none" });
      return { status: "ok", checked: rows.length, changed: 0, missingRowId: 0 };
    }

    // Batch-fetch current prices from live MySQL for all pinned rowIds.
    let liveByRowId = new Map();
    try {
      const rowIds = Array.from(rowIdToLinks.keys()).map(Number).filter((n) => Number.isFinite(n) && n > 0);
      if (rowIds.length && pool) {
        const placeholders = rowIds.map(() => "?").join(",");
        const [liveRows] = await pool.query(
          `SELECT r.RowID AS rowId, r.NativePrice AS price, r.NativeName AS name,
                  r.NativeID AS article, d.PartnerID AS partnerId, p.PartnerName AS partnerName
           FROM OfferRows r
           JOIN OfferDocs d ON d.DocID = r.DocID
           LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
           WHERE r.RowID IN (${placeholders})`,
          rowIds,
        );
        for (const r of liveRows || []) {
          liveByRowId.set(String(r.rowId), r);
        }
        logger.info("pm_price_monitor live MySQL query", { rowIds: rowIds.length, found: liveByRowId.size, pmSource: "live" });
      }
    } catch (liveError) {
      usedLive = false;
      logger.warn("pm_price_monitor live MySQL query failed, falling back to snapshot", { detail: liveError?.message || String(liveError) });
      // Fallback: load snapshot indexes
      try {
        const pmIndexes = await getPriceMasterSnapshotIndexes();
        for (const [sourceRowId, linkEntries] of rowIdToLinks) {
          const snapshotRows = pmIndexes.byRowId.get(sourceRowId) || [];
          if (!snapshotRows.length) {
            missing += linkEntries.length;
            continue;
          }
          const currentFields = priceMasterSnapshotRowFields(snapshotRows[0]);
          const currentPrice = Number(currentFields.price ?? NaN);
          for (const { row, raw, storedPrice } of linkEntries) {
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
                pmSource: "snapshot_fallback",
              });
            }
          }
        }
        logger.info("pm_price_monitor_complete", { source, checked: rows.length, changed, missingRowId: missing, pmSource: "snapshot_fallback" });
        return { status: "ok", checked: rows.length, changed, missingRowId: missing };
      } catch (snapshotError) {
        logger.warn("pm_price_monitor snapshot fallback also failed", { detail: snapshotError?.message || String(snapshotError) });
        return { status: "error", error: snapshotError?.message || String(snapshotError) };
      }
    }

    // Compare stored prices against live MySQL results.
    for (const [sourceRowId, linkEntries] of rowIdToLinks) {
      const liveRow = liveByRowId.get(sourceRowId);
      if (!liveRow) {
        missing += linkEntries.length;
        continue;
      }
      const currentPrice = Number(liveRow.price ?? NaN);
      for (const { row, raw, storedPrice } of linkEntries) {
        if (!Number.isFinite(currentPrice) || currentPrice <= 0) continue;
        const delta = Math.abs(currentPrice - storedPrice) / storedPrice;
        if (delta >= pmPriceMonitorChangeThreshold) {
          changed += 1;
          logger.warn("pm_price_changed", {
            offerId: cleanText(row.offer_id || ""),
            marketplace: cleanText(row.marketplace || ""),
            supplierName: cleanText(row.supplier_name || liveRow.partnerName || ""),
            article: cleanText(row.article || liveRow.article || ""),
            sourceRowId,
            storedPriceUsd: storedPrice,
            currentPriceUsd: currentPrice,
            deltaPct: Math.round(delta * 1000) / 10,
            pmSource: "live",
          });
        }
      }
    }

    logger.info("pm_price_monitor_complete", {
      source,
      checked: rows.length,
      changed,
      missingRowId: missing,
      pmSource: usedLive ? "live" : "snapshot_fallback",
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

    // Query live MySQL directly — PM is on the same server (local socket, ~1 ms).
    // Avoids loading the 77 MB snapshot just to check rowId/article existence.
    // Falls back to snapshot indexes if the pool is unavailable.
    let disappeared = 0;
    let activeOk = 0;

    // Collect unique (rowId, article, partnerId) tuples for batched MySQL lookup.
    const linkChecks = [];
    for (const row of rows) {
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      const sourceRowId = cleanText(row.source_row_id || raw.sourceRowId || "");
      if (!sourceRowId) continue;
      linkChecks.push({
        sourceRowId,
        article: cleanText(raw.article || row.article || ""),
        exactName: cleanText(row.exact_name || raw.exactName || ""),
        partnerId: cleanText(raw.partnerId || row.partner_id || ""),
        offerId: cleanText(row.offer_id || ""),
        marketplace: cleanText(row.marketplace || ""),
        supplierName: cleanText(row.supplier_name || ""),
        linkId: cleanText(row.link_id || ""),
      });
    }

    if (!linkChecks.length) {
      logger.info("pm_disappear_monitor_complete", { source, checked: rows.length, disappeared: 0, activeOk: 0, pmSource: "none" });
      return { status: "ok", checked: rows.length, disappeared: 0, activeOk: 0 };
    }

    // Batch-check rowIds and articles from live MySQL.
    try {
      const uniqueRowIds = [...new Set(linkChecks.map((c) => Number(c.sourceRowId)).filter((n) => Number.isFinite(n) && n > 0))];
      const uniqueArticles = [...new Set(linkChecks.map((c) => c.article).filter(Boolean))];

      let existingRowIds = new Set();
      let existingArticlesByPartner = new Map(); // "partnerId|article" → true

      if (uniqueRowIds.length && pool) {
        const rPlaceholders = uniqueRowIds.map(() => "?").join(",");
        const [rowIdRows] = await pool.query(
          `SELECT r.RowID AS rowId, d.PartnerID AS partnerId, r.NativeID AS article
           FROM OfferRows r JOIN OfferDocs d ON d.DocID = r.DocID
           WHERE r.RowID IN (${rPlaceholders}) AND r.Ignored = 0`,
          uniqueRowIds,
        );
        for (const r of rowIdRows || []) existingRowIds.add(String(r.rowId));
      }

      if (uniqueArticles.length && pool) {
        const aPlaceholders = uniqueArticles.map(() => "?").join(",");
        const [articleRows] = await pool.query(
          `SELECT DISTINCT TRIM(r.NativeID) AS article, d.PartnerID AS partnerId
           FROM OfferRows r JOIN OfferDocs d ON d.DocID = r.DocID
           WHERE BINARY TRIM(r.NativeID) IN (${aPlaceholders}) AND r.Ignored = 0`,
          uniqueArticles,
        );
        for (const r of articleRows || []) {
          existingArticlesByPartner.set(`${r.partnerId}|${cleanText(r.article)}`, true);
          existingArticlesByPartner.set(`any|${cleanText(r.article)}`, true);
        }
      }

      logger.info("pm_disappear_monitor live MySQL query", {
        rowIds: uniqueRowIds.length,
        foundRowIds: existingRowIds.size,
        articles: uniqueArticles.length,
        pmSource: "live",
      });

      for (const check of linkChecks) {
        if (existingRowIds.has(check.sourceRowId)) {
          activeOk += 1;
          continue;
        }
        // rowId gone — check if article still exists under same partner
        const articleKey = check.partnerId
          ? `${check.partnerId}|${check.article}`
          : (check.article ? `any|${check.article}` : "");
        const articleMatch = check.article && (
          (check.partnerId && existingArticlesByPartner.has(`${check.partnerId}|${check.article}`))
          || existingArticlesByPartner.has(`any|${check.article}`)
        );

        if (!articleMatch) {
          disappeared += 1;
          logger.warn("pm_product_disappeared", {
            offerId: check.offerId,
            marketplace: check.marketplace,
            supplierName: check.supplierName,
            article: check.article,
            sourceRowId: check.sourceRowId,
            exactName: check.exactName,
            partnerId: check.partnerId,
            linkId: check.linkId,
            pmSource: "live",
          });
        } else {
          activeOk += 1;
          logger.info("pm_rowid_moved", {
            offerId: check.offerId,
            sourceRowId: check.sourceRowId,
            article: check.article,
            articleFound: true,
            pmSource: "live",
          });
        }
      }
    } catch (liveError) {
      logger.warn("pm_disappear_monitor live MySQL failed, falling back to snapshot", { detail: liveError?.message || String(liveError) });
      // Snapshot fallback
      try {
        const pmIndexes = await getPriceMasterSnapshotIndexes();
        for (const check of linkChecks) {
          const byRowId = pmIndexes.byRowId.get(check.sourceRowId) || [];
          if (byRowId.length) { activeOk += 1; continue; }
          const byArticle = check.article ? (pmIndexes.byArticle.get(check.article) || []) : [];
          const articleMatch = check.partnerId
            ? byArticle.some((r) => cleanText(priceMasterSnapshotRowFields(r).partnerId) === check.partnerId)
            : byArticle.length > 0;
          const nameMatch = check.exactName && (pmIndexes.byName.get(check.exactName.toLowerCase()) || []).some((r) => {
            if (!check.partnerId) return true;
            return cleanText(priceMasterSnapshotRowFields(r).partnerId) === check.partnerId;
          });
          if (!articleMatch && !nameMatch) {
            disappeared += 1;
            logger.warn("pm_product_disappeared", { offerId: check.offerId, marketplace: check.marketplace, supplierName: check.supplierName, article: check.article, sourceRowId: check.sourceRowId, exactName: check.exactName, partnerId: check.partnerId, linkId: check.linkId, pmSource: "snapshot_fallback" });
          } else {
            activeOk += 1;
            logger.info("pm_rowid_moved", { offerId: check.offerId, sourceRowId: check.sourceRowId, article: check.article, articleFound: articleMatch, nameFound: nameMatch, pmSource: "snapshot_fallback" });
          }
        }
      } catch (snapshotError) {
        logger.warn("pm_disappear_monitor snapshot fallback also failed", { detail: snapshotError?.message || String(snapshotError) });
        return { status: "error", error: snapshotError?.message || String(snapshotError) };
      }
    }

    logger.info("pm_disappear_monitor_complete", {
      source,
      checked: rows.length,
      disappeared,
      activeOk,
      pmSource: "live",
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

// ─── Stale Row ID Auto-Fix ────────────────────────────────────────────────────
// Periodically scans for selected_row product links whose pinned sourceRowId is
// no longer active in the PM snapshot (supplier republished with a new RowId),
// then automatically re-pins to the best available row for the same article+partner
// and triggers stock recovery so the products show as in-stock again.

const pmStaleRowFixEnabled = process.env.PM_STALE_ROW_FIX_ENABLED !== "false";
const pmStaleRowFixIntervalMs = Math.max(
  60 * 60_000,
  Number(process.env.PM_STALE_ROW_FIX_INTERVAL_SECONDS || 6 * 3600) * 1000 || 6 * 3_600_000,
);

let pmStaleRowFixTimer = null;
let pmStaleRowFixRunning = false;

async function runPmStaleRowIdFix({ source = "schedule" } = {}) {
  if (pmStaleRowFixRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  pmStaleRowFixRunning = true;
  try {
    const result = await runBulkStaleRecoveryOperation({});
    logger.info("pm_stale_row_fix_complete", {
      source,
      found: result.found || 0,
      fixed: result.fixed || 0,
      products: result.productCount || 0,
    });
    return { status: "ok", ...result };
  } catch (error) {
    logger.warn("pm stale row fix failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    pmStaleRowFixRunning = false;
  }
}

function schedulePmStaleRowFix(delayMs = pmStaleRowFixIntervalMs) {
  if (!pmStaleRowFixEnabled || !backgroundJobsEnabled || isApiServer) return;
  if (pmStaleRowFixTimer) clearTimeout(pmStaleRowFixTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || pmStaleRowFixIntervalMs);
  pmStaleRowFixTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runPmStaleRowIdFix({ source: "schedule" });
    } catch (error) {
      logger.warn("pm stale row fix tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("pm_stale_row_fix", {
        status: result?.status || "unknown",
        intervalMs: pmStaleRowFixIntervalMs,
        detail: result || {},
      }).catch(() => {});
      schedulePmStaleRowFix(pmStaleRowFixIntervalMs);
    }
  }, normalizedDelay);
  pmStaleRowFixTimer.unref?.();
}

if (backgroundJobsEnabled && !isApiServer) {
  // Start after 15 min — well after the PM snapshot has been loaded and indexed.
  schedulePmStaleRowFix(Math.min(pmStaleRowFixIntervalMs, 15 * 60_000));
}
