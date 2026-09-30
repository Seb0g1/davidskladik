// PriceMaster change watcher: marketplace stock and prices follow a supplier's price list
// within about a minute instead of waiting for the 10–30 min sweeps.
//
// PriceMaster MySQL runs on this server, so every PM_CHANGE_WATCH_INTERVAL_SECONDS (20 s) one
// grouped query fingerprints each supplier's current rows (row count, active flags, articles,
// names, prices; ~0.6 s for 325k rows). When a supplier's fingerprint changes and then stays
// the same for one more tick (the upload has finished), its linked products are rebuilt from
// live PriceMaster: products the supplier no longer carries lose their stock at once, products
// back in stock get it back, changed prices are sent, and the express warehouses are updated.
// The last seen fingerprints and the queue of products still to rebuild are kept in
// data/pm-change-watch.json, so a restart neither loses a change nor starts the work over.
//
// The rebuild runs about one product per second, so a backlog (many price lists changed while the
// worker was down) takes hours. It used to run in one tick and saved nothing until the very end:
// every restart began the whole backlog again, and a fresh price list (Инна, 30.09) waited behind
// 20k products for hours. Now each change becomes a batch in a persisted queue, the newest batch
// is served first, each tick works for at most PM_CHANGE_WATCH_TICK_BUDGET_SECONDS, and progress
// is saved after every chunk.

const pmChangeWatchEnabled = process.env.PM_CHANGE_WATCH_ENABLED !== "false";
const pmChangeWatchIntervalMs = Math.max(10_000, Number(process.env.PM_CHANGE_WATCH_INTERVAL_SECONDS || 20) * 1000 || 20_000);
const pmChangeWatchBatchSize = Math.max(20, Math.min(300, Number(process.env.PM_CHANGE_WATCH_BATCH_SIZE || 100) || 100));
const pmChangeWatchTickBudgetMs = Math.max(20_000, Number(process.env.PM_CHANGE_WATCH_TICK_BUDGET_SECONDS || 90) * 1000 || 90_000);
const pmChangeWatchStatePath = path.join(dataDir, "pm-change-watch.json");
let pmChangeWatchTimer = null;
let pmChangeWatchRunning = false;
let pmChangeBaseline = null; // partnerId -> fingerprint already queued for the marketplaces
let pmChangeLastSeen = new Map(); // partnerId -> fingerprint seen on the previous tick
let pmChangePending = []; // [{ at, partners: [{ id, name }], ids: [productId] }], newest first
const pmChangeWatchStatus = { lastTickAt: null, lastChangeAt: null, lastPartners: [], lastProducts: 0, lastError: null, pendingProducts: 0 };

async function readPmPartnerFingerprints() {
  await discoverOfferDocsActiveColumn();
  const activeDocFilter = offerDocsActiveColumn ? ` AND d.${offerDocsActiveColumn}${offerDocsActiveFilterSuffix}` : "";
  const [rows] = await pool.query({
    sql: `SELECT d.PartnerID AS partnerId, MAX(p.PartnerName) AS partnerName, COUNT(*) AS n,
        SUM(r.Active != 0 AND r.Ignored = 0) AS active,
        SUM(CRC32(CONCAT_WS('|', r.RowID, r.NativeID, r.NativeName, r.NativePrice, r.Active, r.Ignored))) AS sig
      FROM OfferRows r
      JOIN OfferDocs d ON d.DocID = r.DocID
      LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
      WHERE 1 = 1${activeDocFilter}
      GROUP BY d.PartnerID`,
    timeout: 30_000,
  });
  const map = new Map();
  for (const row of rows || []) {
    map.set(String(row.partnerId), { name: cleanText(row.partnerName), fingerprint: `${row.n}:${row.active}:${row.sig}` });
  }
  return map;
}

async function readPmChangeWatchState() {
  try {
    const parsed = JSON.parse(await fs.readFile(pmChangeWatchStatePath, "utf8"));
    const pending = Array.isArray(parsed?.pending)
      ? parsed.pending
        .map((batch) => ({ at: batch.at || null, partners: Array.isArray(batch.partners) ? batch.partners : [], ids: Array.isArray(batch.ids) ? batch.ids.map(String) : [] }))
        .filter((batch) => batch.ids.length)
      : [];
    return { baseline: new Map(Object.entries(parsed?.partners || {})), pending };
  } catch {
    return null;
  }
}

async function writePmChangeWatchState() {
  if (!pmChangeBaseline) return;
  const tmpPath = `${pmChangeWatchStatePath}.${process.pid}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify({
    updatedAt: new Date().toISOString(),
    partners: Object.fromEntries(pmChangeBaseline),
    pending: pmChangePending,
  }));
  await fs.rename(tmpPath, pmChangeWatchStatePath);
}

// A new batch goes first; its products leave the older batches (one rebuild reads every supplier).
function enqueuePmChangeBatch(pending, batch) {
  const fresh = new Set(batch.ids);
  const older = pending
    .map((item) => ({ ...item, ids: item.ids.filter((id) => !fresh.has(id)) }))
    .filter((item) => item.ids.length);
  return batch.ids.length ? [batch, ...older] : older;
}

function pmChangePendingCount(pending = pmChangePending) {
  return pending.reduce((sum, batch) => sum + batch.ids.length, 0);
}

async function linkedProductIdsForPmPartners(partners = []) {
  const prisma = getPrisma();
  if (!prisma || !partners.length) return [];
  const rows = await prisma.$queryRawUnsafe(`
    SELECT p.id
    FROM warehouse_products p
    WHERE p.archived = false
      AND EXISTS (
        SELECT 1 FROM product_links l
        WHERE l.product_id = p.id
          AND (l.partner_id = ANY($1::text[]) OR LOWER(TRIM(l.supplier_name)) = ANY($2::text[]))
      )
    -- Products off sale first: they are the ones a returning supplier puts back on sale.
    ORDER BY (p.status = 'out_of_stock') DESC, p.updated_at ASC
  `, partners.map((partner) => partner.id), partners.map((partner) => partner.name.toLowerCase()).filter(Boolean));
  return rows.map((row) => String(row.id));
}

// Rebuild the products from live PriceMaster and bring stock and prices on the marketplaces in
// line with it. The watcher already waited for the upload to settle, so the no-supplier grace
// (meant for half-finished uploads) is skipped.
async function applyPmChangeToProducts(productIds = [], sourceEvent = "pm_change") {
  const totals = { products: 0, zeroed: 0, recovered: 0, pricesSent: 0 };
  for (const chunk of chunkArray(productIds, pmChangeWatchBatchSize)) {
    const part = await applyPmChangeToChunk(chunk, sourceEvent);
    for (const key of Object.keys(totals)) totals[key] += part[key];
  }
  invalidateWarehouseViewCache();
  return totals;
}

async function applyPmChangeToChunk(chunk = [], sourceEvent = "pm_change") {
  const totals = { products: 0, zeroed: 0, recovered: 0, pricesSent: 0 };
  {
    priceMasterLinkLookupCache.clear();
    const products = await buildFreshWarehouseProducts(chunk, {
      refreshPrices: false,
      livePriceMaster: true,
      batchPriceMaster: true,
      priceMasterTimeoutMs: Math.max(autoPricePmTimeoutMs, 8000),
    });
    const linked = products.filter((product) => productHasSupplierLinks(product));
    totals.products += linked.length;
    // Never zero on a failed PriceMaster read ("timeout" = no live or snapshot data).
    const lost = linked.filter((product) => !product.selectedSupplier && product.priceSource !== "timeout");
    if (lost.length) {
      const result = await runNoSupplierMarketplaceAutomation({ products: lost }, {
        productIds: lost.map((product) => product.id),
        includeNoLinks: false,
        source: sourceEvent,
        skipLinkedGrace: true,
      });
      totals.zeroed += Number(result?.zeroStockSent || 0);
    }
    const withSupplier = linked.filter((product) => product.selectedSupplier);
    const recover = pickImmediateLinkRecoveryCandidates(withSupplier);
    if (recover.length) {
      const result = await runSupplierRecoveryAutomation({ products: recover }, {
        productIds: recover.map((product) => product.id),
        source: sourceEvent,
        sourceEvent,
        force: true,
        deferOzonUnarchive: true,
      });
      totals.recovered += Number(result?.recovered || recover.length);
    }
    const priceIds = withSupplier
      .filter((product) => product.changed && !warehouseProductUsesStockOnlyPricing(product))
      .map((product) => product.id);
    if (priceIds.length) {
      const result = await sendWarehousePrices({
        productIds: priceIds,
        onlyChanged: true,
        livePriceMaster: true,
        refreshMarketplacePrices: false,
        reason: sourceEvent,
        sourceEvent,
        marketplace: "all",
      });
      totals.pricesSent += Number(result?.sent || 0);
    }
  }
  return totals;
}

// Next chunk of work: the head of the newest batch.
function takePmChangeChunk(pending, size) {
  const batch = pending[0];
  return batch ? batch.ids.slice(0, size) : [];
}

function completePmChangeChunk(pending, chunk) {
  const done = new Set(chunk);
  const [head, ...rest] = pending;
  if (!head) return { pending, finished: null };
  const left = head.ids.filter((id) => !done.has(id));
  return left.length
    ? { pending: [{ ...head, ids: left }, ...rest], finished: null }
    : { pending: rest, finished: head };
}

async function drainPmChangeQueue(deadline) {
  const totals = { products: 0, zeroed: 0, recovered: 0, pricesSent: 0, chunks: 0 };
  while (pmChangePending.length && Date.now() < deadline) {
    const chunk = takePmChangeChunk(pmChangePending, pmChangeWatchBatchSize);
    const part = await applyPmChangeToChunk(chunk, "pm_change");
    for (const key of ["products", "zeroed", "recovered", "pricesSent"]) totals[key] += part[key];
    totals.chunks += 1;
    const { pending, finished } = completePmChangeChunk(pmChangePending, chunk);
    pmChangePending = pending;
    await writePmChangeWatchState().catch((error) => logger.warn("pm change watch state write failed", { detail: error?.message || String(error) }));
    if (finished) {
      logger.info("pm_change_applied", { partners: finished.partners.map((p) => p.name || p.id), queuedAt: finished.at, pendingProducts: pmChangePendingCount() });
      if (finished.partners.some((partner) => expressSupplierKey(partner.name))) {
        syncSorinExpressStocks().catch((error) => logger.warn("express sync after pm change failed", { detail: error?.message || String(error) }));
      }
    }
  }
  if (totals.chunks) invalidateWarehouseViewCache();
  pmChangeWatchStatus.pendingProducts = pmChangePendingCount();
  return totals;
}

async function runPmChangeWatchTick() {
  if (pmChangeWatchRunning) return { status: "already_running" };
  pmChangeWatchRunning = true;
  try {
    const current = await readPmPartnerFingerprints();
    const fingerprintOf = (map, id) => (map instanceof Map && map.has(id)
      ? (typeof map.get(id) === "string" ? map.get(id) : map.get(id).fingerprint)
      : "gone");
    pmChangeWatchStatus.lastTickAt = new Date().toISOString();
    const deadline = Date.now() + pmChangeWatchTickBudgetMs;
    if (!pmChangeBaseline) {
      const saved = await readPmChangeWatchState();
      pmChangeBaseline = saved?.baseline || new Map([...current].map(([id, value]) => [id, value.fingerprint]));
      pmChangePending = saved?.pending || [];
      pmChangeLastSeen = new Map([...current].map(([id, value]) => [id, value.fingerprint]));
      await writePmChangeWatchState().catch(() => {});
      pmChangeWatchStatus.pendingProducts = pmChangePendingCount();
      if (pmChangePending.length) logger.info("pm_change_queue_resumed", { batches: pmChangePending.length, products: pmChangePendingCount() });
      return { status: "baseline", partners: current.size, pendingProducts: pmChangePendingCount() };
    }
    const ids = new Set([...current.keys(), ...pmChangeBaseline.keys()]);
    const settled = [];
    for (const id of ids) {
      const now = fingerprintOf(current, id);
      // Changed since it was last queued, and unchanged since the previous tick.
      if (now !== fingerprintOf(pmChangeBaseline, id) && now === fingerprintOf(pmChangeLastSeen, id)) settled.push(id);
    }
    pmChangeLastSeen = new Map([...ids].map((id) => [id, fingerprintOf(current, id)]));

    if (settled.length) {
      const partners = settled.map((id) => ({ id, name: current.get(id)?.name || "" }));
      const productIds = await linkedProductIdsForPmPartners(partners);
      logger.info("pm_change_detected", { partners: partners.map((p) => `${p.id}:${p.name}`), products: productIds.length, pendingBefore: pmChangePendingCount() });
      // Queue first, then mark the fingerprints as handled: the queue is saved together with them.
      pmChangePending = enqueuePmChangeBatch(pmChangePending, { at: new Date().toISOString(), partners, ids: productIds });
      for (const id of settled) {
        const fingerprint = fingerprintOf(current, id);
        if (fingerprint === "gone") pmChangeBaseline.delete(id);
        else pmChangeBaseline.set(id, fingerprint);
      }
      await writePmChangeWatchState().catch((error) => logger.warn("pm change watch state write failed", { detail: error?.message || String(error) }));
      Object.assign(pmChangeWatchStatus, { lastChangeAt: new Date().toISOString(), lastPartners: partners, lastProducts: productIds.length, lastError: null });
    }
    if (!pmChangePending.length) return { status: "ok", changed: settled.length };
    const totals = await drainPmChangeQueue(deadline);
    pmChangeWatchStatus.lastError = null;
    return { status: "ok", changed: settled.length, ...totals, pendingProducts: pmChangePendingCount() };
  } catch (error) {
    pmChangeWatchStatus.lastError = error?.message || String(error);
    logger.warn("pm change watch tick failed", { detail: pmChangeWatchStatus.lastError });
    return { status: "error", error: pmChangeWatchStatus.lastError };
  } finally {
    pmChangeWatchRunning = false;
  }
}

function schedulePmChangeWatch(delayMs = pmChangeWatchIntervalMs) {
  if (!pmChangeWatchEnabled) return;
  if (pmChangeWatchTimer) clearTimeout(pmChangeWatchTimer);
  pmChangeWatchTimer = setTimeout(async () => {
    try {
      await runPmChangeWatchTick();
    } finally {
      schedulePmChangeWatch(pmChangeWatchIntervalMs);
    }
  }, Math.max(5_000, Number(delayMs) || pmChangeWatchIntervalMs));
  pmChangeWatchTimer.unref?.();
}
