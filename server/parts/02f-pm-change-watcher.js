// PriceMaster change watcher: marketplace stock and prices follow a supplier's price list
// within about a minute instead of waiting for the 10–30 min sweeps.
//
// PriceMaster MySQL runs on this server, so every PM_CHANGE_WATCH_INTERVAL_SECONDS (20 s) one
// grouped query fingerprints each supplier's current rows (row count, active flags, articles,
// names, prices; ~0.6 s for 325k rows). When a supplier's fingerprint changes and then stays
// the same for one more tick (the upload has finished), its linked products are rebuilt from
// live PriceMaster: products the supplier no longer carries lose their stock at once, products
// back in stock get it back, changed prices are sent, and the express warehouses are updated.
// The last handled fingerprints are kept in data/pm-change-watch.json, so a restart does not
// lose a change that happened while the worker was down.

const pmChangeWatchEnabled = process.env.PM_CHANGE_WATCH_ENABLED !== "false";
const pmChangeWatchIntervalMs = Math.max(10_000, Number(process.env.PM_CHANGE_WATCH_INTERVAL_SECONDS || 20) * 1000 || 20_000);
const pmChangeWatchBatchSize = Math.max(20, Math.min(300, Number(process.env.PM_CHANGE_WATCH_BATCH_SIZE || 100) || 100));
const pmChangeWatchStatePath = path.join(dataDir, "pm-change-watch.json");
let pmChangeWatchTimer = null;
let pmChangeWatchRunning = false;
let pmChangeBaseline = null; // partnerId -> fingerprint already applied to the marketplaces
let pmChangeLastSeen = new Map(); // partnerId -> fingerprint seen on the previous tick
const pmChangeWatchStatus = { lastTickAt: null, lastChangeAt: null, lastPartners: [], lastProducts: 0, lastError: null };

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
    return new Map(Object.entries(parsed?.partners || {}));
  } catch {
    return null;
  }
}

async function writePmChangeWatchState() {
  if (!pmChangeBaseline) return;
  const tmpPath = `${pmChangeWatchStatePath}.${process.pid}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify({ updatedAt: new Date().toISOString(), partners: Object.fromEntries(pmChangeBaseline) }));
  await fs.rename(tmpPath, pmChangeWatchStatePath);
}

async function linkedProductIdsForPmPartners(partners = []) {
  const prisma = getPrisma();
  if (!prisma || !partners.length) return [];
  const rows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT p.id
    FROM warehouse_products p
    JOIN product_links l ON l.product_id = p.id
    WHERE p.archived = false
      AND (l.partner_id = ANY($1::text[]) OR LOWER(TRIM(l.supplier_name)) = ANY($2::text[]))
  `, partners.map((partner) => partner.id), partners.map((partner) => partner.name.toLowerCase()).filter(Boolean));
  return rows.map((row) => String(row.id));
}

// Rebuild the products from live PriceMaster and bring stock and prices on the marketplaces in
// line with it. The watcher already waited for the upload to settle, so the no-supplier grace
// (meant for half-finished uploads) is skipped.
async function applyPmChangeToProducts(productIds = [], sourceEvent = "pm_change") {
  const totals = { products: 0, zeroed: 0, recovered: 0, pricesSent: 0 };
  for (const chunk of chunkArray(productIds, pmChangeWatchBatchSize)) {
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
  invalidateWarehouseViewCache();
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
    if (!pmChangeBaseline) {
      pmChangeBaseline = (await readPmChangeWatchState()) || new Map([...current].map(([id, value]) => [id, value.fingerprint]));
      pmChangeLastSeen = new Map([...current].map(([id, value]) => [id, value.fingerprint]));
      await writePmChangeWatchState().catch(() => {});
      return { status: "baseline", partners: current.size };
    }
    const ids = new Set([...current.keys(), ...pmChangeBaseline.keys()]);
    const settled = [];
    for (const id of ids) {
      const now = fingerprintOf(current, id);
      // Changed since it was last applied, and unchanged since the previous tick.
      if (now !== fingerprintOf(pmChangeBaseline, id) && now === fingerprintOf(pmChangeLastSeen, id)) settled.push(id);
    }
    pmChangeLastSeen = new Map([...ids].map((id) => [id, fingerprintOf(current, id)]));
    if (!settled.length) return { status: "ok", changed: 0 };

    const partners = settled.map((id) => ({ id, name: current.get(id)?.name || "" }));
    const productIds = await linkedProductIdsForPmPartners(partners);
    logger.info("pm_change_detected", { partners: partners.map((p) => `${p.id}:${p.name}`), products: productIds.length });
    const totals = productIds.length ? await applyPmChangeToProducts(productIds, "pm_change") : { products: 0 };
    for (const id of settled) {
      const fingerprint = fingerprintOf(current, id);
      if (fingerprint === "gone") pmChangeBaseline.delete(id);
      else pmChangeBaseline.set(id, fingerprint);
    }
    await writePmChangeWatchState().catch((error) => logger.warn("pm change watch state write failed", { detail: error?.message || String(error) }));
    if (partners.some((partner) => expressSupplierKey(partner.name))) {
      syncSorinExpressStocks().catch((error) => logger.warn("express sync after pm change failed", { detail: error?.message || String(error) }));
    }
    Object.assign(pmChangeWatchStatus, { lastChangeAt: new Date().toISOString(), lastPartners: partners, lastProducts: productIds.length, lastError: null });
    logger.info("pm_change_applied", { partners: partners.map((p) => p.name || p.id), ...totals });
    return { status: "ok", changed: partners.length, ...totals };
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
