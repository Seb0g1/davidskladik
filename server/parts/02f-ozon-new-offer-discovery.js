// Discovers NEW Ozon offers and imports them into the local warehouse.
//
// The daily full marketplace import is disabled in production (DAILY_FULL_IMPORT_ENABLED=false
// in ecosystem.config.cjs — the full import OOM'ed the processes) and the interval auto sync
// deliberately skips marketplace import, so nothing picked up products the operator lists on
// Ozon (no new warehouse rows since 2026-06-12). This job pages the cheap /v3/product/list,
// diffs offerIds against warehouse_products and imports ONLY the missing offers with full
// details — light enough to run hourly on the worker. New rows are then picked up by the
// Ozon->Yandex auto import and the photo backfill automatically.

const ozonNewOfferDiscoveryEnabled = process.env.OZON_NEW_OFFER_DISCOVERY_ENABLED !== "false";
const ozonNewOfferDiscoveryIntervalMinutes = Math.max(15, Math.min(1440, Number(process.env.OZON_NEW_OFFER_DISCOVERY_INTERVAL_MINUTES || 60) || 60));
const ozonNewOfferDiscoveryPerRunLimit = Math.max(10, Math.min(5000, Number(process.env.OZON_NEW_OFFER_DISCOVERY_PER_RUN || 1000) || 1000));
let ozonNewOfferDiscoveryTimer = null;
let ozonNewOfferDiscoveryRunning = false;
let ozonNewOfferDiscoveryNextRunAt = null;

async function runOzonNewOfferDiscovery({ limit = ozonNewOfferDiscoveryPerRunLimit, source = "auto" } = {}) {
  if (ozonNewOfferDiscoveryRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  const accounts = getOzonAccounts().filter((account) => account.clientId && account.apiKey);
  if (!accounts.length) return { status: "no_ozon_accounts" };
  ozonNewOfferDiscoveryRunning = true;
  const startedAt = Date.now();
  try {
    // One light pass over the whole table: offerId+target only.
    const existingRows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "ozon" },
      select: { offerId: true, target: true },
    });
    const perAccount = [];
    let importedTotal = 0;
    for (const account of accounts) {
      const existingOfferKeys = new Set(existingRows
        .filter((row) => matchesOzonTarget(row.target, account.id))
        .map((row) => cleanText(row.offerId).toLowerCase())
        .filter(Boolean));
      const listed = await getOzonProducts(Number.POSITIVE_INFINITY, account);
      const missing = [];
      for (const item of listed) {
        const key = cleanText(item.offer_id || item.offerId).toLowerCase();
        if (!key || existingOfferKeys.has(key)) continue;
        missing.push(item);
        if (missing.length >= limit) break;
      }
      if (!missing.length) {
        perAccount.push({ account: account.id, listed: listed.length, missing: 0, imported: 0 });
        continue;
      }
      const offerIds = missing.map((item) => cleanText(item.offer_id || item.offerId)).filter(Boolean);
      const [infoMap, stockMap, priceMap] = await Promise.all([
        getOzonProductInfoMap(offerIds, account, { continueOnError: true }),
        getOzonStockMap(offerIds, account, { continueOnError: true }),
        getOzonPriceMap(offerIds, account, { continueOnError: true }),
      ]);
      const products = missing.map((item) => buildOzonImportedWarehouseProduct(account, item, {
        info: getOzonOfferMapValue(infoMap, item.offer_id) || {},
        stockInfo: getOzonOfferMapValue(stockMap, item.offer_id) || {},
        priceInfo: getOzonOfferMapValue(priceMap, item.offer_id) || {},
      }));
      await writeWarehouseProductPatch(products, { reason: "ozon_new_offer_discovery", writeLinks: false });
      importedTotal += products.length;
      perAccount.push({ account: account.id, listed: listed.length, missing: missing.length, imported: products.length });
    }
    logger.info("ozon_new_offer_discovery_complete", {
      source,
      imported: importedTotal,
      perAccount,
      elapsedMs: Date.now() - startedAt,
    });
    return { status: "ok", imported: importedTotal, perAccount, elapsedMs: Date.now() - startedAt };
  } catch (error) {
    logger.warn("ozon new offer discovery failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    ozonNewOfferDiscoveryRunning = false;
  }
}

function scheduleOzonNewOfferDiscovery(delayMs = null) {
  if (!ozonNewOfferDiscoveryEnabled) {
    ozonNewOfferDiscoveryNextRunAt = null;
    return;
  }
  if (ozonNewOfferDiscoveryTimer) clearTimeout(ozonNewOfferDiscoveryTimer);
  const intervalMs = ozonNewOfferDiscoveryIntervalMinutes * 60 * 1000;
  const normalizedDelay = Math.max(60_000, Number(delayMs ?? intervalMs) || intervalMs);
  ozonNewOfferDiscoveryNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  ozonNewOfferDiscoveryTimer = setTimeout(async () => {
    let deferred = false;
    try {
      // Only defer on memory/HTTP pressure — do NOT defer when autoSync is running.
      // Discovery is a read-only light job (list + info for NEW offers only); blocking
      // it on autoSyncRunning caused new products to never be imported while the
      // multi-hour autoSync was in progress.
      if (serverUnderMemoryPressure() || serverUnderHttpLoad()) {
        logger.info("ozon new offer discovery deferred under load");
        deferred = true;
        return;
      }
      await runOzonNewOfferDiscovery({ source: "schedule" });
    } catch (error) {
      logger.warn("ozon new offer discovery tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleOzonNewOfferDiscovery(deferred ? 10 * 60 * 1000 : intervalMs);
    }
  }, normalizedDelay);
  ozonNewOfferDiscoveryTimer.unref?.();
}

// Manual trigger for the first catch-up pass and diagnostics.
app.post("/api/ozon/discover-new", requireAdmin, async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(5000, Number(request.body?.limit || 0) || ozonNewOfferDiscoveryPerRunLimit));
    if (request.body?.wait === true) {
      return response.json(await runOzonNewOfferDiscovery({ limit, source: "manual" }));
    }
    void runOzonNewOfferDiscovery({ limit, source: "manual" });
    response.status(202).json({ ok: true, started: true, limit });
  } catch (error) {
    next(error);
  }
});
