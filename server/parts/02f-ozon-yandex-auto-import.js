// Automatic Ozon -> Yandex catalog import (Postgres-backed).
//
// Every interval it finds Ozon products that are missing on Yandex (by offerId), filters
// out blocked ones (volume < 20ml by max volume, Отливант, inactive, not yandex-ready),
// creates the cards on Yandex, sends prices (markup rules) and stocks, and persists the
// new yandex rows locally. This keeps point "новый товар на Ozon появляется на Yandex
// автоматически" working without the heavy manual /send route, which only sees the
// in-memory warehouse subset in Postgres mode.

const ozonYandexAutoImportEnabled = process.env.OZON_YANDEX_AUTO_IMPORT_ENABLED !== "false";
const ozonYandexAutoImportIntervalHours = Math.max(1, Math.min(48, Number(process.env.OZON_YANDEX_AUTO_IMPORT_INTERVAL_HOURS || 6) || 6));
const ozonYandexAutoImportPerRunLimit = Math.max(10, Math.min(5000, Number(process.env.OZON_YANDEX_AUTO_IMPORT_PER_RUN || 500) || 500));
let ozonYandexAutoImportTimer = null;
let ozonYandexAutoImportRunning = false;
let ozonYandexAutoImportNextRunAt = null;

// Shared export pipeline: create Yandex cards for the given Ozon products, then send
// prices + stocks and persist local yandex rows. Used by both the scheduled auto-import
// and the manual import page. `products` are normalized warehouse products.
async function exportOzonProductsToYandex(products = [], shops = null, { reason = "ozon_yandex_import" } = {}) {
  const targetShops = Array.isArray(shops) && shops.length ? shops : uniqueYandexShopsByBusiness();
  const offers = (Array.isArray(products) ? products : [])
    .map((product) => buildYandexOfferMapping(product).offer)
    .filter((offer) => offer?.offerId);
  if (!offers.length || !targetShops.length) {
    return { sentOfferIds: new Set(), exportedProducts: [], failed: 0, results: [], priceStage: { sent: 0 }, stockStage: { sent: 0 } };
  }

  const cardResults = [];
  for (const shop of targetShops) {
    const sent = await sendYandexOfferMappings(shop, offers);
    cardResults.push(...sent.map((item) => ({ ...item, target: shop.id })));
  }
  const sentOfferIds = new Set(cardResults
    .filter((item) => item.ok)
    .map((item) => cleanText(item.offerId).toLowerCase())
    .filter(Boolean));
  const exportedProducts = products.filter((product) => sentOfferIds.has(cleanText(product.offerId).toLowerCase()));

  const priceStage = exportedProducts.length
    ? await sendYandexPricesFromOzonProducts(exportedProducts, { shops: targetShops, existingOfferIds: sentOfferIds })
    : { sent: 0, failed: 0 };
  const stockStage = exportedProducts.length
    ? await sendYandexStocksForExportedOzonProducts(exportedProducts, { shops: targetShops, existingOfferIds: sentOfferIds })
    : { sent: 0, failed: 0 };

  const now = new Date().toISOString();
  const yandexProducts = [];
  for (const product of exportedProducts) {
    const offerPrice = Number(buildYandexOfferMapping(product).offer?.basicPrice?.value) || 0;
    for (const shop of targetShops) {
      yandexProducts.push(buildYandexWarehouseProductFromOzonExport(product, shop, {
        status: "sent",
        targetName: shop.name || shop.id,
        sentAt: now,
        price: offerPrice > 0 ? offerPrice : undefined,
      }));
    }
  }
  if (yandexProducts.length) {
    await writeWarehouseProductPatch(yandexProducts, { reason })
      .catch((error) => logger.warn("ozon yandex export persist failed", { detail: error?.message || String(error) }));
  }

  return {
    sentOfferIds,
    exportedProducts,
    failed: cardResults.filter((item) => !item.ok).length,
    results: cardResults,
    priceStage,
    stockStage,
  };
}

async function runOzonYandexAutoImport({ limit = ozonYandexAutoImportPerRunLimit, source = "auto", onlyLinked = false } = {}) {
  if (ozonYandexAutoImportRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { status: "no_yandex_shops" };
  ozonYandexAutoImportRunning = true;
  const startedAt = Date.now();
  try {
    const yandexRows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      select: { offerId: true },
    });
    const existingOfferIds = new Set(yandexRows.map((row) => cleanText(row.offerId).toLowerCase()).filter(Boolean));

    const selected = [];
    let skippedBlocked = 0;
    let skippedExisting = 0;
    let skippedUnlinked = 0;
    let scanned = 0;
    let cursorId = null;
    // Only import from Ozon accounts with importEnabled (i.e. Ozon 1, not Ozon 2).
    const importTargets = getOzonAccounts()
      .filter((a) => a.importEnabled !== false)
      .map((a) => a.id);
    const baseWhere = onlyLinked
      ? { marketplace: "ozon", archived: false, target: { in: importTargets }, links: { some: {} } }
      : { marketplace: "ozon", archived: false, target: { in: importTargets } };
    while (selected.length < limit) {
      const page = await prisma.warehouseProduct.findMany({
        where: { ...baseWhere, ...(cursorId ? {} : {}) },
        include: { links: true },
        orderBy: { id: "asc" },
        take: 1000,
        ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
      });
      if (!page.length) break;
      cursorId = page[page.length - 1].id;
      for (const row of page) {
        scanned += 1;
        const offerKey = cleanText(row.offerId).toLowerCase();
        if (!offerKey) continue;
        if (existingOfferIds.has(offerKey)) {
          skippedExisting += 1;
          continue;
        }
        if (onlyLinked && !row.links?.length) {
          skippedUnlinked += 1;
          continue;
        }
        const product = productFromPostgres(row);
        if (!product) continue;
        const candidate = buildOzonYandexImportCandidate(product, { yandexExistingOfferIds: existingOfferIds });
        if (candidate.blockReasons?.length || !candidate.yandexReady || !candidate.eligible) {
          skippedBlocked += 1;
          continue;
        }
        selected.push(product);
        if (selected.length >= limit) break;
      }
      if (page.length < 1000) break;
    }

    if (!selected.length) {
      logger.info("ozon yandex auto import: nothing to import", { source, scanned, skippedExisting, skippedBlocked });
      return { status: "ok", sent: 0, scanned, skippedExisting, skippedBlocked, skippedUnlinked };
    }

    // Fetch fresh descriptions from Ozon API for products that lack them in DB.
    // Groups by account so we make one batched API call per account, not per product.
    const missingDescByTarget = {};
    for (const product of selected) {
      if (cleanText(product.ozon?.description)) continue;
      const target = product.target || "ozon";
      if (!missingDescByTarget[target]) missingDescByTarget[target] = [];
      missingDescByTarget[target].push(product);
    }
    for (const [target, accountProducts] of Object.entries(missingDescByTarget)) {
      const account = getOzonAccounts().find((a) => a.id === target);
      if (!account?.clientId || !account?.apiKey) continue;
      const offerIds = accountProducts.map((p) => cleanText(p.offerId)).filter(Boolean);
      if (!offerIds.length) continue;
      try {
        const infoMap = await getOzonProductInfoMap(offerIds, account, { continueOnError: true });
        for (const product of accountProducts) {
          const info = getOzonOfferMapValue(infoMap, product.offerId);
          if (cleanText(info?.description)) {
            product.ozon = { ...(product.ozon || {}), description: cleanText(info.description) };
          }
        }
        logger.info("ozon yandex auto import: fetched descriptions", { target, requested: offerIds.length, resolved: infoMap.size });
      } catch (descError) {
        logger.warn("ozon yandex auto import: description fetch failed", { target, detail: descError?.message || String(descError) });
      }
    }

    const exportResult = await exportOzonProductsToYandex(selected, shops, { reason: "ozon_yandex_auto_import" });
    const sentOfferIds = exportResult.sentOfferIds;
    const priceStage = exportResult.priceStage;
    const stockStage = exportResult.stockStage;
    const failed = exportResult.failed;
    logger.info("ozon_yandex_auto_import_complete", {
      source,
      onlyLinked,
      scanned,
      selected: selected.length,
      sent: sentOfferIds.size,
      failed,
      priceSent: Number(priceStage.sent || 0),
      stockSent: Number(stockStage.sent || 0),
      skippedExisting,
      skippedBlocked,
      skippedUnlinked,
      elapsedMs: Date.now() - startedAt,
    });
    return {
      status: "ok",
      scanned,
      selected: selected.length,
      sent: sentOfferIds.size,
      failed,
      priceSent: Number(priceStage.sent || 0),
      stockSent: Number(stockStage.sent || 0),
      skippedExisting,
      skippedBlocked,
      skippedUnlinked,
    };
  } catch (error) {
    logger.warn("ozon yandex auto import failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    ozonYandexAutoImportRunning = false;
  }
}

function scheduleOzonYandexAutoImport(delayMs = null) {
  if (!ozonYandexAutoImportEnabled) {
    ozonYandexAutoImportNextRunAt = null;
    return;
  }
  if (ozonYandexAutoImportTimer) clearTimeout(ozonYandexAutoImportTimer);
  const intervalMs = ozonYandexAutoImportIntervalHours * 60 * 60 * 1000;
  const normalizedDelay = Math.max(60_000, Number(delayMs ?? intervalMs) || intervalMs);
  ozonYandexAutoImportNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  ozonYandexAutoImportTimer = setTimeout(async () => {
    let deferred = false;
    try {
      if (heavyBackgroundWorkShouldDefer("ozon_yandex_auto_import")) {
        logger.info("ozon yandex auto import deferred under load");
        deferred = true;
        return;
      }
      await runOzonYandexAutoImport({ source: "schedule" });
    } catch (error) {
      logger.warn("ozon yandex auto import tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleOzonYandexAutoImport(deferred ? 15 * 60 * 1000 : intervalMs);
    }
  }, normalizedDelay);
  ozonYandexAutoImportTimer.unref?.();
}
