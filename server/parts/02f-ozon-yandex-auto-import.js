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
const ozonYandexAutoImportStatePath = path.join(dataDir, "ozon-yandex-auto-import-state.json");
let ozonYandexAutoImportTimer = null;
let ozonYandexAutoImportRunning = false;
let ozonYandexAutoImportNextRunAt = null;
let ozonYandexAutoImportLastAt = 0;

// Ozon attributes needed by the transfer rules (Бренд 85, модель 9048, страна 4389, название
// 4180), real barcodes, type and package dimensions — read from /v4/product/info/attributes,
// because the local copy of an Ozon product carries none of them.
async function enrichOzonProductsForYandexExport(products = []) {
  const list = Array.isArray(products) ? products : [];
  const byTarget = new Map();
  for (const product of list) {
    const target = cleanText(product.target) || "ozon";
    if (!byTarget.has(target)) byTarget.set(target, []);
    byTarget.get(target).push(product);
  }
  const infoByKey = new Map();
  for (const [target, items] of byTarget) {
    const account = getOzonAccountByTarget(target);
    if (!account) continue;
    const offerIds = Array.from(new Set(items.map((item) => cleanText(item.offerId)).filter(Boolean)));
    for (const chunk of chunkArray(offerIds, 1000)) {
      try {
        const data = await ozonRequest("/v4/product/info/attributes", { filter: { offer_id: chunk, visibility: "ALL" }, limit: 1000 }, account);
        for (const item of data?.result || []) infoByKey.set(`${target}:${cleanText(item.offer_id).toLowerCase()}`, item);
      } catch (error) {
        logger.warn("ozon attributes for yandex export failed", { target, items: chunk.length, detail: error?.message || String(error) });
      }
    }
  }
  const withAttributes = list.map((product) => {
    const info = infoByKey.get(`${cleanText(product.target) || "ozon"}:${cleanText(product.offerId).toLowerCase()}`);
    if (!info) return product;
    const ozon = product.ozon && typeof product.ozon === "object" ? product.ozon : {};
    return {
      ...product,
      ozon: {
        ...ozon,
        attributes: Array.isArray(info.attributes) ? info.attributes : ozon.attributes,
        typeId: Number(info.type_id) || ozon.typeId,
        name: cleanText(info.name) || ozon.name,
        barcodes: Array.from(new Set([...(Array.isArray(info.barcodes) ? info.barcodes : []), cleanText(info.barcode)].filter(Boolean))),
        barcode: cleanText(info.barcode) || ozon.barcode,
        depth: Number(info.depth) || ozon.depth,
        width: Number(info.width) || ozon.width,
        height: Number(info.height) || ozon.height,
        dimensionUnit: cleanText(info.dimension_unit) || ozon.dimensionUnit,
        weight: Number(info.weight) || ozon.weight,
        weightUnit: cleanText(info.weight_unit) || ozon.weightUnit,
      },
    };
  });
  return fillOzonDescriptionsForYandexExport(withAttributes);
}

// The Ozon list import stores no descriptions, and /v4/product/info/attributes has none, so
// without this every transferred card reached Market with an empty description. Missing ones
// come from /v1/product/info/description (one product per call) and are saved on the Ozon row.
async function fillOzonDescriptionsForYandexExport(products = [], { delayMs = 60 } = {}) {
  let apiErrors = 0;
  const result = [];
  for (const product of products) {
    const ozon = product.ozon && typeof product.ozon === "object" ? product.ozon : {};
    if (cleanText(ozon.description) || cleanText(product.yandex?.description) || apiErrors >= 10) {
      result.push(product);
      continue;
    }
    const account = getOzonAccountByTarget(cleanText(product.target) || "ozon");
    const offerId = cleanText(product.offerId);
    let description = "";
    if (account && offerId) {
      try {
        const body = product.productId ? { product_id: Number(product.productId) || undefined, offer_id: offerId } : { offer_id: offerId };
        const data = await ozonRequest("/v1/product/info/description", body, account);
        description = cleanText(data?.result?.description || "");
      } catch (error) {
        apiErrors += 1;
        logger.warn("ozon description for yandex export failed", { offerId, detail: error?.message || String(error) });
      }
      if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    if (description) {
      const prisma = getPrisma();
      if (prisma) {
        await prisma.$executeRaw`
          UPDATE warehouse_products
          SET raw = jsonb_set(raw, '{ozon,description}', ${JSON.stringify(description)}::jsonb, true)
          WHERE marketplace = 'ozon' AND raw IS NOT NULL AND target = ${cleanText(product.target) || "ozon"} AND LOWER(offer_id) = LOWER(${offerId})
        `.catch(() => {});
      }
      result.push({ ...product, ozon: { ...ozon, description } });
    } else {
      result.push(product);
    }
  }
  return result;
}

// Shared export pipeline: create Yandex cards for the given Ozon products, then send
// prices + stocks and persist local yandex rows. Used by both the scheduled auto-import
// and the manual import page. `products` are normalized warehouse products.
async function exportOzonProductsToYandex(inputProducts = [], shops = null, { reason = "ozon_yandex_import" } = {}) {
  const targetShops = Array.isArray(shops) && shops.length ? shops : uniqueYandexShopsByBusiness();
  await loadYandexVendorCanonicalMap();
  const enriched = await enrichOzonProductsForYandexExport(inputProducts);
  // Re-check readiness with the real Ozon attributes (brand, category, name).
  const skipped = [];
  const products = enriched.filter((product) => {
    const built = buildYandexOfferMapping(product);
    if (built.ready) return true;
    skipped.push({ offerId: product.offerId, missing: built.missing, categoryReview: built.categoryReview });
    return false;
  });
  if (skipped.length) {
    logger.info("ozon yandex export: products left for manual review", { count: skipped.length, sample: skipped.slice(0, 20) });
  }
  const offers = products
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

async function ozonYandexAutoImportIsDue() {
  const intervalMs = ozonYandexAutoImportIntervalHours * 60 * 60 * 1000;
  if (!ozonYandexAutoImportLastAt) {
    try {
      const saved = JSON.parse(await fs.readFile(ozonYandexAutoImportStatePath, "utf8"));
      ozonYandexAutoImportLastAt = new Date(saved.lastRunAt || 0).getTime() || 0;
    } catch {
      ozonYandexAutoImportLastAt = 0;
    }
  }
  return Date.now() - ozonYandexAutoImportLastAt >= intervalMs;
}

async function markOzonYandexAutoImportDone() {
  ozonYandexAutoImportLastAt = Date.now();
  await fs.writeFile(ozonYandexAutoImportStatePath, JSON.stringify({ lastRunAt: new Date().toISOString() }, null, 2)).catch(() => {});
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
      // Only block on memory/HTTP pressure — NOT on autoSyncRunning.
      // The auto-import is Postgres-only and doesn't conflict with the in-memory
      // autoSync, but the worker restarts so frequently that the timer always fires
      // while autoSync is running, causing permanent deferral.
      if (serverUnderMemoryPressure() || serverUnderHttpLoad()) {
        logger.info("ozon yandex auto import deferred under load");
        deferred = true;
        return;
      }
      if (!await ozonYandexAutoImportIsDue()) {
        // Already ran recently (e.g., another process or manual trigger)
        return;
      }
      await runOzonYandexAutoImport({ source: "schedule" });
      await markOzonYandexAutoImportDone();
    } catch (error) {
      logger.warn("ozon yandex auto import tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleOzonYandexAutoImport(deferred ? 5 * 60 * 1000 : intervalMs);
    }
  }, normalizedDelay);
  ozonYandexAutoImportTimer.unref?.();
}
