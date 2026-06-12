// Fast bulk Yandex unarchive for linked products (Medium subscription: 10k offers/min).
//
// Every YANDEX_FAST_UNARCHIVE_INTERVAL_SECONDS (60s) it pulls ALL archived+linked yandex
// products, unarchives them in bulk via offer-mappings/unarchive (chunks of 200), restores
// stock (min 1) and force-queues prices. Much faster than the per-product recovery flow,
// which stays as a deep-repair fallback in the reconciler tick.

const yandexFastUnarchiveEnabled = process.env.YANDEX_FAST_UNARCHIVE_ENABLED !== "false";
const yandexFastUnarchiveIntervalMs = Math.max(20_000, Number(process.env.YANDEX_FAST_UNARCHIVE_INTERVAL_SECONDS || 60) * 1000 || 60_000);
let yandexFastUnarchiveTimer = null;
let yandexFastUnarchiveRunning = false;
let yandexFastUnarchiveLastResult = null;
let yandexFastUnarchiveLastRunAt = null;

async function runFastYandexBulkUnarchive({ source = "schedule" } = {}) {
  if (yandexFastUnarchiveRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  yandexFastUnarchiveRunning = true;
  const startedAt = Date.now();
  try {
    const rows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex", archived: true, links: { some: {} } },
      include: { links: true },
      take: 9000, // stay inside the 10k/min Medium limit per pass
    });
    if (!rows.length) {
      yandexFastUnarchiveLastRunAt = new Date().toISOString();
      return { status: "ok", products: 0, unarchived: 0 };
    }

    const products = rows.map(productFromPostgres);

    // Yandex rows often carry only the offerId as name; pull Ozon sibling names so the
    // volume/keyword block check sees the real product name.
    const offerKeys = products.map((product) => cleanText(product.offerId)).filter(Boolean);
    const ozonNameRows = offerKeys.length
      ? await prisma.warehouseProduct.findMany({
        where: { marketplace: "ozon", offerId: { in: offerKeys } },
        select: { offerId: true, name: true },
      }).catch(() => [])
      : [];
    const ozonNameByOffer = new Map(ozonNameRows
      .map((row) => [cleanText(row.offerId).toLowerCase(), cleanText(row.name)])
      .filter(([key, value]) => key && value));

    // Partition: products that must not be on Yandex at all get their local yandex row
    // deleted (no wasted API calls). Everything else goes through unarchive FIRST — the
    // local "absent" state can be stale, so absence is decided by the live Yandex answer,
    // not by our cached marketplaceState.
    const toDelete = [];
    const toUnarchive = [];
    for (const product of products) {
      if (!cleanText(product.target) || !cleanText(product.offerId)) continue;
      let name = cleanText(product.name || product.yandex?.name || product.offerId);
      if (!name || name === cleanText(product.offerId)) {
        name = ozonNameByOffer.get(cleanText(product.offerId).toLowerCase()) || name;
      }
      const lowerName = name.toLowerCase();
      const hasBlockedKeyword = lowerName.includes("отливант")
        || lowerName.includes("тестер")
        || isYandexNoBoxProduct(name);
      // Named sets ("Парфюмерный набор" …) are sellable even with a small bundled item.
      const volumeAssessment = assessYandexSmallVolume(name);
      const smallVolume = volumeAssessment.blocked && !isYandexSetProduct(name);
      if (hasBlockedKeyword || smallVolume) {
        toDelete.push(product);
        continue;
      }
      toUnarchive.push(product);
    }

    // 1. Drop local yandex rows for <20ml / Тестер / Отливант — they were deleted from
    // Yandex (or never existed) and only waste recovery cycles and API budget.
    let locallyDeleted = 0;
    if (toDelete.length) {
      const deleteIds = toDelete.map((product) => String(product.id));
      await prisma.productLink.deleteMany({ where: { productId: { in: deleteIds } } }).catch(() => {});
      const res = await prisma.warehouseProduct.deleteMany({ where: { id: { in: deleteIds } } }).catch(() => ({ count: 0 }));
      locallyDeleted = res.count || 0;
      logger.info("yandex fast unarchive: removed blocked local yandex rows", {
        removed: locallyDeleted,
        sample: toDelete.slice(0, 5).map((product) => product.offerId),
      });
    }

    const groupByTarget = (list) => {
      const map = new Map();
      for (const product of list) {
        const target = cleanText(product.target);
        if (!map.has(target)) map.set(target, []);
        map.get(target).push(product);
      }
      return map;
    };

    const okIds = new Set();
    const failed = [];
    let createdCount = 0;

    for (const [target, items] of groupByTarget(toUnarchive).entries()) {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;

      // 2. Unarchive first — works for everything that actually sits in the Yandex archive.
      const results = await sendYandexOfferArchiveState(shop, items.map((item) => item.offerId), false);
      const okOffers = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId)));
      const unresolved = [];
      for (const item of items) {
        if (okOffers.has(cleanText(item.offerId))) okIds.add(String(item.id));
        else unresolved.push(item);
      }
      if (!unresolved.length) continue;

      // 3. For failures ask Yandex what it actually knows about these offers:
      //    - present & not archived -> just fix the local flag
      //    - truly missing -> create the card with the full offer payload
      let liveByOffer = new Map();
      try {
        const mappings = await getYandexOfferMappingsByOfferIds(shop, unresolved.map((item) => item.offerId));
        for (const mapping of mappings) {
          const offerId = cleanText(yandexOfferIdFromMapping(mapping)).toLowerCase();
          if (!offerId) continue;
          const offer = pickYandexOfferFromMapping(mapping);
          const state = pickYandexState(mapping, offer);
          liveByOffer.set(offerId, state);
        }
      } catch (error) {
        logger.warn("yandex fast unarchive live state check failed", { target, detail: error?.message || String(error) });
      }
      const missing = [];
      for (const item of unresolved) {
        const state = liveByOffer.get(cleanText(item.offerId).toLowerCase());
        if (state && state.code !== "archived" && !state.archived) {
          okIds.add(String(item.id)); // exists and active on Yandex, only our flag was stale
        } else if (!state) {
          missing.push(item);
        } else {
          failed.push(item.offerId); // exists, still archived, unarchive rejected
        }
      }
      if (missing.length) {
        const offers = missing
          .map((product) => buildYandexOfferMapping(product).offer)
          .filter((offer) => offer?.offerId);
        const createResults = await sendYandexOfferMappings(shop, offers).catch(() => []);
        const createdOffers = new Set(createResults.filter((item) => item.ok).map((item) => cleanText(item.offerId)));
        for (const item of missing) {
          if (createdOffers.has(cleanText(item.offerId))) {
            okIds.add(String(item.id));
            createdCount += 1;
          } else {
            failed.push(item.offerId);
          }
        }
      }
    }

    if (!okIds.size) {
      logger.warn("yandex fast unarchive: nothing accepted", { source, products: products.length, failedSample: failed.slice(0, 10) });
      return { status: "ok", products: products.length, unarchived: 0, failed: failed.length };
    }

    // Mark recovered locally so no-supplier automation never re-archives them, then
    // restore stock and force prices in one go.
    const now = new Date().toISOString();
    const recoveredProducts = products
      .filter((product) => okIds.has(String(product.id)))
      .map((product) => ({
        ...product,
        archived: false,
        status: "active",
        targetStock: Math.max(1, Math.round(Number(product.targetStock || 0)) || 0),
        marketplaceState: {
          ...(product.marketplaceState || {}),
          code: "active",
          status: "active",
          archived: false,
        },
        noSupplierAutomation: {
          ...(product.noSupplierAutomation || {}),
          recoveredAt: now,
          manualSellableAt: now,
          archivedAt: null,
          stockZeroAt: null,
          lastError: null,
        },
        updatedAt: now,
      }));

    await writeWarehouseProductPatch(recoveredProducts, { reason: "yandex_fast_unarchive" })
      .catch((error) => logger.warn("yandex fast unarchive persist failed", { detail: error?.message || String(error) }));

    const stockActions = await restoreStocksOnMarketplaces(recoveredProducts)
      .catch((error) => {
        logger.warn("yandex fast unarchive stock restore failed", { detail: error?.message || String(error) });
        return [];
      });

    const priceRefresh = await queueAuthoritativePriceReprice({
      productIds: recoveredProducts.map((product) => product.id),
      marketplace: "all",
      reason: "yandex_fast_unarchive",
      sourceEvent: "yandex_fast_unarchive",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: false,
      livePriceMaster: true,
      verify: true,
      priority: 1,
    }).catch((error) => {
      logger.warn("yandex fast unarchive price queue failed", { detail: error?.message || String(error) });
      return { queued: 0 };
    });

    yandexFastUnarchiveLastRunAt = new Date().toISOString();
    yandexFastUnarchiveLastResult = {
      at: yandexFastUnarchiveLastRunAt,
      products: products.length,
      unarchived: okIds.size,
      created: createdCount,
      locallyDeleted,
      failed: failed.length,
      failedSample: failed.slice(0, 30),
      stockSent: stockActions.filter((item) => item.ok).length,
      priceQueued: priceRefresh.queued || 0,
      elapsedMs: Date.now() - startedAt,
    };
    logger.info("yandex_fast_unarchive_complete", {
      source,
      products: products.length,
      unarchived: okIds.size,
      created: createdCount,
      locallyDeleted,
      failed: failed.length,
      stockSent: stockActions.filter((item) => item.ok).length,
      priceQueued: priceRefresh.queued || 0,
      elapsedMs: Date.now() - startedAt,
    });
    return { status: "ok", products: products.length, unarchived: okIds.size, created: createdCount, locallyDeleted, failed: failed.length };
  } catch (error) {
    logger.warn("yandex fast unarchive failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    yandexFastUnarchiveRunning = false;
  }
}

function scheduleYandexFastUnarchive(delayMs = yandexFastUnarchiveIntervalMs) {
  if (!yandexFastUnarchiveEnabled) {
    yandexFastUnarchiveNextRunAt = null;
    return;
  }
  if (yandexFastUnarchiveTimer) clearTimeout(yandexFastUnarchiveTimer);
  const normalizedDelay = Math.max(5_000, Number(delayMs) || yandexFastUnarchiveIntervalMs);
  yandexFastUnarchiveNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  yandexFastUnarchiveTimer = setTimeout(async () => {
    try {
      await runFastYandexBulkUnarchive({ source: "schedule" });
    } catch (error) {
      logger.warn("yandex fast unarchive tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleYandexFastUnarchive(yandexFastUnarchiveIntervalMs);
    }
  }, normalizedDelay);
  yandexFastUnarchiveTimer.unref?.();
}

// Read-only status for the Recovery page: yandex archived backlog + fast-pass state.
app.get("/api/yandex/fast-unarchive/status", requireAdmin, async (_request, response, next) => {
  try {
    const prisma = getPrisma();
    const archivedLinked = prisma
      ? await prisma.warehouseProduct.count({
        where: { marketplace: "yandex", archived: true, links: { some: {} } },
      }).catch(() => null)
      : null;
    response.json({
      ok: true,
      enabled: yandexFastUnarchiveEnabled,
      running: yandexFastUnarchiveRunning,
      intervalSeconds: Math.round(yandexFastUnarchiveIntervalMs / 1000),
      nextRunAt: yandexFastUnarchiveNextRunAt || null,
      lastRunAt: yandexFastUnarchiveLastRunAt,
      archivedLinked,
      lastResult: yandexFastUnarchiveLastResult,
    });
  } catch (error) {
    next(error);
  }
});
