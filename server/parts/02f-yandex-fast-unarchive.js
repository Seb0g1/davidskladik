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
    if (!rows.length) return { status: "ok", products: 0, unarchived: 0 };

    const products = rows.map(productFromPostgres);

    // Partition: products that must not be on Yandex at all get their local yandex row
    // deleted (no wasted API calls); products absent on Yandex get a card CREATED via
    // offer mapping instead of a pointless unarchive; the rest are bulk-unarchived.
    const toDelete = [];
    const toCreate = [];
    const toUnarchive = [];
    for (const product of products) {
      if (!cleanText(product.target) || !cleanText(product.offerId)) continue;
      const name = cleanText(product.name || product.yandex?.name || product.offerId);
      const lowerName = name.toLowerCase();
      const hasBlockedKeyword = lowerName.includes("отливант") || lowerName.includes("тестер");
      const volumeAssessment = assessYandexSmallVolume(name);
      if (hasBlockedKeyword || volumeAssessment.blocked) {
        toDelete.push(product);
        continue;
      }
      const stateCode = cleanText(product.marketplaceState?.code).toLowerCase();
      const stateRaw = cleanText(product.marketplaceState?.state).toUpperCase();
      if (stateCode === "absent" || stateRaw === "ABSENT") toCreate.push(product);
      else toUnarchive.push(product);
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

    // 2. Create cards for products absent on Yandex (full offer payload).
    for (const [target, items] of groupByTarget(toCreate).entries()) {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      const offers = items
        .map((product) => buildYandexOfferMapping(product).offer)
        .filter((offer) => offer?.offerId);
      const results = await sendYandexOfferMappings(shop, offers).catch(() => []);
      const okOffers = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId)));
      for (const item of items) {
        if (okOffers.has(cleanText(item.offerId))) okIds.add(String(item.id));
        else failed.push(item.offerId);
      }
    }

    // 3. Bulk unarchive for products that really exist in the Yandex archive.
    for (const [target, items] of groupByTarget(toUnarchive).entries()) {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      const results = await sendYandexOfferArchiveState(shop, items.map((item) => item.offerId), false);
      const okOffers = new Set(results.filter((item) => item.ok).map((item) => cleanText(item.offerId)));
      for (const item of items) {
        if (okOffers.has(cleanText(item.offerId))) okIds.add(String(item.id));
        else failed.push(item.offerId);
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

    logger.info("yandex_fast_unarchive_complete", {
      source,
      products: products.length,
      unarchived: okIds.size,
      created: toCreate.length,
      locallyDeleted,
      failed: failed.length,
      stockSent: stockActions.filter((item) => item.ok).length,
      priceQueued: priceRefresh.queued || 0,
      elapsedMs: Date.now() - startedAt,
    });
    return { status: "ok", products: products.length, unarchived: okIds.size, created: toCreate.length, locallyDeleted, failed: failed.length };
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
