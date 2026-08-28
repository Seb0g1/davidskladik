// Rolling linked-product reconciler.
//
// Walks the entire set of linked warehouse products in small, deterministic,
// cursor-advanced batches (worker-only). Each batch refreshes live marketplace
// archive/stock state, rebuilds fresh products with live PriceMaster, then reuses
// the existing supplier-recovery / no-supplier automations and target-stock send.
// It never loads the whole catalog at once and defers under load. This keeps every
// linked product fresh (out of archive, correct stock, correct price) without the
// site-killing full marketplace import.

async function refreshOzonMarketplaceStateForProducts(products = []) {
  const candidates = (Array.isArray(products) ? products : [])
    .filter((product) => product?.marketplace === "ozon" && product.offerId && product.target);
  if (!candidates.length) return new Map();
  const updatedById = new Map();
  for (const account of getOzonAccounts()) {
    const accountProducts = candidates.filter((product) => matchesOzonTarget(product.target, account.id));
    if (!accountProducts.length) continue;
    const offerIds = accountProducts.map((product) => product.offerId);
    try {
      const [infoMap, stockMap, priceMap] = await Promise.all([
        getOzonProductInfoMap(offerIds, account, { continueOnError: true }),
        getOzonStockMap(offerIds, account, { continueOnError: true }),
        getOzonPriceMap(offerIds, account, { continueOnError: true }),
      ]);
      const missingProductIds = accountProducts
        .filter((product) => !getOzonOfferMapValue(infoMap, product.offerId))
        .map((product) => cleanText(product.productId || product.ozon?.productId))
        .filter(Boolean);
      if (missingProductIds.length) {
        const infoByProductId = await getOzonProductInfoMapByProductIds(missingProductIds, account, { continueOnError: true });
        for (const product of accountProducts) {
          const info = infoByProductId.get(cleanText(product.productId || product.ozon?.productId));
          if (info) setOzonOfferMapValue(infoMap, product.offerId || info.offer_id || info.offerId, info);
        }
      }
      for (const product of accountProducts) {
        const info = getOzonOfferMapValue(infoMap, product.offerId) || {};
        const stockInfo = getOzonOfferMapValue(stockMap, product.offerId) || {};
        const priceInfo = getOzonOfferMapValue(priceMap, product.offerId) || {};
        if (!Object.keys(info).length && !Object.keys(stockInfo).length && !Object.keys(priceInfo).length) continue;
        updatedById.set(product.id, applyOzonInfoToWarehouseProduct(product, info, account, stockInfo, priceInfo));
      }
    } catch (error) {
      logger.warn("linked reconciler ozon state refresh failed", {
        account: account.id,
        detail: error?.message || String(error),
      });
    }
  }
  return updatedById;
}

function mergeYandexLiveMarketplaceState(previous = {}, liveState = {}) {
  const state = liveState && typeof liveState === "object" ? liveState : {};
  const current = normalizeMarketplaceState(previous || {});
  return normalizeMarketplaceState({
    stock: current.stock,
    present: current.present,
    reserved: current.reserved,
    warehouses: current.warehouses,
    hasStocks: current.hasStocks,
    ...state,
    archived: state.archived !== undefined ? state.archived : state.code === "archived",
  });
}

async function refreshYandexMarketplaceStateForProducts(products = []) {
  const candidates = (Array.isArray(products) ? products : [])
    .filter((product) => product?.marketplace === "yandex" && product.offerId && product.target);
  if (!candidates.length) return new Map();
  const updatedById = new Map();
  const byTarget = new Map();
  for (const product of candidates) {
    if (!byTarget.has(product.target)) byTarget.set(product.target, []);
    byTarget.get(product.target).push(product);
  }
  for (const [target, items] of byTarget.entries()) {
    const shop = getYandexShopByTarget(target);
    if (!shop) continue;
    const offerIds = items.map((product) => product.offerId);
    try {
      const mappings = await getYandexOfferMappingsByOfferIds(shop, offerIds);
      const stateByOfferId = new Map();
      for (const item of mappings) {
        const offerId = yandexOfferIdFromMapping(item).toLowerCase();
        if (!offerId) continue;
        const offer = pickYandexOfferFromMapping(item);
        stateByOfferId.set(offerId, pickYandexState(item, offer));
      }
      for (const product of items) {
        // If the offerId is absent from the response entirely (no error), the offer
        // no longer exists in the business's Yandex catalog at all. Treat it the
        // same as "archived" so it stops showing as a live Yandex listing and gets
        // picked up by the yandex-fast-unarchive recovery/recreate job.
        const state = stateByOfferId.get(cleanText(product.offerId).toLowerCase())
          || { code: "archived", label: "Удалён на Яндексе", stateName: "Отсутствует в каталоге ЯМ" };
        updatedById.set(product.id, normalizeWarehouseProduct({
          ...product,
          marketplaceState: mergeYandexLiveMarketplaceState(product.marketplaceState, state),
        }));
      }
    } catch (error) {
      logger.warn("linked reconciler yandex state refresh failed", {
        target: shop.id,
        detail: error?.message || String(error),
      });
    }
  }
  return updatedById;
}

// Refresh live marketplace archive/stock state for an explicit batch so that
// productLooksArchived() and marketplaceState.stock reflect marketplace truth.
async function refreshMarketplaceStateForProducts(products = []) {
  const list = Array.isArray(products) ? products : [];
  if (!list.length) return list;
  const [ozonUpdates, yandexUpdates] = await Promise.all([
    refreshOzonMarketplaceStateForProducts(list),
    refreshYandexMarketplaceStateForProducts(list),
  ]);
  const updatedById = new Map([...ozonUpdates, ...yandexUpdates]);
  if (!updatedById.size) return list;
  try {
    await writeWarehouseProductPatch(Array.from(updatedById.values()), {
      reason: "linked_reconciler_state_refresh",
      writeLinks: false,
    });
  } catch (error) {
    logger.warn("linked reconciler state refresh persist failed", { detail: error?.message || String(error) });
  }
  return list.map((product) => updatedById.get(product.id) || product);
}

// Load the next page of linked products using a deterministic keyset cursor over id.
async function loadNextLinkedReconcilerBatch(state = {}) {
  if (!shouldUsePostgresStorage()) return { products: [], nextCursorId: null, cycleComplete: true, postgres: false };
  const prisma = getPrisma();
  if (!prisma) return { products: [], nextCursorId: null, cycleComplete: true, postgres: false };
  const lastProductId = cleanText(state.lastProductId);
  const query = {
    where: { AND: [enabledWarehouseTargetWhere(), { links: { some: {} } }] },
    include: { links: true },
    orderBy: { id: "asc" },
    take: linkedReconcilerBatchSize,
  };
  if (lastProductId) {
    query.cursor = { id: lastProductId };
    query.skip = 1;
  }
  const rows = await prisma.warehouseProduct.findMany(query);
  const products = rows.map(productFromPostgres);
  const cycleComplete = rows.length < linkedReconcilerBatchSize;
  const nextCursorId = rows.length ? String(rows[rows.length - 1].id) : null;
  return { products, nextCursorId, cycleComplete, postgres: true };
}

// Compare the marketplaceState.code our DB had for each product (before this batch's live
// refresh) against the code the live refresh just produced. A mismatch means our stored
// state had drifted from the marketplace truth — this is the "Детектор расхождений с МП"
// signal surfaced on the dashboard (PLAN-HARDENING.md "Высокий": marketplace-data-correctness).
function countMarketplaceStateMismatches(beforeProducts = [], afterProducts = []) {
  const beforeCodeById = new Map(
    (Array.isArray(beforeProducts) ? beforeProducts : [])
      .filter((product) => product?.id)
      .map((product) => [String(product.id), product.marketplaceState?.code || null]),
  );
  const productIds = [];
  // wronglyHidden = the ACTIONABLE subset: our DB had the product as non-active (out_of_stock/
  // archived/inactive) but the live marketplace says it's active — i.e. we were hiding a
  // sellable product (lost sales). The opposite direction (active -> out_of_stock) is just
  // normal stock depletion the reconciler is catching up on, so it's tracked but not alerted.
  const wronglyHiddenIds = [];
  for (const product of (Array.isArray(afterProducts) ? afterProducts : [])) {
    if (!product?.id) continue;
    const id = String(product.id);
    if (!beforeCodeById.has(id)) continue;
    const beforeCode = beforeCodeById.get(id);
    const afterCode = product.marketplaceState?.code || null;
    if (beforeCode !== afterCode) {
      productIds.push(id);
      if (beforeCode !== "active" && afterCode === "active") wronglyHiddenIds.push(id);
    }
  }
  return { count: productIds.length, productIds, wronglyHidden: wronglyHiddenIds.length, wronglyHiddenIds };
}

async function processLinkedReconcilerBatch(seedProducts = []) {
  const ids = Array.from(new Set(seedProducts.map((product) => cleanText(product.id)).filter(Boolean)));
  if (!ids.length) return { products: 0, recovered: 0, unarchived: 0, zeroStockSent: 0, stockSent: 0, priceQueued: 0 };

  // Build fresh products with live PriceMaster (hydrates ids from Postgres + applies pricing).
  // Фазовые маркеры reconciler_* — бисекция остаточных 7-10 с блокировок.
  let closePhaseMarker = setEventLoopBlockMarker("reconciler_build_prices");
  let products;
  try {
    products = await buildFreshWarehouseProducts(ids, {
      livePriceMaster: true,
      refreshPrices: true,
      batchPriceMaster: true,
      persistMutations: true,
      priceMasterTimeoutMs: autoPricePmTimeoutMs,
    });
  } finally {
    closePhaseMarker();
  }

  // Refresh live marketplace archive/stock state.
  // CRITICAL: capture the return value — it contains fresh archived/stock data even when
  // the internal DB-persist step fails (persist errors are caught-and-logged inside).
  closePhaseMarker = setEventLoopBlockMarker("reconciler_state_refresh");
  let refreshed;
  try {
    refreshed = await refreshMarketplaceStateForProducts(products);
  } finally {
    closePhaseMarker();
  }
  const stateMismatches = countMarketplaceStateMismatches(products, refreshed);
  const liveStateById = new Map(
    (Array.isArray(refreshed) ? refreshed : [])
      .filter((p) => p?.id)
      .map((p) => [String(p.id), p.marketplaceState]),
  );

  // Rebuild with live PriceMaster to get fresh supplier prices, then overlay the live
  // marketplace state captured above so archived/stock flags are always current regardless
  // of whether the DB persist inside refreshMarketplaceStateForProducts succeeded.
  closePhaseMarker = setEventLoopBlockMarker("reconciler_rebuild");
  let rebuilt;
  try {
    rebuilt = await buildFreshWarehouseProducts(ids, {
      livePriceMaster: true,
      refreshPrices: false,
      batchPriceMaster: true,
      persistMutations: true,
      priceMasterTimeoutMs: autoPricePmTimeoutMs,
    });
  } finally {
    closePhaseMarker();
  }
  products = rebuilt.map((p) => {
    const liveState = liveStateById.get(String(p.id));
    return liveState ? { ...p, marketplaceState: liveState } : p;
  });

  // Supplier recovery: unarchive (Yandex immediate / Ozon queued on quota), restore stock,
  // requeue price. Reuses the existing automation verbatim, scoped to this batch.
  closePhaseMarker = setEventLoopBlockMarker("reconciler_recovery");
  let recovery;
  try {
    recovery = await runSupplierRecoveryAutomation(
      { products },
      { productIds: ids, source: "linked_reconciler", sourceEvent: "linked_reconciler", forceOzonDailyLimit: true },
    );
  } finally {
    closePhaseMarker();
  }

  // No-supplier automation: supplier fallback / stock-only ("Наш склад") / zero stock.
  // includeNoLinks:false because the reconciler scope is strictly linked products.
  const automation = await runNoSupplierMarketplaceAutomation(
    { products },
    { productIds: ids, includeNoLinks: false, source: "linked_reconciler" },
  );

  // Products in archive that have a linked supplier need force:true so the price is sent
  // the moment they are unarchived, not just when it drifts from the stale marketplace value.
  const archivedLinkedIds = products
    .filter((p) => productLooksArchived(p) && p.hasLinks && p.selectedSupplier && !warehouseProductUsesStockOnlyPricing(p))
    .map((p) => p.id);
  const normalPriceIds = ids.filter((id) => !archivedLinkedIds.includes(id));

  let priceQueued = 0;
  let priceQueuedBatches = 0;

  // Force-send price for archived+linked products (price must arrive on unarchive).
  if (archivedLinkedIds.length) {
    const forcedRefresh = await queueAuthoritativePriceReprice({
      productIds: archivedLinkedIds,
      marketplace: "all",
      reason: "linked_reconciler_archive_force",
      sourceEvent: "linked_reconciler",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_IMMEDIATE,
    }).catch((error) => {
      logger.warn("linked reconciler archive force price queue failed", { detail: error?.message || String(error), products: archivedLinkedIds.length });
      return { queued: 0, queuedBatches: 0 };
    });
    priceQueued += forcedRefresh.queued || 0;
    priceQueuedBatches += forcedRefresh.queuedBatches || 0;
  }

  // Normal changed-price detection for active linked products.
  if (normalPriceIds.length) {
    const priceRefresh = await queueAuthoritativePriceReprice({
      productIds: normalPriceIds,
      marketplace: "all",
      reason: "linked_reconciler",
      sourceEvent: "linked_reconciler",
      force: false,
      onlyChanged: true,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_BACKGROUND,
    }).catch((error) => {
      logger.warn("linked reconciler price queue failed", { detail: error?.message || String(error), products: normalPriceIds.length });
      return { queued: 0, queuedBatches: 0 };
    });
    priceQueued += priceRefresh.queued || 0;
    priceQueuedBatches += priceRefresh.queuedBatches || 0;
  }

  // Replenish target stock for sellable products whose marketplace stock drifted
  // (e.g. depleted by same-day orders while still in stock at the supplier).
  let stockSent = 0;
  if (linkedReconcilerSendTargetStock) {
    const stockProducts = pickTargetStockSendProducts(products);
    if (stockProducts.length) {
      const stockActions = await sendTargetStocksToMarketplace(stockProducts);
      stockSent = stockActions.filter((item) => item.ok).length;
    }
  }

  return {
    products: ids.length,
    recovered: recovery.recovered || 0,
    unarchived: recovery.unarchived || 0,
    zeroStockSent: automation.zeroStockSent || 0,
    stockSent,
    archivedLinked: archivedLinkedIds.length,
    priceQueued,
    priceQueuedBatches,
    stateMismatches: stateMismatches.count,
    wronglyHidden: stateMismatches.wronglyHidden,
  };
}

async function runLinkedReconcilerBatch(trigger = "rolling") {
  if (linkedReconcilerRunning) return { status: "already_running" };
  if (manualWarehouseSyncPromise) return { status: "manual_sync_running" };
  if (heavyBackgroundWorkShouldDefer(`linked_reconciler:${trigger}`)) {
    return { status: "deferred_under_load" };
  }
  if (!shouldUsePostgresStorage() || !getPrisma()) {
    return { status: "postgres_disabled" };
  }

  linkedReconcilerRunning = true;
  const startedAt = new Date().toISOString();
  const maxBatches = Math.max(1, Math.ceil(linkedReconcilerMaxProductsPerTick / linkedReconcilerBatchSize));
  const totals = { batches: 0, products: 0, recovered: 0, unarchived: 0, zeroStockSent: 0, stockSent: 0, priceQueued: 0, priceQueuedBatches: 0, cyclesCompleted: 0, stateMismatches: 0, wronglyHidden: 0 };
  let lastError = null;
  try {
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const state = await readLinkedReconcilerState();
      if (!state.cycleStartedAt) state.cycleStartedAt = startedAt;
      const { products, nextCursorId, cycleComplete, postgres } = await loadNextLinkedReconcilerBatch(state);
      if (!postgres) break;

      let result = null;
      if (products.length) {
        try {
          // Маркер для event_loop_blocked: батч реконсайлера — главный
          // подозреваемый в 9-10с блокировках (heap-качели ~1 ГБ за цикл).
          const closeMarker = setEventLoopBlockMarker("linked_reconciler_batch");
          try {
            result = await processLinkedReconcilerBatch(products);
          } finally {
            closeMarker();
          }
          totals.batches += 1;
          totals.products += result.products;
          totals.recovered += result.recovered;
          totals.unarchived += result.unarchived;
          totals.zeroStockSent += result.zeroStockSent;
          totals.stockSent += result.stockSent;
          totals.priceQueued += result.priceQueued || 0;
          totals.priceQueuedBatches += result.priceQueuedBatches || 0;
          totals.stateMismatches += result.stateMismatches || 0;
          totals.wronglyHidden += result.wronglyHidden || 0;
        } catch (error) {
          lastError = error?.message || String(error);
          logger.warn("linked reconciler batch failed", { trigger, detail: lastError });
        }
      }

      const cycleStateMismatches = Number(state.cycleStateMismatches || 0) + (result?.stateMismatches || 0);
      const cycleWronglyHidden = Number(state.cycleWronglyHidden || 0) + (result?.wronglyHidden || 0);

      if (cycleComplete) {
        await writeLinkedReconcilerState({
          lastProductId: null,
          cycleStartedAt: null,
          cyclesCompleted: Number(state.cyclesCompleted || 0) + 1,
          processedThisCycle: 0,
          processedTotal: Number(state.processedTotal || 0) + products.length,
          lastBatchAt: new Date().toISOString(),
          lastError,
          cycleStateMismatches: 0,
          lastCycleStateMismatches: cycleStateMismatches,
          cycleWronglyHidden: 0,
          lastCycleWronglyHidden: cycleWronglyHidden,
          lastCycleMismatchAt: new Date().toISOString(),
        });
        totals.cyclesCompleted += 1;
        break; // finished a full pass; next tick starts a new cycle
      }

      await writeLinkedReconcilerState({
        lastProductId: nextCursorId,
        cycleStartedAt: state.cycleStartedAt || startedAt,
        cyclesCompleted: Number(state.cyclesCompleted || 0),
        processedThisCycle: Number(state.processedThisCycle || 0) + products.length,
        processedTotal: Number(state.processedTotal || 0) + products.length,
        lastBatchAt: new Date().toISOString(),
        lastError,
        cycleStateMismatches,
        cycleWronglyHidden,
      });
    }

    logger.info("linked_reconciler_complete", { trigger, ...totals, lastError });
    if (totals.zeroStockSent >= 10) {
      sendTelegramAlert?.("mass_zero_stock", `📦 Реконсайлер: ${totals.zeroStockSent} товаров обнулили остаток за один тик.\nВозможно, поставщик исчез из PM или массово потерял товары.`).catch(() => {});
    }
    return { status: "ok", trigger, ...totals, lastError };
  } finally {
    linkedReconcilerRunning = false;
  }
}

// Dedicated recovery pass for archived+linked Ozon products.
// Runs after every normal reconciler tick so archived Ozon products don't have to wait for
// the rolling cursor to reach them (which can take hours across a large catalog).
// Uses forceOzonDailyLimit:true (reconciler bypass) so the counter is never inflated.
async function runArchivedOzonLinkedRecoveryPass() {
  if (linkedReconcilerRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma) return { status: "no_db" };
  linkedReconcilerRunning = true;
  const maxBatches = Math.max(1, Number(process.env.OZON_RECOVERY_MAX_BATCHES_PER_TICK || 3) || 3);
  const totals = { products: 0, recovered: 0, unarchived: 0, batches: 0 };
  try {
    let lastId = null;
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const rows = await prisma.warehouseProduct.findMany({
        where: {
          marketplace: "ozon",
          archived: true,
          links: { some: {} },
          ...(lastId ? { id: { gt: lastId } } : {}),
        },
        include: { links: true },
        orderBy: { id: "asc" },
        take: linkedReconcilerBatchSize * 2,
      }).catch(() => []);
      if (!rows.length) break;
      lastId = String(rows[rows.length - 1].id);
      const products = rows.map(productFromPostgres);
      const result = await processLinkedReconcilerBatch(products);
      totals.batches += 1;
      totals.products += result.products || 0;
      totals.recovered += result.recovered || 0;
      totals.unarchived += result.unarchived || 0;
      if (rows.length < linkedReconcilerBatchSize * 2) break;
    }
    if (totals.products > 0) {
      logger.info("ozon_archived_recovery_complete", totals);
    }
    return { status: "ok", ...totals };
  } catch (error) {
    logger.warn("ozon archived recovery pass failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message };
  } finally {
    linkedReconcilerRunning = false;
  }
}

// Dedicated recovery pass for archived+linked Yandex products.
// The rolling reconciler cursor walks products alphabetically (ozon- before yandex-), so
// it takes hours to reach Yandex products after a restart. This pass runs after every normal
// reconciler tick and immediately processes up to 2 batches of archived Yandex products.
async function runArchivedYandexLinkedRecoveryPass() {
  if (linkedReconcilerRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma) return { status: "no_db" };
  linkedReconcilerRunning = true;
  // Drain mode: Yandex has no unarchive daily limit, so keep pulling batches until the
  // archived backlog is empty (bounded per tick to keep the event loop responsive).
  const maxBatches = Math.max(1, Number(process.env.YANDEX_RECOVERY_MAX_BATCHES_PER_TICK || 6) || 6);
  const totals = { products: 0, recovered: 0, unarchived: 0, batches: 0 };
  try {
    let lastId = null;
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const rows = await prisma.warehouseProduct.findMany({
        where: {
          marketplace: "yandex",
          archived: true,
          links: { some: {} },
          ...(lastId ? { id: { gt: lastId } } : {}),
        },
        include: { links: true },
        orderBy: { id: "asc" },
        take: linkedReconcilerBatchSize * 2,
      }).catch(() => []);
      if (!rows.length) break;
      lastId = String(rows[rows.length - 1].id);
      const products = rows.map(productFromPostgres);
      const result = await processLinkedReconcilerBatch(products);
      totals.batches += 1;
      totals.products += result.products || 0;
      totals.recovered += result.recovered || 0;
      totals.unarchived += result.unarchived || 0;
      if (rows.length < linkedReconcilerBatchSize * 2) break;
    }
    if (totals.products > 0) {
      logger.info("yandex_archived_recovery_complete", totals);
    }
    return { status: "ok", ...totals };
  } catch (error) {
    logger.warn("yandex archived recovery pass failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message };
  } finally {
    linkedReconcilerRunning = false;
  }
}

function scheduleLinkedReconciler(delayMs = null) {
  if (!linkedReconcilerEnabled) {
    linkedReconcilerNextRunAt = null;
    return;
  }
  if (linkedReconcilerTimer) clearTimeout(linkedReconcilerTimer);
  const intervalMs = linkedReconcilerIntervalMinutes * 60 * 1000;
  const normalizedDelay = Math.max(30_000, Number(delayMs ?? intervalMs) || intervalMs);
  linkedReconcilerNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  linkedReconcilerTimer = setTimeout(async () => {
    let nextDelayMs = intervalMs;
    try {
      const result = await runLinkedReconcilerBatch("rolling");
      if (result?.status && String(result.status).startsWith("deferred_")) {
        nextDelayMs = linkedReconcilerDeferRetryMs;
      }
      // After the normal rolling pass, run dedicated archived-product recovery passes so
      // archived Ozon and Yandex products are processed every tick regardless of cursor position.
      await runArchivedOzonLinkedRecoveryPass().catch((error) => {
        logger.warn("ozon archived recovery pass error", { detail: error?.message || String(error) });
      });
      await runArchivedYandexLinkedRecoveryPass().catch((error) => {
        logger.warn("yandex archived recovery pass error", { detail: error?.message || String(error) });
      });
    } catch (error) {
      logger.error("linked reconciler tick failed", { detail: error?.message || String(error), err: error });
    } finally {
      scheduleLinkedReconciler(nextDelayMs);
    }
  }, normalizedDelay);
  linkedReconcilerTimer.unref?.();
}
