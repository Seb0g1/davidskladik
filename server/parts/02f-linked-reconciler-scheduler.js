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
        const state = stateByOfferId.get(cleanText(product.offerId).toLowerCase());
        if (!state) continue;
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

async function processLinkedReconcilerBatch(seedProducts = []) {
  const ids = Array.from(new Set(seedProducts.map((product) => cleanText(product.id)).filter(Boolean)));
  if (!ids.length) return { products: 0, recovered: 0, unarchived: 0, zeroStockSent: 0, stockSent: 0, priceQueued: 0 };

  // Build fresh products with live PriceMaster (hydrates ids from Postgres + applies pricing).
  let products = await buildFreshWarehouseProducts(ids, {
    livePriceMaster: true,
    refreshPrices: true,
    batchPriceMaster: true,
    persistMutations: true,
  });

  // Refresh live marketplace archive/stock state.
  // CRITICAL: capture the return value — it contains fresh archived/stock data even when
  // the internal DB-persist step fails (persist errors are caught-and-logged inside).
  const refreshed = await refreshMarketplaceStateForProducts(products);
  const liveStateById = new Map(
    (Array.isArray(refreshed) ? refreshed : [])
      .filter((p) => p?.id)
      .map((p) => [String(p.id), p.marketplaceState]),
  );

  // Rebuild with live PriceMaster to get fresh supplier prices, then overlay the live
  // marketplace state captured above so archived/stock flags are always current regardless
  // of whether the DB persist inside refreshMarketplaceStateForProducts succeeded.
  const rebuilt = await buildFreshWarehouseProducts(ids, {
    livePriceMaster: true,
    refreshPrices: false,
    batchPriceMaster: true,
    persistMutations: true,
  });
  products = rebuilt.map((p) => {
    const liveState = liveStateById.get(String(p.id));
    return liveState ? { ...p, marketplaceState: liveState } : p;
  });

  // Supplier recovery: unarchive (Yandex immediate / Ozon queued on quota), restore stock,
  // requeue price. Reuses the existing automation verbatim, scoped to this batch.
  const recovery = await runSupplierRecoveryAutomation(
    { products },
    { productIds: ids, source: "linked_reconciler", sourceEvent: "linked_reconciler" },
  );

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
      priority: 1,
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
      priority: 2,
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
  const totals = { batches: 0, products: 0, recovered: 0, unarchived: 0, zeroStockSent: 0, stockSent: 0, priceQueued: 0, priceQueuedBatches: 0, cyclesCompleted: 0 };
  let lastError = null;
  try {
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const state = await readLinkedReconcilerState();
      if (!state.cycleStartedAt) state.cycleStartedAt = startedAt;
      const { products, nextCursorId, cycleComplete, postgres } = await loadNextLinkedReconcilerBatch(state);
      if (!postgres) break;

      if (products.length) {
        try {
          const result = await processLinkedReconcilerBatch(products);
          totals.batches += 1;
          totals.products += result.products;
          totals.recovered += result.recovered;
          totals.unarchived += result.unarchived;
          totals.zeroStockSent += result.zeroStockSent;
          totals.stockSent += result.stockSent;
          totals.priceQueued += result.priceQueued || 0;
          totals.priceQueuedBatches += result.priceQueuedBatches || 0;
        } catch (error) {
          lastError = error?.message || String(error);
          logger.warn("linked reconciler batch failed", { trigger, detail: lastError });
        }
      }

      if (cycleComplete) {
        await writeLinkedReconcilerState({
          lastProductId: null,
          cycleStartedAt: null,
          cyclesCompleted: Number(state.cyclesCompleted || 0) + 1,
          processedThisCycle: 0,
          processedTotal: Number(state.processedTotal || 0) + products.length,
          lastBatchAt: new Date().toISOString(),
          lastError,
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
      });
    }

    logger.info("linked_reconciler_complete", { trigger, ...totals, lastError });
    return { status: "ok", trigger, ...totals, lastError };
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
    } catch (error) {
      logger.error("linked reconciler tick failed", { detail: error?.message || String(error), err: error });
    } finally {
      scheduleLinkedReconciler(nextDelayMs);
    }
  }, normalizedDelay);
  linkedReconcilerTimer.unref?.();
}
