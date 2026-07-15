// Квота-проба: локальный суточный счётчик может опережать реальный счётчик Ozon
// (повторные unarchive уже активных товаров, ложные срабатывания «лимита» на
// generic-ошибках) — день заканчивался на 80–90 реальных разархивациях при
// локальных 100. Когда локальная квота исчерпана, а due-товары остались,
// периодически шлём малый батч в обход локального гейта: настоящий ответ Ozon —
// единственный надёжный сигнал. Отказ Ozon (daily limit) откладывает всё на
// завтра штатной веткой и выключает пробы до конца суток.
const ozonUnarchiveQuotaProbeLimit = Math.max(1, Math.min(50, Number(process.env.OZON_UNARCHIVE_QUOTA_PROBE_LIMIT || 10) || 10));
const ozonUnarchiveQuotaProbeIntervalMs = Math.max(5 * 60_000, Number(process.env.OZON_UNARCHIVE_QUOTA_PROBE_INTERVAL_MINUTES || 45) * 60_000 || 45 * 60_000);
const ozonUnarchiveQuotaProbeState = new Map(); // target -> { dateKey, lastProbeAt, rejected }

function ozonUnarchiveQuotaProbeAllowed(target) {
  const dateKey = ozonUnarchiveDateKey();
  const state = ozonUnarchiveQuotaProbeState.get(target);
  if (!state || state.dateKey !== dateKey) return true;
  if (state.rejected) return false;
  return Date.now() - (state.lastProbeAt || 0) >= ozonUnarchiveQuotaProbeIntervalMs;
}

function markOzonUnarchiveQuotaProbe(target, patch = {}) {
  const dateKey = ozonUnarchiveDateKey();
  const state = ozonUnarchiveQuotaProbeState.get(target);
  const base = state && state.dateKey === dateKey ? state : { dateKey, lastProbeAt: 0, rejected: false };
  ozonUnarchiveQuotaProbeState.set(target, { ...base, ...patch, dateKey });
}

async function resolveOzonUnarchiveQueueProducts(dueItems = []) {
  const items = Array.isArray(dueItems) ? dueItems : [];
  const directIds = Array.from(new Set(items
    .map((item) => cleanText(item.warehouseProductId || item.id))
    .filter(Boolean)));
  const resolved = new Set();
  const directFound = new Set();
  const rewrites = new Map();
  let fallbackWarehouse = null;
  const rewriteKey = (item) => ozonUnarchiveQueueKey(item) || `${cleanText(item.target)}:${cleanText(item.id)}:${cleanText(item.offerId)}`;
  const addResolved = (id, item = {}, product = {}) => {
    const productId = cleanText(id);
    if (!productId) return;
    resolved.add(productId);
    const queuedId = cleanText(item.warehouseProductId || item.id);
    if (queuedId && queuedId !== productId) {
      rewrites.set(rewriteKey(item), {
        item,
        product: {
          id: productId,
          warehouseProductId: productId,
          marketplace: "ozon",
          target: cleanText(product.target || item.target),
          offerId: cleanText(product.offerId || item.offerId),
          productId: cleanText(product.productId || item.productId || item.ozonProductId),
          ozonProductId: cleanText(product.productId || item.ozonProductId || item.productId),
        },
      });
    }
  };

  if (directIds.length && shouldUsePostgresStorage() && getPrisma()) {
    const rows = await getPrisma().warehouseProduct.findMany({
      where: { id: { in: directIds } },
      select: { id: true },
    });
    for (const row of rows) {
      const id = cleanText(row.id);
      if (id) {
        addResolved(id);
        directFound.add(id);
      }
    }
  } else {
    fallbackWarehouse = await readWarehouse();
    const existingById = new Map((fallbackWarehouse.products || [])
      .map((product) => [cleanText(product.id), product])
      .filter(([id]) => id));
    for (const id of directIds) {
      if (!existingById.has(id)) continue;
      addResolved(id, {}, existingById.get(id));
      directFound.add(id);
    }
  }

  const unresolved = items.filter((item) => {
    const id = cleanText(item.warehouseProductId || item.id);
    return !id || !directFound.has(id);
  });
  const offerIds = Array.from(new Set(unresolved.map((item) => cleanText(item.offerId)).filter(Boolean)));
  if (!offerIds.length) return { ids: Array.from(resolved), rewrites: Array.from(rewrites.values()) };

  const targets = Array.from(new Set(unresolved.map((item) => cleanText(item.target)).filter(Boolean)));
  const matchKey = (target, offerId) => `${cleanText(target).toLowerCase()}:${cleanText(offerId).toLowerCase()}`;
  const wanted = new Map();
  for (const item of unresolved) {
    const key = matchKey(item.target, item.offerId);
    if (key !== ":" && !wanted.has(key)) wanted.set(key, item);
  }

  if (shouldUsePostgresStorage() && getPrisma()) {
    for (const chunk of chunkArray(offerIds, 500)) {
      const rows = await getPrisma().warehouseProduct.findMany({
        where: {
          marketplace: "ozon",
          offerId: { in: chunk },
          ...(targets.length ? { target: { in: targets } } : {}),
        },
        select: { id: true, offerId: true, target: true, productId: true },
      });
      for (const row of rows) {
        const key = matchKey(row.target, row.offerId);
        if (wanted.has(key)) addResolved(cleanText(row.id), wanted.get(key), row);
      }
    }
    return { ids: Array.from(resolved).filter(Boolean), rewrites: Array.from(rewrites.values()) };
  }

  const warehouse = fallbackWarehouse || await readWarehouse();
  for (const product of warehouse.products || []) {
    if (cleanText(product.marketplace).toLowerCase() !== "ozon") continue;
    const key = matchKey(product.target, product.offerId);
    if (wanted.has(key) && product.id) addResolved(String(product.id), wanted.get(key), product);
  }
  return { ids: Array.from(resolved).filter(Boolean), rewrites: Array.from(rewrites.values()) };
}

async function rewriteResolvedOzonUnarchiveQueueItems(rewrites = []) {
  const items = Array.isArray(rewrites) ? rewrites : [];
  if (!items.length) return;
  let queue = await readOzonUnarchiveQueue();
  queue = removeOzonUnarchiveQueueItems(queue, items.map((item) => item.item));
  queue = queueOzonUnarchiveItems(
    queue,
    items.map((item) => item.product),
    {
      nextRetryAt: new Date(Date.now() - 1000).toISOString(),
      warning: "resolved_legacy_queue_id",
      attempted: false,
    },
  );
  await writeOzonUnarchiveQueue(queue);
}

async function processOzonUnarchiveQueue({ source = "manual", limit = ozonUnarchiveQueueBatchLimit, force = false, queueRunId = "" } = {}) {
  if (ozonUnarchiveQueueAutoRunning) {
    return {
      ok: true,
      skipped: true,
      reason: "already_running",
      selected: 0,
      result: { recovered: 0, unarchivePending: 0 },
      queue: ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 }),
      ...ozonUnarchiveQueueAutomationPublic(),
    };
  }
  ozonUnarchiveQueueProcessQueued = false;
  ozonUnarchiveQueueAutoRunning = true;
  const startedAt = new Date().toISOString();
  try {
    const normalizedLimit = Math.max(1, Math.min(5000, Math.round(Number(limit || ozonUnarchiveQueueBatchLimit) || ozonUnarchiveQueueBatchLimit)));
    const queue = await readOzonUnarchiveQueue();
    const publicQueue = ozonUnarchiveQueuePublic(queue, { limit: 5000 });
    // Remaining Ozon daily unarchive quota per target (dailyLimit − dailyUsed). Selecting past
    // it is wasted work: the send core defers over-quota items to the next window anyway.
    const availableByTarget = new Map();
    for (const item of publicQueue.items || []) {
      const target = cleanText(item.target) || "default";
      if (availableByTarget.has(target)) continue;
      const available = force || item.availableToday === null || item.availableToday === undefined
        ? Infinity
        : Math.max(0, Number(item.availableToday) || 0);
      availableByTarget.set(target, available);
    }
    let totalAvailable = Array.from(availableByTarget.values())
      .reduce((sum, available) => sum + Math.min(normalizedLimit, available), 0);
    // Локальная квота исчерпана, но due-товары остались — квота-проба: малый батч
    // в обход локального гейта, реальный ответ Ozon решает. Активируется только
    // когда исчерпаны ВСЕ targets, чтобы force-обход не пересылал лишнее по
    // target'у, у которого квота ещё есть.
    let quotaProbe = false;
    const quotaProbeTargets = new Set();
    if (!force && totalAvailable === 0) {
      const dueTargets = new Set((publicQueue.items || [])
        .filter((item) => item.due)
        .map((item) => cleanText(item.target) || "default"));
      for (const [target, available] of availableByTarget.entries()) {
        if (available > 0 || !dueTargets.has(target)) continue;
        if (!ozonUnarchiveQuotaProbeAllowed(target)) continue;
        availableByTarget.set(target, ozonUnarchiveQuotaProbeLimit);
        markOzonUnarchiveQuotaProbe(target, { lastProbeAt: Date.now() });
        quotaProbeTargets.add(target);
        quotaProbe = true;
      }
      if (quotaProbe) {
        totalAvailable = Array.from(availableByTarget.values())
          .reduce((sum, available) => sum + Math.min(normalizedLimit, available), 0);
      }
    }
    const effectiveLimit = Math.min(normalizedLimit, totalAvailable, quotaProbe ? ozonUnarchiveQuotaProbeLimit : Infinity);
    // Over-select candidates (1.5× + 20 buffer) to ensure we still get effectiveLimit resolved
    // items even when some queue entries are ghosts (deleted warehouse products) or need UUID
    // rewrite (offerId changed). After resolution we trim back to effectiveLimit.
    const candidateLimit = Math.min(5000, Math.ceil(Math.max(1, effectiveLimit) * 1.5) + 20);
    const perTargetTaken = new Map();
    const dueItems = [];
    // Due items whose target has no daily quota left. Without a bulk defer they stay due and the
    // auto scheduler keeps re-running every second, burning heavy product builds for nothing.
    const quotaExhaustedItems = [];
    for (const item of publicQueue.items || []) {
      if (!item.due) continue;
      const target = cleanText(item.target) || "default";
      const available = availableByTarget.get(target);
      if (!(available > 0)) {
        quotaExhaustedItems.push(item);
        continue;
      }
      if (dueItems.length >= candidateLimit) continue;
      const taken = perTargetTaken.get(target) || 0;
      const targetCandidateLimit = available === Infinity
        ? candidateLimit
        : Math.min(candidateLimit, Math.ceil(Math.min(normalizedLimit, available) * 1.5) + 20);
      if (taken >= targetCandidateLimit) continue;
      dueItems.push(item);
      perTargetTaken.set(target, taken + 1);
    }
    if (!force && quotaExhaustedItems.length) {
      try {
        // Пока Ozon сам не подтвердил исчерпание суточного лимита, откладываем не на
        // завтра, а до следующего окна квота-пробы: локальный счётчик может опережать
        // реальный, и без due-товаров проба никогда не сработает.
        const todayKey = ozonUnarchiveDateKey();
        const probeStillPossible = Array.from(new Set(quotaExhaustedItems
          .map((item) => cleanText(item.target) || "default")))
          .some((target) => {
            const state = ozonUnarchiveQuotaProbeState.get(target);
            return !(state && state.dateKey === todayKey && state.rejected);
          });
        const deferRetryAt = probeStillPossible
          ? new Date(Math.min(
            new Date(nextOzonUnarchiveRetryAt()).getTime(),
            Date.now() + ozonUnarchiveQuotaProbeIntervalMs,
          )).toISOString()
          : nextOzonUnarchiveRetryAt();
        let deferQueue = await readOzonUnarchiveQueue();
        deferQueue = queueOzonUnarchiveItems(deferQueue, quotaExhaustedItems, {
          nextRetryAt: deferRetryAt,
          warning: "ozon_unarchive_daily_limit_queued",
          attempted: false,
        });
        await writeOzonUnarchiveQueueDelta(deferQueue, { upsertProducts: quotaExhaustedItems });
        logger.info("ozon_unarchive_queue_daily_defer", { deferred: quotaExhaustedItems.length, nextRetryAt: deferRetryAt });
      } catch (deferError) {
        logger.warn("ozon unarchive queue daily defer failed", { detail: deferError?.message || String(deferError) });
      }
    }
    const resolution = dueItems.length
      ? await resolveOzonUnarchiveQueueProducts(dueItems)
      : { ids: [], rewrites: [] };
    await rewriteResolvedOzonUnarchiveQueueItems(resolution.rewrites);
    // Purge ghost items: queue entries not resolvable to any warehouse product (deleted products,
    // stale offerIds). Previously only purged when ids.length === 0; now purge partial ghosts too
    // so the queue stays clean and the next over-selection finds live items to fill the buffer.
    {
      const resolvedUuidSet = new Set(resolution.ids);
      const rewrittenKeys = new Set((resolution.rewrites || []).map((r) => ozonUnarchiveQueueKey(r.item)));
      const ghostItems = dueItems.filter((item) => {
        const id = cleanText(item.warehouseProductId || item.id);
        if (resolvedUuidSet.has(id)) return false;
        if (rewrittenKeys.has(ozonUnarchiveQueueKey(item))) return false;
        return true;
      });
      if (ghostItems.length) {
        try {
          let currentQueue = await readOzonUnarchiveQueue();
          currentQueue = removeOzonUnarchiveQueueItems(currentQueue, ghostItems);
          await writeOzonUnarchiveQueue(currentQueue);
          logger.info("ozon_unarchive_queue_ghost_purge", { purged: ghostItems.length, remaining: currentQueue.items?.length || 0 });
        } catch (purgeError) {
          logger.warn("ozon unarchive queue ghost purge failed", { detail: purgeError?.message || String(purgeError) });
        }
      }
    }
    // Trim to effectiveLimit so we never exceed the configured batch size nor the remaining
    // daily quota when sending to Ozon
    const ids = resolution.ids.slice(0, effectiveLimit);
    if (!ids.length) {
      const empty = {
        ok: true,
        source,
        queueRunId,
        startedAt,
        finishedAt: new Date().toISOString(),
        selected: 0,
        result: { recovered: 0, unarchivePending: publicQueue.due, queueSize: publicQueue.total },
        queue: publicQueue,
      };
      ozonUnarchiveQueueAutoLastResult = {
        source,
        queueRunId,
        selected: 0,
        recovered: 0,
        unarchivePending: publicQueue.due,
        queueSize: publicQueue.total,
        at: empty.finishedAt,
      };
      return { ...empty, ...ozonUnarchiveQueueAutomationPublic() };
    }
    const products = await buildFreshWarehouseProducts(ids, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: autoPricePmTimeoutMs });
    const result = await runSupplierRecoveryAutomation({ products }, {
      productIds: ids,
      source,
      force,
      // Квота-проба обходит локальный гейт умышленно: реальный ответ Ozon решает.
      forceOzonDailyLimit: Boolean(force) || quotaProbe,
    });
    if (quotaProbe) {
      // Ozon отклонил батч по суточному лимиту → пробы выключаются до конца суток
      // (штатная ветка уже отложила товары на завтра). Принят → квота реально была,
      // разрешаем следующую пробу сразу — 1-секундный планировщик дочерпает очередь.
      const probeRejected = Number(result.queuedByDailyLimit || 0) > 0;
      for (const target of quotaProbeTargets) {
        markOzonUnarchiveQuotaProbe(target, probeRejected ? { rejected: true } : { lastProbeAt: 0 });
      }
      logger.info("ozon_unarchive_quota_probe", {
        targets: Array.from(quotaProbeTargets),
        rejected: probeRejected,
        selected: ids.length,
        unarchived: Number(result.unarchived || 0),
      });
    }
    const freshQueue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 });
    const finishedAt = new Date().toISOString();
    ozonUnarchiveQueueAutoLastResult = {
      source,
      queueRunId,
      selected: ids.length,
      force: Boolean(force),
      recovered: Number(result.recovered || 0),
      restoredStocks: Number(result.restoredStocks || 0),
      unarchived: Number(result.unarchived || 0),
      unarchivePending: Number(result.unarchivePending || 0),
      queuedByDailyLimit: Number(result.queuedByDailyLimit || 0),
      queueSize: Number(freshQueue.total || 0),
      errors: Array.isArray(result.errors) ? result.errors.length : 0,
      at: finishedAt,
    };
    logger.info("ozon unarchive queue processed", ozonUnarchiveQueueAutoLastResult);
    return {
      ok: true,
      source,
      queueRunId,
      startedAt,
      finishedAt,
      selected: ids.length,
      force: Boolean(force),
      productIds: ids,
      result,
      queue: freshQueue,
      ...ozonUnarchiveQueueAutomationPublic(),
    };
  } catch (error) {
    const finishedAt = new Date().toISOString();
    ozonUnarchiveQueueAutoLastResult = {
      source,
      queueRunId,
      selected: 0,
      error: error?.message || String(error),
      at: finishedAt,
    };
    logger.warn("ozon unarchive queue process failed", { source, detail: error?.message || String(error) });
    throw error;
  } finally {
    ozonUnarchiveQueueAutoRunning = false;
    ozonUnarchiveQueueAutoLastRunAt = new Date().toISOString();
  }
}

async function processYandexUnarchiveQueue({ source = "manual", limit = yandexUnarchiveQueueBatchLimit, queueRunId = "" } = {}) {
  if (yandexUnarchiveQueueAutoRunning) {
    return {
      ok: true,
      skipped: true,
      reason: "already_running",
      selected: 0,
      queue: yandexUnarchiveQueuePublic(await readYandexUnarchiveQueue(), { limit: 5000 }),
    };
  }
  yandexUnarchiveQueueProcessQueued = false;
  yandexUnarchiveQueueAutoRunning = true;
  const startedAt = new Date().toISOString();
  try {
    const normalizedLimit = Math.max(1, Math.min(500, Math.round(Number(limit || yandexUnarchiveQueueBatchLimit) || yandexUnarchiveQueueBatchLimit)));
    const queue = await readYandexUnarchiveQueue();
    const publicQueue = yandexUnarchiveQueuePublic(queue, { limit: 5000 });
    const dueItems = (publicQueue.items || []).filter((item) => item.due).slice(0, normalizedLimit);
    const ids = Array.from(new Set(dueItems.map((item) => cleanText(item.warehouseProductId || item.id)).filter(Boolean)));
    if (!ids.length) {
      const empty = {
        ok: true,
        source,
        queueRunId,
        startedAt,
        finishedAt: new Date().toISOString(),
        selected: 0,
        result: { recovered: 0, unarchivePending: publicQueue.due, queueSize: publicQueue.total },
        queue: publicQueue,
      };
      yandexUnarchiveQueueAutoLastResult = {
        source,
        queueRunId,
        selected: 0,
        unarchivePending: publicQueue.due,
        queueSize: publicQueue.total,
        at: empty.finishedAt,
      };
      return empty;
    }
    const products = await buildFreshWarehouseProducts(ids, { refreshPrices: false, livePriceMaster: false, batchPriceMaster: false });
    // Ghost items: product IDs that no longer exist in the warehouse (deleted or renamed).
    // Remove them so they don't retry forever like the Ozon ghost cleanup does.
    const resolvedIds = new Set(products.map((p) => String(p.id)));
    const ghostItems = dueItems.filter((item) => {
      const id = cleanText(item.warehouseProductId || item.id);
      return id && !resolvedIds.has(id);
    });
    if (ghostItems.length) {
      await writeYandexUnarchiveQueueDelta(await readYandexUnarchiveQueue(), { removeProducts: ghostItems }).catch((error) => {
        logger.warn("yandex unarchive queue ghost purge failed", { detail: error?.message || String(error) });
      });
      logger.info("yandex_unarchive_queue_ghost_purge", { purged: ghostItems.length });
    }
    const yandexProducts = products.filter((product) => product.marketplace === "yandex" && productLooksArchived(product));
    // Products that are no longer archived in our DB were already processed (optimistically updated
    // after a prior unarchive command). Remove them from the queue so they don't loop forever.
    const alreadyActiveProducts = products.filter((product) => product.marketplace === "yandex" && !productLooksArchived(product));
    if (alreadyActiveProducts.length) {
      await writeYandexUnarchiveQueueDelta(await readYandexUnarchiveQueue(), { removeProducts: alreadyActiveProducts }).catch((error) => {
        logger.warn("yandex unarchive queue stale cleanup failed", { detail: error?.message || String(error) });
      });
      if (alreadyActiveProducts.length) {
        await restoreStocksOnMarketplaces(alreadyActiveProducts).catch((error) => {
          logger.warn("yandex unarchive queue stale stock restore failed", { detail: error?.message || String(error) });
        });
      }
    }
    const actions = yandexProducts.length
      ? await verifyYandexUnarchiveActions(
        yandexProducts,
        await unarchiveProductsOnMarketplaces(yandexProducts),
      )
      : [];
    const verifiedIds = new Set(actions.filter((item) => item.ok && !item.pending).map((item) => String(item.id)));
    const stillPending = actions.filter((item) => item.pending);
    if (verifiedIds.size) {
      const verifiedProducts = yandexProducts.filter((product) => verifiedIds.has(String(product.id)));
      await writeYandexUnarchiveQueueDelta(await readYandexUnarchiveQueue(), { removeProducts: verifiedProducts });
      if (verifiedProducts.length) {
        await restoreStocksOnMarketplaces(verifiedProducts).catch((error) => {
          logger.warn("yandex unarchive queue stock restore failed", { detail: error?.message || String(error) });
        });
      }
    }
    const freshQueue = yandexUnarchiveQueuePublic(await readYandexUnarchiveQueue(), { limit: 5000 });
    const finishedAt = new Date().toISOString();
    yandexUnarchiveQueueAutoLastResult = {
      source,
      queueRunId,
      selected: ids.length,
      unarchived: verifiedIds.size,
      unarchivePending: stillPending.length,
      queueSize: Number(freshQueue.total || 0),
      at: finishedAt,
    };
    logger.info("yandex unarchive queue processed", yandexUnarchiveQueueAutoLastResult);
    return {
      ok: true,
      source,
      queueRunId,
      startedAt,
      finishedAt,
      selected: ids.length,
      productIds: ids,
      result: {
        unarchived: verifiedIds.size,
        unarchivePending: stillPending.length,
        queueSize: freshQueue.total,
      },
      queue: freshQueue,
    };
  } catch (error) {
    yandexUnarchiveQueueAutoLastResult = {
      source,
      queueRunId,
      selected: 0,
      error: error?.message || String(error),
      at: new Date().toISOString(),
    };
    logger.warn("yandex unarchive queue process failed", { source, detail: error?.message || String(error) });
    throw error;
  } finally {
    yandexUnarchiveQueueAutoRunning = false;
    yandexUnarchiveQueueAutoLastRunAt = new Date().toISOString();
  }
}

function enqueueYandexUnarchiveQueueProcess({ source, limit } = {}) {
  if (yandexUnarchiveQueueAutoRunning || yandexUnarchiveQueueProcessQueued) return false;
  const queueRunId = crypto.randomUUID();
  yandexUnarchiveQueueProcessQueued = true;
  yandexUnarchiveQueueAutoLastResult = {
    source,
    queueRunId,
    selected: 0,
    queued: true,
    at: new Date().toISOString(),
  };
  queueMarketplaceJob("yandex-unarchive-queue-process", { source, limit, queueRunId }, { priority: QUEUE_PRIORITY.UNARCHIVE })
    .catch((error) => {
      yandexUnarchiveQueueProcessQueued = false;
      yandexUnarchiveQueueAutoLastResult = {
        source,
        queueRunId,
        selected: 0,
        error: error?.message || String(error),
        at: new Date().toISOString(),
      };
      logger.warn("yandex unarchive queue job enqueue failed", { source, detail: error?.message || String(error) });
    });
  return true;
}
