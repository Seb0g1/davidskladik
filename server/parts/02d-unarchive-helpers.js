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
    const perTargetTaken = new Map();
    const dueItems = [];
    for (const item of publicQueue.items || []) {
      if (!item.due) continue;
      const target = cleanText(item.target) || "default";
      const available = normalizedLimit;
      const taken = perTargetTaken.get(target) || 0;
      if (taken >= available) continue;
      dueItems.push(item);
      perTargetTaken.set(target, taken + 1);
      if (dueItems.length >= normalizedLimit) break;
    }
    let ids = dueItems.map((item) => cleanText(item.warehouseProductId || item.id)).filter(Boolean);
    const dueWithoutWarehouseId = dueItems.filter((item) => !cleanText(item.warehouseProductId || item.id) && cleanText(item.offerId));
    if (dueWithoutWarehouseId.length) {
      const warehouse = await readWarehouse();
      for (const item of dueWithoutWarehouseId) {
        const offerId = cleanText(item.offerId).toLowerCase();
        const target = cleanText(item.target).toLowerCase();
        const match = (warehouse.products || []).find((product) =>
          cleanText(product.marketplace).toLowerCase() === "ozon"
          && cleanText(product.offerId).toLowerCase() === offerId
          && (!target || cleanText(product.target).toLowerCase() === target)
        );
        if (match?.id) ids.push(String(match.id));
      }
    }
    ids = Array.from(new Set(ids));
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
    const products = await buildFreshWarehouseProducts(ids, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true });
    const result = await runSupplierRecoveryAutomation({ products }, {
      productIds: ids,
      source,
      force,
      forceOzonDailyLimit: Boolean(force),
    });
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
    const yandexProducts = products.filter((product) => product.marketplace === "yandex" && productLooksArchived(product));
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
  queueMarketplaceJob("yandex-unarchive-queue-process", { source, limit, queueRunId }, { priority: 1 })
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

