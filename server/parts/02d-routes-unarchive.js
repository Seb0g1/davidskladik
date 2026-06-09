app.get("/api/ozon/unarchive-queue", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 1000, 5000);
    response.json({
      ...ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit }),
      ...ozonUnarchiveQueueAutomationPublic(),
    });
  } catch (error) {
    next(error);
  }
});

function enqueueOzonUnarchiveQueueProcess({ request, source, limit, force }) {
  if (ozonUnarchiveQueueAutoRunning || ozonUnarchiveQueueProcessQueued) return false;
  const queuedAt = new Date().toISOString();
  const queueRunId = crypto.randomUUID();
  const queuedThroughBullmq = Boolean(marketplaceQueue) && !marketplaceJobsShouldRunInline("ozon-unarchive-queue-process");
  ozonUnarchiveQueueProcessQueued = true;
  ozonUnarchiveQueueAutoLastResult = {
    source,
    queueRunId,
    selected: 0,
    queued: true,
    at: queuedAt,
  };
  if (!queuedThroughBullmq) {
    queueMarketplaceJob("ozon-unarchive-queue-process", { source, limit, force, queueRunId }, { priority: 1 })
      .then((result) => {
        ozonUnarchiveQueueProcessQueued = false;
        if (request && result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "selected") && !result.skipped) {
          appendAudit(request, "ozon.unarchive_queue.process", {
            selected: result.selected || 0,
            productIds: result.productIds || [],
            result,
          }).catch((auditError) => logger.warn("ozon queue audit append failed", { detail: auditError?.message || String(auditError) }));
        }
      })
      .catch((error) => {
        ozonUnarchiveQueueProcessQueued = false;
        ozonUnarchiveQueueAutoLastResult = {
          source,
          queueRunId,
          selected: 0,
          error: error?.message || String(error),
          at: new Date().toISOString(),
        };
        logger.warn("ozon queue inline process failed", { source, queueRunId, detail: error?.message || String(error) });
      });
    return true;
  }
  setTimeout(() => {
    if (
      ozonUnarchiveQueueProcessQueued
      && !ozonUnarchiveQueueAutoRunning
      && ozonUnarchiveQueueAutoLastResult?.queueRunId === queueRunId
    ) {
      ozonUnarchiveQueueProcessQueued = false;
      ozonUnarchiveQueueAutoLastResult = {
        source,
        queueRunId,
        selected: 0,
        warning: "queue_start_timeout_inline_fallback",
        at: new Date().toISOString(),
      };
      logger.warn("ozon queue process did not start in time, running inline fallback", { source, queueRunId });
      processOzonUnarchiveQueue({
        source: `${source}_fallback`,
        limit,
        force,
        queueRunId,
      }).catch((error) => {
        ozonUnarchiveQueueAutoLastResult = {
          source,
          queueRunId,
          selected: 0,
          error: error?.message || String(error),
          at: new Date().toISOString(),
        };
        logger.warn("ozon queue inline fallback failed", { source, queueRunId, detail: error?.message || String(error) });
      });
    }
  }, 5 * 60 * 1000).unref?.();
  setTimeout(() => {
    queueMarketplaceJob("ozon-unarchive-queue-process", { source, limit, force, queueRunId }, { priority: 1 })
      .then((result) => {
        ozonUnarchiveQueueProcessQueued = false;
        if (request && result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "selected") && !result.skipped) {
          appendAudit(request, "ozon.unarchive_queue.process", {
            selected: result.selected || 0,
            productIds: result.productIds || [],
            result,
          }).catch((auditError) => logger.warn("ozon queue audit append failed", { detail: auditError?.message || String(auditError) }));
        }
      })
      .catch((error) => {
        ozonUnarchiveQueueProcessQueued = false;
        ozonUnarchiveQueueAutoLastResult = {
          source,
          selected: 0,
          error: error?.message || String(error),
          at: new Date().toISOString(),
        };
        logger.warn("ozon queue background enqueue failed", { detail: error?.message || String(error) });
      })
      .finally(() => {
        if (!queuedThroughBullmq && !ozonUnarchiveQueueAutoRunning) {
          ozonUnarchiveQueueProcessQueued = false;
        }
      });
  }, 0);
  return true;
}

app.post("/api/ozon/unarchive-queue/rebuild", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body?.limit || request.query?.limit, 20000, 50000);
    const result = await rebuildOzonUnarchiveQueue({
      source: "ozon_unarchive_queue_rebuild_manual",
      limit,
    });
    response.json({
      ...result,
      queue: ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 1000 }),
      ...ozonUnarchiveQueueAutomationPublic(),
    });
    appendAudit(request, "ozon.unarchive_queue.rebuild", {
      scanned: result.scanned,
      queued: result.queued,
      due: result.due,
    }).catch((auditError) => logger.warn("ozon queue rebuild audit append failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/unarchive-queue/process", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body?.limit || request.query?.limit, ozonUnarchiveQueueBatchLimit, 1000);
    const source = "ozon_unarchive_queue_manual";
    const accepted = enqueueOzonUnarchiveQueueProcess({ request, source, limit, force: true });
    const queue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 1000 });
    response.status(202).json({
      ok: true,
      accepted,
      queued: accepted,
      jobId: ozonUnarchiveQueueAutoLastResult?.queueRunId || null,
      statusUrl: "/api/ozon/unarchive-queue",
      skipped: !accepted,
      reason: accepted ? null : "already_running",
      source,
      limit,
      queue,
      ...queue,
      ...ozonUnarchiveQueueAutomationPublic(),
    });
    appendAudit(request, "ozon.unarchive_queue.queued", {
      accepted,
      source,
      limit,
      force: true,
    }).catch((auditError) => logger.warn("ozon queue audit append failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

async function repairWarehouseProductGroup(productId, request = null) {
  const initialWarehouse = await readWarehouse();
  const seed = (initialWarehouse.products || []).find((product) => String(product.id) === String(productId));
  if (!seed) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    throw error;
  }
  const initialGroup = expandWarehouseProductsToGroups(initialWarehouse.products || [], [seed]);
  const initialIds = initialGroup.map((product) => String(product.id)).filter(Boolean);
  return withWarehouseProductMutationLock(initialIds, async () => {
    const warehouse = await readWarehouse();
    const currentSeed = (warehouse.products || []).find((product) => String(product.id) === String(productId));
    if (!currentSeed) {
      const error = new Error("Warehouse product not found.");
      error.statusCode = 404;
      throw error;
    }
    const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [currentSeed]);
    const productIds = groupProducts.map((product) => String(product.id)).filter(Boolean);
    const syncResult = syncWarehouseProductGroupLinks(groupProducts, { now: new Date().toISOString(), username: requestUsername(request) });
    if ((syncResult.changedProducts || []).length) {
      await writeWarehouseProductPatch(syncResult.changedProducts, { reason: "warehouse_product_repair_links_sync" });
    }
    const priceResult = await sendWarehousePrices({
      productIds,
      dryRun: false,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      marketplace: "all",
    });
    const freshProducts = await buildFreshWarehouseProducts(productIds, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true });
    const recoveryResult = await runSupplierRecoveryAutomation({ products: freshProducts }, { productIds, source: "product_repair", force: true });
    const diagnostics = await buildWarehouseSkuDiagnostics(currentSeed.offerId || currentSeed.sku || currentSeed.productId || currentSeed.id, { limit: 50, auditLimit: 30 });
    const payload = {
      ok: true,
      productIds,
      linksSynced: (syncResult.changedProducts || []).length,
      priceSent: Number(priceResult.sent || priceResult.items?.length || 0) || 0,
      stockSent: Number(priceResult.stockSent || recoveryResult.restoredStocks || 0) || 0,
      unarchiveStatus: recoveryResult.unarchivePending ? "pending" : "done",
      pending: Number(recoveryResult.unarchivePending || 0) > 0,
      errors: [...(priceResult.failed || []), ...(recoveryResult.errors || [])],
      nextRetryAt: recoveryResult.nextRetryAt || null,
      priceResult,
      recoveryResult,
      diagnostics,
    };
    appendAudit(request || { session: { username: "system", role: "admin" } }, "warehouse.product.repair", {
      productId: currentSeed.id,
      offerId: currentSeed.offerId,
      productIds,
      result: payload,
    }).catch((auditError) => logger.warn("product repair audit append failed", { detail: auditError?.message || String(auditError) }));
    return payload;
  });
}
