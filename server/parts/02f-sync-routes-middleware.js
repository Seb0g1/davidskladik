function requestErrorTitle(error, request) {
  if (error instanceof multer.MulterError) return "Не удалось загрузить изображение";
  const pathName = cleanText(request?.path).toLowerCase();
  const code = cleanText(error?.code).toLowerCase();
  if (
    pathName.includes("/ai/")
    || pathName.includes("/ai-")
    || pathName.includes("/settings/ai")
    || code.startsWith("openai")
  ) return "AI запрос не выполнен";
  if (pathName.includes("pricemaster")) return "Не удалось выполнить запрос к Price Master";
  if (pathName.includes("ozon") || pathName.includes("yandex") || pathName.includes("marketplace")) {
    return "Не удалось выполнить запрос к маркетплейсу";
  }
  return "Не удалось выполнить запрос";
}

app.post("/api/sync", async (_request, response, next) => {
  try {
    response.json(await runSync());
  } catch (error) {
    next(error);
  }
});

app.get("/api/pricemaster/offer-docs-schema", requireAdmin, async (_request, response, next) => {
  try {
    const col = await discoverOfferDocsActiveColumn();
    response.json({ activeColumn: col || null, found: Boolean(col), filterSuffix: offerDocsActiveFilterSuffix || null });
  } catch (error) {
    next(error);
  }
});

app.get("/api/daily-sync", async (_request, response, next) => {
  try {
    response.json(await getDailySyncStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/daily-sync/run", requireAdmin, async (_request, response, next) => {
  try {
    const alreadyRunning = Boolean(dailySyncPromise);
    if (!alreadyRunning) {
      runDailyRefresh("manual").catch((error) => {
        logger.error("manual daily sync background failed", { detail: error?.code || error?.message || String(error), err: error });
      });
    }
    const status = await getDailySyncStatus();
    response.status(202).json({
      ok: true,
      started: !alreadyRunning,
      running: true,
      status: status.status === "running" ? status : { ...status, status: "running", trigger: "manual" },
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/sync/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(getManualWarehouseSyncStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplace/maintenance/run", requireAdmin, async (_request, response, next) => {
  try {
    const alreadyRunning = Boolean(marketplaceMaintenancePromise);
    if (!alreadyRunning) {
      runMarketplaceMaintenanceCycle("manual_maintenance").catch((error) => {
        logger.error("manual marketplace maintenance failed", { detail: error?.message || String(error), err: error });
      });
    }
    response.status(202).json({
      ok: true,
      started: !alreadyRunning,
      running: true,
      nextRunAt: marketplaceMaintenanceNextRunAt || null,
      everyHours: marketplaceMaintenanceHours,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/marketplace/reconciler/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await getLinkedReconcilerStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplace/reconciler/run", requireAdmin, async (_request, response, next) => {
  try {
    if (isApiServer) {
      return response.status(409).json({
        ok: false,
        code: "worker_only",
        error: "Linked reconciler runs only in the worker process.",
      });
    }
    if (!linkedReconcilerEnabled) {
      return response.status(409).json({
        ok: false,
        code: "linked_reconciler_disabled",
        error: "Linked reconciler is disabled for this process.",
      });
    }
    const alreadyRunning = linkedReconcilerRunning;
    if (!alreadyRunning) {
      runLinkedReconcilerBatch("manual").catch((error) => {
        logger.error("manual linked reconciler failed", { detail: error?.message || String(error), err: error });
      });
    }
    response.status(202).json({
      ok: true,
      started: !alreadyRunning,
      running: true,
      enabled: linkedReconcilerEnabled,
      nextRunAt: linkedReconcilerNextRunAt || null,
      intervalMinutes: linkedReconcilerIntervalMinutes,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/sync/run", requireAdmin, async (_request, response, next) => {
  try {
    const result = startManualWarehouseSync("manual");
    response.status(202).json({
      ok: true,
      started: result.started,
      running: true,
      status: result.status,
    });
  } catch (error) {
    next(error);
  }
});

app.use((error, request, response, _next) => {
  logger.error("request error", {
    path: request.path,
    method: request.method,
    detail: error.statusCode ? error.message : (error.code || error.message),
    code: error.code || null,
    matches: error.matches || undefined,
    err: error,
  });
  // Audit only genuine server-side failures, not 4xx client/validation errors (would be noise).
  if (!error.statusCode || error.statusCode >= 500) {
    recordErrorEvent({ source: `http ${request.method} ${request.path}`, message: error.message, code: error.code || "" });
  }
  const uploadError = error instanceof multer.MulterError;
  // Don't propagate 405/404/401 from upstream APIs (Avito, Ozon…) as-is — they confuse
  // the client into thinking the Express route doesn't exist. Map external 4xx to 502.
  const isExternalApiError = Boolean(error.avito || error.ozon);
  const rawStatus = uploadError ? 400 : (error.statusCode || 500);
  const httpStatus = isExternalApiError && rawStatus === 405 ? 502 : rawStatus;
  response.status(httpStatus).json({
    error: requestErrorTitle(error, request),
    detail: error.statusCode ? error.message : (error.code || error.message),
    code: error.code || null,
    matches: error.matches || undefined,
    ozon: error.ozon,
    avito: error.avito,
  });
});

function startBackgroundSchedulers() {
  if (!backgroundJobsEnabled) {
    logger.info("background schedulers disabled until BACKGROUND_JOBS_ENABLED=true");
    if (marketplaceMaintenanceEnabled) {
      scheduleMarketplaceMaintenance(Math.max(120_000, Number(process.env.MARKETPLACE_MAINTENANCE_INITIAL_DELAY_MS || 300000) || 300000));
      logger.info("marketplace maintenance scheduler enabled standalone", {
        everyHours: marketplaceMaintenanceHours,
        nextRunAt: marketplaceMaintenanceNextRunAt,
      });
    }
    if (ozonUnarchiveQueueAutoEnabled) {
      scheduleOzonUnarchiveQueueAuto(ozonUnarchiveQueueAutoInitialDelaySeconds * 1000);
      logger.info("ozon unarchive queue scheduler enabled standalone", {
        batchLimit: ozonUnarchiveQueueBatchLimit,
        dailyLimit: Number.isFinite(ozonUnarchiveDailyLimit) ? ozonUnarchiveDailyLimit : null,
      });
    }
    if (yandexUnarchiveQueueAutoEnabled) {
      scheduleYandexUnarchiveQueueAuto(yandexUnarchiveQueueAutoInitialDelaySeconds * 1000);
      logger.info("yandex unarchive queue scheduler enabled standalone", {
        batchLimit: yandexUnarchiveQueueBatchLimit,
      });
    }
    return;
  }
  discoverOfferDocsActiveColumn().catch((error) => logger.warn("OfferDocs schema warmup failed", { detail: error?.message || String(error) }));
  scheduleDailySync();
  scheduleOzonMarkingClearNightly();
  schedulePriceRetryProcessing(30_000);
  scheduleOzonUnarchiveQueueAuto(ozonUnarchiveQueueAutoInitialDelaySeconds * 1000);
  if (yandexUnarchiveQueueAutoEnabled) {
    scheduleYandexUnarchiveQueueAuto(yandexUnarchiveQueueAutoInitialDelaySeconds * 1000);
  }
  scheduleSupplierCartAuto(180_000).catch((error) => logger.warn("supplier cart auto scheduler failed", { detail: error?.message || String(error) }));
  scheduleAutoSync(autoSyncInitialDelaySeconds * 1000);
  if (marketplaceMaintenanceEnabled) {
    scheduleMarketplaceMaintenance(Math.max(120_000, Number(process.env.MARKETPLACE_MAINTENANCE_INITIAL_DELAY_MS || 900000) || 900000));
    logger.info("marketplace maintenance scheduler enabled", {
      everyHours: marketplaceMaintenanceHours,
      nextRunAt: marketplaceMaintenanceNextRunAt,
    });
  }
  if (financeOrdersSyncEnabled) {
    scheduleFinanceOrdersSync(3 * 60_000);
    logger.info("finance orders sync scheduler enabled", {
      intervalMinutes: Math.round(financeOrdersSyncIntervalMs / 60000),
      lookbackDays: financeOrdersLookbackDays,
    });
  }
  if (notifyPollEnabled) {
    scheduleNotificationPolling(60_000);
    logger.info("notification polling enabled", {
      intervalSeconds: Math.round(notifyPollIntervalMs / 1000),
    });
  }
  if (stockSweepEnabled) {
    scheduleStockSweep(90_000);
    logger.info("stock sweep scheduler enabled", {
      intervalSeconds: Math.round(stockSweepIntervalMs / 1000),
      batchLimit: stockSweepBatchLimit,
      defaultTarget: linkedDefaultTargetStock,
    });
  }
  if (zeroStockSweepEnabled && autoZeroStockOnNoSupplier) {
    scheduleZeroStockSweep(120_000);
    logger.info("zero stock sweep scheduler enabled", {
      intervalSeconds: Math.round(zeroStockSweepIntervalMs / 1000),
      batchLimit: zeroStockSweepBatchLimit,
    });
  }
  if (snoozeSweepEnabled) {
    scheduleSnoozeSweep(5 * 60_000);
    logger.info("snooze sweep scheduler enabled", {
      intervalSeconds: Math.round(snoozeSweepIntervalMs / 1000),
    });
  }
  if (priceSweepEnabled) {
    schedulePriceSweep(60_000);
    logger.info("price sweep scheduler enabled", {
      intervalSeconds: Math.round(priceSweepIntervalMs / 1000),
      batchLimit: priceSweepBatchLimit,
    });
  }
  if (avitoFeedRefreshEnabled) {
    scheduleAvitoFeedRefresh(4 * 60_000);
    logger.info("avito feed refresh scheduler enabled", {
      intervalMinutes: Math.round(avitoFeedRefreshIntervalMs / 60000),
    });
  }
  if (avitoAutoSyncEnabled) {
    scheduleAvitoAutoSync(10 * 60_000);
    logger.info("avito auto sync scheduler enabled", {
      intervalHours: Math.round(avitoAutoSyncIntervalMs / 3600000),
    });
  }
  if (healthAlertEnabled) {
    scheduleHealthAlertMonitor(150_000);
    logger.info("health alert monitor enabled", {
      intervalSeconds: Math.round(healthAlertIntervalMs / 1000),
      cooldownMinutes: Math.round(healthAlertCooldownMs / 60000),
      telegram: healthAlertTelegramConfigured(),
    });
  }
  if (errorAuditEnabled) {
    scheduleErrorAuditDigest(10 * 60_000);
    logger.info("error audit digest enabled", {
      everyHours: Math.round(errorAuditDigestIntervalMs / 3_600_000),
      retentionDays: errorAuditRetentionDays,
    });
  }
  if (yandexFastUnarchiveEnabled) {
    scheduleYandexFastUnarchive(45_000);
    logger.info("yandex fast unarchive scheduler enabled", {
      intervalSeconds: Math.round(yandexFastUnarchiveIntervalMs / 1000),
      nextRunAt: yandexFastUnarchiveNextRunAt,
    });
  }
  if (ozonYandexAutoImportEnabled) {
    scheduleOzonYandexAutoImport(20 * 60 * 1000);
    logger.info("ozon yandex auto import scheduler enabled", {
      everyHours: ozonYandexAutoImportIntervalHours,
      perRun: ozonYandexAutoImportPerRunLimit,
      nextRunAt: ozonYandexAutoImportNextRunAt,
    });
  }
  if (yandexPhotoBackfillEnabled) {
    scheduleYandexPhotoBackfill(30 * 60 * 1000);
    logger.info("yandex photo backfill scheduler enabled", {
      everyHours: yandexPhotoBackfillIntervalHours,
      perRun: yandexPhotoBackfillPerRunLimit,
      nextRunAt: yandexPhotoBackfillNextRunAt,
    });
  }
  if (wbMediaBackfillEnabled) {
    // Короткая первая задержка: квота WB media/save крошечная (~1 фото/15 мин),
    // каждый рестарт worker не должен отодвигать следующий кадр.
    scheduleWbMediaBackfill(90 * 1000);
    logger.info("wb media backfill scheduler enabled", {
      everyMinutes: wbMediaBackfillIntervalMinutes,
      perRun: wbMediaBackfillPerRunLimit,
      nextRunAt: wbMediaBackfillNextRunAt,
    });
  }
  if (wbSyncEnabled) {
    scheduleWbSync(20 * 60 * 1000);
    logger.info("wb sync scheduler enabled", {
      everyHours: wbSyncIntervalHours,
      nextRunAt: wbSyncNextRunAt,
    });
  }
  if (ozonNewOfferDiscoveryEnabled) {
    // Short initial delay on purpose: the worker restarts often enough that a long
    // first delay would keep pushing the run past the next restart.
    scheduleOzonNewOfferDiscovery(3 * 60 * 1000);
    logger.info("ozon new offer discovery scheduler enabled", {
      everyMinutes: ozonNewOfferDiscoveryIntervalMinutes,
      perRun: ozonNewOfferDiscoveryPerRunLimit,
      nextRunAt: ozonNewOfferDiscoveryNextRunAt,
    });
  }
  if (linkedReconcilerEnabled) {
    scheduleLinkedReconciler(linkedReconcilerInitialDelaySeconds * 1000);
    logger.info("linked reconciler scheduler enabled", {
      batchSize: linkedReconcilerBatchSize,
      intervalMinutes: linkedReconcilerIntervalMinutes,
      maxProductsPerTick: linkedReconcilerMaxProductsPerTick,
      nextRunAt: linkedReconcilerNextRunAt,
    });
  }
  // Автосинк продаж PriceMaster → реализация (каждые CONSIGNMENT_PM_SYNC_HOURS, деф. 2 ч).
  scheduleConsignmentPmSync(5 * 60_000);
  logger.info("consignment pm sync scheduler enabled", {
    everyHours: Math.round(consignmentPmSyncIntervalMs / 3_600_000),
  });
}
