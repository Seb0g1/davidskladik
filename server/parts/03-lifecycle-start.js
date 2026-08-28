async function cleanDataDirStaleFiles() {
  try {
    const entries = await fs.readdir(dataDir);
    const now = Date.now();
    let removed = 0;
    for (const entry of entries) {
      const full = path.join(dataDir, entry);
      try {
        const stat = await fs.stat(full);
        const ageMs = now - stat.mtimeMs;
        if (entry.endsWith(".tmp") && ageMs > 60 * 60 * 1000) {
          await fs.unlink(full);
          removed++;
        } else if (entry.includes(".corrupt-") && ageMs > 7 * 24 * 60 * 60 * 1000) {
          await fs.unlink(full);
          removed++;
        }
      } catch (_) {}
    }
    if (removed > 0) logger.info("data dir stale files cleaned", { removed });
  } catch (error) {
    logger.warn("data dir stale files cleanup failed", { detail: error?.message || String(error) });
  }
}

async function startServer() {
  logEffectiveRuntimeConfig();
  initMarketplaceQueue();
  pruneUploadDirectory().catch((err) => logger.warn("initial upload prune failed", { detail: err?.message || String(err) }));
  cleanDataDirStaleFiles().catch(() => {});

  if (isWorkerServer) {
    startBackgroundSchedulers();
    if (warehouseWarmOnStartup) {
      readWarehouseFull()
        .then((warehouse) => {
          logger.info("warehouse cache warmed", { products: warehouse.products.length, suppliers: warehouse.suppliers.length });
        })
        .catch((err) => {
          logger.warn("warehouse cache warm failed", { detail: err?.message || String(err) });
        });
    }
    const workerHealthPort = Number(process.env.WORKER_HEALTH_PORT || 0) || 0;
    if (workerHealthPort > 0) {
      app.listen(workerHealthPort, () => {
        logger.info("worker health endpoint started", { port: workerHealthPort, healthPath: "/health" });
      });
    }
    logger.info("worker started", { serverRole, bullmq: Boolean(marketplaceWorker) });
    return;
  }

  app.listen(port, () => {
    logger.info("server started", {
      serverRole: isApiServer ? "api" : "monolith",
      port,
      url: `http://localhost:${port}`,
      healthPath: "/health",
      trustProxyHops: trustProxyHops || 0,
    });
    if (!isApiServer) {
      logger.info("auto sync scheduler enabled", {
        defaultEveryMinutes: Math.max(5, Number(autoSyncMinutes || 30) || 30),
        initialDelaySeconds: autoSyncInitialDelaySeconds,
      });
      if (dailySyncEnabled) {
        logger.info("daily sync enabled", { time: dailySyncTime, sendPrices: dailySyncSendPrices });
      }
    }
  });

  if (warehouseWarmOnStartup && !isApiServer) {
    readWarehouseFull()
      .then((warehouse) => {
        logger.info("warehouse cache warmed", { products: warehouse.products.length, suppliers: warehouse.suppliers.length });
      })
      .catch((err) => {
        logger.warn("warehouse cache warm failed", { detail: err?.message || String(err) });
      });
  }

  if (!isApiServer) startBackgroundSchedulers();
  scheduleWarehousePostgresSummaryWarm();
  scheduleWarehouseGroupCountWarm();
  if (shouldUsePostgresStorage()) {
    const prisma = getPrisma();
    if (prisma) kickWarehousePostgresLinksBackfill(prisma);
    // One-time: fix ledger entries for RUB suppliers stored with wrong priceCurrency="USD" default.
    // Runs in background with a small delay to avoid slowing down startup.
    setTimeout(() => fixRubSupplierLedgerAmounts(prisma).catch((e) =>
      logger.warn("rub_supplier_ledger_fix failed", { detail: e?.message || String(e) }),
    ), 15_000);
    // Auto-repair: send missing Yandex descriptions fetched from Ozon API.
    // Runs after each deploy to keep descriptions up-to-date without manual action.
    setTimeout(() => repairYandexDescriptions(prisma, { dryRun: false, limit: 5000 }).then((r) =>
      logger.info("yandex_desc_repair complete", { candidates: r.candidates, sent: r.sent, apiCalls: r.apiCalls, persisted: r.persisted }),
    ).catch((e) =>
      logger.warn("yandex_desc_repair failed", { detail: e?.message || String(e) }),
    ), 60_000);
    // Force-send all prices 60 s after API start so markup changes apply immediately after every deploy.
    setTimeout(() => queueAuthoritativePriceReprice({
      marketplace: "all",
      reason: "post_deploy_price_sync",
      sourceEvent: "server_startup",
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
    }).then((r) => logger.info("post_deploy_price_sync queued", { queued: r?.queued, queuedBatches: r?.queuedBatches }))
    .catch((e) => logger.warn("post_deploy_price_sync failed", { detail: e?.message || String(e) })), 60_000);
  }
}
