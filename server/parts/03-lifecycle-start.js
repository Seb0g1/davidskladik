async function startServer() {
  logEffectiveRuntimeConfig();
  initMarketplaceQueue();
  pruneUploadDirectory().catch((err) => logger.warn("initial upload prune failed", { detail: err?.message || String(err) }));

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
  }
}
