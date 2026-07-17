
function msUntilNextDailyRun(timeString, now = new Date()) {
  const [rawHour = "11", rawMinute = "0"] = String(timeString || "11:00").split(":");
  const hour = Math.min(Math.max(Number(rawHour) || 11, 0), 23);
  const minute = Math.min(Math.max(Number(rawMinute) || 0, 0), 59);
  const next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  return next.getTime() - now.getTime();
}

async function runDailyRefresh(trigger = "manual") {
  if (dailySyncPromise) return dailySyncPromise;

  dailySyncPromise = (async () => {
    const startedAt = new Date().toISOString();
    await writeDailySyncState({
      status: "running",
      trigger,
      startedAt,
      lastRunAt: startedAt,
    });

    try {
      const priceMaster = await runSync();
      // Daily sync must stay lightweight. The heavy marketplace import is still available
      // via /api/warehouse/sync/run, while daily/manual-daily uses the rolling reconciler.
      let fullImport = dailyFullImportEnabled;
      let fullImportSkippedReason = null;
      if (fullImport && dailyFullImportDeferUnderLoad && heavyBackgroundWorkShouldDefer("daily_full_import")) {
        fullImport = false;
        fullImportSkippedReason = "deferred_under_load";
      } else if (!dailyFullImportEnabled) {
        fullImportSkippedReason = "full_import_disabled";
      }
      if (fullImportSkippedReason) {
        logger.info("daily sync skipping full marketplace import", { trigger, reason: fullImportSkippedReason });
      }
      const warehouse = await buildWarehouseView({ sync: fullImport });
      const backgroundAutomation = trigger === "manual"
        ? {
            recovery: await runSupplierRecoveryAutomation(warehouse),
            automation: await runNoSupplierMarketplaceAutomation(warehouse),
            scope: { mode: "full", productIds: null, marketplaceChanged: warehouse.marketplaceSyncChanged || 0 },
          }
        : await runTargetedBackgroundSupplierAutomations(priceMaster, warehouse);
      const automation = backgroundAutomation.automation;
      const recovery = backgroundAutomation.recovery;
      let pricePush = null;
      const shouldSendPrices = trigger === "manual" || (trigger === "schedule" && dailySyncSendPrices);
      if (shouldSendPrices) {
        try {
          pricePush = trigger === "manual"
            ? await sendWarehousePrices({
              usdRate: undefined,
              minDiffRub: 0,
              minDiffPct: 0,
              dryRun: false,
            })
            : await sendPriceMasterDeltaWarehousePrices(priceMaster, warehouse);
        } catch (err) {
          const detail = err?.message || String(err);
          pricePush = { sent: 0, failed: 0, skipped: [], error: detail };
          logger.warn("manual daily sync price push failed", { detail });
        }
      }
      const state = await writeDailySyncState(withDailySyncLog({
        status: "ok",
        trigger,
        startedAt,
        lastRunAt: new Date().toISOString(),
        priceMaster,
        warehouse: {
          total: warehouse.total,
          ready: warehouse.ready,
          changed: warehouse.changed,
          withoutSupplier: warehouse.withoutSupplier,
          sourceError: warehouse.sourceError,
          supplierSync: warehouse.supplierSync,
          zeroStockSent: automation.zeroStockSent,
          autoArchived: automation.archived,
          recovered: recovery.recovered,
          automationScope: backgroundAutomation.scope?.productIds?.length ?? null,
          automationSkippedReason: automation.reason || recovery.reason || null,
          pricePush: pricePush
            ? {
              sent: Number(pricePush.sent || 0),
              failed: Number(pricePush.failed || 0),
              queued: Number(pricePush.queued || 0),
              queuedBatches: Number(pricePush.queuedBatches || 0),
              skipped: Array.isArray(pricePush.skipped) ? pricePush.skipped.length : 0,
              error: pricePush.error || null,
            }
            : null,
        },
      }));
      return state;
    } catch (error) {
      const state = await writeDailySyncState(withDailySyncLog({
        status: "failed",
        trigger,
        startedAt,
        lastRunAt: new Date().toISOString(),
        error: error.code || error.message,
      }));
      return state;
    }
  })().finally(() => {
    dailySyncPromise = null;
  });

  return dailySyncPromise;
}

function scheduleDailySync() {
  if (!dailySyncEnabled) return;
  if (dailySyncTimer) clearTimeout(dailySyncTimer);
  const delay = msUntilNextDailyRun(dailySyncTime);
  dailySyncNextRunAt = new Date(Date.now() + delay).toISOString();
  dailySyncTimer = setTimeout(async () => {
    try {
      const result = await runDailyRefresh("schedule");
      logger.info("daily sync tick", { status: result.status, lastRunAt: result.lastRunAt });
    } catch (error) {
      logger.error("daily sync failed", { detail: error.code || error.message, err: error });
    } finally {
      scheduleDailySync();
    }
  }, delay);
}

// Входящие данные маркетплейсов (цены кабинета, остатки, статусы карточек)
// обновлялись только суточным синком в 23:00 МСК — страницы и карточки жили
// на вчерашних данных. Теперь интервальный автосинк раз в
// MARKETPLACE_IMPORT_HOURS (деф. 3 ч; 0 — только daily) делает полный импорт
// Ozon/Yandex. Отметка последнего импорта переживает рестарты worker в data/.
const marketplaceImportEveryMs = Math.max(0, Number(process.env.MARKETPLACE_IMPORT_HOURS ?? 3) || 0) * 60 * 60 * 1000;
const marketplaceImportStatePath = path.join(dataDir, "marketplace-import-state.json");
let marketplaceImportLastAt = 0;

async function shouldRunIntervalMarketplaceImport() {
  if (!marketplaceImportEveryMs) return false;
  // Полный импорт — самая тяжёлая по памяти операция worker (исторический OOM,
  // из-за которого интервальному автосинку импорт вообще запрещали): под
  // давлением heap пропускаем тик, импорт уйдёт следующему циклу.
  if (serverUnderMemoryPressure()) return false;
  if (!marketplaceImportLastAt) {
    try {
      const saved = JSON.parse(await fs.readFile(marketplaceImportStatePath, "utf8"));
      marketplaceImportLastAt = new Date(saved.lastImportAt || 0).getTime() || 0;
    } catch {
      marketplaceImportLastAt = 0;
    }
  }
  return Date.now() - marketplaceImportLastAt >= marketplaceImportEveryMs;
}

async function markIntervalMarketplaceImportDone() {
  marketplaceImportLastAt = Date.now();
  await fs.writeFile(marketplaceImportStatePath, JSON.stringify({ lastImportAt: new Date().toISOString() }, null, 2)).catch(() => {});
}

async function runAutoSyncCycle(trigger = "auto") {
  if (manualWarehouseSyncPromise) {
    logger.info("auto sync skipped: manual warehouse sync running");
    return { status: "manual_sync_running" };
  }
  if (autoSyncRunning) return { status: "already_running" };
  if (heavyBackgroundWorkShouldDefer(`auto_sync:${trigger}`)) {
    return { status: "deferred_under_load" };
  }
  autoSyncRunning = true;
  try {
    const result = await runSync();
    const importMarketplaces = autoSyncShouldImportMarketplaces(trigger) || await shouldRunIntervalMarketplaceImport();
    const warehouse = await buildWarehouseView({ sync: importMarketplaces });
    if (importMarketplaces) await markIntervalMarketplaceImportDone();
    const backgroundAutomation = await runTargetedBackgroundSupplierAutomations(result, warehouse);
    const automation = backgroundAutomation.automation;
    const recovery = backgroundAutomation.recovery;
    const autoPricePush = await sendPriceMasterDeltaWarehousePrices(result, warehouse);
    const stockReconcileEnabled = process.env.AUTO_SYNC_STOCK_RECONCILE_ENABLED !== "false";
    const stockReconcileMaxProducts = Math.max(
      1,
      Number(process.env.AUTO_SYNC_STOCK_RECONCILE_MAX_PRODUCTS || 15000) || 15000,
    );
    const stockReconcileProducts = stockReconcileEnabled
      ? pickTargetStockSendProducts(warehouse.products || []).slice(0, stockReconcileMaxProducts)
      : [];
    const stockReconcile = stockReconcileProducts.length
      ? await sendTargetStocksToMarketplace(stockReconcileProducts)
      : [];
    logger.info("auto sync complete", {
      trigger,
      importMarketplaces,
      items: result.items,
      changes: result.changes,
      at: result.createdAt,
      warehouseTotal: warehouse.total,
      zeroStockSent: automation.zeroStockSent,
      autoArchived: automation.archived,
      recovered: recovery.recovered,
      marketplaceSyncChanged: warehouse.marketplaceSyncChanged || 0,
      automationScope: backgroundAutomation.scope?.productIds?.length || 0,
      automationSkippedReason: automation.reason || recovery.reason || null,
      priceMasterDeltaProducts: autoPricePush.delta?.productIds?.length || 0,
      priceMasterDeltaSkippedReason: autoPricePush.delta?.reason || null,
      autoPriceQueued: autoPricePush.queued || 0,
      autoPriceQueuedBatches: autoPricePush.queuedBatches || 0,
      autoPriceSent: autoPricePush.sent || 0,
      autoPriceFailed: autoPricePush.failed || 0,
      autoPriceSkipped: Array.isArray(autoPricePush.skipped) ? autoPricePush.skipped.length : 0,
      stockReconcileEnabled,
      stockReconcileScope: stockReconcileProducts.length,
      stockReconcileSent: stockReconcile.filter((item) => item.ok).length,
      stockReconcileFailed: stockReconcile.filter((item) => !item.ok).length,
    });
    if (automation.errors.length) {
      logger.warn("no-supplier automation errors", { count: automation.errors.length, sample: automation.errors.slice(0, 10) });
    }
    return { status: "ok", result, warehouse, automation, recovery, autoPricePush, stockReconcile, automationScope: backgroundAutomation.scope };
  } finally {
    autoSyncRunning = false;
  }
}

async function buildMaintenanceWarehouseScope() {
  if (shouldUsePostgresStorage()) {
    const prisma = getPrisma();
    if (prisma) {
      const appSettings = await readAppSettings();
      const rate = Number(appSettings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);
      const [summary, rows, suppliers] = await Promise.all([
        getWarehousePostgresSummaryLight(prisma, rate),
        prisma.warehouseProduct.findMany({
          where: { AND: [enabledWarehouseTargetWhere(), { links: { some: {} } }] },
          include: { links: true },
          orderBy: { updatedAt: "desc" },
          take: marketplaceMaintenanceLinkedScanLimit,
        }),
        getWarehousePostgresSuppliers(prisma),
      ]);
      const seedProducts = rows.map(productFromPostgres);
      const built = seedProducts.length
        ? await buildFreshWarehouseProductsForWarehouse(
          { products: seedProducts, suppliers },
          seedProducts.map((product) => product.id),
          { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
        )
        : [];
      return {
        products: built,
        suppliers,
        total: summary.totalAll,
        ready: summary.counterStats?.ready || 0,
        changed: summary.counterStats?.changed || 0,
        withoutSupplier: summary.counterStats?.withoutSupplier || 0,
        marketplaceSyncChanged: 0,
        marketplaceSyncChangedProductIds: [],
        maintenanceScopeSampled: seedProducts.length >= marketplaceMaintenanceLinkedScanLimit,
        maintenanceLinkedScanLimit: marketplaceMaintenanceLinkedScanLimit,
      };
    }
  }
  return buildWarehouseView({ sync: false });
}

async function runMarketplaceMaintenanceCycle(trigger = "maintenance") {
  if (marketplaceMaintenancePromise) return marketplaceMaintenancePromise;
  if (manualWarehouseSyncPromise) {
    logger.info("marketplace maintenance skipped: manual warehouse sync running");
    return { status: "manual_sync_running" };
  }
  if (marketplaceMaintenanceRunning || autoSyncRunning) return { status: "already_running" };
  const deferState = marketplaceMaintenanceShouldDefer(trigger);
  if (deferState.defer) {
    return { status: deferState.status };
  }

  marketplaceMaintenancePromise = (async () => {
    marketplaceMaintenanceRunning = true;
    const startedAt = new Date().toISOString();
    try {
      const priceMaster = (isMonolithServer && !marketplaceMaintenancePmSyncEnabled)
        ? await getPriceMasterSnapshotMeta()
        : await runSync();
      if (shouldUsePostgresStorage() && marketplaceMaintenancePairBackfillEnabled) {
        try {
          const prisma = getPrisma();
          if (prisma) {
            await syncOzonManualGroupsFromYandexPostgres(prisma, { dryRun: false });
            await backfillOzonYandexManualGroupsPostgres(prisma, { dryRun: false });
            await syncOzonYandexLinkPairsPostgres(prisma, { dryRun: false });
          }
        } catch (error) {
          logger.warn("marketplace maintenance ozon yandex pair backfill failed", { detail: error?.message || String(error) });
        }
      }
      if (shouldUsePostgresStorage()) {
        try {
          const prisma = getPrisma();
          if (prisma) await rebuildWarehouseBrandIndexPostgres(prisma);
        } catch (error) {
          logger.warn("marketplace maintenance brand index rebuild failed", { detail: error?.message || String(error) });
        }
      }
      const warehouse = isMonolithServer
        ? await buildMaintenanceWarehouseScope()
        : await buildWarehouseView({ sync: false });
      const maintenanceAutomationEnabled = process.env.MARKETPLACE_MAINTENANCE_AUTOMATION_ENABLED === "true";
      const automation = maintenanceAutomationEnabled
        ? await runNoSupplierMarketplaceAutomation(warehouse, {
          includeNoLinks: false,
          skipLinkedGrace: true,
          source: trigger,
        })
        : { zeroStockSent: 0, archived: 0, errors: [], reason: "maintenance_automation_disabled" };
      const recovery = (maintenanceAutomationEnabled && !isMonolithServer)
        ? await runSupplierRecoveryAutomation(warehouse, { source: trigger })
        : { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [], reason: "maintenance_recovery_disabled" };
      let yandexUnarchiveQueueResult = null;
      if (maintenanceAutomationEnabled && yandexUnarchiveQueueAutoEnabled && !isMonolithServer) {
        try {
          yandexUnarchiveQueueResult = await processYandexUnarchiveQueue({
            source: `${trigger}_yandex_unarchive_queue`,
            limit: yandexUnarchiveQueueBatchLimit,
          });
        } catch (error) {
          logger.warn("marketplace maintenance yandex unarchive queue failed", { detail: error?.message || String(error) });
        }
      }
      const result = {
        status: "ok",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        priceMaster: {
          items: priceMaster.items,
          changes: priceMaster.changes,
          updatedAt: priceMaster.createdAt || priceMaster.updatedAt || null,
        },
        warehouseTotal: warehouse.total,
        maintenanceScopeSampled: Boolean(warehouse.maintenanceScopeSampled),
        marketplaceSyncChanged: warehouse.marketplaceSyncChanged || 0,
        zeroStockSent: automation.zeroStockSent,
        autoArchived: automation.archived,
        recovered: recovery.recovered,
        yandexUnarchiveQueue: yandexUnarchiveQueueResult,
      };
      logger.info("marketplace maintenance complete", result);
      if (automation.errors.length) {
        logger.warn("marketplace maintenance no-supplier errors", {
          count: automation.errors.length,
          sample: automation.errors.slice(0, 10),
        });
      }
      return result;
    } catch (error) {
      logger.error("marketplace maintenance failed", {
        trigger,
        detail: error?.message || String(error),
        err: error,
      });
      throw error;
    } finally {
      marketplaceMaintenanceRunning = false;
      marketplaceMaintenancePromise = null;
    }
  })();

  return marketplaceMaintenancePromise;
}

function marketplaceMaintenanceStartupDelayMs(requestedDelayMs = null) {
  const intervalMs = marketplaceMaintenanceHours * 60 * 60 * 1000;
  const requested = Math.max(60_000, Number(requestedDelayMs ?? intervalMs) || intervalMs);
  const minUptimeMs = marketplaceMaintenanceMinUptimeSec * 1000;
  const remainingUptimeMs = Math.max(0, minUptimeMs - (process.uptime() * 1000));
  return Math.max(requested, remainingUptimeMs);
}

function scheduleMarketplaceMaintenance(delayMs = null) {
  if (!marketplaceMaintenanceEnabled) {
    marketplaceMaintenanceNextRunAt = null;
    return;
  }
  if (marketplaceMaintenanceTimer) clearTimeout(marketplaceMaintenanceTimer);
  const intervalMs = marketplaceMaintenanceHours * 60 * 60 * 1000;
  const normalizedDelay = marketplaceMaintenanceStartupDelayMs(delayMs ?? intervalMs);
  marketplaceMaintenanceNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  marketplaceMaintenanceTimer = setTimeout(async () => {
    let nextDelayMs = intervalMs;
    try {
      const result = await runMarketplaceMaintenanceCycle("scheduled_maintenance");
      if (result?.status && String(result.status).startsWith("deferred_")) {
        nextDelayMs = marketplaceMaintenanceDeferRetryMs;
      }
    } catch (error) {
      logger.error("scheduled marketplace maintenance failed", { detail: error?.message || String(error), err: error });
    } finally {
      scheduleMarketplaceMaintenance(nextDelayMs);
    }
  }, normalizedDelay);
}
