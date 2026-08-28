
// Daily scan: find products whose target_price is impossibly low relative to PM prices.
// Resets target_price = NULL so the price sweep recomputes from PM × rate × markup.
// Catches the "rate ≈ 1" bug where target_price was saved without the USD→RUB multiplier.
async function runStalePriceTargetScan() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { skipped: true, reason: "postgres_disabled" };
  try {
    const rateData = await getUsdRate({ force: false }).catch(() => null);
    const rate = Number(rateData?.rate || 0);
    if (!rate) return { skipped: true, reason: "no_usd_rate" };

    // Find products where target_price < cheapest_active_PM_price_usd × rate × 0.5
    // Joins via product_links → pm_snapshot_items for both matchType=selected_row and article
    const staleRows = await prisma.$queryRawUnsafe(`
      SELECT DISTINCT wp.id::text AS id, wp.target_price, wp.offer_id, wp.marketplace
      FROM warehouse_products wp
      WHERE wp.archived = false
        AND wp.target_stock > 0
        AND wp.target_price IS NOT NULL AND wp.target_price > 0 AND wp.target_price < 500
        AND EXISTS (
          SELECT 1 FROM product_links pl
          JOIN pm_snapshot_items pm ON (
            (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = COALESCE(pl.source_row_id, pl.raw->>'sourceRowId'))
            OR (pl.raw->>'matchType' = 'article'
                AND pm.partner_id::text = pl.partner_id::text
                AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
          )
          WHERE pl.product_id = wp.id::text
            AND pm.active = true
            AND pm.currency = 'USD'
            AND pm.price IS NOT NULL
            AND wp.target_price < (pm.price::float * ${rate} * 0.5)
        )
    `);

    if (!staleRows.length) return { ok: true, reset: 0, rate };

    const ids = staleRows.map((r) => r.id);
    await prisma.$executeRawUnsafe(
      `UPDATE warehouse_products SET target_price = NULL, updated_at = now()
       WHERE id::text = ANY($1)`,
      ids,
    );

    logger.warn("stale_price_target_scan_reset", {
      reset: ids.length,
      rate,
      sample: staleRows.slice(0, 5).map((r) => `${r.offer_id}[${r.marketplace}]=${r.target_price}₽`),
    });

    // Alert via Telegram (sendHealthAlertTelegram defined in 02f-health-alert-monitor.js, loaded after this file)
    if (typeof sendHealthAlertTelegram === "function") {
      const sample = staleRows.slice(0, 5).map((r) => `${r.offer_id}[${r.marketplace}]=${r.target_price}₽`).join(", ");
      await sendHealthAlertTelegram(
        `🔧 DavidSklad: сброшены зависшие цены (${ids.length} SKU с target_price < 500₽ при PM-цене ×${rate.toFixed(0)} > 500₽).\nПримеры: ${sample}\nPrice sweep пересчитает автоматически.`
      ).catch(() => {});
    }

    return { ok: true, reset: ids.length, rate };
  } catch (error) {
    logger.warn("stale_price_target_scan_failed", { detail: error?.message || String(error) });
    return { ok: false, error: error?.message };
  }
}

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
      // Stale target_price scan: reset products whose stored price contradicts current PM×rate.
      // Runs fire-and-forget — don't let a scan failure abort the daily sync.
      runStalePriceTargetScan().catch((err) =>
        logger.warn("stale price target scan failed in daily refresh", { detail: err?.message || String(err) }),
      );

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
      recordAppError?.("pm_sync", "02f-daily-maintenance-schedulers/runDailyRefresh", error?.message || String(error), { trigger });
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
    pruneOldAppErrors?.().catch(() => {});
    dailySyncPromise = null;
  });

  return dailySyncPromise;
}

function scheduleDailySync() {
  if (!dailySyncEnabled) return;
  if (dailySyncTimer) clearTimeout(dailySyncTimer);
  const delay = msUntilNextDailyRun(dailySyncTime);
  dailySyncNextRunAt = new Date(Date.now() + delay).toISOString();
  // Persist nextRunAt so the API process can read it from daily-sync.json
  writeDailySyncState({}).catch(() => {});
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
      // Авто-заполнение ТН ВЭД: код берётся из справочника WB по предмету (auto-discovery).
      // Не требует ручного ввода кода.
      let tnvedResult = null;
      try {
        const wbAcc = getWbAccountByTarget("wb");
        let resolvedCode = null;

        // WB: самостоятельно определяет код по subjectId правил импорта
        const wbTnved = wbAcc
          ? await backfillWbTnvedCharacteristics(wbAcc).catch((e) => ({ ok: false, error: e?.message }))
          : null;
        if (wbTnved?.tnved) resolvedCode = wbTnved.tnved;

        // Фолбэк: сохранённый код в настройках (если WB не настроен или не дал кода)
        if (!resolvedCode) {
          const appSettings = await readAppSettings();
          resolvedCode = cleanText(appSettings.tnved?.code) || null;
        }

        if (resolvedCode) {
          const ozonAcc = getOzonAccountByTarget("ozon");
          // OZON_TNVED_AUTO_BACKFILL_ENABLED=true чтобы включить (default: выключено).
          // Каждый запуск проходит ~12 000 товаров через /v1/product/attributes/update —
          // выжигает суточную квоту и не оставляет лимита для добавления новых карточек.
          const ozonTnvedAutoEnabled = process.env.OZON_TNVED_AUTO_BACKFILL_ENABLED === "true";
          const ozonTnved = (!ozonTnvedAutoEnabled || !ozonAcc)
            ? { ok: true, skipped: true, reason: ozonTnvedAutoEnabled ? "no_ozon_account" : "disabled_by_env" }
            : await ozonBackfillTnved(ozonAcc, resolvedCode, { dryRun: false }).catch((e) => ({ ok: false, error: e?.message }));
          const yandexTnved = await yandexBackfillTnved(resolvedCode, { dryRun: false })
            .catch((e) => ({ ok: false, error: e?.message }));
          tnvedResult = { code: resolvedCode, wb: wbTnved, ozon: ozonTnved, yandex: yandexTnved };
          logger.info("marketplace maintenance tnved auto-backfill complete", tnvedResult);
        }
      } catch (error) {
        logger.warn("marketplace maintenance tnved auto-backfill failed", { detail: error?.message || String(error) });
      }

      const result = {
        status: "ok",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        tnvedAutoBackfill: tnvedResult,
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
      recordAppError?.("pm_sync", "02f-daily-maintenance-schedulers", error?.message || String(error), { trigger });
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

// ─── Ночной сброс "Нужен код маркировки" ────────────────────────────────────
// ОТКЛЮЧЁН: ozonApplyTnvedByCategory уже записывает marking=false в том же
// вызове /v1/product/attributes/update, что и ТН ВЭД. Нет нужды гонять
// отдельный полный проход (12 000+ товаров) каждую ночь — это выжигало всю
// дневную квоту к 00:05 UTC и не оставляло лимита для добавления новых карточек.
// Для ручного запуска используй POST /api/ozon/attributes/clear-marking.
let ozonMarkingClearNightlyTimer = null;
let ozonMarkingClearNightlyNextRunAt = null;

function scheduleOzonMarkingClearNightly(_delayMs = null) {
  // no-op: scheduler disabled — see comment above
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
