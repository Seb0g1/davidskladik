async function runManualWarehouseSync(trigger = "manual_sync") {
  setManualWarehouseSyncProgress({
    percent: 8,
    stage: "PriceMaster",
    meta: "Обновляю прайс, поставщиков и snapshot PriceMaster.",
    processed: 0,
    total: 0,
  });
  const priceMaster = await runSync();
  setManualWarehouseSyncProgress({
    percent: 24,
    stage: "Маркетплейсы",
    meta: `PriceMaster готов: ${formatRuNumber(priceMaster?.items || 0)} строк. Загружаю карточки Ozon/Yandex.`,
    processed: Number(priceMaster?.items || 0),
    total: Number(priceMaster?.items || 0),
  });
  const warehouse = await buildWarehouseView({
    sync: true,
    onProgress: (progress) => setManualWarehouseSyncProgress(progress),
  });
  setManualWarehouseSyncProgress({
    percent: 74,
    stage: "Склад",
    meta: `Карточки загружены: ${formatRuNumber(warehouse.total || 0)}. Сверяю поставщиков и правила остатков.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  const automation = await runNoSupplierMarketplaceAutomation(warehouse);
  setManualWarehouseSyncProgress({
    percent: 84,
    stage: "Автоматизация",
    meta: `Проверены пропавшие поставщики. Нулевые остатки: ${formatRuNumber(automation.zeroStockSent || 0)}, архив: ${formatRuNumber(automation.archived || 0)}.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  const recovery = await runSupplierRecoveryAutomation(warehouse);
  setManualWarehouseSyncProgress({
    percent: 92,
    stage: "Yandex",
    meta: "Материализую Yandex-строки для Ozon-экспортов.",
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  let yandexMaterialize = { added: 0, scanned: 0, skipped: 0 };
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const pairSync = await syncOzonYandexLinkPairsPostgres(prisma, { dryRun: false });
      const pairGroups = await backfillOzonYandexManualGroupsPostgres(prisma, { dryRun: false });
      yandexMaterialize = await materializeYandexExportedProductsForPostgres(prisma, { dryRun: false });
      yandexMaterialize.pairSync = pairSync;
      yandexMaterialize.pairGroups = pairGroups;
      logger.info("manual warehouse sync yandex materialize", yandexMaterialize);
      try {
        const smallVolumeCleanup = await deleteYandexSmallVolumeOffers({ dryRun: false, limit: 5000 });
        yandexMaterialize.smallVolumeCleanup = {
          planned: smallVolumeCleanup.planned || 0,
          deleted: smallVolumeCleanup.deleted || 0,
          failed: smallVolumeCleanup.failed || 0,
        };
        logger.info("manual warehouse sync yandex small volume cleanup", yandexMaterialize.smallVolumeCleanup);
      } catch (cleanupError) {
        logger.warn("manual warehouse sync yandex small volume cleanup failed", { detail: cleanupError?.message || String(cleanupError) });
      }
    } catch (error) {
      logger.warn("manual warehouse sync yandex materialize failed", { detail: error?.message || String(error) });
    }
  }
  setManualWarehouseSyncProgress({
    percent: 94,
    stage: "Финал",
    meta: `Восстановлено: ${formatRuNumber(recovery.recovered || 0)}. Yandex добавлено: ${formatRuNumber(yandexMaterialize.added || 0)}.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  return {
    ok: true,
    trigger,
    priceMaster,
    yandexMaterialize,
    warehouse: {
      total: warehouse.total,
      ready: warehouse.ready,
      changed: warehouse.changed,
      withoutSupplier: warehouse.withoutSupplier,
      supplierSync: warehouse.supplierSync,
      zeroStockSent: automation.zeroStockSent,
      autoArchived: automation.archived,
      recovered: recovery.recovered,
    },
  };
}

function getManualWarehouseSyncStatus() {
  return {
    ...manualWarehouseSyncState,
    running: manualWarehouseSyncState.status === "running",
  };
}

function startManualWarehouseSync(trigger = "manual") {
  if (manualWarehouseSyncPromise) return { started: false, status: getManualWarehouseSyncStatus() };
  const startedAt = new Date().toISOString();
  manualWarehouseSyncState = {
    status: "running",
    trigger,
    startedAt,
    finishedAt: null,
    result: null,
    error: null,
    progress: {
      percent: 3,
      stage: "Старт",
      meta: "Запуск фоновой синхронизации склада.",
      processed: 0,
      total: 0,
      updatedAt: startedAt,
    },
  };
  manualWarehouseSyncPromise = runManualWarehouseSync(trigger)
    .then((result) => {
      manualWarehouseSyncState = {
        status: "ok",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        result,
        error: null,
        progress: {
          ...(manualWarehouseSyncState.progress || {}),
          percent: 100,
          stage: "Готово",
          meta: `Синхронизация завершена. Карточек: ${formatRuNumber(result?.warehouse?.total || 0)}.`,
          processed: Number(result?.warehouse?.total || manualWarehouseSyncState.progress?.processed || 0),
          total: Number(result?.warehouse?.total || manualWarehouseSyncState.progress?.total || 0),
          updatedAt: new Date().toISOString(),
        },
      };
      return result;
    })
    .catch((error) => {
      const detail = error?.code || error?.message || String(error);
      manualWarehouseSyncState = {
        status: "failed",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        result: null,
        error: detail,
        progress: {
          ...(manualWarehouseSyncState.progress || {}),
          percent: 100,
          stage: "Ошибка",
          meta: detail,
          updatedAt: new Date().toISOString(),
        },
      };
      logger.error("manual warehouse sync failed", { detail, err: error });
      throw error;
    })
    .finally(() => {
      manualWarehouseSyncPromise = null;
    });
  manualWarehouseSyncPromise.catch(() => {});
  return { started: true, status: getManualWarehouseSyncStatus() };
}
