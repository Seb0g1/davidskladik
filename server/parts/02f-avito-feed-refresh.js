// Фоновый рефреш фида Avito: периодически переносит актуальные цены и остатки
// склада в сохранённые объявления (avito-listings.json). Живое обогащение в
// buildAvitoFeedXml делает фид свежим в момент скачивания, а рефреш держит
// файл и страницу /app/avito в актуальном состоянии и служит фолбэком, если
// Postgres недоступен при отдаче фида.

const avitoFeedRefreshEnabled = process.env.AVITO_FEED_REFRESH_ENABLED !== "false";
const avitoFeedRefreshIntervalMs = Math.max(5 * 60_000, Number(process.env.AVITO_FEED_REFRESH_MINUTES || 30) * 60_000 || 30 * 60_000);
let avitoFeedRefreshTimer = null;
let avitoFeedRefreshRunning = false;
let avitoFeedRefreshNextRunAt = null;
let avitoFeedRefreshLastResult = null;

// Авто-синк фида: каждые N часов полностью переимпортирует привязанные товары —
// добавляет новые (появился поставщик, добавили привязку) и удаляет потерявшие
// поставщика. Управляется AVITO_AUTO_SYNC_ENABLED / AVITO_AUTO_SYNC_HOURS.
const avitoAutoSyncEnabled = process.env.AVITO_AUTO_SYNC_ENABLED !== "false";
const avitoAutoSyncIntervalMs = Math.max(60 * 60_000, Number(process.env.AVITO_AUTO_SYNC_HOURS || 4) * 60 * 60_000 || 4 * 60 * 60_000);
let avitoAutoSyncTimer = null;
let avitoAutoSyncRunning = false;
let avitoAutoSyncNextRunAt = null;
let avitoAutoSyncLastResult = null;

async function runAvitoFeedRefresh({ source = "schedule" } = {}) {
  if (avitoFeedRefreshRunning) return { status: "already_running" };
  avitoFeedRefreshRunning = true;
  try {
    const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
    if (!state.items.length) {
      return { status: "empty", updatedPrices: 0, outOfStock: 0, total: 0 };
    }
    const liveStates = await loadAvitoLiveProductStates(state.items);
    if (liveStates === null) return { status: "postgres_unavailable" };
    const pricing = await loadAvitoPricingContext();
    // Снапшот поставщика в raw есть не у всех товаров (авто-архивные на Ozon
    // могут никогда не получать отправку цены) — добираем живым расчётом из
    // PriceMaster. Делаем всегда: outOfStock = false если поставщик даёт цену,
    // независимо от autoUpdatePrices; результат сохраняется в файл и потом
    // используется buildAvitoFeedXml как фолбэк без живого запроса в PM.
    {
      const missingIds = [...liveStates.entries()]
        .filter(([, liveState]) => liveState && !liveState.supplier)
        .map(([id]) => id);
      if (missingIds.length) {
        const supplierMap = await loadAvitoSupplierPricingMap(missingIds);
        for (const id of missingIds) {
          const supplier = supplierMap.get(id);
          if (supplier) liveStates.get(id).supplier = supplier;
        }
      }
    }

    const syncedAt = new Date().toISOString();
    let updatedPrices = 0;
    let outOfStockCount = 0;
    let reclassified = 0;
    let changed = false;
    const nextItems = state.items.map((item) => {
      const sourceProductId = cleanText(item.sourceProductId);
      if (!sourceProductId) return item;
      const { listing, outOfStock } = applyAvitoLiveState(item, liveStates.get(sourceProductId), rules, pricing);
      if (outOfStock) outOfStockCount += 1;
      if (listing.priceRub !== item.priceRub) updatedPrices += 1;
      if (listing.priceRub !== item.priceRub || outOfStock !== (item.outOfStock === true) || listing.stockQuantity !== item.stockQuantity) changed = true;
      const next = { ...listing, outOfStock, lastSyncedAt: syncedAt };
      // Переклассификация по актуальному справочнику: например, пробники должны
      // уходить с PerfumeryType «Пробники и отливанты» — валидатор Avito
      // отклонял старые объявления с типом «Духи и туалетная вода».
      const classification = classifyAvitoCategory(item.title, rules);
      const spec = classification.spec;
      if (spec && spec.key !== cleanText(item.categoryKey)) {
        next.categoryKey = spec.key;
        next.categoryAutoDefaulted = classification.autoDefaulted;
        const gender = spec.gender ? detectAvitoPerfumeGender(item.title) : "";
        const perfumeType = spec.gender ? detectAvitoPerfumeType(item.title) : "";
        const volume = spec.gender ? detectAvitoVolumeMl(item.title) : "";
        const newExtra = { ...(item.extraFields || {}) };
        if (gender) newExtra.Gender = gender; else delete newExtra.Gender;
        if (perfumeType) newExtra.PerfumeType = perfumeType; else delete newExtra.PerfumeType;
        if (volume) newExtra.Volume = volume; else delete newExtra.Volume;
        next.extraFields = newExtra;
        reclassified += 1;
        changed = true;
      } else if (spec?.gender) {
        // Обогащение PerfumeType/Volume у уже правильно классифицированных объявлений.
        const perfumeType = detectAvitoPerfumeType(item.title);
        const volume = detectAvitoVolumeMl(item.title);
        const needsPerfumeType = perfumeType && !(item.extraFields?.PerfumeType);
        const needsVolume = volume && !(item.extraFields?.Volume);
        if (needsPerfumeType || needsVolume) {
          next.extraFields = {
            ...(item.extraFields || {}),
            ...(needsPerfumeType ? { PerfumeType: perfumeType } : {}),
            ...(needsVolume ? { Volume: volume } : {}),
          };
          changed = true;
        }
      }
      return next;
    });

    // lastSyncedAt меняется всегда — пишем файл только при реальных изменениях,
    // чтобы не дёргать диск каждые полчаса впустую.
    if (changed) await writeAvitoListingsFile({ ...state, items: nextItems });

    const result = {
      status: "ok",
      source,
      total: state.items.length,
      updatedPrices,
      outOfStock: outOfStockCount,
      reclassified,
      persisted: changed,
      at: syncedAt,
    };
    avitoFeedRefreshLastResult = result;
    if (changed) logger.info("avito feed refresh applied", result);
    return result;
  } catch (error) {
    const result = { status: "error", error: error?.message || String(error), at: new Date().toISOString() };
    avitoFeedRefreshLastResult = result;
    logger.warn("avito feed refresh failed", { detail: result.error });
    return result;
  } finally {
    avitoFeedRefreshRunning = false;
  }
}

function scheduleAvitoFeedRefresh(delayMs = avitoFeedRefreshIntervalMs) {
  if (!avitoFeedRefreshEnabled) {
    avitoFeedRefreshNextRunAt = null;
    return;
  }
  if (avitoFeedRefreshTimer) clearTimeout(avitoFeedRefreshTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs) || avitoFeedRefreshIntervalMs);
  avitoFeedRefreshNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  avitoFeedRefreshTimer = setTimeout(async () => {
    try {
      await runAvitoFeedRefresh({ source: "schedule" });
    } catch (error) {
      logger.warn("avito feed refresh tick failed", { detail: error?.message || String(error) });
    }
    // Фоновое дозаполнение описаний с Ozon — порция за цикл, пока не кончатся
    // объявления без description.
    try {
      await backfillAvitoListingDescriptionsFromOzon({ source: "schedule" });
    } catch (error) {
      logger.warn("avito description backfill tick failed", { detail: error?.message || String(error) });
    }
    // Фоновое дозаполнение фото: без фото объявление скрыто из XML, бэкфилл
    // возвращает его в фид (Postgres → Ozon API).
    try {
      await backfillAvitoListingImages({ source: "schedule" });
    } catch (error) {
      logger.warn("avito image backfill tick failed", { detail: error?.message || String(error) });
    }
    scheduleAvitoFeedRefresh(avitoFeedRefreshIntervalMs);
  }, normalizedDelay);
  avitoFeedRefreshTimer.unref?.();
}

async function runAvitoAutoSync({ source = "schedule" } = {}) {
  if (avitoAutoSyncRunning) return { status: "already_running" };
  avitoAutoSyncRunning = true;
  try {
    const result = await syncAvitoOzonListings();
    const summary = {
      status: "ok",
      source,
      ...result,
      at: new Date().toISOString(),
    };
    avitoAutoSyncLastResult = summary;
    logger.info("avito auto sync complete", summary);
    return summary;
  } catch (error) {
    const summary = { status: "error", source, error: error?.message || String(error), at: new Date().toISOString() };
    avitoAutoSyncLastResult = summary;
    logger.warn("avito auto sync failed", { detail: summary.error });
    return summary;
  } finally {
    avitoAutoSyncRunning = false;
  }
}

function scheduleAvitoAutoSync(delayMs = avitoAutoSyncIntervalMs) {
  if (!avitoAutoSyncEnabled) {
    avitoAutoSyncNextRunAt = null;
    return;
  }
  if (avitoAutoSyncTimer) clearTimeout(avitoAutoSyncTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || avitoAutoSyncIntervalMs);
  avitoAutoSyncNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  avitoAutoSyncTimer = setTimeout(async () => {
    try {
      await runAvitoAutoSync({ source: "schedule" });
    } catch (error) {
      logger.warn("avito auto sync tick failed", { detail: error?.message || String(error) });
    }
    scheduleAvitoAutoSync(avitoAutoSyncIntervalMs);
  }, normalizedDelay);
  avitoAutoSyncTimer.unref?.();
}
