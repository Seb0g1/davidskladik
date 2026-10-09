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

// Авто-триггер загрузки: после refresh с изменениями говорим Avito скачать фид
// прямо сейчас. Лимит Avito — раз в час; держим минимальный интервал 55 мин.
const avitoAutoUploadEnabled = process.env.AVITO_AUTO_UPLOAD_ENABLED !== "false";
const avitoAutoUploadMinIntervalMs = Math.max(55 * 60_000, Number(process.env.AVITO_AUTO_UPLOAD_MIN_MINUTES || 55) * 60_000 || 55 * 60_000);
let avitoLastUploadTriggerAt = null;
let avitoLastUploadTriggerResult = null;
let avitoUploadTriggerRunning = false;

async function runAvitoUploadTriggerIfNeeded({ force = false } = {}) {
  if (avitoUploadTriggerRunning) return { skipped: true, reason: "already_running" };
  const accounts = getAvitoAccounts ? getAvitoAccounts() : [];
  if (!accounts.length) return { skipped: true, reason: "no_avito_account" };
  const account = accounts[0];
  const now = Date.now();
  const lastMs = avitoLastUploadTriggerAt ? new Date(avitoLastUploadTriggerAt).getTime() : 0;
  if (!force && now - lastMs < avitoAutoUploadMinIntervalMs) {
    return { skipped: true, reason: "rate_limited", nextAllowedAt: new Date(lastMs + avitoAutoUploadMinIntervalMs).toISOString() };
  }
  avitoUploadTriggerRunning = true;
  try {
    const result = await triggerAvitoAutoloadUpload(account);
    avitoLastUploadTriggerAt = new Date().toISOString();
    avitoLastUploadTriggerResult = { ok: true, at: avitoLastUploadTriggerAt, result };
    logger.info("avito autoload upload triggered", { at: avitoLastUploadTriggerAt });
    return avitoLastUploadTriggerResult;
  } catch (error) {
    avitoLastUploadTriggerResult = { ok: false, at: new Date().toISOString(), error: error?.message || String(error) };
    logger.warn("avito autoload upload trigger failed", { detail: error?.message || String(error) });
    return avitoLastUploadTriggerResult;
  } finally {
    avitoUploadTriggerRunning = false;
  }
}

// ─── Ежедневный запуск автозагрузки ───────────────────────────────────────
// Профиль автозагрузки (Magic Stick) скачивает фид ParfumDeclaration, а тот на лету берёт этот фид и
// добавляет документы. Расписание в кабинете Авито — раз в неделю; раз в день (AVITO_DAILY_UPLOAD_HOUR по
// Москве) сравниваем объявления фида с теми, что ушли в прошлую загрузку, и если появились новые товары
// или какие-то ушли из фида (продан, нет поставщика — Авито снимет их только при загрузке), просим Авито
// скачать фид сейчас (POST /autoload/v1/upload). Цены сами по себе загрузку не запускают.
const avitoDailyUploadEnabled = process.env.AVITO_DAILY_UPLOAD_ENABLED !== "false";
const avitoDailyUploadHour = Math.max(0, Math.min(23, Number(process.env.AVITO_DAILY_UPLOAD_HOUR ?? 10) || 0));
const avitoDailyUploadOn = new Set(String(process.env.AVITO_DAILY_UPLOAD_ON || "new,removed").split(",").map((s) => s.trim()).filter(Boolean));
const AVITO_DAILY_UPLOAD_KEY = "avito-daily-upload";
let avitoDailyUploadTimer = null;
let avitoDailyUploadLast = null;

/** Ads the feed publishes now: in stock and with photos (what buildAvitoFeedXml puts in, by its ad id). */
async function avitoPublishedAdIds() {
  const state = await readAvitoListingsFile();
  return new Set(state.items
    .filter((item) => item.outOfStock !== true && Array.isArray(item.imageUrls) && item.imageUrls.length)
    .map((item) => avitoFeedAdId(item.adId))
    .filter(Boolean));
}

async function runAvitoDailyUpload({ source = "schedule" } = {}) {
  const prisma = getPrisma();
  const row = await prisma.appSetting.findUnique({ where: { key: AVITO_DAILY_UPLOAD_KEY } }).catch(() => null);
  const saved = row?.value && typeof row.value === "object" ? row.value : {};
  const current = await avitoPublishedAdIds();
  const save = (value) => prisma.appSetting.upsert({ where: { key: AVITO_DAILY_UPLOAD_KEY }, create: { key: AVITO_DAILY_UPLOAD_KEY, value }, update: { value } });
  if (!Array.isArray(saved.ids)) {
    // the first run only remembers what the feed has; the next days compare against it
    await save({ ids: [...current], at: new Date().toISOString(), last: { status: "baseline", ads: current.size } });
    avitoDailyUploadLast = { status: "baseline", ads: current.size, at: new Date().toISOString(), source };
    logger.info("avito daily upload baseline", avitoDailyUploadLast);
    return avitoDailyUploadLast;
  }
  const before = new Set(saved.ids);
  const added = [...current].filter((id) => !before.has(id)).length;
  const removed = [...before].filter((id) => !current.has(id)).length;
  const needed = (added > 0 && avitoDailyUploadOn.has("new")) || (removed > 0 && avitoDailyUploadOn.has("removed"));
  let result = { status: "nothing_new", added, removed, ads: current.size, at: new Date().toISOString(), source };
  if (needed) {
    const trigger = await runAvitoUploadTriggerIfNeeded({ force: true });
    result = { ...result, status: trigger?.ok ? "uploaded" : "upload_failed", error: trigger?.error || null };
  }
  // the snapshot moves only when Avito was asked to download (or nothing had to be done)
  if (result.status !== "upload_failed") await save({ ids: [...current], at: result.at, last: result });
  else await save({ ...saved, last: result });
  avitoDailyUploadLast = result;
  logger.info("avito daily upload", result);
  return result;
}

function scheduleAvitoDailyUpload() {
  if (!avitoDailyUploadEnabled) return;
  if (avitoDailyUploadTimer) clearTimeout(avitoDailyUploadTimer);
  // the next AVITO_DAILY_UPLOAD_HOUR:00 by Moscow time (UTC+3)
  const now = Date.now();
  const msk = new Date(now + 3 * 3600_000);
  let next = Date.UTC(msk.getUTCFullYear(), msk.getUTCMonth(), msk.getUTCDate(), avitoDailyUploadHour) - 3 * 3600_000;
  if (next <= now + 60_000) next += 24 * 3600_000;
  avitoDailyUploadTimer = setTimeout(async () => {
    try {
      await runAvitoDailyUpload();
    } catch (error) {
      logger.warn("avito daily upload failed", { detail: error?.message || String(error) });
    } finally {
      scheduleAvitoDailyUpload();
    }
  }, next - now);
  avitoDailyUploadTimer.unref?.();
  logger.info("avito daily upload scheduled", { at: new Date(next).toISOString(), on: [...avitoDailyUploadOn] });
}

function avitoSyncPublic() {
  return {
    feedRefreshEnabled: avitoFeedRefreshEnabled,
    feedRefreshIntervalMs: avitoFeedRefreshIntervalMs,
    feedRefreshRunning: avitoFeedRefreshRunning,
    feedRefreshNextRunAt: avitoFeedRefreshNextRunAt,
    feedRefreshLastResult: avitoFeedRefreshLastResult,
    autoUploadEnabled: avitoAutoUploadEnabled,
    lastUploadTriggerAt: avitoLastUploadTriggerAt,
    lastUploadTriggerResult: avitoLastUploadTriggerResult,
    uploadTriggerRunning: avitoUploadTriggerRunning,
    dailyUploadEnabled: avitoDailyUploadEnabled,
    dailyUploadHour: avitoDailyUploadHour,
    dailyUploadLast: avitoDailyUploadLast,
  };
}

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
    // При изменении цен/остатков — говорим Avito скачать обновлённый фид прямо сейчас (только без ежедневного
    // режима: с ним Авито качает фид раз в день, когда в нём появились или ушли товары).
    if (changed && avitoAutoUploadEnabled && !avitoDailyUploadEnabled) {
      runAvitoUploadTriggerIfNeeded().catch((error) => {
        logger.warn("avito auto upload trigger failed after refresh", { detail: error?.message || String(error) });
      });
    }
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
