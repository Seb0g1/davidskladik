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

    const syncedAt = new Date().toISOString();
    let updatedPrices = 0;
    let outOfStockCount = 0;
    let changed = false;
    const nextItems = state.items.map((item) => {
      const sourceProductId = cleanText(item.sourceProductId);
      if (!sourceProductId) return item;
      const { listing, outOfStock } = applyAvitoLiveState(item, liveStates.get(sourceProductId), rules, pricing);
      if (outOfStock) outOfStockCount += 1;
      if (listing.priceRub !== item.priceRub) updatedPrices += 1;
      if (listing.priceRub !== item.priceRub || outOfStock !== (item.outOfStock === true)) changed = true;
      return { ...listing, outOfStock, lastSyncedAt: syncedAt };
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
    scheduleAvitoFeedRefresh(avitoFeedRefreshIntervalMs);
  }, normalizedDelay);
  avitoFeedRefreshTimer.unref?.();
}
