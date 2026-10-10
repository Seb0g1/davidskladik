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

// ─── Цены активных объявлений напрямую через API ──────────────────────────
// Автозагрузка идёт раз в неделю / при новых товарах, а цена должна меняться сразу: каждые
// AVITO_PRICE_SYNC_MINUTES активные объявления Авито сравниваются с ценой фида (тот же расчёт, что в фиде)
// и расходящиеся больше чем на 1% меняются методом POST /core/v1/items/{id}/update_price — без загрузки и
// без лимитов размещения. Падение цены больше чем в AVITO_PRICE_SYNC_MAX_JUMP раз не применяется, а пишется в лог;
// повышение уходит всегда (правило владельца, как у защиты цен маркетплейсов).
const avitoPriceSyncEnabled = process.env.AVITO_PRICE_SYNC_ENABLED !== "false";
const avitoPriceSyncIntervalMs = Math.max(10 * 60_000, Number(process.env.AVITO_PRICE_SYNC_MINUTES || 30) * 60_000 || 30 * 60_000);
const avitoPriceSyncMaxJump = Math.max(1.5, Number(process.env.AVITO_PRICE_SYNC_MAX_JUMP || 3) || 3);
const avitoAdIdCache = new Map(); // avito item id → feed ad id
// active Avito item ids from the last walk: the instant path touches only these (an archived or removed ad answers
// «Can not update item price in current status»)
let avitoActiveItemIds = new Set();
let avitoPriceSyncTimer = null;
let avitoPriceSyncRunning = false;
let avitoPriceSyncLast = null;

// Остатки тем же проходом: PUT /stock-management/1/stocks (до 200 объявлений за запрос, 100 запросов в минуту).
// Значение — то же, что <Stock> в фиде (listing.stockQuantity, 0 у «нет в наличии»), так что API и фид не спорят.
// Отправляется только изменившееся; что ушло — в data/avito-stock-sync.json. Раз в сутки уходит всё заново:
// Авито само уменьшает остаток при заказе, и без этого наше «5» больше не дошло бы.
const avitoStockSyncEnabled = process.env.AVITO_STOCK_SYNC_ENABLED !== "false";
const avitoStockSyncPath = path.join(dataDir, "avito-stock-sync.json");
const AVITO_STOCK_FULL_RESEND_MS = 24 * 60 * 60_000;

async function readAvitoStockSyncState() {
  try {
    const raw = JSON.parse(await fs.readFile(avitoStockSyncPath, "utf8"));
    return { sent: raw.sent && typeof raw.sent === "object" ? raw.sent : {}, fullAt: raw.fullAt || null };
  } catch {
    return { sent: {}, fullAt: null };
  }
}

async function writeAvitoStockSyncState(state) {
  await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
  const tmp = `${avitoStockSyncPath}.${process.pid}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(state));
  await fs.rename(tmp, avitoStockSyncPath);
}

/** Pure: the stock an ad must show — the feed's <Stock>; null = the feed has none (leave Avito's). */
function avitoWantedStock(listing) {
  if (!listing) return null;
  if (listing.outOfStock === true) return 0;
  const qty = Number(listing.stockQuantity);
  if (listing.stockQuantity === null || listing.stockQuantity === undefined || !Number.isFinite(qty)) return null;
  return Math.max(0, Math.min(999999, Math.round(qty)));
}

async function syncAvitoStocks(account, active, byAd, sleep) {
  const out = { stockChecked: 0, stockUpdated: 0, stockFailed: 0, stockErrors: [] };
  const state = await readAvitoStockSyncState();
  const full = !state.fullAt || Date.now() - Date.parse(state.fullAt) > AVITO_STOCK_FULL_RESEND_MS;
  const updates = [];
  for (const ad of active) {
    const want = avitoWantedStock(byAd.get(avitoAdIdCache.get(Number(ad.id))));
    if (want === null) continue;
    out.stockChecked += 1;
    if (!full && state.sent[String(ad.id)] === want) continue;
    updates.push({ item_id: Number(ad.id), quantity: want });
  }
  for (let i = 0; i < updates.length; i += 200) {
    const batch = updates.slice(i, i + 200);
    try {
      const r = await avitoRequest("/stock-management/1/stocks", { method: "PUT", account, body: { stocks: batch } });
      const byId = new Map((r?.stocks || []).map((row) => [Number(row.item_id), row]));
      for (const u of batch) {
        const row = byId.get(u.item_id);
        if (row && row.success === false) {
          out.stockFailed += 1;
          if (out.stockErrors.length < 10) out.stockErrors.push({ id: u.item_id, errors: row.errors || [] });
          continue;
        }
        state.sent[String(u.item_id)] = u.quantity;
        out.stockUpdated += 1;
      }
    } catch (error) {
      out.stockFailed += batch.length;
      if (out.stockErrors.length < 10) out.stockErrors.push({ batch: i / 200, error: error?.message || String(error) });
      // the whole method refused (not enabled for the account, wrong scope): no point sending the rest
      if ([401, 403, 404].includes(Number(error?.statusCode))) break;
    }
    await sleep(700);
  }
  // ads no longer active drop out of the memory, so a reactivated ad gets its stock again
  const activeIds = new Set(active.map((ad) => String(ad.id)));
  for (const id of Object.keys(state.sent)) if (!activeIds.has(id)) delete state.sent[id];
  if (full && !out.stockFailed) state.fullAt = new Date().toISOString();
  await writeAvitoStockSyncState(state);
  return out;
}

async function runAvitoPriceSync({ source = "schedule" } = {}) {
  if (avitoPriceSyncRunning) return { status: "already_running" };
  const account = (getAvitoAccounts ? getAvitoAccounts() : [])[0];
  if (!account) return { status: "no_avito_account" };
  avitoPriceSyncRunning = true;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const result = { status: "ok", source, active: 0, checked: 0, updated: 0, skippedJump: [], failed: 0, at: new Date().toISOString() };
  try {
    const active = [];
    // no page cap at 99: the old «page < 100» stopped at 9 900 ads and ~1 400 active ads never got a price
    for (let page = 1; page <= 500; page += 1) {
      const r = await avitoRequest("/core/v1/items", { account, query: { status: "active", per_page: 100, page } });
      const list = r?.resources || [];
      active.push(...list);
      if (list.length < 100) break;
      await sleep(1000);
    }
    result.active = active.length;
    avitoActiveItemIds = new Set(active.map((a) => Number(a.id)));
    const unknown = active.map((a) => Number(a.id)).filter((id) => !avitoAdIdCache.has(id));
    for (let i = 0; i < unknown.length; i += 100) {
      const r = await getAvitoAdIdsByAvitoIds(account, unknown.slice(i, i + 100));
      for (const it of r?.items || []) if (it.ad_id) avitoAdIdCache.set(Number(it.avito_id), String(it.ad_id));
      await sleep(1000);
    }
    const state = await readAvitoListingsFile();
    const byAd = new Map(state.items.map((item) => [avitoFeedAdId(item.adId), item]));
    for (const ad of active) {
      const listing = byAd.get(avitoAdIdCache.get(Number(ad.id)));
      const want = Math.round(Number(listing?.priceRub) || 0);
      const have = Number(ad.price) || 0;
      if (!listing || !(want > 0) || listing.outOfStock === true) continue;
      result.checked += 1;
      if (have > 0 && Math.abs(want - have) / have <= 0.01) continue;
      // owner's rule (as the marketplaces' price guard): a rise goes out by itself, only a big drop waits
      if (have > 0 && have / want > avitoPriceSyncMaxJump) {
        result.skippedJump.push({ id: ad.id, title: cleanText(ad.title).slice(0, 60), avito: have, feed: want });
        continue;
      }
      try {
        await avitoRequest(`/core/v1/items/${ad.id}/update_price`, { method: "POST", account, body: { price: want } });
        result.updated += 1;
      } catch (error) {
        result.failed += 1;
        logger.warn("avito price update failed", { id: ad.id, detail: error?.message || String(error) });
      }
      await sleep(400);
    }
    if (avitoStockSyncEnabled) {
      try {
        Object.assign(result, await syncAvitoStocks(account, active, byAd, sleep));
        if (result.stockErrors.length) logger.warn("avito stock update errors", { errors: result.stockErrors });
      } catch (error) {
        result.stockError = error?.message || String(error);
        logger.warn("avito stock sync failed", { detail: result.stockError });
      }
    }
    if (result.skippedJump.length) logger.warn("avito price jumps not applied", { items: result.skippedJump.slice(0, 20) });
    logger.info("avito price sync", { ...result, skippedJump: result.skippedJump.length });
    return result;
  } catch (error) {
    result.status = "error";
    result.error = error?.message || String(error);
    logger.warn("avito price sync failed", { detail: result.error });
    return result;
  } finally {
    avitoPriceSyncLast = { ...result, skippedJump: result.skippedJump.slice(0, 50) };
    avitoPriceSyncRunning = false;
  }
}

// ─── Мгновенно: цена и остаток на Авито сразу после реального изменения цены ──────────────
// sendWarehousePrices (привязка, новый прайс поставщика, смена поставщика — любой процесс) ставит товары, чья цена
// только что ушла на Ozon / Маркет, в avito_price_queue. Воркер каждые AVITO_INSTANT_PRICE_SECONDS (20) берёт их,
// пересчитывает объявление тем же applyAvitoLiveState, что фид, сохраняет в файл объявлений и сразу меняет цену
// (update_price) и остаток (stock-management) на Авито. Полный обход раз в 30 минут остаётся страховкой.
const avitoInstantPriceEnabled = process.env.AVITO_INSTANT_PRICE !== "false";
const avitoInstantPriceIntervalMs = Math.max(5, Number(process.env.AVITO_INSTANT_PRICE_SECONDS || 20) || 20) * 1000;
let avitoPriceQueueReady = false;
let avitoInstantPriceTimer = null;
let avitoInstantPriceRunning = false;
let avitoInstantPriceLast = null;

async function ensureAvitoPriceQueue() {
  if (avitoPriceQueueReady) return true;
  const prisma = getPrisma();
  if (!prisma) return false;
  await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS avito_price_queue (product_id TEXT PRIMARY KEY, queued_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
  avitoPriceQueueReady = true;
  return true;
}

/** Products whose marketplace price just changed: Avito follows within seconds. Never throws. */
async function queueAvitoPriceRefresh(productIds = []) {
  if (!avitoInstantPriceEnabled) return 0;
  const ids = [...new Set((productIds || []).map((id) => cleanText(id)).filter(Boolean))];
  if (!ids.length) return 0;
  try {
    if (!(await ensureAvitoPriceQueue())) return 0;
    await getPrisma().$executeRawUnsafe(
      `INSERT INTO avito_price_queue (product_id) SELECT unnest($1::text[]) ON CONFLICT (product_id) DO UPDATE SET queued_at = now()`, ids);
    return ids.length;
  } catch (error) {
    logger.warn("avito price queue insert failed", { detail: error?.message || String(error) });
    return 0;
  }
}

// Any change of a warehouse product behind an ad — a link removed or added, archived, another supplier — is queued
// too, not only a price send: Serge Lutens Arabie lost its link and kept «в наличии, 5» on Avito until the next
// full refresh. Products of the ad's offer on every marketplace count (a link can live on the Market twin).
let avitoChangeWatermark = new Date(Date.now() - 2 * 60_000);
let avitoSourceByOfferCache = { at: 0, map: new Map() };
async function avitoSourceByOffer() {
  if (Date.now() - avitoSourceByOfferCache.at > 5 * 60_000) {
    const state = await readAvitoListingsFile();
    const map = new Map();
    for (const item of state.items || []) {
      const offer = cleanText(item.sourceOfferId).toLowerCase();
      if (offer && cleanText(item.sourceProductId)) map.set(offer, cleanText(item.sourceProductId));
    }
    avitoSourceByOfferCache = { at: Date.now(), map };
  }
  return avitoSourceByOfferCache.map;
}

async function queueChangedAvitoSources() {
  const since = avitoChangeWatermark;
  const now = new Date();
  const byOffer = await avitoSourceByOffer();
  if (!byOffer.size) return 0;
  const rows = await getPrisma().$queryRawUnsafe(
    `SELECT DISTINCT lower(offer_id) AS offer FROM warehouse_products WHERE updated_at > $1 AND lower(offer_id) = ANY($2::text[])`,
    since, [...byOffer.keys()]);
  avitoChangeWatermark = now;
  return queueAvitoPriceRefresh(rows.map((row) => byOffer.get(cleanText(row.offer))).filter(Boolean));
}

async function drainAvitoPriceQueue() {
  if (avitoInstantPriceRunning || !(await ensureAvitoPriceQueue().catch(() => false))) return null;
  const account = (getAvitoAccounts ? getAvitoAccounts() : [])[0];
  if (!account) return null;
  await queueChangedAvitoSources().catch((error) => logger.warn("avito changed sources queue failed", { detail: error?.message || String(error) }));
  // right after a restart the walk has not listed the active ads yet: the queue waits for it
  if (!avitoActiveItemIds.size) return null;
  avitoInstantPriceRunning = true;
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const result = { at: new Date().toISOString(), queued: 0, listings: 0, priceUpdated: 0, priceHeld: 0, stockUpdated: 0, failed: 0, noAvitoId: 0, inactive: 0 };
  let taken = [];
  try {
    taken = (await getPrisma().$queryRawUnsafe(
      `DELETE FROM avito_price_queue WHERE product_id IN (SELECT product_id FROM avito_price_queue ORDER BY queued_at LIMIT 400) RETURNING product_id`))
      .map((row) => cleanText(row.product_id));
    result.queued = taken.length;
    if (!taken.length) return null;
    const ids = new Set(taken);
    const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
    const listings = state.items.filter((item) => item.enabled !== false && ids.has(cleanText(item.sourceProductId)));
    result.listings = listings.length;
    if (!listings.length) return result;
    const liveStates = await loadAvitoLiveProductStates(listings);
    if (liveStates === null) throw new Error("postgres unavailable");
    const pricing = await loadAvitoPricingContext();
    const missing = [...liveStates.entries()].filter(([, live]) => live && !live.supplier).map(([id]) => id);
    if (missing.length) {
      const supplierMap = await loadAvitoSupplierPricingMap(missing);
      for (const id of missing) { const supplier = supplierMap.get(id); if (supplier) liveStates.get(id).supplier = supplier; }
    }
    const next = new Map();
    for (const item of listings) {
      const { listing, outOfStock } = applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules, pricing);
      next.set(avitoFeedAdId(item.adId), { before: item, after: { ...listing, outOfStock, lastSyncedAt: result.at } });
    }
    // the listings file: the feed shows the same price at its next download
    const fresh = await readAvitoListingsFile();
    await writeAvitoListingsFile({ ...fresh, items: fresh.items.map((item) => next.get(avitoFeedAdId(item.adId))?.after || item) });

    // feed ad id → Avito item id (the 30-minute sync fills the cache; the rest is asked from autoload)
    const avitoIdByAd = new Map();
    for (const [avitoId, adId] of avitoAdIdCache.entries()) avitoIdByAd.set(adId, avitoId);
    const unknown = [...next.keys()].filter((adId) => !avitoIdByAd.has(adId));
    for (let i = 0; i < unknown.length; i += 100) {
      const r = await getAvitoIdsByAdIds(account, unknown.slice(i, i + 100)).catch(() => null);
      for (const it of r?.items || []) {
        if (!it.avito_id || !it.ad_id) continue;
        avitoIdByAd.set(String(it.ad_id), Number(it.avito_id));
        avitoAdIdCache.set(Number(it.avito_id), String(it.ad_id));
      }
    }
    const stockState = avitoStockSyncEnabled ? await readAvitoStockSyncState() : null;
    const stockUpdates = [];
    for (const [adId, { before, after }] of next) {
      const avitoId = avitoIdByAd.get(adId);
      if (!avitoId) { result.noAvitoId += 1; continue; }
      if (!avitoActiveItemIds.has(Number(avitoId))) { result.inactive += 1; continue; }
      const want = Math.round(Number(after.priceRub) || 0);
      const was = Math.round(Number(before.priceRub) || 0);
      if (!after.outOfStock && want > 0 && want !== was) {
        // owner's rule: a rise goes out, a drop over AVITO_PRICE_SYNC_MAX_JUMP waits for a person
        if (was > 0 && was / want > avitoPriceSyncMaxJump) {
          result.priceHeld += 1;
          logger.warn("avito instant price drop held", { adId, from: was, to: want });
        } else {
          try {
            await avitoRequest(`/core/v1/items/${avitoId}/update_price`, { method: "POST", account, body: { price: want } });
            result.priceUpdated += 1;
          } catch (error) {
            result.failed += 1;
            logger.warn("avito instant price update failed", { adId, detail: error?.message || String(error) });
          }
          await sleep(300);
        }
      }
      const stock = stockState ? avitoWantedStock(after) : null;
      if (stock !== null && stockState.sent[String(avitoId)] !== stock) stockUpdates.push({ item_id: Number(avitoId), quantity: stock });
    }
    for (let i = 0; i < stockUpdates.length; i += 200) {
      const batch = stockUpdates.slice(i, i + 200);
      try {
        const r = await avitoRequest("/stock-management/1/stocks", { method: "PUT", account, body: { stocks: batch } });
        const refused = new Set((r?.stocks || []).filter((row) => row.success === false).map((row) => Number(row.item_id)));
        for (const u of batch) {
          if (refused.has(u.item_id)) { result.failed += 1; continue; }
          stockState.sent[String(u.item_id)] = u.quantity;
          result.stockUpdated += 1;
        }
      } catch (error) {
        result.failed += batch.length;
        logger.warn("avito instant stock update failed", { detail: error?.message || String(error) });
      }
    }
    if (stockUpdates.length) await writeAvitoStockSyncState(stockState);
    logger.info("avito instant price", result);
    return result;
  } catch (error) {
    result.error = error?.message || String(error);
    logger.warn("avito instant price failed", { detail: result.error });
    // nothing is lost: the products go back to the queue for the next tick
    await queueAvitoPriceRefresh(taken);
    return result;
  } finally {
    if (result.queued) avitoInstantPriceLast = result;
    avitoInstantPriceRunning = false;
  }
}

function scheduleAvitoInstantPrice(delayMs = avitoInstantPriceIntervalMs) {
  if (!avitoInstantPriceEnabled) return;
  if (avitoInstantPriceTimer) clearTimeout(avitoInstantPriceTimer);
  avitoInstantPriceTimer = setTimeout(async () => {
    try {
      await drainAvitoPriceQueue();
    } catch (error) {
      logger.warn("avito instant price tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleAvitoInstantPrice(avitoInstantPriceIntervalMs);
    }
  }, Math.max(5_000, Number(delayMs) || avitoInstantPriceIntervalMs));
  avitoInstantPriceTimer.unref?.();
}

function scheduleAvitoPriceSync(delayMs = avitoPriceSyncIntervalMs) {
  if (!avitoPriceSyncEnabled) return;
  if (avitoPriceSyncTimer) clearTimeout(avitoPriceSyncTimer);
  avitoPriceSyncTimer = setTimeout(async () => {
    try {
      await runAvitoPriceSync();
    } finally {
      scheduleAvitoPriceSync(avitoPriceSyncIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || avitoPriceSyncIntervalMs));
  avitoPriceSyncTimer.unref?.();
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
    priceSyncEnabled: avitoPriceSyncEnabled,
    priceSyncLast: avitoPriceSyncLast,
    instantPriceEnabled: avitoInstantPriceEnabled,
    instantPriceLast: avitoInstantPriceLast,
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
