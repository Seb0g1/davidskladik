// Методы API Wildberries: пинг/профиль продавца, справочник предметов
// (категорий), карточки товаров, фото, штрихкоды, цены и остатки FBS.
// Все методы принимают account из настроек маркетплейсов (marketplace: "wb").

// --- Служебные ---

async function wbPing(account) {
  return wbRequest(account, "common", "GET", "/ping");
}

async function wbSellerInfo(account) {
  return wbRequest(account, "common", "GET", "/api/v1/seller-info");
}

// --- Справочник категорий (предметов) ---

async function wbParentSubjects(account) {
  const data = await wbRequest(account, "content", "GET", "/content/v2/object/parent/all?locale=ru");
  return Array.isArray(data?.data) ? data.data : [];
}

async function wbSearchSubjects(account, query, { limit = 30 } = {}) {
  const params = new URLSearchParams({ locale: "ru", limit: String(Math.max(1, Math.min(1000, limit))) });
  const name = cleanText(query);
  if (name) params.set("name", name);
  const data = await wbRequest(account, "content", "GET", `/content/v2/object/all?${params.toString()}`);
  return Array.isArray(data?.data) ? data.data : [];
}

async function wbSubjectCharacteristics(account, subjectId) {
  const id = Number(subjectId);
  if (!Number.isFinite(id) || id <= 0) throw new Error("Нужен subjectId предмета WB.");
  const data = await wbRequest(account, "content", "GET", `/content/v2/object/charcs/${id}?locale=ru`);
  return Array.isArray(data?.data) ? data.data : [];
}

// Справочник кодов ТН ВЭД по предмету: [{ tnVed, isKiz }]. search — фильтр по
// началу кода (опционально).
async function wbTnvedList(account, subjectId, search = "") {
  const id = Number(subjectId);
  if (!Number.isFinite(id) || id <= 0) throw new Error("Нужен subjectId предмета WB.");
  const params = new URLSearchParams({ subjectID: String(id), locale: "ru" });
  const query = cleanText(search);
  if (query) params.set("search", query);
  const data = await wbRequest(account, "content", "GET", `/content/v2/directory/tnved?${params.toString()}`);
  return Array.isArray(data?.data) ? data.data : [];
}

// --- Штрихкоды ---

async function wbGenerateBarcodes(account, count = 1) {
  const amount = Math.max(1, Math.min(5000, Number(count) || 1));
  const data = await wbRequest(account, "content", "POST", "/content/v2/barcodes", { count: amount });
  return Array.isArray(data?.data) ? data.data : [];
}

// --- Карточки ---

// Полный список карточек кабинета (курсорная пагинация WB: updatedAt + nmID).
async function wbCardsList(account, { limit = Number.POSITIVE_INFINITY, textSearch = "", withPhoto = -1 } = {}) {
  const maxItems = Number.isFinite(Number(limit)) && Number(limit) > 0 ? Number(limit) : Number.MAX_SAFE_INTEGER;
  const cards = [];
  let cursor = { limit: Math.min(100, maxItems) };
  while (cards.length < maxItems) {
    const settings = {
      cursor: { ...cursor, limit: Math.min(100, maxItems - cards.length) },
      filter: compactObject({
        withPhoto,
        textSearch: cleanText(textSearch) || undefined,
      }),
    };
    const data = await wbRequest(account, "content", "POST", "/content/v2/get/cards/list", { settings });
    const pageCards = Array.isArray(data?.cards) ? data.cards : [];
    cards.push(...pageCards);
    const nextCursor = data?.cursor || {};
    if (!pageCards.length || pageCards.length < settings.cursor.limit) break;
    cursor = { updatedAt: nextCursor.updatedAt, nmID: nextCursor.nmID };
    if (!cursor.updatedAt || !cursor.nmID) break;
  }
  return cards.slice(0, maxItems);
}

// Создание карточек: [{ subjectID, variants: [{ vendorCode, title, description,
// brand, dimensions, sizes: [{ techSize, wbSize, skus: [barcode] }] }] }].
// Создание асинхронное: ошибки приходят позже в /cards/error/list.
async function wbCardsUpload(account, cards = []) {
  if (!Array.isArray(cards) || !cards.length) return { ok: true, uploaded: 0 };
  const data = await wbRequest(account, "content", "POST", "/content/v2/cards/upload", cards);
  if (data?.error) {
    const error = new Error(cleanText(data.errorText) || "WB отклонил загрузку карточек");
    error.wb = data;
    throw error;
  }
  return { ok: true, uploaded: cards.reduce((sum, card) => sum + (card.variants?.length || 0), 0), result: data };
}

async function wbCardsUpdate(account, cards = []) {
  if (!Array.isArray(cards) || !cards.length) return { ok: true, updated: 0 };
  const data = await wbRequest(account, "content", "POST", "/content/v2/cards/update", cards);
  if (data?.error) {
    const error = new Error(cleanText(data.errorText) || "WB отклонил обновление карточек");
    error.wb = data;
    throw error;
  }
  return { ok: true, updated: cards.length, result: data };
}

// Необработанные ошибки создания карточек (vendorCode + список ошибок).
// С октября 2025 GET-версия отключена WB (PLUG-404): только POST с
// пагинацией page/limit (limit 100–1000).
async function wbCardErrors(account, { maxItems = 5000 } = {}) {
  const items = [];
  const limit = 1000;
  let page = 1;
  while (items.length < maxItems) {
    const data = await wbRequest(account, "content", "POST", "/content/v2/cards/error/list?locale=ru", { page, limit });
    const pageItems = Array.isArray(data?.data) ? data.data : [];
    items.push(...pageItems);
    if (pageItems.length < limit) break;
    page += 1;
  }
  return items.slice(0, maxItems);
}

// Фото по URL для существующей карточки (nmID уже присвоен).
async function wbMediaSave(account, nmId, urls = []) {
  const data = Array.isArray(urls) ? urls.map((url) => cleanText(url)).filter(Boolean) : [];
  if (!Number(nmId) || !data.length) return { ok: false, error: "Нужны nmId и список URL фото." };
  const result = await wbRequest(account, "content", "POST", "/content/v3/media/save", { nmId: Number(nmId), data });
  return { ok: !result?.error, result };
}

// --- Цены ---

// Установка цен: [{ nmID, price, discount }]. Цена — до скидки; выставляем
// финальную цену как price с discount 0.
async function wbSetPrices(account, items = []) {
  const data = (Array.isArray(items) ? items : [])
    .map((item) => ({
      nmID: Number(item.nmID || item.nmId || 0),
      price: Math.max(1, Math.round(Number(item.price || 0))),
      discount: Math.max(0, Math.min(99, Math.round(Number(item.discount || 0)))),
    }))
    .filter((item) => item.nmID > 0 && item.price > 0);
  if (!data.length) return { ok: true, sent: 0 };
  const results = [];
  for (const chunk of chunkArray(data, 1000)) {
    const result = await wbRequest(account, "prices", "POST", "/api/v2/upload/task", { data: chunk });
    results.push(result);
  }
  return { ok: true, sent: data.length, tasks: results.map((result) => result?.data?.id).filter(Boolean) };
}

async function wbGoodsPrices(account, { limit = 1000, offset = 0, filterNmID = 0 } = {}) {
  const params = new URLSearchParams({
    limit: String(Math.max(1, Math.min(1000, limit))),
    offset: String(Math.max(0, offset)),
  });
  if (Number(filterNmID) > 0) params.set("filterNmID", String(Number(filterNmID)));
  const data = await wbRequest(account, "prices", "GET", `/api/v2/list/goods/filter?${params.toString()}`);
  return Array.isArray(data?.data?.listGoods) ? data.data.listGoods : [];
}

// --- Склады и остатки (FBS) ---

async function wbWarehouses(account) {
  const data = await wbRequest(account, "marketplace", "GET", "/api/v3/warehouses");
  return Array.isArray(data) ? data : [];
}

// Обновление остатков FBS: sku = штрихкод размера карточки.
async function wbUpdateStocks(account, warehouseId, stocks = []) {
  const id = Number(warehouseId);
  if (!Number.isFinite(id) || id <= 0) throw new Error("Нужен ID склада WB (FBS). Список: GET /api/wb/warehouses.");
  const payload = (Array.isArray(stocks) ? stocks : [])
    .map((item) => ({ sku: cleanText(item.sku || item.barcode), amount: Math.max(0, Math.round(Number(item.amount || 0))) }))
    .filter((item) => item.sku);
  if (!payload.length) return { ok: true, sent: 0 };
  for (const chunk of chunkArray(payload, 1000)) {
    await wbRequest(account, "marketplace", "PUT", `/api/v3/stocks/${id}`, { stocks: chunk });
  }
  return { ok: true, sent: payload.length };
}

async function wbGetStocks(account, warehouseId, skus = []) {
  const id = Number(warehouseId);
  if (!Number.isFinite(id) || id <= 0) throw new Error("Нужен ID склада WB (FBS).");
  const list = (Array.isArray(skus) ? skus : []).map((sku) => cleanText(sku)).filter(Boolean);
  const stocks = [];
  for (const chunk of chunkArray(list, 1000)) {
    const data = await wbRequest(account, "marketplace", "POST", `/api/v3/stocks/${id}`, { skus: chunk });
    stocks.push(...(Array.isArray(data?.stocks) ? data.stocks : []));
  }
  return stocks;
}
