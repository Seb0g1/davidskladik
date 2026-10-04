// «Конвейер» страницы «Фрагрантика»: много ароматов за раз, минимум кликов.
//
//   POST   /api/fragrantica/drafts            — в конвейер: { perfumeIds } или { query } (фильтры списка, «выбрать все»)
//                                               + targets (магазины). Аромат раскладывается на объёмы из PriceMaster.
//   GET    /api/fragrantica/drafts            — черновики со стадиями и статусами отправки
//   PATCH  /api/fragrantica/drafts/:id        — правки (цена, название, магазины, привязки, описание, бренд, тип/объём)
//   POST   /api/fragrantica/drafts/:id/rebuild — собрать заново ({ newDescription } — переписать описание)
//   DELETE /api/fragrantica/drafts/:id
//   POST   /api/fragrantica/drafts/volume      — добавить объём аромату вручную
//   POST   /api/fragrantica/drafts/approve     — одобрить { ids } → worker создаёт карточки
//   POST   /api/fragrantica/drafts/clear       — убрать отправленные / пропущенные
//
// Сборка идёт на worker (runFragranticaDraftsTick): форма Ozon → привязка PriceMaster + цена → фото
// (флакон + «Пирамида аромата» каждого магазина, без ручного одобрения) → описание ИИ (одно на аромат:
// другие объёмы берут уже написанное) → «готово» или «нужно внимание» (чего не хватает).
//
// Статусы: queued → working → ready | attention | skipped; approved → sending → sent | failed.

const fragranticaDraftsEnabled = process.env.FRAGRANTICA_DRAFTS_ENABLED !== "false";
const fragranticaDraftsTickMs = 10_000;
const fragranticaDraftsParallel = Math.max(1, Number(process.env.FRAGRANTICA_DRAFTS_PARALLEL || 6) || 6);
const FRAG_DRAFT_ACTIVE = "('queued', 'working', 'ready', 'attention', 'approved', 'sending', 'failed')";
let fragranticaDraftsTablesReady = false;
let fragranticaDraftsRunning = false;
// continuous pool: a free slot takes the next card at once (a slow card no longer holds the whole batch)
let fragranticaDraftsInFlight = 0;
// «Улучшение карточек» has its own pool: a backlog of hundreds never holds up new Fragrantica cards
const fragranticaImprovePinned = Number(process.env.FRAGRANTICA_IMPROVE_PARALLEL) || 0;
const fragranticaImproveParallelNow = () => fragranticaImprovePinned || (isNightWorkWindow() ? 14 : 4);
let fragranticaImproveInFlight = 0;
let fragranticaPauseShown = null;
let fragranticaExpandInFlight = 0;
let fragranticaDraftsStarted = false;
const FRAG_EXPAND_PARALLEL = 6;
const FRAG_PAGE_FETCH_TIMEOUT_MS = 60_000;

async function requireFragranticaDraftTables() {
  const prisma = await requireFragranticaTables();
  if (fragranticaDraftsTablesReady) return prisma;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_drafts (
      id BIGSERIAL PRIMARY KEY,
      perfume_id INTEGER NOT NULL,
      kind TEXT NOT NULL DEFAULT 'card',
      volume_ml NUMERIC,
      tester BOOLEAN NOT NULL DEFAULT false,
      type_key TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      stage TEXT,
      targets JSONB NOT NULL DEFAULT '[]'::jsonb,
      data JSONB,
      error TEXT,
      export_ids JSONB,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_drafts_status_idx ON fragrantica_drafts (status, id)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_drafts_perfume_idx ON fragrantica_drafts (perfume_id)`);
  await prisma.$executeRawUnsafe(`ALTER TABLE fragrantica_drafts ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ`);
  fragranticaDraftsTablesReady = true;
  return prisma;
}

function fragranticaDraftTargets(keys = []) {
  const all = fragranticaTargets();
  return [...new Set((Array.isArray(keys) ? keys : []).map(cleanText))].map((key) => all.find((t) => t.key === key)).filter(Boolean);
}

async function updateFragranticaDraft(id, fields = {}) {
  const prisma = await requireFragranticaDraftTables();
  const sets = [];
  const params = [Number(id)];
  for (const [column, value] of Object.entries(fields)) {
    params.push(value !== null && typeof value === "object" && !(value instanceof Date) ? JSON.stringify(value) : value);
    sets.push(`${column} = $${params.length}${["data", "targets", "export_ids"].includes(column) ? "::jsonb" : ""}`);
  }
  const rows = await prisma.$queryRawUnsafe(`UPDATE fragrantica_drafts SET ${sets.join(", ")}, updated_at = now() WHERE id = $1 RETURNING *`, ...params);
  return rows[0] || null;
}

async function readFragranticaDraft(id) {
  const prisma = await requireFragranticaDraftTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_drafts WHERE id = $1`, Number(id));
  return rows[0] || null;
}

async function insertFragranticaDraft({ perfumeId, kind = "card", volume = null, tester = false, typeKey = null, targets = [], createdBy = null, data = null }) {
  const prisma = await requireFragranticaDraftTables();
  const rows = await prisma.$queryRawUnsafe(
    `INSERT INTO fragrantica_drafts (perfume_id, kind, volume_ml, tester, type_key, status, targets, created_by, data)
     VALUES ($1, $2, $3, $4, $5, 'queued', $6::jsonb, $7, $8::jsonb) RETURNING *`,
    Number(perfumeId), kind, volume === null ? null : Number(volume), Boolean(tester), typeKey, JSON.stringify(targets), createdBy, data ? JSON.stringify(data) : null,
  );
  return rows[0];
}

/** Rows of this perfume's live exports (for «уже есть в магазине» by volume). */
async function fragranticaPerfumePresence(perfumeId) {
  const prisma = await requireFragranticaDraftTables();
  const exports = await prisma.$queryRawUnsafe(
    `SELECT account_id AS "accountId", volume_ml AS volume, tester, status FROM fragrantica_exports WHERE perfume_id = $1`,
    Number(perfumeId),
  );
  const [row] = await prisma.$queryRawUnsafe(`SELECT shop_stock FROM fragrantica_perfumes WHERE id = $1`, Number(perfumeId));
  return { exports: exports.map((e) => ({ ...e, volume: e.volume === null ? null : Number(e.volume) })), stock: row?.shop_stock || {} };
}

function fragranticaTargetsMissingVolume(targets, presence, volume, tester = false) {
  return targets.filter((t) => !fragranticaVolumeInShop({ ...presence, shopId: t.id, volume, tester }));
}

// ─── Индекс PriceMaster в памяти ───────────────────────────────────────────
// Все активные строки последних прайсов одним запросом (~190k строк, доли секунды), дальше поиск
// объёмов и поставщиков — по словам в памяти. Кэш FRAGRANTICA_PM_INDEX_MINUTES (деф. 10), потом
// отпускается, чтобы не держать память worker без дела.

const fragranticaPmIndexTtlMs = Math.max(1, Number(process.env.FRAGRANTICA_PM_INDEX_MINUTES || 10) || 10) * 60_000;
let fragranticaPmIndexCache = null;
let fragranticaPmIndexLoading = null;

async function loadFragranticaPmRowIndex() {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const cte = await pmLatestDocsCteSql();
  const startedAt = Date.now();
  const [raw] = await pool.query(
    `${cte}
     SELECT r.NativeID AS article, r.NativeName AS name, r.BarCode AS barcode, r.NativePrice AS price, r.Active AS active,
            r.RowID AS rowId, d.DocDate AS docDate, d.PartnerID AS partnerId, p.PartnerName AS partnerName
     FROM pm_latest_docs ld
     JOIN OfferDocs d ON d.DocID = ld.DocID
     JOIN OfferRows r ON r.DocID = d.DocID
     LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
     WHERE r.Ignored = 0 AND r.Active != 0 AND r.NativePrice > 0`,
  );
  const maps = managedSupplierMaps();
  const rows = raw.map((row) => {
    const m = mapPriceMasterSearchResponseRow(row, usdRate, maps);
    return {
      id: m.id, rowId: m.rowId, article: m.article, name: m.name, supplierName: m.supplierName, partnerId: m.partnerId,
      price: m.price, priceCurrency: m.priceCurrency, currency: m.currency, available: m.available, updatedAt: m.updatedAt,
    };
  });
  const index = buildFragranticaRowIndex(rows);
  logger.info("fragrantica pm index loaded", { rows: rows.length, ms: Date.now() - startedAt });
  return { at: Date.now(), index, usdRate, settings };
}

async function getFragranticaPmRowIndex() {
  if (fragranticaPmIndexCache && Date.now() - fragranticaPmIndexCache.at < fragranticaPmIndexTtlMs) return fragranticaPmIndexCache;
  if (!fragranticaPmIndexLoading) {
    fragranticaPmIndexLoading = loadFragranticaPmRowIndex()
      .then((cache) => { fragranticaPmIndexCache = cache; return cache; })
      .finally(() => { fragranticaPmIndexLoading = null; });
  }
  return fragranticaPmIndexLoading;
}

/** PriceMaster rows of a perfume: from the memory index, SQL search only when the index finds nothing. */
async function fragranticaPerfumePmRows(perfume) {
  try {
    const cache = await getFragranticaPmRowIndex();
    const rows = findFragranticaPmCandidates(cache.index, perfume);
    if (rows.length) return { rows, usdRate: cache.usdRate, settings: cache.settings, source: "index" };
  } catch (error) {
    logger.warn("fragrantica pm index failed, using search", { detail: error?.message || String(error) });
  }
  const result = await fragranticaSearchPmRows(fragNameWithBrand(perfume), 80);
  return { ...result, source: "search" };
}

// ─── Общая работа на аромат ────────────────────────────────────────────────
// Фото и описание не зависят от объёма: первый объём (или раскладка аромата) запускает их, остальные объёмы
// ждут тот же промис — поэтому объёмы одного аромата собираются параллельно, а не друг за другом.

const fragranticaPhotoWork = new Map(); // perfumeId → { styles: Set, promise, at }
const fragranticaDescriptionWork = new Map(); // perfumeId → { promise, at }
const FRAG_SHARED_WORK_TTL_MS = 30 * 60_000;

function fragranticaSharedPhotos(perfumeId, styles) {
  const now = Date.now();
  const entry = fragranticaPhotoWork.get(perfumeId);
  if (entry && now - entry.at < FRAG_SHARED_WORK_TTL_MS && styles.every((s) => entry.styles.has(s))) return entry.promise;
  const all = [...new Set([...(entry && now - entry.at < FRAG_SHARED_WORK_TTL_MS ? entry.styles : []), ...styles])];
  const promise = runFragranticaImageJob({}, { perfumeId, styles: all, refresh: false });
  fragranticaPhotoWork.set(perfumeId, { styles: new Set(all), promise, at: now });
  promise.catch(() => fragranticaPhotoWork.delete(perfumeId));
  return promise;
}

function fragranticaSharedDescription(perfumeId, { typeKey, tester, marketplace }) {
  const now = Date.now();
  const entry = fragranticaDescriptionWork.get(perfumeId);
  if (entry && now - entry.at < FRAG_SHARED_WORK_TTL_MS) return entry.promise;
  const promise = (async () => {
    const saved = await readFragranticaCardDescription(perfumeId).catch(() => "");
    if (saved) return { description: saved, source: "shared" };
    const res = await generateFragranticaDescription({ perfumeId, typeKey, tester, marketplace });
    return { description: res.description, source: "ai" };
  })();
  fragranticaDescriptionWork.set(perfumeId, { promise, at: now });
  promise.catch(() => fragranticaDescriptionWork.delete(perfumeId));
  return promise;
}

// ─── Сборка ────────────────────────────────────────────────────────────────

// Аромат → черновики по объёмам из PriceMaster (только тех магазинов, где этого объёма ещё нет).
async function expandFragranticaPerfumeDraft(draft) {
  let perfume;
  try {
    perfume = await Promise.race([
      fragranticaPerfumeForExport(Number(draft.perfume_id)),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Фрагрантика не ответила за минуту")), FRAG_PAGE_FETCH_TIMEOUT_MS).unref?.()),
    ]);
  } catch (error) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Страница аромата не скачана: ${error?.message || error}. Откройте аромат и загрузите его закладкой «→ Склад».` });
  }
  if (!perfume.notes && !perfume.accords) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: "Фрагрантика не отдала страницу аромата (нет пирамиды). Откройте аромат и загрузите его закладкой «→ Склад», затем «Собрать заново»." });
  }
  const { rows } = await fragranticaPerfumePmRows(perfume);
  const plan = planFragranticaVolumes(rows, perfume);
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === draft.type_key) ? draft.type_key : plan.typeKey;
  const targets = fragranticaDraftTargets(draft.targets);
  const presence = await fragranticaPerfumePresence(draft.perfume_id);
  const prisma = await requireFragranticaDraftTables();
  const active = await prisma.$queryRawUnsafe(
    `SELECT volume_ml AS volume, tester FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'card' AND NOT coalesce(data ? 'existing', false) AND status IN ${FRAG_DRAFT_ACTIVE}`,
    Number(draft.perfume_id),
  );
  let created = 0;
  const skipped = [];
  // Volumes chosen in the «Какие объёмы?» window win over the PriceMaster plan (the parser can miss one)
  const chosen = Array.isArray(draft.data?.volumes) && draft.data.volumes.length
    ? draft.data.volumes.map((v) => ({ volume: Number(fragFormatVolume(v.volume)), tester: Boolean(v.tester) })).filter((v) => v.volume > 0)
    : null;
  for (const { volume, tester = false } of chosen || plan.volumes) {
    if (active.some((a) => Boolean(a.tester) === Boolean(tester) && Math.abs(Number(a.volume) - volume) < 0.01)) continue;
    const marketBlocked = fragranticaMarketBlockReasons(perfume, { volume, typeKey, tester }).length > 0;
    const missing = fragranticaTargetsMissingVolume(targets, presence, volume, tester).filter((t) => !(marketBlocked && t.kind === "yandex"));
    if (!missing.length) {
      skipped.push(volume);
      continue;
    }
    await insertFragranticaDraft({ perfumeId: draft.perfume_id, volume, tester, typeKey, targets: missing.map((t) => t.key), createdBy: draft.created_by });
    created += 1;
  }
  if (created) {
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE id = $1`, Number(draft.id));
    // photos and the description start now, while the cards wait for a slot
    const styles = [...new Set(targets.map((t) => t.style))];
    if (styles.length) fragranticaSharedPhotos(Number(draft.perfume_id), styles).catch(() => {});
    fragranticaSharedDescription(Number(draft.perfume_id), { typeKey, tester: false, marketplace: targets.some((t) => t.kind === "yandex") ? "yandex" : "ozon" }).catch(() => {});
    return null;
  }
  const reason = chosen
    ? `Выбранные объёмы (${chosen.map((v) => `${v.volume}${v.tester ? " тестер" : ""}`).join(", ")} мл) уже есть в выбранных магазинах.`
    : !plan.volumes.length
      ? "В PriceMaster нет подходящих строк с объёмом — добавьте объём вручную."
      : `Все объёмы из PriceMaster (${plan.volumes.map((v) => v.volume).join(", ")} мл) уже есть в выбранных магазинах.`;
  return updateFragranticaDraft(draft.id, { status: "skipped", stage: null, type_key: typeKey, error: reason, data: { skippedVolumes: skipped } });
}

// ─── Улучшение существующих карточек ──────────────────────────────────────────
// Карточка, сделанная не через «Фрагрантику» и сопоставленная с ароматом (fragrantica_card_matches), получает
// тот же набор, что новые карточки: фото, пирамиды, «О аромате», крупный план, видеообложку, Rich, описание ИИ,
// название по формуле Маркета, характеристики. Артикул, цена, цена до скидки, штрихкод и габариты — как сейчас
// на маркетплейсе; привязки поставщиков и остаток не трогаются (Маркет — только контент). Отправка — после
// «Одобрить», как у любого черновика конвейера.

const FRAG_IMPROVE_PER_SCAN = Math.max(0, Number(process.env.FRAGRANTICA_IMPROVE_PER_SCAN || 40) || 0);

/** Live price / old price / barcode / sizes (mm, g) of an existing card. */
async function fragranticaExistingCardState(existing = {}) {
  const offerId = cleanText(existing.offerId);
  if (existing.marketplace === "ozon") {
    const account = fragranticaResolveOzonAccount(existing.target);
    const [attrs, prices] = await Promise.all([
      ozonRequest("/v4/product/info/attributes", { filter: { offer_id: [offerId], visibility: "ALL" }, limit: 1 }, account),
      ozonRequest("/v5/product/info/prices", { filter: { offer_id: [offerId], visibility: "ALL" }, limit: 1 }, account),
    ]);
    const a = (attrs?.result || attrs?.items || [])[0];
    const p = (prices?.items || [])[0]?.price || {};
    if (!a) throw Object.assign(new Error(`Карточка ${offerId} не найдена на Ozon`), { notFound: true });
    const descAttr = (a.attributes || []).find((x) => Number(x.id) === 4191);
    // the card's own Ozon brand (dictionary value) — used when Fragrantica's brand is not in Ozon's list
    const brandValue = ((a.attributes || []).find((x) => Number(x.id) === FRAG_OZON_ATTR.brand)?.values || [])[0];
    const before = {
      name: cleanText(a.name),
      photos: [a.primary_image, ...(Array.isArray(a.images) ? a.images : [])].map((u) => cleanText(typeof u === "string" ? u : u?.url || u?.file_name)).filter(Boolean).slice(0, 15),
      description: cleanText((descAttr?.values || [])[0]?.value).slice(0, 3000),
    };
    const mm = (v) => Math.round(Number(v || 0) * (cleanText(a.dimension_unit) === "cm" ? 10 : 1));
    const g = (v) => Math.round(Number(v || 0) * (cleanText(a.weight_unit) === "kg" ? 1000 : 1));
    return {
      price: Math.round(Number(p.price || 0)), oldPrice: Math.round(Number(p.old_price || 0)), yandexPrice: 0,
      barcode: cleanText(a.barcode || (Array.isArray(a.barcodes) ? a.barcodes[0] : "")),
      dims: { depth: mm(a.depth), width: mm(a.width), height: mm(a.height), weight: g(a.weight) },
      before,
      brand: Number(brandValue?.dictionary_value_id) ? { id: Number(brandValue.dictionary_value_id), value: cleanText(brandValue.value) } : null,
    };
  }
  const shop = fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(existing.target));
  if (!shop) throw new Error("Магазин Маркета не найден");
  const mapping = (await getYandexOfferMappingsByOfferIds(shop, [offerId]))[0];
  const offer = mapping?.offer || mapping || {};
  if (!offer.offerId && !offer.name) throw Object.assign(new Error(`Карточка ${offerId} не найдена на Маркете`), { notFound: true });
  const wd = offer.weightDimensions || {};
  return {
    before: { name: cleanText(offer.name), photos: (offer.pictures || []).map(cleanText).filter(Boolean).slice(0, 15), description: cleanText(offer.description).slice(0, 3000) },
    price: 0, oldPrice: 0, yandexPrice: Math.round(Number(offer.basicPrice?.value || 0)),
    barcode: cleanText((offer.barcodes || [])[0]),
    dims: { depth: Math.round(Number(wd.length || 0) * 10), width: Math.round(Number(wd.width || 0) * 10), height: Math.round(Number(wd.height || 0) * 10), weight: Math.round(Number(wd.weight || 0) * 1000) },
  };
}

// ─── Фото старой карточки ────────────────────────────────────────────────────
// Хорошие фото старой карточки (коробка, флакон с разных сторон, фактура) остаются при улучшении. Не берём
// только «фото в конце» магазина (parfumdeclaration добавит актуальные сам): это одни и те же картинки на
// многих карточках магазина — находим их по совпадению dHash на выборке карточек этого магазина.

const FRAG_PROMO_TTL_MS = 6 * 60 * 60_000;
const fragranticaPromoCache = new Map();

async function fragranticaPhotoBuffer(url) {
  // marketplace CDNs drop a request now and then under load: three tries
  for (let attempt = 1; ; attempt += 1) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(20_000) });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return Buffer.from(await response.arrayBuffer());
    } catch (error) {
      if (attempt >= 3) throw error;
      await new Promise((resolve) => setTimeout(resolve, attempt * 2000));
    }
  }
}

async function fragranticaBufferHash(buffer) {
  const raw = await sharp(buffer).flatten({ background: "#ffffff" }).toColourspace("b-w").resize(9, 8, { fit: "fill" }).raw().toBuffer();
  let bits = "";
  for (let y = 0; y < 8; y += 1) for (let x = 0; x < 8; x += 1) bits += raw[y * 9 + x] > raw[y * 9 + x + 1] ? "1" : "0";
  return bits;
}

async function fragranticaPhotoHash(url) {
  return fragranticaBufferHash(await fragranticaPhotoBuffer(url));
}

// Чёткость: доля мелких деталей (лапласиан на 1024 px по самому товару, без белого фона), которая пропадает
// при уменьшении в 2 раза и обратном увеличении. Увеличенные нейросетью маленькие картинки («мыло»,
// поплывшие надписи) дают 0.29–0.44, нормальные фото 0.50–0.62 (калибровка 2026-10-03 на карточках Ozon).
const FRAG_SHARP_MIN = Number(process.env.FRAGRANTICA_KEEP_PHOTO_SHARPNESS || 0.45);

async function fragranticaPhotoSharpness(buffer) {
  const W = 1024;
  const full = await sharp(buffer).flatten({ background: "#ffffff" }).toColourspace("b-w").resize(W, W, { fit: "inside" }).raw().toBuffer({ resolveWithObject: true });
  const { width: w, height: h, channels } = full.info;
  if (channels !== 1) return 1;
  const px = full.data;
  const half = await sharp(px, { raw: { width: w, height: h, channels: 1 } }).resize(Math.max(1, Math.round(w / 2)), Math.max(1, Math.round(h / 2))).toColourspace("b-w").raw().toBuffer({ resolveWithObject: true });
  const blur = await sharp(half.data, { raw: { width: half.info.width, height: half.info.height, channels: half.info.channels } }).resize(w, h, { fit: "fill" }).toColourspace("b-w").raw().toBuffer();
  if (blur.length !== px.length) return 1;
  let e = 0;
  let eb = 0;
  for (let y = 1; y < h - 1; y += 1) {
    for (let x = 1; x < w - 1; x += 1) {
      const i = y * w + x;
      const lap = 4 * px[i] - px[i - 1] - px[i + 1] - px[i - w] - px[i + w];
      if (Math.abs(lap) < 3 && px[i] > 245) continue;
      e += Math.abs(lap);
      eb += Math.abs(4 * blur[i] - blur[i - 1] - blur[i + 1] - blur[i - w] - blur[i + w]);
    }
  }
  return e > 0 ? 1 - eb / e : 0;
}

function fragranticaHashDistance(a, b) {
  let d = 0;
  for (let i = 0; i < 64; i += 1) if (a[i] !== b[i]) d += 1;
  return d;
}

async function fragranticaMapLimit(list, limit, fn) {
  const out = new Array(list.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, list.length) }, async () => {
    while (next < list.length) {
      const i = next++;
      out[i] = await fn(list[i], i).catch(() => null);
    }
  }));
  return out;
}

/** Pictures of several cards of one shop: Map offerId → [urls]. */
async function fragranticaShopCardPhotos(existing, offerIds) {
  const result = new Map();
  if (existing.marketplace === "ozon") {
    const account = fragranticaResolveOzonAccount(existing.target);
    const data = await ozonRequest("/v3/product/info/list", { offer_id: offerIds }, account);
    for (const item of data.items || data.result?.items || []) {
      const primary = Array.isArray(item.primary_image) ? item.primary_image : [item.primary_image];
      result.set(cleanText(item.offer_id), [...primary, ...(item.images || [])].map((u) => cleanText(typeof u === "string" ? u : u?.url)).filter(Boolean));
    }
    return result;
  }
  const shop = fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(existing.target));
  for (const mapping of await getYandexOfferMappingsByOfferIds(shop, offerIds)) {
    const offer = mapping?.offer || mapping || {};
    result.set(cleanText(offer.offerId), (offer.pictures || []).map(cleanText).filter(Boolean));
  }
  return result;
}

/** dHashes of the shop's promo pictures («фото в конце»): the same picture on ≥ 4 of ~30 sampled cards. */
async function fragranticaShopPromoHashes(existing) {
  const key = `${existing.marketplace}:${existing.target}`;
  const cached = fragranticaPromoCache.get(key);
  if (cached && Date.now() - cached.at < FRAG_PROMO_TTL_MS) return cached.promise;
  const promise = (async () => {
    const prisma = await requireFragranticaDraftTables();
    const sample = await prisma.$queryRawUnsafe(
      `SELECT offer_id FROM fragrantica_card_matches WHERE target = $1 AND NOT archived ORDER BY random() LIMIT 30`, cleanText(existing.target),
    );
    const photos = await fragranticaShopCardPhotos(existing, sample.map((r) => r.offer_id));
    // the promo tail is at the end of a card: the last 4 pictures of every sampled card
    const tails = [...photos.entries()].filter(([, urls]) => urls.length >= 3).map(([offerId, urls]) => urls.slice(-4).map((url) => ({ offerId, url })));
    const flat = tails.flat();
    const hashes = await fragranticaMapLimit(flat, 6, (p) => fragranticaPhotoHash(p.url));
    const seen = flat.map((p, i) => ({ ...p, hash: hashes[i] })).filter((p) => p.hash);
    const promo = [];
    for (const p of seen) {
      if (promo.some((h) => fragranticaHashDistance(h, p.hash) <= 6)) continue;
      const cards = new Set(seen.filter((x) => fragranticaHashDistance(x.hash, p.hash) <= 6).map((x) => x.offerId));
      if (cards.size >= 4) promo.push(p.hash);
    }
    logger.info("fragrantica shop promo photos", { shop: key, sampled: photos.size, promo: promo.length });
    return promo;
  })();
  fragranticaPromoCache.set(key, { at: Date.now(), promise });
  promise.catch(() => fragranticaPromoCache.delete(key));
  return promise;
}

/** The old card's own product photos: promo tail, duplicates and blurry upscales removed, in their order. */
async function fragranticaKeepExistingPhotos(existing, urls = [], { anySharpness = false } = {}) {
  const out = { keep: [], blurry: 0 };
  if (!urls.length) return out;
  const promo = await fragranticaShopPromoHashes(existing);
  const checked = await fragranticaMapLimit(urls, 4, async (url) => {
    const buffer = await fragranticaPhotoBuffer(url);
    return { hash: await fragranticaBufferHash(buffer), sharpness: await fragranticaPhotoSharpness(buffer) };
  });
  const kept = [];
  for (const [i, url] of urls.entries()) {
    const c = checked[i];
    // could not be downloaded to check: kept (a good photo is never lost silently), the operator sees it
    if (!c?.hash) {
      out.keep.push(url);
      out.unchecked = (out.unchecked || 0) + 1;
      continue;
    }
    if (promo.some((h) => fragranticaHashDistance(h, c.hash) <= 6)) continue;
    if (kept.some((h) => fragranticaHashDistance(h, c.hash) <= 3)) continue;
    if (!anySharpness && c.sharpness < FRAG_SHARP_MIN) {
      out.blurry += 1;
      continue;
    }
    out.keep.push(url);
    kept.push(c.hash);
  }
  out.keep = out.keep.slice(0, 14);
  return out;
}

/** The shops of one improvement: data.existing.targets (new) or the single target of older drafts. */
function fragranticaExistingTargets(existing = {}) {
  const list = Array.isArray(existing.targets) && existing.targets.length
    ? existing.targets
    : [{ marketplace: existing.marketplace, target: existing.target, offerId: existing.offerId }];
  return list.filter((t) => t.marketplace && t.target).map((t) => ({ marketplace: t.marketplace, target: cleanText(t.target), offerId: cleanText(t.offerId || existing.offerId) }));
}

/**
 * Puts products into «Улучшение карточек»: one draft per product = one offer id in every shop it is matched in
 * (Ozon and Market together). Best sellers first (units in 90 days over all shops), then the weakest Market rating.
 */
async function enqueueFragranticaImprovements({ limit = FRAG_IMPROVE_PER_SCAN, createdBy = "улучшение", activeOnly = false } = {}) {
  const wanted = Math.min(1000, Math.max(0, Number(limit) || 0));
  if (!wanted) return { queued: 0 };
  const prisma = await requireFragranticaDraftTables();
  await requireCardHealthTables();
  const exists = await prisma.$queryRawUnsafe(`SELECT to_regclass('fragrantica_card_matches') IS NOT NULL AS ok`);
  if (!exists[0]?.ok) return { queued: 0, reason: "сопоставление ещё не готово" };
  const rows = await prisma.$queryRawUnsafe(
    `WITH m AS (
       SELECT m.target, m.offer_id, m.perfume_id, m.volume_ml, m.tester, w.marketplace::text AS marketplace,
              (w.status = 'active' AND coalesce(w.target_stock, 0) > 0) AS live
         FROM fragrantica_card_matches m
         JOIN warehouse_products w ON w.target = m.target AND w.offer_id = m.offer_id AND w.archived = false
        WHERE m.volume_ml IS NOT NULL AND m.offer_id !~* '^FR[0-9]' AND w.name !~* $2
     ), g AS (
       SELECT lower(offer_id) AS k, min(offer_id) AS offer_id,
              mode() WITHIN GROUP (ORDER BY perfume_id) AS perfume_id, max(volume_ml) AS volume_ml, bool_or(tester) AS tester,
              jsonb_agg(jsonb_build_object('marketplace', marketplace, 'target', target, 'offerId', offer_id) ORDER BY marketplace, target) AS targets,
              bool_or(live) AS live
         FROM m GROUP BY 1
     )
     SELECT g.*, coalesce(s.sold, 0)::int AS sold, q.rating
       FROM g
       LEFT JOIN (SELECT lower(offer_id) AS k, sum(quantity)::int AS sold FROM finance_orders
                   WHERE offer_id IS NOT NULL AND coalesce(sold_at, created_at) > now() - interval '90 days' GROUP BY 1) s ON s.k = g.k
       LEFT JOIN (SELECT lower(offer_id) AS k, min(content_rating) AS rating FROM card_quality GROUP BY 1) q ON q.k = g.k
      WHERE NOT EXISTS (SELECT 1 FROM fragrantica_drafts d WHERE lower(d.data->'existing'->>'offerId') = g.k)
        AND NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE lower(e.offer_id) = g.k AND coalesce(e.item->>'improve', '') <> 'true')
        AND (g.live OR NOT $3::boolean)
      ORDER BY g.live DESC, sold DESC, rating ASC NULLS LAST, g.k
      LIMIT $1`,
    wanted, FRAG_SET_NAME_PATTERN, Boolean(activeOnly),
  );
  const all = fragranticaTargets();
  let queued = 0;
  let withSales = 0;
  for (const r of rows) {
    const shops = (Array.isArray(r.targets) ? r.targets : [])
      .map((t) => ({ ...t, key: all.find((x) => x.kind === t.marketplace && x.id === cleanText(t.target))?.key }))
      .filter((t) => t.key);
    if (!shops.length) continue;
    // Ozon first: its card has the price, sizes and the photos we keep
    shops.sort((a, b) => (a.marketplace === b.marketplace ? 0 : a.marketplace === "ozon" ? -1 : 1));
    await insertFragranticaDraft({
      perfumeId: Number(r.perfume_id), kind: "card", volume: Number(r.volume_ml), tester: Boolean(r.tester),
      targets: [...new Set(shops.map((t) => t.key))], createdBy,
      data: {
        existing: {
          offerId: cleanText(r.offer_id),
          marketplace: shops[0].marketplace,
          target: cleanText(shops[0].target),
          targets: shops.map(({ marketplace, target, offerId }) => ({ marketplace, target: cleanText(target), offerId: cleanText(offerId) })),
          rating: r.rating === null ? null : Number(r.rating),
          sold: Number(r.sold || 0),
        },
      },
    });
    queued += 1;
    if (Number(r.sold) > 0) withSales += 1;
  }
  if (queued) logger.info("fragrantica improvements queued", { queued, withSales });
  return { queued, withSales };
}

// The type of the product behind an existing card (card improvement): the supplier rows linked to the article say
// what is really sold, else the card's own titles. Fragrantica's guess («eau de parfum» by default) turned EDT cards
// into EDP ones (Kenzo Flower, Light Blue, Born in Roma…) — it is the last resort only.
const DRAFT_TYPE_BY_CONCENTRATION = { edp: "edp", edt: "edt", parfum: "parfum", edc: "cologne" };
async function existingCardTypeKey(offerId) {
  return (await existingCardFacts(offerId)).typeKey;
}

/** Type and gender (male / female / unisex) of the product behind an existing card: supplier rows, else its titles. */
async function existingCardFacts(offerId) {
  const prisma = getPrisma();
  if (!prisma || !cleanText(offerId)) return { typeKey: null, gender: null };
  const parser = require("./lib/perfume-match");
  const voteBy = (pick) => (names) => {
    const counts = new Map();
    for (const name of names) {
      const key = pick(parser.parsePerfumeName(name));
      if (key) counts.set(key, (counts.get(key) || 0) + 1);
    }
    return [...counts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || null;
  };
  const vote = voteBy((p) => (p.type === "oil" ? "oil" : DRAFT_TYPE_BY_CONCENTRATION[p.concentration]));
  const voteGender = voteBy((p) => ({ men: "male", women: "female", unisex: "unisex" }[p.gender]));
  const links = await prisma.$queryRawUnsafe(
    `SELECT l.exact_name AS name FROM product_links l JOIN warehouse_products w ON w.id = l.product_id
      WHERE lower(w.offer_id) = lower($1) AND coalesce(l.exact_name, '') <> ''`, cleanText(offerId),
  ).catch(() => []);
  const cards = await prisma.$queryRawUnsafe(`SELECT name FROM warehouse_products WHERE lower(offer_id) = lower($1)`, cleanText(offerId)).catch(() => []);
  const linkNames = links.map((r) => r.name);
  const cardNames = cards.map((r) => r.name);
  return { typeKey: vote(linkNames) || vote(cardNames), gender: voteGender(cardNames) || voteGender(linkNames) };
}

/**
 * Card improvement must not turn a product into another perfume (Paco Rabanne XS → «Paco», Boucheron pour Homme →
 * Boucheron women, EDT → EDP). The product's truth: supplier rows linked to the article and the Market card titles
 * (never rewritten by an improvement). The chosen Fragrantica perfume must match one of them by brand, name words and
 * gender (men ≠ women). → "" when it matches or nothing is known, else the reason.
 */
async function existingCardPerfumeMismatch(offerId, perfume) {
  const prisma = getPrisma();
  if (!prisma || !cleanText(offerId) || !perfume) return "";
  const parser = require("./lib/perfume-match");
  const brands = await supplierMatchBrands(prisma).catch(() => null);
  const links = await prisma.$queryRawUnsafe(
    `SELECT l.exact_name AS name FROM product_links l JOIN warehouse_products w ON w.id = l.product_id
      WHERE lower(w.offer_id) = lower($1) AND coalesce(l.exact_name, '') <> ''`, cleanText(offerId),
  ).catch(() => []);
  const cards = await prisma.$queryRawUnsafe(
    `SELECT name FROM warehouse_products WHERE lower(offer_id) = lower($1) AND marketplace = 'yandex'`, cleanText(offerId),
  ).catch(() => []);
  const truths = [...links.map((r) => r.name), ...cards.map((r) => r.name)].filter(Boolean)
    .map((name) => parser.parsePerfumeName(name, { brands })).filter((t) => t.brandKey && t.volume);
  if (!truths.length) return "";
  const genderWord = { male: "men", female: "women", unisex: "unisex" }[perfume.gender] || "";
  const reasons = new Set();
  for (const t of truths) {
    // the perfume written like the product: same volume / concentration / flags, so only brand, name and gender count
    const title = [perfume.brand, perfume.name, genderWord, t.concentration, `${t.volume} ml`, t.tester ? "tester" : ""].filter(Boolean).join(" ");
    const candidate = parser.parsePerfumeName(title, { brands });
    const res = parser.comparePerfumes(t, { ...candidate, set: t.set, decant: t.decant, sample: t.sample, nonPerfume: t.nonPerfume, type: t.type, defect: false });
    if (res.ok) return "";
    reasons.add(res.reason);
  }
  const label = { brand: "другой бренд", name: "другое название аромата", gender: "другой пол (мужской / женский)" };
  return `Аромат Фрагрантики «${perfume.brand} ${perfume.name}» не совпадает с товаром (${[...reasons].map((r) => label[r] || r).join(", ")}) — выберите аромат вручную`;
}

async function buildFragranticaCardDraft(draft) {
  const perfumeId = Number(draft.perfume_id);
  const targets = fragranticaDraftTargets(draft.targets);
  const volume = fragFormatVolume(draft.volume_ml);
  const tester = Boolean(draft.tester);
  const stage = (name) => updateFragranticaDraft(draft.id, { stage: name });
  const warnings = [];

  const buildStartedAt = Date.now();
  const existing = draft.data?.existing || null;
  await stage("build");
  const done = new Set();
  // when each part finished (ms from the start) — shows where a slow build spends its time
  const parts = {};
  const mark = (part) => (value) => {
    done.add(part);
    parts[part] = Date.now() - buildStartedAt;
    updateFragranticaDraft(draft.id, { stage: `build:${[...done].join(",")}` }).catch(() => {});
    return value;
  };
  const ozonTarget = targets.find((t) => t.kind === "ozon");
  let perfume = await fragranticaPerfumeForExport(perfumeId);
  // Fragrantica without a gender: the card says who it is for («унисекс», «для женщин» stay in the new title)
  if (!perfume.gender && draft.data?.existing?.offerId) {
    const facts = await existingCardFacts(draft.data.existing.offerId).catch(() => ({}));
    if (facts.gender) perfume = { ...perfume, gender: facts.gender };
  }
  parts.perfume = Date.now() - buildStartedAt;
  // the type is known before the form: the draft's own; an existing card keeps its product's type; else the guess
  const existingType = !FRAG_OZON_TYPES.some((t) => t.key === draft.type_key) && draft.data?.existing?.offerId
    ? await existingCardTypeKey(draft.data.existing.offerId).catch(() => null) : null;
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === draft.type_key) ? draft.type_key : existingType || fragOzonGuessTypeKey(perfume);
  // an improvement of an existing card only for the same perfume; a manual choice (perfumeChosen) is trusted
  if (draft.data?.existing?.offerId && !draft.data?.perfumeChosen) {
    const mismatch = await existingCardPerfumeMismatch(draft.data.existing.offerId, perfume).catch(() => "");
    if (mismatch) return updateFragranticaDraft(draft.id, { status: "attention", stage: null, type_key: typeKey, error: mismatch });
  }
  const styles = [...new Set(targets.map((t) => t.style))];
  const marketplace = targets.some((t) => t.kind === "yandex") ? "yandex" : "ozon";

  // improvement: the old card in every shop + which of its photos are worth keeping — parallel with the render
  const ownChosen = Array.isArray(draft.data?.customPhotos) && !draft.data.customPhotosAuto;
  // a tester or a miniature (< 20 ml) looks different from Fragrantica's bottle: its card keeps only its own photos
  const ownBottleOnly = Boolean(existing) && (tester || Number(volume) < 20);
  const existingPart = existing
    ? (async () => {
      const read = await Promise.all(fragranticaExistingTargets(existing).map(async (t) => {
        try {
          return { ...t, state: await fragranticaExistingCardState({ ...t }) };
        } catch (error) {
          if (error?.notFound) return { ...t, missing: true, error };
          throw error;
        }
      }));
      // the card's own data comes from the shops that have it (Ozon first)
      const shopStates = read.filter((x) => !x.missing).sort((a, b) => (a.marketplace === b.marketplace ? 0 : a.marketplace === "ozon" ? -1 : 1));
      if (!shopStates.length) throw new Error(`Карточка ${existing.offerId} не найдена ни в одном магазине`);
      const restore = Array.isArray(existing.restoreBefore?.photos) && existing.restoreBefore.photos.length ? existing.restoreBefore : null;
      const beforePhotos = (restore || shopStates[0].state.before)?.photos || [];
      const kept = ownChosen ? null : await fragranticaKeepExistingPhotos(shopStates[0], beforePhotos, { anySharpness: ownBottleOnly }).catch((error) => ({ keep: [], blurry: 0, error }));
      return { shopStates, kept, missing: read.filter((x) => x.missing) };
    })().then(mark("existing")).catch((error) => ({ error }))
    : Promise.resolve(null);

  // Ozon form, supplier links + price, photos and the description don't depend on each other — run together
  const [form, linkPart, photoPart, descriptionPart, existingResult] = await Promise.all([
    buildFragranticaFormData({ perfumeId, typeKey, volume, tester, accountId: ozonTarget?.id }).then(mark("form")),
    (async () => {
      // improvement: the card keeps its own suppliers and price — no new links
      if (existing) return { linkRows: [], selected: [], preview: null, warning: "" };
      try {
        const pm = await fragranticaPerfumePmRows(perfume);
        const suggestions = await fragranticaLinkSuggestionsData({ perfumeId, typeKey, volume, tester, rows: pm.rows, usdRate: pm.usdRate, settings: pm.settings });
        const linkRows = suggestions.rows.slice(0, 12);
        const selected = suggestions.suggested.filter((id) => linkRows.some((r) => r.id === id));
        const rows = linkRows.filter((r) => selected.includes(r.id));
        const preview = rows.length ? await fragranticaPricePreview(rows) : null;
        return { linkRows, selected, preview, warning: rows.length ? "" : "Подходящих строк поставщика нет — цену поставьте вручную или отметьте строку." };
      } catch (error) {
        return { linkRows: [], selected: [], preview: null, warning: `Привязка: ${error?.message || error}` };
      }
    })().then(mark("links")),
    (styles.length ? fragranticaSharedPhotos(perfumeId, styles).catch((error) => ({ warnings: [`Фото: ${error?.message || error}`] })) : Promise.resolve(null)).then(mark("photos")),
    fragranticaSharedDescription(perfumeId, { typeKey, tester, marketplace }).catch((error) => ({ description: "", source: "", error })).then(mark("description")),
    existingPart,
  ]);
  const { linkRows, selected, preview } = linkPart;
  if (linkPart.warning) warnings.push(linkPart.warning);
  let images = { main: form.sourceImage, notes: {} };
  if (photoPart) {
    images = { main: photoPart.main || form.sourceImage, notes: photoPart.notes || {}, specs: photoPart.specs || {}, closeup: photoPart.closeup || null };
    warnings.push(...(photoPart.warnings || []));
  }
  let description = descriptionPart.description || "";
  let descriptionSource = descriptionPart.source || "";
  if (!description) {
    description = cleanText((form.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value);
    descriptionSource = description ? "fragrantica" : "";
    if (descriptionPart.error) warnings.push(`Описание ИИ не получилось: ${descriptionPart.error?.message || descriptionPart.error}`);
  }

  const data = {
    name: form.name,
    offerId: form.offerId,
    typeKey,
    volume,
    tester,
    dims: form.dims,
    attributes: form.attributes.filter((a) => a.values.length).map((a) => ({ id: a.id, values: a.values })),
    requiredAttrs: form.attributes.filter((a) => a.required).map((a) => ({ id: a.id, name: a.name })),
    brandMatched: form.brandMatched,
    brandCandidates: form.brandCandidates,
    country: form.country,
    vatByTarget: form.vatByTarget,
    sourceImage: form.sourceImage,
    price: preview?.price || 0,
    oldPrice: preview?.oldPrice || 0,
    yandexPrice: preview?.yandexPrice || preview?.price || 0,
    supplierName: preview?.supplierName || "",
    markup: preview?.markup || 0,
    linkRows,
    selectedLinks: selected,
    images,
    description,
    descriptionSource,
    barcode: "",
    warnings,
    buildMs: Date.now() - buildStartedAt,
    buildParts: parts,
  };
  // no pyramid for a shop (the photo download failed) → the draft is not «ready»: rebuilt up to 3 times, then «attention»
  const noPyramid = styles.filter((style) => !images.notes?.[style]);
  if (noPyramid.length) {
    const retries = Number(draft.data?.photoRetries || 0);
    const why = warnings.find((w) => /^Фото/.test(w)) || "пирамида не собралась";
    if (retries < 3) {
      logger.warn("fragrantica draft photos missing, retry", { id: Number(draft.id), retries, why });
      return updateFragranticaDraft(draft.id, { status: "queued", stage: null, error: null, data: { ...(draft.data || {}), photoRetries: retries + 1 } });
    }
    warnings.push(`Пирамида аромата не собралась после 3 попыток (${why}) — нажмите «Пересобрать».`);
  }
  // own photos survive a rebuild (the operator chose them)
  if (Array.isArray(draft.data?.customPhotos) && !draft.data.customPhotosAuto) {
    data.customPhotos = draft.data.customPhotos;
    data.onlyCustomPhotos = Boolean(draft.data.onlyCustomPhotos);
  }
  if (existing) {
    try {
      // every shop of the product: Ozon gives the price, old price, barcode and sizes; Market its own price
      if (existingResult?.error) throw existingResult.error;
      const { shopStates } = existingResult;
      // not on Market → the improvement uploads it there (content; its price and stock come with the usual sync);
      // not on Ozon → that shop is left out (a new Ozon card needs a price the improvement does not set)
      const allKeys = fragranticaTargets();
      const keyOf = (x) => allKeys.find((t) => t.kind === x.marketplace && t.id === x.target)?.key;
      // Market refuses some goods outright (hidden brand, tester, < 20 ml): no upload there, the reason is shown
      const marketRefuses = fragranticaMarketBlockReasons(perfume, { volume, typeKey, tester });
      const missingMarket = existingResult.missing.filter((x) => x.marketplace === "yandex").map(keyOf).filter(Boolean);
      const createOn = marketRefuses.length ? [] : missingMarket;
      const dropOzon = [
        ...existingResult.missing.filter((x) => x.marketplace === "ozon").map(keyOf).filter(Boolean),
        // Market refuses these goods (hidden brand, tester, < 20 ml): its shops are left out entirely
        ...(marketRefuses.length ? fragranticaDraftTargets(draft.targets).filter((t) => t.kind === "yandex").map((t) => t.key) : []),
      ];
      if (marketRefuses.length && missingMarket.length) warnings.push(`На Маркете карточки ${existing.offerId} нет — и не загружаем: ${marketRefuses.join("; ")}.`);
      if (dropOzon.length) {
        draft.targets = (draft.targets || []).filter((k) => !dropOzon.includes(k));
        if (existingResult.missing.some((x) => x.marketplace === "ozon")) warnings.push(`На Ozon карточки ${existing.offerId} нет — улучшаем только Маркет.`);
        if (marketRefuses.length && !missingMarket.length && dropOzon.some((k) => k.startsWith("yandex:"))) warnings.push(`Маркет не улучшаем: ${marketRefuses.join("; ")}.`);
      }
      if (!(draft.targets || []).length) {
        return updateFragranticaDraft(draft.id, {
          status: "skipped", stage: null, type_key: typeKey, targets: [],
          error: `Улучшать негде: ${marketRefuses.length ? marketRefuses.join("; ") : "карточки нет ни в одном магазине"}`,
          data: { ...data, existing },
        });
      }
      if (createOn.length) warnings.push(`На Маркете карточки ${existing.offerId} нет — загрузим её (цену и остаток отправит обычная синхронизация).`);
      const ozonState = shopStates.find((x) => x.marketplace === "ozon")?.state;
      const marketState = shopStates.find((x) => x.marketplace === "yandex")?.state;
      const primary = shopStates[0].state;
      const state = {
        price: ozonState?.price || 0,
        oldPrice: ozonState?.oldPrice || 0,
        yandexPrice: marketState?.yandexPrice || 0,
        barcode: ozonState?.barcode || marketState?.barcode || "",
        dims: [ozonState, marketState].find((x) => x?.dims?.depth && x.dims.width && x.dims.height && x.dims.weight)?.dims || primary.dims,
        before: primary.before,
      };
      // a card already sent with the old logic: its photos from before that send are restored
      if (Array.isArray(existing.restoreBefore?.photos) && existing.restoreBefore.photos.length) state.before = existing.restoreBefore;
      // each Ozon cabinet keeps its own current price
      const allTargets = fragranticaTargets();
      const prices = {};
      for (const x of shopStates) {
        const key = allTargets.find((t) => t.kind === x.marketplace && t.id === x.target)?.key;
        if (key && x.marketplace === "ozon") prices[key] = { price: x.state.price, oldPrice: x.state.oldPrice };
      }
      Object.assign(data, {
        offerId: existing.offerId,
        price: state.price, oldPrice: state.oldPrice, yandexPrice: state.yandexPrice,
        barcode: state.barcode,
        supplierName: "", markup: 0,
        existing: { ...existing, state, prices, createOn },
        // the Market title the export will use (Ozon keeps data.name)
        marketName: buildFragranticaMarketName({ perfume, typeKey, volume: draft.volume_ml, tester: Boolean(draft.tester) }),
      });
      if (state.dims.depth && state.dims.width && state.dims.height && state.dims.weight) data.dims = state.dims;
      data.ownBottleOnly = ownBottleOnly;
      // brand not found in Ozon's list by name → the brand the live Ozon card already has
      if (!data.brandMatched && ozonState?.brand) {
        data.attributes = [...(data.attributes || []).filter((x) => Number(x.id) !== FRAG_OZON_ATTR.brand), { id: FRAG_OZON_ATTR.brand, values: [{ dictionary_value_id: ozonState.brand.id, value: ozonState.brand.value }] }];
        data.brandMatched = true;
      }
      // the new bottle photo is the main one; then the old card's SHARP product photos; then the pyramid and cards
      if (!Array.isArray(data.customPhotos)) {
        const kept = existingResult.kept || { keep: [], blurry: 0 };
        if (kept.error) warnings.push(`Фото старой карточки не разобрали: ${kept.error?.message || kept.error}`);
        const main = images.main || form.sourceImage;
        data.customPhotos = ownBottleOnly ? kept.keep : kept.keep.length && main ? [fragranticaAbsoluteUrl(main), ...kept.keep] : [];
        data.customPhotosAuto = true;
        data.keptExistingPhotos = kept.keep.length;
        data.blurryExistingPhotos = kept.blurry;
      }
    } catch (error) {
      data.existing = existing;
      return updateFragranticaDraft(draft.id, { status: "attention", stage: null, type_key: typeKey, error: `Улучшение: ${error?.message || error}`, data });
    }
  }
  data.buildMs = Date.now() - buildStartedAt;
  const built = { ...draft, perfumeId, data };
  const targetsNow = fragranticaDraftTargets(draft.targets);
  const missing = fragranticaDraftMissing(built, targetsNow);
  // the Ozon brand dictionary matters only when the card goes to Ozon (Market takes the brand as text)
  if (!data.brandMatched && targetsNow.some((t) => t.kind === "ozon") && !missing.includes("Бренд")) missing.unshift("Бренд");
  if (!targetsNow.some((t) => t.kind === "ozon")) {
    const i = missing.indexOf("Бренд");
    if (i >= 0) missing.splice(i, 1);
  }
  if (data.ownBottleOnly && !(data.customPhotos || []).length) missing.push("Фото товара (тестер / до 20 мл — загрузите в «Свои фото»)");
  if (noPyramid.length) missing.push("Пирамида аромата");
  return updateFragranticaDraft(draft.id, {
    status: missing.length ? "attention" : "ready",
    stage: null,
    type_key: typeKey,
    targets: targetsNow.map((t) => t.key),
    error: missing.length ? `Не хватает: ${missing.join(", ")}` : null,
    data: { ...data, missing },
  });
}

async function sendFragranticaDraft(draft) {
  const targets = fragranticaDraftTargets(draft.targets);
  // the video cover is rendered in the background; the send waits for it (max 4 min), else goes without it
  await fragranticaVideoCoversReady(Number(draft.perfume_id), [...new Set(targets.map((t) => t.style))], 240_000).catch(() => {});
  const built = { ...draft, perfumeId: Number(draft.perfume_id) };
  const missing = fragranticaDraftMissing(built, targets);
  if (missing.length) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Не хватает: ${missing.join(", ")}`, data: { ...(draft.data || {}), missing } });
  }
  try {
    const body = buildFragranticaDraftExportBody(built, targets);
    const result = await createFragranticaExports(body, { session: { username: cleanText(draft.created_by) || "fragrantica" } });
    return updateFragranticaDraft(draft.id, { status: "sent", stage: null, error: null, export_ids: result.exports.map((e) => e.id) });
  } catch (error) {
    const detail = error?.detail?.missing;
    if (Array.isArray(detail) && detail.length) {
      return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Не хватает: ${detail.join(", ")}`, data: { ...(draft.data || {}), missing: detail } });
    }
    return updateFragranticaDraft(draft.id, { status: "failed", stage: null, error: String(error?.message || error).slice(0, 2000) });
  }
}

async function processFragranticaDraft(draft) {
  try {
    if (draft.status === "sending") return await sendFragranticaDraft(draft);
    if (draft.kind === "perfume") return await expandFragranticaPerfumeDraft(draft);
    return await buildFragranticaCardDraft(draft);
  } catch (error) {
    // Фрагрантика занята (429 / проверка Cloudflare): черновик ждёт и пересобирается сам — 1, 2, 4 … 30 мин, до 8 раз
    // Fragrantica busy, or a marketplace rate limit (Market «Hit rate limit», Ozon 429): wait and retry
    const busy = /ответила (429|403)|too many|challenge|rate limit|METHOD_FAILURE|429/i.test(String(error?.message || ""));
    const tries = Number(draft.data?.fetchRetries || 0);
    if (busy && draft.status !== "sending") {
      // 1, 2, 4 … 30 min; after 8 tries once an hour — a long Cloudflare block never turns into «attention»
      const pauseMin = error?.pausedUntil ? Math.ceil((error.pausedUntil - Date.now()) / 60_000) + 1 : 0;
      const waitMin = Math.max(pauseMin, tries < 8 ? Math.min(30, 2 ** tries) : 60);
      logger.info("fragrantica draft waits for Fragrantica", { id: Number(draft.id), tries: tries + 1, waitMin });
      return updateFragranticaDraft(draft.id, {
        status: "queued",
        stage: null,
        error: null,
        data: { ...(draft.data || {}), fetchRetries: tries + 1, retryAt: new Date(Date.now() + waitMin * 60_000).toISOString() },
      });
    }
    logger.warn("fragrantica draft failed", { id: Number(draft.id), detail: error?.message || String(error) });
    return updateFragranticaDraft(draft.id, {
      status: draft.status === "sending" ? "failed" : "attention",
      stage: null,
      error: String(error?.message || error).slice(0, 2000),
    });
  }
}

// Worker: одобренные — отправить; в очереди — собрать (по ароматам параллельно, объёмы одного аромата — по очереди,
// чтобы фото и описание делались один раз).
async function runFragranticaDraftsTick() {
  if (fragranticaDraftsRunning) return { status: "already_running" };
  fragranticaDraftsRunning = true;
  try {
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(
      `UPDATE fragrantica_drafts SET status = CASE WHEN status = 'sending' THEN 'approved' ELSE 'queued' END, stage = NULL, updated_at = now()
       WHERE status IN ('working', 'sending') AND (updated_at < now() - interval '10 minutes' OR $1::boolean)`,
      // first tick after a (re)start: nothing can be in flight in this process yet
      !fragranticaDraftsStarted,
    );
    fragranticaDraftsStarted = true;
    for (let i = 0; i < 20; i += 1) {
      const [next] = await prisma.$queryRawUnsafe(
        `UPDATE fragrantica_drafts SET status = 'sending', updated_at = now()
         WHERE id = (SELECT id FROM fragrantica_drafts WHERE status = 'approved' ORDER BY updated_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
         RETURNING *`,
      );
      if (!next) break;
      await processFragranticaDraft(next);
    }
    // Ароматы → объёмы: по индексу это миллисекунды, но аромат без скачанной страницы ждёт Фрагрантику —
    // раскладка идёт в своём пуле и не держит сборку карточек
    const perfumes = await prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'working', stage = 'volumes', started_at = now(), updated_at = now()
       WHERE id IN (SELECT id FROM fragrantica_drafts WHERE status = 'queued' AND kind = 'perfume' ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
       RETURNING *`,
      Math.max(0, FRAG_EXPAND_PARALLEL - fragranticaExpandInFlight),
    );
    for (const draft of perfumes) {
      fragranticaExpandInFlight += 1;
      processFragranticaDraft(draft)
        .catch(() => {})
        .finally(() => { fragranticaExpandInFlight = Math.max(0, fragranticaExpandInFlight - 1); });
    }
    // Fragrantica pause is shown on «Улучшение карточек» (the API process reads it from card_health_state)
    const pausedUntil = fragranticaPagesPausedUntil();
    const pagesPaused = pausedUntil > 0;
    if (pagesPaused !== fragranticaPauseShown) {
      fragranticaPauseShown = pagesPaused;
      writeCardHealthState("fragrantica", { pausedUntil: pagesPaused ? new Date(pausedUntil).toISOString() : null }).catch(() => {});
    }
    const claimPool = (improve, limit) => prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'working', stage = 'start', started_at = now(), updated_at = now()
       WHERE id IN (
         SELECT d.id FROM fragrantica_drafts d
           LEFT JOIN fragrantica_perfumes p ON p.id = d.perfume_id
          WHERE d.status = 'queued' AND d.kind = 'card' AND coalesce(d.data ? 'existing', false) = $2
            AND coalesce((d.data->>'retryAt')::timestamptz, 'epoch'::timestamptz) <= now()
            AND (p.detail_at IS NOT NULL OR NOT $3::boolean)
          -- perfumes whose Fragrantica page is already here build first (no request to Fragrantica)
          ORDER BY (p.detail_at IS NULL), d.id
          LIMIT $1
          FOR UPDATE OF d SKIP LOCKED)
       RETURNING *`,
      Math.max(0, limit), improve, pagesPaused,
    );
    const claimed = await claimPool(false, fragranticaDraftsParallel - fragranticaDraftsInFlight);
    for (const draft of claimed) {
      fragranticaDraftsInFlight += 1;
      processFragranticaDraft(draft)
        .catch(() => {})
        .finally(() => { fragranticaDraftsInFlight = Math.max(0, fragranticaDraftsInFlight - 1); });
    }
    const improving = await claimPool(true, fragranticaImproveParallelNow() - fragranticaImproveInFlight);
    for (const draft of improving) {
      fragranticaImproveInFlight += 1;
      processFragranticaDraft(draft)
        .catch(() => {})
        .finally(() => { fragranticaImproveInFlight = Math.max(0, fragranticaImproveInFlight - 1); });
    }
    return { status: "ok", built: claimed.length + improving.length + perfumes.length + fragranticaDraftsInFlight + fragranticaImproveInFlight + fragranticaExpandInFlight };
  } finally {
    fragranticaDraftsRunning = false;
  }
}

function scheduleFragranticaDrafts(delayMs = fragranticaDraftsTickMs) {
  if (!fragranticaDraftsEnabled) return;
  setTimeout(async () => {
    let busy = false;
    try {
      const result = await runFragranticaDraftsTick();
      busy = Boolean(result?.built);
    } catch (error) {
      logger.warn("fragrantica drafts tick failed", { detail: error?.message });
    } finally {
      scheduleFragranticaDrafts(busy ? 1_000 : fragranticaDraftsTickMs);
    }
  }, Math.max(1_000, Number(delayMs) || fragranticaDraftsTickMs)).unref?.();
}

// ─── Роуты ─────────────────────────────────────────────────────────────────

function fragranticaDraftResponse(row, exportsById = new Map()) {
  const data = row.data || {};
  return {
    id: Number(row.id),
    perfumeId: Number(row.perfume_id),
    brand: row.brand || "",
    perfumeName: row.perfume_name || "",
    thumb: fragranticaMediaUrl("thumbs", `${Number(row.perfume_id)}.jpg`),
    kind: row.kind,
    volume: row.volume_ml === null ? null : Number(row.volume_ml),
    tester: Boolean(row.tester),
    typeKey: row.type_key || data.typeKey || "",
    status: row.status,
    stage: row.stage || null,
    startedAt: row.started_at || null,
    targets: Array.isArray(row.targets) ? row.targets : [],
    error: row.error || null,
    data,
    exports: (Array.isArray(row.export_ids) ? row.export_ids : []).map((id) => exportsById.get(Number(id))).filter(Boolean),
    updatedAt: row.updated_at,
  };
}

app.get("/api/fragrantica/drafts", requireAdmin, async (_request, response, next) => {
  try {
    const prisma = await requireFragranticaDraftTables();
    const rows = await prisma.$queryRawUnsafe(
      `SELECT d.*, p.brand, p.name AS perfume_name FROM fragrantica_drafts d
         LEFT JOIN fragrantica_perfumes p ON p.id = d.perfume_id
        WHERE (d.status <> 'sent' OR d.updated_at > now() - interval '3 days')
          -- improvement drafts live on their own page «Улучшение карточек»
          AND NOT coalesce(d.data ? 'existing', false)
        ORDER BY d.id LIMIT 600`,
    );
    const ids = [...new Set(rows.flatMap((r) => (Array.isArray(r.export_ids) ? r.export_ids : []).map(Number)))];
    let exportRows = ids.length ? await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = ANY($1::bigint[])`, ids) : [];
    // Ozon checks a new card for a minute or two: ask it about a few waiting ones on each poll
    const waiting = exportRows.filter((e) => e.status === "pending" && Date.now() - new Date(e.updated_at).getTime() > 20_000).slice(0, 4);
    if (waiting.length) {
      const fresh = await Promise.all(waiting.map((e) => refreshFragranticaExport(e).catch(() => e)));
      const byId = new Map(fresh.map((e) => [Number(e.id), e]));
      exportRows = exportRows.map((e) => byId.get(Number(e.id)) || e);
    }
    const exportsById = new Map(exportRows.map((e) => [Number(e.id), fragranticaExportFromRow(e)]));
    const counts = {};
    for (const row of rows) counts[row.status] = (counts[row.status] || 0) + 1;
    // time bar: the median build time of the last finished cards (first volumes draw photos, the rest are quick)
    const recent = await prisma.$queryRawUnsafe(
      `SELECT (data->>'buildMs')::int AS ms FROM fragrantica_drafts WHERE kind = 'card' AND data ? 'buildMs' ORDER BY updated_at DESC LIMIT 40`,
    ).catch(() => []);
    const times = recent.map((r) => Number(r.ms)).filter((v) => v > 0).sort((a, b) => a - b);
    const medianBuildMs = times.length ? times[Math.floor(times.length / 2)] : 30_000;
    response.json({
      ok: true,
      items: rows.map((r) => fragranticaDraftResponse(r, exportsById)),
      counts,
      targets: fragranticaTargets(),
      timing: { medianBuildMs, parallel: fragranticaDraftsParallel, serverNow: new Date().toISOString() },
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const targets = fragranticaDraftTargets(body.targets);
    if (!targets.length) return response.status(400).json({ error: "Выберите хотя бы один магазин." });
    let perfumeIds = (Array.isArray(body.perfumeIds) ? body.perfumeIds : []).map(Number).filter((id) => id > 0);
    if (!perfumeIds.length && body.query && typeof body.query === "object") {
      // «Выбрать все» по текущим фильтрам списка — не больше 300 ароматов за раз
      const max = Math.min(300, Math.max(1, Number(body.limit) || 300));
      for (let page = 1; perfumeIds.length < max; page += 1) {
        const list = await listFragranticaPerfumes({ ...body.query, limit: 120, page });
        perfumeIds.push(...list.items.map((item) => item.id));
        if (!list.hasMore) break;
      }
      perfumeIds = perfumeIds.slice(0, max);
    }
    perfumeIds = [...new Set(perfumeIds)].slice(0, 300);
    if (!perfumeIds.length) return response.status(400).json({ error: "Нет ароматов для конвейера." });
    const prisma = await requireFragranticaDraftTables();
    const busy = await prisma.$queryRawUnsafe(
      `SELECT DISTINCT perfume_id AS id FROM fragrantica_drafts WHERE perfume_id = ANY($1::int[]) AND status IN ${FRAG_DRAFT_ACTIVE}`,
      perfumeIds,
    );
    const busyIds = new Set(busy.map((r) => Number(r.id)));
    const username = cleanText(request.session?.username) || null;
    let added = 0;
    const chosenVolumes = body.volumes && typeof body.volumes === "object" ? body.volumes : {};
    for (const perfumeId of perfumeIds) {
      if (busyIds.has(perfumeId)) continue;
      await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'perfume' AND status = 'skipped'`, perfumeId);
      const volumes = (Array.isArray(chosenVolumes[perfumeId]) ? chosenVolumes[perfumeId] : [])
        .map((v) => ({ volume: Number(fragFormatVolume(v?.volume)), tester: Boolean(v?.tester) }))
        .filter((v) => v.volume > 0 && v.volume < 5000)
        .slice(0, 20);
      await insertFragranticaDraft({
        perfumeId, kind: "perfume", typeKey: cleanText(body.typeKey) || null, targets: targets.map((t) => t.key), createdBy: username,
        data: volumes.length ? { volumes } : null,
      });
      added += 1;
    }
    await appendAudit(request, "fragrantica.drafts.add", { entityType: "fragrantica_draft", newValue: { added, alreadyInQueue: busyIds.size, shops: targets.map((t) => t.label) } });
    response.json({ ok: true, added, alreadyInQueue: busyIds.size });
  } catch (error) {
    next(error);
  }
});

// «Какие объёмы?» при отметке аромата: объёмы из PriceMaster (сколько строк, от какой цены) и в каких из
// выбранных магазинов этот объём уже продаётся
app.get("/api/fragrantica/drafts/volume-plan", requireAdmin, async (request, response, next) => {
  try {
    const perfumeId = Number(request.query.perfumeId);
    if (!perfumeId) return response.status(400).json({ error: "perfumeId обязателен" });
    const targets = fragranticaDraftTargets(cleanText(request.query.targets).split(",").filter(Boolean));
    const perfume = await Promise.race([
      fragranticaPerfumeForExport(perfumeId),
      new Promise((_, reject) => setTimeout(() => reject(new Error("Фрагрантика не ответила за минуту")), FRAG_PAGE_FETCH_TIMEOUT_MS).unref?.()),
    ]);
    const { rows } = await fragranticaPerfumePmRows(perfume);
    const plan = planFragranticaVolumes(rows, perfume);
    const presence = await fragranticaPerfumePresence(perfumeId);
    const volumes = plan.volumes.map((v) => ({
      volume: v.volume,
      rows: v.rows,
      inShops: targets.filter((t) => fragranticaVolumeInShop({ ...presence, shopId: t.id, volume: v.volume })).map((t) => t.label),
    }));
    // volumes the shops already sell but PriceMaster does not list now — shown, unchecked
    const known = new Set(volumes.map((v) => v.volume));
    for (const t of targets) {
      for (const v of presence.stock?.[t.id]?.v || []) {
        const key = Number(v);
        if (key > 3 && !known.has(key)) {
          known.add(key);
          volumes.push({ volume: key, rows: 0, inShops: targets.filter((x) => fragranticaVolumeInShop({ ...presence, shopId: x.id, volume: key })).map((x) => x.label) });
        }
      }
    }
    volumes.sort((a, b) => a.volume - b.volume);
    response.json({ ok: true, perfumeId, brand: perfume.brand, name: perfume.name, typeKey: plan.typeKey, volumes });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/volume", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const perfumeId = Number(body.perfumeId);
    const volume = Number(fragFormatVolume(body.volume));
    const targets = fragranticaDraftTargets(body.targets);
    if (!perfumeId || !volume) return response.status(400).json({ error: "Укажите объём." });
    if (!targets.length) return response.status(400).json({ error: "Выберите хотя бы один магазин." });
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'perfume' AND status IN ('skipped', 'attention')`, perfumeId);
    const draft = await insertFragranticaDraft({
      perfumeId, volume, tester: Boolean(body.tester), typeKey: FRAG_OZON_TYPES.some((t) => t.key === body.typeKey) ? body.typeKey : null,
      targets: targets.map((t) => t.key), createdBy: cleanText(request.session?.username) || null,
    });
    response.json({ ok: true, id: Number(draft.id) });
  } catch (error) {
    next(error);
  }
});

// Правки черновика. Тип, объём или тестер меняют карточку целиком — она собирается заново (описание остаётся).
/** A PriceMaster row picked by hand — only the fields the card and the price need. */
function fragranticaManualLinkRow(raw = {}) {
  const id = cleanText(raw.id);
  const price = Number(raw.price);
  if (!id || !cleanText(raw.name) || !(price > 0)) return null;
  return {
    id,
    rowId: cleanText(raw.rowId) || null,
    article: cleanText(raw.article),
    name: cleanText(raw.name).slice(0, 300),
    supplierName: cleanText(raw.supplierName).slice(0, 120),
    partnerId: cleanText(raw.partnerId),
    price,
    priceCurrency: cleanText(raw.priceCurrency) || "USD",
    updatedAt: raw.updatedAt || null,
    recommended: Boolean(raw.recommended),
    issues: Array.isArray(raw.issues) ? raw.issues.map(cleanText).slice(0, 5) : [],
    markup: Number(raw.markup) || 0,
    ozonPrice: Number(raw.ozonPrice) || 0,
    yandexPrice: Number(raw.yandexPrice) || 0,
    manual: true,
  };
}

// «Найти в PriceMaster» у черновика: свой запрос вместо автоматического подбора (без фильтров объёма и
// тестера — человек сам видит строку); цена посчитана по тем же правилам наценки
app.get("/api/fragrantica/drafts/:id/pm-search", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const q = cleanText(request.query.q);
    if (q.length < 2) return response.json({ ok: true, rows: [] });
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    const result = await fragranticaLinkSuggestionsData({
      perfumeId: draft.perfume_id,
      typeKey: draft.type_key || draft.data?.typeKey,
      volume: draft.volume_ml,
      tester: Boolean(draft.tester),
      q,
    });
    const have = new Set((draft.data?.linkRows || []).map((r) => r.id));
    response.json({ ok: true, rows: result.rows.slice(0, 30).map((r) => ({ ...r, linked: have.has(r.id) })) });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/fragrantica/drafts/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    if (["working", "sending", "sent"].includes(draft.status)) return response.status(409).json({ error: "Черновик сейчас собирается или уже отправлен." });
    const body = request.body || {};
    const data = { ...(draft.data || {}) };
    const fields = {};
    let rebuild = false;
    if (body.targets !== undefined) {
      const targets = fragranticaDraftTargets(body.targets);
      fields.targets = targets.map((t) => t.key);
      // a shop of a new style needs its «Пирамида аромата» — rebuild only the photos part (cached otherwise)
      if (targets.some((t) => !data.images?.notes?.[t.style])) rebuild = true;
    }
    if (body.typeKey !== undefined && FRAG_OZON_TYPES.some((t) => t.key === body.typeKey) && body.typeKey !== (draft.type_key || data.typeKey)) {
      fields.type_key = body.typeKey;
      rebuild = true;
    }
    if (body.volume !== undefined && Number(fragFormatVolume(body.volume)) && Math.abs(Number(fragFormatVolume(body.volume)) - Number(draft.volume_ml)) > 0.001) {
      fields.volume_ml = Number(fragFormatVolume(body.volume));
      rebuild = true;
    }
    if (body.tester !== undefined && Boolean(body.tester) !== Boolean(draft.tester)) {
      fields.tester = Boolean(body.tester);
      rebuild = true;
    }
    for (const key of ["name", "barcode"]) if (body[key] !== undefined) data[key] = cleanText(body[key]);
    // own photos (тестер, пробник, набор look different): reorder / remove — only ones uploaded to this draft
    if (Array.isArray(body.customPhotos)) {
      const known = new Set(Array.isArray(data.customPhotos) ? data.customPhotos : []);
      data.customPhotos = body.customPhotos.map(cleanText).filter((u) => known.has(u)).slice(0, 15);
      data.customPhotosAuto = false;
      if (!data.customPhotos.length) data.onlyCustomPhotos = false;
    }
    if (typeof body.onlyCustomPhotos === "boolean") data.onlyCustomPhotos = body.onlyCustomPhotos && (data.customPhotos || []).length > 0;
    for (const key of ["price", "oldPrice", "yandexPrice"]) if (body[key] !== undefined) data[key] = Math.max(0, Math.round(Number(body[key]) || 0));
    if (body.dims && typeof body.dims === "object") {
      data.dims = { ...(data.dims || {}) };
      for (const key of ["depth", "width", "height", "weight"]) if (body.dims[key] !== undefined) data.dims[key] = Math.max(0, Math.round(Number(body.dims[key]) || 0));
    }
    // Rows found by hand («Найти в PriceMaster»): added to the card's rows and selected
    if (Array.isArray(body.addLinks) && body.addLinks.length) {
      const rows = [...(data.linkRows || [])];
      const added = [];
      for (const raw of body.addLinks.slice(0, 10)) {
        const row = fragranticaManualLinkRow(raw);
        if (!row) continue;
        if (!rows.some((r) => r.id === row.id)) rows.push(row);
        added.push(row.id);
      }
      data.linkRows = rows.slice(-60);
      if (!Array.isArray(body.selectedLinks)) body.selectedLinks = [...new Set([...(data.selectedLinks || []), ...added])];
    }
    if (Array.isArray(body.selectedLinks)) {
      data.selectedLinks = body.selectedLinks.map(String).filter((id) => (data.linkRows || []).some((r) => r.id === id));
      const rows = (data.linkRows || []).filter((r) => data.selectedLinks.includes(r.id));
      if (rows.length && !body.keepPrice) {
        const preview = await fragranticaPricePreview(rows);
        Object.assign(data, { price: preview.price, oldPrice: preview.oldPrice, yandexPrice: preview.yandexPrice || preview.price, supplierName: preview.supplierName, markup: preview.markup });
      }
    }
    if (body.brand && Number(body.brand.id)) {
      const value = { dictionary_value_id: Number(body.brand.id), value: cleanText(body.brand.value) };
      data.attributes = [...(data.attributes || []).filter((a) => Number(a.id) !== FRAG_OZON_ATTR.brand), { id: FRAG_OZON_ATTR.brand, values: [value] }];
      data.brandMatched = true;
    }
    const prisma = await requireFragranticaDraftTables();
    if (typeof body.description === "string") {
      // Описание общее для аромата: правка уходит во все его черновики и в следующие объёмы
      data.description = body.description;
      data.descriptionSource = "edited";
      await saveFragranticaCardDescription(draft.perfume_id, body.description);
      await prisma.$executeRawUnsafe(
        `UPDATE fragrantica_drafts SET data = jsonb_set(jsonb_set(data, '{description}', to_jsonb($2::text)), '{descriptionSource}', '"edited"'), updated_at = now()
          WHERE perfume_id = $1 AND id <> $3 AND kind = 'card' AND data IS NOT NULL AND status IN ('ready', 'attention', 'failed')`,
        Number(draft.perfume_id), body.description, Number(draft.id),
      );
    }
    if (rebuild) {
      fields.status = "queued";
      fields.stage = null;
      fields.error = null;
    } else {
      const targets = fragranticaDraftTargets(fields.targets || draft.targets);
      const missing = fragranticaDraftMissing({ ...draft, perfumeId: Number(draft.perfume_id), data }, targets);
      if (data.brandMatched === false && !missing.includes("Бренд")) missing.unshift("Бренд");
      data.missing = missing;
      if (["ready", "attention", "failed"].includes(draft.status)) {
        fields.status = missing.length ? "attention" : "ready";
        fields.error = missing.length ? `Не хватает: ${missing.join(", ")}` : null;
      }
    }
    fields.data = data;
    const updated = await updateFragranticaDraft(draft.id, fields);
    response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
  } catch (error) {
    next(error);
  }
});

// «Свои фото»: the first one replaces the bottle photo (main), the rest go right after it. Kept with the
// Fragrantica media (uploads/images is pruned after 14 days, marketplaces may fetch a picture again later).
app.post("/api/fragrantica/drafts/:id/photos", requireAdmin, uploadImages.array("photos", 10), async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    if (["working", "sending", "sent"].includes(draft.status)) return response.status(409).json({ error: "Черновик сейчас собирается или уже отправлен." });
    const files = Array.isArray(request.files) ? request.files : [];
    if (!files.length) return response.status(400).json({ error: "Выберите фото." });
    await fs.mkdir(path.dirname(fragranticaMediaPath("cards", "x.jpg")), { recursive: true });
    const urls = [];
    for (const [i, file] of files.entries()) {
      const name = `custom-${Number(draft.id)}-${Date.now().toString(36)}${i}.jpg`;
      // white background (PNG with transparency), upright, ≤ 2000 px, JPEG — what Ozon and Market accept best
      const meta = await sharp(file.buffer).metadata().catch(() => null);
      if (!meta || !meta.width || !meta.height) return response.status(400).json({ error: `«${file.originalname}» — не картинка.` });
      if (Math.min(meta.width, meta.height) < 400) return response.status(400).json({ error: `«${file.originalname}» слишком маленькое (${meta.width}×${meta.height}) — нужно от 400 px.` });
      const jpg = await sharp(file.buffer).rotate().flatten({ background: "#ffffff" })
        .resize({ width: 2000, height: 2000, fit: "inside", withoutEnlargement: true }).jpeg({ quality: 92 }).toBuffer();
      await fs.writeFile(fragranticaMediaPath("cards", name), jpg);
      urls.push(fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", name)));
    }
    const data = { ...(draft.data || {}) };
    data.customPhotos = [...(Array.isArray(data.customPhotos) ? data.customPhotos : []), ...urls].slice(0, 15);
    data.customPhotosAuto = false;
    const updated = await updateFragranticaDraft(draft.id, { data });
    response.json({ ok: true, added: urls.length, draft: fragranticaDraftResponse(updated) });
  } catch (error) {
    next(error);
  }
});

// «Подобрать похожий бренд»: поиск по справочнику брендов Ozon (для карточки, чей бренд не нашёлся по имени)
app.get("/api/fragrantica/drafts/:id/brand-search", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    const q = cleanText(request.query.q);
    if (q.length < 2) return response.json({ ok: true, items: draft.data?.brandCandidates || [] });
    const ozon = fragranticaDraftTargets(draft.targets).find((t) => t.kind === "ozon");
    const account = fragranticaResolveOzonAccount(ozon?.id);
    const type = fragOzonTypeByKey(draft.type_key || draft.data?.typeKey || "edp");
    const items = await fragranticaSearchDict(account, type.typeId, FRAG_OZON_ATTR.brand, q, 20);
    response.json({ ok: true, items: items.map((v) => ({ id: v.id, value: v.value })) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/:id/rebuild", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    if (["working", "sending", "sent"].includes(draft.status)) return response.status(409).json({ error: "Черновик сейчас собирается или уже отправлен." });
    if (request.body?.newDescription) {
      // a new AI text replaces the shared description (generateFragranticaDescription saves it) in every draft
      const prisma = await requireFragranticaDraftTables();
      const res = await generateFragranticaDescription({ perfumeId: draft.perfume_id, typeKey: draft.type_key, tester: draft.tester, marketplace: fragranticaDraftTargets(draft.targets).some((t) => t.kind === "yandex") ? "yandex" : "ozon" });
      await prisma.$executeRawUnsafe(
        `UPDATE fragrantica_drafts SET data = jsonb_set(jsonb_set(data, '{description}', to_jsonb($2::text)), '{descriptionSource}', '"ai"'), updated_at = now()
          WHERE perfume_id = $1 AND kind = 'card' AND data IS NOT NULL AND status IN ('ready', 'attention', 'failed')`,
        Number(draft.perfume_id), res.description,
      );
      const updated = await readFragranticaDraft(draft.id);
      return response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
    }
    const data = { ...(draft.data || {}) };
    delete data.retryAt;
    delete data.fetchRetries;
    const updated = await updateFragranticaDraft(draft.id, { status: "queued", stage: null, error: null, data });
    response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/fragrantica/drafts/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE id = $1 AND status NOT IN ('working', 'sending')`, Number(request.params.id));
    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/approve", requireAdmin, async (request, response, next) => {
  try {
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter((id) => id > 0).slice(0, 600);
    if (!ids.length) return response.status(400).json({ error: "Нечего одобрять." });
    const prisma = await requireFragranticaDraftTables();
    const rows = await prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'approved', error = NULL, created_by = COALESCE($2, created_by), updated_at = now()
        WHERE id = ANY($1::bigint[]) AND kind = 'card' AND status IN ('ready', 'failed') RETURNING id`,
      ids, cleanText(request.session?.username) || null,
    );
    await appendAudit(request, "fragrantica.drafts.approve", { entityType: "fragrantica_draft", entityId: rows.map((r) => r.id).join(","), newValue: { approved: rows.length } });
    response.json({ ok: true, approved: rows.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/clear", requireAdmin, async (request, response, next) => {
  try {
    const allowed = ["sent", "skipped", "attention", "ready", "failed", "queued"];
    const statuses = (Array.isArray(request.body?.statuses) ? request.body.statuses : ["sent", "skipped"]).filter((s) => allowed.includes(s));
    if (!statuses.length) return response.status(400).json({ error: "statuses required" });
    const prisma = await requireFragranticaDraftTables();
    // improvement drafts are kept: a skipped one marks «не улучшать», a sent one is the history of that card
    const removed = await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE status = ANY($1::text[]) AND NOT coalesce(data ? 'existing', false)`, statuses);
    response.json({ ok: true, removed: Number(removed) || 0 });
  } catch (error) {
    next(error);
  }
});
