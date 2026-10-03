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
const fragranticaDraftsParallel = Math.max(1, Number(process.env.FRAGRANTICA_DRAFTS_PARALLEL || 12) || 12);
const FRAG_DRAFT_ACTIVE = "('queued', 'working', 'ready', 'attention', 'approved', 'sending', 'failed')";
let fragranticaDraftsTablesReady = false;
let fragranticaDraftsRunning = false;
// continuous pool: a free slot takes the next card at once (a slow card no longer holds the whole batch)
let fragranticaDraftsInFlight = 0;
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
    if (!a) throw new Error(`Карточка ${offerId} не найдена на Ozon`);
    const descAttr = (a.attributes || []).find((x) => Number(x.id) === 4191);
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
    };
  }
  const shop = fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(existing.target));
  if (!shop) throw new Error("Магазин Маркета не найден");
  const mapping = (await getYandexOfferMappingsByOfferIds(shop, [offerId]))[0];
  const offer = mapping?.offer || mapping || {};
  if (!offer.offerId && !offer.name) throw new Error(`Карточка ${offerId} не найдена на Маркете`);
  const wd = offer.weightDimensions || {};
  return {
    before: { name: cleanText(offer.name), photos: (offer.pictures || []).map(cleanText).filter(Boolean).slice(0, 15), description: cleanText(offer.description).slice(0, 3000) },
    price: 0, oldPrice: 0, yandexPrice: Math.round(Number(offer.basicPrice?.value || 0)),
    barcode: cleanText((offer.barcodes || [])[0]),
    dims: { depth: Math.round(Number(wd.length || 0) * 10), width: Math.round(Number(wd.width || 0) * 10), height: Math.round(Number(wd.height || 0) * 10), weight: Math.round(Number(wd.weight || 0) * 1000) },
  };
}

/** Puts the weakest matched cards (not from Fragrantica, not improved yet) into the conveyor. */
async function enqueueFragranticaImprovements({ limit = FRAG_IMPROVE_PER_SCAN, createdBy = "улучшение" } = {}) {
  if (!(limit > 0)) return { queued: 0 };
  const prisma = await requireFragranticaDraftTables();
  const exists = await prisma.$queryRawUnsafe(`SELECT to_regclass('fragrantica_card_matches') IS NOT NULL AS ok`);
  if (!exists[0]?.ok) return { queued: 0, reason: "сопоставление ещё не готово" };
  const targets = fragranticaTargets();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT m.target, m.offer_id, m.perfume_id, m.volume_ml, m.tester, w.marketplace::text AS marketplace,
            (SELECT min(q.content_rating) FROM card_quality q WHERE q.offer_id = m.offer_id) AS rating
       FROM fragrantica_card_matches m
       JOIN warehouse_products w ON w.target = m.target AND w.offer_id = m.offer_id AND w.archived = false
      WHERE m.volume_ml IS NOT NULL AND m.offer_id !~* '^FR[0-9]'
        AND NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.account_id = m.target AND e.offer_id = m.offer_id)
        AND NOT EXISTS (SELECT 1 FROM fragrantica_drafts d WHERE d.data->'existing'->>'offerId' = m.offer_id AND d.data->'existing'->>'target' = m.target)
      ORDER BY rating ASC NULLS LAST, m.offer_id
      LIMIT $1`,
    Math.min(500, Number(limit) || 0),
  ).catch(async (error) => {
    // card_quality appears with the first card check — order by offer id until then
    if (!/card_quality/.test(String(error?.message))) throw error;
    return prisma.$queryRawUnsafe(
      `SELECT m.target, m.offer_id, m.perfume_id, m.volume_ml, m.tester, w.marketplace::text AS marketplace, NULL AS rating
         FROM fragrantica_card_matches m JOIN warehouse_products w ON w.target = m.target AND w.offer_id = m.offer_id AND w.archived = false
        WHERE m.volume_ml IS NOT NULL AND m.offer_id !~* '^FR[0-9]'
          AND NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.account_id = m.target AND e.offer_id = m.offer_id)
          AND NOT EXISTS (SELECT 1 FROM fragrantica_drafts d WHERE d.data->'existing'->>'offerId' = m.offer_id AND d.data->'existing'->>'target' = m.target)
        ORDER BY m.offer_id LIMIT $1`,
      Math.min(500, Number(limit) || 0),
    );
  });
  let queued = 0;
  for (const r of rows) {
    const target = targets.find((t) => t.id === cleanText(r.target) && t.kind === cleanText(r.marketplace));
    if (!target) continue;
    await insertFragranticaDraft({
      perfumeId: Number(r.perfume_id), kind: "card", volume: Number(r.volume_ml), tester: Boolean(r.tester), targets: [target.key], createdBy,
      data: { existing: { marketplace: target.kind, target: target.id, offerId: cleanText(r.offer_id), rating: r.rating === null ? null : Number(r.rating) } },
    });
    queued += 1;
  }
  if (queued) logger.info("fragrantica improvements queued", { queued });
  return { queued };
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
  const mark = (part) => (value) => {
    done.add(part);
    updateFragranticaDraft(draft.id, { stage: `build:${[...done].join(",")}` }).catch(() => {});
    return value;
  };
  const ozonTarget = targets.find((t) => t.kind === "ozon");
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  // the type is known before the form: the draft's own, else the guess from the name (same rule as the form)
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === draft.type_key) ? draft.type_key : fragOzonGuessTypeKey(perfume);
  const styles = [...new Set(targets.map((t) => t.style))];
  const marketplace = targets.some((t) => t.kind === "yandex") ? "yandex" : "ozon";

  // Ozon form, supplier links + price, photos and the description don't depend on each other — run together
  const [form, linkPart, photoPart, descriptionPart] = await Promise.all([
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
  };
  if (existing) {
    try {
      const state = await fragranticaExistingCardState(existing);
      Object.assign(data, {
        offerId: existing.offerId,
        price: state.price, oldPrice: state.oldPrice, yandexPrice: state.yandexPrice,
        barcode: state.barcode,
        supplierName: "", markup: 0,
        existing: { ...existing, state },
        // the Market title the export will use (Ozon keeps data.name)
        marketName: buildFragranticaMarketName({ perfume, typeKey, volume: draft.volume_ml, tester: Boolean(draft.tester) }),
      });
      if (state.dims.depth && state.dims.width && state.dims.height && state.dims.weight) data.dims = state.dims;
    } catch (error) {
      data.existing = existing;
      return updateFragranticaDraft(draft.id, { status: "attention", stage: null, type_key: typeKey, error: `Улучшение: ${error?.message || error}`, data });
    }
  }
  const built = { ...draft, perfumeId, data };
  const missing = fragranticaDraftMissing(built, targets);
  if (!form.brandMatched && !missing.includes("Бренд")) missing.unshift("Бренд");
  return updateFragranticaDraft(draft.id, {
    status: missing.length ? "attention" : "ready",
    stage: null,
    type_key: typeKey,
    error: missing.length ? `Не хватает: ${missing.join(", ")}` : null,
    data: { ...data, missing },
  });
}

async function sendFragranticaDraft(draft) {
  const targets = fragranticaDraftTargets(draft.targets);
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
    const claimed = await prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'working', stage = 'start', started_at = now(), updated_at = now()
       WHERE id IN (
         SELECT d.id FROM fragrantica_drafts d
          WHERE d.status = 'queued' AND d.kind = 'card'
          ORDER BY d.id
          LIMIT $1
          FOR UPDATE SKIP LOCKED)
       RETURNING *`,
      Math.max(0, fragranticaDraftsParallel - fragranticaDraftsInFlight),
    );
    for (const draft of claimed) {
      fragranticaDraftsInFlight += 1;
      processFragranticaDraft(draft)
        .catch(() => {})
        .finally(() => { fragranticaDraftsInFlight = Math.max(0, fragranticaDraftsInFlight - 1); });
    }
    return { status: "ok", built: claimed.length + perfumes.length + fragranticaDraftsInFlight + fragranticaExpandInFlight };
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
    const updated = await updateFragranticaDraft(draft.id, { data });
    response.json({ ok: true, added: urls.length, draft: fragranticaDraftResponse(updated) });
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
    const updated = await updateFragranticaDraft(draft.id, { status: "queued", stage: null, error: null });
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
