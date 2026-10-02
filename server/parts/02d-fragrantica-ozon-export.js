// «Добавить на Ozon» со страницы «Фрагрантика».
//
//   GET  /api/fragrantica/ozon/form              — предзаполненная форма (атрибуты категории + значения)
//   GET  /api/fragrantica/ozon/attribute-values  — поиск по справочнику атрибута
//   POST /api/fragrantica/ozon/images            — задача: фото флакона + «Пирамида аромата» через parfumdeclaration
//   GET  /api/fragrantica/ozon/images/:jobId
//   POST /api/fragrantica/ozon/export            — создать карточку (/v3/product/import) или поставить в очередь лимита
//   GET  /api/fragrantica/ozon/exports[/:id]     — история / статус (pending опрашивается в /v1/product/import/info)
//   POST /api/fragrantica/ozon/exports/:id/retry
//
// Дневной лимит создания (/v4/product/info/limit) исчерпан → status=queued_limit, worker отправит после
// сброса (00:00 UTC = 03:00 МСК). После imported Ozon генерирует штрихкод (/v1/barcode/generate).
// Фото в конце карточки («Секреты нанесения», «Спасибо») добавляет parfumdeclaration сам, ежечасно.

const fragranticaExportQueueEnabled = process.env.FRAGRANTICA_EXPORT_QUEUE_ENABLED !== "false";
const fragranticaExportQueueIntervalMs = 5 * 60_000;
const fragranticaDictCache = new Map();
const fragranticaImageJobs = new Map();

function fragranticaOzonAccounts() {
  return getOzonAccounts().map((account) => ({
    id: cleanText(account.id),
    name: cleanText(account.name) || `Ozon ${cleanText(account.clientId)}`,
    clientId: cleanText(account.clientId),
    style: fragranticaCardStyleForAccount(account),
  }));
}

// Стиль картинки нот: AURA для её кабинета, иначе Magic Stick. FRAGRANTICA_CARD_STYLES='{"<clientId>":"aura"}'.
function fragranticaCardStyleForAccount(account = {}) {
  try {
    const map = JSON.parse(process.env.FRAGRANTICA_CARD_STYLES || "{}");
    const style = map[cleanText(account.clientId)] || map[cleanText(account.id)];
    if (style) return style;
  } catch {
    // ignore bad JSON — fall back to the name check
  }
  return /aura/i.test(cleanText(account.name)) || cleanText(account.clientId) === "2533393" ? "aura" : "magicstick";
}

// Магазины для галочек в форме: кабинеты Ozon + бизнес Яндекс Маркета. Подписи и стили картинки нот
// по clientId / businessId (FRAGRANTICA_SHOP_LABELS / FRAGRANTICA_CARD_STYLES — JSON для других).
const FRAG_DEFAULT_SHOP_LABELS = { "1304220": "Magic Stick", "2533393": "AURA", "171782339": "Parfumerius" };
const FRAG_CARD_STYLES = ["magicstick", "aura", "parfumerius"];

function fragranticaEnvMap(name) {
  try {
    const value = JSON.parse(process.env[name] || "{}");
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

function fragranticaShopLabel(key, fallback) {
  return fragranticaEnvMap("FRAGRANTICA_SHOP_LABELS")[key] || FRAG_DEFAULT_SHOP_LABELS[key] || fallback;
}

function fragranticaYandexShops() {
  return uniqueYandexShopsByBusiness();
}

function fragranticaTargets() {
  const ozon = getOzonAccounts().map((account) => ({
    key: `ozon:${cleanText(account.id)}`,
    kind: "ozon",
    id: cleanText(account.id),
    marketplace: "Ozon",
    label: fragranticaShopLabel(cleanText(account.clientId), cleanText(account.name) || `Ozon ${cleanText(account.clientId)}`),
    style: fragranticaCardStyleForAccount(account),
  }));
  const yandex = fragranticaYandexShops().map((shop) => {
    const business = cleanText(shop.businessId);
    const style = fragranticaEnvMap("FRAGRANTICA_CARD_STYLES")[business] || "parfumerius";
    return {
      key: `yandex:${cleanText(shop.id)}`,
      kind: "yandex",
      id: cleanText(shop.id),
      marketplace: "Яндекс Маркет",
      label: fragranticaShopLabel(business, cleanText(shop.name) || `Маркет ${business}`),
      style: FRAG_CARD_STYLES.includes(style) ? style : "parfumerius",
    };
  });
  return [...ozon, ...yandex];
}

function fragranticaResolveOzonAccount(accountId) {
  const accounts = getOzonAccounts();
  const target = cleanText(accountId);
  const account = (target && accounts.find((a) => cleanText(a.id) === target || cleanText(a.clientId) === target)) || accounts[0];
  if (!account) {
    const error = new Error("Кабинет Ozon не найден. Добавьте его в настройках.");
    error.statusCode = 400;
    throw error;
  }
  return account;
}

async function fragranticaDictValues(account, typeId, attributeId) {
  const key = `${account.id}:${typeId}:${attributeId}`;
  const cached = fragranticaDictCache.get(key);
  if (cached && Date.now() - cached.at < 6 * 3_600_000) return cached.values;
  const values = await ozonGetAttributeDictValues(account, FRAG_OZON_CATEGORY_ID, typeId, attributeId);
  fragranticaDictCache.set(key, { at: Date.now(), values });
  return values;
}

// Dictionary searches repeat for every volume of a perfume (brand, country, TN VED) — cached 6 h
const fragranticaDictSearchCache = new Map();

async function fragranticaSearchDict(account, typeId, attributeId, value, limit = 20) {
  const query = cleanText(value);
  if (query.length < 2) return [];
  const key = `${cleanText(account?.id)}:${Number(typeId)}:${Number(attributeId)}:${query.toLowerCase()}:${limit}`;
  const cached = fragranticaDictSearchCache.get(key);
  if (cached && Date.now() - cached.at < 6 * 3_600_000) return cached.values;
  const data = await ozonRequest("/v1/description-category/attribute/values/search", {
    description_category_id: FRAG_OZON_CATEGORY_ID,
    type_id: Number(typeId),
    attribute_id: Number(attributeId),
    value: query,
    limit,
  }, account);
  const values = (data.result || []).map((v) => ({ id: Number(v.id), value: String(v.value || ""), info: v.info || "" }));
  if (fragranticaDictSearchCache.size > 5000) fragranticaDictSearchCache.clear();
  fragranticaDictSearchCache.set(key, { at: Date.now(), values });
  return values;
}

function fragBrandKey(text) {
  return String(text || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^0-9a-zа-яё]+/gi, "");
}

async function fragranticaFindBrand(account, typeId, brand) {
  const candidates = await fragranticaSearchDict(account, typeId, FRAG_OZON_ATTR.brand, brand, 20).catch(() => []);
  const key = fragBrandKey(brand);
  const exact = candidates.find((c) => fragBrandKey(c.value) === key);
  return { match: exact || null, candidates: candidates.slice(0, 10) };
}

function fragranticaTnvedCode(typeId) {
  try {
    const file = require("fs").readFileSync(path.join(dataDir, "ozon-tnved-assignments.json"), "utf8");
    const assignments = JSON.parse(file).assignments || [];
    const hit = assignments.find((a) => Number(a.descCatId) === FRAG_OZON_CATEGORY_ID && Number(a.typeId) === Number(typeId))
      || assignments.find((a) => Number(a.descCatId) === FRAG_OZON_CATEGORY_ID && !Number(a.typeId));
    if (hit?.tnvedCode) return cleanText(hit.tnvedCode);
  } catch {
    // no assignments file — default code
  }
  return FRAG_OZON_DEFAULTS.tnvedCode;
}

async function fragranticaFindTnved(account, typeId, volume) {
  const code = fragranticaTnvedCode(typeId);
  const values = await fragranticaSearchDict(account, typeId, FRAG_OZON_ATTR.tnved, code, 10).catch(() => []);
  const small = Number(volume) > 0 && Number(volume) <= 3;
  return values.find((v) => v.value.startsWith(code) && /до 3 мл/i.test(v.value) === small) || values[0] || null;
}

async function fragranticaPerfumeForExport(perfumeId) {
  let row = await readFragranticaPerfume(perfumeId);
  if (!row) {
    const error = new Error("Аромат не найден в каталоге.");
    error.statusCode = 404;
    throw error;
  }
  if (!row.detail_at) {
    await fetchAndStoreFragranticaPerfume(row.url);
    row = await readFragranticaPerfume(perfumeId);
  }
  return { ...(row.detail || {}), id: Number(row.id), brand: row.brand, name: row.name, gender: row.gender, year: row.year, url: row.url };
}

function fragranticaAttributeForForm(attr) {
  return {
    id: Number(attr.id),
    name: attr.name,
    description: attr.description || "",
    type: attr.type,
    required: Boolean(attr.is_required),
    collection: Boolean(attr.is_collection),
    dictionaryId: Number(attr.dictionary_id || 0),
    maxValues: Number(attr.max_value_count || 0),
    group: attr.group_name || "",
  };
}

function fragranticaAbsoluteUrl(url) {
  const value = cleanText(url);
  return value.startsWith("/") ? `${fragranticaPublicBaseUrl()}${value}` : value;
}

// Предзаполненная форма карточки: и для формы на странице, и для конвейера (02d-fragrantica-drafts.js).
// Описание — общее для всех объёмов аромата (readFragranticaCardDescription), иначе текст Фрагрантики.
async function buildFragranticaFormData(query = {}) {
  const perfumeId = Number(query.perfumeId);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const account = fragranticaResolveOzonAccount(query.accountId);
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === query.typeKey) ? query.typeKey : fragOzonGuessTypeKey(perfume);
  const type = fragOzonTypeByKey(typeKey);
  const volume = fragFormatVolume(query.volume) || "";
  const tester = query.tester === "1" || query.tester === "true" || query.tester === true;
  const savedDescription = await readFragranticaCardDescription(perfumeId).catch(() => "");

  const brandInfo = await fragranticaBrandInfo(perfume.brandSlug).catch(() => ({ country: "", owner: "" }));
  const countryRu = fragranticaCountryRu(brandInfo.country);
  const [categoryAttrs, brand, genderDict, classificationDict, tnved, countryValues] = await Promise.all([
    ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId),
    fragranticaFindBrand(account, type.typeId, perfume.brand),
    fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.gender).catch(() => []),
    fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.classification).catch(() => []),
    fragranticaFindTnved(account, type.typeId, volume),
    countryRu ? fragranticaSearchDict(account, type.typeId, FRAG_OZON_ATTR.country, countryRu, 10).catch(() => []) : Promise.resolve([]),
  ]);
  const country = countryValues.find((v) => v.value.toLowerCase() === countryRu.toLowerCase()) || null;
  const offerId = buildFragranticaOfferId({ perfumeId, volume, tester });
  const dimsTemplates = (await readFragranticaState("settings").catch(() => ({}))).dimsTemplates || [];
  const prefill = buildFragranticaOzonPrefill({
    dimsTemplates,
    perfume: savedDescription ? { ...perfume, description: savedDescription } : perfume,
    typeKey,
    volume,
    tester,
    offerId,
    lookups: {
      brand: brand.match,
      type: { id: type.typeId, value: type.label },
      gender: fragGenderValues(perfume.gender, genderDict),
      classification: matchFragranticaClassification(perfume, classificationDict),
      tnved,
      country,
      producer: perfume.brand,
    },
  });
  const values = Object.fromEntries(prefill.attributes.map((a) => [a.id, a.values]));
  const attributes = categoryAttrs
    .map(fragranticaAttributeForForm)
    .sort((a, b) => Number(b.required) - Number(a.required))
    .map((attr) => ({ ...attr, values: values[attr.id] || [] }));
  const prisma = await requireFragranticaTables();
  const exports = await prisma.$queryRawUnsafe(
    `SELECT id, marketplace, account_name AS "accountName", offer_id AS "offerId", volume_ml AS "volume", tester, status, product_id AS "productId", error, created_at AS "createdAt"
     FROM fragrantica_exports WHERE perfume_id = $1 ORDER BY id DESC`,
    perfumeId,
  );
  return {
    ok: true,
    perfume: { id: perfume.id, brand: perfume.brand, name: perfume.name, gender: perfume.gender, year: perfume.year, notes: perfume.notes, accords: perfume.accords },
    accounts: fragranticaOzonAccounts(),
    targets: fragranticaTargets(),
    account: { id: cleanText(account.id), name: cleanText(account.name), style: fragranticaCardStyleForAccount(account) },
    types: FRAG_OZON_TYPES,
    typeKey,
    typeId: type.typeId,
    volume,
    tester,
    offerId,
    name: prefill.name,
    vat: FRAG_OZON_DEFAULTS.vat,
    vatByTarget: Object.fromEntries(fragranticaTargets().map((t) => [t.key, t.kind === "ozon"
      ? fragranticaVatForClientId(cleanText(getOzonAccounts().find((a) => cleanText(a.id) === t.id)?.clientId), fragranticaEnvMap("FRAGRANTICA_VAT_BY_ACCOUNT"))
      : "0.05"])),
    country: brandInfo.country ? { source: brandInfo.country, ozon: country?.value || null } : null,
    dims: prefill.dims,
    attributes,
    brandMatched: Boolean(brand.match),
    brandCandidates: brand.candidates,
    sourceImage: fragranticaMediaUrl("images", `${perfumeId}.jpg`),
    exports: exports.map((e) => ({ ...e, id: Number(e.id), productId: e.productId ? Number(e.productId) : null, volume: e.volume === null ? null : Number(e.volume) })),
    descriptionShared: Boolean(savedDescription),
    categoryAttrs,
  };
}

app.get("/api/fragrantica/ozon/form", requireAdmin, async (request, response, next) => {
  try {
    const { categoryAttrs: _attrs, ...form } = await buildFragranticaFormData(request.query);
    response.json(form);
  } catch (error) {
    next(error);
  }
});

// «Написать описание ИИ» в форме: тот же промпт, что у склада, по фактам Фрагрантики.
// marketplace=yandex, если среди магазинов есть Маркет (у него правила строже — текст годится обоим).
async function generateFragranticaDescription(body = {}) {
  const perfume = await fragranticaPerfumeForExport(Number(body.perfumeId));
  const type = fragOzonTypeByKey(body.typeKey);
  const marketplace = body.marketplace === "ozon" ? "ozon" : "yandex";
  const tester = Boolean(body.tester);
  const genderLabel = { male: "мужской", female: "женский", unisex: "унисекс" }[perfume.gender] || undefined;
  const source = Object.fromEntries(Object.entries({
    marketplace,
    // No volume in the text: the same description goes to every volume of the perfume
    name: buildFragranticaOzonName({ perfume, typeKey: type.key, tester }),
    brand: perfume.brand,
    perfume: perfume.name,
    type: type.nameLabel,
    gender: genderLabel,
    tester: tester || undefined,
    ...fragranticaFactsFromDetail(perfume),
  }).filter(([, v]) => v !== undefined));
  const { data, completion } = await createTextAiJson(buildPerfumeCopyMessages(source, marketplace), { temperature: 0.7 });
  const description = normalizeParagraphText(data.description, 5000);
  if (!description) {
    const error = new Error("AI не вернул описание. Попробуйте ещё раз.");
    error.statusCode = 502;
    error.code = "text_ai_empty";
    throw error;
  }
  // every other volume of this perfume reuses this text
  await saveFragranticaCardDescription(perfume.id, description);
  return {
    ok: true,
    description,
    name: cleanText(data.name).slice(0, 200),
    bulletPoints: (Array.isArray(data.bulletPoints) ? data.bulletPoints : []).map((b) => cleanText(b)).filter(Boolean).slice(0, 8),
    seoKeywords: (Array.isArray(data.seoKeywords) ? data.seoKeywords : []).map((b) => cleanText(b)).filter(Boolean).slice(0, 12),
    model: cleanText(completion?.model),
  };
}

app.post("/api/fragrantica/ozon/describe", requireAdmin, async (request, response, next) => {
  try {
    response.json(await generateFragranticaDescription(request.body || {}));
  } catch (error) {
    if (error?.code === "text_ai_empty") return response.status(502).json({ error: error.message, code: error.code });
    next(error);
  }
});

// Настройки страницы «Фрагрантика»: шаблоны габаритов по объёму.
app.get("/api/fragrantica/settings", requireAdmin, async (_request, response, next) => {
  try {
    const settings = await readFragranticaState("settings");
    response.json({ ok: true, dimsTemplates: normalizeFragranticaDimsTemplates(settings.dimsTemplates) });
  } catch (error) {
    next(error);
  }
});

app.put("/api/fragrantica/settings", requireAdmin, async (request, response, next) => {
  try {
    const dimsTemplates = normalizeFragranticaDimsTemplates(request.body?.dimsTemplates);
    await writeFragranticaState("settings", { dimsTemplates });
    await appendAudit(request, "fragrantica.settings.update", { entityType: "fragrantica_settings", newValue: { dimsTemplates } });
    response.json({ ok: true, dimsTemplates });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/attribute-values", requireAdmin, async (request, response, next) => {
  try {
    const account = fragranticaResolveOzonAccount(request.query.accountId);
    const typeId = Number(request.query.typeId) || FRAG_OZON_TYPES[0].typeId;
    const attributeId = Number(request.query.attributeId);
    if (!attributeId) return response.status(400).json({ error: "attributeId required" });
    const q = cleanText(request.query.q);
    const items = q.length >= 2
      ? await fragranticaSearchDict(account, typeId, attributeId, q, 30)
      : (await fragranticaDictValues(account, typeId, attributeId)).slice(0, 100).map((v) => ({ id: Number(v.id), value: String(v.value || ""), info: v.info || "" }));
    response.json({ ok: true, items });
  } catch (error) {
    next(error);
  }
});

// ─── Фото через parfumdeclaration ──────────────────────────────────────────

// Фото рисуются здесь, на davidsklad (мощнее): тот же рендер parfumdeclaration, собранный в
// lib/perfume-render (scripts/build-perfume-render.cjs). Нет onnxruntime-node / ошибка — как раньше,
// через parfumdeclaration. FRAGRANTICA_RENDER=pd — всегда через parfumdeclaration.
let fragranticaLocalRenderer;
let fragranticaLocalRenderChain = Promise.resolve();

function loadFragranticaLocalRenderer() {
  if (process.env.FRAGRANTICA_RENDER === "pd") return null;
  if (fragranticaLocalRenderer !== undefined) return fragranticaLocalRenderer;
  try {
    fragranticaLocalRenderer = require("./lib/perfume-render/index.cjs");
  } catch (error) {
    logger.warn("fragrantica local renderer unavailable, using parfumdeclaration", { detail: error?.message || String(error) });
    fragranticaLocalRenderer = null;
  }
  return fragranticaLocalRenderer;
}

async function fragranticaSourceBuffer(imageUrl) {
  const local = /\/uploads\/fragrantica\/(images|thumbs|cards)\/([^/?#]+)$/.exec(String(imageUrl || ""));
  if (local && fragFs.existsSync(fragranticaMediaPath(local[1], local[2]))) return fragFs.promises.readFile(fragranticaMediaPath(local[1], local[2]));
  const res = await fetch(imageUrl, { signal: AbortSignal.timeout(30_000) });
  if (!res.ok) throw new Error(`Фото не скачалось: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

/** Render on this server; one perfume at a time (the upscale uses the CPU hard). Same answer shape as parfumdeclaration. */
async function renderFragranticaCardHere(payload) {
  const renderer = loadFragranticaLocalRenderer();
  if (!renderer) return null;
  const run = fragranticaLocalRenderChain.then(async () => {
    const source = await fragranticaSourceBuffer(payload.imageUrl);
    const out = await renderer.renderPerfumeCard({ source, styles: payload.styles, perfume: payload.perfume, extras: payload.extras !== false });
    const b64 = (buf) => (buf ? Buffer.from(buf).toString("base64") : null);
    const map = (obj) => Object.fromEntries(Object.entries(obj || {}).map(([k, v]) => [k, b64(v)]));
    return {
      status: "done",
      main: b64(out.main),
      notesByStyle: map(out.notesByStyle),
      specsByStyle: map(out.specsByStyle),
      closeup: b64(out.closeup),
      tiersByStyle: Object.fromEntries(Object.entries(out.tiersByStyle || {}).map(([k, list]) => [k, list.map((t) => ({ tier: t.tier, image: b64(t.image) }))])),
      accordsByStyle: map(out.accordsByStyle),
      warnings: out.warnings || [],
      renderedBy: "davidsklad",
    };
  });
  fragranticaLocalRenderChain = run.catch(() => {});
  return run;
}

async function requestPerfumeCardPhotos(payload) {
  try {
    const here = await renderFragranticaCardHere(payload);
    if (here) return here;
  } catch (error) {
    logger.warn("fragrantica local render failed, using parfumdeclaration", { detail: error?.message || String(error) });
  }
  return requestPdPerfumeCard(payload);
}

async function requestPdPerfumeCard(payload) {
  if (!fragranticaPdToolsToken) throw new Error("PD_TOOLS_TOKEN не задан — обработка фото в parfumdeclaration недоступна.");
  const headers = { "Content-Type": "application/json", "x-tools-token": fragranticaPdToolsToken };
  const start = await fetch(`${fragranticaPdToolsUrl}/api/tools/perfume-card`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30_000),
  });
  const started = await start.json().catch(() => ({}));
  if (!start.ok || !started.jobId) throw new Error(started.error || `parfumdeclaration ответил ${start.status}`);
  const deadline = Date.now() + 6 * 60_000;
  while (Date.now() < deadline) {
    await sleep(2500);
    const poll = await fetch(`${fragranticaPdToolsUrl}/api/tools/perfume-card/${encodeURIComponent(started.jobId)}`, { headers, signal: AbortSignal.timeout(60_000) });
    const data = await poll.json().catch(() => ({}));
    if (!poll.ok) throw new Error(data.error || `parfumdeclaration ответил ${poll.status}`);
    if (data.status === "done") return data;
    if (data.status === "failed") throw new Error(data.error || "parfumdeclaration не смог обработать фото");
  }
  throw new Error("parfumdeclaration не успел обработать фото за 6 минут");
}

/** Absolute URLs of the extra card photos already made for a perfume: «Характеристики» (shop style), close-up. */
const FRAG_TIER_KEYS = ["top", "middle", "base", "flat"];

function fragranticaExtraPhotos(perfumeId, style) {
  const id = Number(perfumeId);
  // After the shop's «Пирамида аромата»: tiers with big icons, accords, «Характеристики», close-up (Ozon: 8+ photos)
  const files = [
    ...FRAG_TIER_KEYS.map((tier) => `${id}-tier-${tier}-${style}-v1.jpg`),
    `${id}-accords-${style}-v1.jpg`,
    `${id}-specs-${style}-v1.jpg`,
    `${id}-closeup-v1.jpg`,
  ];
  return files.filter((file) => fragFs.existsSync(fragranticaMediaPath("cards", file))).map((file) => fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", file)));
}

async function runFragranticaImageJob(job, options) {
  const result = await runFragranticaImageJobPhotos(job, options);
  // Видеообложка Ozon по стилю магазина: флакон → пирамида → характеристики → крупный план
  result.video = {};
  const local = (url) => (url && url.startsWith("/uploads/fragrantica/cards/") ? fragranticaMediaPath("cards", url.split("/").pop()) : "");
  for (const style of options.styles || []) {
    job.stage = "video";
    const slides = [local(result.main), local(result.notes?.[style]), local(result.specs?.[style]), local(result.closeup)].filter(Boolean);
    const url = await ensureFragranticaVideoCover(options.perfumeId, style, slides, { refresh: Boolean(options.refresh) }).catch(() => null);
    if (url) result.video[style] = url;
  }
  return result;
}

async function runFragranticaImageJobPhotos(job, { perfumeId, styles, refresh }) {
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  await ensureFragranticaPerfumeImage(perfumeId);
  const hdSource = await ensureFragranticaHdImage(perfumeId);
  // v2: 1500×2000, JPEG q95 без прореживания цвета, исходник — оригинал Фрагрантики (см. ensureFragranticaHdImage)
  const mainFile = `${perfumeId}-main-v2.jpg`;
  const notesFile = (style) => `${perfumeId}-notes-${style}-v2.jpg`;
  const have = (file) => fragFs.existsSync(fragranticaMediaPath("cards", file));
  // Extra photos (up to 6 per card): close-up of the real photo + «Характеристики»; «.none» = no close-up possible
  const closeupFile = `${perfumeId}-closeup-v1.jpg`;
  const closeupNone = `${perfumeId}-closeup-v1.none`;
  const specsFile = (style) => `${perfumeId}-specs-${style}-v1.jpg`;
  // «Ноты крупно» + «Аккорды» per style; the marker says they were made (a perfume may have no tiers/accords)
  const extras2Marker = (style) => `${perfumeId}-extras2-${style}.done`;
  const result = { main: null, notes: {}, specs: {}, closeup: null, source: hdSource || fragranticaMediaUrl("images", `${perfumeId}.jpg`), warnings: [] };
  const cached = () => {
    result.main = fragranticaMediaUrl("cards", mainFile);
    for (const style of styles) result.notes[style] = fragranticaMediaUrl("cards", notesFile(style));
    for (const style of styles) if (have(specsFile(style))) result.specs[style] = fragranticaMediaUrl("cards", specsFile(style));
    if (have(closeupFile)) result.closeup = fragranticaMediaUrl("cards", closeupFile);
    return result;
  };
  if (!refresh && have(mainFile) && styles.every((style) => have(notesFile(style)) && have(specsFile(style)) && have(extras2Marker(style))) && (have(closeupFile) || have(closeupNone))) {
    return cached();
  }
  job.stage = "parfumdeclaration";
  try {
    const brandInfo = await fragranticaBrandInfo(perfume.brandSlug).catch(() => ({ country: "" }));
    const pd = await requestPerfumeCardPhotos({
      imageUrl: fragranticaAbsoluteUrl(result.source),
      style: styles[0],
      styles,
      extras: true,
      perfume: {
        brand: perfume.brand,
        name: perfume.name,
        year: perfume.year || null,
        gender: perfume.gender || "",
        family: perfume.family || "",
        perfumers: Array.isArray(perfume.perfumers) ? perfume.perfumers.slice(0, 2) : [],
        country: fragranticaCountryRu(brandInfo.country) || "",
        notes: {
          top: (perfume.notes?.top || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          middle: (perfume.notes?.middle || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          base: (perfume.notes?.base || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          flat: (perfume.notes?.flat || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
        },
        accords: (perfume.accords || []).slice(0, 6),
      },
    });
    await fragFs.promises.mkdir(path.join(fragranticaMediaDir, "cards"), { recursive: true });
    if (pd.main) {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", mainFile), Buffer.from(pd.main, "base64"));
      result.main = fragranticaMediaUrl("cards", mainFile);
    }
    const byStyle = pd.notesByStyle || (pd.notes ? { [styles[0]]: pd.notes } : {});
    for (const style of styles) {
      if (!byStyle[style]) continue;
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", notesFile(style)), Buffer.from(byStyle[style], "base64"));
      result.notes[style] = fragranticaMediaUrl("cards", notesFile(style));
    }
    for (const style of styles) {
      for (const { tier, image } of pd.tiersByStyle?.[style] || []) {
        if (FRAG_TIER_KEYS.includes(tier)) await fragFs.promises.writeFile(fragranticaMediaPath("cards", `${perfumeId}-tier-${tier}-${style}-v1.jpg`), Buffer.from(image, "base64"));
      }
      if (pd.accordsByStyle?.[style]) await fragFs.promises.writeFile(fragranticaMediaPath("cards", `${perfumeId}-accords-${style}-v1.jpg`), Buffer.from(pd.accordsByStyle[style], "base64"));
      if (pd.tiersByStyle) await fragFs.promises.writeFile(fragranticaMediaPath("cards", extras2Marker(style)), "");
    }
    for (const style of styles) {
      const specs = pd.specsByStyle?.[style];
      if (!specs) continue;
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", specsFile(style)), Buffer.from(specs, "base64"));
      result.specs[style] = fragranticaMediaUrl("cards", specsFile(style));
    }
    if (pd.closeup) {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", closeupFile), Buffer.from(pd.closeup, "base64"));
      result.closeup = fragranticaMediaUrl("cards", closeupFile);
    } else {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", closeupNone), "");
    }
    result.warnings.push(...(pd.warnings || []));
  } catch (error) {
    logger.warn("fragrantica pd card failed", { perfumeId, detail: error?.message });
    result.warnings.push(`Фото не обработано: ${error?.message || error}. Можно создать карточку с исходным фото.`);
  }
  if (!result.main) result.main = result.source;
  return result;
}

app.post("/api/fragrantica/ozon/images", requireAdmin, async (request, response, next) => {
  try {
    const perfumeId = Number(request.body?.perfumeId);
    if (!perfumeId) return response.status(400).json({ error: "perfumeId required" });
    const requested = (Array.isArray(request.body?.styles) ? request.body.styles : []).filter((s) => FRAG_CARD_STYLES.includes(s));
    const styles = requested.length ? [...new Set(requested)] : [fragranticaCardStyleForAccount(fragranticaResolveOzonAccount(request.body?.accountId))];
    const jobId = `${perfumeId}-${styles.join("_")}-${Date.now().toString(36)}`;
    const job = { id: jobId, status: "running", stage: "download", result: null, error: null, at: Date.now() };
    fragranticaImageJobs.set(jobId, job);
    for (const [id, old] of fragranticaImageJobs) if (Date.now() - old.at > 3_600_000) fragranticaImageJobs.delete(id);
    runFragranticaImageJob(job, { perfumeId, styles, refresh: Boolean(request.body?.refresh) })
      .then((result) => Object.assign(job, { status: "done", result }))
      .catch((error) => Object.assign(job, { status: "failed", error: error?.message || String(error) }));
    response.json({ ok: true, jobId });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/images/:jobId", requireAdmin, (request, response) => {
  const job = fragranticaImageJobs.get(String(request.params.jobId));
  if (!job) return response.status(404).json({ error: "Задача не найдена (сервер перезапускался?) — запустите заново." });
  response.json({ ok: true, status: job.status, stage: job.stage, result: job.result, error: job.error });
});

// ─── Создание карточки и очередь ────────────────────────────────────────────

function fragranticaExportFromRow(row = {}) {
  return {
    id: Number(row.id),
    perfumeId: Number(row.perfume_id),
    marketplace: row.marketplace || "ozon",
    accountId: row.account_id,
    accountName: row.account_name,
    offerId: row.offer_id,
    volume: row.volume_ml === null || row.volume_ml === undefined ? null : Number(row.volume_ml),
    tester: Boolean(row.tester),
    status: row.status,
    productId: row.product_id ? Number(row.product_id) : null,
    result: row.result || null,
    links: Array.isArray(row.links) ? row.links : [],
    error: row.error || null,
    nextAttemptAt: row.next_attempt_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function readFragranticaExport(id) {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = $1`, Number(id));
  return rows[0] || null;
}

async function updateFragranticaExport(id, fields = {}) {
  const prisma = await requireFragranticaTables();
  const sets = [];
  const params = [Number(id)];
  for (const [column, value] of Object.entries(fields)) {
    params.push(value !== null && typeof value === "object" && !(value instanceof Date) ? JSON.stringify(value) : value);
    sets.push(`${column} = $${params.length}${["result", "item", "links"].includes(column) ? "::jsonb" : ""}`);
  }
  await prisma.$executeRawUnsafe(`UPDATE fragrantica_exports SET ${sets.join(", ")}, updated_at = now() WHERE id = $1`, ...params);
  return readFragranticaExport(id);
}

function fragranticaOzonErrorsText(errors = []) {
  return errors
    .map((e) => [e.attribute_name || e.field, e.description || e.message || e.code].filter(Boolean).join(": "))
    .filter(Boolean)
    .join("; ")
    .slice(0, 2000);
}

// Отправить карточку в Ozon или поставить в очередь лимита.
async function submitFragranticaExport(row) {
  const account = fragranticaResolveOzonAccount(row.account_id);
  const attempts = Number(row.attempts || 0) + 1;
  try {
    const quota = await ozonRequest("/v4/product/info/limit", {}, account).catch(() => null);
    const daily = quota?.daily_create;
    if (daily && Number(daily.limit) !== -1 && Number(daily.usage) >= Number(daily.limit)) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        attempts,
        error: `Дневной лимит создания карточек Ozon исчерпан (${daily.usage}/${daily.limit}) — отправим после сброса.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date(), daily.reset_at),
      });
    }
    const data = await ozonRequest("/v3/product/import", { items: [row.item] }, account);
    const taskId = data?.result?.task_id;
    if (!taskId) throw new Error("Ozon не вернул task_id");
    return updateFragranticaExport(row.id, { status: "pending", task_id: taskId, attempts, error: null, next_attempt_at: null });
  } catch (error) {
    const message = error?.ozon?.message || error?.message || String(error);
    if (isOzonLimitErrorText(message)) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        attempts,
        error: `Ozon: ${message} — отправим после сброса лимита.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date()),
      });
    }
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Ozon: ${message}`.slice(0, 2000) });
  }
}

// Карточка Маркета собирается теми же правилами, что перенос Ozon→Яндекс (категория по типу, бренд,
// параметры), из «синтетического» Ozon-товара с атрибутами формы и своими фото (Parfumerius).
function fragranticaSyntheticOzonProduct(row) {
  const item = row.item || {};
  const attr = (id) => cleanText((item.attributes || []).find((a) => Number(a.id) === id)?.values?.[0]?.value);
  return {
    id: `fragrantica-yandex-${row.offer_id}`,
    offerId: row.offer_id,
    marketplace: "ozon",
    // Not an Ozon cabinet on purpose: the Ozon→Market export would otherwise re-read the Ozon card with
    // the same offer id (enrichOzonProductsForYandexExport) and send its older dimensions/attributes.
    target: "fragrantica",
    name: item.yandexName || item.name,
    imageUrl: "",
    links: [],
    ozon: {
      name: item.yandexName || item.name,
      typeId: Number(item.type_id),
      attributes: item.attributes || [],
      description: attr(FRAG_OZON_ATTR.annotation),
      depth: item.depth,
      width: item.width,
      height: item.height,
      dimensionUnit: "mm",
      weight: item.weight,
      weightUnit: "g",
      images: [],
      primaryImage: "",
      barcodes: item.barcode ? [item.barcode] : [],
      barcode: item.barcode || "",
    },
    yandex: {
      pictures: item.yandexPictures || [],
      description: attr(FRAG_OZON_ATTR.annotation),
      price: Number(item.price) || undefined,
      extra: item.yandexExtra || {},
    },
  };
}

async function submitFragranticaYandexExport(row) {
  const attempts = Number(row.attempts || 0) + 1;
  const shop = fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(row.account_id));
  if (!shop) return updateFragranticaExport(row.id, { status: "failed", attempts, error: "Магазин Яндекс Маркета не найден в настройках." });
  try {
    const product = fragranticaSyntheticOzonProduct(row);
    const built = buildYandexOfferMapping(product);
    if (!built.ready) {
      const reason = built.categoryReview ? `категория: ${built.categoryReview}` : `не хватает: ${built.missing.join(", ")}`;
      return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${reason}` });
    }
    const result = await exportOzonProductsToYandex([product], [shop], { reason: "fragrantica_export" });
    if (result.sentOfferIds.has(cleanText(row.offer_id).toLowerCase())) {
      const vat = await sendFragranticaYandexVatPrice(shop, row.offer_id, row.item?.price);
      return updateFragranticaExport(row.id, {
        status: "imported",
        attempts,
        error: null,
        result: row.result?.mediaRefresh
          ? { ...(row.result || {}), market: "sent", ...vat, mediaRefresh: false, mediaRefreshedAt: new Date().toISOString() }
          : { ...(row.result || {}), market: "sent", ...vat, docs: "pending", links: Array.isArray(row.links) && row.links.length ? "pending" : "none" },
      });
    }
    const failure = (result.results || []).find((r) => !r.ok);
    const message = failure?.error || failure?.message || (failure?.errors || []).map((e) => e.message || e.type).join("; ") || "Маркет не принял карточку";
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${message}`.slice(0, 2000) });
  } catch (error) {
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${error?.message || error}`.slice(0, 2000) });
  }
}

const fragranticaYandexParamsCache = new Map();
async function fragranticaYandexCategoryParams(shop, categoryId) {
  const key = `${shop.businessId}:${categoryId}`;
  const cached = fragranticaYandexParamsCache.get(key);
  if (cached && Date.now() - cached.at < 12 * 3_600_000) return cached.params;
  const data = await yandexRequest(shop, "POST", `/v2/category/${Number(categoryId)}/parameters`, {});
  const params = data?.result?.parameters || [];
  fragranticaYandexParamsCache.set(key, { at: Date.now(), params });
  return params;
}

// Код НДС Маркета (VatType): 10 — 5% УСН. FRAGRANTICA_YANDEX_VAT переопределяет.
const fragranticaYandexVat = Number(process.env.FRAGRANTICA_YANDEX_VAT || 10) || 10;

async function sendFragranticaYandexVatPrice(shop, offerId, price) {
  if (!shop?.campaignId || !(Number(price) > 0)) return { vat: "skipped" };
  try {
    await yandexRequest(shop, "POST", `/v2/campaigns/${shop.campaignId}/offer-prices/updates`, {
      offers: [{ offerId, price: { value: Math.round(Number(price)), currencyId: "RUR", vat: fragranticaYandexVat } }],
    });
    return { vat: String(fragranticaYandexVat) };
  } catch (error) {
    const message = cleanText(error?.message);
    // «Partner use only default price; LOCKED»: the cabinet sells at one basic price — its VAT comes from
    // the Market cabinet settings, not from the API
    if (/LOCKED|default price/i.test(message)) return { vat: "cabinet" };
    return { vat: "failed", vatError: message.slice(0, 300) };
  }
}

function submitFragranticaTargetExport(row) {
  return row.marketplace === "yandex" ? submitFragranticaYandexExport(row) : submitFragranticaExport(row);
}

async function generateFragranticaBarcode(row, account) {
  try {
    const data = await ozonRequest("/v1/barcode/generate", { product_ids: [String(row.product_id)] }, account);
    const errors = data?.errors || [];
    if (errors.length) throw new Error(errors.map((e) => e.error || e.code).join("; "));
    return { barcode: "generated" };
  } catch (error) {
    const message = error?.ozon?.message || error?.message || String(error);
    return { barcode: "pending", barcodeError: message };
  }
}

// pending → спросить Ozon статус импорта; imported → штрихкод.
// ─── Декларация (+ GTIN Маркета) через parfumdeclaration ─────────────────────
// Карточка создана → parfumdeclaration привязывает декларацию по бренду (на Маркете ещё GTIN бренда из
// справочника, если у карточки нет своего). Бесплатно, только магазины владельца. Пока у карточки Ozon нет
// SKU — повтор каждые 2 мин (до 60 попыток).
async function attachFragranticaDocs(row) {
  const tries = Number(row.result?.docsTries || 0) + 1;
  if (!fragranticaPdToolsToken) return { docs: "none", docsInfo: "PD_TOOLS_TOKEN не задан" };
  const prisma = await requireFragranticaTables();
  const [perfume] = await prisma.$queryRawUnsafe(`SELECT brand, name FROM fragrantica_perfumes WHERE id = $1`, Number(row.perfume_id));
  const body = {
    marketplace: row.marketplace === "yandex" ? "yandex" : "ozon",
    offerId: row.offer_id,
    brand: cleanText(perfume?.brand),
    name: cleanText(row.item?.name) || `${cleanText(perfume?.brand)} ${cleanText(perfume?.name)}`.trim(),
    barcode: cleanText(row.item?.barcode || (Array.isArray(row.item?.barcodes) ? row.item.barcodes[0] : "")),
  };
  if (body.marketplace === "ozon") {
    body.clientId = cleanText(fragranticaResolveOzonAccount(row.account_id)?.clientId);
  } else {
    body.businessId = cleanText(fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(row.account_id))?.businessId);
  }
  try {
    const response = await fetch(`${fragranticaPdToolsUrl}/api/tools/attach-docs`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-tools-token": fragranticaPdToolsToken },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60_000),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `parfumdeclaration ответил ${response.status}`);
    if (data.status === "queued") {
      return { docs: "done", docsTries: tries, docsInfo: `Декларация ${data.declaration}${data.gtin ? `, GTIN ${data.gtin}` : ""}` };
    }
    if (data.status === "already") return { docs: "done", docsTries: tries, docsInfo: data.message };
    if (data.status === "not_ready" && tries < 60) return { docs: "pending", docsTries: tries, docsInfo: data.message };
    return { docs: "none", docsTries: tries, docsInfo: data.message || data.status };
  } catch (error) {
    const message = error?.message || String(error);
    return tries < 60 ? { docs: "pending", docsTries: tries, docsInfo: message } : { docs: "none", docsTries: tries, docsInfo: message };
  }
}

async function refreshFragranticaExport(row) {
  if (!row) return row;
  if (row.status === "imported" && row.result?.docs === "pending" && (row.marketplace === "yandex" || row.product_id)) {
    const docs = await attachFragranticaDocs(row);
    const updated = await updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...docs } });
    if (docs.docs === "pending") return updated;
    return refreshFragranticaExport(updated);
  }
  if (row.marketplace === "yandex") {
    if (row.status !== "imported" || row.result?.links !== "pending") return row;
    let linkResult;
    try {
      linkResult = await applyFragranticaExportLinks(row);
    } catch (error) {
      linkResult = { links: "pending", linksError: error?.message || String(error) };
    }
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...linkResult } });
  }
  const account = fragranticaResolveOzonAccount(row.account_id);
  if (row.status === "pending" && row.task_id) {
    const data = await ozonRequest("/v1/product/import/info", { task_id: Number(row.task_id) }, account);
    const item = (data?.result?.items || []).find((i) => i.offer_id === row.offer_id) || data?.result?.items?.[0];
    if (!item) return row;
    const errors = (item.errors || []).filter((e) => cleanText(e.level).toLowerCase() !== "warning");
    // «исчерпали суточный лимит на обновление товаров» — the card was not updated: resend after 03:00 MSK
    const limitText = (item.errors || []).map((e) => cleanText(e.description || e.message || e.code)).find((t) => isOzonLimitErrorText(t));
    if (limitText) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        product_id: item.product_id || row.product_id || null,
        error: `Ozon: ${limitText.slice(0, 300)} — отправим ещё раз после сброса лимита.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date()),
      });
    }
    if (item.status === "imported" || (item.product_id && !errors.length && item.status !== "failed")) {
      const updated = await updateFragranticaExport(row.id, {
        status: "imported",
        product_id: item.product_id || null,
        error: null,
        result: row.result?.mediaRefresh
          ? { ...(row.result || {}), warnings: fragranticaOzonErrorsText(item.errors || []) || null, mediaRefresh: false, mediaRefreshedAt: new Date().toISOString() }
          : {
            ...(row.result || {}),
            warnings: fragranticaOzonErrorsText(item.errors || []) || null,
            barcode: "pending",
            docs: "pending",
            links: Array.isArray(row.links) && row.links.length ? "pending" : "none",
          },
      });
      return refreshFragranticaExport(updated);
    }
    if ((item.status === "failed" || errors.length) && !row.result?.mediaRetried && fragranticaMediaExtrasFailed(errors)) {
      // Ozon refused the Rich-контент or the video cover: resend once without them, the card itself is fine
      const plainItem = { ...row.item, attributes: (row.item?.attributes || []).filter((a) => Number(a.id) !== FRAG_RICH_ATTR) };
      delete plainItem.complex_attributes;
      logger.warn("fragrantica export: Ozon refused rich content / video cover, resending without them", { id: Number(row.id), errors: fragranticaOzonErrorsText(errors) });
      const resent = await updateFragranticaExport(row.id, { item: plainItem, result: { ...(row.result || {}), mediaRetried: true, mediaError: fragranticaOzonErrorsText(errors) } });
      return submitFragranticaExport(resent);
    }
    if (item.status === "failed" || errors.length) {
      return updateFragranticaExport(row.id, {
        status: "failed",
        product_id: item.product_id || null,
        error: fragranticaOzonErrorsText(item.errors || []) || "Ozon отклонил карточку",
        result: { ...(row.result || {}), errors: item.errors || [] },
      });
    }
    return row;
  }
  if (row.status === "imported" && row.product_id && row.result?.links === "pending") {
    let linkResult;
    try {
      linkResult = await applyFragranticaExportLinks(row);
    } catch (error) {
      linkResult = { links: "pending", linksError: error?.message || String(error) };
      logger.warn("fragrantica export links failed", { id: Number(row.id), detail: linkResult.linksError });
    }
    const updated = await updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...linkResult } });
    if (linkResult.links === "pending") return updated;
    return refreshFragranticaExport(updated);
  }
  if (row.status === "imported" && row.product_id && row.result?.barcode === "pending" && row.item?.barcode) {
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), barcode: "manufacturer" } });
  }
  if (row.status === "imported" && row.product_id && row.result?.barcode === "pending") {
    const barcode = await generateFragranticaBarcode(row, account);
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...barcode } });
  }
  return row;
}

// body.targets: [{ key: "ozon:<id>" | "yandex:<id>", notes: "<url of this shop's notes picture>" | null }]
// (без targets — один кабинет Ozon из accountId, как раньше). Общие: фото флакона images[0], атрибуты,
// цена Ozon (price/oldPrice) и цена Маркета (yandexPrice). Артикул один на все магазины.
// Создать/обновить карточки в выбранных магазинах. request нужен для журнала (session.username).
async function createFragranticaExports(body = {}, request = { session: {} }) {
  const perfumeId = Number(body.perfumeId);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const allTargets = fragranticaTargets();
  const wanted = Array.isArray(body.targets) && body.targets.length
    ? body.targets.map((t) => ({ ...allTargets.find((x) => x.key === cleanText(t.key)), notes: cleanText(t.notes) || null })).filter((t) => t.key)
    : [{ ...(allTargets.find((x) => x.kind === "ozon" && x.id === cleanText(fragranticaResolveOzonAccount(body.accountId).id)) || {}), notes: null }].filter((t) => t.key);
  if (!wanted.length) throw fragranticaHttpError(400, "Выберите хотя бы один магазин.", { code: "fragrantica_export_no_targets" });

  const type = fragOzonTypeById(body.typeId) || fragOzonTypeByKey(body.typeKey);
  const attrsAccount = fragranticaResolveOzonAccount(wanted.find((t) => t.kind === "ozon")?.id || body.accountId);
  const categoryAttrs = await ozonGetCategoryAttributes(attrsAccount, FRAG_OZON_CATEGORY_ID, type.typeId);
  const bottle = (Array.isArray(body.images) ? body.images : []).map(fragranticaAbsoluteUrl).filter(Boolean)[0] || "";
  const { item: baseItem, missing } = buildFragranticaOzonItem({ ...body, typeId: type.typeId, images: bottle ? [bottle] : [] }, categoryAttrs);
  if (missing.length) {
    throw fragranticaHttpError(400, `Заполните: ${missing.join(", ")}`, { code: "fragrantica_export_missing", missing });
  }

  // Один артикул на все магазины (FR<id>-<мл>[T]). Повторная отправка того же аромата не плодит
  // дубли: карточка с этим артикулом обновляется (Ozon /v3/product/import и Маркет обновляют по offer_id).
  const prisma = await requireFragranticaTables();
  const offerId = baseItem.offer_id;
  for (const attr of baseItem.attributes) {
    if (attr.id === FRAG_OZON_ATTR.sellerCode) attr.values = [{ value: offerId }];
  }

  // Маркет: характеристики категории, ТН ВЭД + ОКПД2, срок годности — по фактам Фрагрантики
  let yandexExtra = null;
  if (wanted.some((t) => t.kind === "yandex")) {
    const shop = fragranticaYandexShops().find((x) => wanted.some((t) => t.kind === "yandex" && t.id === cleanText(x.id)));
    const volume = fragFormatVolume((baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume)?.values || [])[0]?.value);
    yandexExtra = await buildFragranticaYandexExtra({ perfume, type, name: baseItem.name, volume, tester: Boolean(body.tester), shop });
  }
  const vatOverrides = fragranticaEnvMap("FRAGRANTICA_VAT_BY_ACCOUNT");

  const volumeAttr = baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume);
  const links = JSON.stringify((Array.isArray(body.links) ? body.links : []).slice(0, 10).map(fragranticaLinkDraft));
  const yandexPrice = Math.round(Number(body.yandexPrice) || Number(baseItem.price) || 0);
  const results = [];
  for (const target of wanted) {
    const notes = target.notes ? fragranticaAbsoluteUrl(target.notes) : "";
    const ozonAttributes = baseItem.attributes.map((a) => (a.id === FRAG_OZON_ATTR.annotation
      ? { ...a, values: a.values.map((v) => ({ ...v, value: formatDescriptionForMarketplace(v.value, "ozon") })) }
      : a));
    const ozonAccount = target.kind === "ozon" ? getOzonAccounts().find((a) => cleanText(a.id) === target.id) : null;
    // After the notes: «Характеристики» in this shop's style and the close-up (when they were made)
    const extras = fragranticaExtraPhotos(perfumeId, target.style);
    // Ozon: Rich-контент из описания и фото магазина + видеообложка (если категория знает атрибут 11254)
    const ozonMedia = target.kind === "ozon" ? fragranticaOzonMediaExtras({ perfumeId, style: target.style, notes, baseItem, categoryAttrs, perfume }) : null;
    const item = target.kind === "ozon"
      ? {
        ...baseItem,
        vat: fragranticaVatForClientId(cleanText(ozonAccount?.clientId), vatOverrides),
        attributes: [...ozonAttributes, ...ozonMedia.attributes],
        ...(ozonMedia.complex.length ? { complex_attributes: ozonMedia.complex } : {}),
        primary_image: bottle,
        images: [notes, ...extras].filter(Boolean),
      }
      : {
        ...baseItem,
        // Маркет: «Парфюмерная вода <бренд> <аромат> <для кого> <мл> мл» — так карточка получает больше баллов
        yandexName: buildFragranticaMarketName({ perfume, typeKey: type.key, volume: (volumeAttr?.values || [])[0]?.value, tester: Boolean(body.tester) }),
        price: String(yandexPrice), yandexPictures: [bottle, notes, ...extras].filter(Boolean), yandexExtra,
      };
    // the same perfume+volume already sent to this shop → update that row (and that card), no duplicate
    const inserted = await prisma.$queryRawUnsafe(
      `INSERT INTO fragrantica_exports (perfume_id, marketplace, account_id, account_name, offer_id, volume_ml, tester, status, item, created_by, links)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'new', $8::jsonb, $9, $10::jsonb)
       ON CONFLICT (account_id, offer_id) DO UPDATE SET perfume_id = EXCLUDED.perfume_id, account_name = EXCLUDED.account_name,
         volume_ml = EXCLUDED.volume_ml, tester = EXCLUDED.tester, status = 'new', item = EXCLUDED.item, links = EXCLUDED.links,
         error = NULL, attempts = 0, next_attempt_at = NULL, task_id = NULL, updated_at = now()
       RETURNING *`,
      perfumeId,
      target.kind,
      target.id,
      target.label,
      offerId,
      Number(volumeAttr?.values?.[0]?.value) || null,
      Boolean(body.tester),
      JSON.stringify(item),
      cleanText(request.session?.username) || null,
      links,
    );
    results.push(await submitFragranticaTargetExport(inserted[0]));
  }
  await appendAudit(request, "fragrantica.export", {
    entityType: "fragrantica_export",
    entityId: results.map((r) => r.id).join(","),
    newValue: { perfumeId, brand: perfume.brand, name: perfume.name, offerId, shops: wanted.map((t) => t.label), statuses: results.map((r) => r.status) },
  });
  // the text the operator sent becomes the shared description of the perfume (other volumes reuse it)
  const sentDescription = String((baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value || "");
  if (sentDescription.trim()) await saveFragranticaCardDescription(perfumeId, sentDescription).catch(() => {});
  return { ok: true, offerId, exports: results.map(fragranticaExportFromRow), export: fragranticaExportFromRow(results[0]) };
}

async function buildFragranticaYandexExtra({ perfume, type, name, volume, tester, shop, style = "parfumerius" }) {
  const category = resolveYandexCategoryForOzonProduct({ typeId: type.typeId, name });
  const params = category.categoryId && shop ? await fragranticaYandexCategoryParams(shop, category.categoryId).catch(() => []) : [];
  const facts = fragranticaFactsFromDetail(perfume);
  const video = fragranticaVideoCoverUrl(perfume.id, style);
  return {
    commodityCodes: [{ code: fragranticaTnvedCode(type.typeId), type: "CUSTOMS_COMMODITY_CODE" }, { code: fragOkpd2ForType(type.key), type: "OKPD2_CODE" }],
    shelfLife: { timePeriod: Number(FRAG_OZON_DEFAULTS.shelfLifeDays), timeUnit: "DAY" },
    // Market rating: a video (+8 points) — the same cover as on Ozon, 1080×1440 MP4
    ...(video ? { videos: [video] } : {}),
    parameterValues: buildFragranticaYandexParameters(params, {
      typeLabel: type.nameLabel.toLowerCase(),
      gender: { male: "мужской", female: "женский", unisex: "унисекс" }[perfume.gender] || "",
      family: facts.family,
      accords: facts.accords,
      year: facts.year,
      topNotes: facts.topNotes,
      middleNotes: facts.middleNotes,
      baseNotes: facts.baseNotes,
      netWeight: fragOzonNetWeight(volume),
      tester,
      // filterable «Линейка» (the perfume itself) and «Особенности флакона» (spray bottle; oils have none)
      line: fragStripConcentration(perfume.name),
      bottleFeature: type.key === "oil" ? "" : "с распылителем",
    }),
  };
}

function fragranticaOzonMediaExtras({ perfumeId, style, notes, baseItem, categoryAttrs = [], perfume = {} }) {
  const attributes = [];
  const ownRich = baseItem.attributes.some((a) => Number(a.id) === FRAG_RICH_ATTR && (a.values || []).length);
  if (!ownRich && process.env.FRAGRANTICA_OZON_RICH !== "false" && categoryAttrs.some((a) => Number(a.id) === FRAG_RICH_ATTR)) {
    const description = (baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value || "";
    const file = (name) => (fragFs.existsSync(fragranticaMediaPath("cards", name)) ? fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", name)) : "");
    const rich = buildFragranticaRichContent({
      title: fragNameWithBrand(perfume),
      description,
      images: { notes, specs: file(`${Number(perfumeId)}-specs-${style}-v1.jpg`), closeup: file(`${Number(perfumeId)}-closeup-v1.jpg`) },
    });
    if (rich) attributes.push({ id: FRAG_RICH_ATTR, complex_id: 0, values: [{ value: rich }] });
  }
  const complex = process.env.FRAGRANTICA_OZON_VIDEO_COVER === "false" ? [] : buildFragranticaVideoCoverComplex(fragranticaVideoCoverUrl(perfumeId, style));
  return { attributes, complex };
}

function fragranticaHttpError(statusCode, message, detail = {}) {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.detail = detail;
  return error;
}

app.post("/api/fragrantica/ozon/export", requireAdmin, async (request, response, next) => {
  try {
    response.json(await createFragranticaExports(request.body || {}, request));
  } catch (error) {
    if (error?.detail?.code) return response.status(error.statusCode || 400).json({ error: error.message, ...error.detail });
    next(error);
  }
});

// ─── Дозаливка медиа в уже созданные карточки ────────────────────────────────
// Ozon: фото «Ноты крупно» + «Аккорды», Rich-контент, видеообложка. Маркет: те же фото, видео, название
// 60–120 знаков, «Линейка» и «Особенности флакона». Карточка обновляется по тому же offer_id.

const fragranticaMediaRefreshState = { running: false, total: 0, done: 0, failed: 0, startedAt: null, finishedAt: null, last: null };

async function refreshFragranticaExportMedia(row) {
  const perfumeId = Number(row.perfume_id);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const target = fragranticaTargets().find((t) => t.kind === row.marketplace && t.id === cleanText(row.account_id));
  if (!target) return { skipped: "магазин не найден" };
  await runFragranticaImageJob({}, { perfumeId, styles: [target.style], refresh: false });
  const media = (file) => (fragFs.existsSync(fragranticaMediaPath("cards", file)) ? fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", file)) : "");
  const notes = media(`${perfumeId}-notes-${target.style}-v2.jpg`);
  const extras = fragranticaExtraPhotos(perfumeId, target.style);
  const item = { ...(row.item || {}) };
  const type = fragOzonTypeById(item.type_id) || fragOzonTypeByKey(fragOzonGuessTypeKey(perfume));
  // A short annotation (Market docks 20 points below ~1000 chars): the perfume's shared AI text, written now if missing
  const annotation = (item.attributes || []).find((a) => Number(a.id) === FRAG_OZON_ATTR.annotation);
  const currentText = String(annotation?.values?.[0]?.value || "").replace(/<[^>]+>/g, "");
  if (currentText.length < 800) {
    let text = await readFragranticaCardDescription(perfumeId).catch(() => "");
    if (text.length < 800) text = (await generateFragranticaDescription({ perfumeId, typeKey: type.key, tester: Boolean(row.tester), marketplace: "yandex" }).catch(() => ({}))).description || text;
    if (text.length > currentText.length) {
      const value = row.marketplace === "ozon" ? formatDescriptionForMarketplace(text, "ozon") : text;
      item.attributes = [...(item.attributes || []).filter((a) => Number(a.id) !== FRAG_OZON_ATTR.annotation), { id: FRAG_OZON_ATTR.annotation, complex_id: 0, values: [{ value }] }];
    }
  }
  if (row.marketplace === "ozon") {
    const account = fragranticaResolveOzonAccount(row.account_id);
    const categoryAttrs = await ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId).catch(() => []);
    const attributes = (item.attributes || []).filter((a) => Number(a.id) !== FRAG_RICH_ATTR);
    const extra = fragranticaOzonMediaExtras({ perfumeId, style: target.style, notes, baseItem: { attributes }, categoryAttrs, perfume });
    // parfumdeclaration appended its «фото в конце» after our photos (Ozon re-hosts them): keep that tail
    const info = getOzonOfferMapValue(await getOzonProductInfoMap([row.offer_id], account, { continueOnError: true }), row.offer_id) || {};
    const current = [...[].concat(info.primary_image || []), ...(Array.isArray(info.images) ? info.images : [])].filter(Boolean);
    const tail = current.slice(1 + (Array.isArray(row.item?.images) ? row.item.images.length : 0));
    item.images = [...new Set([notes, ...extras, ...tail].filter(Boolean))].slice(0, 29);
    item.attributes = [...attributes, ...extra.attributes];
    if (extra.complex.length) item.complex_attributes = extra.complex;
  } else {
    const shop = fragranticaYandexShops().find((x) => cleanText(x.id) === cleanText(row.account_id));
    const volume = row.volume_ml === null || row.volume_ml === undefined ? "" : fragFormatVolume(row.volume_ml);
    const bottle = (item.yandexPictures || [])[0] || "";
    // keep what parfumdeclaration appended after our pictures («фото в конце»)
    const mapping = shop ? (await getYandexOfferMappingsByOfferIds(shop, [row.offer_id]).catch(() => []))[0] : null;
    const current = Array.isArray(mapping?.offer?.pictures) ? mapping.offer.pictures.map(String) : [];
    const tail = current.slice((item.yandexPictures || []).length);
    item.yandexPictures = [...new Set([bottle, notes, ...extras, ...tail].filter(Boolean))].slice(0, 20);
    item.yandexExtra = { ...(item.yandexExtra || {}), ...(await buildFragranticaYandexExtra({ perfume, type, name: item.name, volume, tester: Boolean(row.tester), shop, style: target.style })) };
    item.yandexName = buildFragranticaMarketName({ perfume, typeKey: type.key, volume, tester: Boolean(row.tester) });
  }
  const updated = await updateFragranticaExport(row.id, { item, status: "new", error: null, attempts: 0, next_attempt_at: null, task_id: null, result: { ...(row.result || {}), mediaRefresh: true } });
  return submitFragranticaTargetExport(updated);
}

async function runFragranticaMediaRefresh(ids = []) {
  const prisma = await requireFragranticaTables();
  const rows = ids.length
    ? await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = ANY($1::bigint[]) AND status = 'imported' ORDER BY id`, ids)
    : await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE status = 'imported' ORDER BY id`);
  Object.assign(fragranticaMediaRefreshState, { running: true, total: rows.length, done: 0, failed: 0, startedAt: new Date().toISOString(), finishedAt: null, last: null });
  for (const row of rows) {
    try {
      const res = await refreshFragranticaExportMedia(row);
      fragranticaMediaRefreshState.last = `${row.offer_id} ${row.account_name}: ${res?.status || res?.skipped || "ok"}`;
      if (res?.status === "failed") fragranticaMediaRefreshState.failed += 1;
    } catch (error) {
      fragranticaMediaRefreshState.failed += 1;
      fragranticaMediaRefreshState.last = `${row.offer_id} ${row.account_name}: ${error?.message || error}`;
      logger.warn("fragrantica media refresh failed", { id: Number(row.id), detail: error?.message });
    }
    fragranticaMediaRefreshState.done += 1;
  }
  Object.assign(fragranticaMediaRefreshState, { running: false, finishedAt: new Date().toISOString() });
  logger.info("fragrantica media refresh done", { ...fragranticaMediaRefreshState });
}

app.post("/api/fragrantica/ozon/exports/refresh-media", requireAdmin, async (request, response, next) => {
  try {
    if (fragranticaMediaRefreshState.running) return response.json({ ok: true, started: false, state: fragranticaMediaRefreshState });
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter((id) => id > 0);
    void runFragranticaMediaRefresh(ids).catch((error) => logger.warn("fragrantica media refresh crashed", { detail: error?.message }));
    response.json({ ok: true, started: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/exports/refresh-media", requireAdmin, (_request, response) => {
  response.json({ ok: true, state: fragranticaMediaRefreshState });
});

app.get("/api/fragrantica/ozon/exports", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireFragranticaTables();
    const perfumeId = Number(request.query.perfumeId) || null;
    const rows = await prisma.$queryRawUnsafe(
      `SELECT * FROM fragrantica_exports ${perfumeId ? "WHERE perfume_id = $1" : ""} ORDER BY id DESC LIMIT 200`,
      ...(perfumeId ? [perfumeId] : []),
    );
    response.json({ ok: true, items: rows.map(fragranticaExportFromRow) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/exports/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    let row = await readFragranticaExport(request.params.id);
    if (!row) return response.status(404).json({ error: "Экспорт не найден." });
    row = await refreshFragranticaExport(row).catch((error) => {
      logger.warn("fragrantica export refresh failed", { id: Number(row.id), detail: error?.message });
      return row;
    });
    response.json({ ok: true, export: fragranticaExportFromRow(row) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/ozon/exports/:id/retry", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const row = await readFragranticaExport(request.params.id);
    if (!row) return response.status(404).json({ error: "Экспорт не найден." });
    if (!["failed", "queued_limit"].includes(row.status)) return response.status(409).json({ error: `Статус ${row.status} — повтор не нужен.` });
    const submitted = await submitFragranticaTargetExport(row);
    response.json({ ok: true, export: fragranticaExportFromRow(submitted) });
  } catch (error) {
    next(error);
  }
});

// Worker: очередь лимита (после 03:00 МСК) и досмотр pending / штрихкодов.
async function runFragranticaExportQueueTick() {
  const prisma = await requireFragranticaTables();
  const due = await prisma.$queryRawUnsafe(
    `SELECT * FROM fragrantica_exports
     WHERE (status = 'queued_limit' AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
        OR (status = 'pending' AND marketplace = 'ozon' AND updated_at < now() - interval '1 minute')
        OR (status = 'imported' AND result->>'links' = 'pending' AND updated_at < now() - interval '2 minutes')
        OR (status = 'imported' AND marketplace = 'ozon' AND result->>'barcode' = 'pending' AND updated_at < now() - interval '30 minutes')
        OR (status = 'imported' AND result->>'docs' = 'pending' AND updated_at < now() - interval '2 minutes')
     ORDER BY id LIMIT 50`,
  );
  let sent = 0;
  for (const row of due) {
    try {
      if (row.status === "queued_limit") {
        const updated = await submitFragranticaTargetExport(row);
        if (updated.status === "queued_limit") break; // лимит всё ещё исчерпан — остальные ждут
        sent += 1;
      } else {
        await refreshFragranticaExport(row);
      }
    } catch (error) {
      logger.warn("fragrantica export queue item failed", { id: Number(row.id), detail: error?.message });
    }
  }
  if (due.length) logger.info("fragrantica export queue tick", { due: due.length, sent });
}

function scheduleFragranticaExportQueue(delayMs = fragranticaExportQueueIntervalMs) {
  if (!fragranticaExportQueueEnabled) return;
  setTimeout(async () => {
    try {
      await runFragranticaExportQueueTick();
    } catch (error) {
      logger.warn("fragrantica export queue tick failed", { detail: error?.message });
    } finally {
      scheduleFragranticaExportQueue(fragranticaExportQueueIntervalMs);
    }
  }, Math.max(10_000, Number(delayMs) || fragranticaExportQueueIntervalMs)).unref?.();
}
