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

async function fragranticaSearchDict(account, typeId, attributeId, value, limit = 20) {
  const query = cleanText(value);
  if (query.length < 2) return [];
  const data = await ozonRequest("/v1/description-category/attribute/values/search", {
    description_category_id: FRAG_OZON_CATEGORY_ID,
    type_id: Number(typeId),
    attribute_id: Number(attributeId),
    value: query,
    limit,
  }, account);
  return (data.result || []).map((v) => ({ id: Number(v.id), value: String(v.value || ""), info: v.info || "" }));
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

app.get("/api/fragrantica/ozon/form", requireAdmin, async (request, response, next) => {
  try {
    const perfumeId = Number(request.query.perfumeId);
    const perfume = await fragranticaPerfumeForExport(perfumeId);
    const account = fragranticaResolveOzonAccount(request.query.accountId);
    const typeKey = FRAG_OZON_TYPES.some((t) => t.key === request.query.typeKey) ? request.query.typeKey : fragOzonGuessTypeKey(perfume);
    const type = fragOzonTypeByKey(typeKey);
    const volume = fragFormatVolume(request.query.volume) || "";
    const tester = request.query.tester === "1" || request.query.tester === "true";

    const [categoryAttrs, brand, genderDict, classificationDict, tnved] = await Promise.all([
      ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId),
      fragranticaFindBrand(account, type.typeId, perfume.brand),
      fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.gender).catch(() => []),
      fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.classification).catch(() => []),
      fragranticaFindTnved(account, type.typeId, volume),
    ]);
    const offerId = buildFragranticaOfferId({ perfumeId, volume, tester });
    const prefill = buildFragranticaOzonPrefill({
      perfume,
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
      },
    });
    const values = Object.fromEntries(prefill.attributes.map((a) => [a.id, a.values]));
    const attributes = categoryAttrs
      .map(fragranticaAttributeForForm)
      .sort((a, b) => Number(b.required) - Number(a.required))
      .map((attr) => ({ ...attr, values: values[attr.id] || [] }));
    const prisma = await requireFragranticaTables();
    const exports = await prisma.$queryRawUnsafe(
      `SELECT id, account_name AS "accountName", offer_id AS "offerId", volume_ml AS "volume", tester, status, product_id AS "productId", error, created_at AS "createdAt"
       FROM fragrantica_exports WHERE perfume_id = $1 ORDER BY id DESC`,
      perfumeId,
    );
    response.json({
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
      dims: prefill.dims,
      attributes,
      brandMatched: Boolean(brand.match),
      brandCandidates: brand.candidates,
      sourceImage: fragranticaMediaUrl("images", `${perfumeId}.jpg`),
      exports: exports.map((e) => ({ ...e, id: Number(e.id), productId: e.productId ? Number(e.productId) : null, volume: e.volume === null ? null : Number(e.volume) })),
    });
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

async function runFragranticaImageJob(job, { perfumeId, styles, refresh }) {
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  await ensureFragranticaPerfumeImage(perfumeId);
  const mainFile = `${perfumeId}-main.jpg`;
  const notesFile = (style) => `${perfumeId}-notes-${style}.jpg`;
  const have = (file) => fragFs.existsSync(fragranticaMediaPath("cards", file));
  const result = { main: null, notes: {}, source: fragranticaMediaUrl("images", `${perfumeId}.jpg`), warnings: [] };
  if (!refresh && have(mainFile) && styles.every((style) => have(notesFile(style)))) {
    result.main = fragranticaMediaUrl("cards", mainFile);
    for (const style of styles) result.notes[style] = fragranticaMediaUrl("cards", notesFile(style));
    return result;
  }
  job.stage = "parfumdeclaration";
  try {
    const pd = await requestPdPerfumeCard({
      imageUrl: fragranticaAbsoluteUrl(result.source),
      style: styles[0],
      styles,
      perfume: {
        brand: perfume.brand,
        name: perfume.name,
        year: perfume.year || null,
        gender: perfume.gender || "",
        family: perfume.family || "",
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
  const ozonAccount = getOzonAccounts().find((a) => a.importEnabled !== false) || getOzonAccounts()[0] || {};
  return {
    id: `fragrantica-yandex-${row.offer_id}`,
    offerId: row.offer_id,
    marketplace: "ozon",
    target: cleanText(ozonAccount.id) || "ozon",
    name: item.name,
    imageUrl: "",
    links: [],
    ozon: {
      name: item.name,
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
      barcodes: [],
    },
    yandex: {
      pictures: item.yandexPictures || [],
      description: attr(FRAG_OZON_ATTR.annotation),
      price: Number(item.price) || undefined,
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
      return updateFragranticaExport(row.id, {
        status: "imported",
        attempts,
        error: null,
        result: { ...(row.result || {}), market: "sent", links: Array.isArray(row.links) && row.links.length ? "pending" : "none" },
      });
    }
    const failure = (result.results || []).find((r) => !r.ok);
    const message = failure?.error || failure?.message || (failure?.errors || []).map((e) => e.message || e.type).join("; ") || "Маркет не принял карточку";
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${message}`.slice(0, 2000) });
  } catch (error) {
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${error?.message || error}`.slice(0, 2000) });
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
async function refreshFragranticaExport(row) {
  if (!row) return row;
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
    if (item.status === "imported" || (item.product_id && !errors.length && item.status !== "failed" && item.status !== "pending")) {
      const updated = await updateFragranticaExport(row.id, {
        status: "imported",
        product_id: item.product_id || null,
        error: null,
        result: {
          ...(row.result || {}),
          warnings: fragranticaOzonErrorsText(item.errors || []) || null,
          barcode: "pending",
          links: Array.isArray(row.links) && row.links.length ? "pending" : "none",
        },
      });
      return refreshFragranticaExport(updated);
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
      logger.warn("fragrantica export links failed", { id: row.id, detail: linkResult.linksError });
    }
    const updated = await updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...linkResult } });
    if (linkResult.links === "pending") return updated;
    return refreshFragranticaExport(updated);
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
app.post("/api/fragrantica/ozon/export", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const perfumeId = Number(body.perfumeId);
    const perfume = await fragranticaPerfumeForExport(perfumeId);
    const allTargets = fragranticaTargets();
    const wanted = Array.isArray(body.targets) && body.targets.length
      ? body.targets.map((t) => ({ ...allTargets.find((x) => x.key === cleanText(t.key)), notes: cleanText(t.notes) || null })).filter((t) => t.key)
      : [{ ...(allTargets.find((x) => x.kind === "ozon" && x.id === cleanText(fragranticaResolveOzonAccount(body.accountId).id)) || {}), notes: null }].filter((t) => t.key);
    if (!wanted.length) return response.status(400).json({ error: "Выберите хотя бы один магазин.", code: "fragrantica_export_no_targets" });

    const type = fragOzonTypeById(body.typeId) || fragOzonTypeByKey(body.typeKey);
    const attrsAccount = fragranticaResolveOzonAccount(wanted.find((t) => t.kind === "ozon")?.id || body.accountId);
    const categoryAttrs = await ozonGetCategoryAttributes(attrsAccount, FRAG_OZON_CATEGORY_ID, type.typeId);
    const bottle = (Array.isArray(body.images) ? body.images : []).map(fragranticaAbsoluteUrl).filter(Boolean)[0] || "";
    const { item: baseItem, missing } = buildFragranticaOzonItem({ ...body, typeId: type.typeId, images: bottle ? [bottle] : [] }, categoryAttrs);
    if (missing.length) {
      return response.status(400).json({ error: `Заполните: ${missing.join(", ")}`, code: "fragrantica_export_missing", missing });
    }

    // Один свободный артикул для всех выбранных магазинов (наша история, кабинеты Ozon, склад Маркета).
    const prisma = await requireFragranticaTables();
    const base = baseItem.offer_id;
    const candidates = [base, ...Array.from({ length: 9 }, (_, i) => `${base}-${i + 2}`)];
    const ourRows = await prisma.$queryRawUnsafe(
      `SELECT offer_id, status FROM fragrantica_exports WHERE offer_id = ANY($1::text[])`,
      candidates,
    );
    // A failed attempt keeps its offer id: sending again updates that (possibly half-created) card.
    const retryable = new Set(ourRows.filter((r) => r.status === "failed").map((r) => r.offer_id));
    const taken = new Set(ourRows.map((r) => r.offer_id));
    for (const target of wanted.filter((t) => t.kind === "ozon")) {
      const existing = await ozonRequest("/v3/product/info/list", { offer_id: candidates }, fragranticaResolveOzonAccount(target.id)).catch(() => ({ items: [] }));
      for (const it of existing.items || []) if (!retryable.has(it.offer_id)) taken.add(it.offer_id);
    }
    if (wanted.some((t) => t.kind === "yandex")) {
      const yandexRows = await prisma.warehouseProduct.findMany({ where: { marketplace: "yandex", offerId: { in: candidates } }, select: { offerId: true } }).catch(() => []);
      for (const r of yandexRows) if (!retryable.has(r.offerId)) taken.add(r.offerId);
    }
    for (const offer of retryable) if (!ourRows.some((r) => r.offer_id === offer && r.status !== "failed")) taken.delete(offer);
    const offerId = fragranticaUniqueOfferId(base, taken);
    if (retryable.has(offerId)) {
      await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_exports WHERE offer_id = $1 AND status = 'failed'`, offerId);
    }
    for (const attr of baseItem.attributes) {
      if (attr.id === FRAG_OZON_ATTR.sellerCode) attr.values = [{ value: offerId }];
    }
    baseItem.offer_id = offerId;

    const volumeAttr = baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume);
    const links = JSON.stringify((Array.isArray(body.links) ? body.links : []).slice(0, 10).map(fragranticaLinkDraft));
    const yandexPrice = Math.round(Number(body.yandexPrice) || Number(baseItem.price) || 0);
    const results = [];
    for (const target of wanted) {
      const notes = target.notes ? fragranticaAbsoluteUrl(target.notes) : "";
      const item = target.kind === "ozon"
        ? { ...baseItem, primary_image: bottle, images: notes ? [notes] : [] }
        : { ...baseItem, price: String(yandexPrice), yandexPictures: [bottle, notes].filter(Boolean) };
      const inserted = await prisma.$queryRawUnsafe(
        `INSERT INTO fragrantica_exports (perfume_id, marketplace, account_id, account_name, offer_id, volume_ml, tester, status, item, created_by, links)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'new', $8::jsonb, $9, $10::jsonb) RETURNING *`,
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
    response.json({ ok: true, offerId, exports: results.map(fragranticaExportFromRow), export: fragranticaExportFromRow(results[0]) });
  } catch (error) {
    next(error);
  }
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
      logger.warn("fragrantica export refresh failed", { id: row.id, detail: error?.message });
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
      logger.warn("fragrantica export queue item failed", { id: row.id, detail: error?.message });
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
