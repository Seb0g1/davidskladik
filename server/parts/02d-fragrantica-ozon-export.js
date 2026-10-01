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

const fragranticaPdToolsUrl = cleanText(process.env.PD_TOOLS_URL || "https://parfumdeclaration.ru").replace(/\/+$/, "");
const fragranticaPdToolsToken = cleanText(process.env.PD_TOOLS_TOKEN || "");
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

async function runFragranticaImageJob(job, { perfumeId, style, refresh }) {
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  await ensureFragranticaPerfumeImage(perfumeId);
  const mainFile = `${perfumeId}-main.jpg`;
  const notesFile = `${perfumeId}-notes-${style}.jpg`;
  const have = (file) => fragFs.existsSync(fragranticaMediaPath("cards", file));
  const result = { main: null, notes: null, source: fragranticaMediaUrl("images", `${perfumeId}.jpg`), warnings: [] };
  if (!refresh && have(mainFile) && have(notesFile)) {
    result.main = fragranticaMediaUrl("cards", mainFile);
    result.notes = fragranticaMediaUrl("cards", notesFile);
    return result;
  }
  job.stage = "parfumdeclaration";
  try {
    const pd = await requestPdPerfumeCard({
      imageUrl: fragranticaAbsoluteUrl(result.source),
      style,
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
    if (pd.notes) {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", notesFile), Buffer.from(pd.notes, "base64"));
      result.notes = fragranticaMediaUrl("cards", notesFile);
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
    const account = fragranticaResolveOzonAccount(request.body?.accountId);
    const style = fragranticaCardStyleForAccount(account);
    const jobId = `${perfumeId}-${style}-${Date.now().toString(36)}`;
    const job = { id: jobId, status: "running", stage: "download", result: null, error: null, at: Date.now() };
    fragranticaImageJobs.set(jobId, job);
    for (const [id, old] of fragranticaImageJobs) if (Date.now() - old.at > 3_600_000) fragranticaImageJobs.delete(id);
    runFragranticaImageJob(job, { perfumeId, style, refresh: Boolean(request.body?.refresh) })
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
    accountId: row.account_id,
    accountName: row.account_name,
    offerId: row.offer_id,
    volume: row.volume_ml === null || row.volume_ml === undefined ? null : Number(row.volume_ml),
    tester: Boolean(row.tester),
    status: row.status,
    productId: row.product_id ? Number(row.product_id) : null,
    result: row.result || null,
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
    sets.push(`${column} = $${params.length}${column === "result" || column === "item" ? "::jsonb" : ""}`);
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
        result: { ...(row.result || {}), warnings: fragranticaOzonErrorsText(item.errors || []) || null, barcode: "pending" },
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
  if (row.status === "imported" && row.product_id && row.result?.barcode === "pending") {
    const barcode = await generateFragranticaBarcode(row, account);
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...barcode } });
  }
  return row;
}

app.post("/api/fragrantica/ozon/export", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const perfumeId = Number(body.perfumeId);
    const perfume = await fragranticaPerfumeForExport(perfumeId);
    const account = fragranticaResolveOzonAccount(body.accountId);
    const type = fragOzonTypeById(body.typeId) || fragOzonTypeByKey(body.typeKey);
    const categoryAttrs = await ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId);
    const images = (Array.isArray(body.images) ? body.images : []).map(fragranticaAbsoluteUrl).filter(Boolean);
    const { item, missing } = buildFragranticaOzonItem({ ...body, typeId: type.typeId, images }, categoryAttrs);
    if (missing.length) {
      return response.status(400).json({ error: `Заполните: ${missing.join(", ")}`, code: "fragrantica_export_missing", missing });
    }

    // Уникальный артикул: свободный и в нашей истории, и в кабинете Ozon.
    const prisma = await requireFragranticaTables();
    const base = item.offer_id;
    const candidates = [base, ...Array.from({ length: 9 }, (_, i) => `${base}-${i + 2}`)];
    const takenRows = await prisma.$queryRawUnsafe(
      `SELECT offer_id FROM fragrantica_exports WHERE account_id = $1 AND offer_id = ANY($2::text[])`,
      cleanText(account.id),
      candidates,
    );
    const ozonExisting = await ozonRequest("/v3/product/info/list", { offer_id: candidates }, account).catch(() => ({ items: [] }));
    const taken = new Set([...takenRows.map((r) => r.offer_id), ...(ozonExisting.items || []).map((i) => i.offer_id)]);
    item.offer_id = fragranticaUniqueOfferId(base, taken);
    for (const attr of item.attributes) {
      if (attr.id === FRAG_OZON_ATTR.sellerCode) attr.values = [{ value: item.offer_id }];
    }

    const volumeAttr = item.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume);
    const inserted = await prisma.$queryRawUnsafe(
      `INSERT INTO fragrantica_exports (perfume_id, marketplace, account_id, account_name, offer_id, volume_ml, tester, status, item, created_by)
       VALUES ($1, 'ozon', $2, $3, $4, $5, $6, 'new', $7::jsonb, $8) RETURNING *`,
      perfumeId,
      cleanText(account.id),
      cleanText(account.name),
      item.offer_id,
      Number(volumeAttr?.values?.[0]?.value) || null,
      Boolean(body.tester),
      JSON.stringify(item),
      cleanText(request.session?.username) || null,
    );
    const submitted = await submitFragranticaExport(inserted[0]);
    await appendAudit(request, "fragrantica.ozon.export", {
      entityType: "fragrantica_export",
      entityId: String(submitted.id),
      newValue: { perfumeId, brand: perfume.brand, name: perfume.name, offerId: item.offer_id, account: account.name, status: submitted.status },
    });
    response.json({ ok: true, export: fragranticaExportFromRow(submitted) });
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
    const submitted = await submitFragranticaExport(row);
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
        OR (status = 'pending' AND updated_at < now() - interval '1 minute')
        OR (status = 'imported' AND result->>'barcode' = 'pending' AND updated_at < now() - interval '30 minutes')
     ORDER BY id LIMIT 50`,
  );
  let sent = 0;
  for (const row of due) {
    try {
      if (row.status === "queued_limit") {
        const updated = await submitFragranticaExport(row);
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
