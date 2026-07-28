// Массовое обновление атрибутов Ozon и Yandex:
// • Снять "Нужен код маркировки" (Честный знак) с всех товаров Ozon
// • Заполнить код ТН ВЭД на всех товарах Ozon и Yandex Market

const ozonCatAttribCache = new Map();
const ozonCatAttribCacheTtlMs = 30 * 60 * 1000;

async function ozonGetCategoryAttributes(account, descCatId, descTypeId) {
  const key = `${account?.id || "ozon"}:${descCatId}:${descTypeId}`;
  const cached = ozonCatAttribCache.get(key);
  if (cached && Date.now() - cached.at < ozonCatAttribCacheTtlMs) return cached.attrs;
  const data = await ozonRequest("/v1/description-category/attribute", {
    description_category_id: Number(descCatId),
    description_type_id: Number(descTypeId || 0),
    language: "DEFAULT",
  }, account);
  const attrs = data.result || [];
  ozonCatAttribCache.set(key, { at: Date.now(), attrs });
  return attrs;
}

async function ozonGetAttributeDictValues(account, descCatId, descTypeId, attributeId) {
  const all = [];
  let lastValueId = 0;
  for (let page = 0; page < 20; page += 1) {
    const data = await ozonRequest("/v1/description-category/attribute/values", {
      description_category_id: Number(descCatId),
      description_type_id: Number(descTypeId || 0),
      attribute_id: Number(attributeId),
      language: "DEFAULT",
      last_value_id: lastValueId,
      limit: 100,
    }, account);
    const items = data.result || [];
    all.push(...items);
    if (items.length < 100) break;
    lastValueId = items[items.length - 1]?.id || 0;
    if (!lastValueId) break;
  }
  return all;
}

// Единый проход по всем Ozon-товарам: offer_id + description_category_id + type_id
async function ozonGetAllProductsInfo(account, options = {}) {
  const offerIds = [];
  let lastId = "";
  while (true) {
    const data = await ozonRequest("/v3/product/list", {
      filter: { visibility: "ALL" },
      last_id: lastId,
      limit: 1000,
    }, account);
    const batch = data.result?.items || [];
    offerIds.push(...batch.map((item) => cleanText(item.offer_id)).filter(Boolean));
    lastId = data.result?.last_id || "";
    if (batch.length < 1000 || !lastId) break;
    if (options.limitIds && offerIds.length >= options.limitIds) break;
  }

  const products = [];
  for (const chunk of chunkArray(offerIds, 100)) {
    const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk }, account);
    for (const item of data.items || data.result?.items || []) {
      const offerId = cleanText(item.offer_id || item.offerId);
      const descCatId = Number(item.description_category_id || 0);
      const typeId = Number(item.type_id || 0);
      if (offerId) products.push({ offerId, descCatId, typeId });
    }
  }
  return products;
}

function ozonAttrNameMatches(name, patterns) {
  const normalized = cleanText(name || "").toLowerCase().replace(/[\s.,]+/g, "");
  return patterns.some((p) => normalized.includes(p));
}

// ─── Снятие "Нужен код маркировки" ────────────────────────────────────────────

async function ozonClearMarkingRequirement(account, { dryRun = true, limit = 50000 } = {}) {
  const products = (await ozonGetAllProductsInfo(account)).slice(0, limit);
  if (!products.length) return { ok: true, dryRun, total: 0, updated: 0, reason: "no_products" };

  // Per-category: find attribute + "Нет" dict value
  const categoryMeta = new Map();
  const uniqueCategories = [...new Map(products.map((p) => [`${p.descCatId}:${p.typeId}`, p])).values()];

  for (const { descCatId, typeId } of uniqueCategories) {
    if (!descCatId) continue;
    const catKey = `${descCatId}:${typeId}`;
    const attrs = await ozonGetCategoryAttributes(account, descCatId, typeId);
    const markAttr = attrs.find((a) => ozonAttrNameMatches(a.name, [
      "маркировк", "kiz", "киз", "честный", "cheznyi", "нуженкодмаркировки",
    ]));
    if (!markAttr) { categoryMeta.set(catKey, null); continue; }

    let noValue = null;
    if (Number(markAttr.dictionary_id) > 0) {
      const dictValues = await ozonGetAttributeDictValues(account, descCatId, typeId, markAttr.id);
      const noEntry = dictValues.find((v) =>
        ["нет", "false", "0", "ненужен", "безмаркировки", "netrebyet", "нетребует"].includes(
          cleanText(v.value || "").toLowerCase().replace(/[\s.,]+/g, ""),
        ),
      );
      if (noEntry) noValue = { value: cleanText(noEntry.value), dictionary_value_id: Number(noEntry.id) };
    } else {
      noValue = { value: "Нет" };
    }
    categoryMeta.set(catKey, { attributeId: Number(markAttr.id), attrName: cleanText(markAttr.name), noValue });
  }

  const updateItems = [];
  for (const { offerId, descCatId, typeId } of products) {
    const meta = categoryMeta.get(`${descCatId}:${typeId}`);
    if (!meta?.noValue) continue;
    const attrValue = meta.noValue.dictionary_value_id
      ? { value: meta.noValue.value, dictionary_value_id: meta.noValue.dictionary_value_id }
      : { value: meta.noValue.value };
    updateItems.push({ offer_id: offerId, attributes: [{ id: meta.attributeId, values: [attrValue] }] });
  }

  const categoriesSummary = [...categoryMeta.entries()].map(([k, v]) => ({ key: k, attr: v?.attrName || null, found: Boolean(v?.noValue) }));

  if (!updateItems.length) {
    return { ok: true, dryRun, total: products.length, updated: 0, reason: "no_matching_attr", categories: categoriesSummary };
  }
  if (dryRun) {
    return { ok: true, dryRun, total: products.length, candidates: updateItems.length, sample: updateItems.slice(0, 5), categories: categoriesSummary };
  }

  let updated = 0;
  const errors = [];
  for (const chunk of chunkArray(updateItems, 100)) {
    try {
      await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
      updated += chunk.length;
    } catch (error) {
      errors.push({ count: chunk.length, error: cleanText(error?.message || String(error)).slice(0, 200) });
    }
  }
  logger.info("ozon clear marking requirement", { updated, total: products.length, errors: errors.length });
  return { ok: errors.length === 0, dryRun, total: products.length, updated, errors: errors.slice(0, 10), categories: categoriesSummary };
}

// ─── Заполнение ТН ВЭД на Ozon ───────────────────────────────────────────────

async function ozonBackfillTnved(account, tnvedCode, { dryRun = true, limit = 50000 } = {}) {
  const code = cleanText(tnvedCode);
  if (!code) return { ok: false, error: "tnved_code_required" };

  const products = (await ozonGetAllProductsInfo(account)).slice(0, limit);
  if (!products.length) return { ok: true, dryRun, total: 0, updated: 0, reason: "no_products" };

  const categoryMeta = new Map();
  const uniqueCategories = [...new Map(products.map((p) => [`${p.descCatId}:${p.typeId}`, p])).values()];

  for (const { descCatId, typeId } of uniqueCategories) {
    if (!descCatId) continue;
    const catKey = `${descCatId}:${typeId}`;
    const attrs = await ozonGetCategoryAttributes(account, descCatId, typeId);
    const tnvedAttr = attrs.find((a) => ozonAttrNameMatches(a.name, [
      "тнвэд", "tnved", "тнвэд", "кодтн", "tarifcode", "тарифный",
    ]));
    categoryMeta.set(catKey, tnvedAttr ? { attributeId: Number(tnvedAttr.id), attrName: cleanText(tnvedAttr.name) } : null);
  }

  const updateItems = [];
  for (const { offerId, descCatId, typeId } of products) {
    const meta = categoryMeta.get(`${descCatId}:${typeId}`);
    if (!meta) continue;
    updateItems.push({ offer_id: offerId, attributes: [{ id: meta.attributeId, values: [{ value: code }] }] });
  }

  const categoriesSummary = [...categoryMeta.entries()].map(([k, v]) => ({ key: k, attr: v?.attrName || null, found: Boolean(v) }));

  if (!updateItems.length) {
    return { ok: true, dryRun, total: products.length, updated: 0, reason: "no_tnved_attr_found", categories: categoriesSummary };
  }
  if (dryRun) {
    return { ok: true, dryRun, total: products.length, candidates: updateItems.length, sample: updateItems.slice(0, 5), categories: categoriesSummary };
  }

  let updated = 0;
  const errors = [];
  for (const chunk of chunkArray(updateItems, 100)) {
    try {
      await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
      updated += chunk.length;
    } catch (error) {
      errors.push({ count: chunk.length, error: cleanText(error?.message || String(error)).slice(0, 200) });
    }
  }
  logger.info("ozon backfill tnved", { code, updated, total: products.length, errors: errors.length });
  return { ok: errors.length === 0, dryRun, total: products.length, updated, tnvedCode: code, errors: errors.slice(0, 10), categories: categoriesSummary };
}

// ─── Заполнение ТН ВЭД на Yandex Market ──────────────────────────────────────

async function yandexBackfillTnved(tnvedCode, { dryRun = true, limit = 50000 } = {}) {
  const code = cleanText(tnvedCode);
  if (!code) return { ok: false, error: "tnved_code_required" };

  const prisma = getPrisma();
  if (!prisma) return { ok: false, error: "postgres_unavailable" };

  // Collect all Yandex offer IDs from postgres
  const offerIds = [];
  let cursorId = null;
  while (offerIds.length < limit) {
    const page = await prisma.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      select: { id: true, offerId: true, target: true },
      orderBy: { id: "asc" },
      take: 1000,
      ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
    });
    if (!page.length) break;
    cursorId = page[page.length - 1].id;
    for (const row of page) {
      if (row.offerId) offerIds.push({ offerId: cleanText(row.offerId), target: cleanText(row.target || "") });
      if (offerIds.length >= limit) break;
    }
    if (page.length < 1000) break;
  }

  if (!offerIds.length) return { ok: true, dryRun, total: 0, updated: 0, reason: "no_yandex_products" };

  if (dryRun) {
    return { ok: true, dryRun, total: offerIds.length, candidates: offerIds.length, tnvedCode: code, sample: offerIds.slice(0, 5).map((r) => ({ offerId: r.offerId, customsTariffCode: code })) };
  }

  const shops = uniqueYandexShopsByBusiness ? uniqueYandexShopsByBusiness() : getYandexShops().filter((s) => s.apiKey && s.businessId);
  if (!shops.length) return { ok: false, error: "yandex_not_configured" };

  // Build minimal offers — only offerId + customsTariffCode
  const offers = offerIds.map((r) => ({ offerId: r.offerId, customsTariffCode: code }));
  const results = [];
  for (const shop of shops) {
    const shopResults = await sendYandexOfferMappings(shop, offers);
    results.push(...shopResults);
  }

  const ok = results.every((r) => r.ok);
  const failed = results.filter((r) => !r.ok).length;
  logger.info("yandex backfill tnved", { code, total: offers.length, failed, shops: shops.map((s) => s.id) });
  return { ok, dryRun, total: offers.length, updated: results.filter((r) => r.ok).length, tnvedCode: code, failed, errors: results.filter((r) => !r.ok).slice(0, 10) };
}

// ─── ТН ВЭД по категориям ────────────────────────────────────────────────────

const ozonTnvedAssignmentsPath = path.join(dataDir, "ozon-tnved-assignments.json");
let ozonTnvedApplyProgress = {
  running: false,
  phase: null,
  totalProducts: null,
  processed: null,
  updated: null,
  errors: null,
  startedAt: null,
  completedAt: null,
  categoryStats: null,
};

async function readOzonTnvedAssignments() {
  try {
    const raw = await fs.readFile(ozonTnvedAssignmentsPath, "utf8");
    const data = JSON.parse(raw);
    return Array.isArray(data.assignments) ? data.assignments : [];
  } catch {
    return [];
  }
}

async function writeOzonTnvedAssignments(assignments) {
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(
    ozonTnvedAssignmentsPath,
    JSON.stringify({ updatedAt: new Date().toISOString(), assignments }, null, 2),
  );
}

// Разворачиваем дерево категорий Ozon в плоский Map { descCatId → name }.
async function ozonFlatCategoryMap(account) {
  try {
    const data = await ozonRequest("/v1/description-category/tree", { language: "DEFAULT" }, account);
    const map = new Map();
    function walk(nodes) {
      for (const node of Array.isArray(nodes) ? nodes : []) {
        const id = Number(node.description_category_id);
        if (id) map.set(id, cleanText(node.category_name || node.name || ""));
        if (Array.isArray(node.children)) walk(node.children);
        if (Array.isArray(node.types)) {
          for (const type of node.types) {
            const typeKey = `${id}:${Number(type.type_id || 0)}`;
            map.set(typeKey, cleanText(type.type_name || type.name || ""));
          }
        }
      }
    }
    walk(data.result || []);
    return map;
  } catch (error) {
    logger.warn("ozon category tree failed", { detail: error?.message || String(error) });
    return new Map();
  }
}

// Кэш категорий: { data, expiresAt } — 10 минут, per account
const _categoryBreakdownCache = new Map();

// Группировка по descCatId:typeId — один ряд на каждую комбинацию категория+тип.
async function ozonGetCategoryBreakdown(account) {
  const cacheKey = String(account?.clientId || "default");
  const cached = _categoryBreakdownCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.data;

  const products = await ozonGetAllProductsInfo(account);
  const byKey = new Map();
  for (const { offerId, descCatId, typeId } of products) {
    const key = `${descCatId}:${typeId}`;
    const entry = byKey.get(key) || { descCatId, typeId, count: 0, offerIds: [] };
    entry.count += 1;
    if (entry.offerIds.length < 3) entry.offerIds.push(offerId);
    byKey.set(key, entry);
  }
  const data = { categories: [...byKey.values()], totalProducts: products.length };
  _categoryBreakdownCache.set(cacheKey, { data, expiresAt: Date.now() + 10 * 60 * 1000 });
  return data;
}

// Загружает словарь ТН ВЭД (атрибут attrId) из нескольких категорий Ozon.
// Возвращает Map: числовой код (10 цифр) → { value: полный текст из словаря, dictionary_value_id: id }
// Это нужно, чтобы отправлять не голый "3304990000", а точное значение из словаря Ozon
// и избегать сообщений "Заменили некорректное значение".
async function ozonFetchTnvedDictMap(account, uniqueCategories, attrId) {
  const map = new Map();
  for (const { descCatId, typeId } of uniqueCategories.slice(0, 20)) {
    if (!descCatId) continue;
    try {
      const dictValues = await ozonGetAttributeDictValues(account, descCatId, typeId || 0, attrId);
      for (const entry of dictValues) {
        const text = cleanText(entry.value || "");
        const m = text.match(/^(\d{10})/);
        if (m && !map.has(m[1])) {
          map.set(m[1], { value: text, dictionary_value_id: Number(entry.id) || 0 });
        }
      }
    } catch { /* категория может не иметь этот атрибут */ }
    if (map.size >= 500) break;
  }
  return map;
}

// Применяет назначения { descCatId, tnvedCode } ко всем товарам Ozon (группировка по descCatId).
async function ozonApplyTnvedByCategory(account, assignments, { dryRun = false, fallbackCode = "" } = {}) {
  // Ozon официально подтвердил: атрибут «Код ТН ВЭД ЕАЭС» имеет id=22232 для всех категорий.
  const TNVED_ATTR_ID = 22232;

  // assignMap: "descCatId:typeId" → tnvedCode (typeId=0 означает «все типы в категории»)
  const assignMap = new Map(
    assignments
      .filter((a) => cleanText(a.tnvedCode))
      .map((a) => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)]),
  );
  if (!assignMap.size && !fallbackCode) return { ok: true, dryRun, updated: 0, reason: "no_assignments" };

  ozonTnvedApplyProgress = {
    running: true, phase: "loading_products",
    totalProducts: null, processed: 0, updated: 0, errors: 0,
    startedAt: new Date().toISOString(), completedAt: null, categoryStats: null,
  };

  try {
    const products = await ozonGetAllProductsInfo(account);
    ozonTnvedApplyProgress.totalProducts = products.length;
    ozonTnvedApplyProgress.phase = "loading_dict";

    // Загружаем словарь Ozon для ТН ВЭД — получаем полные тексты вида
    // "3304990000 - Прочие косметические средства..." вместо голого кода.
    const uniqueCategories = [...new Map(products.map((p) => [`${p.descCatId}:${p.typeId}`, p])).values()];
    const tnvedDictMap = await ozonFetchTnvedDictMap(account, uniqueCategories, TNVED_ATTR_ID);
    logger.info("ozon tnved dict map built", { entries: tnvedDictMap.size, categoriesSampled: Math.min(uniqueCategories.length, 20) });

    ozonTnvedApplyProgress.phase = "sending";

    // Build update items — code priority: descCatId:typeId → descCatId:0 → fallbackCode
    const updateItems = [];
    for (const { offerId, descCatId, typeId } of products) {
      const code = assignMap.get(`${descCatId}:${typeId}`) || assignMap.get(`${descCatId}:0`) || cleanText(fallbackCode);
      if (!code) continue;
      // Используем полный текст из словаря Ozon (если нашли), иначе только числовой код
      const numericCode = code.match(/^(\d{10})/)?.[1] || code;
      const dictEntry = tnvedDictMap.get(numericCode);
      const values = dictEntry
        ? [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }]
        : [{ value: code }];
      updateItems.push({ offer_id: offerId, attributes: [{ id: TNVED_ATTR_ID, values }] });
    }

    const categoryStats = [...assignMap.entries()].map(([key, tnvedCode]) => ({
      key, attrName: "Код ТН ВЭД ЕАЭС", found: true, tnvedCode,
    }));
    ozonTnvedApplyProgress.categoryStats = categoryStats;

    if (dryRun) {
      ozonTnvedApplyProgress.running = false;
      ozonTnvedApplyProgress.completedAt = new Date().toISOString();
      ozonTnvedApplyProgress.updated = updateItems.length;
      return { ok: true, dryRun, total: products.length, candidates: updateItems.length, categoryStats, sample: updateItems.slice(0, 5) };
    }

    let updated = 0;
    let errors = 0;
    const errorSamples = [];
    for (const chunk of chunkArray(updateItems, 100)) {
      try {
        await ozonRequest("/v1/product/attributes/update", { items: chunk }, account);
        updated += chunk.length;
      } catch (error) {
        errors += chunk.length;
        const errMsg = cleanText(error?.message || String(error)).slice(0, 300);
        if (errorSamples.length < 3) errorSamples.push(errMsg);
        logger.warn("ozon tnved apply chunk failed", { detail: errMsg, count: chunk.length });
      }
      ozonTnvedApplyProgress.processed += chunk.length;
      ozonTnvedApplyProgress.updated = updated;
      ozonTnvedApplyProgress.errors = errors;
      ozonTnvedApplyProgress.errorSamples = errorSamples;
    }

    logger.info("ozon tnved apply by category", { updated, total: products.length, errors });
    return { ok: errors === 0, dryRun, total: products.length, updated, errors, errorSamples, categoryStats };
  } finally {
    ozonTnvedApplyProgress.running = false;
    ozonTnvedApplyProgress.completedAt = new Date().toISOString();
  }
}

// ─── Маршруты ────────────────────────────────────────────────────────────────

function resolveOzonAccountOr400(request, response) {
  const accountId = cleanText(request.body?.accountId || request.query?.accountId || "");
  const account = accountId ? getOzonAccountByTarget(accountId) : getOzonAccountByTarget("ozon");
  if (!account || !account.clientId || !account.apiKey) {
    response.status(400).json({ error: "Ozon аккаунт не настроен." });
    return null;
  }
  return account;
}

app.post("/api/ozon/attributes/clear-marking", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveOzonAccountOr400(request, response);
    if (!account) return;
    const dryRun = request.body?.dryRun !== false;
    const result = await ozonClearMarkingRequirement(account, { dryRun });
    if (!dryRun) await appendAudit(request, "ozon.attributes.clear_marking", { newValue: { updated: result.updated, total: result.total } });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/attributes/backfill-tnved", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveOzonAccountOr400(request, response);
    if (!account) return;
    const dryRun = request.body?.dryRun !== false;
    const tnvedCode = cleanText(request.body?.tnvedCode || "");
    if (!tnvedCode) return response.status(400).json({ error: "tnvedCode is required" });
    const result = await ozonBackfillTnved(account, tnvedCode, { dryRun });
    if (!dryRun) await appendAudit(request, "ozon.attributes.backfill_tnved", { newValue: { code: tnvedCode, updated: result.updated } });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/yandex/attributes/backfill-tnved", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun !== false;
    const tnvedCode = cleanText(request.body?.tnvedCode || "");
    if (!tnvedCode) return response.status(400).json({ error: "tnvedCode is required" });
    const result = await yandexBackfillTnved(tnvedCode, { dryRun });
    if (!dryRun) await appendAudit(request, "yandex.attributes.backfill_tnved", { newValue: { code: tnvedCode, updated: result.updated } });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── ТН ВЭД по категориям ─────────────────────────────────────────────────────

// Список всех категорий на Ozon + сохранённые назначения.
app.get("/api/ozon/tnved/categories", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveOzonAccountOr400(request, response);
    if (!account) return;
    const [breakdown, categoryNameMap, savedAssignments] = await Promise.all([
      ozonGetCategoryBreakdown(account),
      ozonFlatCategoryMap(account),
      readOzonTnvedAssignments(),
    ]);
    // assignMap поддерживает оба ключа: "descCatId:typeId" и "descCatId:0" (весь тип)
    const assignMap = new Map(savedAssignments.map((a) => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)]));
    const categories = breakdown.categories.map((cat) => {
      const key = `${cat.descCatId}:${cat.typeId}`;
      const catName = categoryNameMap.get(Number(cat.descCatId)) || "";
      const typeName = categoryNameMap.get(key) || "";
      // Fallback: берём код типа, потом код всей категории (typeId=0)
      const tnvedCode = assignMap.get(key) || assignMap.get(`${cat.descCatId}:0`) || "";
      return {
        descCatId: cat.descCatId,
        typeId: cat.typeId,
        count: cat.count,
        offerIds: cat.offerIds,
        categoryName: catName,
        typeName,
        displayName: typeName ? `${catName} / ${typeName}` : (catName || `Категория ${cat.descCatId}`),
        tnvedCode,
      };
    }).sort((a, b) => b.count - a.count);
    response.json({ categories, totalProducts: breakdown.totalProducts });
  } catch (error) {
    next(error);
  }
});

// Сохранить назначения ТН ВЭД по категориям.
app.put("/api/ozon/tnved/assignments", requireAdmin, async (request, response, next) => {
  try {
    const assignments = (Array.isArray(request.body?.assignments) ? request.body.assignments : [])
      .map((a) => ({
        descCatId: Number(a.descCatId || 0),
        typeId: Number(a.typeId || 0),
        tnvedCode: cleanText(a.tnvedCode || ""),
      }))
      .filter((a) => a.descCatId > 0);
    await writeOzonTnvedAssignments(assignments);
    await appendAudit(request, "ozon.tnved.assignments_saved", { newValue: { count: assignments.length } });
    response.json({ ok: true, saved: assignments.length });
  } catch (error) {
    next(error);
  }
});

// Статус текущего применения ТН ВЭД.
app.get("/api/ozon/tnved/progress", async (_request, response) => {
  response.json({ ...ozonTnvedApplyProgress });
});

// Запустить применение ТН ВЭД ко всем товарам Ozon (асинхронно).
app.post("/api/ozon/tnved/apply", requireAdmin, async (request, response, next) => {
  try {
    const account = resolveOzonAccountOr400(request, response);
    if (!account) return;
    if (ozonTnvedApplyProgress.running) {
      return response.status(409).json({ error: "Уже выполняется", progress: ozonTnvedApplyProgress });
    }
    const dryRun = request.body?.dryRun === true;
    const assignments = await readOzonTnvedAssignments();
    if (!assignments.some((a) => cleanText(a.tnvedCode))) {
      return response.status(400).json({ error: "Нет назначений. Сохраните коды ТН ВЭД для категорий." });
    }
    if (dryRun) {
      const settingsDry = await readAppSettings().catch(() => null);
      const fallbackCodeDry = cleanText(settingsDry?.tnved?.code || "");
      const result = await ozonApplyTnvedByCategory(account, assignments, { dryRun: true, fallbackCode: fallbackCodeDry });
      return response.json(result);
    }
    const settings = await readAppSettings().catch(() => null);
    const fallbackCode = cleanText(settings?.tnved?.code || "");
    void appendAudit(request, "ozon.tnved.apply", { newValue: { assignmentCount: assignments.length, fallbackCode } });
    response.status(202).json({ ok: true, async: true, message: "Применение ТН ВЭД запущено в фоне" });
    setImmediate(() => {
      ozonApplyTnvedByCategory(account, assignments, { dryRun: false, fallbackCode }).catch((error) => {
        logger.error("ozon tnved apply background failed", { detail: error?.message || String(error) });
        ozonTnvedApplyProgress.running = false;
        ozonTnvedApplyProgress.completedAt = new Date().toISOString();
      });
    });
  } catch (error) {
    next(error);
  }
});

// ─── Умный бэкфилл Яндекс: per-product код через Ozon-категории ───────────────
// Для каждого товара Яндекс находим связанный Ozon-товар по offerId,
// берём его descCatId/typeId из raw.ozon, ищем код в assignments.
// При отсутствии категории — используем код из настроек tnved.code.

async function yandexBackfillTnvedFromAssignments({ dryRun = true } = {}) {
  const prisma = getPrisma();
  if (!prisma) return { ok: false, error: "postgres_unavailable" };

  const [assignments, settings] = await Promise.all([
    readOzonTnvedAssignments(),
    readAppSettings().catch(() => null),
  ]);

  const fallback = cleanText(settings?.tnved?.code || "");
  const assignMap = new Map(
    assignments
      .filter((a) => cleanText(a.tnvedCode))
      .map((a) => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)]),
  );

  if (!assignMap.size && !fallback) {
    return { ok: false, error: "Нет назначений ТН ВЭД и не задан код по умолчанию. Заполните коды на странице «Коды ТН ВЭД»." };
  }

  const yandexRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: "yandex", archived: false },
    select: { offerId: true, target: true },
    orderBy: { id: "asc" },
    take: 50000,
  });

  if (!yandexRows.length) return { ok: true, dryRun, total: 0, candidates: 0, reason: "no_yandex_products" };

  // Получаем категории Ozon-товаров по совпадению offerId
  const offerIdSet = [...new Set(yandexRows.map((r) => cleanText(r.offerId)).filter(Boolean))];
  const ozonRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: "ozon", offerId: { in: offerIdSet } },
    select: { offerId: true, raw: true },
  });

  const ozonCatByOfferId = new Map();
  for (const row of ozonRows) {
    const offerId = cleanText(row.offerId);
    if (!offerId || ozonCatByOfferId.has(offerId)) continue;
    const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
    const ozonData = (raw.ozon && typeof raw.ozon === "object") ? raw.ozon : {};
    const descCatId = Number(ozonData.categoryId || ozonData.descCatId || 0);
    const typeId = Number(ozonData.typeId || 0);
    if (descCatId) ozonCatByOfferId.set(offerId, { descCatId, typeId });
  }

  const offers = [];
  let withCategory = 0;
  let withFallback = 0;
  let skipped = 0;

  for (const row of yandexRows) {
    const offerId = cleanText(row.offerId);
    if (!offerId) { skipped++; continue; }
    const cat = ozonCatByOfferId.get(offerId);
    let code = null;
    if (cat) {
      code = assignMap.get(`${cat.descCatId}:${cat.typeId}`) || assignMap.get(`${cat.descCatId}:0`) || null;
      if (code) withCategory++;
    }
    if (!code && fallback) { code = fallback; withFallback++; }
    if (!code) { skipped++; continue; }
    offers.push({ offerId, customsTariffCode: code });
  }

  const total = yandexRows.length;
  const candidates = offers.length;

  if (dryRun) {
    return { ok: true, dryRun, total, candidates, withCategory, withFallback, skipped, sample: offers.slice(0, 5) };
  }
  if (!candidates) {
    return { ok: false, dryRun, total, candidates: 0, reason: "no_codes_resolved", withCategory, withFallback, skipped };
  }

  const shops = (typeof uniqueYandexShopsByBusiness === "function" ? uniqueYandexShopsByBusiness() : null)
    || getYandexShops().filter((s) => s.apiKey && s.businessId);
  if (!shops.length) return { ok: false, error: "yandex_not_configured" };

  const results = [];
  for (const shop of shops) {
    const shopResults = await sendYandexOfferMappings(shop, offers);
    results.push(...shopResults);
  }

  const failed = results.filter((r) => !r.ok).length;
  logger.info("yandex backfill tnved from assignments", {
    total, candidates, withCategory, withFallback, skipped, failed,
    shops: shops.map((s) => s.id),
  });
  return {
    ok: failed === 0,
    dryRun,
    total,
    updated: results.filter((r) => r.ok).length,
    candidates,
    withCategory,
    withFallback,
    skipped,
    failed,
    errors: results.filter((r) => !r.ok).slice(0, 10),
  };
}

// Применить ТН ВЭД на Яндекс.Маркет (per-product, из Ozon-категорий).
app.post("/api/yandex/tnved/apply", requireAdmin, async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun === true;
    const result = await yandexBackfillTnvedFromAssignments({ dryRun });
    if (!dryRun && result.ok) {
      void appendAudit(request, "yandex.tnved.apply", {
        newValue: { updated: result.updated, withCategory: result.withCategory, withFallback: result.withFallback },
      });
    }
    response.json(result);
  } catch (error) {
    next(error);
  }
});

// Сохранить код ТН ВЭД по умолчанию (используется как fallback для Яндекс).
app.put("/api/settings/tnved-code", requireAdmin, async (request, response, next) => {
  try {
    const code = cleanText(request.body?.code || "").replace(/\s/g, "").slice(0, 20);
    const previous = await readAppSettings();
    await writeAppSettings({ ...previous, tnved: { ...(previous.tnved || {}), code } });
    response.json({ ok: true, code });
  } catch (error) {
    next(error);
  }
});

// ─── Отчёт о покрытии ТН ВЭД по всем площадкам ───────────────────────────────

app.get("/api/tnved/report", requireAdmin, async (request, response, next) => {
  try {
    const [assignments, wbRules, settings] = await Promise.all([
      readOzonTnvedAssignments(),
      readWbImportRules().catch(() => null),
      readAppSettings().catch(() => null),
    ]);

    const assignedCategories = assignments.filter((a) => cleanText(a.tnvedCode)).length;
    const totalCategories = assignments.length;
    const prisma = getPrisma();
    let yandexTotal = 0;
    let ozonTotal = 0;
    if (prisma) {
      try {
        [yandexTotal, ozonTotal] = await Promise.all([
          prisma.warehouseProduct.count({ where: { marketplace: "yandex", archived: false } }),
          prisma.warehouseProduct.count({ where: { marketplace: "ozon", archived: false } }),
        ]);
      } catch {
        // DB недоступна — показываем 0
      }
    }

    response.json({
      ozon: {
        assignedCategories,
        totalCategories,
        totalProducts: ozonTotal,
        lastApplied: ozonTnvedApplyProgress.completedAt,
        assignments: assignments.filter((a) => cleanText(a.tnvedCode)).map((a) => ({
          descCatId: a.descCatId,
          typeId: a.typeId,
          tnvedCode: a.tnvedCode,
        })),
      },
      yandex: {
        totalProducts: yandexTotal,
        defaultCode: cleanText(settings?.tnved?.code || ""),
      },
      wb: {
        tnvedCode: cleanText(wbRules?.tnved || ""),
        subjectId: wbRules?.subjectId || 0,
        subjectName: cleanText(wbRules?.subjectName || ""),
      },
    });
  } catch (error) {
    next(error);
  }
});
