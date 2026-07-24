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
