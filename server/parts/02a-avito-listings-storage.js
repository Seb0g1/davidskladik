// Хранилище объявлений для фида Avito Автозагрузки (avito-listings.json) и
// гибких правил импорта Ozon → Avito (avito-import-rules.json). Объявления не
// живут в Postgres (enum Marketplace пока ozon|yandex), поэтому файл — как
// marketplace-accounts.json.

function normalizeAvitoListing(input = {}, current = {}) {
  const adId = cleanText(input.adId ?? input.ad_id ?? current.adId);
  if (!adId) return null;
  const imageUrls = (Array.isArray(input.imageUrls ?? input.image_urls) ? (input.imageUrls ?? input.image_urls) : (current.imageUrls || []))
    .map((url) => cleanText(url))
    .filter(Boolean)
    .slice(0, 10);
  // Личный коэффициент наценки Avito (как markup у Ozon/Yandex): цена =
  // закупка поставщика × коэффициент, общие правила наценки не применяются.
  // 0 = не задан — работают общие правила (Настройки → Цены).
  const markupCoefficientRaw = Number(input.markupCoefficient ?? input.markup_coefficient ?? current.markupCoefficient ?? 0);
  const markupCoefficient = Number.isFinite(markupCoefficientRaw) && markupCoefficientRaw > 0
    ? Number(Math.min(100, markupCoefficientRaw).toFixed(4))
    : 0;
  const priceRub = Number(input.priceRub ?? input.price_rub ?? input.price ?? current.priceRub ?? 0);
  const rawExtra = input.extraFields ?? input.extra_fields ?? current.extraFields;
  const extraFields = rawExtra && typeof rawExtra === "object" && !Array.isArray(rawExtra) ? rawExtra : {};
  return {
    adId,
    sourceProductId: cleanText(input.sourceProductId ?? input.source_product_id ?? current.sourceProductId),
    sourceOfferId: cleanText(input.sourceOfferId ?? input.source_offer_id ?? current.sourceOfferId),
    source: cleanText(input.source ?? current.source) || "manual",
    title: cleanText(input.title ?? current.title),
    description: typeof (input.description ?? current.description) === "string" ? (input.description ?? current.description) : "",
    brand: cleanText(input.brand ?? current.brand),
    volumeMl: Number(input.volumeMl ?? input.volume_ml ?? current.volumeMl ?? 0) || 0,
    priceRub: Number.isFinite(priceRub) && priceRub > 0 ? Math.round(priceRub) : 0,
    markupCoefficient,
    imageUrls,
    category: cleanText(input.category ?? current.category),
    goodsType: cleanText(input.goodsType ?? input.goods_type ?? current.goodsType),
    goodsSubType: cleanText(input.goodsSubType ?? input.goods_sub_type ?? current.goodsSubType),
    subType: cleanText(input.subType ?? input.sub_type ?? current.subType),
    perfumeryType: cleanText(input.perfumeryType ?? input.perfumery_type ?? current.perfumeryType),
    categoryKey: cleanText(input.categoryKey ?? input.category_key ?? current.categoryKey),
    categoryAutoDefaulted: parseBooleanSetting(input.categoryAutoDefaulted ?? input.category_auto_defaulted, current.categoryAutoDefaulted === true),
    adType: cleanText(input.adType ?? input.ad_type ?? current.adType),
    condition: cleanText(input.condition ?? current.condition),
    address: cleanText(input.address ?? current.address),
    extraFields,
    enabled: parseBooleanSetting(input.enabled, current.enabled !== false),
    // Актуализируется фоновым рефрешем и живым обогащением фида: товар без
    // остатков или в архиве не попадает в XML (Avito снимет объявление).
    outOfStock: parseBooleanSetting(input.outOfStock ?? input.out_of_stock, current.outOfStock === true),
    lastSyncedAt: cleanText(input.lastSyncedAt ?? input.last_synced_at ?? current.lastSyncedAt) || null,
    createdAt: current.createdAt || input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

async function readAvitoListingsFile() {
  try {
    const data = JSON.parse(await fs.readFile(avitoListingsPath, "utf8"));
    const items = Array.isArray(data.items) ? data.items : [];
    return {
      feedToken: cleanText(data.feedToken),
      updatedAt: data.updatedAt || null,
      items: items.map((item) => normalizeAvitoListing(item)).filter(Boolean),
    };
  } catch (error) {
    if (error.code !== "ENOENT") logger.warn("read avito listings failed", { detail: error?.message || String(error) });
    return { feedToken: "", updatedAt: null, items: [] };
  }
}

// Один товар склада = одно объявление. Дубли появляются, когда у товара
// меняется offerId (adId = oz-<offerId> становится новым, старое объявление
// остаётся) — на Avito это два одинаковых объявления. Оставляем самое свежее
// (createdAt новее, при равенстве — позже в списке), ручные объявления без
// sourceProductId не трогаем.
function dedupeAvitoListingsBySource(items) {
  const keptByProductId = new Map();
  const result = [];
  for (const item of items) {
    const sourceProductId = cleanText(item.sourceProductId);
    if (!sourceProductId) {
      result.push(item);
      continue;
    }
    const existing = keptByProductId.get(sourceProductId);
    if (!existing) {
      keptByProductId.set(sourceProductId, item);
      result.push(item);
      continue;
    }
    const existingCreatedAt = Date.parse(existing.createdAt || 0) || 0;
    const itemCreatedAt = Date.parse(item.createdAt || 0) || 0;
    if (itemCreatedAt >= existingCreatedAt) {
      result[result.indexOf(existing)] = item;
      keptByProductId.set(sourceProductId, item);
    }
  }
  return result;
}

async function writeAvitoListingsFile(state) {
  await fs.mkdir(dataDir, { recursive: true });
  const items = (Array.isArray(state.items) ? state.items : []).map((item) => normalizeAvitoListing(item)).filter(Boolean);
  const deduped = dedupeAvitoListingsBySource(items);
  if (deduped.length !== items.length) {
    logger.info("avito listings dedupe", { removed: items.length - deduped.length, total: deduped.length });
  }
  const payload = {
    updatedAt: new Date().toISOString(),
    feedToken: cleanText(state.feedToken),
    items: deduped,
  };
  await fs.writeFile(avitoListingsPath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

// Секрет в URL фида: Авито скачивает XML без сессии, поэтому маршрут публичный,
// но угадать ссылку без токена нельзя.
async function ensureAvitoFeedToken() {
  const state = await readAvitoListingsFile();
  if (state.feedToken) return state.feedToken;
  const feedToken = crypto.randomBytes(24).toString("hex");
  await writeAvitoListingsFile({ ...state, feedToken });
  return feedToken;
}

async function upsertAvitoListings(newItems, { source = "manual" } = {}) {
  const state = await readAvitoListingsFile();
  const byAdId = new Map(state.items.map((item) => [item.adId, item]));
  let created = 0;
  let updated = 0;
  for (const raw of Array.isArray(newItems) ? newItems : []) {
    const current = byAdId.get(cleanText(raw?.adId ?? raw?.ad_id));
    // source по умолчанию ставится только новым объявлениям: частичные
    // обновления (например, дозаполнение описаний) не должны перетирать
    // исходный source существующего объявления.
    const normalized = normalizeAvitoListing(current ? { ...raw } : { source, ...raw }, current || {});
    if (!normalized) continue;
    if (current) updated += 1;
    else created += 1;
    byAdId.set(normalized.adId, normalized);
  }
  const saved = await writeAvitoListingsFile({ ...state, items: [...byAdId.values()] });
  return { created, updated, total: saved.items.length };
}

async function removeAvitoListings(adIds) {
  const removeSet = new Set((Array.isArray(adIds) ? adIds : splitList(adIds)).map((value) => cleanText(value)).filter(Boolean));
  if (!removeSet.size) return { removed: 0 };
  const state = await readAvitoListingsFile();
  const kept = state.items.filter((item) => !removeSet.has(item.adId));
  const removed = state.items.length - kept.length;
  if (removed > 0) await writeAvitoListingsFile({ ...state, items: kept });
  return { removed };
}

// --- Правила импорта Ozon → Avito ---

function defaultAvitoImportRules() {
  return {
    enabled: true,
    // Товары с распознанным объёмом меньше порога не импортируются (0 = выкл).
    minVolumeMl: 0,
    // Пропускать товары, в названии которых объём распознать не удалось.
    skipWithoutVolume: false,
    // Стоп-слова по названию (регистронезависимо, «ё» приравнивается к «е»).
    excludeTitleWords: ["дубль", "удаленый", "удаленный"],
    // Если список не пуст — импортируются только названия, содержащие эти слова.
    includeTitleWords: [],
    excludeBrands: [],
    includeBrands: [],
    minPriceRub: 0,
    maxPriceRub: 0,
    requireImages: true,
    skipArchived: false,
    minStock: 0,
    // Категория по умолчанию для товаров без уверенной классификации по
    // названию (ключ из AVITO_CATEGORY_SPECS, см. 02a-avito-categorizer.js).
    defaultCategoryKey: "parfum-edt",
    // Множитель к рублёвой цене Ozon для цены на Avito.
    priceCoefficient: 1,
    // Гибкие правила цены: от базовой цены (₽) действует свой коэффициент,
    // берётся правило с наибольшим подходящим порогом. Пустой список — везде
    // используется priceCoefficient.
    priceRules: [],
    // Автообновление фида: подтягивать актуальную цену склада при каждой
    // сборке XML и скрывать товары без остатков/в архиве.
    autoUpdatePrices: true,
    hideOutOfStock: true,
    maxItems: 0,
    // Фолбэки для объявлений без распознанной категории и обязательные
    // реквизиты фида. AdType — строго «приобретен» без «ё» (значение Avito).
    feedDefaults: {
      category: "Красота и здоровье",
      goodsType: "Парфюмерия",
      adType: "Товар приобретен на продажу",
      condition: "Новое",
      address: "Москва, Складочная ул., 1с1",
      description: "{title} — оригинальная продукция, новый товар. Быстрая отправка, Авито Доставка.",
    },
  };
}

function normalizeAvitoPriceRule(input = {}) {
  const minPriceRub = Number(input.minPriceRub ?? input.min_price_rub ?? input.minPrice ?? 0);
  const coefficient = Number(input.coefficient ?? input.markup ?? 0);
  if (!Number.isFinite(minPriceRub) || minPriceRub < 0) return null;
  if (!Number.isFinite(coefficient) || coefficient <= 0) return null;
  return {
    minPriceRub: Math.round(minPriceRub),
    coefficient: Number(Math.min(100, coefficient).toFixed(4)),
  };
}

// Коэффициент для базовой цены: правило с наибольшим порогом <= цены,
// иначе базовый priceCoefficient.
function avitoPriceCoefficientFor(basePriceRub, rules = {}) {
  const price = Number(basePriceRub || 0);
  let matched = null;
  for (const rule of Array.isArray(rules.priceRules) ? rules.priceRules : []) {
    if (price >= rule.minPriceRub && (!matched || rule.minPriceRub > matched.minPriceRub)) matched = rule;
  }
  return matched ? matched.coefficient : (Number(rules.priceCoefficient) > 0 ? Number(rules.priceCoefficient) : 1);
}

function normalizeAvitoWordList(value, fallback = []) {
  const source = Array.isArray(value) ? value : (value === undefined || value === null ? fallback : splitList(value));
  return [...new Set(source.map((word) => cleanText(word).toLowerCase().replace(/ё/g, "е")).filter(Boolean))].slice(0, 200);
}

function normalizeAvitoImportRules(input = {}) {
  const fallback = defaultAvitoImportRules();
  const raw = input && typeof input === "object" ? input : {};
  const rawDefaults = raw.feedDefaults && typeof raw.feedDefaults === "object" ? raw.feedDefaults : {};
  const clampNumber = (value, fallbackValue, { min = 0, max = Number.MAX_SAFE_INTEGER, round = true } = {}) => {
    const parsed = Number(value ?? fallbackValue);
    if (!Number.isFinite(parsed)) return fallbackValue;
    const bounded = Math.min(max, Math.max(min, parsed));
    return round ? Math.round(bounded) : bounded;
  };
  return {
    enabled: parseBooleanSetting(raw.enabled, fallback.enabled),
    minVolumeMl: clampNumber(raw.minVolumeMl ?? raw.min_volume_ml, fallback.minVolumeMl, { max: 100000 }),
    skipWithoutVolume: parseBooleanSetting(raw.skipWithoutVolume ?? raw.skip_without_volume, fallback.skipWithoutVolume),
    excludeTitleWords: normalizeAvitoWordList(raw.excludeTitleWords ?? raw.exclude_title_words, fallback.excludeTitleWords),
    includeTitleWords: normalizeAvitoWordList(raw.includeTitleWords ?? raw.include_title_words, fallback.includeTitleWords),
    excludeBrands: normalizeAvitoWordList(raw.excludeBrands ?? raw.exclude_brands, fallback.excludeBrands),
    includeBrands: normalizeAvitoWordList(raw.includeBrands ?? raw.include_brands, fallback.includeBrands),
    minPriceRub: clampNumber(raw.minPriceRub ?? raw.min_price_rub, fallback.minPriceRub),
    maxPriceRub: clampNumber(raw.maxPriceRub ?? raw.max_price_rub, fallback.maxPriceRub),
    requireImages: parseBooleanSetting(raw.requireImages ?? raw.require_images, fallback.requireImages),
    skipArchived: parseBooleanSetting(raw.skipArchived ?? raw.skip_archived, fallback.skipArchived),
    minStock: clampNumber(raw.minStock ?? raw.min_stock, fallback.minStock, { max: 100000 }),
    defaultCategoryKey: getAvitoCategorySpec(raw.defaultCategoryKey ?? raw.default_category_key)
      ? cleanText(raw.defaultCategoryKey ?? raw.default_category_key)
      : fallback.defaultCategoryKey,
    priceCoefficient: clampNumber(raw.priceCoefficient ?? raw.price_coefficient, fallback.priceCoefficient, { min: 0.01, max: 100, round: false }),
    priceRules: (Array.isArray(raw.priceRules ?? raw.price_rules) ? (raw.priceRules ?? raw.price_rules) : fallback.priceRules)
      .map(normalizeAvitoPriceRule)
      .filter(Boolean)
      .sort((a, b) => a.minPriceRub - b.minPriceRub)
      .slice(0, 100),
    autoUpdatePrices: parseBooleanSetting(raw.autoUpdatePrices ?? raw.auto_update_prices, fallback.autoUpdatePrices),
    hideOutOfStock: parseBooleanSetting(raw.hideOutOfStock ?? raw.hide_out_of_stock, fallback.hideOutOfStock),
    maxItems: clampNumber(raw.maxItems ?? raw.max_items, fallback.maxItems, { max: 1000000 }),
    feedDefaults: {
      category: cleanText(rawDefaults.category ?? fallback.feedDefaults.category),
      goodsType: cleanText(rawDefaults.goodsType ?? rawDefaults.goods_type ?? fallback.feedDefaults.goodsType),
      adType: normalizeAvitoAdType(rawDefaults.adType ?? rawDefaults.ad_type, fallback.feedDefaults.adType),
      condition: normalizeAvitoCondition(rawDefaults.condition, fallback.feedDefaults.condition),
      // Адрес и шаблон описания обязательны для публикации на Avito —
      // пустое значение заменяется дефолтом.
      address: cleanText(rawDefaults.address) || fallback.feedDefaults.address,
      description: typeof rawDefaults.description === "string" && rawDefaults.description.trim()
        ? rawDefaults.description
        : fallback.feedDefaults.description,
    },
  };
}

async function readAvitoImportRules() {
  try {
    const parsed = JSON.parse(await fs.readFile(avitoImportRulesPath, "utf8"));
    return normalizeAvitoImportRules(parsed);
  } catch (error) {
    if (error.code !== "ENOENT") logger.warn("read avito import rules failed", { detail: error?.message || String(error) });
    return defaultAvitoImportRules();
  }
}

async function writeAvitoImportRules(rules) {
  const normalized = normalizeAvitoImportRules(rules);
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(avitoImportRulesPath, JSON.stringify(normalized, null, 2), "utf8");
  return normalized;
}
