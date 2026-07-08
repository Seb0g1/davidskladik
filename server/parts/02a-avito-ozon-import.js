// Импорт товаров Ozon → Avito по гибким правилам (avito-import-rules.json):
// объём меньше порога, стоп-слова («Дубль», «Удаленый», …), бренды, цена,
// остатки, наличие фото. Результат — объявления в avito-listings.json,
// которые попадают в XML-фид Автозагрузки.

function normalizeAvitoMatchText(value) {
  return cleanText(value).toLowerCase().replace(/ё/g, "е");
}

// «100 мл», «20мл», «7.5 ml» — берём максимальный найденный объём
// (для наборов вида «5 мл + 10 мл» фильтр по порогу применяется к большему).
function parseVolumeMlFromText(value) {
  const text = normalizeAvitoMatchText(value);
  if (!text) return 0;
  let maxVolume = 0;
  for (const match of text.matchAll(/(\d+(?:[.,]\d+)?)\s*(?:мл|ml)(?![a-zа-я])/gi)) {
    const volume = Number(match[1].replace(",", "."));
    if (Number.isFinite(volume) && volume > maxVolume) maxVolume = volume;
  }
  return maxVolume;
}

function extractAvitoImageUrls(images) {
  // В Postgres колонка images — объект { imageUrl, images: [...] }
  // (см. productToPostgresData), но встречается и плоский массив URL.
  const list = Array.isArray(images)
    ? images
    : images && typeof images === "object"
      ? [images.imageUrl, ...(Array.isArray(images.images) ? images.images : [])]
      : [];
  return [...new Set(list
    .map((item) => cleanText(typeof item === "string" ? item : item?.url || item?.fileName || ""))
    .filter((url) => /^https?:\/\//i.test(url)))]
    .slice(0, 10);
}

// --- Цена от поставщика ---
// Цена Avito считается от закупочной цены привязанного поставщика (PriceMaster)
// с наценкой Avito из общих «Гибких правил наценки» (Настройки → Цены), а не от
// листинговой цены Ozon. Снимок выбранного поставщика лежит в
// warehouse_products.raw->selectedSupplier. Фолбэк — targetPrice (та же цена от
// поставщика, посчитанная для Ozon); листинговая цена Ozon не используется.

async function loadAvitoSupplierPricingMap(productIds = []) {
  const prisma = getPrisma();
  const ids = [...new Set(productIds.map((value) => cleanText(value)).filter(Boolean))];
  if (!prisma || !ids.length) return new Map();
  const map = new Map();
  const chunkSize = 5000;
  for (let index = 0; index < ids.length; index += chunkSize) {
    const chunk = ids.slice(index, index + chunkSize);
    const rows = await prisma.$queryRaw`
      SELECT id, raw->'selectedSupplier' AS supplier
      FROM warehouse_products
      WHERE id = ANY(${chunk})
    `;
    for (const row of rows) {
      if (row.supplier && typeof row.supplier === "object") map.set(cleanText(row.id), row.supplier);
    }
  }
  return map;
}

async function loadAvitoPricingContext() {
  const [appSettings, rate] = await Promise.all([
    readAppSettings().catch(() => null),
    getUsdRate().catch(() => null),
  ]);
  return {
    appSettings: appSettings || {},
    usdRate: Number(rate?.rate || appSettings?.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95),
  };
}

// Разбивка цены Avito от поставщика: закупка (₽), USD-цена, курс и коэффициент.
// Формула: цена поставщика (USD) × курс × коэффициент — как у Ozon/Yandex.
// productMarkup > 0 — личный коэффициент объявления, он побеждает общие
// правила наценки (Настройки → Цены). null = нет пригодной цены поставщика.
function avitoSupplierPriceBreakdown(supplier, { usdRate, appSettings } = {}, productMarkup = 0) {
  if (!supplier || typeof supplier !== "object") return null;
  const purchaseRub = warehouseSupplierPurchaseRubPrice(supplier, usdRate);
  if (!Number.isFinite(purchaseRub) || purchaseRub <= 0 || purchaseRub >= Number.MAX_SAFE_INTEGER) return null;
  const rate = Number(usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  // Рублёвый поставщик: закупка уже в рублях, курс в цене не участвует —
  // USD-эквивалент считается только для подбора правила наценки по порогу.
  const rubNative = supplierPriceIsRubNative(supplier);
  const usd = !rubNative && Number(supplier.price || 0) > 0
    ? Number(supplier.price)
    : purchaseRub / rate;
  const coefficient = resolveMarkupCoefficient({
    productMarkup,
    marketplace: "avito",
    supplierUsdPrice: usd,
    usdRate: rate,
    appSettings,
  });
  return {
    purchaseRub,
    supplierUsd: Number(Number(usd).toFixed(2)),
    usdRate: rate,
    rubNative,
    coefficient,
    priceRub: roundPrice(purchaseRub * coefficient),
  };
}

function computeAvitoSupplierPriceRub(supplier, pricing = {}, productMarkup = 0) {
  const breakdown = avitoSupplierPriceBreakdown(supplier, pricing, productMarkup);
  return breakdown ? breakdown.priceRub : 0;
}

// Базовая цена листинга: поставщик → targetPrice; поверх — локальные правила
// страницы Avito (priceCoefficient/priceRules, по умолчанию ×1). Личный
// коэффициент (productMarkup) даёт конечную цену от закупки поставщика —
// локальные правила страницы к ней не применяются.
function resolveAvitoListingPriceRub(product = {}, supplier, rules = {}, pricing = {}, productMarkup = 0) {
  const supplierPriceRub = computeAvitoSupplierPriceRub(supplier, pricing, productMarkup);
  if (productMarkup > 0 && supplierPriceRub > 0) return Math.round(supplierPriceRub);
  const basePriceRub = supplierPriceRub > 0 ? supplierPriceRub : Number(product.targetPrice || 0);
  if (!(basePriceRub > 0)) return 0;
  return Math.round(basePriceRub * avitoPriceCoefficientFor(basePriceRub, rules));
}

// Возвращает { ok, reasons, listing } — причины пропуска нужны для предпросмотра.
function evaluateAvitoImportCandidate(product = {}, rules = {}, pricing = {}) {
  const reasons = [];
  const title = cleanText(product.name);
  const matchTitle = normalizeAvitoMatchText(title);
  const matchBrand = normalizeAvitoMatchText(product.brand);

  if (!title) reasons.push("no_title");
  if (rules.skipArchived && product.archived) reasons.push("archived");

  const excludeWord = (rules.excludeTitleWords || []).find((word) => matchTitle.includes(word));
  if (excludeWord) reasons.push(`title_word:${excludeWord}`);
  if ((rules.includeTitleWords || []).length && !(rules.includeTitleWords || []).some((word) => matchTitle.includes(word))) {
    reasons.push("title_not_in_include_list");
  }

  const excludeBrand = (rules.excludeBrands || []).find((word) => matchBrand && matchBrand.includes(word));
  if (excludeBrand) reasons.push(`brand:${excludeBrand}`);
  if ((rules.includeBrands || []).length && !(rules.includeBrands || []).some((word) => matchBrand.includes(word))) {
    reasons.push("brand_not_in_include_list");
  }

  const volumeMl = parseVolumeMlFromText(title);
  if (rules.minVolumeMl > 0 && volumeMl > 0 && volumeMl < rules.minVolumeMl) {
    reasons.push(`volume_below_min:${volumeMl}`);
  }
  if (rules.skipWithoutVolume && volumeMl <= 0) reasons.push("volume_unknown");

  const priceRub = resolveAvitoListingPriceRub(product, pricing.supplierByProductId?.get(cleanText(product.id)), rules, pricing);
  if (priceRub <= 0) reasons.push("no_price");
  if (rules.minPriceRub > 0 && priceRub > 0 && priceRub < rules.minPriceRub) reasons.push(`price_below_min:${priceRub}`);
  if (rules.maxPriceRub > 0 && priceRub > rules.maxPriceRub) reasons.push(`price_above_max:${priceRub}`);

  const imageUrls = extractAvitoImageUrls(product.images);
  if (rules.requireImages && !imageUrls.length) reasons.push("no_images");

  const stock = Number(product.targetStock || 0);
  if (rules.minStock > 0 && stock < rules.minStock) reasons.push(`stock_below_min:${stock}`);

  if (reasons.length) return { ok: false, reasons, listing: null };

  // Категоризация по названию: полная цепочка тегов Avito из справочника
  // (см. 02a-avito-categorizer.js), Gender — только для парфюмерии.
  const classification = classifyAvitoCategory(title, rules);
  const spec = classification.spec;
  const gender = spec.gender ? detectAvitoPerfumeGender(title) : "";

  return {
    ok: true,
    reasons: [],
    listing: {
      adId: `oz-${cleanText(product.offerId) || cleanText(product.id)}`,
      sourceProductId: cleanText(product.id),
      sourceOfferId: cleanText(product.offerId),
      source: "ozon",
      title,
      brand: cleanText(product.brand),
      volumeMl,
      priceRub,
      imageUrls,
      categoryKey: spec.key,
      category: AVITO_FEED_CATEGORY,
      goodsType: spec.tags.GoodsType,
      goodsSubType: spec.tags.GoodsSubType || "",
      subType: spec.tags.SubType || "",
      perfumeryType: spec.tags.PerfumeryType || "",
      condition: spec.condition ? (rules.feedDefaults?.condition || "Новое") : "",
      adType: normalizeAvitoAdType(rules.feedDefaults?.adType),
      categoryAutoDefaulted: classification.autoDefaulted,
      ...(gender ? { extraFields: { Gender: gender } } : {}),
      enabled: true,
    },
  };
}

async function collectAvitoImportCandidates(rulesOverride = null) {
  const prisma = getPrisma();
  if (!prisma) {
    const error = new Error("Postgres недоступен: импорт Ozon → Avito требует базы товаров склада.");
    error.statusCode = 503;
    throw error;
  }
  const rules = normalizeAvitoImportRules(rulesOverride || (await readAvitoImportRules()));
  const products = await prisma.warehouseProduct.findMany({
    where: { marketplace: "ozon" },
    select: {
      id: true,
      offerId: true,
      name: true,
      brand: true,
      images: true,
      archived: true,
      targetStock: true,
      targetPrice: true,
    },
    orderBy: { updatedAt: "desc" },
  });

  const pricing = {
    ...(await loadAvitoPricingContext()),
    supplierByProductId: await loadAvitoSupplierPricingMap(products.map((product) => product.id)),
  };

  const matched = [];
  const skipped = [];
  for (const product of products) {
    if (rules.maxItems > 0 && matched.length >= rules.maxItems) break;
    const result = evaluateAvitoImportCandidate(product, rules, pricing);
    if (result.ok) matched.push(result.listing);
    else skipped.push({ id: product.id, offerId: product.offerId, name: product.name, reasons: result.reasons });
  }
  return { rules, totalOzonProducts: products.length, matched, skipped };
}

function summarizeAvitoMatchedByCategory(matched) {
  const byCategory = {};
  for (const listing of matched) {
    const label = avitoListingCategoryPath(listing) || "Без категории";
    byCategory[label] = (byCategory[label] || 0) + 1;
  }
  return byCategory;
}

function summarizeAvitoSkipReasons(skipped) {
  const byReason = {};
  for (const item of skipped) {
    // Причина с деталью («volume_below_min:15») агрегируется по префиксу.
    const key = String(item.reasons[0] || "unknown").split(":")[0];
    byReason[key] = (byReason[key] || 0) + 1;
  }
  return byReason;
}

async function previewAvitoOzonImport({ rules = null, sampleLimit = 50 } = {}) {
  const result = await collectAvitoImportCandidates(rules);
  return {
    rules: result.rules,
    totalOzonProducts: result.totalOzonProducts,
    matchedCount: result.matched.length,
    skippedCount: result.skipped.length,
    skippedByReason: summarizeAvitoSkipReasons(result.skipped),
    matchedByCategory: summarizeAvitoMatchedByCategory(result.matched),
    matchedSample: result.matched.slice(0, sampleLimit),
    skippedSample: result.skipped.slice(0, sampleLimit),
  };
}

async function applyAvitoOzonImport({ rules = null } = {}) {
  const result = await collectAvitoImportCandidates(rules);
  // Описания берём с Ozon: подставляем сохранённые в raw склада. Ключ не
  // задаётся вовсе, если описания нет, — upsert тогда сохранит уже
  // дозаполненное ранее описание существующего объявления.
  const descriptions = await loadStoredOzonDescriptionsMap(result.matched.map((listing) => listing.sourceProductId));
  for (const listing of result.matched) {
    const description = descriptions.get(cleanText(listing.sourceProductId));
    if (description) listing.description = description;
  }
  const { created, updated, total } = await upsertAvitoListings(result.matched, { source: "ozon" });
  return {
    totalOzonProducts: result.totalOzonProducts,
    matchedCount: result.matched.length,
    skippedCount: result.skipped.length,
    skippedByReason: summarizeAvitoSkipReasons(result.skipped),
    matchedByCategory: summarizeAvitoMatchedByCategory(result.matched),
    created,
    updated,
    totalListings: total,
  };
}
