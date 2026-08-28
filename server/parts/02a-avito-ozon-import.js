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

// Принятые Avito значения поля Volume (категория «Духи и туалетная вода»).
// 1-15 мл поштучно, затем только 30/50/75/90/100/120/150, иначе «Другой».
const AVITO_VOLUME_VALUES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 30, 50, 75, 90, 100, 120, 150];

function detectAvitoVolumeMl(title) {
  const ml = parseVolumeMlFromText(title);
  if (!ml || ml <= 0) return "";
  let closest = AVITO_VOLUME_VALUES[0];
  let minDiff = Infinity;
  for (const v of AVITO_VOLUME_VALUES) {
    const diff = Math.abs(ml - v);
    if (diff < minDiff) { minDiff = diff; closest = v; }
  }
  // Если ближайшее значение отличается более чем на 30%, возвращаем «Другой».
  if (minDiff > ml * 0.3) return "Другой";
  return `${closest} мл`;
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

// Карта поставщиков для цены Avito. Снапшот raw->selectedSupplier есть только
// у товаров, прошедших через отправку цены (normalizeWarehouseProduct не
// сохраняет selectedSupplier, а отправка цены дописывает его отдельно) — для
// остальных считаем живого поставщика из PriceMaster тем же механизмом, что и
// репрайс склада (самый дешёвый активный). Без этого фолбэка большинство
// товаров оставались без цены поставщика и фид падал на цену Ozon.
async function loadAvitoSupplierPricingMap(productIds = [], { fresh = true, freshChunkSize = 400 } = {}) {
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
  if (!fresh) return map;
  const missing = ids.filter((id) => !map.has(id));
  for (let index = 0; index < missing.length; index += freshChunkSize) {
    const chunk = missing.slice(index, index + freshChunkSize);
    try {
      const built = await buildFreshWarehouseProducts(chunk, {
        refreshPrices: false,
        persistMutations: false,
        livePriceMaster: true,
        batchPriceMaster: true,
      });
      for (const product of built) {
        if (product?.selectedSupplier) map.set(cleanText(product.id), product.selectedSupplier);
      }
    } catch (error) {
      logger.warn("avito supplier fresh build failed", { detail: error?.message || String(error), products: chunk.length });
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
    // Как у расчёта склада (buildFreshWarehouseProducts...): фиксированный курс
    // из настроек важнее живого курса из API.
    usdRate: Number(appSettings?.fixedUsdRate || rate?.rate || process.env.DEFAULT_USD_RATE || 95),
  };
}

// Разбивка цены Avito от поставщика: закупка (₽), USD-цена, курс и коэффициент.
// Формула: цена поставщика (USD) × курс × коэффициент — как у Ozon/Yandex.
// productMarkup > 0 — личный коэффициент объявления, он побеждает общие
// правила наценки (Настройки → Цены). null = нет пригодной цены поставщика.
function avitoSupplierPriceBreakdown(supplier, { usdRate, appSettings } = {}, productMarkup = 0) {
  if (!supplier || typeof supplier !== "object") return null;
  if (supplierUsesStockOnlyPricing(null, supplier)) return null;
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
    // Эффективный курс: закупка может прийти готовым снапшотом (purchaseRubPrice),
    // посчитанным по другому курсу, — в формуле показываем курс, из которого
    // реально сложилась закупка, чтобы умножение сходилось.
    usdRate: rubNative ? rate : Number((purchaseRub / usd).toFixed(2)),
    rubNative,
    coefficient,
    priceRub: roundPrice(purchaseRub * coefficient),
  };
}

function computeAvitoSupplierPriceRub(supplier, pricing = {}, productMarkup = 0) {
  const breakdown = avitoSupplierPriceBreakdown(supplier, pricing, productMarkup);
  return breakdown ? breakdown.priceRub : 0;
}

// Цена листинга. Приоритет:
// 1. Цена поставщика (закупка × коэффициент) — если поставщик даёт цену.
// 2. Фолбэк на targetPrice (цена склада/Ozon) — если поставщика нет или он
//    stock-only («наш склад»). Это позволяет публиковать на Avito товары без
//    привязки к поставщику с ценой, выставленной вручную или пришедшей с Ozon.
// Поверх — локальные правила страницы Avito (priceCoefficient/priceRules,
// по умолчанию ×1). Личный коэффициент (productMarkup) побеждает любые правила.
function resolveAvitoListingPriceRub(product = {}, supplier, rules = {}, pricing = {}, productMarkup = 0) {
  const supplierPriceRub = computeAvitoSupplierPriceRub(supplier, pricing, productMarkup);
  if (supplierPriceRub > 0) {
    if (productMarkup > 0) return Math.round(supplierPriceRub);
    return Math.round(supplierPriceRub * avitoPriceCoefficientFor(supplierPriceRub, rules));
  }
  // Нет цены поставщика → берём цену склада (targetPrice).
  const targetPrice = Number(product.targetPrice || product.target_price || 0);
  if (targetPrice > 0) {
    if (productMarkup > 0) return Math.round(targetPrice * productMarkup);
    const importCoeff = avitoPriceCoefficientFor(targetPrice, rules);
    // Если priceCoefficient не настроен (=1) и priceRules пусты, применяем
    // соотношение Avito/Ozon-наценки из общих настроек. targetPrice рассчитан
    // по Ozon-наценке; делим и умножаем на Avito-наценку чтобы не показывать
    // цену Ozon напрямую на Avito (новые товары, временный сбой PM).
    if (importCoeff === 1 && !(Array.isArray(rules.priceRules) && rules.priceRules.length) && pricing?.appSettings) {
      const ozonMarkup = Number(pricing.appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7) || 1.7;
      const avitoMarkup = Number(pricing.appSettings.defaultMarkups?.avito || process.env.DEFAULT_AVITO_MARKUP || 1.6) || 1.6;
      if (Math.abs(avitoMarkup - ozonMarkup) > 0.001) return Math.round(targetPrice * avitoMarkup / ozonMarkup);
    }
    return Math.round(targetPrice * importCoeff);
  }
  return 0;
}

// Возвращает { ok, reasons, listing } — причины пропуска нужны для предпросмотра.
function evaluateAvitoImportCandidate(product = {}, rules = {}, pricing = {}) {
  const reasons = [];
  const title = cleanText(product.name);
  const matchTitle = normalizeAvitoMatchText(title);
  const matchBrand = normalizeAvitoMatchText(product.brand);

  if (!title || isWeakProductName(title, product.offerId)) reasons.push("no_title");
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

  // pricing.skipPriceChecks — первый проход импорта: дешёвые фильтры без
  // похода за поставщиком, цена проверяется вторым проходом.
  let priceRub = 0;
  if (!pricing.skipPriceChecks) {
    priceRub = resolveAvitoListingPriceRub(product, pricing.supplierByProductId?.get(cleanText(product.id)), rules, pricing);
    if (priceRub <= 0) reasons.push("no_price");
    if (rules.minPriceRub > 0 && priceRub > 0 && priceRub < rules.minPriceRub) reasons.push(`price_below_min:${priceRub}`);
    if (rules.maxPriceRub > 0 && priceRub > rules.maxPriceRub) reasons.push(`price_above_max:${priceRub}`);
  }

  const imageUrls = (Array.isArray(product.avitoImages) && product.avitoImages.length)
    ? product.avitoImages
    : extractAvitoImageUrls(product.images);
  if (rules.requireImages && !imageUrls.length) reasons.push("no_images");

  const stock = Number(product.targetStock || 0);
  if (rules.minStock > 0 && stock < rules.minStock) reasons.push(`stock_below_min:${stock}`);

  if (reasons.length) return { ok: false, reasons, listing: null };

  // Категоризация по названию: полная цепочка тегов Avito из справочника
  // (см. 02a-avito-categorizer.js), Gender и PerfumeType — только для парфюмерии.
  const classification = classifyAvitoCategory(title, rules);
  const spec = classification.spec;
  const gender = spec.gender ? detectAvitoPerfumeGender(title) : "";
  const perfumeType = spec.gender ? detectAvitoPerfumeType(title) : "";
  const volume = spec.gender ? detectAvitoVolumeMl(title) : "";

  const extraFields = {};
  if (gender) extraFields.Gender = gender;
  if (perfumeType) extraFields.PerfumeType = perfumeType;
  if (volume) extraFields.Volume = volume;

  return {
    ok: true,
    reasons: [],
    listing: {
      adId: `oz-${cleanText(product.offerId) || cleanText(product.id)}-r1`,
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
      ...(Object.keys(extraFields).length ? { extraFields } : {}),
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
    where: { marketplace: "ozon", target: "ozon", links: { some: {} } },
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

  // Load manually-set Avito images (raw->'avitoImages') in chunks to avoid
  // pulling the full raw column for thousands of products.
  {
    const avitoImagesMap = new Map();
    const chunkSize = 1000;
    const allIds = products.map((p) => cleanText(p.id)).filter(Boolean);
    for (let i = 0; i < allIds.length; i += chunkSize) {
      const chunk = allIds.slice(i, i + chunkSize);
      const rows = await prisma.$queryRaw`
        SELECT id, COALESCE(raw->'avitoImages', '[]'::jsonb) AS avito_images
        FROM warehouse_products
        WHERE id = ANY(${chunk})
      `.catch(() => []);
      for (const row of rows) {
        const imgs = Array.isArray(row.avito_images) ? row.avito_images.map((u) => cleanText(u)).filter(Boolean) : [];
        if (imgs.length) avitoImagesMap.set(cleanText(row.id), imgs);
      }
    }
    for (const product of products) {
      product.avitoImages = avitoImagesMap.get(cleanText(product.id)) || [];
    }
    // For products that still have no avitoImages, check sibling variants (same offerId).
    // This handles the case where images were saved on a Yandex/WB variant but the
    // Ozon variant (used as sourceProductId in the feed) has an empty cache.
    const missingOfferIds = [...new Set(
      products.filter((p) => !p.avitoImages.length && cleanText(p.offerId)).map((p) => cleanText(p.offerId))
    )];
    if (missingOfferIds.length) {
      const siblingChunkSize = 1000;
      const siblingMap = new Map();
      for (let i = 0; i < missingOfferIds.length; i += siblingChunkSize) {
        const chunk = missingOfferIds.slice(i, i + siblingChunkSize);
        const rows = await prisma.$queryRaw`
          SELECT offer_id, raw->'avitoImages' AS avito_images
          FROM warehouse_products
          WHERE offer_id = ANY(${chunk})
            AND jsonb_array_length(COALESCE(raw->'avitoImages', '[]'::jsonb)) > 0
        `.catch(() => []);
        for (const row of rows) {
          const oid = cleanText(row.offer_id);
          if (!siblingMap.has(oid)) {
            const imgs = Array.isArray(row.avito_images) ? row.avito_images.map((u) => cleanText(u)).filter(Boolean) : [];
            if (imgs.length) siblingMap.set(oid, imgs);
          }
        }
      }
      for (const product of products) {
        if (!product.avitoImages.length) {
          const imgs = siblingMap.get(cleanText(product.offerId));
          if (imgs) product.avitoImages = imgs;
        }
      }
    }
  }

  const pricingContext = await loadAvitoPricingContext();
  // Проход 1: дешёвые фильтры (архив, объём, стоп-слова, фото) без похода за
  // поставщиком. Поставщиков (в т.ч. живой расчёт из PriceMaster) тянем только
  // для прошедших — иначе на 18k+ товаров это неподъёмно.
  const prelim = products.map((product) => ({
    product,
    result: evaluateAvitoImportCandidate(product, rules, { ...pricingContext, skipPriceChecks: true }),
  }));
  const priceCandidateIds = prelim.filter((row) => row.result.ok).map((row) => row.product.id);
  const pricing = {
    ...pricingContext,
    supplierByProductId: await loadAvitoSupplierPricingMap(priceCandidateIds),
  };

  const matched = [];
  const skipped = [];
  // Дедупликация: одинаковые названия на складе (дубли карточек Ozon) не должны
  // превращаться в несколько объявлений — Avito блокирует повторы. Товары
  // отсортированы по updatedAt desc, остаётся самый свежий.
  const seenTitleKeys = new Set();
  for (const { product, result } of prelim) {
    if (rules.maxItems > 0 && matched.length >= rules.maxItems) break;
    if (!result.ok) {
      skipped.push({ id: product.id, offerId: product.offerId, name: product.name, reasons: result.reasons });
      continue;
    }
    const full = evaluateAvitoImportCandidate(product, rules, pricing);
    if (!full.ok) {
      skipped.push({ id: product.id, offerId: product.offerId, name: product.name, reasons: full.reasons });
      continue;
    }
    const titleKey = normalizeAvitoMatchText(full.listing.title);
    if (titleKey && seenTitleKeys.has(titleKey)) {
      skipped.push({ id: product.id, offerId: product.offerId, name: product.name, reasons: ["duplicate_title"] });
      continue;
    }
    if (titleKey) seenTitleKeys.add(titleKey);
    matched.push(full.listing);
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

// Полный синк: добавляет новые привязанные товары + удаляет объявления
// ozon-источника, которые больше не проходят фильтры (потеряли поставщика,
// ушли в архив и т.п.). Ручные объявления (source != "ozon") не трогает.
async function syncAvitoOzonListings({ rules = null } = {}) {
  const result = await collectAvitoImportCandidates(rules);
  const matchedIds = new Set(
    result.matched.map((listing) => cleanText(listing.sourceProductId)).filter(Boolean),
  );

  const descriptions = await loadStoredOzonDescriptionsMap(result.matched.map((listing) => listing.sourceProductId));
  for (const listing of result.matched) {
    const description = descriptions.get(cleanText(listing.sourceProductId));
    if (description) listing.description = description;
  }

  const currentState = await readAvitoListingsFile();
  // Временные причины пропуска — не повод снимать объявление:
  //  • no_price — таймаут PriceMaster, пустой/карантинный снапшот; фид
  //    показывает последнюю сохранённую цену.
  //  • no_images — колонка images в Postgres временно пуста (напр. после
  //    деплоя с перезаписью строк); бэкфилл (backfillAvitoListingImages)
  //    постепенно восстанавливает фото из Ozon API; фид сам скрывает
  //    объявления без imageUrls.
  // Удаление при временной причине вызывает мигание (archive→re-create)
  // 700–1300 объявлений каждые 4 часа без пользы.
  // Удаляем только за структурные причины (дубль, стоп-слово, объём и т.п.).
  const TRANSIENT_SKIP_REASONS = new Set(["no_price", "no_images"]);
  const transientSkipIds = new Set(
    result.skipped
      .filter((item) => (item.reasons || []).length
        && item.reasons.every((reason) => TRANSIENT_SKIP_REASONS.has(String(reason).split(":")[0])))
      .map((item) => cleanText(item.id))
      .filter(Boolean),
  );
  let keptTransient = 0;
  const toRemoveAdIds = currentState.items
    .filter((item) => {
      const sourceProductId = cleanText(item.sourceProductId);
      if (item.source !== "ozon" || !sourceProductId || matchedIds.has(sourceProductId)) return false;
      if (transientSkipIds.has(sourceProductId)) {
        keptTransient += 1;
        return false;
      }
      return true;
    })
    .map((item) => item.adId);

  const upsertResult = await upsertAvitoListings(result.matched, { source: "ozon" });
  let removed = 0;
  if (toRemoveAdIds.length) {
    const removeResult = await removeAvitoListings(toRemoveAdIds);
    removed = removeResult.removed;
  }

  return {
    totalOzonProducts: result.totalOzonProducts,
    matchedCount: result.matched.length,
    skippedCount: result.skipped.length,
    skippedByReason: summarizeAvitoSkipReasons(result.skipped),
    matchedByCategory: summarizeAvitoMatchedByCategory(result.matched),
    created: upsertResult.created,
    updated: upsertResult.updated,
    removed,
    keptTransient,
    totalListings: upsertResult.total - removed,
  };
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
