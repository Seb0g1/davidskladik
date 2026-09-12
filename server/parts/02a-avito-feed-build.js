// Построение XML-фида Avito Автозагрузки (Ads formatVersion 3).
// Авито скачивает фид по публичной ссылке из настроек профиля автозагрузки.

// Коды ТН ВЭД ЕАЭС по категории объявления. Духи + туалетная вода = 3303001000.
// Подставляются автоматически если listing.tnVed не задан явно.
const AVITO_CATEGORY_TNVED = {
  "parfum-edt":        "3303001000",
  "parfum-samples":    "3303001000",
  "parfum-sets":       "3303001000",
  "parfum-oils":       "3303001000",
  "parfum-diffusers":  "3303001000",
  "parfum-atomizers":  "3303001000",
  "parfum-other":      "3303001000",
  "body-deo":          "3307200000",
  "body-soap":         "3401110000",
  "body-shower":       "3401190000",
  "body-scrubs":       "3307900000",
  "body-creams":       "3304990000",
  "body-oils":         "3304990000",
  "body-lotions":      "3304990000",
  "body-correctors":   "3304990000",
  "face-creams":       "3304990000",
  "face-masks":        "3304990000",
  "face-oils":         "3304990000",
  "face-patches":      "3304990000",
  "face-scrubs":       "3304990000",
  "face-cleansers":    "3304990000",
  "face-serums":       "3304990000",
  "face-toners":       "3304990000",
  "face-fluids":       "3304990000",
  "hair":              "3305900000",
  "care-sun":          "3304990000",
  "care-oral":         "3306100000",
  "care-hygiene":      "3307900000",
  "care-sets":         "3307900000",
  "makeup-lips":       "3304100000",
  "makeup-eyes":       "3304200000",
  "makeup-face":       "3304990000",
  "makeup-nails":      "3304300000",
  "makeup":            "3304990000",
};

// Целевой остаток для тега <Stock>. Без тега Авито считает количество = 1 и после
// первой продажи остаток остаётся 0 навсегда; с тегом каждая автозагрузка
// восстанавливает остаток до целевого значения, пока поставщик даёт цену.
const avitoFeedDefaultStock = Math.max(1, Math.min(999999, Number(process.env.AVITO_DEFAULT_STOCK || 5) || 5));

function escapeAvitoXml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function avitoXmlTag(name, value) {
  const text = cleanText(value);
  if (!text) return "";
  return `    <${name}>${escapeAvitoXml(text)}</${name}>\n`;
}

function avitoXmlCdataTag(name, value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  return `    <${name}><![CDATA[${text.replace(/\]\]>/g, "]]]]><![CDATA[>")}]]></${name}>\n`;
}

// Генерирует уникальное описание по атрибутам товара когда Ozon-описание
// отсутствует. Включает артикул поставщика как гарантию уникальности.
// Шесть вариантов фраз выбираются по хешу adId — соседние товары получают
// разные формулировки даже при одинаковых атрибутах.
function buildUniqueAvitoDescription(listing, feedDefaults = {}) {
  const title = cleanText(listing.title) || "";
  const brand = cleanText(listing.brand) || "";
  const categoryKey = cleanText(listing.categoryKey) || "";
  const extraFields = listing.extraFields || {};
  const gender = cleanText(extraFields.Gender) || "";
  const perfumeType = cleanText(extraFields.PerfumeType) || "";
  const volumeTag = cleanText(extraFields.Volume) || (listing.volumeMl > 0 ? `${listing.volumeMl} мл` : "");
  const rawArticle = cleanText(listing.sourceOfferId || listing.adId?.replace(/^oz-/, "") || "").replace(/#/g, "").trim();
  const articleLine = rawArticle ? `Артикул: ${rawArticle}.` : "";

  const adId = cleanText(listing.adId) || "";
  let h = 0;
  for (let i = 0; i < adId.length; i++) h = ((h << 5) - h + adId.charCodeAt(i)) | 0;
  const v = Math.abs(h) % 6;

  const isParfum = categoryKey.startsWith("parfum");
  const isMakeup = categoryKey.startsWith("makeup");
  const isHair = categoryKey === "hair";
  const isBody = categoryKey.startsWith("body");
  const isFace = categoryKey.startsWith("face");

  const DELIVERY = [
    "Доставляем по всей России через Авито Доставку.",
    "Отправка по России — Авито Доставка.",
    "Авито Доставка по всей стране.",
    "Быстрая отправка через Авито Доставку.",
    "Доставка в любой регион России.",
    "Работаем с Авито Доставкой по всей России.",
  ];
  const CONDITION = [
    "Товар новый, оригинал, в нераспечатанной упаковке.",
    "Новый, в фирменной упаковке производителя, подлинность гарантируется.",
    "Оригинальная продукция, новый товар, упаковка не вскрыта.",
    "Новый, в заводской упаковке, без признаков вскрытия.",
    "Товар оригинальный, новый, упаковка целая.",
    "Подлинный товар, новый, в нераспечатанной коробке.",
  ];
  const delivery = DELIVERY[v];
  const condition = CONDITION[v];

  let parts = [];

  if (isParfum) {
    // Prepositional gender phrase: "для женщин", "для мужчин", "для всех"
    // avoids adjective-noun gender agreement issues (женский/женская парфюмерная вода).
    const genderPrep = { "Женщины": "женщин", "Мужчины": "мужчин", "Унисекс": "всех" }[gender] || "";
    const typeStr = (perfumeType || "парфюм").toLowerCase();
    const BRAND_OPENERS = brand ? [
      `${brand} — парфюмерный дом с репутацией на мировом рынке.`,
      `Продукция бренда ${brand} — выбор ценителей качества.`,
      `${brand}: аромат, который говорит сам за себя.`,
      `Аромат от ${brand} — сочетание стиля и стойкости.`,
      `${brand} — один из признанных брендов в парфюмерии.`,
      `${brand}: изысканный вкус в каждом флаконе.`,
    ] : Array(6).fill("Аромат от известного производителя.");
    // Title уже содержит объём — не дублируем volumeTag в строке описания.
    const productLine = genderPrep
      ? `${title} — ${typeStr} для ${genderPrep}.`
      : `${title} — ${typeStr}.`;
    parts = [BRAND_OPENERS[v], productLine, condition, delivery, articleLine].filter(Boolean);
  } else if (isMakeup) {
    const BRAND_OPENERS = brand ? [
      `${brand} — профессиональная косметика высокого класса.`,
      `Косметика ${brand}: качество для ценителей.`,
      `${brand}: средства для макияжа от ведущего бренда.`,
      `Продукция ${brand} — выбор профессионалов и любителей.`,
      `${brand} — проверенный бренд в мире косметики.`,
      `${brand}: каждое средство — результат многолетних разработок.`,
    ] : Array(6).fill("Профессиональная косметика для макияжа.");
    parts = [BRAND_OPENERS[v], `${title}${volumeTag ? ` (${volumeTag})` : ""}.`, condition, delivery, articleLine].filter(Boolean);
  } else if (isHair) {
    const BRAND_OPENERS = brand ? [
      `${brand} — уход за волосами профессионального качества.`,
      `Средства ${brand} для волос: профессиональный результат дома.`,
      `${brand}: линейка по уходу за волосами.`,
      `Продукция ${brand} для волос — качество без компромиссов.`,
      `${brand} — надёжный бренд средств для волос.`,
      `${brand}: уход за волосами от ведущего производителя.`,
    ] : Array(6).fill("Профессиональное средство по уходу за волосами.");
    parts = [BRAND_OPENERS[v], `${title}${volumeTag ? ` (${volumeTag})` : ""}.`, condition, delivery, articleLine].filter(Boolean);
  } else if (isBody || isFace) {
    const typeStr = isBody ? "уходовое средство для тела" : "средство по уходу за лицом";
    const BRAND_OPENERS = brand ? [
      `${brand} — косметика для ухода за кожей.`,
      `Продукция ${brand}: эффективный уход за кожей.`,
      `${brand}: линейка уходовой косметики.`,
      `Средства ${brand} для ухода — качество и результат.`,
      `${brand} — бренд с репутацией в уходовой косметике.`,
      `${brand}: профессиональный уход для вашей кожи.`,
    ] : Array(6).fill(`Уходовая косметика — ${typeStr}.`);
    parts = [BRAND_OPENERS[v], `${title}${volumeTag ? ` (${volumeTag})` : ""} — ${typeStr}.`, condition, delivery, articleLine].filter(Boolean);
  } else {
    const tmpl = cleanText(feedDefaults.description || "");
    const base = tmpl.replace(/\{title\}/g, title).replace(/\{brand\}/g, brand);
    return base + (articleLine ? " " + articleLine : "");
  }

  return parts.join(" ");
}

function buildAvitoAdXml(listing, feedDefaults = {}) {
  const description = listing.description?.trim() || buildUniqueAvitoDescription(listing, feedDefaults);
  const emitted = new Set();
  let xml = "  <Ad>\n";
  const emit = (name, value) => {
    const tag = avitoXmlTag(name, value);
    if (tag) emitted.add(name);
    xml += tag;
  };
  emit("Id", listing.adId + "-r1");
  emit("Title", listing.title);
  xml += avitoXmlCdataTag("Description", description);
  emitted.add("Description");
  if (listing.priceRub > 0) emit("Price", listing.priceRub);

  // Цепочка категоризации по справочнику: выводим ровно те теги, которые есть
  // в шаблоне категории (у Парфюмерии — PerfumeryType и Condition, у «Уход и
  // гигиена» — GoodsSubType/SubType без Condition). Для старых объявлений без
  // categoryKey — прежнее поведение на feedDefaults.
  //
  // Используем точный spec по categoryKey — это единственный способ получить
  // нужный CosmeticsType для makeup-eyes/lips/face/nails. Fallback на "makeup"
  // (без CosmeticsType) вызывал «Ошибка параметра» во всех макияж-листингах.
  const specKey = listing.categoryKey;
  const spec = getAvitoCategorySpec(specKey);
  if (spec) {
    emit("Category", AVITO_FEED_CATEGORY);
    emit("GoodsType", spec.tags.GoodsType);
    if (spec.tags.GoodsSubType) emit("GoodsSubType", spec.tags.GoodsSubType);
    if (spec.tags.SubType) emit("SubType", spec.tags.SubType);
    if (spec.tags.PerfumeryType) emit("PerfumeryType", spec.tags.PerfumeryType);
    if (spec.tags.CosmeticsType) emit("CosmeticsType", spec.tags.CosmeticsType);
    emit("AdType", listing.adType || feedDefaults.adType);
    if (spec.condition) emit("Condition", listing.condition || feedDefaults.condition || "Новое");
  } else {
    emit("Category", listing.category || feedDefaults.category);
    emit("GoodsType", listing.goodsType || feedDefaults.goodsType);
    if (listing.goodsSubType) emit("GoodsSubType", listing.goodsSubType);
    if (listing.subType) emit("SubType", listing.subType);
    if (listing.perfumeryType) emit("PerfumeryType", listing.perfumeryType);
    emit("AdType", listing.adType || feedDefaults.adType);
    emit("Condition", listing.condition || feedDefaults.condition);
  }
  emit("Address", listing.address || feedDefaults.address);
  emit("Brand", listing.brand);
  const tnVed = cleanText(listing.tnVed || AVITO_CATEGORY_TNVED[listing.categoryKey] || feedDefaults.tnVed || "");
  if (tnVed) emit("TnVed", tnVed);
  if (listing.stockQuantity !== null && listing.stockQuantity !== undefined && Number.isFinite(Number(listing.stockQuantity))) {
    emit("Stock", String(Math.max(0, Math.round(Number(listing.stockQuantity)))));
  }
  if (listing.imageUrls.length) {
    xml += "    <Images>\n";
    for (const url of listing.imageUrls) {
      xml += `      <Image url="${escapeAvitoXml(url)}"/>\n`;
    }
    xml += "    </Images>\n";
  }
  for (const [tag, value] of Object.entries(listing.extraFields || {})) {
    const tagName = cleanText(tag).replace(/[^A-Za-z0-9_]/g, "");
    if (!tagName || emitted.has(tagName)) continue;
    xml += avitoXmlTag(tagName, value);
  }
  xml += "  </Ad>\n";
  return xml;
}

// Живое состояние товаров-источников: свежая цена и остаток из Postgres на
// момент сборки XML (Avito скачивает фид по расписанию — данные всегда
// актуальные без пересохранения объявлений). null = Postgres недоступен,
// используем сохранённые значения.
async function loadAvitoLiveProductStates(listings) {
  const ids = [...new Set(
    listings
      .map((item) => cleanText(item.sourceProductId))
      .filter(Boolean),
  )];
  if (!ids.length) return new Map();
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return null;
  try {
    const map = new Map();
    const chunkSize = 5000;
    for (let index = 0; index < ids.length; index += chunkSize) {
      const chunk = ids.slice(index, index + chunkSize);
      // Снимок выбранного поставщика нужен для цены «от поставщика», поэтому
      // обычного select колонок недостаточно — тянем путь из raw.
      const rows = await prisma.$queryRaw`
        SELECT id, target_price, target_stock, archived,
               raw->'selectedSupplier' AS supplier,
               COALESCE(raw->'avitoImages', '[]'::jsonb) AS avito_images
        FROM warehouse_products
        WHERE id = ANY(${chunk})
      `;
      for (const row of rows) {
        map.set(cleanText(row.id), {
          id: cleanText(row.id),
          targetPrice: Number(row.target_price || 0),
          targetStock: Number(row.target_stock || 0),
          archived: Boolean(row.archived),
          supplier: row.supplier && typeof row.supplier === "object" ? row.supplier : null,
          avitoImages: Array.isArray(row.avito_images) ? row.avito_images.map((url) => cleanText(url)).filter(Boolean) : [],
        });
      }
    }
    return map;
  } catch (error) {
    logger.warn("avito feed live state load failed, using stored prices", { detail: error?.message || String(error) });
    return null;
  }
}

// Возвращает объявление со свежей ценой и признак «нет в наличии».
//
// Авто-архив Ozon — статус маркетплейса, не физическое отсутствие товара.
// Логика outOfStock:
//  • supplier есть → outOfStock = !hasSupplierPrice (PM-цена = реальная доступность
//    для Avito: здесь дропшипинг, не FBS; targetStock отражает остаток Ozon-склада,
//    а не доступность у поставщика)
//  • supplier нет, targetStock > 0 → в наличии (физический остаток подтверждён)
//  • supplier нет, targetStock = 0 → доверяем сохранённому outOfStock из JSON
//    (рефреш фида делает живой запрос в PM и обновляет значение каждые 30 мин;
//    XML-билдер этого PM-запроса не делает — не тратим 26 запросов на каждый GET)
//  • supplier stopped → outOfStock = true
// trustStoredOutOfStock: XML builder does not run a live PM lookup — it relies on the
// outOfStock value that runAvitoFeedRefresh computed (with loadAvitoSupplierPricingMap)
// and saved to JSON. Pass true from the XML builder so we don't mark PM-supplied
// products OOS just because their DB selectedSupplier hasn't been refreshed yet.
// runAvitoFeedRefresh passes false (default) because it already resolved PM suppliers
// before calling this function, so the no-supplier branch truly means "no PM supplier".
function applyAvitoLiveState(listing, product, rules, pricing = {}, { trustStoredOutOfStock = false } = {}) {
  if (!listing.sourceProductId) return { listing, outOfStock: false };
  if (!product) return { listing: { ...listing, stockQuantity: 0 }, outOfStock: true };
  // Product-specific Avito photos override the listing's imageUrls (sourced from Ozon import).
  if (Array.isArray(product.avitoImages) && product.avitoImages.length) {
    listing = { ...listing, imageUrls: product.avitoImages };
  }
  // Остаток для <Stock>:
  // • поставщик с ценой → показываем max(targetStock, avitoFeedDefaultStock) — минимум дефолт
  //   (Avito дропшипинг: PM даёт цену → можно заказать даже при нулевом FBS-остатке)
  // • без поставщика → остаток из targetStock или сохранённый, минимум 0
  const listingStock = (outOfStock, targetStock, hasSupplier) => {
    if (outOfStock) return 0;
    const ts = Math.max(0, Math.round(Number(targetStock || 0)) || 0);
    if (hasSupplier) return Math.max(avitoFeedDefaultStock, ts);
    return Math.max(0, ts, Number(listing.stockQuantity || 0) || 0);
  };
  const withStock = (base, outOfStock, hasSupplier) => {
    const stockQuantity = listingStock(outOfStock, product.targetStock, hasSupplier);
    return stockQuantity === base.stockQuantity ? base : { ...base, stockQuantity };
  };
  if (product.supplier?.stopped) {
    return { listing: withStock(listing, true, false), outOfStock: true };
  }
  if (!product.supplier) {
    if (Number(product.targetStock || 0) > 0) {
      // Есть физический FBS-остаток — точно в наличии.
      return { listing: withStock(listing, false, false), outOfStock: false };
    }
    if (trustStoredOutOfStock) {
      // XML builder: refresh already did the live PM lookup and saved the correct
      // outOfStock. Trust it — marking true here would hide PM-supplied products
      // whose DB selectedSupplier hasn't synced yet.
      const outOfStock = listing.outOfStock === true;
      return { listing: withStock(listing, outOfStock, false), outOfStock };
    }
    // Feed refresh: loadAvitoSupplierPricingMap already ran above — if no supplier was
    // found, the product truly has no PM supplier right now → mark OOS.
    return { listing: withStock(listing, true, false), outOfStock: true };
  }
  const hasSupplierPrice = computeAvitoSupplierPriceRub(product.supplier, pricing) > 0;
  // PM-цена = доступность для Avito (дропшипинг): если поставщик даёт цену —
  // товар в наличии даже при targetStock=0 (FBS-остаток Ozon не ограничивает Avito).
  // Поставщики «Наш склад» (stock-only) цены не дают — доступность определяем
  // по физическому остатку (targetStock), а не по PM-цене.
  const isStockOnlySupplier = supplierUsesStockOnlyPricing(null, product.supplier);
  const outOfStock = isStockOnlySupplier
    ? Number(product.targetStock || 0) <= 0
    : !hasSupplierPrice;
  if (!rules.autoUpdatePrices) return { listing: withStock(listing, outOfStock, hasSupplierPrice), outOfStock };
  const markupOverride = Number(listing.markupCoefficient) > 0 ? Number(listing.markupCoefficient) : 0;
  const priceRub = resolveAvitoListingPriceRub(product, product.supplier, rules, pricing, markupOverride) || listing.priceRub;
  const nextListing = priceRub === listing.priceRub ? listing : { ...listing, priceRub };
  return { listing: withStock(nextListing, outOfStock, hasSupplierPrice), outOfStock };
}

// CSV с остатками для раздела Avito «Управление остатками» (способ загрузки
// «Автоматический»: Авито скачивает файл по ссылке раз в час). Формат из
// справки Авито: строка «#date,<время>», затем колонки Id,Stock — Id совпадает
// с Id объявления из фида автозагрузки, Stock 0 снимает объявление с продажи.
function avitoStockCsvCell(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}

function renderAvitoStockCsv(listings, rules, liveStates, pricing, now = new Date()) {
  const pad = (part) => String(part).padStart(2, "0");
  const stamp = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}T${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  let csv = `#date,${stamp}\nId,Stock\n`;
  let count = 0;
  let outOfStockCount = 0;
  for (const item of listings) {
    const { listing, outOfStock } = liveStates === null
      ? { listing: item, outOfStock: item.outOfStock === true }
      : applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules, pricing, { trustStoredOutOfStock: true });
    const hasQuantity = listing.stockQuantity !== null && listing.stockQuantity !== undefined
      && Number.isFinite(Number(listing.stockQuantity));
    const stock = outOfStock
      ? 0
      : hasQuantity ? Math.max(0, Math.round(Number(listing.stockQuantity))) : avitoFeedDefaultStock;
    if (stock <= 0) outOfStockCount += 1;
    csv += `${avitoStockCsvCell(listing.adId)},${stock}\n`;
    count += 1;
  }
  return { csv, count, outOfStock: outOfStockCount };
}

async function buildAvitoStockCsv() {
  const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
  const enabled = state.items.filter((item) => item.enabled !== false && item.title);
  const liveStates = await loadAvitoLiveProductStates(enabled);
  // Прайсинг нужен applyAvitoLiveState для признака «нет в наличии» (поставщик
  // без цены = нет товара), поэтому грузим его всегда при живых данных, а не
  // только при autoUpdatePrices, как в XML.
  const pricing = liveStates ? await loadAvitoPricingContext() : {};
  return {
    ...renderAvitoStockCsv(enabled, rules, liveStates, pricing),
    liveSource: liveStates === null ? "stored" : "postgres",
  };
}

// Читает список старых adId (формат oz-XXXX-r1) для включения в фид как
// Status=Удалено — это единственный способ удалить autoupload-объявления из
// Avito: core API /archive на них не работает, нужен явный тег в XML.
// Файл data/avito-old-adids.json формируется скриптом сбора adId через API.
async function loadAvitoOldAdIdsForDeletion() {
  const deletionPath = path.join(dataDir, "avito-old-adids.json");
  try {
    const raw = await fs.readFile(deletionPath, "utf8");
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(Boolean) : [];
  } catch {
    return [];
  }
}

async function buildAvitoFeedXml() {
  const [state, rules, oldAdIds] = await Promise.all([
    readAvitoListingsFile(),
    readAvitoImportRules(),
    loadAvitoOldAdIdsForDeletion(),
  ]);
  const enabled = state.items.filter((item) => item.enabled !== false && item.title);
  const liveStates = rules.autoUpdatePrices || rules.hideOutOfStock
    ? await loadAvitoLiveProductStates(enabled)
    : new Map();
  const pricing = liveStates && rules.autoUpdatePrices ? await loadAvitoPricingContext() : {};
  let hiddenOutOfStock = 0;
  let hiddenNoImages = 0;
  let hiddenDuplicates = 0;
  // Страховка от повторов прямо на отдаче XML: один товар склада и одно
  // название = одно объявление, даже если в файле листингов остались дубли.
  const seenSourceProductIds = new Set();
  const seenTitleKeys = new Set();
  // Собираем части в массив и join'им в конце — O(n) вместо O(n²) строкового +=.
  const parts = ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n", "<Ads formatVersion=\"3\" target=\"Avito.ru\">\n"];
  let count = 0;
  for (const item of enabled) {
    // Без живых данных (Postgres недоступен) полагаемся на сохранённый флаг
    // outOfStock из фонового рефреша.
    const { listing, outOfStock } = liveStates === null
      ? { listing: item, outOfStock: item.outOfStock === true }
      : applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules, pricing, { trustStoredOutOfStock: true });
    if (rules.hideOutOfStock && outOfStock) {
      hiddenOutOfStock += 1;
      continue;
    }
    const sourceProductId = cleanText(listing.sourceProductId);
    const titleKey = cleanText(listing.title).toLowerCase().replace(/ё/g, "е");
    if ((sourceProductId && seenSourceProductIds.has(sourceProductId)) || (titleKey && seenTitleKeys.has(titleKey))) {
      hiddenDuplicates += 1;
      continue;
    }
    // Avito отклоняет объявления без фото и может завалить всю загрузку —
    // товар без картинок не публикуем, фоновый бэкфилл фото вернёт его в фид.
    if (!listing.imageUrls.length) {
      hiddenNoImages += 1;
      continue;
    }
    if (sourceProductId) seenSourceProductIds.add(sourceProductId);
    if (titleKey) seenTitleKeys.add(titleKey);
    parts.push(buildAvitoAdXml(listing, rules.feedDefaults));
    count += 1;
  }
  // Включаем старые adId (формат oz-XXXX-r1) с Status=Удалено — Avito удалит
  // их из системы, что снимет блокировку «Повторное размещение» для новых
  // объявлений с теми же товарами. После обработки Avito'м файл можно очистить.
  const activeAdIds = new Set(state.items.map((item) => cleanText(item.adId) + "-r1").filter(Boolean));
  let deletedCount = 0;
  for (const oldAdId of oldAdIds) {
    if (!oldAdId || activeAdIds.has(oldAdId)) continue; // не удаляем активные
    const safeId = oldAdId.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    parts.push(`<Ad><Id>${safeId}</Id><Status>Удалено</Status></Ad>\n`);
    deletedCount += 1;
  }
  parts.push("</Ads>\n");
  const xml = parts.join("");
  return {
    xml,
    count,
    total: state.items.length,
    hiddenOutOfStock,
    hiddenNoImages,
    hiddenDuplicates,
    deletedOldIds: deletedCount,
    liveSource: liveStates === null ? "stored" : "postgres",
  };
}
