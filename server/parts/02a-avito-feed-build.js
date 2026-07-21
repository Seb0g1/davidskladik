// Построение XML-фида Avito Автозагрузки (Ads formatVersion 3).
// Авито скачивает фид по публичной ссылке из настроек профиля автозагрузки.

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

function buildAvitoAdXml(listing, feedDefaults = {}) {
  const description = listing.description
    || cleanText(feedDefaults.description)
      .replace(/\{title\}/g, listing.title || "")
      .replace(/\{brand\}/g, listing.brand || "");
  const emitted = new Set();
  let xml = "  <Ad>\n";
  const emit = (name, value) => {
    const tag = avitoXmlTag(name, value);
    if (tag) emitted.add(name);
    xml += tag;
  };
  emit("Id", listing.adId);
  emit("Title", listing.title);
  xml += avitoXmlCdataTag("Description", description);
  emitted.add("Description");
  if (listing.priceRub > 0) emit("Price", listing.priceRub);

  // Цепочка категоризации по справочнику: выводим ровно те теги, которые есть
  // в шаблоне категории (у Парфюмерии — PerfumeryType и Condition, у «Уход и
  // гигиена» — GoodsSubType/SubType без Condition). Для старых объявлений без
  // categoryKey — прежнее поведение на feedDefaults.
  const spec = getAvitoCategorySpec(listing.categoryKey);
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
//  • supplier есть → hasSupplierPrice: true = в наличии, false = archived||stock<=0
//  • supplier нет  → доверяем outOfStock из файла (рефреш каждые 30 мин делает
//    живой запрос в PriceMaster и сохраняет актуальное значение)
function applyAvitoLiveState(listing, product, rules, pricing = {}) {
  if (!listing.sourceProductId) return { listing, outOfStock: false };
  if (!product) return { listing: { ...listing, stockQuantity: 0 }, outOfStock: true };
  // Product-specific Avito photos override the listing's imageUrls (sourced from Ozon import).
  if (Array.isArray(product.avitoImages) && product.avitoImages.length) {
    listing = { ...listing, imageUrls: product.avitoImages };
  }
  // Остаток для <Stock>: поставщик с ценой → целевой (5) — продажа на Avito не
  // уменьшает физический склад, автозагрузка восстанавливает количество; без
  // поставщика — остаток склада, «нет в наличии» → 0.
  const listingStock = (outOfStock, targetStock, hasSupplier) => {
    if (outOfStock) return 0;
    if (hasSupplier) return Math.max(avitoFeedDefaultStock, Math.round(Number(targetStock || 0)) || 0);
    return Math.max(1, Math.round(Number(targetStock || 0)) || 0, Number(listing.stockQuantity || 0) || 0);
  };
  const withStock = (base, outOfStock, hasSupplier) => {
    const stockQuantity = listingStock(outOfStock, product.targetStock, hasSupplier);
    return stockQuantity === base.stockQuantity ? base : { ...base, stockQuantity };
  };
  if (!product.supplier) {
    const outOfStock = listing.outOfStock === true;
    return { listing: withStock(listing, outOfStock, false), outOfStock };
  }
  const hasSupplierPrice = computeAvitoSupplierPriceRub(product.supplier, pricing) > 0;
  const outOfStock = hasSupplierPrice
    ? false
    : (Boolean(product.archived) || Number(product.targetStock || 0) <= 0);
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
      : applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules, pricing);
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

async function buildAvitoFeedXml() {
  const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
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
  let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml += "<Ads formatVersion=\"3\" target=\"Avito.ru\">\n";
  let count = 0;
  for (const item of enabled) {
    // Без живых данных (Postgres недоступен) полагаемся на сохранённый флаг
    // outOfStock из фонового рефреша.
    const { listing, outOfStock } = liveStates === null
      ? { listing: item, outOfStock: item.outOfStock === true }
      : applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules, pricing);
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
    xml += buildAvitoAdXml(listing, rules.feedDefaults);
    count += 1;
  }
  xml += "</Ads>\n";
  return {
    xml,
    count,
    total: state.items.length,
    hiddenOutOfStock,
    hiddenNoImages,
    hiddenDuplicates,
    liveSource: liveStates === null ? "stored" : "postgres",
  };
}
