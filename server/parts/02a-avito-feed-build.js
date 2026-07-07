// Построение XML-фида Avito Автозагрузки (Ads formatVersion 3).
// Авито скачивает фид по публичной ссылке из настроек профиля автозагрузки.

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
  let xml = "  <Ad>\n";
  xml += avitoXmlTag("Id", listing.adId);
  xml += avitoXmlTag("Title", listing.title);
  xml += avitoXmlCdataTag("Description", description);
  if (listing.priceRub > 0) xml += avitoXmlTag("Price", listing.priceRub);
  xml += avitoXmlTag("Category", listing.category || feedDefaults.category);
  xml += avitoXmlTag("GoodsType", listing.goodsType || feedDefaults.goodsType);
  xml += avitoXmlTag("AdType", listing.adType || feedDefaults.adType);
  xml += avitoXmlTag("Condition", listing.condition || feedDefaults.condition);
  xml += avitoXmlTag("Address", listing.address || feedDefaults.address);
  xml += avitoXmlTag("Brand", listing.brand);
  if (listing.imageUrls.length) {
    xml += "    <Images>\n";
    for (const url of listing.imageUrls) {
      xml += `      <Image url="${escapeAvitoXml(url)}"/>\n`;
    }
    xml += "    </Images>\n";
  }
  for (const [tag, value] of Object.entries(listing.extraFields || {})) {
    const tagName = cleanText(tag).replace(/[^A-Za-z0-9_]/g, "");
    if (!tagName) continue;
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
    const rows = await prisma.warehouseProduct.findMany({
      where: { id: { in: ids } },
      select: { id: true, targetPrice: true, currentPrice: true, targetStock: true, archived: true },
    });
    return new Map(rows.map((row) => [row.id, row]));
  } catch (error) {
    logger.warn("avito feed live state load failed, using stored prices", { detail: error?.message || String(error) });
    return null;
  }
}

// Возвращает объявление со свежей ценой и признак «нет в наличии».
// Товар, удалённый со склада, считается отсутствующим в наличии.
function applyAvitoLiveState(listing, product, rules) {
  if (!listing.sourceProductId) return { listing, outOfStock: false };
  if (!product) return { listing, outOfStock: true };
  const outOfStock = Boolean(product.archived) || Number(product.targetStock || 0) <= 0;
  if (!rules.autoUpdatePrices) return { listing, outOfStock };
  const basePriceRub = Number(product.targetPrice || product.currentPrice || 0);
  const priceRub = basePriceRub > 0 ? Math.round(basePriceRub * avitoPriceCoefficientFor(basePriceRub, rules)) : listing.priceRub;
  return { listing: priceRub === listing.priceRub ? listing : { ...listing, priceRub }, outOfStock };
}

async function buildAvitoFeedXml() {
  const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
  const enabled = state.items.filter((item) => item.enabled !== false && item.title);
  const liveStates = rules.autoUpdatePrices || rules.hideOutOfStock
    ? await loadAvitoLiveProductStates(enabled)
    : new Map();
  let hiddenOutOfStock = 0;
  let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml += "<Ads formatVersion=\"3\" target=\"Avito.ru\">\n";
  let count = 0;
  for (const item of enabled) {
    // Без живых данных (Postgres недоступен) полагаемся на сохранённый флаг
    // outOfStock из фонового рефреша.
    const { listing, outOfStock } = liveStates === null
      ? { listing: item, outOfStock: item.outOfStock === true }
      : applyAvitoLiveState(item, liveStates.get(cleanText(item.sourceProductId)), rules);
    if (rules.hideOutOfStock && outOfStock) {
      hiddenOutOfStock += 1;
      continue;
    }
    xml += buildAvitoAdXml(listing, rules.feedDefaults);
    count += 1;
  }
  xml += "</Ads>\n";
  return {
    xml,
    count,
    total: state.items.length,
    hiddenOutOfStock,
    liveSource: liveStates === null ? "stored" : "postgres",
  };
}
