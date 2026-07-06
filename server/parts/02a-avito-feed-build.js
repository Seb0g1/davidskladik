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

async function buildAvitoFeedXml() {
  const [state, rules] = await Promise.all([readAvitoListingsFile(), readAvitoImportRules()]);
  const enabled = state.items.filter((item) => item.enabled !== false && item.title);
  let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  xml += "<Ads formatVersion=\"3\" target=\"Avito.ru\">\n";
  for (const listing of enabled) {
    xml += buildAvitoAdXml(listing, rules.feedDefaults);
  }
  xml += "</Ads>\n";
  return { xml, count: enabled.length, total: state.items.length };
}
