
function marketplaceTargets() {
  const ozonAccounts = getOzonAccounts();
  const yandexShops = getYandexShops();
  return [
    ...(ozonAccounts.length
      ? ozonAccounts.map((account) => ({
          id: account.id,
          marketplace: "ozon",
          name: account.name || "Ozon",
          configured: Boolean(account.clientId && account.apiKey),
          source: account.source,
          readOnly: Boolean(account.readOnly),
        }))
      : [{ id: "ozon", marketplace: "ozon", name: "Ozon", configured: false }]),
    ...(yandexShops.length
      ? yandexShops.map((shop) => ({
          id: shop.id,
          marketplace: "yandex",
          name: shop.name || "Yandex Market",
          businessId: shop.businessId,
          configured: Boolean(shop.apiKey && shop.businessId),
          source: shop.source,
          readOnly: Boolean(shop.readOnly),
        }))
      : []),
  ];
}

function targetById(targetId) {
  if (targetId === "ozon") {
    const [account] = getOzonAccounts();
    if (account) return { id: account.id, marketplace: "ozon", name: account.name || "Ozon" };
  }
  if (targetId === "yandex") {
    const [shop] = getYandexShops();
    if (shop) return { id: shop.id, marketplace: "yandex", name: shop.name || "Yandex Market", businessId: shop.businessId };
  }
  return marketplaceTargets().find((target) => target.id === targetId) || null;
}

function isGenericMarketplaceTargetName(name, marketplace = "") {
  const value = cleanText(name).toLowerCase();
  if (!value) return true;
  if (value === cleanText(marketplace).toLowerCase()) return true;
  return value === "ozon" || value === "yandex" || value === "yandex market" || value === "marketplace";
}

function isOpaqueMarketplaceTargetId(target = "", marketplace = "") {
  const value = cleanText(target).toLowerCase();
  if (!value) return false;
  if (value === "yandex" || value === "ozon") return true;
  // Internal ids: yandex-06c2112c, ozon-ecdad851b0b1383068f320b7
  return /^yandex-[0-9a-f]{8,}$/i.test(value) || /^ozon-[0-9a-f]{8,}$/i.test(value);
}

function resolveWarehouseProductTargetName(input = {}) {
  const target = cleanText(input.target || input.marketplace || "");
  const marketplace = cleanText(input.marketplace || "").toLowerCase();
  const exports = input.exports && typeof input.exports === "object" && !Array.isArray(input.exports) ? input.exports : {};
  const exportState = exports[target] || exports[marketplace];
  const exportName = cleanText(exportState?.targetName || "");
  if (exportName && !isGenericMarketplaceTargetName(exportName, marketplace)) return exportName;
  const meta = targetById(target);
  if (meta?.name && !isGenericMarketplaceTargetName(meta.name, marketplace) && !isOpaqueMarketplaceTargetId(meta.name, marketplace)) return meta.name;
  if (target && !isGenericMarketplaceTargetName(target, marketplace) && !isOpaqueMarketplaceTargetId(target, marketplace)) return target;
  return meta?.name || exportName || cleanText(input.targetName) || (marketplace === "yandex" ? "Yandex Market" : "Ozon");
}

function isWarehouseProductTargetEnabled(product = {}) {
  const marketplace = cleanText(product.marketplace || "").toLowerCase();
  if (!marketplace) return true;
  if (marketplace === "yandex") {
    const target = cleanText(product.target).toLowerCase();
    const accounts = getYandexShops({ includeSyncDisabled: true });
    if (!accounts.length) return true;
    if (target === "yandex" || target.startsWith("yandex")) return true;
    return accounts.some((account) => matchesYandexTarget(product.target, account.id));
  }
  const accounts = getOzonAccounts();
  if (!accounts.length) return marketplace !== "yandex";
  return accounts.some((account) => (
    matchesOzonTarget(product.target, account.id)
  ));
}

function supplierPriceIsRubNative(supplier = {}) {
  return supplierUsesRubPriceMasterPricing(supplier);
}

function calculateRubPrice(basePrice, usdRate, markupCoefficient, context = null) {
  const markup = Number(markupCoefficient || 0);
  const ctx = context && typeof context === "object" ? context : {};
  const rubNative = ctx.rubNative === true || supplierPriceIsRubNative(ctx);
  const price = Number(basePrice || 0);
  if (rubNative) {
    const rubBase = Number(ctx.originalPrice ?? ctx.purchaseRubPrice ?? price);
    const defaultRate = Number(process.env.DEFAULT_USD_RATE || 95) || 95;
    const rate = Number(usdRate || 0) > 0 ? Number(usdRate) : defaultRate;
    return roundPrice((rubBase / defaultRate) * rate * markup);
  }
  return roundPrice(price * Number(usdRate || 0) * markup);
}

function normalizePriceMasterPrice(rawPrice, usdRate, currency = "USD") {
  const originalPrice = Number(rawPrice || 0);
  const mode = cleanText(currency || "USD").toUpperCase();
  const isRub = mode === "RUB" || mode === "RUR";
  const price = originalPrice;
  return {
    price: Number(Number(price || 0).toFixed(4)),
    originalPrice,
    sourceCurrency: isRub ? "RUB" : "USD",
    convertedFromRub: Boolean(isRub),
    priceCurrency: isRub ? "RUB" : "USD",
  };
}

function cleanText(value) {
  return String(value || "").trim();
}

function extractBrandFromAttributes(attributes = []) {
  for (const attribute of attributes || []) {
    const id = Number(attribute.id || attribute.attribute_id || 0);
    const name = cleanText(attribute.name || attribute.attribute_name || attribute.attributeName).toLowerCase();
    if (id !== 85 && name !== "бренд" && name !== "brand") continue;
    const values = Array.isArray(attribute.values) ? attribute.values : [];
    const fromValues = values
      .map((item) => cleanText(item.value || item.name || item.text))
      .find(Boolean);
    return fromValues || cleanText(attribute.value);
  }
  return "";
}

function extractBrandFromNestedAttributes(value = {}, depth = 0) {
  if (!value || depth > 6) return "";
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = extractBrandFromNestedAttributes(item, depth + 1);
      if (found) return found;
    }
    return "";
  }
  if (typeof value !== "object") return "";
  const directName = cleanText(value.name || value.attribute_name || value.attributeName || value.title).toLowerCase();
  const directId = Number(value.id || value.attribute_id || value.attributeId || 0);
  if (directId === 85 || directName === "бренд" || directName === "brand" || directName.includes("brand")) {
    const values = Array.isArray(value.values) ? value.values : [];
    const fromValues = values
      .map((item) => cleanText(item?.value || item?.name || item?.text))
      .find(Boolean);
    const directValue = cleanText(value.value || value.text);
    if (fromValues || directValue) return fromValues || directValue;
  }
  for (const [key, child] of Object.entries(value)) {
    const normalizedKey = normalizeSearchText(key);
    if (normalizedKey.includes("brand") || normalizedKey.includes("vendor") || normalizedKey.includes("бренд")) {
      if (typeof child === "string" || typeof child === "number") {
        const text = cleanText(child);
        if (text) return text;
      }
    }
    const found = extractBrandFromNestedAttributes(child, depth + 1);
    if (found) return found;
  }
  return "";
}

function flattenAttributeText(attributes = []) {
  return (attributes || [])
    .flatMap((attribute) => [
      attribute?.name,
      attribute?.attribute_name,
      attribute?.attributeName,
      attribute?.value,
      ...(Array.isArray(attribute?.values) ? attribute.values.flatMap((item) => [item?.value, item?.name, item?.text]) : []),
    ])
    .map(cleanText)
    .filter(Boolean)
    .join(" ");
}

function collectWarehouseBrandCandidates(value, { depth = 0, key = "" } = {}) {
  if (depth > 8 || value === null || value === undefined) return [];
  const normalizedKey = normalizeSearchText(key);
  const keyLooksLikeBrand =
    normalizedKey.includes("brand")
    || normalizedKey.includes("vendor")
    || normalizedKey.includes("manufacturer")
    || normalizedKey.includes("trademark")
    || normalizedKey.includes("бренд")
    || normalizedKey.includes("производитель");
  if (typeof value === "string" || typeof value === "number") {
    const text = cleanText(value);
    return keyLooksLikeBrand && text ? [text] : [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => collectWarehouseBrandCandidates(item, { depth: depth + 1, key }));
  }
  if (typeof value !== "object") return [];
  const direct = [];
  if (keyLooksLikeBrand) {
    const fromNamedValue = cleanText(value.value || value.text || value.name || value.title);
    if (fromNamedValue) direct.push(fromNamedValue);
  }
  if (Array.isArray(value.values)) {
    const attributeName = normalizeSearchText(value.name || value.attribute_name || value.attributeName || value.id || value.attributeId || key);
    const attributeLooksLikeBrand = attributeName.includes("brand") || attributeName.includes("vendor") || attributeName.includes("бренд") || attributeName.includes("производитель");
    if (attributeLooksLikeBrand) {
      direct.push(...value.values.flatMap((item) => [item?.value, item?.name, item?.text]).map(cleanText).filter(Boolean));
    }
  }
  return [
    ...direct,
    ...Object.entries(value).flatMap(([childKey, childValue]) => collectWarehouseBrandCandidates(childValue, { depth: depth + 1, key: childKey })),
  ];
}

function resolveWarehouseBrandCandidates(product = {}) {
  return Array.from(new Map([
    product.brand,
    product.vendor,
    product.brandName,
    product.raw?.brand,
    product.raw?.vendor,
    product.raw?.brandName,
    product.ozon?.vendor,
    product.ozon?.brand,
    product.ozon?.brandName,
    product.yandex?.vendor,
    product.yandex?.brand,
    product.yandex?.brandName,
    extractBrandFromAttributes(product.ozon?.attributes),
    extractBrandFromAttributes(product.yandex?.attributes),
    extractBrandFromNestedAttributes(product.ozon),
    extractBrandFromNestedAttributes(product.yandex),
    extractBrandFromNestedAttributes(product.rawPayload),
    extractBrandFromNestedAttributes(product.raw),
  ].map(cleanText).filter(Boolean).map((item) => [item.toLowerCase(), item])).values());
}

function resolveWarehouseBrand(product = {}) {
  return resolveWarehouseBrandCandidates(product)[0] || "";
}

function normalizedBrandIndexKey(value = "") {
  return normalizeSearchText(value).replace(/\s+/g, " ").trim();
}

function warehouseBrandIndexRowsForProduct(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const candidates = [];
  const push = (value, source, confidence = 80) => {
    const displayBrand = cleanText(value);
    const normalizedBrand = normalizedBrandIndexKey(displayBrand);
    if (!displayBrand || !normalizedBrand || normalizedBrand.length < 2) return;
    candidates.push({
      normalizedBrand,
      displayBrand,
      productId: normalized.id,
      marketplace: normalized.marketplace === "yandex" ? "yandex" : "ozon",
      offerId: normalized.offerId || normalized.id,
      source,
      confidence,
    });
  };
  push(normalized.brand, "brand", 100);
  push(normalized.vendor, "vendor", 95);
  push(normalized.brandName, "brand_name", 95);
  push(normalized.raw?.brand, "raw", 90);
  push(normalized.raw?.vendor, "raw", 88);
  push(normalized.ozon?.brand || normalized.ozon?.vendor || normalized.ozon?.brandName, "ozon_attribute", 90);
  push(normalized.yandex?.brand || normalized.yandex?.vendor || normalized.yandex?.brandName, "yandex_attribute", 90);
  for (const brand of resolveWarehouseBrandCandidates(normalized)) push(brand, "raw", 80);
  const unique = new Map();
  for (const row of candidates) {
    const key = `${row.normalizedBrand}|${row.productId}|${row.source}`;
    if (!unique.has(key)) unique.set(key, row);
  }
  return Array.from(unique.values());
}
