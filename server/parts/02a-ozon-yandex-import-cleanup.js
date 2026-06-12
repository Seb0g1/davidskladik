// Sets/kits are sellable on Yandex even when a bundled item is under 20ml ("Парфюмерный
// набор 100 мл + 10 мл", "набор кремов", "набор средств"). The whitelist OVERRIDES the
// small-volume block. "Без коробки" must NOT be on Yandex at all (delete + import block).
const YANDEX_SET_KEYWORDS = ["парфюмерный набор", "подарочный набор", "набор средств", "набор кремов", "набор косметики", "набор миниатюр", "gift set", " набор "];
const YANDEX_NO_BOX_RE = /без\s+коробк/iu;

function isYandexSetProduct(textOrProduct = {}) {
  const text = (typeof textOrProduct === "string" ? textOrProduct : collectYandexVolumeSearchText(textOrProduct)).toLowerCase();
  if (!text) return false;
  // Pad with spaces so " набор " word-boundary check works at string ends too.
  const padded = ` ${text} `;
  return YANDEX_SET_KEYWORDS.some((keyword) => padded.includes(keyword));
}

function isYandexNoBoxProduct(textOrProduct = {}) {
  const text = typeof textOrProduct === "string" ? textOrProduct : collectYandexVolumeSearchText(textOrProduct);
  return YANDEX_NO_BOX_RE.test(text || "");
}

function extractOzonYandexImportVolumesMl(name = "") {
  const raw = cleanText(name);
  if (!raw) return [];
  const text = raw.replace(/(\d),(\d)/g, "$1.$2");
  const volumes = [];
  const seen = new Set();
  const add = (value) => {
    const number = Number(value);
    if (!Number.isFinite(number) || number <= 0) return;
    const key = String(number);
    if (seen.has(key)) return;
    seen.add(key);
    volumes.push(number);
  };
  const mlSuffix = "(?:мл|ml)(?![a-zа-яё])";
  // travel-set handled outside the exec loop: a non-global regex inside while(exec)
  // never advances lastIndex and spins the event loop forever (froze api+worker).
  if (/travel\s*set/iu.test(text)) add(10);
  const patterns = [
    new RegExp(`(\\d+(?:\\.\\d+)?)\\s*${mlSuffix}`, "giu"),
    new RegExp(`(\\d+(?:\\.\\d+)?)${mlSuffix}`, "giu"),
    new RegExp(`(\\d+)\\s*[xх×]\\s*(\\d+(?:\\.\\d+)?)\\s*${mlSuffix}`, "giu"),
  ];
  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(text))) {
      if (pattern.source.includes("[xх×]")) add(match[2]);
      else add(match[1]);
    }
  }
  return volumes;
}

function collectYandexVolumeSearchText(product = {}) {
  if (typeof product === "string") return cleanText(product);
  const normalized = normalizeWarehouseProduct(product);
  return [
    normalized.name,
    normalized.offerId,
    normalized.ozon?.name,
    normalized.ozon?.description,
    normalized.yandex?.name,
    normalized.yandex?.description,
    flattenAttributeText(normalized.ozon?.attributes),
    flattenAttributeText(normalized.yandex?.attributes),
  ].map(cleanText).filter(Boolean).join(" ");
}

function assessYandexSmallVolume(productOrText = {}) {
  const searchText = collectYandexVolumeSearchText(productOrText);
  const volumesMl = extractOzonYandexImportVolumesMl(searchText);
  const minVolumeMl = volumesMl.length ? Math.min(...volumesMl) : null;
  const maxVolumeMl = volumesMl.length ? Math.max(...volumesMl) : null;
  // Block by the LARGEST volume: "50 мл + 10 мл" sets are full products with a bundled
  // sampler and must not be blocked/deleted because of the 10ml part. Named sets
  // ("Парфюмерный набор", "набор кремов" …) are always allowed regardless of volume.
  const isSet = isYandexSetProduct(searchText);
  const blocked = !isSet && maxVolumeMl !== null && maxVolumeMl < YANDEX_MIN_VOLUME_ML;
  const smallVolumes = volumesMl.filter((value) => value < YANDEX_MIN_VOLUME_ML);
  return {
    blocked,
    minVolumeMl,
    maxVolumeMl,
    volumesMl,
    searchText,
    reason: blocked ? `Объем меньше ${YANDEX_MIN_VOLUME_ML} мл: ${smallVolumes.join(", ")} мл` : "",
  };
}

function isYandexSmallVolumeBlocked(productOrText = {}) {
  return assessYandexSmallVolume(productOrText).blocked;
}

function ozonYandexImportBlockReasons(product = {}) {
  const name = cleanText(product.name || product.ozon?.name || product.offerId);
  const lower = name.toLowerCase();
  const reasons = [];
  const vendor = cleanText(product.ozon?.vendor || product.yandex?.vendor || product.brand || "");
  const vendorLower = vendor.toLowerCase();
  const marketplaceState = product.marketplaceState || {};
  const stateCode = cleanText(marketplaceState.code).toLowerCase();
  const stateVisibility = cleanText(marketplaceState.visibility).toUpperCase();
  const rawState = cleanText(marketplaceState.state).toUpperCase();
  if (["inactive", "unknown"].includes(stateCode) || ["REMOVED_FROM_SALE", "DISABLED", "BANNED"].includes(stateVisibility) || ["REMOVED_FROM_SALE", "DISABLED", "BANNED"].includes(rawState)) {
    reasons.push("Товар неактивен или статус Ozon не подтвержден");
  }
  if (lower.includes("отливант")) reasons.push("Название содержит «Отливант»");
  if (/без\s+коробк/iu.test(lower)) reasons.push("Название содержит «без коробки»");
  const volumeText = collectYandexVolumeSearchText(product);
  const smallVolumeCheck = assessYandexSmallVolume(volumeText);
  if (smallVolumeCheck.blocked) reasons.push(smallVolumeCheck.reason);
  const volumes = extractOzonYandexImportVolumesMl(volumeText);
  if (!volumes.length) reasons.push("В названии нет объема в мл");
  const hugeVolumes = extractOzonYandexImportVolumesMl(volumeText).filter((value) => value > 500);
  if (hugeVolumes.length || /\d+\s+\d{3}\s*(?:мл|ml)(?![a-zа-я])/iu.test(name)) {
    reasons.push(`Подозрительный объем${hugeVolumes.length ? `: ${hugeVolumes.join(", ")} мл` : ""}`);
  }
  const blockedCategories = [
    "свеч",
    "помад",
    "шампун",
    "бальзам",
    "крем",
    "маск",
    "гель",
    "лак",
    "краск",
    "стик",
    "stick",
    "дезодорант",
  ];
  const matchedCategory = blockedCategories.find((word) => lower.includes(word));
  if (matchedCategory) reasons.push("Категория не подходит для импорта парфюмерии");
  const genericNames = [
    "парфюмерная вода",
    "парфюмерная вода для мужчин",
    "туалетная вода",
    "духи",
  ];
  if ((!vendor || vendorLower.includes("без бренда")) && genericNames.some((genericName) => lower === genericName || lower.startsWith(`${genericName} `))) {
    reasons.push("Слишком общее название без бренда");
  }
  const meaningfulWords = name.match(/[a-zа-яё]{3,}/giu) || [];
  const wordsWithVowels = meaningfulWords.filter((word) => /[aeiouyаеёиоуыэюя]/iu.test(word));
  const alphaCount = (name.match(/[a-zа-яё]/giu) || []).length;
  if (name.length < 10 || alphaCount < 3 || (meaningfulWords.length > 0 && wordsWithVowels.length === 0)) {
    reasons.push("Подозрительное название товара");
  }
  return reasons;
}

function buildOzonYandexImportCandidate(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const built = buildYandexOfferMapping(normalized);
  const blockReasons = ozonYandexImportBlockReasons(normalized);
  const missing = Array.isArray(built.missing) ? built.missing : [];
  const yandexExistingOfferIds = options.yandexExistingOfferIds instanceof Set ? options.yandexExistingOfferIds : new Set();
  const offerIdKey = cleanText(normalized.offerId).toLowerCase();
  const existingInYandex = Boolean(offerIdKey && yandexExistingOfferIds.has(offerIdKey));
  const imageUrl = normalized.imageUrl || firstImageUrl(ozon.primaryImage || ozon.images || normalized.images);
  return {
    id: normalized.id,
    marketplace: normalized.marketplace,
    target: normalized.target,
    offerId: normalized.offerId,
    productId: normalized.productId,
    sku: normalized.sku || ozon.sku || "",
    name: normalized.name || ozon.name || normalized.offerId || "",
    vendor: built.offer?.vendor || ozon.vendor || "",
    imageUrl,
    price: Number(built.offer?.basicPrice?.value || normalized.marketplacePrice || ozon.price || 0) || null,
    categoryId: built.offer?.marketCategoryId || null,
    picturesCount: Array.isArray(built.offer?.pictures) ? built.offer.pictures.length : 0,
    hasDescription: Boolean(built.offer?.description),
    blockReasons,
    missing,
    existingInYandex,
    yandexReady: Boolean(built.ready),
    eligible: !existingInYandex && !blockReasons.length && Boolean(built.ready),
    offerPreview: built.offer,
  };
}

function summarizeOzonYandexImportPreview(rows = []) {
  return {
    total: rows.length,
    eligible: rows.filter((row) => row.eligible).length,
    blocked: rows.filter((row) => row.blockReasons?.length).length,
    existingInYandex: rows.filter((row) => row.existingInYandex).length,
    missingRequired: rows.filter((row) => !row.yandexReady).length,
  };
}

function parseProtectedBrandList(input) {
  const values = Array.isArray(input)
    ? input
    : String(input || "").split(/[\n,;]+/u);
  return Array.from(new Set(values
    .map((item) => cleanText(item))
    .filter((item) => item.length >= 2)));
}

function normalizeBrandSearchText(value) {
  return cleanText(value)
    .toLowerCase()
    .replaceAll("ё", "е")
    .replace(/[^0-9a-zа-я]+/giu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function collectYandexOfferTextParts(value, parts = [], depth = 0) {
  if (value == null || depth > 6) return parts;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    const text = cleanText(value);
    if (text) parts.push(text);
    return parts;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectYandexOfferTextParts(item, parts, depth + 1);
    return parts;
  }
  if (typeof value === "object") {
    for (const item of Object.values(value)) collectYandexOfferTextParts(item, parts, depth + 1);
  }
  return parts;
}

function buildYandexCleanupCandidate(item = {}, shop = {}, protectedBrandsInput = []) {
  const offer = pickYandexOfferFromMapping(item);
  const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
  const name = cleanText(offer.name || item.offer?.name || offerId);
  const vendor = cleanText(offer.vendor || item.offer?.vendor || item.mapping?.vendor);
  const state = pickYandexState(item, offer);
  const archived = state.code === "archived" || Boolean(offer.archived || item.archived || item.offer?.archived);
  const brands = parseProtectedBrandList(protectedBrandsInput);
  const searchableTextRaw = collectYandexOfferTextParts(item).join(" ");
  const searchText = normalizeBrandSearchText(searchableTextRaw);
  const matchedBrands = brands.filter((brand) => {
    const normalizedBrand = normalizeBrandSearchText(brand);
    return normalizedBrand && searchText.includes(normalizedBrand);
  });
  const volumeAssessment = assessYandexSmallVolume(searchableTextRaw);
  const volumesMl = volumeAssessment.volumesMl;
  const minVolumeMl = volumeAssessment.minVolumeMl;
  // Named sets stay even with small parts; "без коробки" must be removed from Yandex.
  const isSet = isYandexSetProduct(searchableTextRaw);
  const smallVolume = volumeAssessment.blocked && !isSet;
  const lowerName = name.toLowerCase();
  const hasBlockedKeyword = lowerName.includes("отливант")
    || lowerName.includes("тестер")
    || isYandexNoBoxProduct(searchableTextRaw);
  const forceDelete = smallVolume || hasBlockedKeyword;
  const protectedByBrand = matchedBrands.length > 0 && !forceDelete;
  return {
    id: `${shop.businessId || shop.id || "yandex"}:${offerId}`,
    shopId: shop.id || "yandex",
    shopName: shop.name || "Yandex Market",
    offerId,
    name,
    vendor,
    archived,
    state: state.code,
    stateLabel: state.label,
    stateName: state.stateName || "",
    matchedBrands,
    minVolumeMl,
    smallVolume,
    hasBlockedKeyword,
    protected: protectedByBrand,
    action: protectedByBrand ? "keep" : "delete",
  };
}

function summarizeYandexCleanupPreview(rows = []) {
  const toDelete = rows.filter((row) => row.action === "delete").length;
  const alreadyArchived = rows.filter((row) => row.archived).length;
  return {
    total: rows.length,
    protected: rows.filter((row) => row.protected).length,
    toDelete,
    toArchive: toDelete,
    alreadyArchived,
  };
}

