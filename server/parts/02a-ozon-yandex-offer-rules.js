// Rules for Ozon → Yandex Market card transfer (cabinet Parfumerius, businessId 171782339).
// Pure helpers used by buildYandexOfferMapping and sendYandexOfferMappings.

const YANDEX_CATEGORY_PERFUMERY = 15927546;
const YANDEX_PARAM_VARIANT_GROUP = 200;
const YANDEX_PARAM_BOTTLE_VOLUME = 24139073;
const YANDEX_PARAM_TESTER = 53763303;

// Ozon attribute ids (description-category attributes).
const OZON_ATTR_BRAND = 85;
const OZON_ATTR_MODEL = 9048;
const OZON_ATTR_COUNTRY = 4389;
const OZON_ATTR_NAME = 4180;

// Ozon product type → Market category. Types that need the name to confirm the kind
// (face vs body cream, shower products) carry a `nameRequired` pattern.
const OZON_TYPE_TO_YANDEX_CATEGORY = new Map([
  [93403, { categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "парфюмерная вода" }],
  [93405, { categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "туалетная вода" }],
  [93397, { categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "духи" }],
  [970674005, { categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "духи" }],
  [93402, { categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "одеколон" }],
  [97704, { categoryId: 91176, kind: "гель для душа", nameRequired: /гель\s+для\s+душа|shower\s+gel/i }],
  [93950, { categoryId: 91183, kind: "шампунь" }],
  [93466, { categoryId: 8480725, kind: "дезодорант" }],
  [93883, { categoryId: 8475955, kind: "крем для тела", nameRequired: /для\s+тела|body/i }],
  [93873, { categoryId: 8475955, kind: "лосьон для тела", nameRequired: /для\s+тела|body/i }],
  [93887, { categoryId: 8475955, kind: "масло для тела", nameRequired: /для\s+тела|body/i }],
  [95741, { categoryId: 91304, kind: "свеча" }],
  [92718, { categoryId: 61329715, kind: "диффузор" }],
  [92721, { categoryId: 61343235, kind: "парфюм для дома" }],
]);

// Name keywords, in priority order (more specific first).
const YANDEX_NAME_CATEGORY_RULES = [
  { re: /парфюм[a-zа-яё]*\s+для\s+дома|аромат[a-zа-яё]*\s+для\s+дома|home\s+(fragrance|spray|perfume)/i, categoryId: 61343235, kind: "парфюм для дома" },
  { re: /диффузор|diffuser/i, categoryId: 61329715, kind: "диффузор" },
  { re: /свеч[аиу]|candle/i, categoryId: 91304, kind: "свеча" },
  { re: /гель\s+для\s+душа|shower\s+gel/i, categoryId: 91176, kind: "гель для душа" },
  { re: /шампун|shampoo/i, categoryId: 91183, kind: "шампунь" },
  { re: /дезодорант|deodorant/i, categoryId: 8480725, kind: "дезодорант" },
  { re: /(крем|лосьон|масло|молочко)\s+для\s+тела|body\s+(cream|lotion|oil|milk)/i, categoryId: 8475955, kind: "уход за телом" },
  { re: /парфюмерн[a-zа-яё]*\s+вод|eau\s+de\s+parfum|\bedp\b/i, categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "парфюмерная вода" },
  { re: /туалетн[a-zа-яё]*\s+вод|eau\s+de\s+toilette|\bedt\b/i, categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "туалетная вода" },
  { re: /одеколон|eau\s+de\s+cologne|\bedc\b|cologne/i, categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "одеколон" },
  { re: /(?<![а-яё])духи(?![а-яё])|экстракт|extrait|\bparfum\b|perfume/i, categoryId: YANDEX_CATEGORY_PERFUMERY, kind: "духи" },
];

// Words that mean the product is not a fragrance itself.
const NOT_PERFUME_NAME_RE = /гель|крем|масл[оа]\s+для|мыл[оа]|шампун|бальзам|лосьон|дезодорант|заправк|скраб|пилинг|маск[аи]|сыворотк|для\s+волос|hair|body\s+(lotion|cream|wash)|свеч|диффузор|для\s+дома|спрей\s+для\s+тела|(?<![а-яё])мист|\bmist\b|body\s+(spray|mist)/i;

// A set of several products must not land in a single-product category.
function isProductSetName(name = "") {
  const text = String(name || "");
  return /набор|комплект|\bset\b|\bkit\b|gift\s*box|подарочн/i.test(text)
    || /\d+\s*(мл|ml)?\s*\+\s*\d+/i.test(text)
    || /\d\s*(мл|ml|гр?|g)\s*\+\s*\p{L}/iu.test(text)
    || /\d+\s*[xх×*]\s*\d+\s*(мл|ml)/i.test(text)
    || /\d+\s*(мл|ml)\s*[xх×*]\s*\d+/i.test(text);
}

// Market category for an Ozon product, or { categoryId: null, reason } for manual review.
function resolveYandexCategoryForOzonProduct({ typeId = 0, name = "" } = {}) {
  const text = String(name || "");
  if (isProductSetName(text)) return { categoryId: null, reason: "set_manual_review" };
  const byType = OZON_TYPE_TO_YANDEX_CATEGORY.get(Number(typeId) || 0) || null;
  const byName = YANDEX_NAME_CATEGORY_RULES.find((rule) => rule.re.test(text)) || null;
  if (byType) {
    // Ozon types are often wrong (a hand gel filed as "парфюмерная вода"): a perfumery type
    // whose name says it is another product is left for manual review.
    if (byType.categoryId === YANDEX_CATEGORY_PERFUMERY && NOT_PERFUME_NAME_RE.test(text)) {
      return { categoryId: null, reason: `type_name_conflict:${byType.kind}/not_perfume` };
    }
    // Refills and spare parts are not the device itself.
    if (/заправк|сменн[a-zа-яё]*\s+блок|refill|запасн/i.test(text) && byType.categoryId !== YANDEX_CATEGORY_PERFUMERY) {
      return { categoryId: null, reason: `refill_manual_review:${byType.kind}` };
    }
    // Body masks, anti-cellulite and sun care are their own Market categories, not body cream.
    if (byType.categoryId === 8475955 && /маск|антицеллюлит|загар|\bspf\b|\bsun\b/i.test(text)) {
      return { categoryId: null, reason: `body_care_special_manual_review:${byType.kind}` };
    }
    if (byType.nameRequired && !byType.nameRequired.test(text)) {
      return { categoryId: null, reason: `type_needs_name_confirmation:${byType.kind}` };
    }
    if (byName && byName.categoryId !== byType.categoryId) {
      return { categoryId: null, reason: `type_name_conflict:${byType.kind}/${byName.kind}` };
    }
    return { categoryId: byType.categoryId, kind: byType.kind, source: "ozon_type" };
  }
  if (Number(typeId) > 0) return { categoryId: null, reason: `unmapped_ozon_type:${typeId}` };
  if (byName) return { categoryId: byName.categoryId, kind: byName.kind, source: "name" };
  return { categoryId: null, reason: "type_unknown" };
}

// GTIN (8/12/13/14 digits) with a valid GS1 check digit. Internal codes starting with 2 / 02
// and Ozon codes (OZN…) are not GTINs.
function isValidGtin(value) {
  const code = String(value || "").trim();
  if (!/^\d+$/.test(code) || ![8, 12, 13, 14].includes(code.length)) return false;
  if (code.startsWith("2") || code.startsWith("02")) return false;
  if (/^0+$/.test(code)) return false;
  const digits = code.split("").map(Number);
  const check = digits.pop();
  let sum = 0;
  for (let i = digits.length - 1, weight = 3; i >= 0; i -= 1, weight = weight === 3 ? 1 : 3) sum += digits[i] * weight;
  return (10 - (sum % 10)) % 10 === check;
}

function pickYandexGtins(values = [], { offerId = "" } = {}) {
  const article = String(offerId || "").trim().toLowerCase();
  const out = [];
  for (const raw of values) {
    const code = String(raw || "").trim();
    if (!code || code.toLowerCase() === article) continue;
    if (isValidGtin(code) && !out.includes(code)) out.push(code);
  }
  return out;
}

function isPlaceholderVendor(value = "") {
  const text = String(value || "").trim();
  if (!text) return true;
  if (/^(без|нет)\s+бренд|^no\s*-?\s*(brand|name)$|^noname$|^без\s+марки$/i.test(text)) return true;
  if (/^нф[-\s]?\d*/i.test(text)) return true;
  if (typeof isBrandGarbageValue === "function" && isBrandGarbageValue(text)) return true;
  return false;
}

function ozonAttributeValue(attributes = [], id) {
  const list = Array.isArray(attributes) ? attributes : [];
  const attribute = list.find((item) => Number(item?.id ?? item?.attribute_id) === Number(id));
  const values = Array.isArray(attribute?.values) ? attribute.values : [];
  return String(values.map((item) => item?.value ?? "").filter(Boolean).join(", ")).trim();
}

// Country names as Yandex Maps spells them.
const YANDEX_COUNTRY_ALIASES = new Map([
  ["корея (южная)", "Южная Корея"],
  ["корея, республика", "Южная Корея"],
  ["республика корея", "Южная Корея"],
  ["корея", "Южная Корея"],
  ["оаэ", "Объединённые Арабские Эмираты"],
  ["объединенные арабские эмираты", "Объединённые Арабские Эмираты"],
  ["сша", "США"],
  ["соединенные штаты", "США"],
  ["соединенные штаты америки", "США"],
  ["англия", "Великобритания"],
  ["соединенное королевство", "Великобритания"],
  ["чехия", "Чехия"],
  ["чешская республика", "Чехия"],
  ["кнр", "Китай"],
  ["китайская народная республика", "Китай"],
  ["тайвань (китай)", "Тайвань"],
]);

function yandexCountryName(value = "") {
  const text = String(value || "").trim();
  if (!text) return "";
  const alias = YANDEX_COUNTRY_ALIASES.get(text.toLowerCase().replace(/ё/g, "е"));
  return alias || text;
}

// Name must not be an article and must not be written entirely in capitals.
function looksLikeArticle(name = "", offerId = "") {
  const text = String(name || "").trim();
  if (!text) return true;
  if (offerId && text.toLowerCase() === String(offerId).trim().toLowerCase()) return true;
  // Ozon placeholder / junk cards («Дубль95», «ъ-Оъ»): never transferred.
  if (/^дубль\s*\d*$/i.test(text)) return true;
  if (!/[A-Za-zА-Яа-яЁё]{3,}/.test(text)) return true;
  return !/\s/.test(text) && /\d/.test(text) && text.length <= 20;
}

const YANDEX_NAME_KEEP_UPPER = new Set(["EDP", "EDT", "EDC", "SPF", "UV", "BB", "CC", "USA", "UK", "XL", "XXL"]);

function isAllCapsName(name = "") {
  const letters = String(name || "").match(/\p{L}/gu) || [];
  if (letters.length < 4) return false;
  const upper = letters.filter((ch) => ch === ch.toUpperCase() && ch !== ch.toLowerCase()).length;
  return upper / letters.length >= 0.9;
}

function softenAllCapsName(name = "", { vendor = "" } = {}) {
  let text = String(name || "").trim();
  if (!isAllCapsName(text)) return text;
  text = text.replace(/[\p{L}][\p{L}'’-]*/gu, (word) => {
    if (YANDEX_NAME_KEEP_UPPER.has(word)) return word;
    if (/^(ML|МЛ)$/i.test(word)) return "мл";
    return word.charAt(0) + word.slice(1).toLowerCase();
  });
  const brand = String(vendor || "").trim();
  if (brand && text.toLowerCase().startsWith(brand.toLowerCase())) text = brand + text.slice(brand.length);
  return text;
}

function resolveYandexOfferName({ candidates = [], offerId = "", vendor = "" } = {}) {
  const name = candidates.map((value) => String(value || "").trim()).find((value) => value && !looksLikeArticle(value, offerId)) || "";
  return name ? softenAllCapsName(name, { vendor }) : "";
}

function extractBottleVolumeMl(name = "") {
  // (?![a-zа-я]) instead of \b: \b does not work after Cyrillic «мл».
  const match = String(name || "").match(/(\d+(?:[.,]\d+)?)\s*(мл|ml)(?![a-zа-я])/i);
  if (!match) return null;
  const volume = Number(match[1].replace(",", "."));
  return Number.isFinite(volume) && volume > 0 ? volume : null;
}

function isTesterName(name = "") {
  return /тестер|tester/i.test(String(name || ""));
}

// Variant-group parameters for «Парфюмерия»: one group per aroma of one brand and one kind
// (EDP/EDT differ), variants distinguished by bottle volume and tester flag.
function buildPerfumeVariantParameters({ categoryId, vendor = "", model = "", kind = "", name = "" } = {}) {
  if (Number(categoryId) !== YANDEX_CATEGORY_PERFUMERY) return [];
  const params = [];
  const volume = extractBottleVolumeMl(name);
  if (volume) params.push({ parameterId: YANDEX_PARAM_BOTTLE_VOLUME, value: String(volume) });
  params.push({ parameterId: YANDEX_PARAM_TESTER, value: isTesterName(name) ? "true" : "false" });
  const aroma = String(model || "").trim();
  if (vendor && aroma && volume) {
    const group = [vendor, aroma, kind].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
    if (group.length >= 3) params.push({ parameterId: YANDEX_PARAM_VARIANT_GROUP, value: group.slice(0, 255) });
  }
  return params;
}

// commodityCodes: ТН ВЭД + full ОКПД2 together in one array (the array is replaced whole).
function sanitizeYandexCommodityCodes(codes) {
  const list = Array.isArray(codes) ? codes : [];
  const tnved = list.find((item) => item?.type === "CUSTOMS_COMMODITY_CODE" && /^\d{10}(\d{4})?$/.test(String(item.code || "")));
  const okpd2 = list.find((item) => item?.type === "OKPD2_CODE" && /^\d{2}\.\d{2}\.\d{2}\.\d{3}$/.test(String(item.code || "")));
  if (!tnved || !okpd2) return undefined;
  return [{ code: String(tnved.code), type: "CUSTOMS_COMMODITY_CODE" }, { code: String(okpd2.code), type: "OKPD2_CODE" }];
}

// Declaration numbers keep the hyphen after «Д»: «ЕАЭС N RU Д-FR.РА03.В.19152/26».
function normalizeYandexCertificate(value = "") {
  const text = String(value || "").trim().replace(/\s+/g, " ");
  if (!text) return "";
  return text.replace(/(ЕАЭС\s+N\s+RU\s+)Д\s*-?\s*(?=[A-ZА-Я]{2}\.)/i, "$1Д-");
}

function sanitizeYandexCertificates(values) {
  const list = (Array.isArray(values) ? values : []).map(normalizeYandexCertificate).filter(Boolean);
  return list.length ? Array.from(new Set(list)) : undefined;
}

const YANDEX_TIME_UNITS = new Map([
  ["HOUR", "HOUR"], ["HOURS", "HOUR"], ["DAY", "DAY"], ["DAYS", "DAY"], ["WEEK", "WEEK"], ["WEEKS", "WEEK"],
  ["MONTH", "MONTH"], ["MONTHS", "MONTH"], ["YEAR", "YEAR"], ["YEARS", "YEAR"],
]);

function sanitizeYandexShelfLife(value) {
  if (!value || typeof value !== "object") return undefined;
  const period = Math.round(Number(value.timePeriod));
  const unit = YANDEX_TIME_UNITS.get(String(value.timeUnit || "").toUpperCase());
  if (!(period > 0) || !unit) return undefined;
  return { timePeriod: period, timeUnit: unit, ...(value.comment ? { comment: String(value.comment) } : {}) };
}

// Fields another system fills in the Market cabinet (GTIN, ТН ВЭД/ОКПД2, documents, shelf life):
// arrays are replaced whole by offer-mappings/update, so they are only sent when Market has
// nothing yet and we have a real value.
const YANDEX_FOREIGN_OWNED_FIELDS = ["barcodes", "commodityCodes", "certificates", "shelfLife"];
const YANDEX_MANAGED_FIELDS = ["name", "marketCategoryId", "pictures", "vendor", "description", "weightDimensions", "manufacturerCountries"];

function yandexValuesEqual(a, b) {
  const norm = (value) => {
    if (Array.isArray(value)) return JSON.stringify(value.map((item) => (typeof item === "object" ? JSON.stringify(item) : String(item).trim())).sort());
    if (value && typeof value === "object") {
      return JSON.stringify(Object.keys(value).sort().map((key) => [key, typeof value[key] === "number" ? Number(value[key]) : value[key]]));
    }
    return String(value ?? "").trim();
  };
  return norm(a) === norm(b);
}

function weightDimensionsEqual(a = {}, b = {}) {
  return ["length", "width", "height", "weight"].every((key) => Math.abs(Number(a?.[key] || 0) - Number(b?.[key] || 0)) < 0.01);
}

// For a card that already exists: only the fields that changed. Returns null when nothing did.
function diffYandexOfferForUpdate(built = {}, current = {}, currentCategoryId = 0) {
  const update = { offerId: built.offerId };
  let changed = false;
  for (const field of YANDEX_MANAGED_FIELDS) {
    if (built[field] === undefined) continue;
    if (field === "marketCategoryId") {
      if (Number(built.marketCategoryId) !== Number(currentCategoryId || current.marketCategoryId || 0)) {
        update.marketCategoryId = built.marketCategoryId;
        changed = true;
      }
      continue;
    }
    const equal = field === "weightDimensions"
      ? weightDimensionsEqual(built[field], current[field])
      : yandexValuesEqual(built[field], current[field]);
    if (!equal) {
      update[field] = built[field];
      changed = true;
    }
  }
  for (const field of YANDEX_FOREIGN_OWNED_FIELDS) {
    if (built[field] === undefined) continue;
    const existing = current[field];
    const hasExisting = Array.isArray(existing) ? existing.length > 0 : Boolean(existing && (typeof existing !== "object" || Object.keys(existing).length));
    if (!hasExisting) {
      update[field] = built[field];
      changed = true;
    }
  }
  if (Array.isArray(built.parameterValues) && built.parameterValues.length) {
    const currentParams = new Map((Array.isArray(current.parameterValues) ? current.parameterValues : [])
      .map((item) => [Number(item.parameterId), String(item.value ?? item.valueId ?? "")]));
    const params = built.parameterValues.filter((item) => currentParams.get(Number(item.parameterId)) !== String(item.value));
    if (params.length) {
      update.parameterValues = params;
      if (!update.marketCategoryId && built.marketCategoryId) update.marketCategoryId = built.marketCategoryId;
      changed = true;
    }
  }
  return changed ? update : null;
}

// offer-mappings/update answers per offer: results[].errors reject the WHOLE request,
// warnings are informational.
function parseYandexOfferMappingsResult(apiResult = {}) {
  const results = Array.isArray(apiResult?.results) ? apiResult.results
    : (Array.isArray(apiResult?.result?.results) ? apiResult.result.results : []);
  const errorsByOffer = new Map();
  const warningsByOffer = new Map();
  for (const item of results) {
    const offerId = String(item?.offerId || "").trim();
    if (!offerId) continue;
    if (Array.isArray(item.errors) && item.errors.length) errorsByOffer.set(offerId, item.errors);
    if (Array.isArray(item.warnings) && item.warnings.length) warningsByOffer.set(offerId, item.warnings);
  }
  return { errorsByOffer, warningsByOffer };
}

// The Ozon product and the Market card under the same offerId are the same product: the Market
// name shares a meaningful word with the Ozon name and does not name another product kind.
function yandexCardMatchesOzonProduct(marketName = "", ozonName = "", ruleCategoryId = 0) {
  const market = String(marketName || "").trim();
  if (!market || looksLikeArticle(market, "")) return false;
  const words = (value) => new Set((String(value || "").toLowerCase().match(/[\p{L}]{4,}/gu) || [])
    .filter((word) => !["парфюмерная", "туалетная", "вода", "духи", "женская", "мужская", "унисекс", "женский", "мужской", "набор"].includes(word)));
  const ozonWords = words(ozonName);
  if (![...words(market)].some((word) => ozonWords.has(word))) return false;
  const byMarketName = resolveYandexCategoryForOzonProduct({ typeId: 0, name: market });
  if (byMarketName.categoryId && Number(byMarketName.categoryId) !== Number(ruleCategoryId)) return false;
  if (Number(ruleCategoryId) === YANDEX_CATEGORY_PERFUMERY && NOT_PERFUME_NAME_RE.test(market)) return false;
  return true;
}
