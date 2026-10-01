// Сборка карточки Ozon из аромата Фрагрантики — чистые функции без зависимостей
// (тесты: test/fragrantica-ozon-build.test.cjs). Роуты и очередь — 02d-fragrantica-ozon-export.js.
//
// Значения по умолчанию повторяют существующие карточки парфюмерии в кабинетах (2026-10-01):
// ТН ВЭД 3303001000, срок годности 1000 дней, без маркировки, класс опасности 9, ЦА «Взрослая»,
// НДС 5%, ноты — в «Состав» (8050), габариты ~190×120×110 мм / 350 г для 60 мл.

const FRAG_OZON_CATEGORY_ID = 17028988; // Красота и гигиена / Парфюмерия

const FRAG_OZON_TYPES = [
  { key: "edp", typeId: 93403, label: "Вода парфюмерная", nameLabel: "Парфюмерная вода" },
  { key: "edt", typeId: 93405, label: "Туалетная вода", nameLabel: "Туалетная вода" },
  { key: "parfum", typeId: 93397, label: "Духи", nameLabel: "Духи" },
  { key: "cologne", typeId: 93402, label: "Одеколон", nameLabel: "Одеколон" },
  { key: "oil", typeId: 970674005, label: "Духи-масло", nameLabel: "Духи-масло" },
];

const FRAG_OZON_ATTR = {
  brand: 85,
  type: 8229,
  name: 4180,
  annotation: 4191,
  composition: 8050,
  volume: 8163,
  gender: 9163,
  model: 9048,
  modelTemplate: 12141,
  similar: 22390,
  classification: 8008,
  tnved: 22232,
  marking: 23536,
  shelfLife: 8205,
  hazard: 9782,
  audience: 9390,
  sellerCode: 9024,
  weightPack: 4497,
  hashtags: 23171,
  country: 4389,
  producer: 23487,
  weightNet: 4383,
  weight: 8044,
  packaging: 4386,
  unitsPerItem: 8962,
  factoryPacks: 11650,
  releaseKind: 22270,
  adult: 9070,
};

const FRAG_OZON_DEFAULTS = {
  vat: "0.05",
  tnvedCode: "3303001000",
  shelfLifeDays: "900",
  hazardValueId: 970593909, // Класс 9. Прочие опасные вещества
  audienceValueId: 43241, // Взрослая
  packagingBoxId: 85921, // Картонная коробка
  packagingTesterId: 115933094, // Коробка (тестер в простой коробке)
  releaseFactoryId: 971417785, // Фабричное производство
  okpd2: "20.42.11.000", // Духи и туалетная вода
};

// НДС по кабинету (clientId Ozon): AURA — без НДС, остальные — 5% (УСН). FRAGRANTICA_VAT_BY_ACCOUNT переопределяет.
const FRAG_DEFAULT_VAT_BY_CLIENT = { "2533393": "0" };

function fragranticaVatForClientId(clientId, overrides = {}) {
  const key = String(clientId || "").trim();
  if (overrides[key] !== undefined) return String(overrides[key]);
  if (FRAG_DEFAULT_VAT_BY_CLIENT[key] !== undefined) return FRAG_DEFAULT_VAT_BY_CLIENT[key];
  return FRAG_OZON_DEFAULTS.vat;
}

// Страна бренда с Фрагрантики (англ.) → русское название, как в справочниках Ozon и Маркета.
const FRAG_COUNTRY_RU = {
  "france": "Франция", "italy": "Италия", "united states": "США", "usa": "США", "united kingdom": "Великобритания",
  "spain": "Испания", "germany": "Германия", "united arab emirates": "ОАЭ", "uae": "ОАЭ", "switzerland": "Швейцария",
  "netherlands": "Нидерланды", "belgium": "Бельгия", "sweden": "Швеция", "japan": "Япония", "south korea": "Южная Корея",
  "russia": "Россия", "saudi arabia": "Саудовская Аравия", "kuwait": "Кувейт", "oman": "Оман", "qatar": "Катар",
  "bahrain": "Бахрейн", "turkey": "Турция", "brazil": "Бразилия", "canada": "Канада", "australia": "Австралия",
  "poland": "Польша", "czech republic": "Чехия", "austria": "Австрия", "denmark": "Дания", "norway": "Норвегия",
  "finland": "Финляндия", "portugal": "Португалия", "greece": "Греция", "india": "Индия", "china": "Китай",
  "israel": "Израиль", "lebanon": "Ливан", "monaco": "Монако", "ireland": "Ирландия", "hungary": "Венгрия",
  "romania": "Румыния", "ukraine": "Украина", "belarus": "Беларусь", "mexico": "Мексика", "argentina": "Аргентина",
  "singapore": "Сингапур", "thailand": "Таиланд", "indonesia": "Индонезия", "pakistan": "Пакистан", "jordan": "Иордания",
  "egypt": "Египет", "morocco": "Марокко", "south africa": "ЮАР", "new zealand": "Новая Зеландия", "luxembourg": "Люксембург",
};

function fragranticaCountryRu(country) {
  const key = String(country || "").trim().toLowerCase().replace(/-/g, " ");
  return FRAG_COUNTRY_RU[key] || "";
}

// Вес флакона без коробки, г (оценка по объёму; коробка — в «Весе с упаковкой»).
function fragOzonNetWeight(volume) {
  const vol = Number(fragFormatVolume(volume)) || 0;
  if (vol && vol <= 10) return 25;
  if (vol && vol <= 35) return 120;
  if (vol && vol <= 60) return 200;
  if (vol && vol <= 110) return 300;
  return 450;
}

function fragOzonTypeByKey(key) {
  return FRAG_OZON_TYPES.find((t) => t.key === key) || FRAG_OZON_TYPES[0];
}

function fragOzonTypeById(typeId) {
  return FRAG_OZON_TYPES.find((t) => t.typeId === Number(typeId)) || null;
}

// «Eau de Toilette» / «Cologne» в названии подсказывают тип; по умолчанию — парфюмерная вода.
function fragOzonGuessTypeKey(perfume = {}) {
  const text = `${perfume.name || ""} ${perfume.title || ""}`.toLowerCase();
  if (/eau de toilette|\bedt\b|туалетн/.test(text)) return "edt";
  if (/eau de cologne|cologne|одеколон/.test(text)) return "cologne";
  if (/perfume oil|parfum oil|attar|масло/.test(text)) return "oil";
  if (/extrait|(^|\s)parfum$|(^|\s)духи/.test(text)) return "parfum";
  return "edp";
}

function fragFormatVolume(volume) {
  const value = Number(String(volume ?? "").replace(",", "."));
  if (!Number.isFinite(value) || value <= 0) return "";
  return String(Math.round(value * 100) / 100);
}

function fragNameWithBrand(perfume = {}) {
  const brand = String(perfume.brand || "").trim();
  const name = String(perfume.name || "").trim();
  if (!brand) return name;
  return name.toLowerCase().startsWith(brand.toLowerCase()) ? name : `${brand} ${name}`;
}

function buildFragranticaOzonName({ perfume = {}, typeKey = "edp", volume, tester = false } = {}) {
  const type = fragOzonTypeByKey(typeKey);
  const vol = fragFormatVolume(volume);
  return [fragNameWithBrand(perfume), type.nameLabel, tester ? "тестер" : "", vol ? `${vol} мл` : ""].filter(Boolean).join(" ");
}

// FR<id фрагрантики>-<объём>[T]; при занятости — суффикс -2, -3… (fragranticaUniqueOfferId)
function buildFragranticaOfferId({ perfumeId, volume, tester = false } = {}) {
  const vol = fragFormatVolume(volume).replace(".", "_");
  return `FR${Number(perfumeId)}${vol ? `-${vol}` : ""}${tester ? "T" : ""}`;
}

function fragranticaUniqueOfferId(base, taken = new Set()) {
  if (!taken.has(base)) return base;
  for (let n = 2; n < 100; n += 1) {
    const candidate = `${base}-${n}`;
    if (!taken.has(candidate)) return candidate;
  }
  return `${base}-${Date.now()}`;
}

// Габариты коробки и вес по объёму (по существующим карточкам парфюмерии).
// Шаблоны габаритов (страница «Фрагрантика» → «Шаблоны габаритов»): строка с объёмом, равным
// объёму товара, иначе ближайшая большая, иначе самая большая. Пусто — встроенная таблица ниже.
function normalizeFragranticaDimsTemplates(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((r) => ({
      volume: Number(String(r?.volume ?? "").replace(",", ".")) || 0,
      depth: Math.round(Number(r?.depth) || 0),
      width: Math.round(Number(r?.width) || 0),
      height: Math.round(Number(r?.height) || 0),
      weight: Math.round(Number(r?.weight) || 0),
    }))
    .filter((r) => r.volume > 0 && r.depth > 0 && r.width > 0 && r.height > 0 && r.weight > 0)
    .sort((a, b) => a.volume - b.volume)
    .filter((r, i, list) => i === 0 || list[i - 1].volume !== r.volume)
    .slice(0, 100);
}

function fragOzonDimensions(volume, templates = []) {
  const vol = Number(fragFormatVolume(volume)) || 0;
  const rows = normalizeFragranticaDimsTemplates(templates);
  if (rows.length) {
    const row = (vol && rows.find((r) => r.volume >= vol)) || rows[rows.length - 1];
    return { depth: row.depth, width: row.width, height: row.height, weight: row.weight };
  }
  if (vol && vol <= 10) return { depth: 150, width: 125, height: 60, weight: 50 };
  if (vol && vol <= 35) return { depth: 195, width: 125, height: 115, weight: 300 };
  if (vol && vol <= 60) return { depth: 190, width: 120, height: 110, weight: 350 };
  if (vol && vol <= 110) return { depth: 200, width: 130, height: 120, weight: 450 };
  return { depth: 220, width: 140, height: 130, weight: 600 };
}

function fragNotesList(notes = []) {
  return notes.map((n) => String(n.name || "").trim()).filter(Boolean).join(", ");
}

// «Верхние ноты: …\nНоты сердца: …\nБазовые ноты: …» — как в существующих карточках (атрибут «Состав»).
function buildFragranticaCompositionText(notes = {}) {
  const lines = [];
  if (notes.top?.length) lines.push(`Верхние ноты: ${fragNotesList(notes.top)}`);
  if (notes.middle?.length) lines.push(`Ноты сердца: ${fragNotesList(notes.middle)}`);
  if (notes.base?.length) lines.push(`Базовые ноты: ${fragNotesList(notes.base)}`);
  if (!lines.length && notes.flat?.length) lines.push(`Ноты: ${fragNotesList(notes.flat)}`);
  return lines.join("\n");
}

function fragHashtag(text) {
  const word = String(text || "").toLowerCase().replace(/[^0-9a-zа-яё]+/gi, "_").replace(/^_+|_+$/g, "");
  return word ? `#${word.slice(0, 28)}` : "";
}

function buildFragranticaHashtags({ perfume = {}, typeKey = "edp" } = {}) {
  const type = fragOzonTypeByKey(typeKey);
  return [...new Set([fragHashtag(type.nameLabel), "#оригинальная_парфюмерия", fragHashtag(perfume.brand)].filter(Boolean))].join(" ");
}

function fragNormalizeWord(text) {
  return String(text || "").toLowerCase().replace(/ё/g, "е").trim();
}

// Аккорды Фрагрантики → значения справочника «Классификация аромата» (до 3).
// Сначала группа («фужерные» → Фужерный), потом аккорды по убыванию доли. Сравнение по основе слова —
// \b в JS не работает рядом с кириллицей, поэтому слова режутся явным классом.
function matchFragranticaClassification({ family = "", accords = [] } = {}, dictionary = [], max = 3) {
  const values = dictionary.map((v) => ({ id: Number(v.id), value: String(v.value || ""), norm: fragNormalizeWord(v.value) }));
  const stem = (word) => fragNormalizeWord(word).slice(0, 5);
  // Слова Фрагрантики, которых нет в справочнике Ozon дословно (справочник 1120 на 2026-10-01).
  const synonyms = {
    "белые": "цвето", "цветы": "цвето", "ароматический": "фужер", "сладкий": "гурма", "ванильный": "гурма",
    "карамельный": "гурма", "шоколадный": "гурма", "кофейный": "гурма", "медовый": "гурма", "водный": "аква",
    "озоновый": "аква", "морской": "аква", "пряный": "восто", "уд": "восто", "смолистый": "восто",
    "ладанный": "восто", "теплый": "восто", "бальзамический": "бальза", "животный": "анима", "ромовый": "алког",
    "фужерные": "фужер", "шипровые": "шипро", "восточные": "восто",
  };
  const picked = [];
  const tryWord = (word) => {
    const norm = fragNormalizeWord(word);
    if (!norm) return;
    const wanted = synonyms[norm] || stem(norm);
    if (wanted.length < 4) return;
    const hit = values.find((v) => v.norm.startsWith(wanted));
    if (hit && !picked.some((p) => p.id === hit.id)) picked.push(hit);
  };
  for (const word of String(family).split(/[^а-яёa-z]+/i)) tryWord(word);
  for (const accord of [...accords].sort((a, b) => (b.share || 0) - (a.share || 0))) {
    if (picked.length >= max) break;
    const words = String(accord.name || "").split(/[^а-яёa-z]+/i).filter(Boolean);
    // «свежий пряный» → «пряный», «белые цветы» → «цветы»
    for (const word of [...words].reverse()) {
      const before = picked.length;
      tryWord(word);
      if (picked.length > before) break;
    }
  }
  return picked.slice(0, max).map((v) => ({ dictionary_value_id: v.id, value: v.value }));
}

function fragGenderValues(gender, dictionary = []) {
  const find = (re) => dictionary.find((v) => re.test(String(v.value || "")));
  const male = find(/^мужск/i);
  const female = find(/^женск/i);
  const list = gender === "male" ? [male] : gender === "female" ? [female] : [male, female];
  return list.filter(Boolean).map((v) => ({ dictionary_value_id: Number(v.id), value: String(v.value) }));
}

function fragAttr(id, values) {
  const list = (Array.isArray(values) ? values : [values]).filter((v) => v && (v.dictionary_value_id || String(v.value ?? "").trim() !== ""));
  return list.length ? { id, complex_id: 0, values: list.map((v) => (v.dictionary_value_id ? { dictionary_value_id: Number(v.dictionary_value_id), value: String(v.value ?? "") } : { value: String(v.value) })) } : null;
}

// Предзаполнение атрибутов. lookups: { brand, type, gender[], classification[], tnved } — найденные значения справочников.
function buildFragranticaOzonPrefill({ perfume = {}, typeKey = "edp", volume, tester = false, offerId = "", lookups = {}, dimsTemplates = [] } = {}) {
  const name = buildFragranticaOzonName({ perfume, typeKey, volume, tester });
  const model = String(perfume.name || "").trim();
  const dims = fragOzonDimensions(volume, dimsTemplates);
  const attrs = [
    fragAttr(FRAG_OZON_ATTR.brand, lookups.brand ? { dictionary_value_id: lookups.brand.id, value: lookups.brand.value } : null),
    fragAttr(FRAG_OZON_ATTR.type, lookups.type ? { dictionary_value_id: lookups.type.id, value: lookups.type.value } : null),
    fragAttr(FRAG_OZON_ATTR.name, { value: name }),
    fragAttr(FRAG_OZON_ATTR.model, { value: model }),
    fragAttr(FRAG_OZON_ATTR.modelTemplate, { value: model }),
    fragAttr(FRAG_OZON_ATTR.similar, { value: model }),
    fragAttr(FRAG_OZON_ATTR.annotation, { value: perfume.description || "" }),
    fragAttr(FRAG_OZON_ATTR.composition, { value: buildFragranticaCompositionText(perfume.notes) }),
    fragAttr(FRAG_OZON_ATTR.volume, { value: fragFormatVolume(volume) }),
    fragAttr(FRAG_OZON_ATTR.gender, lookups.gender || []),
    fragAttr(FRAG_OZON_ATTR.classification, lookups.classification || []),
    fragAttr(FRAG_OZON_ATTR.tnved, lookups.tnved ? { dictionary_value_id: lookups.tnved.id, value: lookups.tnved.value } : null),
    fragAttr(FRAG_OZON_ATTR.marking, { value: "false" }),
    fragAttr(FRAG_OZON_ATTR.shelfLife, { value: FRAG_OZON_DEFAULTS.shelfLifeDays }),
    fragAttr(FRAG_OZON_ATTR.hazard, { dictionary_value_id: FRAG_OZON_DEFAULTS.hazardValueId, value: "Класс 9. Прочие опасные вещества" }),
    fragAttr(FRAG_OZON_ATTR.audience, { dictionary_value_id: FRAG_OZON_DEFAULTS.audienceValueId, value: "Взрослая" }),
    fragAttr(FRAG_OZON_ATTR.sellerCode, { value: offerId }),
    fragAttr(FRAG_OZON_ATTR.weightPack, { value: String(dims.weight) }),
    fragAttr(FRAG_OZON_ATTR.hashtags, { value: buildFragranticaHashtags({ perfume, typeKey }) }),
    fragAttr(FRAG_OZON_ATTR.country, lookups.country ? { dictionary_value_id: lookups.country.id, value: lookups.country.value } : null),
    fragAttr(FRAG_OZON_ATTR.producer, { value: lookups.producer || perfume.brand || "" }),
    fragAttr(FRAG_OZON_ATTR.weightNet, { value: String(fragOzonNetWeight(volume)) }),
    fragAttr(FRAG_OZON_ATTR.weight, { value: String(fragOzonNetWeight(volume)) }),
    fragAttr(FRAG_OZON_ATTR.packaging, tester
      ? { dictionary_value_id: FRAG_OZON_DEFAULTS.packagingTesterId, value: "Коробка" }
      : { dictionary_value_id: FRAG_OZON_DEFAULTS.packagingBoxId, value: "Картонная коробка" }),
    fragAttr(FRAG_OZON_ATTR.unitsPerItem, { value: "1" }),
    fragAttr(FRAG_OZON_ATTR.factoryPacks, { value: "1" }),
    fragAttr(FRAG_OZON_ATTR.releaseKind, { dictionary_value_id: FRAG_OZON_DEFAULTS.releaseFactoryId, value: "Фабричное производство" }),
    fragAttr(FRAG_OZON_ATTR.adult, { value: "false" }),
  ].filter(Boolean);
  return { name, offerId, dims, attributes: attrs };
}

// Итоговый item для /v3/product/import + список того, чего не хватает (по обязательным атрибутам категории).
function buildFragranticaOzonItem(input = {}, categoryAttributes = []) {
  const type = fragOzonTypeById(input.typeId) || fragOzonTypeByKey(input.typeKey);
  const attributes = (Array.isArray(input.attributes) ? input.attributes : [])
    .map((a) => fragAttr(Number(a.id), a.values || []))
    .filter(Boolean);
  const images = (Array.isArray(input.images) ? input.images : []).map((u) => String(u || "").trim()).filter(Boolean).slice(0, 30);
  const price = Number(String(input.price ?? "").replace(",", "."));
  const oldPrice = Number(String(input.oldPrice ?? "").replace(",", "."));
  const item = {
    offer_id: String(input.offerId || "").trim(),
    name: String(input.name || "").trim(),
    description_category_id: FRAG_OZON_CATEGORY_ID,
    type_id: type.typeId,
    price: Number.isFinite(price) && price > 0 ? String(Math.round(price)) : "",
    old_price: Number.isFinite(oldPrice) && oldPrice > price ? String(Math.round(oldPrice)) : "",
    currency_code: "RUB",
    vat: String(input.vat || FRAG_OZON_DEFAULTS.vat),
    depth: Math.round(Number(input.depth) || 0),
    width: Math.round(Number(input.width) || 0),
    height: Math.round(Number(input.height) || 0),
    dimension_unit: "mm",
    weight: Math.round(Number(input.weight) || 0),
    weight_unit: "g",
    primary_image: images[0] || "",
    images: images.slice(1),
    attributes,
  };
  if (!item.old_price) delete item.old_price;
  const missing = [];
  if (!item.offer_id) missing.push("Артикул");
  if (!item.name) missing.push("Название");
  if (!item.price) missing.push("Цена");
  if (!item.primary_image) missing.push("Фото");
  for (const key of ["depth", "width", "height", "weight"]) if (!item[key]) missing.push({ depth: "Длина", width: "Ширина", height: "Высота", weight: "Вес" }[key]);
  const filled = new Set(attributes.map((a) => a.id));
  for (const attr of categoryAttributes) {
    if (attr.is_required && !filled.has(Number(attr.id))) missing.push(attr.name);
  }
  return { item, missing };
}

// ─── Оценка строки поставщика для «Предложений привязки» ─────────────────────
// Строки PriceMaster пишут по-разному: «C.Dior Sauvage men 60ml edt», «DIOR SAUVAGE (M) EDT 60 ML».
// Отсекаются клоны (оригинал только в скобках: «AREEJ DIORIT 100 ml (Sauvage Dior)»), не-парфюм
// (лосьон/бальзам после бритья, дезодорант, мист, гель…), другая концентрация и фланкеры
// («Sauvage Elixir» для «Sauvage»: лишние значимые слова → не рекомендуем).

const FRAG_ROW_STOP_WORDS = new Set([
  "ml", "мл", "m", "w", "l", "u", "men", "man", "women", "woman", "lady", "unisex", "унисекс", "муж", "жен", "мужской", "женский",
  "мужская", "женская", "edp", "edt", "edc", "parfum", "perfume", "toilette", "cologne", "туалетная", "парфюмерная",
  "вода", "духи", "одеколон", "spray", "vapo", "спрей", "new", "шт", "оригинал", "original", "lux", "люкс", "box", "без", "коробки",
  "коробка", "christian", "c", "for", "pour", "для", "и", "the", "tester", "тестер", "edition", "version", "версия",
]);
const FRAG_ROW_NOT_PERFUME = new Set([
  "lotion", "лосьон", "balm", "бальзам", "deo", "deodorant", "дезодорант", "mist", "мист", "gel", "гель", "shower", "душа",
  "cream", "крем", "soap", "мыло", "shampoo", "шампунь", "aftershave", "stick", "стик", "body", "hair", "волос", "candle", "свеча",
  "powder", "пудра", "oil", "масло", "milk", "молочко", "refill", "рефил",
]);

const FRAG_BRAND_ALIASES = {
  "yves saint laurent": ["ysl"],
  "dolce gabbana": ["d&g", "dg"],
  "calvin klein": ["ck"],
  "jean paul gaultier": ["jpg", "gaultier"],
  "maison francis kurkdjian": ["mfk", "kurkdjian"],
  "giorgio armani": ["armani"],
  "carolina herrera": ["herrera"],
  "hugo boss": ["boss"],
  "paco rabanne": ["rabanne"],
  "rabanne": ["paco"],
  "christian louboutin": ["louboutin"],
  "abercrombie fitch": ["a&f", "abercrombie"],
  "estee lauder": ["lauder"],
  "van cleef arpels": ["vca", "arpels"],
};

function fragRowTokens(text) {
  return String(text || "").toLowerCase().replace(/ё/g, "е").split(/[^0-9a-zа-я]+/i).filter(Boolean);
}

function fragRowConcentration(tokens, text) {
  const t = new Set(tokens);
  if (t.has("edp") || t.has("парфюмерная") || /eau\s+de\s+parfum/.test(text)) return "edp";
  if (t.has("edt") || t.has("туалетная") || /eau\s+de\s+toilette/.test(text)) return "edt";
  if (t.has("edc") || t.has("cologne") || t.has("одеколон")) return "cologne";
  if (t.has("extrait") || t.has("духи") || (t.has("parfum") && !t.has("eau"))) return "parfum";
  return "";
}

function assessFragranticaSupplierRow(rowName, { brand = "", name = "", typeKey = "edp", oilAllowed = false } = {}) {
  const text = String(rowName || "").toLowerCase().replace(/ё/g, "е");
  const outside = text.replace(/\([^)]*\)/g, " ");
  const outsideTokens = fragRowTokens(outside);
  const outsideSet = new Set(outsideTokens);
  const brandTokens = fragRowTokens(brand).filter((w) => w.length >= 3);
  const nameTokens = fragRowTokens(name);
  // The brand must be named outside the brackets (half of its words or a common abbreviation)
  const present = brandTokens.filter((w) => outsideSet.has(w)).length;
  const aliases = FRAG_BRAND_ALIASES[fragRowTokens(brand).join(" ")] || [];
  const aliasHit = aliases.some((alias) => (alias.includes("&") ? outside.includes(alias) : outsideSet.has(alias)));
  const clone = brandTokens.length > 0 && present < Math.ceil(brandTokens.length / 2) && !aliasHit;
  const afterShave = /after\s*-?\s*shave|a\s*\/\s*sh(?![a-z])|после\s+бритья/.test(text);
  const notPerfume = afterShave || fragRowTokens(text).some((w) => FRAG_ROW_NOT_PERFUME.has(w) && !(oilAllowed && (w === "oil" || w === "масло")));
  const concentration = fragRowConcentration(outsideTokens, outside);
  const wanted = typeKey === "oil" ? "parfum" : typeKey;
  const concentrationOk = !concentration || concentration === wanted;
  const known = new Set([...brandTokens, ...fragRowTokens(brand), ...nameTokens]);
  // «eau de parfum» is a concentration, a lone «eau» is part of a name (Eau Sauvage ≠ Sauvage)
  const wordsOutside = fragRowTokens(outside.replace(/eau\s+de\s+(parfum|toilette|cologne)/g, " "));
  const extraWords = wordsOutside.filter((w) => !known.has(w) && !FRAG_ROW_STOP_WORDS.has(w) && !/^\d/.test(w) && (w.length >= 3 || w === "eau"));
  // Every word of the perfume name must be there: «Chanel Coco» is not «Coco Mademoiselle»
  const insideAll = new Set(fragRowTokens(text));
  const missingNameWords = nameTokens.filter((w) => !brandTokens.includes(w) && !FRAG_ROW_STOP_WORDS.has(w) && w.length >= 2 && !insideAll.has(w));
  return { clone, notPerfume, concentration, concentrationOk, extraWords, missingNameWords };
}

// ─── «Есть в PriceMaster» для каталога ───────────────────────────────────────
// Индекс слов по всем активным строкам PriceMaster; аромат «найден», если есть строка с брендом вне
// скобок, всеми словами названия, без лишних слов (фланкеры) и не тестер/пробник/лосьон.

function fragPmIsTesterOrSample(name) {
  const text = String(name || "").toLowerCase().replace(/ё/g, "е");
  return /(^|[^a-zа-я])(tester|testr|тестер|sample|vial|пробник|отливант|распив|decant)/.test(text);
}

function fragPmVolumes(name) {
  return [...String(name || "").toLowerCase().matchAll(/(\d+(?:[.,]\d+)?)\s*(?:ml|мл)(?![a-zа-я])/g)]
    .map((m) => Number(m[1].replace(",", ".")))
    .filter((v) => v > 0 && v < 2000);
}

function buildFragranticaPmIndex(rows = []) {
  const postings = new Map();
  const tokenSets = rows.map((row, index) => {
    const tokens = new Set(fragRowTokens(row.name));
    for (const token of tokens) {
      let list = postings.get(token);
      if (!list) postings.set(token, (list = []));
      list.push(index);
    }
    return tokens;
  });
  return { rows, postings, tokenSets };
}

function matchFragranticaPmIndex(index, perfume = {}) {
  const nameTokens = [...new Set(fragRowTokens(perfume.name).filter((w) => !FRAG_ROW_STOP_WORDS.has(w)))];
  const brandTokens = fragRowTokens(perfume.brand).filter((w) => w.length >= 3);
  const required = [...new Set([...nameTokens, ...brandTokens])];
  if (!nameTokens.length) return { count: 0, minUsd: null, volumes: [] };
  let best = null;
  for (const token of required) {
    const list = index.postings.get(token);
    if (!list) {
      // a brand word may be abbreviated (YSL, D&G) — only name words are hard requirements
      if (nameTokens.includes(token)) return { count: 0, minUsd: null, volumes: [] };
      continue;
    }
    if (!best || list.length < best.length) best = list;
  }
  if (!best) return { count: 0, minUsd: null, volumes: [] };
  let count = 0;
  let minUsd = null;
  const volumes = new Set();
  for (const i of best) {
    const tokens = index.tokenSets[i];
    if (!nameTokens.every((w) => tokens.has(w))) continue;
    const row = index.rows[i];
    if (fragPmIsTesterOrSample(row.name)) continue;
    const rowVolumes = fragPmVolumes(row.name);
    if (rowVolumes.length && Math.max(...rowVolumes) <= 3) continue; // пробник / отливант
    const check = assessFragranticaSupplierRow(row.name, { brand: perfume.brand, name: perfume.name, typeKey: "edp" });
    if (check.clone || check.notPerfume || check.missingNameWords.length || check.extraWords.length) continue;
    count += 1;
    if (Number(row.usd) > 0 && (minUsd === null || Number(row.usd) < minUsd)) minUsd = Number(row.usd);
    for (const v of fragPmVolumes(row.name)) volumes.add(v);
  }
  return { count, minUsd, volumes: [...volumes].sort((a, b) => a - b) };
}

// ─── Маркет: характеристики категории «Парфюмерия» ───────────────────────────
// categoryParams — ответ /v2/category/{id}/parameters (id, name, type, values). Параметры ищутся по
// названию: id у Маркета стабильны, но так переживём и их смену. Пустые факты не передаются.
function buildFragranticaYandexParameters(categoryParams = [], facts = {}) {
  const byName = (re) => categoryParams.find((p) => re.test(String(p.name || "")));
  const out = [];
  const enumValue = (param, wanted) => {
    const w = String(wanted || "").toLowerCase().replace(/ё/g, "е").trim();
    if (!param || !w) return null;
    return (param.values || []).find((v) => String(v.value || "").toLowerCase().replace(/ё/g, "е").trim() === w) || null;
  };
  const pushEnum = (param, wanted) => {
    const hit = enumValue(param, wanted);
    if (hit) out.push({ parameterId: Number(param.id), valueId: Number(hit.id), value: String(hit.value) });
  };
  const pushText = (param, value) => {
    const text = String(value ?? "").trim();
    if (param && text) out.push({ parameterId: Number(param.id), value: text.slice(0, 1000) });
  };
  pushEnum(byName(/^Тип$/i), facts.typeLabel);
  pushEnum(byName(/^Пол$/i), facts.gender);
  const family = byName(/^Семейство$/i);
  if (family) {
    // «фужерные» с Фрагрантики — прямое совпадение; иначе по основе слова аккордов (до 2 значений)
    const words = [facts.family, ...(facts.accords || [])].filter(Boolean).map((w) => String(w).toLowerCase().replace(/ё/g, "е"));
    const picked = [];
    for (const word of words) {
      if (picked.length >= 2) break;
      const exact = enumValue(family, word);
      const stem = word.split(/[^а-яa-z]+/).filter(Boolean).pop() || "";
      const hit = exact || (stem.length >= 5 && (family.values || []).find((v) => {
        const name = String(v.value || "").toLowerCase().replace(/ё/g, "е");
        return !/\s/.test(name) && name.slice(0, 5) === stem.slice(0, 5);
      }));
      if (hit && !picked.some((p) => p.id === hit.id)) picked.push(hit);
    }
    for (const hit of picked) out.push({ parameterId: Number(family.id), valueId: Number(hit.id), value: String(hit.value) });
  }
  pushEnum(byName(/^Год$/i), facts.year ? String(facts.year) : "");
  pushText(byName(/^Верхние ноты$/i), (facts.topNotes || []).join(", "));
  pushText(byName(/^Средние ноты$/i), (facts.middleNotes || []).join(", "));
  pushText(byName(/^Базовые ноты$/i), (facts.baseNotes || []).join(", "));
  pushText(byName(/^Вес$/i), facts.netWeight ? String(facts.netWeight) : "");
  pushText(byName(/^Количество упаковок в товаре/i), "1");
  pushText(byName(/^Единиц в одной упаковке/i), "1");
  const tester = byName(/^Тестер$/i);
  if (tester) out.push({ parameterId: Number(tester.id), value: facts.tester ? "true" : "false" });
  return out;
}

// Ближайший сброс дневного лимита Ozon (00:00 UTC = 03:00 МСК) + 5 минут запаса.
function fragranticaNextOzonLimitReset(now = new Date(), resetAt = null) {
  const parsed = resetAt ? Date.parse(resetAt) : NaN;
  if (Number.isFinite(parsed) && parsed > now.getTime()) return new Date(parsed + 5 * 60_000);
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 5, 0));
  return next;
}

function isOzonLimitErrorText(text) {
  return /limit|лимит/i.test(String(text || "")) && /(exceed|превыш|исчерпа|reached|daily|суточн|дневн)/i.test(String(text || ""));
}
