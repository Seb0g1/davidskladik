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
};

// ОКПД2 как в действующих карточках Маркета: духи .110, парфюмерная/туалетная вода .120, одеколон .130.
function fragOkpd2ForType(typeKey = "edp") {
  if (typeKey === "parfum" || typeKey === "oil") return "20.42.11.110";
  if (typeKey === "cologne") return "20.42.11.130";
  return "20.42.11.120";
}

// Штрихкод производителя: только настоящий GTIN (8/12/13/14 цифр с верной контрольной цифрой).
function fragValidGtin(value) {
  const digits = String(value || "").replace(/\s+/g, "");
  if (!/^(\d{8}|\d{12}|\d{13}|\d{14})$/.test(digits)) return "";
  const nums = digits.split("").map(Number);
  const check = nums.pop();
  const sum = nums.reverse().reduce((acc, n, i) => acc + n * (i % 2 === 0 ? 3 : 1), 0);
  return (10 - (sum % 10)) % 10 === check ? digits : "";
}

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
  // «attar» is not a type: Attar Collection makes eau de parfum
  if (/perfume oil|parfum oil|масло/.test(text)) return "oil";
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

// Маркет даёт 10 из 10 за название вида «Парфюмерная вода Creed Iris Debonair Eau de Parfum унисекс 50 мл»:
// тип продукта, бренд, аромат, концентрация, для кого (в роде типа), тестер, объём.
const FRAG_MARKET_GENDER = {
  edp: { male: "мужская", female: "женская", unisex: "унисекс" },
  edt: { male: "мужская", female: "женская", unisex: "унисекс" },
  parfum: { male: "мужские", female: "женские", unisex: "унисекс" },
  cologne: { male: "мужской", female: "женский", unisex: "унисекс" },
  oil: { male: "мужские", female: "женские", unisex: "унисекс" },
};
// Концентрация на латинице после аромата — с ней Маркет даёт за название 10 из 10
const FRAG_MARKET_CONCENTRATION = { edp: "Eau de Parfum", edt: "Eau de Toilette", parfum: "Parfum", cologne: "Eau de Cologne", oil: "Perfume Oil" };
function buildFragranticaMarketName({ perfume = {}, typeKey = "edp", volume, tester = false } = {}) {
  const type = fragOzonTypeByKey(typeKey);
  const vol = fragFormatVolume(volume);
  const gender = (FRAG_MARKET_GENDER[type.key] || FRAG_MARKET_GENDER.edp)[perfume.gender] || "";
  const aroma = fragNameWithBrand(perfume);
  const conc = FRAG_MARKET_CONCENTRATION[type.key] || "";
  const withConc = conc && !aroma.toLowerCase().includes(conc.toLowerCase()) ? `${aroma} ${conc}` : aroma;
  const base = [type.nameLabel, withConc, gender, tester ? "тестер" : "", vol ? `${vol} мл` : ""].filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
  // Market wants 60–120 characters («тип + бренд + модель + особенности»): a short name gets the aroma's
  // real character from Fragrantica (family or the top accords)
  if (base.length >= 60) return base;
  // accords are masculine adjectives («цветочный, фруктовый») → «… аромат»; the family («фужерные») goes as is
  const accords = (perfume.accords || []).slice(0, 2).map((a) => String(a?.name || a || "").trim().toLowerCase()).filter(Boolean);
  const family = String(perfume.family || "").trim().toLowerCase();
  const longer = accords.length ? `${base}, ${accords.join(" ")} аромат` : family ? `${base}, ${family}` : base;
  return longer.length <= 120 ? longer : base;
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
  const gtin = fragValidGtin(input.barcode);
  if (gtin) item.barcode = gtin;
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
  // former / full brand names that supplier price lists still use
  "mugler": ["thierry"],
  "jo malone": ["london"],
  "jo malone london": ["jo", "malone"],
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
  // brand aliases are part of the brand, not extra words («Thierry Mugler» = Mugler)
  const known = new Set([...brandTokens, ...fragRowTokens(brand), ...nameTokens, ...aliases.flatMap((alias) => fragRowTokens(alias))]);
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

// opts.russianExtrasOk — our own warehouse titles: Russian words («для мужчин», «парфюмерная вода»)
// only describe the product; Latin extra words (Elixir, Intense…) still mean another perfume
/** Bare numbers in a product name that are not its volume / a decimal / a pack count and not part of the perfume or brand name. */
function fragExtraNameNumbers(rowName, perfume = {}, rowVolumes = []) {
  const text = String(rowName || "").toLowerCase().replace(/ё/g, "е").replace(/\([^)]*\)/g, " ");
  const known = new Set([...fragRowTokens(perfume.name), ...fragRowTokens(perfume.brand)]);
  const volumes = new Set((rowVolumes || []).map((v) => String(Number(v))));
  // where the perfume's own name starts: a number before it is a line / series («03 Vanilla», «№6 Brume»).
  // The first name word that is not a brand word; if the name is the brand word itself («Billie Eilish Eilish»),
  // its last occurrence.
  const brandWords = fragRowTokens(perfume.brand);
  const nameWords = fragRowTokens(perfume.name).filter((w) => !/^\d+$/.test(w));
  const firstWord = nameWords.find((w) => !brandWords.includes(w)) || nameWords[0];
  let at = -1;
  if (firstWord) {
    // tokens are letters and digits only — nothing to escape
    const wordRe = new RegExp(`(^|[^0-9a-zа-я])${firstWord}(?![0-9a-zа-я])`, "g");
    for (const m of text.matchAll(wordRe)) at = m.index;
  }
  const out = [];
  // a bare 1–3 digit number: not glued to letters, not a decimal («3.4»), not followed by a unit or «x» (pack),
  // not part of «17/17» or «/43»
  const re = /(^|[^0-9a-zа-я.,/])(\d{1,3})(?![0-9a-zа-я/]|[.,]\d|\s*(ml|мл|oz|fl|шт|x|х|×|\*|%|г|g)(?![a-zа-я]))/gi;
  for (const m of text.matchAll(re)) {
    const n = m[2];
    if (at < 0 || m.index < at) continue;
    if (known.has(n) || volumes.has(String(Number(n)))) continue;
    out.push(n);
  }
  return out;
}

function matchFragranticaPmIndex(index, perfume = {}, opts = {}) {
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
  const matched = [];
  for (const i of best) {
    const tokens = index.tokenSets[i];
    if (!nameTokens.every((w) => tokens.has(w))) continue;
    const row = index.rows[i];
    // opts.keepTesters — the caller sorts testers out itself («Подбор поставщиков»: a tester card takes tester rows)
    if (!opts.keepTesters && fragPmIsTesterOrSample(row.name)) continue;
    const rowVolumes = fragPmVolumes(row.name);
    if (rowVolumes.length && Math.max(...rowVolumes) <= 3) continue; // пробник / отливант
    const check = assessFragranticaSupplierRow(row.name, { brand: perfume.brand, name: perfume.name, typeKey: "edp" });
    const extra = opts.russianExtrasOk ? check.extraWords.filter((w) => !/[а-яё]/i.test(w)) : check.extraWords;
    if (check.clone || check.notPerfume || check.missingNameWords.length || extra.length) continue;
    // cards: a number that is not a volume means another perfume («Torino 21» ≠ «Torino», «Code 2» ≠ «Code»)
    if (opts.strictNumbers && fragExtraNameNumbers(row.name, perfume, rowVolumes).length) continue;
    count += 1;
    matched.push(i);
    if (Number(row.usd) > 0 && (minUsd === null || Number(row.usd) < minUsd)) minUsd = Number(row.usd);
    for (const v of fragPmVolumes(row.name)) volumes.add(v);
  }
  return { count, minUsd, volumes: [...volumes].sort((a, b) => a - b), matched };
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
  // Enum that accepts own values: the dictionary value when it matches, else the text itself
  const pushEnumOrCustom = (param, wanted) => {
    const text = String(wanted || "").trim();
    if (!param || !text) return;
    const hit = enumValue(param, text);
    if (hit) out.push({ parameterId: Number(param.id), valueId: Number(hit.id), value: String(hit.value) });
    else if (param.allowCustomValues !== false) out.push({ parameterId: Number(param.id), value: text.slice(0, 200) });
  };
  pushEnumOrCustom(byName(/^Линейка$/i), facts.line);
  pushEnumOrCustom(byName(/^Особенности флакона$/i), facts.bottleFeature);
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

// ─── Конвейер: объёмы из PriceMaster, что уже есть в магазинах, тело отправки ──
// (02d-fragrantica-drafts.js). Чистые функции — тесты в test/fragrantica-ozon-build.test.cjs.

const FRAG_SET_RE = /\+|набор|(^|[^a-z])(set|kit|coffret|discovery)(?![a-z])|\d\s*[xх×*]\s*\d|\d+\s*шт/i;

// Концентрация названа в самом названии аромата («Sauvage Eau de Parfum», «Bleu de Chanel Parfum»)?
function fragExplicitTypeKey(perfume = {}) {
  const guess = fragOzonGuessTypeKey(perfume);
  if (guess !== "edp") return guess;
  return /eau de parfum|\bedp\b|парфюмерн/i.test(`${perfume.name || ""}`) ? "edp" : "";
}

/**
 * Which cards to make for a perfume from its PriceMaster rows: the type (named in the perfume name, else the
 * concentration most rows have, else EDP) and every bottle volume that boxed, same-perfume rows of that type
 * offer. Testers, samples (≤ 3 ml), sets, clones, flankers and lotions are left out.
 * rows: [{ name, available? }]. Returns { typeKey, volumes: [{ volume, rows }] } sorted by volume.
 */
function planFragranticaVolumes(rows = [], perfume = {}) {
  const good = [];
  for (const row of rows) {
    const name = String(row?.name || "");
    if (!name || row.available === false) continue;
    if (fragPmIsTesterOrSample(name) || FRAG_SET_RE.test(name)) continue;
    const volumes = fragPmVolumes(name);
    if (volumes.length !== 1 || volumes[0] <= 3) continue;
    const check = assessFragranticaSupplierRow(name, { brand: perfume.brand, name: fragStripConcentration(perfume.name), typeKey: "edp" });
    if (check.clone || check.notPerfume || check.extraWords.length || check.missingNameWords.length) continue;
    good.push({ volume: volumes[0], concentration: check.concentration });
  }
  let typeKey = fragExplicitTypeKey(perfume);
  if (!typeKey) {
    const counts = new Map();
    for (const r of good) if (r.concentration) counts.set(r.concentration, (counts.get(r.concentration) || 0) + 1);
    typeKey = [...counts.entries()].sort((a, b) => b[1] - a[1] || (a[0] === "edp" ? -1 : 1))[0]?.[0] || "edp";
  }
  const wanted = typeKey === "oil" ? "parfum" : typeKey;
  const byVolume = new Map();
  for (const r of good) {
    if (r.concentration && r.concentration !== wanted) continue;
    const key = Math.round(r.volume * 100) / 100;
    byVolume.set(key, (byVolume.get(key) || 0) + 1);
  }
  return { typeKey, volumes: [...byVolume.entries()].sort((a, b) => a[0] - b[0]).map(([volume, n]) => ({ volume, rows: n })) };
}

/**
 * Is this volume already sold in the shop? A live Fragrantica export of that volume (not failed, not a
 * tester) or a warehouse card of that shop with that volume in its name (shop_stock { "<shop>": { v: [] } }).
 */
function fragranticaVolumeInShop({ exports = [], stock = {}, shopId, volume, tester = false }) {
  const v = Number(volume);
  const same = (x) => Math.abs(Number(x) - v) < 0.01;
  if (exports.some((e) => String(e.accountId) === String(shopId) && e.status !== "failed" && Boolean(e.tester) === Boolean(tester) && same(e.volume))) return true;
  if (tester) return false;
  const had = stock?.[shopId];
  return Boolean(had && Array.isArray(had.v) && had.v.some(same));
}

// Атрибуты, которые форма собирает из своих полей (название, артикул, тип, описание, вес)
const FRAG_DRAFT_OWN_ATTRS = new Set([4180, 9024, 8229, 4497, 4191]);

/**
 * Body for createFragranticaExports from a ready draft. data — what the conveyor built (form + links + photos +
 * description); targets — [{ key, style }] of the shops chosen for this card. The shop's «Пирамида аромата»
 * goes in without a manual approval.
 */
function buildFragranticaDraftExportBody(draft = {}, targets = []) {
  const d = draft.data || {};
  const type = fragOzonTypeByKey(d.typeKey);
  const attributes = (Array.isArray(d.attributes) ? d.attributes : [])
    .filter((a) => !FRAG_DRAFT_OWN_ATTRS.has(Number(a.id)) && Array.isArray(a.values) && a.values.length)
    .map((a) => ({ id: Number(a.id), values: a.values }));
  attributes.push({ id: FRAG_OZON_ATTR.name, values: [{ value: d.name || "" }] });
  attributes.push({ id: FRAG_OZON_ATTR.sellerCode, values: [{ value: d.offerId || "" }] });
  attributes.push({ id: FRAG_OZON_ATTR.type, values: [{ dictionary_value_id: type.typeId, value: type.label }] });
  if (String(d.description || "").trim()) attributes.push({ id: FRAG_OZON_ATTR.annotation, values: [{ value: d.description }] });
  if (Number(d.dims?.weight)) attributes.push({ id: FRAG_OZON_ATTR.weightPack, values: [{ value: String(d.dims.weight) }] });
  const hasOzon = targets.some((t) => String(t.key).startsWith("ozon:"));
  const price = hasOzon ? d.price : d.price || d.yandexPrice;
  return {
    perfumeId: Number(draft.perfumeId),
    typeId: type.typeId,
    typeKey: type.key,
    targets: targets.map((t) => ({ key: t.key, notes: d.images?.notes?.[t.style] || null })),
    offerId: d.offerId,
    name: d.name,
    price: price ? String(price) : "",
    oldPrice: d.oldPrice ? String(d.oldPrice) : "",
    yandexPrice: d.yandexPrice ? String(d.yandexPrice) : "",
    barcode: d.barcode || "",
    depth: d.dims?.depth, width: d.dims?.width, height: d.dims?.height, weight: d.dims?.weight,
    tester: Boolean(d.tester),
    improve: Boolean(d.existing),
    pricesByTarget: d.existing?.prices || null,
    ownBottleOnly: Boolean(d.ownBottleOnly),
    // own photos: the first replaces the bottle photo, the rest follow it (onlyCustomPhotos drops the generated ones)
    images: [(d.customPhotos || [])[0] || d.images?.main || d.sourceImage || ""].filter(Boolean),
    customPhotos: (d.customPhotos || []).slice(1),
    onlyCustomPhotos: Boolean(d.onlyCustomPhotos && (d.customPhotos || []).length),
    attributes,
    links: (Array.isArray(d.linkRows) ? d.linkRows : [])
      .filter((r) => (d.selectedLinks || []).includes(r.id))
      .map((r) => ({ rowId: r.rowId, article: r.article, name: r.name, supplierName: r.supplierName, partnerId: r.partnerId, priceCurrency: r.priceCurrency })),
  };
}

/** What a draft still lacks (same rules as the export: required Ozon attributes, price, photo, sizes). */
function fragranticaDraftMissing(draft = {}, targets = []) {
  const body = buildFragranticaDraftExportBody(draft, targets);
  const required = (draft.data?.requiredAttrs || []).map((a) => ({ id: a.id, name: a.name, is_required: true }));
  const { missing } = buildFragranticaOzonItem(body, required);
  if (!targets.length) missing.unshift("Магазины");
  return missing;
}

// ─── Rich-контент Ozon (атрибут 11254) ───────────────────────────────────────
// Ozon прячет аннотацию, если есть Rich-контент, но её текст по-прежнему участвует в поиске — поэтому
// описание остаётся в 4191, а Rich собирается из того же текста и фото карточки: пирамида аромата,
// абзацы описания, «Характеристики», крупный план. Формат — JSON конструктора Ozon (version 0.3).

const FRAG_RICH_ATTR = 11254;

function fragRichImage(src, alt, width = 1500, height = 2000) {
  return {
    widgetName: "raShowcase",
    type: "roll",
    blocks: [{
      imgLink: "",
      img: { src, srcMobile: src, alt: String(alt || "").slice(0, 200), position: "width_full", positionMobile: "width_full", widthMobile: width, heightMobile: height, isParandjaMobile: false },
    }],
  };
}

function fragRichText(title, paragraphs = []) {
  const block = {
    widgetName: "raTextBlock",
    theme: "default",
    padding: "type2",
    gapSize: "m",
    text: { size: "size2", align: "left", color: "color1", content: paragraphs.map((p) => String(p).slice(0, 2000)) },
  };
  if (title) block.title = { content: [String(title).slice(0, 200)], size: "size5", align: "left", color: "color1" };
  return block;
}

/**
 * description — plain text with blank-line paragraphs; images — absolute URLs { notes, specs, closeup }.
 * Returns the JSON string for attribute 11254, or "" when there is nothing to show.
 */
function buildFragranticaRichContent({ title = "", description = "", images = {} } = {}) {
  const paragraphs = String(description || "")
    .replace(/<br\s*\/?>/gi, "\n")
    .split(/\n\s*\n|\n/)
    .map((p) => p.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim())
    .filter(Boolean);
  const content = [];
  if (images.notes) content.push(fragRichImage(images.notes, `${title} — пирамида аромата`));
  const half = Math.max(1, Math.ceil(paragraphs.length / 2));
  if (paragraphs.length) content.push(fragRichText(title, paragraphs.slice(0, half)));
  if (images.specs) content.push(fragRichImage(images.specs, `${title} — характеристики`));
  if (paragraphs.length > half) content.push(fragRichText("", paragraphs.slice(half)));
  if (images.closeup) content.push(fragRichImage(images.closeup, `${title} — флакон крупным планом`));
  if (content.length < 2) return "";
  return JSON.stringify({ content, version: 0.3 });
}

/** Ozon video cover (complex attribute 100002 / 21845) for /v3/product/import. */
function buildFragranticaVideoCoverComplex(url) {
  if (!url) return [];
  return [{ attributes: [{ id: 21845, complex_id: 100002, values: [{ dictionary_value_id: 0, value: url }] }] }];
}

/** Import errors that belong to the Rich-контент or the video cover (the card is then resent without them). */
function fragranticaMediaExtrasFailed(errors = []) {
  return (Array.isArray(errors) ? errors : []).some((e) => {
    const id = Number(e?.attribute_id || 0);
    const text = `${e?.attribute_name || ""} ${e?.description || ""} ${e?.message || ""} ${e?.code || ""}`;
    return id === FRAG_RICH_ATTR || id === 21845 || /rich|видеообложк|video|21845|11254/i.test(text);
  });
}

// ─── Конвейер: индекс строк PriceMaster в памяти ─────────────────────────────
// Вместо SQL-поиска на каждый аромат и объём — все активные строки последних прайсов один раз
// (02d-fragrantica-drafts.js, кэш 10 мин) и поиск по словам названия за миллисекунды.

// «Eau de Parfum», «Extrait de Parfum» в названии аромата — это концентрация, у поставщиков её пишут «EDP»
const FRAG_CONCENTRATION_PHRASE_RE = /\b(eau\s+de\s+(parfum|toilette|cologne)|extrait\s+de\s+parfum|parfum\s+extrait|eau\s+fraiche)\b/gi;

function fragStripConcentration(name) {
  return String(name || "").replace(FRAG_CONCENTRATION_PHRASE_RE, " ").replace(/\s+/g, " ").trim();
}

function fragPmSearchTokens(text) {
  return fragRowTokens(String(text || "").normalize("NFD").replace(/[̀-ͯ]/g, ""));
}

function buildFragranticaRowIndex(rows = []) {
  const postings = new Map();
  const tokenSets = rows.map((row, index) => {
    const tokens = new Set(fragPmSearchTokens(row.name));
    for (const token of tokens) {
      let list = postings.get(token);
      if (!list) postings.set(token, (list = []));
      list.push(index);
    }
    return tokens;
  });
  return { rows, postings, tokenSets };
}

/** Rows that contain every word of the perfume name (accents ignored, concentration words dropped). */
function findFragranticaPmCandidates(index, perfume = {}, limit = 600) {
  const nameTokens = [...new Set(fragPmSearchTokens(fragStripConcentration(perfume.name)).filter((w) => !FRAG_ROW_STOP_WORDS.has(w)))];
  if (!nameTokens.length || !index?.postings) return [];
  let best = null;
  for (const token of nameTokens) {
    const list = index.postings.get(token);
    if (!list) return [];
    if (!best || list.length < best.length) best = list;
  }
  const out = [];
  for (const i of best) {
    if (nameTokens.every((w) => index.tokenSets[i].has(w))) out.push(index.rows[i]);
    if (out.length >= limit) break;
  }
  return out;
}
