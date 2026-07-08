// Справочник категорий Avito («Красота и здоровье») и классификатор товара по
// названию. Значения тегов GoodsType/GoodsSubType/SubType/PerfumeryType и
// AdType взяты из содержимого сохранённых страниц шаблонов Автозагрузки
// (avito/*.html, парсер scripts/avito-parse-category-specs.cjs). Имена файлов
// страниц сдвинуты относительно содержимого и не используются.
// ВАЖНО: канонический AdType — «Товар приобретен на продажу» (без «ё»).

const AVITO_FEED_CATEGORY = "Красота и здоровье";

const AVITO_AD_TYPES = ["Товар приобретен на продажу", "Товар от производителя"];
const AVITO_CONDITIONS = ["Новое", "Б/у"];

// condition: у Парфюмерии тег Condition обязателен, у «Уход и гигиена» и
// «Средства для волос» его в шаблоне нет — не выводим.
// gender: страницы духов/наборов/масел содержат тег Gender.
const AVITO_CATEGORY_SPECS = [
  { key: "parfum-edt", label: "Парфюмерия / Духи и туалетная вода", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Духи и туалетная вода" }, condition: true, gender: true },
  // Пробники/отливанты: валидатор Avito отклоняет их с PerfumeryType «Духи и
  // туалетная вода» («Неправильно заполнен обязательный параметр — Тип
  // парфюмерии») и сам подсказывает значение «Пробники и отливанты».
  { key: "parfum-samples", label: "Парфюмерия / Пробники и отливанты", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Пробники и отливанты" }, condition: true, gender: true },
  { key: "parfum-sets", label: "Парфюмерия / Парфюмерные наборы", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Парфюмерные наборы" }, condition: true, gender: true },
  { key: "parfum-oils", label: "Парфюмерия / Парфюмерные масла", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Парфюмерные масла" }, condition: true, gender: true },
  { key: "parfum-diffusers", label: "Парфюмерия / Диффузоры, спреи и саше", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Диффузоры, спреи и саше" }, condition: true, gender: false },
  { key: "parfum-atomizers", label: "Парфюмерия / Атомайзеры и флаконы", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Атомайзеры и флаконы" }, condition: true, gender: false },
  { key: "parfum-other", label: "Парфюмерия / Другое", tags: { GoodsType: "Парфюмерия", PerfumeryType: "Другое" }, condition: true, gender: false },
  { key: "face-creams", label: "Уход за лицом / Кремы", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Кремы" }, condition: false, gender: false },
  { key: "face-masks", label: "Уход за лицом / Маски", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Маски" }, condition: false, gender: false },
  { key: "face-oils", label: "Уход за лицом / Масла", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Масла" }, condition: false, gender: false },
  { key: "face-patches", label: "Уход за лицом / Патчи", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Патчи" }, condition: false, gender: false },
  { key: "face-scrubs", label: "Уход за лицом / Скрабы и пилинги", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Скрабы и пилинги" }, condition: false, gender: false },
  { key: "face-cleansers", label: "Уход за лицом / Средства для умывания", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Средства для умывания" }, condition: false, gender: false },
  { key: "face-serums", label: "Уход за лицом / Сыворотки и эссенции", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Сыворотки и эссенции" }, condition: false, gender: false },
  { key: "face-toners", label: "Уход за лицом / Тоники и лосьоны", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Тоники и лосьоны" }, condition: false, gender: false },
  { key: "face-fluids", label: "Уход за лицом / Эмульсии и флюиды", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за лицом", SubType: "Эмульсии и флюиды" }, condition: false, gender: false },
  { key: "body-deo", label: "Уход за телом / Дезодоранты", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Дезодоранты" }, condition: false, gender: false },
  { key: "body-creams", label: "Уход за телом / Кремы", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Кремы" }, condition: false, gender: false },
  { key: "body-correctors", label: "Уход за телом / Корректирующие средства", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Корректирующие средства" }, condition: false, gender: false },
  { key: "body-lotions", label: "Уход за телом / Лосьоны, спреи и молочко", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Лосьоны, спреи и молочко" }, condition: false, gender: false },
  { key: "body-oils", label: "Уход за телом / Масла", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Масла" }, condition: false, gender: false },
  { key: "body-soap", label: "Уход за телом / Мыло", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Мыло" }, condition: false, gender: false },
  { key: "body-scrubs", label: "Уход за телом / Скрабы и пилинги", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Скрабы и пилинги" }, condition: false, gender: false },
  { key: "body-shower", label: "Уход за телом / Средства для душа и ванны", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за телом", SubType: "Средства для душа и ванны" }, condition: false, gender: false },
  { key: "care-sets", label: "Уход и гигиена / Наборы уходовой косметики", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Наборы уходовой косметики" }, condition: false, gender: false },
  { key: "care-devices", label: "Уход и гигиена / Приборы и аксессуары", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Приборы и аксессуары" }, condition: false, gender: false },
  { key: "care-hygiene", label: "Уход и гигиена / Гигиена и контрацепция", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Гигиена и контрацепция" }, condition: false, gender: false },
  { key: "care-oral", label: "Уход и гигиена / Уход за полостью рта", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Уход за полостью рта" }, condition: false, gender: false },
  { key: "care-sun", label: "Уход и гигиена / Загар и защита от солнца", tags: { GoodsType: "Уход и гигиена", GoodsSubType: "Загар и защита от солнца" }, condition: false, gender: false },
  { key: "hair", label: "Средства для волос", tags: { GoodsType: "Средства для волос" }, condition: false, gender: false },
];

const AVITO_CATEGORY_SPEC_BY_KEY = new Map(AVITO_CATEGORY_SPECS.map((spec) => [spec.key, spec]));
const AVITO_DEFAULT_CATEGORY_KEY = "parfum-edt";

function getAvitoCategorySpec(key) {
  return AVITO_CATEGORY_SPEC_BY_KEY.get(cleanText(key)) || null;
}

function normalizeAvitoAdType(value, fallback = AVITO_AD_TYPES[0]) {
  const text = cleanText(value).replace(/ё/g, "е").toLowerCase();
  return AVITO_AD_TYPES.find((item) => item.toLowerCase() === text) || fallback;
}

function normalizeAvitoCondition(value, fallback = AVITO_CONDITIONS[0]) {
  const text = cleanText(value).replace(/ё/g, "е").toLowerCase();
  return AVITO_CONDITIONS.find((item) => item.replace(/ё/g, "е").toLowerCase() === text) || fallback;
}

// Классификация по названию: упорядоченные правила, первое совпадение
// побеждает. Специфичные категории (наборы, масла-духи, волосы) проверяются
// раньше общих («крем», «духи»), чтобы не утекать в широкие категории.
function classifyAvitoCategory(title, { defaultCategoryKey = AVITO_DEFAULT_CATEGORY_KEY } = {}) {
  const text = ` ${normalizeAvitoMatchText(title)} `;
  const perfumeContext = /дух|парфюм|туалетн|одеколон|edp|edt|edc|parfum|cologne|extrait/;
  const pick = (key, autoDefaulted = false) => ({
    key,
    spec: getAvitoCategorySpec(key) || getAvitoCategorySpec(AVITO_DEFAULT_CATEGORY_KEY),
    autoDefaulted,
  });

  if (/пробник|отливант|\bдекант\b|\bdecant\b|\bsample\b/.test(text) && perfumeContext.test(text)) {
    return pick("parfum-samples");
  }
  if (/набор|подарочн|gift set|\bset\b|\bkit\b/.test(text)) {
    if (perfumeContext.test(text)) return pick("parfum-sets");
    if (/крем|маск|сыворот|уход|космети|шампун|патч/.test(text)) return pick("care-sets");
  }
  if (/атомайзер|atomizer|флакон дл|пустой флакон/.test(text)) return pick("parfum-atomizers");
  if (/диффузор|саше|аромат для дома|home spray|благовони/.test(text)) return pick("parfum-diffusers");
  if (/масля?н\w* дух|дух\w* масл|парфюмерн\w+ масл|масло-дух|\battar\b/.test(text)) return pick("parfum-oils");
  if (/шампун|кондиционер для волос|бальзам для волос|маск\w* для волос|масл\w* для волос|для роста волос|укладк/.test(text)) return pick("hair");
  if (/дезодорант|антиперспирант/.test(text)) return pick("body-deo");
  if (/\bspf\b|солнцезащит|автозагар|после загара|для загара|от солнца/.test(text)) return pick("care-sun");
  if (/зубн|полост\w* рта|ополаскиватель для рта|ирригатор/.test(text)) return pick("care-oral");
  if (/презерватив|прокладк|тампон|интимн\w* гигиен/.test(text)) return pick("care-hygiene");
  if (/патч/.test(text)) return pick("face-patches");
  if (/мицелляр|тоник/.test(text)) return pick("face-toners");
  if (/сыворотк|эссенци|\bserum\b/.test(text)) return pick("face-serums");
  if (/эмульси|флюид/.test(text)) return pick("face-fluids");
  if (/умывани|очищающ/.test(text)) return pick("face-cleansers");
  if (/скраб|пилинг|гоммаж/.test(text)) {
    return /для тела|для рук|для ног/.test(text) ? pick("body-scrubs") : pick("face-scrubs");
  }
  if (/маск/.test(text)) return pick("face-masks");
  if (/гель для душа|пена для ванн|для душа и ванн/.test(text)) return pick("body-shower");
  if (/мыло/.test(text)) return pick("body-soap");
  if (/корректирующ|корректор/.test(text)) return pick("body-correctors");
  if (/лосьон для лица/.test(text)) return pick("face-toners");
  if (/лосьон|молочко|спрей для тела/.test(text)) return pick("body-lotions");
  if (/крем/.test(text)) {
    return /для рук|для ног|для тела/.test(text) ? pick("body-creams") : pick("face-creams");
  }
  if (/масло/.test(text) && !perfumeContext.test(text)) {
    return /для тела|массаж/.test(text) ? pick("body-oils") : pick("face-oils");
  }
  if (/эпилятор|триммер|массаж[её]р|расческ|щетка для/.test(text)) return pick("care-devices");
  if (perfumeContext.test(text)) return pick("parfum-edt");
  const fallbackKey = getAvitoCategorySpec(defaultCategoryKey) ? cleanText(defaultCategoryKey) : AVITO_DEFAULT_CATEGORY_KEY;
  return pick(fallbackKey, true);
}

// Gender для парфюмерных категорий (тег опционален, но повышает качество карточки).
function detectAvitoPerfumeGender(title) {
  const text = ` ${normalizeAvitoMatchText(title)} `;
  if (/унисекс|unisex/.test(text)) return "Унисекс";
  const female = /женск|для женщин|для нее|woman|women|femme|for her|\blady\b|\bher\b/.test(text);
  const male = /мужск|для мужчин|для него|\bmen\b|\bman\b|homme|for him|\bhis\b/.test(text);
  if (female && male) return "Унисекс";
  if (female) return "Женщины";
  if (male) return "Мужчины";
  return "";
}

// Читаемый путь категории для UI и сводок.
function avitoListingCategoryPath(listing = {}) {
  const spec = getAvitoCategorySpec(listing.categoryKey);
  if (spec) return spec.label;
  return [cleanText(listing.goodsType), cleanText(listing.goodsSubType), cleanText(listing.subType), cleanText(listing.perfumeryType)]
    .filter(Boolean)
    .join(" / ");
}
