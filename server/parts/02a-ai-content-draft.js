function latestAiContentDraft(product = {}, status = "pending") {
  const normalizedStatus = cleanText(status).toLowerCase();
  return [...(normalizeWarehouseProduct(product).aiContentDrafts || [])]
    .filter((draft) => !normalizedStatus || draft.status === normalizedStatus)
    .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))[0] || null;
}

function latestAiImageBatch(product = {}, status = "pending") {
  const normalizedStatus = cleanText(status).toLowerCase();
  const drafts = [...(normalizeWarehouseProduct(product).aiImages || [])]
    .filter((draft) => (!normalizedStatus || draft.status === normalizedStatus) && draft.resultUrl)
    .sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  if (!drafts.length) return { batchId: "", drafts: [] };
  const latest = drafts[drafts.length - 1];
  const batchId = latest.batchId || latest.id;
  return {
    batchId,
    drafts: drafts.filter((draft) => (latest.batchId ? draft.batchId === latest.batchId : draft.id === latest.id)),
  };
}

function buildAiQualityReviewRow(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const imageBatch = latestAiImageBatch(normalized, "pending");
  return {
    product: {
      id: normalized.id,
      offerId: normalized.offerId,
      name: normalized.name,
      marketplace: normalized.marketplace,
      target: normalized.target,
      imageUrl: normalized.imageUrl,
      updatedAt: normalized.updatedAt,
      cardQuality: normalized.yandex?.extra?.cardQuality || null,
    },
    contentDraft: latestAiContentDraft(normalized, "pending"),
    imageBatchId: imageBatch.batchId,
    imageDrafts: imageBatch.drafts,
  };
}

function compactAiText(value = "", maxLength = 6000) {
  return cleanText(value).replace(/\s+/g, " ").slice(0, maxLength);
}

function productContentQuality(product = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const yandex = normalized.yandex || {};
  const name = cleanText(marketplace === "yandex" ? (yandex.name || ozon.name || normalized.name) : (ozon.name || normalized.name));
  const description = cleanText(marketplace === "yandex" ? (yandex.description || ozon.description || normalized.description) : (ozon.description || normalized.description));
  const vendor = cleanText(marketplace === "yandex" ? (yandex.vendor || ozon.vendor || normalized.brand) : (ozon.vendor || normalized.brand));
  const reasons = [];
  if (!name) reasons.push("no_name");
  if (!vendor || /без бренда/i.test(vendor)) reasons.push("weak_vendor");
  if (!description) reasons.push("no_description");
  if (description && description.length < 800) reasons.push("short_description");
  if (description && name && description.toLowerCase() === name.toLowerCase()) reasons.push("description_equals_name");
  if (/^(описание|товар|парфюмерная вода|духи|туалетная вода)$/i.test(description)) reasons.push("generic_description");
  const built = marketplace === "yandex" ? buildYandexOfferMapping(normalized) : { missing: [], ready: true };
  return {
    marketplace,
    ready: Boolean(built.ready && !reasons.includes("no_description") && !reasons.includes("description_equals_name")),
    missing: built.missing || [],
    reasons,
    nameLength: name.length,
    descriptionLength: description.length,
  };
}

function applyAiContentDraftToProduct(product = {}, draft = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const next = { ...normalized };
  const cleanDraft = {
    name: compactAiText(draft.name, 240),
    description: normalizeParagraphText(draft.description, 5000),
    vendor: compactAiText(draft.vendor, 120),
    bulletPoints: Array.isArray(draft.bulletPoints || draft.bullets)
      ? (draft.bulletPoints || draft.bullets).map((item) => compactAiText(item, 180)).filter(Boolean).slice(0, 8)
      : [],
    seoKeywords: Array.isArray(draft.seoKeywords || draft.keywords)
      ? (draft.seoKeywords || draft.keywords).map((item) => compactAiText(item, 80)).filter(Boolean).slice(0, 12)
      : [],
  };
  if (marketplace === "yandex") {
    const current = next.yandex || {};
    next.yandex = normalizeYandexDraft({
      ...current,
      name: cleanDraft.name || current.name || next.ozon?.name || next.name,
      description: cleanDraft.description || current.description || next.ozon?.description || next.name,
      vendor: cleanDraft.vendor || current.vendor || next.ozon?.vendor || next.brand || "Без бренда",
      extra: {
        ...parseJsonField(current.extra, {}),
        aiBulletPoints: cleanDraft.bulletPoints,
        aiSeoKeywords: cleanDraft.seoKeywords,
        aiContentUpdatedAt: new Date().toISOString(),
      },
    });
    if (next.marketplace === "yandex") {
      next.name = next.yandex.name || next.name;
      next.description = next.yandex.description || next.description;
      next.brand = next.yandex.vendor || next.brand;
    }
  }
  if (marketplace === "ozon") {
    const current = next.ozon || {};
    next.ozon = normalizeOzonDraft({
      ...current,
      name: cleanDraft.name || current.name || next.yandex?.name || next.name,
      description: cleanDraft.description || current.description || next.yandex?.description || next.description || next.name,
      vendor: cleanDraft.vendor || current.vendor || next.yandex?.vendor || next.brand,
      extra: {
        ...parseJsonField(current.extra, {}),
        aiBulletPoints: cleanDraft.bulletPoints,
        aiSeoKeywords: cleanDraft.seoKeywords,
        aiContentUpdatedAt: new Date().toISOString(),
      },
    });
    if (next.marketplace === "ozon") {
      next.name = next.ozon.name || next.name;
      next.description = next.ozon.description || next.description;
      next.brand = next.ozon.vendor || next.brand;
    }
  }
  return normalizeWarehouseProduct(next);
}

// ─── Описания парфюмерии (промпт agent-prompts/deepseek-text-ai.md) ─────────

const AI_PERFUME_ATTRIBUTE_NAMES = {
  85: "Бренд", 9048: "Модель", 4389: "Страна", 8163: "Объём, мл", 9163: "Пол", 8008: "Классификация аромата",
  8050: "Состав / ноты", 8229: "Тип", 9390: "Целевая аудитория", 4191: "Аннотация",
};

function aiPerfumeTypeFromText(text = "") {
  const value = String(text || "").toLowerCase();
  if (/парфюмерн|(^|[^a-z])edp([^a-z]|$)|eau de parfum/.test(value)) return "Парфюмерная вода";
  if (/туалетн|(^|[^a-z])edt([^a-z]|$)|eau de toilette/.test(value)) return "Туалетная вода";
  if (/одеколон|cologne|(^|[^a-z])edc([^a-z]|$)/.test(value)) return "Одеколон";
  if (/духи|extrait|(^|[^a-z])parfum([^a-z]|$)/.test(value)) return "Духи";
  return "";
}

function aiPerfumeGenderFromText(text = "") {
  const value = String(text || "").toLowerCase();
  if (/унисекс|unisex/.test(value)) return "унисекс";
  if (/женск|для женщин|(^|[^a-z])(women|woman|femme|lady|w)([^a-z]|$)/.test(value)) return "женский";
  if (/мужск|для мужчин|(^|[^a-z])(men|man|homme|m)([^a-z]|$)/.test(value)) return "мужской";
  return "";
}

// Системный промпт: копирайтер маркетплейсов + парфюмерный эксперт. Маркет запрещает слова
// «подарок», «новинка», «хит», «аналог», «скидка» (OfferDescription в API Маркета) — для него строже.
function buildPerfumeCopyMessages(source = {}, marketplace = "yandex") {
  const market = marketplace === "ozon" ? "Ozon" : "Яндекс Маркета";
  const yandexRules = marketplace === "ozon"
    ? ""
    : "\n- Для Яндекс Маркета дополнительно нельзя: «подарок», «в подарок», «новинка», «new», «хит», «аналог», «скидка», «распродажа», «акция», «бесплатно», «дешёвый», «заказ».";
  const finale = marketplace === "ozon"
    ? "6) Завершение: формат и объём флакона, тип (парфюмерная вода / туалетная вода / духи), чем хорош как покупка или подарок."
    : "6) Завершение: формат и объём флакона, тип (парфюмерная вода / туалетная вода / духи), чем хорош как покупка — без слова «подарок».";
  const seoGift = marketplace === "ozon" ? ", «в подарок»" : "";
  const system = `Ты — профессиональный копирайтер маркетплейсов и парфюмерный эксперт. Пишешь продающие, живые и точные описания парфюмерии и косметики для карточек ${market} на русском языке.

ЗАДАЧА: по данным товара (JSON в сообщении пользователя) напиши улучшенную карточку.

ОПИСАНИЕ (поле description):
- Объём 1500–2500 знаков, 4–6 абзацев, абзацы разделяй пустой строкой. Без markdown, заголовков, списков, эмодзи, HTML.
- Структура:
  1) Вступление (2–3 предложения): образ и настроение аромата, кому и для чего он — цепляющее, но без клише вроде «окутает вас» в каждой фразе.
  2) Раскрытие композиции: как звучит старт (верхние ноты), сердце (средние), база (шлейф). Описывай ощущения и ассоциации, а не просто перечисляй ноты.
  3) Характер и стойкость/шлейф — только в общих словах, если нет точных данных.
  4) Когда и куда носить: сезон, время суток, ситуации (офис, свидание, вечер, повседневно).
  5) Для кого: пол/унисекс и стиль человека — только если пол есть в данных; иначе нейтрально.
  ${finale}
- Естественно впиши бренд, полное название, тип и объём 1–2 раза и 3–5 поисковых фраз из seoKeywords — без переспама.
- Пиши разнообразно: не начинай абзацы одинаково, избегай канцелярита и воды («данный товар», «является», «высокое качество»).

ФАКТЫ:
- Используй только факты из входных данных. Не выдумывай ноты, парфюмера, год, страну, концентрацию, пол, объём, стойкость в часах.
- Если нот нет — пиши о стиле, жанре и впечатлении в осторожных формулировках («в духе…», «подойдёт тем, кто…»), но не называй конкретные ноты.
- Без нот во входных данных не называй ни одного ингредиента или аккорда (роза, уд, ваниль, амбра, мускус, цитрусы и т. п.), даже если их подсказывает название аромата.
- Пол (мужской / женский / унисекс) упоминай только если он есть во входных данных (поле gender); иначе не пиши «для мужчин», «для женщин», «унисекс».
- Никогда не пиши покупателю о входных данных: никаких «в данных нет», «точных нот нет», «судя по названию», «информации о нотах нет».
- Это не обзор аналогов: не упоминай другие бренды и ароматы, не предлагай «похожие варианты».

ЗАПРЕЩЕНО (правила маркетплейсов):
- «оригинал», «100%», «гарантия», «лучший», «№1», «дешевле всех», сравнения с конкурентами, медицинские и лечебные обещания, призывы купить в другом месте, контакты, ссылки, цены и скидки.${yandexRules}

ОСТАЛЬНЫЕ ПОЛЯ:
- name: название по шаблону «Бренд Название, тип, объём мл» (+ «тестер», если это тестер), до 200 знаков, без капса и лишних символов.
- vendor: бренд из данных, без изменений написания.
- bulletPoints: 5–8 коротких преимуществ (до 120 знаков каждое), по фактам.
- seoKeywords: 8–12 разных поисковых фраз (бренд+название, тип, «для женщин/мужчин» только если пол известен${seoGift}, сезон и т.п.).

ФОРМАТ ОТВЕТА: только валидный JSON без пояснений и без \`\`\`:
{"name": "...", "description": "...", "vendor": "...", "bulletPoints": ["..."], "seoKeywords": ["..."]}`;
  return [
    { role: "system", content: system },
    { role: "user", content: JSON.stringify(source) },
  ];
}

function aiNamedAttributes(attributes = []) {
  const named = {};
  for (const attr of Array.isArray(attributes) ? attributes : []) {
    const id = Number(attr.id || attr.attribute_id);
    const label = AI_PERFUME_ATTRIBUTE_NAMES[id];
    if (!label) continue;
    const values = (attr.values || []).map((v) => cleanText(v.value)).filter(Boolean);
    if (values.length) named[label] = values.join(", ");
  }
  return named;
}

// Факты для промпта: всё, что известно о товаре. extraFacts — ноты/семейство/парфюмер/год (Фрагрантика, база).
function buildAiProductSource(product = {}, marketplace = "yandex", extraFacts = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const name = normalized.yandex?.name || normalized.ozon?.name || normalized.name;
  const attributes = normalized.ozon?.attributes || normalized.yandex?.attributes || [];
  const named = aiNamedAttributes(attributes);
  const text = `${name} ${named["Тип"] || ""}`;
  const volumes = extractOzonYandexImportVolumesMl(normalized.name || normalized.ozon?.name || "");
  return compactObject({
    marketplace,
    offerId: normalized.offerId,
    name,
    brand: normalized.yandex?.vendor || normalized.ozon?.vendor || named["Бренд"] || normalized.brand,
    type: aiPerfumeTypeFromText(text) || named["Тип"] || undefined,
    volumeMl: volumes,
    gender: named["Пол"] || aiPerfumeGenderFromText(name) || undefined,
    tester: /тестер|tester/i.test(name) || undefined,
    currentDescription: normalizeParagraphText(normalized.yandex?.description || normalized.ozon?.description || normalized.description, 3000) || undefined,
    attributes: Object.keys(named).length ? named : undefined,
    categoryId: normalized.yandex?.marketCategoryId || normalized.ozon?.marketCategoryId || normalized.ozon?.categoryId,
    ...extraFacts,
  });
}

function buildAiContentMessages(product = {}, marketplace = "yandex", extraFacts = {}) {
  return buildPerfumeCopyMessages(buildAiProductSource(product, marketplace, extraFacts), marketplace);
}

// Ноты из базы (ручные или с Фрагрантики; угаданные моделью — нет) и карточка каталога «Фрагрантика».
async function aiFragranceFactsForProduct(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const facts = {};
  const prisma = getPrisma();
  if (!prisma) return facts;
  try {
    const row = normalized.id ? await prisma.warehouseProduct.findUnique({ where: { id: normalized.id }, select: { fragranceNotes: true } }) : null;
    const notes = row?.fragranceNotes;
    if (notes && notes.source !== "llm") {
      if (notes.topNotes?.length) facts.topNotes = notes.topNotes;
      if (notes.middleNotes?.length) facts.middleNotes = notes.middleNotes;
      if (notes.baseNotes?.length) facts.baseNotes = notes.baseNotes;
      if (notes.accords?.length) facts.accords = notes.accords;
    }
  } catch {
    // no fragranceNotes column on this install
  }
  if (typeof fragranticaFactsForProductName === "function") {
    const named = aiNamedAttributes(normalized.ozon?.attributes || []);
    const brand = cleanText(normalized.ozon?.vendor || normalized.yandex?.vendor || named["Бренд"] || normalized.brand);
    const fragrantica = await fragranticaFactsForProductName(brand, normalized.ozon?.name || normalized.yandex?.name || normalized.name).catch(() => null);
    if (fragrantica) Object.assign(facts, fragrantica, facts.topNotes ? { topNotes: facts.topNotes, middleNotes: facts.middleNotes, baseNotes: facts.baseNotes } : {});
  }
  return facts;
}

async function generateAiProductContentDraft(product = {}, options = {}) {
  const marketplace = cleanText(options.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
  const facts = await aiFragranceFactsForProduct(product);
  const { data: parsed, completion: response } = await createTextAiJson(buildAiContentMessages(product, marketplace, facts), { temperature: 0.7 });
  const draft = {
    name: compactAiText(parsed.name, 240),
    description: normalizeParagraphText(parsed.description, 5000),
    vendor: compactAiText(parsed.vendor, 120),
    bulletPoints: Array.isArray(parsed.bulletPoints || parsed.bullets) ? (parsed.bulletPoints || parsed.bullets) : [],
    seoKeywords: Array.isArray(parsed.seoKeywords || parsed.keywords) ? (parsed.seoKeywords || parsed.keywords) : [],
    model: cleanText(response?.model) || resolveTextAiProvider(await readEffectiveAiSettings()).model,
    generatedAt: new Date().toISOString(),
  };
  if (!draft.name && !draft.description) {
    const error = new Error("AI не вернул название или описание. Попробуйте повторить.");
    error.statusCode = 502;
    error.code = "openai_text_empty";
    throw error;
  }
  return draft;
}

const YANDEX_MIN_VOLUME_ML = 20;

