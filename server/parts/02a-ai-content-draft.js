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
  if (description && description.length < 140) reasons.push("short_description");
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
    description: compactAiText(draft.description, 5000),
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

function buildAiContentMessages(product = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const source = {
    marketplace,
    offerId: normalized.offerId,
    name: normalized.yandex?.name || normalized.ozon?.name || normalized.name,
    description: normalized.yandex?.description || normalized.ozon?.description || normalized.description,
    vendor: normalized.yandex?.vendor || normalized.ozon?.vendor || normalized.brand,
    categoryId: normalized.yandex?.marketCategoryId || normalized.ozon?.marketCategoryId || normalized.ozon?.categoryId,
    price: normalized.nextPrice || normalized.currentPrice,
    volumeMl: extractOzonYandexImportVolumesMl(normalized.name || normalized.ozon?.name || ""),
    attributes: normalized.ozon?.attributes || normalized.yandex?.attributes || [],
  };
  return [
    {
      role: "system",
      content: [
        "Ты редактор карточек маркетплейса для парфюмерии и косметики.",
        "Улучши карточку так, чтобы текст был пригоден для Yandex Market и не нарушал правила.",
        "Описание должно быть подробным: 900-1400 знаков, 2-3 связных абзаца без markdown, списков и эмодзи.",
        "Раскрой характер аромата, звучание верхних/средних/базовых нот, настроение, сезонность, уместные сценарии использования и ощущение от шлейфа, но только если эти данные есть в исходных данных.",
        "Если данных о нотах мало, расширяй описание за счет нейтральных формулировок о стиле, формате, назначении и впечатлении от композиции, не выдумывая факты.",
        "bulletPoints верни отдельным массивом из 5-8 коротких преимуществ для карточки.",
        "seoKeywords верни отдельным массивом из 8-12 поисковых фраз без повторов.",
        "Не выдумывай бренд, объем, концентрацию, страну, пол и ноты, если их нет в исходных данных.",
        "Не добавляй медицинские обещания, слова 'оригинал', '100% гарантия', запрещенные сравнения и агрессивные обещания.",
        "Верни только JSON: name, description, vendor, bulletPoints, seoKeywords.",
      ].join(" "),
    },
    {
      role: "user",
      content: JSON.stringify(source),
    },
  ];
}

async function generateAiProductContentDraft(product = {}, options = {}) {
  const marketplace = cleanText(options.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
  const response = await createOpenAiJsonChat(buildAiContentMessages(product, marketplace));
  const content = response?.choices?.[0]?.message?.content || "";
  const parsed = extractJsonObjectFromText(content);
  const draft = {
    name: compactAiText(parsed.name, 240),
    description: compactAiText(parsed.description, 5000),
    vendor: compactAiText(parsed.vendor, 120),
    bulletPoints: Array.isArray(parsed.bulletPoints || parsed.bullets) ? (parsed.bulletPoints || parsed.bullets) : [],
    seoKeywords: Array.isArray(parsed.seoKeywords || parsed.keywords) ? (parsed.seoKeywords || parsed.keywords) : [],
    model: cleanText(response?.model) || openaiTextModel,
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

