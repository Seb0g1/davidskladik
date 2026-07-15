// Импорт Ozon → Wildberries: правила, кандидаты и payload карточек.
//
// Бизнес-правило WB: продаём только дорогие товары — итоговая закупочная цена
// поставщика (₽) должна быть не ниже WB_MIN_SUPPLIER_PRICE_RUB (по умолчанию
// 14 500 ₽ — «от 15 000, допускается 14 500»). Ниже порога товар не загружается,
// цена не отправляется, остаток обнуляется.
//
// Цена WB = закупка поставщика × наценка WB (Настройки → Цены: defaultMarkups.wb
// + гибкие правила marketplace "wb") — та же формула, что у Ozon/Yandex/Avito.

const WB_MIN_SUPPLIER_PRICE_RUB = Math.max(0, Number(process.env.WB_MIN_SUPPLIER_PRICE_RUB || 14500) || 14500);
const wbImportRulesPath = path.join(dataDir, "wb-import-rules.json");

function normalizeWbImportRules(input = {}) {
  const fallbackSubjectId = Number(process.env.WB_DEFAULT_SUBJECT_ID || 0) || 0;
  return {
    // Предмет (категория) WB по умолчанию для создаваемых карточек.
    subjectId: Number(input.subjectId ?? fallbackSubjectId) || 0,
    subjectName: cleanText(input.subjectName),
    minSupplierPriceRub: Number(input.minSupplierPriceRub) > 0 ? Number(input.minSupplierPriceRub) : WB_MIN_SUPPLIER_PRICE_RUB,
    skipArchived: input.skipArchived !== false,
    excludeTitleWords: Array.isArray(input.excludeTitleWords)
      ? input.excludeTitleWords.map((word) => cleanText(word).toLowerCase().replace(/ё/g, "е")).filter(Boolean)
      : ["дубль", "удаленый", "удаленный", "отливант", "пробник", "тестер"],
    // Остаток FBS для карточек с валидным поставщиком.
    defaultStock: Math.max(0, Math.min(999, Number(input.defaultStock ?? 3) || 3)),
    updatedAt: input.updatedAt || null,
  };
}

async function readWbImportRules() {
  try {
    return normalizeWbImportRules(JSON.parse(await fs.readFile(wbImportRulesPath, "utf8")));
  } catch (error) {
    if (error.code !== "ENOENT") logger.warn("read wb import rules failed", { detail: error?.message || String(error) });
    return normalizeWbImportRules({});
  }
}

async function writeWbImportRules(input = {}) {
  const rules = normalizeWbImportRules({ ...(await readWbImportRules()), ...input, updatedAt: new Date().toISOString() });
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(wbImportRulesPath, JSON.stringify(rules, null, 2), "utf8");
  return rules;
}

// Закупка поставщика в рублях; MAX_SAFE_INTEGER от warehouseSupplierPurchaseRubPrice
// означает «цены нет».
function wbSupplierPurchaseRub(supplier, pricing = {}) {
  if (!supplier || typeof supplier !== "object") return 0;
  const purchaseRub = warehouseSupplierPurchaseRubPrice(supplier, pricing.usdRate);
  if (!Number.isFinite(purchaseRub) || purchaseRub <= 0 || purchaseRub >= Number.MAX_SAFE_INTEGER) return 0;
  return purchaseRub;
}

function wbSupplierPriceRub(supplier, pricing = {}) {
  const purchaseRub = wbSupplierPurchaseRub(supplier, pricing);
  if (!(purchaseRub > 0)) return 0;
  const rate = Number(pricing.usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const rubNative = supplierPriceIsRubNative(supplier);
  const usd = !rubNative && Number(supplier.price || 0) > 0 ? Number(supplier.price) : purchaseRub / rate;
  const coefficient = resolveMarkupCoefficient({
    productMarkup: 0,
    marketplace: "wb",
    supplierUsdPrice: usd,
    supplierPriceCurrency: rubNative ? "RUB" : "USD",
    usdRate: rate,
    appSettings: pricing.appSettings,
  });
  return Math.round(purchaseRub * coefficient);
}

// Габариты для карточки WB (см и кг брутто): из Ozon (мм и граммы), фолбэк —
// парфюмерная эвристика от объёма (как у Яндекса).
function wbDimensionsFromProduct(product = {}) {
  const ozon = product.ozon || {};
  const dimsMm = [Number(ozon.depth || 0), Number(ozon.width || 0), Number(ozon.height || 0)]
    .filter((value) => Number.isFinite(value) && value > 0);
  const weightG = Number(ozon.weight || 0);
  if (dimsMm.length === 3 && weightG > 0) {
    const sorted = dimsMm.sort((a, b) => b - a);
    return {
      length: Math.max(1, Math.round(sorted[0] / 10)),
      width: Math.max(1, Math.round(sorted[1] / 10)),
      height: Math.max(1, Math.round(sorted[2] / 10)),
      weightBrutto: Math.max(0.05, Number((weightG / 1000).toFixed(3))),
    };
  }
  const volumes = extractOzonYandexImportVolumesMl([product.name, ozon.name, product.offerId].filter(Boolean).join(" "));
  const volumeMl = volumes.length ? Math.max(...volumes) : 100;
  return {
    length: 12,
    width: 8,
    height: Math.max(6, Math.min(22, Math.round(volumeMl / 10))),
    weightBrutto: Math.max(0.12, Math.min(1.5, Number((volumeMl * 0.004 + 0.08).toFixed(3)))),
  };
}

function wbExtractImageUrls(product = {}) {
  const ozon = product.ozon || {};
  return Array.from(new Set([
    cleanText(product.imageUrl),
    ...splitList(ozon.primaryImage),
    ...splitList(ozon.images),
  ].filter(Boolean)));
}

// Возвращает { ok, reasons, listing } — по образцу evaluateAvitoImportCandidate.
// Ключевой блок: price_below_min при закупке поставщика ниже порога 14 500 ₽.
function evaluateWbImportCandidate(product = {}, rules = {}, pricing = {}) {
  const normalizedRules = normalizeWbImportRules(rules);
  const reasons = [];
  const title = cleanText(product.name);
  const matchTitle = title.toLowerCase().replace(/ё/g, "е");

  if (!title) reasons.push("no_title");
  if (normalizedRules.skipArchived && product.archived) reasons.push("archived");
  const excludeWord = normalizedRules.excludeTitleWords.find((word) => matchTitle.includes(word));
  if (excludeWord) reasons.push(`title_word:${excludeWord}`);

  const imageUrls = wbExtractImageUrls(product);
  if (!imageUrls.length) reasons.push("no_images");
  if (!normalizedRules.subjectId) reasons.push("no_subject");

  const supplier = pricing.supplierByProductId instanceof Map
    ? pricing.supplierByProductId.get(cleanText(product.id)) || null
    : product.selectedSupplier || null;
  const purchaseRub = wbSupplierPurchaseRub(supplier, pricing);
  if (!(purchaseRub > 0)) {
    reasons.push("no_price");
  } else if (purchaseRub < normalizedRules.minSupplierPriceRub) {
    reasons.push(`price_below_min:${Math.round(purchaseRub)}`);
  }
  const priceRub = purchaseRub > 0 ? wbSupplierPriceRub(supplier, pricing) : 0;

  if (reasons.length) return { ok: false, reasons, listing: null };
  return {
    ok: true,
    reasons: [],
    listing: {
      sourceProductId: cleanText(product.id),
      vendorCode: cleanText(product.offerId) || cleanText(product.id),
      // Лимит WB на название — 60 символов.
      title: title.slice(0, 60),
      description: cleanText(product.ozon?.description) || title,
      brand: cleanText(product.brand || product.ozon?.vendor),
      barcode: cleanText(product.ozon?.barcode) || splitList(product.ozon?.barcodes)[0] || "",
      imageUrls,
      dimensions: wbDimensionsFromProduct(product),
      purchaseRub: Math.round(purchaseRub),
      priceRub,
      subjectId: normalizedRules.subjectId,
    },
  };
}

// Payload создания карточки WB (content/v2/cards/upload).
function buildWbCardPayload(listing = {}) {
  return {
    subjectID: Number(listing.subjectId),
    variants: [compactObject({
      vendorCode: cleanText(listing.vendorCode),
      title: cleanText(listing.title),
      description: cleanText(listing.description) || undefined,
      brand: cleanText(listing.brand) || undefined,
      dimensions: listing.dimensions,
      sizes: [{
        techSize: "0",
        wbSize: "",
        skus: [cleanText(listing.barcode)].filter(Boolean),
      }],
    })],
  };
}

// Кандидаты импорта из Postgres: дешёвые фильтры первым проходом, поставщики —
// только для прошедших (двухпроходный, как у Avito: живой PriceMaster дорогой).
async function collectWbImportCandidates({ rules = null, limit = 50000 } = {}) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) {
    const error = new Error("Postgres недоступен: импорт WB требует основную БД.");
    error.statusCode = 503;
    throw error;
  }
  // Частичные правила из body накладываются на сохранённые (проверка без сохранения).
  const effectiveRules = normalizeWbImportRules({ ...(await readWbImportRules()), ...(rules || {}) });
  const pricing = await loadAvitoPricingContext();

  const products = [];
  let cursorId = null;
  while (products.length < limit) {
    const page = await prisma.warehouseProduct.findMany({
      where: { AND: [enabledWarehouseTargetWhere(), { marketplace: "ozon" }] },
      select: { id: true, raw: true },
      orderBy: { id: "asc" },
      take: 1000,
      ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
    });
    if (!page.length) break;
    cursorId = page[page.length - 1].id;
    for (const row of page) {
      const product = row.raw && typeof row.raw === "object" ? normalizeWarehouseProduct(row.raw) : null;
      if (product && cleanText(product.offerId)) products.push(product);
      if (products.length >= limit) break;
    }
    if (page.length < 1000) break;
  }

  // Проход 1: без цен поставщика (subjectId проверяем всегда, чтобы отчёт был честным).
  const cheapPass = products.map((product) => ({
    product,
    result: evaluateWbImportCandidate(product, effectiveRules, {
      ...pricing,
      supplierByProductId: new Map([[cleanText(product.id), product.selectedSupplier || null]]),
    }),
  }));
  const survivors = cheapPass.filter(({ result }) => (
    result.ok || result.reasons.every((reason) => reason === "no_price" || reason.startsWith("price_below_min"))
  ));

  // Проход 2: живая цена поставщика только для прошедших дешёвые фильтры.
  const supplierMap = await loadAvitoSupplierPricingMap(survivors.map(({ product }) => cleanText(product.id)));
  const evaluated = cheapPass.map(({ product, result }) => {
    const isSurvivor = survivors.some((item) => item.product === product);
    if (!isSurvivor) return { product, result };
    return {
      product,
      result: evaluateWbImportCandidate(product, effectiveRules, { ...pricing, supplierByProductId: supplierMap }),
    };
  });

  return { rules: effectiveRules, pricing, evaluated, total: products.length };
}

function summarizeWbImportPreview(evaluated = []) {
  const reasonCounts = new Map();
  let okCount = 0;
  for (const { result } of evaluated) {
    if (result.ok) {
      okCount += 1;
      continue;
    }
    for (const reason of result.reasons) {
      const key = reason.split(":")[0];
      reasonCounts.set(key, (reasonCounts.get(key) || 0) + 1);
    }
  }
  return {
    ok: okCount,
    skipped: evaluated.length - okCount,
    reasons: Object.fromEntries([...reasonCounts.entries()].sort((a, b) => b[1] - a[1])),
  };
}
