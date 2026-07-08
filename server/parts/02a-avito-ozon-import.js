// Импорт товаров Ozon → Avito по гибким правилам (avito-import-rules.json):
// объём меньше порога, стоп-слова («Дубль», «Удаленый», …), бренды, цена,
// остатки, наличие фото. Результат — объявления в avito-listings.json,
// которые попадают в XML-фид Автозагрузки.

function normalizeAvitoMatchText(value) {
  return cleanText(value).toLowerCase().replace(/ё/g, "е");
}

// «100 мл», «20мл», «7.5 ml» — берём максимальный найденный объём
// (для наборов вида «5 мл + 10 мл» фильтр по порогу применяется к большему).
function parseVolumeMlFromText(value) {
  const text = normalizeAvitoMatchText(value);
  if (!text) return 0;
  let maxVolume = 0;
  for (const match of text.matchAll(/(\d+(?:[.,]\d+)?)\s*(?:мл|ml)(?![a-zа-я])/gi)) {
    const volume = Number(match[1].replace(",", "."));
    if (Number.isFinite(volume) && volume > maxVolume) maxVolume = volume;
  }
  return maxVolume;
}

function extractAvitoImageUrls(images) {
  // В Postgres колонка images — объект { imageUrl, images: [...] }
  // (см. productToPostgresData), но встречается и плоский массив URL.
  const list = Array.isArray(images)
    ? images
    : images && typeof images === "object"
      ? [images.imageUrl, ...(Array.isArray(images.images) ? images.images : [])]
      : [];
  return [...new Set(list
    .map((item) => cleanText(typeof item === "string" ? item : item?.url || item?.fileName || ""))
    .filter((url) => /^https?:\/\//i.test(url)))]
    .slice(0, 10);
}

// Возвращает { ok, reasons, listing } — причины пропуска нужны для предпросмотра.
function evaluateAvitoImportCandidate(product = {}, rules = {}) {
  const reasons = [];
  const title = cleanText(product.name);
  const matchTitle = normalizeAvitoMatchText(title);
  const matchBrand = normalizeAvitoMatchText(product.brand);

  if (!title) reasons.push("no_title");
  if (rules.skipArchived && product.archived) reasons.push("archived");

  const excludeWord = (rules.excludeTitleWords || []).find((word) => matchTitle.includes(word));
  if (excludeWord) reasons.push(`title_word:${excludeWord}`);
  if ((rules.includeTitleWords || []).length && !(rules.includeTitleWords || []).some((word) => matchTitle.includes(word))) {
    reasons.push("title_not_in_include_list");
  }

  const excludeBrand = (rules.excludeBrands || []).find((word) => matchBrand && matchBrand.includes(word));
  if (excludeBrand) reasons.push(`brand:${excludeBrand}`);
  if ((rules.includeBrands || []).length && !(rules.includeBrands || []).some((word) => matchBrand.includes(word))) {
    reasons.push("brand_not_in_include_list");
  }

  const volumeMl = parseVolumeMlFromText(title);
  if (rules.minVolumeMl > 0 && volumeMl > 0 && volumeMl < rules.minVolumeMl) {
    reasons.push(`volume_below_min:${volumeMl}`);
  }
  if (rules.skipWithoutVolume && volumeMl <= 0) reasons.push("volume_unknown");

  const basePriceRub = Number(product.targetPrice || product.currentPrice || 0);
  const priceRub = basePriceRub > 0 ? Math.round(basePriceRub * avitoPriceCoefficientFor(basePriceRub, rules)) : 0;
  if (priceRub <= 0) reasons.push("no_price");
  if (rules.minPriceRub > 0 && priceRub > 0 && priceRub < rules.minPriceRub) reasons.push(`price_below_min:${priceRub}`);
  if (rules.maxPriceRub > 0 && priceRub > rules.maxPriceRub) reasons.push(`price_above_max:${priceRub}`);

  const imageUrls = extractAvitoImageUrls(product.images);
  if (rules.requireImages && !imageUrls.length) reasons.push("no_images");

  const stock = Number(product.targetStock || 0);
  if (rules.minStock > 0 && stock < rules.minStock) reasons.push(`stock_below_min:${stock}`);

  if (reasons.length) return { ok: false, reasons, listing: null };

  return {
    ok: true,
    reasons: [],
    listing: {
      adId: `oz-${cleanText(product.offerId) || cleanText(product.id)}`,
      sourceProductId: cleanText(product.id),
      sourceOfferId: cleanText(product.offerId),
      source: "ozon",
      title,
      brand: cleanText(product.brand),
      volumeMl,
      priceRub,
      imageUrls,
      enabled: true,
    },
  };
}

async function collectAvitoImportCandidates(rulesOverride = null) {
  const prisma = getPrisma();
  if (!prisma) {
    const error = new Error("Postgres недоступен: импорт Ozon → Avito требует базы товаров склада.");
    error.statusCode = 503;
    throw error;
  }
  const rules = normalizeAvitoImportRules(rulesOverride || (await readAvitoImportRules()));
  const products = await prisma.warehouseProduct.findMany({
    where: { marketplace: "ozon" },
    select: {
      id: true,
      offerId: true,
      name: true,
      brand: true,
      images: true,
      archived: true,
      targetStock: true,
      currentPrice: true,
      targetPrice: true,
    },
    orderBy: { updatedAt: "desc" },
  });

  const matched = [];
  const skipped = [];
  for (const product of products) {
    if (rules.maxItems > 0 && matched.length >= rules.maxItems) break;
    const result = evaluateAvitoImportCandidate(product, rules);
    if (result.ok) matched.push(result.listing);
    else skipped.push({ id: product.id, offerId: product.offerId, name: product.name, reasons: result.reasons });
  }
  return { rules, totalOzonProducts: products.length, matched, skipped };
}

function summarizeAvitoSkipReasons(skipped) {
  const byReason = {};
  for (const item of skipped) {
    // Причина с деталью («volume_below_min:15») агрегируется по префиксу.
    const key = String(item.reasons[0] || "unknown").split(":")[0];
    byReason[key] = (byReason[key] || 0) + 1;
  }
  return byReason;
}

async function previewAvitoOzonImport({ rules = null, sampleLimit = 50 } = {}) {
  const result = await collectAvitoImportCandidates(rules);
  return {
    rules: result.rules,
    totalOzonProducts: result.totalOzonProducts,
    matchedCount: result.matched.length,
    skippedCount: result.skipped.length,
    skippedByReason: summarizeAvitoSkipReasons(result.skipped),
    matchedSample: result.matched.slice(0, sampleLimit),
    skippedSample: result.skipped.slice(0, sampleLimit),
  };
}

async function applyAvitoOzonImport({ rules = null } = {}) {
  const result = await collectAvitoImportCandidates(rules);
  const { created, updated, total } = await upsertAvitoListings(result.matched, { source: "ozon" });
  return {
    totalOzonProducts: result.totalOzonProducts,
    matchedCount: result.matched.length,
    skippedCount: result.skipped.length,
    skippedByReason: summarizeAvitoSkipReasons(result.skipped),
    created,
    updated,
    totalListings: total,
  };
}
