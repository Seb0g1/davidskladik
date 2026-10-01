// One spelling per brand (normalized key → most common spelling in the catalog), loaded by
// loadYandexVendorCanonicalMap before a transfer; empty until then.
let yandexVendorCanonicalMap = new Map();

async function loadYandexVendorCanonicalMap() {
  const prisma = getPrisma();
  if (!prisma) return yandexVendorCanonicalMap;
  try {
    const rows = await prisma.$queryRawUnsafe(`
      SELECT normalized_brand AS key, display_brand AS brand, count(*)::int AS n
      FROM brand_index_items GROUP BY 1, 2`);
    const best = new Map();
    for (const row of rows) {
      const brand = cleanText(row.brand);
      if (!brand || isPlaceholderVendor(brand)) continue;
      const current = best.get(row.key);
      const mixedCase = brand !== brand.toUpperCase();
      const score = Number(row.n) * 2 + (mixedCase ? 1 : 0);
      if (!current || score > current.score) best.set(row.key, { brand, score });
    }
    yandexVendorCanonicalMap = new Map(Array.from(best, ([key, value]) => [key, value.brand]));
  } catch (error) {
    logger.warn("yandex vendor canonical map load failed", { detail: error?.message || String(error) });
  }
  return yandexVendorCanonicalMap;
}

function canonicalYandexVendor(value = "") {
  const brand = cleanText(value);
  if (!brand || isPlaceholderVendor(brand)) return "";
  const key = typeof normalizedBrandIndexKey === "function" ? normalizedBrandIndexKey(brand) : brand.toLowerCase();
  return yandexVendorCanonicalMap.get(key) || brand;
}

// A brand guessed from the product name («CREED Aventus Парфюмированная», «Givenchy AMARIGE»)
// often carries the model: keep only its longest leading part that is a known brand, else none.
function knownYandexVendorPrefix(value = "") {
  const brand = cleanText(value);
  if (!brand || isPlaceholderVendor(brand)) return "";
  if (!yandexVendorCanonicalMap.size) return brand;
  const keyOf = (text) => (typeof normalizedBrandIndexKey === "function" ? normalizedBrandIndexKey(text) : text.toLowerCase());
  const tokens = brand.split(/\s+/);
  for (let count = tokens.length; count >= 1; count -= 1) {
    const known = yandexVendorCanonicalMap.get(keyOf(tokens.slice(0, count).join(" ")));
    if (known) return known;
  }
  return "";
}

// Extra characteristics (e.g. from the Fragrantica page) replace generated ones with the same parameterId.
function mergeYandexParameterValues(base = [], extra = []) {
  const extraList = (Array.isArray(extra) ? extra : []).filter((p) => Number(p?.parameterId) > 0 && (p.valueId || String(p.value ?? "").trim() !== ""));
  const ids = new Set(extraList.map((p) => Number(p.parameterId)));
  const merged = [...(Array.isArray(base) ? base : []).filter((p) => !ids.has(Number(p.parameterId))), ...extraList];
  return merged.length ? merged : undefined;
}

function buildYandexOfferMapping(product, overrides = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const yandex = {
    ...(normalized.yandex || {}),
    ...(overrides.yandex || {}),
  };
  const ozonAttributes = Array.isArray(ozon.attributes) ? ozon.attributes : [];
  const offerId = cleanText(overrides.offerId || yandex.offerId || normalized.offerId);

  // Use approved AI content draft when available — provides better names and descriptions
  // than raw Ozon data. Inline lookup (latestAiContentDraft loads after this file).
  const approvedDraft = [...(normalized.aiContentDrafts || [])]
    .filter((d) => d.status === "approved" && d.marketplace !== "ozon")
    .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))[0] || null;

  const pictures = Array.from(
    new Set([
      cleanText(normalized.imageUrl),
      ...splitList(yandex.pictures),
      ...splitList(yandex.images),
      ...splitList(ozon.primaryImage),
      ...splitList(ozon.images),
    ].filter(Boolean)),
  );
  // Only real GTINs: Ozon codes (OZN…), internal 2/02 codes and articles make Market reject
  // marked perfumery («Штрихкод производителя должен быть с GTIN»). None → field not sent.
  const barcodes = pickYandexGtins(
    [...splitList(yandex.barcodes), ...splitList(ozon.barcode), ...splitList(ozon.barcodes)],
    { offerId },
  );
  const price = Number(
    overrides.price ||
      yandex.price ||
      normalized.targetPrice ||
      normalized.nextPrice ||
      normalized.marketplacePrice ||
      ozon.price ||
      0,
  );
  const extra = parseJsonField(yandex.extra, {});
  const weightDimensions = resolveYandexWeightDimensionsFromProduct(normalized);
  // Brand: the Ozon «Бренд» attribute (no model), one spelling per brand, never a placeholder.
  const vendor = canonicalYandexVendor(ozonAttributeValue(ozonAttributes, OZON_ATTR_BRAND))
    || knownYandexVendorPrefix(yandex.vendor || resolveYandexVendorFromProduct(normalized));
  const name = resolveYandexOfferName({
    candidates: [overrides.name, yandex.name, approvedDraft?.name, ozon.name, ozonAttributeValue(ozonAttributes, OZON_ATTR_NAME), normalized.name],
    offerId,
    vendor,
  });
  // Category from the Ozon product type / name; unknown → left for manual review (not guessed).
  const category = resolveYandexCategoryForOzonProduct({ typeId: ozon.typeId, name: ozon.name || name });
  const marketCategoryId = Number(overrides.marketCategoryId || category.categoryId || 0) || undefined;
  const country = yandexCountryName(ozonAttributeValue(ozonAttributes, OZON_ATTR_COUNTRY));
  const parameterValues = buildPerfumeVariantParameters({
    categoryId: marketCategoryId,
    vendor,
    model: ozonAttributeValue(ozonAttributes, OZON_ATTR_MODEL),
    kind: category.kind,
    name,
  });

  // Description priority: manual yandex override → approved AI draft → Ozon description →
  // AI draft bullet points as fallback. Never fall back to product name — that produces
  // duplicate name/description pairs that Yandex penalises.
  const realDescription = (value) => (isPlaceholderYandexDescription(value, { name, offerId }) ? "" : value);
  const descriptionRaw =
    realDescription(yandex.description) ||
    realDescription(approvedDraft?.description) ||
    realDescription(ozon.description) ||
    (approvedDraft?.bulletPoints?.length ? approvedDraft.bulletPoints.join(". ") : "");

  // Only explicit, validated extras reach Market (internal bookkeeping stays local).
  const offer = compactObject({
    offerId,
    name,
    marketCategoryId,
    pictures,
    vendor: vendor || undefined,
    description: formatDescriptionForMarketplace(normalizeParagraphText(descriptionRaw, 5600), "yandex") || undefined, // Market limit: 6000 chars; paragraphs as <p>
    barcodes: barcodes.length ? barcodes : undefined,
    weightDimensions,
    manufacturerCountries: country ? [country] : undefined,
    parameterValues: mergeYandexParameterValues(parameterValues, extra.parameterValues),
    commodityCodes: sanitizeYandexCommodityCodes(extra.commodityCodes),
    certificates: sanitizeYandexCertificates(extra.certificates),
    shelfLife: sanitizeYandexShelfLife(extra.shelfLife),
    vendorCode: cleanText(extra.vendorCode) || undefined,
    basicPrice: price > 0 ? { value: roundPrice(price), currencyId: "RUR" } : undefined,
  });

  const missing = [];
  if (!offer.offerId) missing.push("offerId");
  if (!offer.name) missing.push("name");
  if (!offer.marketCategoryId) missing.push("marketCategoryId");
  if (!offer.pictures?.length) missing.push("pictures");
  if (!offer.vendor) missing.push("vendor");
  if (!offer.description) missing.push("description");
  if (!(Number(offer.weightDimensions?.length) > 0 && Number(offer.weightDimensions?.weight) > 0)) {
    missing.push("weightDimensions");
  }

  // description is informational — Yandex doesn't require it, so don't block export.
  const blockingMissing = missing.filter((field) => field !== "description");
  return { offer, missing, ready: blockingMissing.length === 0, categoryReview: category.categoryId ? null : category.reason };
}

async function sendApprovedYandexProductContent(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "yandex") {
    return { ok: true, skipped: true, reason: "not_yandex" };
  }
  const smallVolumeCheck = assessYandexSmallVolume(normalized);
  if (smallVolumeCheck.blocked) {
    return { ok: false, error: "yandex_small_volume_blocked", ...smallVolumeCheck };
  }
  const shop = getYandexShopByTarget(normalized.target);
  if (!shop) return { ok: false, error: "yandex_shop_not_found" };
  if (!shop.apiKey || !shop.businessId) return { ok: false, error: "yandex_shop_not_configured", target: shop.id || normalized.target };

  const built = buildYandexOfferMapping(normalized);
  const mode = cleanText(options.mode || "content").toLowerCase();
  const pictureOverride = Array.isArray(options.pictures)
    ? options.pictures.map((url) => cleanText(url)).filter(Boolean)
    : [];
  const partialOffer = compactObject({
    offerId: built.offer?.offerId || normalized.offerId,
    name: mode === "image" ? undefined : built.offer?.name,
    vendor: mode === "image" ? undefined : built.offer?.vendor,
    description: mode === "image" ? undefined : built.offer?.description,
    pictures: mode === "content" ? undefined : (pictureOverride.length ? pictureOverride : built.offer?.pictures),
  });
  const missing = [];
  if (!partialOffer.offerId) missing.push("offerId");
  if (mode !== "image" && !partialOffer.description) missing.push("description");
  if (mode !== "content" && !partialOffer.pictures?.length) missing.push("pictures");
  if (missing.length) {
    return {
      ok: false,
      error: `yandex_update_not_ready: ${missing.join(", ")}`,
      mode,
      missing,
      target: shop.id,
      offerId: built.offer?.offerId || normalized.offerId,
    };
  }

  const [result] = await sendYandexOfferMappings(shop, [partialOffer]);
  if (!result?.ok) {
    const failed = {
      ok: false,
      error: result?.error || "yandex_content_send_failed",
      mode,
      target: shop.id,
      businessId: shop.businessId,
      offerId: partialOffer.offerId,
      result,
    };
    logger.warn("approved yandex product content send failed", failed);
    return failed;
  }
  const sent = {
    ok: true,
    mode,
    target: shop.id,
    businessId: shop.businessId,
    offerId: partialOffer.offerId,
    fields: Object.keys(partialOffer).filter((key) => key !== "offerId"),
    result,
  };
  logger.info("approved yandex product content sent", sent);
  return sent;
}

async function sendApprovedYandexProductCardRepair(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "yandex") {
    return { ok: true, skipped: true, reason: "not_yandex" };
  }
  const smallVolumeCheck = assessYandexSmallVolume(normalized);
  if (smallVolumeCheck.blocked) {
    return { ok: false, error: "yandex_small_volume_blocked", ...smallVolumeCheck };
  }
  const shop = getYandexShopByTarget(normalized.target);
  if (!shop) return { ok: false, error: "yandex_shop_not_found" };
  if (!shop.apiKey || !shop.businessId) {
    return { ok: false, error: "yandex_shop_not_configured", target: shop.id || normalized.target };
  }

  const built = buildYandexOfferMapping(normalized);
  const vendor = cleanText(built.offer?.vendor);
  const partialOffer = compactObject({
    offerId: built.offer?.offerId || normalized.offerId,
    name: built.offer?.name,
    marketCategoryId: built.offer?.marketCategoryId,
    vendor: vendor && vendor.toLowerCase() !== "без бренда" ? vendor : undefined,
    description: built.offer?.description,
    pictures: built.offer?.pictures?.length ? built.offer.pictures : undefined,
    weightDimensions: built.offer?.weightDimensions,
    barcodes: built.offer?.barcodes?.length ? built.offer.barcodes : undefined,
  });
  const missing = [];
  if (!partialOffer.offerId) missing.push("offerId");
  if (!partialOffer.weightDimensions) missing.push("weightDimensions");
  if (missing.length) {
    return {
      ok: false,
      error: `yandex_card_repair_not_ready: ${missing.join(", ")}`,
      mode: "card_repair",
      missing,
      target: shop.id,
      offerId: partialOffer.offerId || normalized.offerId,
      builtMissing: built.missing,
    };
  }

  const [result] = await sendYandexOfferMappings(shop, [partialOffer]);
  if (!result?.ok) {
    const failed = {
      ok: false,
      error: result?.error || "yandex_card_repair_failed",
      mode: "card_repair",
      target: shop.id,
      businessId: shop.businessId,
      offerId: partialOffer.offerId,
      result,
    };
    logger.warn("approved yandex product card repair failed", failed);
    return failed;
  }
  const sent = {
    ok: true,
    mode: "card_repair",
    target: shop.id,
    businessId: shop.businessId,
    offerId: partialOffer.offerId,
    fields: Object.keys(partialOffer).filter((key) => key !== "offerId"),
    vendor: partialOffer.vendor || "",
    pictures: partialOffer.pictures?.length || 0,
    weightDimensions: partialOffer.weightDimensions,
    result,
  };
  logger.info("approved yandex product card repair sent", sent);
  return sent;
}

async function sendApprovedOzonProductContent(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "ozon") {
    return { ok: true, skipped: true, reason: "not_ozon" };
  }
  const account = getOzonAccountByTarget(normalized.target || "ozon");
  if (!account) return { ok: false, error: "ozon_account_not_found" };
  if (!account.clientId || !account.apiKey) return { ok: false, error: "ozon_account_not_configured", target: account.id || normalized.target };

  const mode = cleanText(options.mode || "content").toLowerCase() === "image" ? "image" : "content";
  const built = buildOzonWarehouseProductItem(normalized, options.overrides || {});
  if (!built.ready) {
    return {
      ok: false,
      error: "ozon_update_not_ready",
      code: "ozon_update_not_ready",
      mode,
      target: account.id,
      offerId: normalized.ozon?.offerId || normalized.offerId,
      missing: built.missing || [],
    };
  }

  const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
  const sent = {
    ok: true,
    mode,
    target: account.id,
    offerId: built.item.offer_id || normalized.offerId,
    fields: mode === "image" ? ["primary_image", "images"] : ["name", "description"],
    result,
  };
  logger.info("approved ozon product content sent", sent);
  return sent;
}

function marketplaceSendResultError(result = {}, fallbackCode = "marketplace_send_failed") {
  if (result?.ok) return null;
  const error = new Error(result?.error || fallbackCode);
  error.statusCode = 400;
  error.code = result?.code || result?.error || fallbackCode;
  if (Array.isArray(result?.missing)) error.missing = result.missing;
  error.marketplace = result?.marketplace;
  error.result = result;
  return error;
}

