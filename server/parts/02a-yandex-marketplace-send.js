function buildYandexOfferMapping(product, overrides = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const yandex = {
    ...(normalized.yandex || {}),
    ...(overrides.yandex || {}),
  };

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
  const barcodes = Array.from(new Set([...splitList(yandex.barcodes), ...splitList(ozon.barcode), ...splitList(ozon.barcodes)]));
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
  const { weightDimensions: _cachedDims, ...extraRest } = extra;
  const weightDimensions = resolveYandexWeightDimensionsFromProduct(normalized);
  const vendor = resolveYandexVendorFromProduct(normalized) || "Без бренда";

  // Description priority: manual yandex override → approved AI draft → Ozon description →
  // AI draft bullet points as fallback. Never fall back to product name — that produces
  // duplicate name/description pairs that Yandex penalises.
  const descriptionRaw =
    yandex.description ||
    approvedDraft?.description ||
    ozon.description ||
    (approvedDraft?.bulletPoints?.length ? approvedDraft.bulletPoints.join(". ") : "");

  const offer = compactObject({
    offerId: cleanText(overrides.offerId || yandex.offerId || normalized.offerId),
    name: cleanText(overrides.name || yandex.name || approvedDraft?.name || ozon.name || normalized.name),
    marketCategoryId: Number(yandex.marketCategoryId || ozon.marketCategoryId || 0) || undefined,
    pictures,
    vendor,
    description: cleanText(descriptionRaw) || undefined,
    barcodes,
    weightDimensions,
    basicPrice: price > 0 ? { value: roundPrice(price), currencyId: "RUR" } : undefined,
    ...extraRest,
  });

  const missing = [];
  if (!offer.offerId) missing.push("offerId");
  if (!offer.name) missing.push("name");
  if (!offer.pictures?.length) missing.push("pictures");
  if (!offer.vendor || offer.vendor.toLowerCase() === "без бренда") missing.push("vendor");
  if (!offer.description) missing.push("description");
  if (!(Number(offer.weightDimensions?.length) > 0 && Number(offer.weightDimensions?.weight) > 0)) {
    missing.push("weightDimensions");
  }

  return { offer, missing, ready: missing.length === 0 };
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

