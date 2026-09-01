
function normalizeOzonDraft(input = {}) {
  if (!hasDraftInput(input)) return {};
  const draft = compactObject({
    offerId: cleanText(input.offerId || input.offer_id),
    name: cleanText(input.name),
    vendor: cleanText(input.vendor || input.brand),
    description: cleanText(input.description),
    marketCategoryId: Number(input.marketCategoryId || input.market_category_id || 0) || undefined,
    categoryId: Number(input.categoryId || input.category_id || 0) || undefined,
    typeId: Number(input.typeId || input.type_id || input.descriptionTypeId || input.description_type_id || 0) || undefined,
    price: Number(input.price || 0) || undefined,
    minPrice: Number(input.minPrice || input.min_price || 0) || undefined,
    oldPrice: Number(input.oldPrice || input.old_price || 0) || undefined,
    marketingPrice: Number(input.marketingPrice || input.marketing_price || 0) || undefined,
    marketingSellerPrice: Number(input.marketingSellerPrice || input.marketing_seller_price || 0) || undefined,
    retailPrice: Number(input.retailPrice || input.retail_price || 0) || undefined,
    currencyCode: cleanText(input.currencyCode || input.currency_code || "RUB"),
    vat: input.vat !== undefined ? String(input.vat) : undefined,
    barcode: cleanText(input.barcode),
    barcodes: splitList(input.barcodes),
    depth: Number(input.depth || 0) || undefined,
    width: Number(input.width || 0) || undefined,
    height: Number(input.height || 0) || undefined,
    dimensionUnit: cleanText(input.dimensionUnit || input.dimension_unit || "mm"),
    weight: Number(input.weight || 0) || undefined,
    weightUnit: cleanText(input.weightUnit || input.weight_unit || "g"),
    primaryImage: cleanText(input.primaryImage || input.primary_image),
    images: splitList(input.images),
    images360: splitList(input.images360),
    colorImage: cleanText(input.colorImage || input.color_image),
    attributes: parseJsonField(input.attributesJson ?? input.attributes, []),
    complexAttributes: parseJsonField(input.complexAttributesJson ?? input.complex_attributes, []),
    extra: parseJsonField(input.extraJson ?? input.extra, {}),
  });

  return hasObjectData(draft) ? draft : {};
}

function normalizeYandexDraft(input = {}) {
  if (!hasDraftInput(input)) return {};
  const draft = compactObject({
    offerId: cleanText(input.offerId || input.offer_id),
    name: cleanText(input.name),
    description: cleanText(input.description),
    marketCategoryId: Number(input.marketCategoryId || input.market_category_id || input.categoryId || 0) || undefined,
    vendor: cleanText(input.vendor || input.brand),
    pictures: splitList(input.pictures || input.images),
    barcodes: splitList(input.barcodes || input.barcode),
    price: Number(input.price || 0) || undefined,
    extra: parseJsonField(input.yandexExtraJson ?? input.extra, {}),
  });

  return hasObjectData(draft) ? draft : {};
}

function normalizeProductExports(exports = {}) {
  if (!exports || typeof exports !== "object") return {};
  return Object.fromEntries(
    Object.entries(exports)
      .filter(([target]) => target)
      .map(([target, value]) => [
        target,
        compactObject({
          status: cleanText(value?.status),
          sentAt: value?.sentAt || null,
          error: cleanText(value?.error),
          targetName: cleanText(value?.targetName),
        }),
      ]),
  );
}

function normalizeAiImageDraft(input = {}) {
  if (!input || typeof input !== "object") return null;
  const status = cleanText(input.status || "pending").toLowerCase();
  const allowedStatus = new Set(["pending", "approved", "rejected"]);
  const draft = compactObject({
    id: cleanText(input.id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "pending",
    prompt: cleanText(input.prompt),
    productName: cleanText(input.productName || input.product_name),
    sourceImageUrl: cleanText(input.sourceImageUrl || input.source_image_url),
    resultUrl: cleanText(input.resultUrl || input.result_url || input.url),
    batchId: cleanText(input.batchId || input.batch_id),
    variantIndex: Number(input.variantIndex || input.variant_index || 0) || 0,
    variantTotal: Number(input.variantTotal || input.variant_total || 0) || 0,
    presetId: cleanText(input.presetId || input.preset_id),
    presetLabel: cleanText(input.presetLabel || input.preset_label),
    slotOrder: Number(input.slotOrder) || undefined,
    layout: cleanText(input.layout),
    model: cleanText(input.model),
    size: cleanText(input.size),
    quality: cleanText(input.quality),
    format: cleanText(input.format),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    reviewedAt: input.reviewedAt || input.reviewed_at || null,
    sentAt: input.sentAt || input.sent_at || null,
    sentMarketplace: cleanText(input.sentMarketplace || input.sent_marketplace),
    sendResult: input.sendResult && typeof input.sendResult === "object" ? input.sendResult : undefined,
  });
  return draft.resultUrl || draft.sourceImageUrl || draft.prompt ? draft : null;
}

function normalizeAiImageDrafts(input = []) {
  const drafts = Array.isArray(input) ? input : [];
  return drafts.map(normalizeAiImageDraft).filter(Boolean).slice(-50);
}

function aiRecommendationText(input) {
  if (input === undefined || input === null) return "";
  if (typeof input === "string" || typeof input === "number" || typeof input === "boolean") {
    const value = cleanText(input);
    return value === "[object Object]" ? "" : value;
  }
  if (Array.isArray(input)) return input.map(aiRecommendationText).filter(Boolean).join(": ");
  if (typeof input === "object") {
    return aiRecommendationText(
      input.message
        || input.text
        || input.name
        || input.title
        || input.description
        || input.comment
        || input.reason
        || input.recommendation
        || input.value
        || input.details
        || input.error,
    );
  }
  return "";
}

function normalizeAiContentDraft(input = {}) {
  if (!input || typeof input !== "object") return null;
  const status = cleanText(input.status || "pending").toLowerCase();
  const allowedStatus = new Set(["pending", "approved", "rejected"]);
  const draft = compactObject({
    id: cleanText(input.id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "pending",
    marketplace: cleanText(input.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex",
    source: cleanText(input.source || "manual"),
    name: cleanText(input.name),
    vendor: cleanText(input.vendor || input.brand),
    description: cleanText(input.description),
    bulletPoints: Array.isArray(input.bulletPoints || input.bullets)
      ? (input.bulletPoints || input.bullets).map((item) => cleanText(item)).filter(Boolean).slice(0, 12)
      : [],
    seoKeywords: Array.isArray(input.seoKeywords || input.keywords)
      ? (input.seoKeywords || input.keywords).map((item) => cleanText(item)).filter(Boolean).slice(0, 20)
      : [],
    qualityBefore: Number.isFinite(Number(input.qualityBefore)) ? Number(input.qualityBefore) : undefined,
    recommendations: Array.isArray(input.recommendations)
      ? input.recommendations.map(aiRecommendationText).filter(Boolean).slice(0, 20)
      : [],
    model: cleanText(input.model),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    reviewedAt: input.reviewedAt || input.reviewed_at || null,
    sentAt: input.sentAt || input.sent_at || null,
    sentMarketplace: cleanText(input.sentMarketplace || input.sent_marketplace),
    sendResult: input.sendResult && typeof input.sendResult === "object" ? input.sendResult : undefined,
  });
  return draft.name || draft.description || draft.vendor || draft.bulletPoints?.length ? draft : null;
}

function normalizeAiContentDrafts(input = []) {
  const drafts = Array.isArray(input) ? input : [];
  return drafts.map(normalizeAiContentDraft).filter(Boolean).slice(-50);
}

function normalizeMarketplaceState(input = {}) {
  if (!input || typeof input !== "object") {
    return { code: "unknown", label: "Статус не загружен" };
  }
  const warehouses = Array.isArray(input.warehouses)
    ? input.warehouses
        .map((warehouse) => ({
          warehouseId: cleanText(warehouse.warehouseId || warehouse.warehouse_id || warehouse.id),
          warehouseName: cleanText(warehouse.warehouseName || warehouse.warehouse_name || warehouse.name),
          present: Number.isFinite(Number(warehouse.present)) ? Number(warehouse.present) : 0,
          reserved: Number.isFinite(Number(warehouse.reserved)) ? Number(warehouse.reserved) : 0,
          stock: Number.isFinite(Number(warehouse.stock)) ? Number(warehouse.stock) : undefined,
        }))
        .filter((warehouse) => warehouse.warehouseId || warehouse.warehouseName)
    : [];
  return compactObject({
    code: cleanText(input.code || "unknown"),
    label: cleanText(input.label || "Статус не загружен"),
    visibility: cleanText(input.visibility),
    state: cleanText(input.state),
    stateName: cleanText(input.stateName || input.state_name),
    stateDescription: cleanText(input.stateDescription || input.state_description),
    stock: Number.isFinite(Number(input.stock)) ? Number(input.stock) : undefined,
    present: Number.isFinite(Number(input.present)) ? Number(input.present) : undefined,
    reserved: Number.isFinite(Number(input.reserved)) ? Number(input.reserved) : undefined,
    warehouses,
    archived: input.archived !== undefined ? Boolean(input.archived) : undefined,
    isAutoArchived: input.isAutoArchived !== undefined ? Boolean(input.isAutoArchived) : undefined,
    hasStocks: input.hasStocks !== undefined ? Boolean(input.hasStocks) : undefined,
    partial: input.partial || input.partialSync || input.isPartial ? true : undefined,
  });
}

function normalizeOzonPriceDetails(input = {}) {
  const price = input.price && typeof input.price === "object" ? input.price : input;
  const minPrice = parseMoneyValue(price.min_price ?? price.minPrice ?? input.min_price ?? input.minPrice);
  const currentPrice = parseMoneyValue(
    price.price ?? input.price?.price ?? (typeof input.price === "object" ? input.price?.price : undefined),
  );
  const oldPrice = parseMoneyValue(price.old_price ?? price.oldPrice ?? input.old_price ?? input.oldPrice);
  const marketingPrice = parseMoneyValue(
    price.marketing_price ?? price.marketingPrice ?? input.marketing_price ?? input.marketingPrice,
  );
  const marketingSellerPrice = parseMoneyValue(
    price.marketing_seller_price ?? price.marketingSellerPrice ?? input.marketing_seller_price ?? input.marketingSellerPrice,
  );
  const retailPrice = parseMoneyValue(price.retail_price ?? price.retailPrice ?? input.retail_price ?? input.retailPrice);
  return compactObject({
    currentPrice,
    minPrice,
    oldPrice,
    marketingPrice,
    marketingSellerPrice,
    retailPrice,
    currencyCode: cleanText(price.currency_code || price.currencyCode || input.currency_code || input.currencyCode),
  });
}

function normalizeLastPriceSend(input = {}) {
  if (!input || typeof input !== "object") return null;
  if (!Object.keys(input).length) return null;
  const status = cleanText(input.status || "").toLowerCase();
  const at = cleanText(input.at || input.sentAt || input.updatedAt);
  const requestedPrice = Number(input.requestedPrice ?? input.price ?? input.newPrice ?? 0);
  const cabinetPriceAtSend = Number(input.cabinetPriceAtSend ?? input.oldPrice ?? 0);
  const oldPriceForRetry = Number(input.oldPriceForRetry ?? 0);
  const detail = cleanText(input.detail || input.error);
  const nextRetryAt = cleanText(input.nextRetryAt || input.retryAt);
  if (!status && !at && !detail && !nextRetryAt && !Number.isFinite(requestedPrice) && !Number.isFinite(cabinetPriceAtSend)) return null;
  return {
    status: status || null,
    at: at || null,
    requestedPrice: Number.isFinite(requestedPrice) && requestedPrice > 0 ? roundPrice(requestedPrice) : null,
    cabinetPriceAtSend: Number.isFinite(cabinetPriceAtSend) && cabinetPriceAtSend > 0 ? roundPrice(cabinetPriceAtSend) : null,
    oldPriceForRetry: Number.isFinite(oldPriceForRetry) && oldPriceForRetry > 0 ? roundPrice(oldPriceForRetry) : null,
    detail: detail || "",
    nextRetryAt: nextRetryAt || null,
  };
}

function normalizeLastMarketplaceCommand(input = {}) {
  if (!input || typeof input !== "object") return null;
  if (!Object.keys(input).length) return null;
  const type = cleanText(input.type || input.action || input.command);
  const status = cleanText(input.status || (input.ok === true ? "success" : (input.ok === false ? "error" : ""))).toLowerCase();
  const at = cleanText(input.at || input.sentAt || input.updatedAt);
  const target = cleanText(input.target || input.shopId || input.account || "");
  const offerId = cleanText(input.offerId || input.offer_id || input.sku || "");
  const stock = Number(input.stock ?? input.count ?? input.targetStock ?? NaN);
  const error = cleanText(input.error || "");
  const warning = cleanText(input.warning || "");
  const detail = cleanText(input.detail || input.message || "");
  const nextRetryAt = cleanText(input.nextRetryAt || input.next_retry_at || "");
  const pending = Boolean(input.pending);
  const queuedByDailyLimit = Boolean(input.queuedByDailyLimit);
  if (!type && !status && !at && !target && !offerId && !error && !warning && !detail && !Number.isFinite(stock)) return null;
  return compactObject({
    type: type || null,
    status: status || null,
    at: at || null,
    target: target || null,
    offerId: offerId || null,
    stock: Number.isFinite(stock) ? Math.max(0, Math.round(stock)) : null,
    error: error || null,
    warning: warning || null,
    detail: detail || null,
    pending,
    queuedByDailyLimit,
    nextRetryAt: nextRetryAt || null,
  });
}

