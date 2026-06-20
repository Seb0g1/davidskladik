function marketplaceCommandFromAction(action = {}, product = {}, at = new Date().toISOString()) {
  return normalizeLastMarketplaceCommand({
    type: action.type,
    status: action.ok ? "success" : (action.skipped ? "skipped" : "error"),
    at,
    target: action.target || product.target,
    offerId: action.offerId || product.offerId,
    stock: action.stock,
    error: action.error,
    warning: action.warning,
    detail: action.reason || "",
    pending: action.pending,
    queuedByDailyLimit: action.queuedByDailyLimit,
    nextRetryAt: action.nextRetryAt,
  });
}

function normalizeWarehouseProduct(input = {}) {
  const target = cleanText(input.target || input.marketplace || "ozon");
  const inputMarketplace = cleanText(input.marketplace || input.marketplace_id || "").toLowerCase();
  const normalizedTarget = target.toLowerCase();
  const fallbackMarketplace = inputMarketplace === "yandex" || normalizedTarget === "yandex" || normalizedTarget.startsWith("yandex-") ? "yandex" : "ozon";
  const targetMeta = targetById(target) || {
    id: target,
    marketplace: fallbackMarketplace,
    name: fallbackMarketplace === "yandex" ? "Yandex Market" : target,
  };
  const ozonDraft = normalizeOzonDraft(input.ozon || input.ozonDraft || {});
  const yandexDraft = normalizeYandexDraft(input.yandex || input.yandexDraft || {});
  const imageUrl = firstImageUrl(input.imageUrl || input.image || input.primaryImage || ozonDraft.primaryImage || ozonDraft.images || yandexDraft.pictures);
  const name = cleanText(input.name || ozonDraft.name || yandexDraft.name || input.offerId || input.offer_id);
  const rawMarkup = Number(input.markup || 0);
  const keepYandexMarkup = Boolean(input.markupSource === "manual" || input.yandex?.extra?.manualMarkup === true);
  const offerId = cleanText(input.offerId || input.offer_id);
  const resolvedId = cleanText(input.id)
    || warehouseProductCanonicalId({ target: targetMeta.id, marketplace: targetMeta.marketplace, offerId })
    || crypto.randomUUID();
  const links = compactWarehouseLinks(input.links || []);
  const rawEverHadLinks = input.raw && typeof input.raw === "object" && !Array.isArray(input.raw)
    ? input.raw.everHadLinks
    : false;
  const everHadLinks = Boolean(input.everHadLinks || input.ever_had_links || rawEverHadLinks || links.length > 0);
  return {
    id: resolvedId,
    target: targetMeta.id,
    marketplace: targetMeta.marketplace,
    targetName: resolveWarehouseProductTargetName({
      target: targetMeta.id,
      marketplace: targetMeta.marketplace,
      targetName: cleanText(input.targetName) || targetMeta.name,
      exports: input.exports,
    }),
    offerId,
    productId: cleanText(input.productId || input.product_id),
    sku: cleanText(input.sku || input.productSku || input.fboSku || input.fbsSku),
    productUrl: cleanText(input.productUrl || input.product_url || input.url),
    manualGroupId: cleanText(input.manualGroupId || input.manual_group_id),
    imageUrl,
    marketplacePrice: Number(input.marketplacePrice ?? input.currentPrice ?? input.current_price ?? 0) || null,
    marketplaceMinPrice: Number(input.marketplaceMinPrice ?? input.minPrice ?? input.min_price ?? input.ozonMinPrice ?? 0) || null,
    currentPrice: Number(input.currentPrice ?? input.marketplacePrice ?? input.current_price ?? 0) || null,
    targetPrice: Number(input.targetPrice ?? input.nextPrice ?? input.calculatedPrice ?? 0) || null,
    targetStock: Number.isFinite(Number(input.targetStock)) ? Number(input.targetStock) : null,
    stockOnlyManualPrices: normalizeStockOnlyManualPrices(input.stockOnlyManualPrices || input.stock_only_manual_prices || input.raw?.stockOnlyManualPrices),
    supplierCount: Number.isFinite(Number(input.supplierCount)) ? Number(input.supplierCount) : 0,
    availableSupplierCount: Number.isFinite(Number(input.availableSupplierCount)) ? Number(input.availableSupplierCount) : 0,
    name,
    keyword: cleanText(input.keyword),
    markup: targetMeta.marketplace === "yandex" && !keepYandexMarkup ? 0 : rawMarkup,
    autoPriceEnabled: input.autoPriceEnabled !== undefined ? Boolean(input.autoPriceEnabled) : true,
    autoPriceMin: Number.isFinite(Number(input.autoPriceMin)) && Number(input.autoPriceMin) > 0 ? roundPrice(Number(input.autoPriceMin)) : null,
    autoPriceMax: Number.isFinite(Number(input.autoPriceMax)) && Number(input.autoPriceMax) > 0 ? roundPrice(Number(input.autoPriceMax)) : null,
    source: cleanText(input.source || (input.productId || input.product_id ? "marketplace" : "manual")),
    ozon: ozonDraft,
    yandex: yandexDraft,
    lastOzonPriceSend: normalizeLastPriceSend(input.lastOzonPriceSend || input.last_ozon_price_send),
    lastYandexPriceSend: normalizeLastPriceSend(input.lastYandexPriceSend || input.last_yandex_price_send),
    lastStockSend: normalizeLastMarketplaceCommand(input.lastStockSend || input.last_stock_send),
    lastArchiveSend: normalizeLastMarketplaceCommand(input.lastArchiveSend || input.last_archive_send),
    marketplaceState: normalizeMarketplaceState(input.marketplaceState || input.marketplace_state || input.ozonState),
    exports: normalizeProductExports(input.exports),
    aiImages: normalizeAiImageDrafts(input.aiImages || input.ai_images || input.imageDrafts),
    aiContentDrafts: normalizeAiContentDrafts(input.aiContentDrafts || input.ai_content_drafts || input.contentDrafts),
    priceHistory: Array.isArray(input.priceHistory) ? input.priceHistory.slice(-100) : [],
    noSupplierAutomation: {
      stockZeroAt: input.noSupplierAutomation?.stockZeroAt || null,
      archivedAt: input.noSupplierAutomation?.archivedAt || null,
      recoveredAt: input.noSupplierAutomation?.recoveredAt || null,
      manualSellableAt: input.noSupplierAutomation?.manualSellableAt || null,
      lastError: input.noSupplierAutomation?.lastError || null,
    },
    hasSnoozedLinks: Boolean(input.hasSnoozedLinks),
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || new Date().toISOString(),
    // Bumped ONLY by user-initiated edits (link save, product patch, group ops) — never by
    // background automation. Optimistic-locking compares this, so price/stock sweeps bumping
    // updatedAt no longer raise a false "изменено другим пользователем" conflict.
    userUpdatedAt: input.userUpdatedAt || null,
    links,
    everHadLinks,
  };
}

function normalizeManualPriceValue(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return null;
  return roundPrice(number);
}

function normalizeStockOnlyManualPrices(input = {}) {
  const value = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  return compactObject({
    default: normalizeManualPriceValue(value.default ?? value.base ?? value.group),
    ozon: normalizeManualPriceValue(value.ozon),
    yandex: normalizeManualPriceValue(value.yandex),
  });
}

function stockOnlyManualPriceForProduct(product = {}) {
  const prices = normalizeStockOnlyManualPrices(product.stockOnlyManualPrices || product.raw?.stockOnlyManualPrices);
  const marketplace = cleanText(product.marketplace).toLowerCase();
  const scoped = marketplace === "ozon" ? prices.ozon : marketplace === "yandex" ? prices.yandex : null;
  return normalizeManualPriceValue(scoped ?? prices.default);
}

function parsePriceMasterNoArticleRowId(value) {
  const text = cleanText(value);
  const prefix = "__no_article__:";
  if (!text.toLowerCase().startsWith(prefix)) return "";
  return cleanText(text.slice(prefix.length));
}

