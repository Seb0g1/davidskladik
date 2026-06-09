function resolveWarehouseCanonicalTarget(input = {}) {
  const marketplace = cleanText(input.marketplace || input.marketplace_id || "").toLowerCase();
  const target = cleanText(input.target || input.marketplace || "ozon");
  const normalizedTarget = target.toLowerCase();
  if (marketplace === "yandex" || normalizedTarget === "yandex" || normalizedTarget.startsWith("yandex-")) {
    const shop = getYandexShopByTarget(target);
    if (shop?.id) return shop.id;
  }
  if (marketplace === "ozon" || normalizedTarget === "ozon" || normalizedTarget.startsWith("ozon-")) {
    const account = getOzonAccountByTarget(target);
    if (account?.id) return account.id;
  }
  return target;
}

function warehouseProductCanonicalId(input = {}) {
  const target = resolveWarehouseCanonicalTarget(input);
  const marketplace = cleanText(input.marketplace || input.marketplace_id || "").toLowerCase();
  const normalizedTarget = target.toLowerCase();
  const resolvedMarketplace = marketplace === "yandex" || normalizedTarget === "yandex" || normalizedTarget.startsWith("yandex-")
    ? "yandex"
    : "ozon";
  const offerId = cleanText(input.offerId || input.offer_id);
  if (!offerId) return cleanText(input.id) || "";
  if (resolvedMarketplace === "yandex") return yandexWarehouseProductId({ id: target }, offerId);
  if (resolvedMarketplace === "ozon") return ozonWarehouseProductId({ id: target }, offerId);
  return cleanText(input.id) || "";
}

function pickWarehouseProductMergeId(current = null, imported = null) {
  const target = cleanText(imported?.target || current?.target);
  const marketplace = cleanText(imported?.marketplace || current?.marketplace);
  const offerId = cleanText(imported?.offerId || current?.offerId);
  const canonicalId = warehouseProductCanonicalId({ target, marketplace, offerId });
  const candidates = [current?.id, imported?.id].filter(Boolean);
  if (canonicalId && candidates.includes(canonicalId)) return canonicalId;
  return current?.id || imported?.id || canonicalId || crypto.randomUUID();
}

function buildYandexWarehouseProductFromOzonExport(product = {}, shop = {}, exportState = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const offerId = cleanText(normalized.offerId);
  const price = Number(exportState.price || exportState.targetPrice || 0) || null;
  const sentAt = exportState.sentAt || new Date().toISOString();
  const lastYandexPriceSend = price > 0
    ? {
        status: "success",
        at: sentAt,
        requestedPrice: price,
        cabinetPriceAtSend: null,
        detail: "",
        nextRetryAt: null,
      }
    : null;
  const stock = Number.isFinite(Number(exportState.stock))
    ? Math.max(0, Math.round(Number(exportState.stock)))
    : pickOzonProductStockForYandex(normalized);
  const pictures = [
    ...new Set([
      ...([normalized.imageUrl].filter(Boolean)),
      ...(Array.isArray(normalized.ozon?.images) ? normalized.ozon.images : []),
      ...(Array.isArray(normalized.yandex?.pictures) ? normalized.yandex.pictures : []),
    ].map(cleanText).filter(Boolean)),
  ];
  const pairGroupId = normalized.manualGroupId || buildOzonYandexAutoPairGroupId(normalized);
  return normalizeWarehouseProduct({
    id: yandexWarehouseProductId(shop, offerId),
    target: shop.id || "yandex",
    marketplace: "yandex",
    targetName: shop.name || "Yandex Market",
    offerId,
    productId: normalized.productId,
    sku: normalized.sku,
    manualGroupId: pairGroupId,
    name: normalized.name || normalized.ozon?.name || normalized.yandex?.name || offerId,
    keyword: normalized.keyword,
    imageUrl: normalized.imageUrl || firstImageUrl(pictures),
    productUrl: normalized.yandex?.url || "",
    marketplacePrice: price,
    currentPrice: price,
    targetPrice: price,
    targetStock: stock,
    markup: Number(exportState.productMarkup || 0) || 0,
    autoPriceEnabled: normalized.autoPriceEnabled,
    autoPriceMin: normalized.autoPriceMin,
    autoPriceMax: normalized.autoPriceMax,
    lastYandexPriceSend,
    source: "marketplace",
    yandex: {
      ...(normalized.yandex || {}),
      offerId,
      name: normalized.name || normalized.ozon?.name || normalized.yandex?.name || offerId,
      description: normalized.ozon?.description || normalized.yandex?.description || "",
      vendor: normalized.ozon?.vendor || normalized.yandex?.vendor || normalized.brand || "",
      pictures,
      price: price || undefined,
      extra: {
        ...(normalized.yandex?.extra || {}),
        exportedFrom: "ozon",
        sourceProductId: normalized.id,
        shopId: shop.id || "",
        businessId: shop.businessId || "",
        campaignId: shop.campaignId || "",
        exportedAt: sentAt,
      },
    },
    marketplaceState: {
      code: stock > 0 ? "active" : "out_of_stock",
      label: stock > 0 ? "Активен ЯМ" : "Нет наличия ЯМ",
      stock,
      stateName: exportState.status === "sent" ? "Выгружено из Ozon" : "Создано из Ozon",
    },
    exports: {
      ...(normalized.exports || {}),
      yandex: exportState,
      [shop.id || "yandex"]: exportState,
    },
    links: Array.isArray(normalized.links) ? normalized.links : [],
    createdAt: normalized.createdAt,
    updatedAt: sentAt,
  });
}

