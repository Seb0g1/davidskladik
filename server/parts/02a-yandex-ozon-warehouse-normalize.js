function pickYandexOfferFromMapping(item = {}) {
  return item.offer || item.mapping?.offer || item.mapping || item;
}

function buildYandexProductUrl(offer = {}, item = {}) {
  const directUrl = cleanText(
    offer.url || offer.marketUrl || offer.publicUrl || item.url || item.marketUrl || item.publicUrl,
  );
  if (directUrl) return directUrl;

  const sku = cleanText(offer.marketSku || offer.modelId || item.marketSku || item.mapping?.marketSku);
  if (sku) return `https://market.yandex.ru/product--/${encodeURIComponent(sku)}`;

  const query = cleanText(offer.name || item.offer?.name || offer.offerId || item.offerId || item.mapping?.offerId);
  return query ? `https://market.yandex.ru/search?text=${encodeURIComponent(query)}` : "";
}

function pickYandexState(item = {}, offer = {}) {
  const rawState = cleanText(
    offer.campaignStatus?.status ||
      offer.processingState?.status ||
      offer.status ||
      offer.state ||
      offer.availability ||
      item.status ||
      item.state ||
      item.offer?.campaignStatus?.status ||
      item.offer?.processingState?.status ||
      item.offer?.status ||
      item.offer?.availability,
  );
  const availability = cleanText(offer.availability || item.offer?.availability || item.availability);
  const state = rawState.toLowerCase();
  const priceValue = Number(offer.basicPrice?.value ?? offer.price?.value ?? item.offer?.basicPrice?.value ?? item.price?.value ?? 0);
  const archived = Boolean(offer.archived || item.archived || item.offer?.archived || state.includes("archive") || availability === "DELISTED");
  const disabled = state.includes("inactive")
    || state.includes("disabled")
    || state.includes("disabled_by_partner")
    || state.includes("disabled_automatically")
    || state.includes("delisted")
    || state.includes("rejected")
    || state.includes("rejected_by_market")
    || state.includes("no_card")
    || state.includes("need_content")
    || state.includes("hidden")
    || state.includes("not published")
    || availability === "INACTIVE";
  const outOfStock = state.includes("out_of_stock")
    || state.includes("out-of-stock")
    || state.includes("no_stocks")
    || state.includes("unavailable")
    || state.includes("not_available")
    || state.includes("нет в наличии");

  if (archived) return { code: "archived", label: "В архиве ЯМ", stateName: rawState || availability || "Архив" };
  if (outOfStock || priceValue <= 0) return { code: "out_of_stock", label: "Нет наличия ЯМ", stateName: rawState || availability || "Нет цены или остатка" };
  if (disabled) return { code: "inactive", label: "Неактивен ЯМ", stateName: rawState || availability || "Неактивен" };
  return { code: "active", label: "Активен ЯМ", stateName: rawState || availability || "Опубликован" };
}

function normalizeYandexWarehouseProduct(item = {}, shop) {
  const offer = pickYandexOfferFromMapping(item);
  const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
  if (!offerId) return null;

  const priceValue = offer.basicPrice?.value ?? offer.price?.value ?? item.offer?.basicPrice?.value ?? item.price?.value;
  const pictures = offer.pictures || offer.urls || item.offer?.pictures || [];
  const barcodes = offer.barcodes || item.offer?.barcodes || [];

  return normalizeWarehouseProduct({
    id: yandexWarehouseProductId(shop, offerId),
    target: shop.id,
    marketplace: "yandex",
    targetName: shop.name || "Yandex Market",
    offerId,
    name: offer.name || item.offer?.name || offerId,
    source: "marketplace",
    imageUrl: firstImageUrl(pictures),
    productUrl: buildYandexProductUrl(offer, item),
    marketplacePrice: Number(priceValue || 0) || null,
    marketplaceState: pickYandexState(item, offer),
    yandex: {
      offerId,
      name: offer.name || item.offer?.name || offerId,
      url: buildYandexProductUrl(offer, item),
      description: offer.description || "",
      marketCategoryId: offer.marketCategoryId || item.mapping?.marketCategoryId,
      marketCategoryName: cleanText(offer.marketCategoryName || item.mapping?.marketCategoryName || ""),
      vendor: offer.vendor || "",
      pictures,
      barcodes,
      price: Number(priceValue || 0) || undefined,
      extra: {},
    },
    createdAt: new Date().toISOString(),
  });
}

function yandexWarehouseProductId(shop = {}, offerId = "") {
  const key = `${cleanText(shop.id || shop.businessId || "yandex")}:${cleanText(offerId).toLowerCase()}`;
  return `yandex-${crypto.createHash("sha1").update(key).digest("hex").slice(0, 24)}`;
}

function ozonWarehouseProductId(account = {}, offerId = "") {
  const key = `${cleanText(account.id || account.target || "ozon")}:${cleanText(offerId).toLowerCase()}`;
  return `ozon-${crypto.createHash("sha1").update(key).digest("hex").slice(0, 24)}`;
}


