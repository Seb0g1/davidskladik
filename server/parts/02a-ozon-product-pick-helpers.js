function ozonExistingProductMap(products = [], account = {}) {
  const map = new Map();
  const accountId = cleanText(account.id || "ozon");
  for (const product of products || []) {
    if (cleanText(product.marketplace) !== "ozon") continue;
    const offerId = cleanText(product.offerId || product.ozon?.offerId);
    if (!offerId) continue;
    const target = cleanText(product.target || "ozon");
    if (target !== accountId && target !== "ozon") continue;
    setOzonOfferMapValue(map, offerId, product);
  }
  return map;
}

function ozonProductNeedsDetailRefresh(product = {}) {
  if (!product || !product.id) return true;
  const offerId = cleanText(product.offerId || product.ozon?.offerId);
  if (isWeakProductName(product.name, offerId)) return true;
  if (!cleanText(product.imageUrl || product.ozon?.primaryImage)) return true;
  if (!cleanText(product.productId || product.ozon?.productId)) return true;
  if (!cleanText(product.marketplaceState?.code) || product.marketplaceState?.partial) return true;
  if (!Number(product.marketplacePrice || product.ozon?.price || 0)) return true;
  if (!cleanText(product.ozon?.description)) return true;
  return false;
}

function isWeakOzonWarehouseProduct(product = {}) {
  if (!product || cleanText(product.marketplace) !== "ozon") return false;
  const offerId = cleanText(product.offerId || product.ozon?.offerId);
  if (!offerId) return false;
  return ozonProductNeedsDetailRefresh(product);
}

function pickWeakOzonProductIds(products = [], maxItems = 400) {
  const limit = Math.max(0, Math.min(1000, Number(maxItems || 0) || 0));
  if (!limit) return [];
  const ids = [];
  const seen = new Set();
  for (const product of products || []) {
    const id = cleanText(product?.id);
    if (!id || seen.has(id) || !isWeakOzonWarehouseProduct(product)) continue;
    ids.push(id);
    seen.add(id);
    if (ids.length >= limit) break;
  }
  return ids;
}

function pickOzonDetailOfferIds(products = [], existingByOffer = new Map(), maxItems = 800) {
  const limit = Math.max(0, Number(maxItems || 0) || 0);
  if (!limit) return [];
  const prioritized = [];
  const seen = new Set();
  for (const product of products || []) {
    const offerId = cleanText(product.offer_id || product.offerId);
    const seenKey = ozonOfferMapKey(offerId);
    if (!offerId || seen.has(seenKey)) continue;
    const existing = getOzonOfferMapValue(existingByOffer, offerId);
    if (!existing || ozonProductNeedsDetailRefresh(existing)) {
      prioritized.push(offerId);
      seen.add(seenKey);
      if (prioritized.length >= limit) break;
    }
  }
  return prioritized;
}


