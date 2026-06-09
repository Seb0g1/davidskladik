async function getOzonProducts(limit = Number.POSITIVE_INFINITY, account = null) {
  const parsedLimit = Number(limit);
  const maxItems = Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : Number.MAX_SAFE_INTEGER;
  const byKey = new Map();
  const visibilityModes = ["ALL", "ARCHIVED"];

  async function loadVisibility(visibility) {
    const items = [];
    let lastId = "";
    const visibilityMax = visibility === "ALL" ? maxItems : Number.MAX_SAFE_INTEGER;

    while (items.length < visibilityMax) {
      const batchLimit = Math.min(1000, visibilityMax - items.length);
      const data = await ozonRequest("/v3/product/list", {
        filter: { visibility },
        limit: batchLimit,
        last_id: lastId,
      }, account);

      const batch = data.result?.items || [];
      items.push(...batch);
      lastId = data.result?.last_id || "";

      if (!batch.length || !lastId) break;
    }
    return items;
  }

  for (const visibility of visibilityModes) {
    try {
      const items = await loadVisibility(visibility);
      for (const item of items) {
        const key = cleanText(item.offer_id || item.product_id || JSON.stringify(item));
        if (!key) continue;
        byKey.set(key, { ...item, visibility: item.visibility || visibility });
      }
    } catch (error) {
      if (visibility === "ALL") throw error;
      logger.warn("ozon archived list failed", { account: account?.id, visibility, detail: error?.message || String(error) });
    }
  }

  return Array.from(byKey.values());
}

function ozonOfferMapKey(value) {
  return cleanText(value).toLowerCase();
}

function setOzonOfferMapValue(map, offerId, value) {
  const exact = cleanText(offerId);
  if (!exact) return;
  map.set(exact, value);
  map.set(ozonOfferMapKey(exact), value);
}

function getOzonOfferMapValue(map, offerId) {
  const exact = cleanText(offerId);
  if (!exact) return undefined;
  return map.get(exact) || map.get(ozonOfferMapKey(exact));
}

function ozonProductIdFromInfo(info = {}) {
  return ozonNumericProductId(info.product_id || info.productId || info.id);
}

async function getOzonProductInfoMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v3/product/info/list", {
        offer_id: chunk,
      }, account);
    } catch (error) {
      logger.warn("ozon product info chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || data.result?.items || []) {
      const offerId = item.offer_id || item.offerId;
      if (offerId) setOzonOfferMapValue(map, offerId, item);
    }
    options.onProgress?.({
      stage: "Детали Ozon",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}

async function resolveOzonUnarchiveProductIds(items = [], account = null) {
  const rows = (Array.isArray(items) ? items : []).map((item) => ({
    item,
    productId: ozonNumericProductId(item.ozonProductId || item.productId || item.product_id),
    offerId: cleanText(item.offerId || item.offer_id),
  }));
  const missing = rows.filter((row) => !row.productId && row.offerId);
  if (missing.length) {
    const infoByOfferId = await getOzonProductInfoMap(
      Array.from(new Set(missing.map((row) => row.offerId))).filter(Boolean),
      account,
      { continueOnError: true },
    );
    for (const row of missing) {
      row.productId = ozonProductIdFromInfo(getOzonOfferMapValue(infoByOfferId, row.offerId));
    }
  }
  return rows;
}

async function getOzonProductInfoMapByProductIds(productIds, account = null, options = {}) {
  const map = new Map();
  const ids = productIds
    .map((productId) => String(productId || "").trim())
    .filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v3/product/info/list", {
        product_id: chunk.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0),
      }, account);
    } catch (error) {
      logger.warn("ozon product info by product id chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || data.result?.items || []) {
      const productId = cleanText(item.product_id || item.productId || item.id);
      if (productId) map.set(productId, item);
      const offerId = item.offer_id || item.offerId;
      if (offerId) setOzonOfferMapValue(map, offerId, item);
    }
    options.onProgress?.({
      stage: "Восстановление Ozon по product_id",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}
