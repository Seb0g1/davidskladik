async function getOzonStockMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v4/product/info/stocks", {
        filter: { offer_id: chunk, visibility: "ALL" },
        limit: chunk.length,
      }, account);
    } catch (error) {
      logger.warn("ozon stock chunk failed", {
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
      if (!offerId) continue;
      const stocks = Array.isArray(item.stocks) ? item.stocks : [];
      const warehouses = stocks.map(normalizeOzonStockWarehouse).filter(Boolean);
      const present = warehouses.reduce((sum, stock) => sum + Number(stock.present || 0), 0);
      const reserved = warehouses.reduce((sum, stock) => sum + Number(stock.reserved || 0), 0);
      const total = Number.isFinite(Number(item.stock)) ? Number(item.stock) : Math.max(0, present - reserved);
      setOzonOfferMapValue(map, offerId, { ...item, present, reserved, stock: total, warehouses });
    }
    options.onProgress?.({
      stage: "Остатки Ozon",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}

function pickOzonState(product = {}, info = {}, stockInfo = {}) {
  const visibility = cleanText(info.visibility || product.visibility || stockInfo.visibility).toUpperCase();
  const state = cleanText(info.status?.state || info.state || product.status || product.state).toUpperCase();
  const stateName = cleanText(info.status?.state_name || info.state_name || info.status_name);
  const stateDescription = cleanText(info.status?.state_description || info.state_description || info.status_description);
  const archived = Boolean(product.archived || info.archived || visibility === "ARCHIVED" || state === "ARCHIVED");
  const present = Number(stockInfo.present || 0);
  const reserved = Number(stockInfo.reserved || 0);
  const warehouses = Array.isArray(stockInfo.warehouses) ? stockInfo.warehouses : [];
  const stock = Number.isFinite(Number(stockInfo.stock)) ? Number(stockInfo.stock) : Math.max(0, present - reserved);
  const hasStocks = Boolean(product.has_fbs_stocks || product.hasFbsStocks || stock > 0);

  if (archived) {
    return normalizeMarketplaceState({ code: "archived", label: "В архиве Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (visibility === "EMPTY_STOCK" || (!hasStocks && stock <= 0)) {
    return normalizeMarketplaceState({ code: "out_of_stock", label: "Нет в наличии Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(visibility)
    || ["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(state)) {
    return normalizeMarketplaceState({ code: "inactive", label: "Неактивен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (visibility || state || hasStocks) {
    return normalizeMarketplaceState({ code: "active", label: "Активен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  return normalizeMarketplaceState({ code: "unknown", label: "Статус Ozon не загружен", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
}

async function getOzonPriceMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const chunks = chunkArray(offerIds, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v5/product/info/prices", {
        filter: { offer_id: chunk, visibility: "ALL" },
        limit: chunk.length,
      }, account);
    } catch (error) {
      logger.warn("ozon price info chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || []) {
      setOzonOfferMapValue(map, item.offer_id, item);
    }
    options.onProgress?.({
      stage: "Цены Ozon",
      processed: Math.min(offerIds.length, (index + 1) * 100),
      total: offerIds.length,
    });
  }

  return map;
}
