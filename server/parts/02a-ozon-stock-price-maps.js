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

    const stockItems = data.items || data.result?.items || [];
    if (stockItems.length > 0) {
      validateApiShape(data.result || data, ["items[0].offer_id"], "ozon_stocks_v4");
      // Ozon returns stocks:[] for zero-stock products — only validate the present field
      // on an item that actually has stock entries, to avoid false shape-mismatch noise.
      const firstWithStocks = stockItems.findIndex((i) => Array.isArray(i.stocks) && i.stocks.length > 0);
      if (firstWithStocks >= 0) {
        const stockSample = stockItems[firstWithStocks].stocks[0];
        if (!("present" in stockSample)) {
          logger.warn("ozon_stocks_v4: stocks[0] missing 'present' field", {
            keys: Object.keys(stockSample),
            sample: stockSample,
          });
        }
      }
    }
    for (const item of stockItems) {
      const offerId = item.offer_id || item.offerId;
      if (!offerId) continue;
      const stocks = Array.isArray(item.stocks) ? item.stocks : [];
      const warehouses = stocks.map(normalizeOzonStockWarehouse).filter(Boolean);
      // Sum present/reserved from the RAW stocks array, not from `warehouses`: the
      // /v4/product/info/stocks response keys stock by `type` (fbs/rfbs/fbo) with an EMPTY
      // `warehouse_ids`, so normalizeOzonStockWarehouse drops every entry (it needs a
      // warehouse id/name) and the warehouse-based sum was always 0 → FBS products were
      // wrongly reported out_of_stock (e.g. #YV005928# had fbs present=5 but read as 0).
      const present = stocks.reduce((sum, stock) => sum + Math.max(0, Number(stock.present || 0)), 0);
      const reserved = stocks.reduce((sum, stock) => sum + Math.max(0, Number(stock.reserved || 0)), 0);
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
  // /v3/product/info/list has no visibility/state strings — archive state comes only from the
  // explicit is_archived/is_autoarchived booleans. When present they are authoritative in both
  // directions (detect autoarchive AND clear the flag after a successful unarchive); otherwise
  // fall back to the legacy sticky detection used by the full /v3/product/list import.
  const hasExplicitArchiveFlags = typeof info.is_archived === "boolean" || typeof info.is_autoarchived === "boolean";
  const archived = hasExplicitArchiveFlags
    ? Boolean(info.is_archived || info.is_autoarchived)
    : Boolean(product.archived || info.archived || visibility === "ARCHIVED" || state === "ARCHIVED");
  const present = Number(stockInfo.present || 0);
  const reserved = Number(stockInfo.reserved || 0);
  const warehouses = Array.isArray(stockInfo.warehouses) ? stockInfo.warehouses : [];
  const stock = Number.isFinite(Number(stockInfo.stock)) ? Number(stockInfo.stock) : Math.max(0, present - reserved);
  const hasStocks = Boolean(product.has_fbs_stocks || product.hasFbsStocks || stock > 0);

  if (archived) {
    return normalizeMarketplaceState({ code: "archived", label: info.is_autoarchived ? "В автоархиве Ozon" : "В архиве Ozon", isAutoArchived: Boolean(info.is_autoarchived), visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  // Ozon reports visibility=EMPTY_STOCK for FBS/rfbs products even when the seller's own
  // stock is positive (it reflects Ozon's cross-dock stock, not ours) — don't let that alone
  // flip a product to out_of_stock; trust our own stock/hasStocks reading instead.
  if (!hasStocks && stock <= 0) {
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
