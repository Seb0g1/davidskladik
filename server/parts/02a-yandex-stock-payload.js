function pickOzonProductStockForYandex(product = {}) {
  const state = product.marketplaceState || {};
  const stateCode = cleanText(state.code || state.state).toLowerCase();
  const visibility = cleanText(state.visibility || product.visibility).toUpperCase();
  if (stateCode === "archived" || visibility === "ARCHIVED" || state.archived || product.archived) return 0;
  const direct = Number(state.stock ?? state.present ?? product.targetStock ?? 0);
  if (Number.isFinite(direct) && direct > 0) return Math.max(0, Math.round(direct));
  const warehouses = Array.isArray(state.warehouses) ? state.warehouses : [];
  const sum = warehouses.reduce((total, warehouse) => {
    const value = Number(warehouse.stock ?? warehouse.present ?? 0);
    return total + (Number.isFinite(value) ? Math.max(0, value) : 0);
  }, 0);
  return Math.max(0, Math.round(sum));
}

function buildYandexStockUpdatePayload(rows = [], updatedAt = new Date().toISOString()) {
  const skus = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      sku: cleanText(row.sku || row.offerId || row.offer_id),
      count: Math.max(0, Math.round(Number(row.count ?? row.stock ?? 0) || 0)),
    }))
    .filter((row) => row.sku)
    .map((row) => ({
      sku: row.sku,
      items: [{ type: "FIT", count: row.count, updatedAt }],
    }));
  return { skus };
}

function buildYandexStockRestoreProducts(rows = [], shops = null) {
  const positiveRows = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      offerId: cleanText(row.offerId || row.offer_id),
      stock: Math.max(0, Math.round(Number(row.stock ?? row.targetStock ?? 0) || 0)),
    }))
    .filter((row) => row.offerId && row.stock > 0);
  const restoreShops = uniqueYandexShopsByBusiness(shops);
  return restoreShops.flatMap((shop) => positiveRows.map((row) => ({
    id: yandexWarehouseProductId(shop, row.offerId),
    marketplace: "yandex",
    target: shop.id || "yandex",
    offerId: row.offerId,
  })));
}

async function restoreYandexArchivedStocks(rows = [], shops = null) {
  const restoreProducts = buildYandexStockRestoreProducts(rows, shops);
  if (!restoreProducts.length) return [];
  const actions = await unarchiveProductsOnMarketplaces(restoreProducts);
  return verifyYandexUnarchiveActions(restoreProducts, actions);
}

async function preUnarchiveYandexOffersBeforeStockSend(products = [], shops = [], options = {}) {
  const warnings = [];
  if (!Array.isArray(products) || !products.length || !shops.length) {
    return { warnings, unarchiveActions: [] };
  }
  const warehouse = options.warehouse || await readWarehouse().catch(() => ({ products: [] }));
  const offerIds = new Set(products.map((product) => cleanText(product.offerId).toLowerCase()).filter(Boolean));
  const yandexProducts = (warehouse.products || []).filter((product) =>
    product.marketplace === "yandex"
    && offerIds.has(cleanText(product.offerId).toLowerCase())
    && productLooksArchived(product));
  if (!yandexProducts.length) return { warnings, unarchiveActions: [] };
  const actions = await verifyYandexUnarchiveActions(
    yandexProducts,
    await unarchiveProductsOnMarketplaces(yandexProducts),
  );
  const pending = actions.filter((item) => item.pending);
  if (pending.length) {
    warnings.push(`Yandex: ${pending.length} offer(s) still pending unarchive before stock send`);
  }
  return { warnings, unarchiveActions: actions };
}

function yandexStockShops(shops = null) {
  const source = Array.isArray(shops) && shops.length ? shops : getYandexShops();
  const expanded = source.flatMap((shop) => parseYandexCampaignIds(shop.campaignId || shop.campaign_id).map((campaignId, index) => ({
    ...shop,
    id: index === 0 ? shop.id : `${shop.id}-${campaignId}`,
    name: index === 0 ? shop.name : `${shop.name || shop.id} #${campaignId}`,
    campaignId,
  }))).filter((shop) => (
    shop.apiKey
    && shop.businessId
    && shop.campaignId
  ));
  const allowed = expanded.filter((shop) => yandexStockCampaignIds.has(String(shop.campaignId)));
  const skipped = expanded.filter((shop) => !yandexStockCampaignIds.has(String(shop.campaignId)));
  if (skipped.length) {
    logger.warn("yandex stock campaign skipped", {
      allowedCampaignIds: Array.from(yandexStockCampaignIds),
      skippedCampaignIds: [...new Set(skipped.map((shop) => String(shop.campaignId)))],
    });
  }
  return allowed;
}

function parseYandexCampaignIds(value) {
  return [...new Set(cleanText(value)
    .split(/[\s,;]+/)
    .map(cleanText)
    .filter(Boolean))];
}

function yandexMissingStockCampaignWarning(count) {
  return `Yandex: ${count} магазин(ов) без campaignId пропущены для остатков. Добавьте Campaign ID в кабинетах маркетплейсов.`;
}

