async function deleteYandexSmallVolumeOffers({
  dryRun = true,
  limit = 50000,
  protectedBrands = ["__protected__"],
} = {}) {
  const preview = await buildYandexCleanupPreview({ protectedBrands, limit });
  const rows = (preview.rows || []).filter((row) => row.smallVolume && row.action === "delete");
  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      generatedAt: preview.generatedAt,
      warnings: preview.warnings || [],
      totalScanned: (preview.rows || []).length,
      toDelete: rows.length,
      sample: rows.slice(0, 20).map((row) => ({
        offerId: row.offerId,
        name: row.name,
        minVolumeMl: row.minVolumeMl,
        shopId: row.shopId,
      })),
    };
  }
  const deleteLimit = Math.max(1, Number(process.env.YANDEX_CLEANUP_DELETE_LIMIT || 10000) || 10000);
  const limitedRows = rows.slice(0, deleteLimit);
  const results = await deleteYandexCleanupRows(limitedRows);
  const deleted = results.filter((item) => item.ok).length;
  const failed = results.filter((item) => !item.ok).length;
  logger.info("yandex small volume cleanup finished", {
    planned: rows.length,
    deleted,
    failed,
    deleteLimit,
  });
  return {
    ok: failed === 0,
    dryRun: false,
    totalScanned: (preview.rows || []).length,
    planned: rows.length,
    deleted,
    failed,
    skippedByLimit: Math.max(0, rows.length - limitedRows.length),
    warnings: preview.warnings || [],
    failedSample: results.filter((item) => !item.ok).slice(0, 20),
    sample: limitedRows.slice(0, 20).map((row) => ({
      offerId: row.offerId,
      name: row.name,
      minVolumeMl: row.minVolumeMl,
    })),
  };
}

function materializeYandexExportedProductsForWarehouse(warehouse = {}) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { warehouse: { ...warehouse, products }, added: 0 };

  const existingKeys = new Set(products.map((product) => warehouseProductExactMergeKey(normalizeWarehouseProduct(product))));
  const additions = [];

  for (const product of products) {
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== "ozon") continue;
    if (!normalized.offerId) continue;
    if (isYandexSmallVolumeBlocked(normalized)) continue;
    const exports = normalized.exports || {};
    const yandexSent = exports.yandex?.status === "sent";
    const matchingShops = shops.filter((shop) => exports[shop.id]?.status === "sent");
    const targetShops = matchingShops.length ? matchingShops : (yandexSent ? shops.slice(0, 1) : []);
    for (const shop of targetShops) {
      const exactKey = warehouseProductExactMergeKey({ target: shop.id, marketplace: "yandex", offerId: normalized.offerId });
      if (existingKeys.has(exactKey)) continue;
      const exportState = exports[shop.id] || exports.yandex || { status: "sent", targetName: shop.name || "Yandex Market" };
      const yandexProduct = buildYandexWarehouseProductFromOzonExport(normalized, shop, exportState);
      additions.push(yandexProduct);
      existingKeys.add(exactKey);
    }
  }

  if (!additions.length) return { warehouse: { ...warehouse, products }, added: 0 };
  return {
    warehouse: { ...warehouse, products: mergeProducts(products, additions) },
    added: additions.length,
  };
}

function ozonProductHasYandexExport(product = {}, shop = {}) {
  const exports = product.exports && typeof product.exports === "object" ? product.exports : {};
  if (exports.yandex?.status === "sent") return true;
  const shopId = cleanText(shop.id);
  if (shopId && exports[shopId]?.status === "sent") return true;
  return Object.values(exports).some((entry) => entry?.status === "sent"
    && /yandex/i.test(cleanText(entry.targetName || entry.marketplace || entry.target || shopId)));
}

function buildOzonYandexAutoPairGroupId(ozonProduct = {}) {
  const ozonId = cleanText(ozonProduct.id);
  return ozonId ? `auto-pair-${ozonId}` : "";
}

function extractOzonIdFromAutoPairGroupId(groupId = "") {
  const normalized = cleanText(groupId);
  if (!normalized.startsWith("auto-pair-")) return "";
  return cleanText(normalized.slice("auto-pair-".length));
}

function extractYandexSourceProductId(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const raw = normalized.raw && typeof normalized.raw === "object" && !Array.isArray(normalized.raw) ? normalized.raw : {};
  const yandex = normalized.yandex && typeof normalized.yandex === "object" ? normalized.yandex : {};
  const rawYandex = raw.yandex && typeof raw.yandex === "object" ? raw.yandex : {};
  return cleanText(
    yandex.extra?.sourceProductId
    || rawYandex.extra?.sourceProductId
    || raw.yandex?.extra?.sourceProductId,
  );
}

function resolveWarehouseProductPairOzonId(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const raw = normalized.raw && typeof normalized.raw === "object" && !Array.isArray(normalized.raw) ? normalized.raw : {};
  const manualGroupId = cleanText(normalized.manualGroupId || raw.manualGroupId || raw.manual_group_id).toLowerCase();
  if (manualGroupId.startsWith("auto-pair-")) {
    return extractOzonIdFromAutoPairGroupId(manualGroupId);
  }
  if (cleanText(normalized.marketplace).toLowerCase() === "yandex") {
    return extractYandexSourceProductId(normalized);
  }
  return "";
}

function buildWarehouseCatalogGroupContext(products = []) {
  const ozonIdsReferencedByYandex = new Set();
  const offerIdMarketplaces = new Map();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const marketplace = cleanText(normalized.marketplace).toLowerCase();
    const offerId = cleanText(normalized.offerId || normalized.offer_id).toLowerCase();
    if (offerId) {
      if (!offerIdMarketplaces.has(offerId)) offerIdMarketplaces.set(offerId, new Set());
      if (marketplace) offerIdMarketplaces.get(offerId).add(marketplace);
    }
    if (marketplace !== "yandex") continue;
    const sourceId = extractYandexSourceProductId(normalized);
    if (sourceId) ozonIdsReferencedByYandex.add(sourceId.toLowerCase());
  }
  const pairedOfferIds = new Set();
  for (const [offerId, marketplaces] of offerIdMarketplaces) {
    if (marketplaces.has("ozon") && marketplaces.has("yandex")) pairedOfferIds.add(offerId);
  }
  return { ozonIdsReferencedByYandex, pairedOfferIds };
}

function warehouseProductSharesGroup(product = {}, groupContext = null, groupKeys = null, pairOzonIds = null) {
  const normalized = normalizeWarehouseProduct(product);
  const groupKey = warehouseProductPageGroupKey(normalized, groupContext);
  if (groupKeys && groupKey && groupKeys.has(groupKey)) return true;
  const pairId = resolveWarehouseProductPairOzonId(normalized);
  if (pairId && pairOzonIds?.has(pairId.toLowerCase())) return true;
  const marketplace = cleanText(normalized.marketplace).toLowerCase();
  const productId = cleanText(normalized.id).toLowerCase();
  if (marketplace === "ozon" && productId && pairOzonIds?.has(productId)) return true;
  if (marketplace === "yandex") {
    const sourceId = extractYandexSourceProductId(normalized);
    if (sourceId && pairOzonIds?.has(sourceId.toLowerCase())) return true;
  }
  return false;
}

function collectWarehouseGroupExpansionKeys(seedProducts = [], groupContext = null) {
  const groupKeys = new Set();
  const pairOzonIds = new Set();
  for (const seed of Array.isArray(seedProducts) ? seedProducts : []) {
    const normalized = normalizeWarehouseProduct(seed);
    const groupKey = warehouseProductPageGroupKey(normalized, groupContext);
    if (groupKey) groupKeys.add(groupKey);
    const pairId = resolveWarehouseProductPairOzonId(normalized);
    if (pairId) {
      const normalizedPairId = pairId.toLowerCase();
      pairOzonIds.add(normalizedPairId);
      groupKeys.add(`pair:${normalizedPairId}`);
    }
    if (cleanText(normalized.marketplace).toLowerCase() === "ozon" && normalized.id) {
      const ozonId = cleanText(normalized.id).toLowerCase();
      pairOzonIds.add(ozonId);
      groupKeys.add(`pair:${ozonId}`);
    }
  }
  return { groupKeys, pairOzonIds };
}

function buildOzonProductLookupIndexes(ozonProducts = []) {
  const byId = new Map();
  const byOffer = new Map();
  const byProductId = new Map();
  for (const product of Array.isArray(ozonProducts) ? ozonProducts : []) {
    const normalized = normalizeWarehouseProduct(product);
    if (!normalized.id) continue;
    byId.set(String(normalized.id), normalized);
    const offerId = cleanText(normalized.offerId).toLowerCase();
    if (offerId && !byOffer.has(offerId)) byOffer.set(offerId, normalized);
    const productId = cleanText(normalized.productId);
    if (productId && !byProductId.has(productId)) byProductId.set(productId, normalized);
  }
  return { byId, byOffer, byProductId };
}

function findOzonMatchForYandexProduct(yandexProduct = {}, indexes = {}) {
  const yandex = normalizeWarehouseProduct(yandexProduct);
  const sourceId = extractYandexSourceProductId(yandex);
  if (sourceId && indexes.byId?.get(sourceId)) return indexes.byId.get(sourceId);
  const offerId = cleanText(yandex.offerId).toLowerCase();
  if (offerId && indexes.byOffer?.get(offerId)) return indexes.byOffer.get(offerId);
  const productId = cleanText(yandex.productId);
  if (productId && indexes.byProductId?.get(productId)) return indexes.byProductId.get(productId);
  return null;
}

function warehouseProductsShareAutoPairGroup(left = {}, right = {}) {
  const leftGroup = cleanText(left.manualGroupId || left.raw?.manualGroupId).toLowerCase();
  const rightGroup = cleanText(right.manualGroupId || right.raw?.manualGroupId).toLowerCase();
  if (leftGroup && rightGroup && leftGroup === rightGroup) return true;
  const leftOzon = cleanText(left.marketplace) === "ozon" ? left : right;
  const rightYandex = cleanText(right.marketplace) === "yandex" ? right : left;
  if (cleanText(leftOzon.marketplace) !== "ozon" || cleanText(rightYandex.marketplace) !== "yandex") return false;
  return extractYandexSourceProductId(rightYandex) === cleanText(leftOzon.id);
}

function ozonProductShouldMaterializeYandexSibling(product = {}, shop = {}, options = {}) {
  if (isYandexSmallVolumeBlocked(product)) return false;
  const offerId = cleanText(product.offerId).toLowerCase();
  if (!offerId) return false;
  const yandexOfferIds = options.yandexOfferIds instanceof Set ? options.yandexOfferIds : new Set();
  if (yandexOfferIds.has(offerId)) return false;
  if (ozonProductHasYandexExport(product, shop)) return true;
  const yandexCache = options.yandexCacheOfferIds instanceof Set ? options.yandexCacheOfferIds : new Set();
  if (yandexCache.has(offerId)) return true;
  return Array.isArray(product.links) && product.links.length > 0;
}

function rememberOzonYandexPair(pairs, seen, ozon = {}, yandex = {}) {
  const left = normalizeWarehouseProduct(ozon);
  const right = normalizeWarehouseProduct(yandex);
  if (!left.id || !right.id) return false;
  const key = [String(left.id), String(right.id)].sort().join("|");
  if (seen.has(key)) return false;
  seen.add(key);
  pairs.push([left, right]);
  return true;
}

function collectOzonYandexPairsFromProducts(ozonProducts = [], yandexProducts = [], options = {}) {
  const yandexByOffer = options.yandexByOffer instanceof Map ? options.yandexByOffer : new Map();
  const ozonIndexes = options.ozonIndexes || buildOzonProductLookupIndexes(ozonProducts);
  const pairs = [];
  const seen = new Set();
  for (const ozon of ozonProducts) {
    const offerId = cleanText(ozon.offerId).toLowerCase();
    if (offerId && yandexByOffer.has(offerId)) {
      rememberOzonYandexPair(pairs, seen, ozon, yandexByOffer.get(offerId));
    }
  }
  for (const yandex of yandexProducts) {
    const ozon = findOzonMatchForYandexProduct(yandex, ozonIndexes);
    if (ozon) rememberOzonYandexPair(pairs, seen, ozon, yandex);
  }
  return pairs;
}

