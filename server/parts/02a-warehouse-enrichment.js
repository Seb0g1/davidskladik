function isWeakProductName(name, offerId) {
  const current = cleanText(name);
  const article = cleanText(offerId);
  if (!current) return true;
  if (/^товар\s+ozon$/i.test(current)) return true;
  if (/^ozon\s+\d+$/i.test(current)) return true;
  if (article && current.toLowerCase() === article.toLowerCase()) return true;
  return /^[A-ZА-Я0-9._-]{4,}$/i.test(current) && !/\s/.test(current);
}

function applyOzonInfoToWarehouseProduct(product, info = {}, account = {}, stockInfo = {}, priceInfo = {}) {
  const sourceSku = info.sources?.find((source) => source.sku)?.sku;
  const sku = product.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku;
  const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image);
  const productUrl = info.product_url || info.url || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : product.productUrl);
  const betterName = cleanText(info.name);
  const nextName = betterName && !isWeakProductName(betterName, product.offerId)
    ? betterName
    : (betterName && isWeakProductName(product.name, product.offerId) ? betterName : product.name || betterName);
  const priceDetails = normalizeOzonPriceDetails(priceInfo);
  const cabinetPrice =
    pickOzonCabinetListedPrice(priceDetails) || parseMoneyValue(info.price) || product.marketplacePrice || null;
  const hasStockInfo = Boolean(stockInfo && Object.keys(stockInfo).length);
  const marketplaceState = hasStockInfo || info.visibility || info.status || info.state
    || typeof info.is_archived === "boolean" || typeof info.is_autoarchived === "boolean"
    ? pickOzonState(product, info, stockInfo)
    : product.marketplaceState;
  return normalizeWarehouseProduct({
    ...product,
    target: product.target || account.id,
    marketplace: "ozon",
    targetName: account.name || product.targetName || "Ozon",
    name: nextName,
    productId: product.productId || info.product_id || info.id,
    sku: sku || product.sku,
    productUrl,
    imageUrl: primaryImage || product.imageUrl,
    marketplacePrice: cabinetPrice,
    marketplaceMinPrice: priceDetails.minPrice || product.marketplaceMinPrice || null,
    marketplaceState,
    ozon: {
      ...(product.ozon || {}),
      offerId: product.offerId,
      vendor: cleanText(info.brand || info.vendor || (product.ozon || {}).vendor || ""),
      name: betterName || product.ozon?.name || nextName,
      description: info.description || product.ozon?.description || "",
      categoryId: info.description_category_id || info.category_id || product.ozon?.categoryId,
      typeId: info.type_id || info.description_type_id || product.ozon?.typeId,
      price: cabinetPrice || undefined,
      minPrice: priceDetails.minPrice || product.ozon?.minPrice || undefined,
      oldPrice: priceDetails.oldPrice || parseMoneyValue(info.old_price) || product.ozon?.oldPrice || undefined,
      marketingSellerPrice: priceDetails.marketingSellerPrice || product.ozon?.marketingSellerPrice || undefined,
      marketingPrice: priceDetails.marketingPrice || product.ozon?.marketingPrice || undefined,
      retailPrice: priceDetails.retailPrice || product.ozon?.retailPrice || undefined,
      barcode: (info.barcodes || [])[0] || product.ozon?.barcode || "",
      barcodes: info.barcodes || product.ozon?.barcodes || [],
      primaryImage: primaryImage || product.ozon?.primaryImage || "",
      images: info.images || product.ozon?.images || [],
      images360: info.images360 || product.ozon?.images360 || [],
      colorImage: firstImageUrl(info.color_image) || product.ozon?.colorImage || "",
      depth: Number(info.depth || product.ozon?.depth || 0) || undefined,
      width: Number(info.width || product.ozon?.width || 0) || undefined,
      height: Number(info.height || product.ozon?.height || 0) || undefined,
      weight: Number(info.weight || product.ozon?.weight || 0) || undefined,
      dimensionUnit: cleanText(info.dimension_unit || info.dimensionUnit || product.ozon?.dimensionUnit || "mm"),
      weightUnit: cleanText(info.weight_unit || info.weightUnit || product.ozon?.weightUnit || "g"),
    },
  });
}

function ozonDonorNeedsApiRefresh(donor = {}) {
  const normalized = normalizeWarehouseProduct(donor);
  const pictures = [
    cleanText(normalized.imageUrl),
    cleanText(normalized.ozon?.primaryImage),
    ...splitList(normalized.ozon?.images),
  ].filter(Boolean);
  return !pictures.length
    || isWeakProductName(normalized.name, normalized.offerId)
    || !(Number(normalized.ozon?.depth) > 0 && Number(normalized.ozon?.weight) > 0);
}

async function refreshOzonWarehouseDonorFromApi(donor = {}) {
  const normalized = normalizeWarehouseProduct(donor);
  const account = getOzonAccountByTarget(normalized.target);
  if (!account || !normalized.offerId) return normalized;
  const infoMap = await getOzonProductInfoMap([normalized.offerId], account, { continueOnError: true });
  const info = infoMap.get(normalized.offerId) || {};
  if (!Object.keys(info).length) return normalized;
  return applyOzonInfoToWarehouseProduct(normalized, info, account, {}, {});
}

async function enrichWarehouseProducts(productIds = []) {
  const ids = new Set((Array.isArray(productIds) ? productIds : []).map(String));
  if (!ids.size) return [];

  const warehouse = await readWarehouse();
  const updated = [];

  for (const account of getOzonAccounts()) {
    const products = warehouse.products.filter(
      (product) => ids.has(product.id) && product.marketplace === "ozon" && matchesOzonTarget(product.target, account.id) && product.offerId,
    );
    if (!products.length) continue;

    const offerIds = products.map((product) => product.offerId);
    const [infoMap, stockMap, priceMap] = await Promise.all([
      getOzonProductInfoMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon info skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
      getOzonStockMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon stock skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
      getOzonPriceMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon price skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
    ]);
    for (const product of products) {
      const info = infoMap.get(product.offerId) || {};
      const stockInfo = stockMap.get(product.offerId) || {};
      const priceInfo = priceMap.get(product.offerId) || {};
      if (!Object.keys(info).length && !Object.keys(stockInfo).length && !Object.keys(priceInfo).length) continue;
      const index = warehouse.products.findIndex((item) => item.id === product.id);
      if (index < 0) continue;
      warehouse.products[index] = applyOzonInfoToWarehouseProduct(
        warehouse.products[index],
        info,
        account,
        stockInfo,
        priceInfo,
      );
      updated.push(warehouse.products[index]);
    }
  }

  if (updated.length) {
    await writeWarehouseProductPatch(updated, { reason: "warehouse_ozon_enrich", writeLinks: false });
  }
  return updated;
}

async function enrichWeakOzonProductsForPage(products = []) {
  if (process.env.WAREHOUSE_PAGE_AUTO_ENRICH_ENABLED !== "true") return products;
  const limit = Math.max(0, Number(process.env.WAREHOUSE_PAGE_AUTO_ENRICH_LIMIT || 60) || 60);
  if (!limit) return products;
  const candidates = (products || [])
    .filter((product) => product?.marketplace === "ozon" && product.offerId && ozonProductNeedsDetailRefresh(product))
    .slice(0, limit);
  if (!candidates.length) return products;

  const updatedById = new Map();
  for (const account of getOzonAccounts()) {
    const accountProducts = candidates.filter((product) => matchesOzonTarget(product.target, account.id));
    if (!accountProducts.length) continue;
    const offerIds = accountProducts.map((product) => product.offerId);
    try {
      const [infoMap, stockMap, priceMap] = await Promise.all([
        getOzonProductInfoMap(offerIds, account, { continueOnError: true }),
        getOzonStockMap(offerIds, account, { continueOnError: true }),
        getOzonPriceMap(offerIds, account, { continueOnError: true }),
      ]);
      const missingProductIds = accountProducts
        .filter((product) => !getOzonOfferMapValue(infoMap, product.offerId))
        .map((product) => cleanText(product.productId || product.ozon?.productId))
        .filter(Boolean);
      if (missingProductIds.length) {
        const infoByProductId = await getOzonProductInfoMapByProductIds(missingProductIds, account, { continueOnError: true });
        for (const product of accountProducts) {
          const info = infoByProductId.get(cleanText(product.productId || product.ozon?.productId));
          if (info) setOzonOfferMapValue(infoMap, product.offerId || info.offer_id || info.offerId, info);
        }
      }
      for (const product of accountProducts) {
        const info = getOzonOfferMapValue(infoMap, product.offerId) || {};
        const stockInfo = getOzonOfferMapValue(stockMap, product.offerId) || {};
        const priceInfo = getOzonOfferMapValue(priceMap, product.offerId) || {};
        if (!Object.keys(info).length && !Object.keys(stockInfo).length && !Object.keys(priceInfo).length) continue;
        updatedById.set(product.id, applyOzonInfoToWarehouseProduct(product, info, account, stockInfo, priceInfo));
      }
    } catch (error) {
      logger.warn("warehouse page Ozon detail auto-enrich failed", { account: account.id, detail: error?.message || String(error) });
    }
  }

  if (!updatedById.size) return products;
  try {
    await writeWarehouseProductPatch(Array.from(updatedById.values()), { reason: "warehouse_page_ozon_auto_enrich", writeLinks: false });
  } catch (error) {
    logger.warn("warehouse page Ozon detail auto-enrich persist failed", { detail: error?.message || String(error) });
  }
  return products.map((product) => updatedById.get(product.id) || product);
}

function stoppedSupplierMap(suppliers = []) {
  const map = new Map();
  for (const supplier of suppliers || []) {
    if (!supplier.stopped || !supplier.name) continue;
    const key = normalizeSupplierName(supplier.name);
    map.set(key, supplier);
    // Also index by legal-form-stripped key so "Авангард" matches "ООО Авангард" and vice versa.
    const stripped = stripSupplierLegalFormPrefix(key);
    if (stripped !== key) map.set(stripped, supplier);
  }
  return map;
}

function managedSupplierMaps(suppliers = []) {
  const byName = new Map();
  const byPartnerId = new Map();
  for (const supplier of suppliers || []) {
    const normalized = normalizeManagedSupplier(supplier);
    if (normalized.partnerId) byPartnerId.set(String(normalized.partnerId), normalized);
    if (normalized.name) {
      const key = normalizeSupplierName(normalized.name);
      byName.set(key, normalized);
      // Also index by stripped key for legal-form-tolerant lookup.
      const stripped = stripSupplierLegalFormPrefix(key);
      if (stripped !== key) byName.set(stripped, normalized);
    }
  }
  return { byName, byPartnerId };
}

function findManagedSupplierForPriceMasterRow(row = {}, maps = managedSupplierMaps(), link = {}) {
  const byId = maps.byPartnerId.get(String(row.partnerId || link.partnerId || ""));
  if (byId) return byId;
  const partnerName = row.partnerName || link.supplierName || "";
  const key = normalizeSupplierName(partnerName);
  return maps.byName.get(key)
    || maps.byName.get(stripSupplierLegalFormPrefix(key))
    || null;
}

function priceMasterSupplierPricingMeta(row = {}, maps = managedSupplierMaps()) {
  const supplier = findManagedSupplierForPriceMasterRow(row, maps);
  const stockOnly = supplierUsesStockOnlyPricing(supplier, row);
  const pricingMode = stockOnly ? "stock_only" : normalizeSupplierPricingMode(supplier || {});
  return {
    managedSupplier: supplier || null,
    pricingMode,
    stockOnly,
    priceEligible: !stockOnly,
    stockEligible: true,
    trustFactor: normalizeSupplierTrustFactor(supplier?.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(supplier?.orderCutoffTime || ""),
    reseller: Boolean(supplier?.reseller),
  };
}

function supplierOrderCutoffPassed(orderCutoffTime = "", now = new Date()) {
  const cutoff = normalizeSupplierOrderCutoff(orderCutoffTime);
  if (!cutoff) return false;
  const [hour, minute] = cutoff.split(":").map((item) => Number(item) || 0);
  const moscow = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Moscow",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const currentHour = Number(moscow.find((part) => part.type === "hour")?.value || 0);
  const currentMinute = Number(moscow.find((part) => part.type === "minute")?.value || 0);
  return currentHour * 60 + currentMinute > hour * 60 + minute;
}

function supplierCartOrderScore(row = {}, usdRate = 95, now = new Date()) {
  const rawPrice = Number(row.price || Number.POSITIVE_INFINITY);
  const isRub = cleanText(row.priceCurrency || row.currency || "USD").toUpperCase() === "RUB";
  // Normalize to USD for uniform comparison regardless of supplier currency
  const price = isRub ? rawPrice / usdRate : rawPrice;
  const trust = normalizeSupplierTrustFactor(row.trustFactor, 100);
  const trustPenalty = (100 - trust) / 100 * 0.18;
  const resellerPenalty = row.reseller ? 0.12 : 0;
  const cutoffPenalty = supplierOrderCutoffPassed(row.orderCutoffTime, now) ? 1000 : 0;
  const result = price * (1 + trustPenalty + resellerPenalty) + cutoffPenalty;
  // Infinity (price=0 rows) serializes to null in JSON — clamp to a large finite sentinel.
  return Number.isFinite(result) ? result : 999999;
}

function resolvePriceMasterRowCurrency(row = {}, link = {}, maps = managedSupplierMaps()) {
  const supplier = findManagedSupplierForPriceMasterRow(row, maps, link);
  return managedSupplierPriceCurrency(supplier, row, link);
}

