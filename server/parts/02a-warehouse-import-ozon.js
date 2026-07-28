// Single source of truth for turning an Ozon /v3/product/list item (+ detail maps) into
// a normalized warehouse product. Used by the full import below and by the new-offer
// discovery job (02f-ozon-new-offer-discovery.js).
function buildOzonImportedWarehouseProduct(account, product, { info = {}, stockInfo = {}, priceInfo = {} } = {}) {
  const hasInfo = Boolean(Object.keys(info).length);
  const hasStock = Boolean(Object.keys(stockInfo).length);
  const hasPrice = Boolean(Object.keys(priceInfo).length);
  const priceDetails = normalizeOzonPriceDetails(priceInfo);
  const cabinetPrice =
    pickOzonCabinetListedPrice(priceDetails) || parseMoneyValue(info.price) || parseMoneyValue(product.price) || null;
  const sourceSku = info.sources?.find((source) => source.sku)?.sku;
  const sku = product.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku;
  const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image);
  const productUrl = info.product_url || info.url || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : "");
  const marketplaceState = pickOzonState(product, info, stockInfo);
  if (!hasInfo && !hasStock) marketplaceState.partial = true;
  return normalizeWarehouseProduct({
    id: ozonWarehouseProductId(account, product.offer_id),
    target: account.id,
    marketplace: "ozon",
    targetName: account.name || "Ozon",
    offerId: product.offer_id,
    productId: product.product_id || info.product_id || info.id,
    sku,
    productUrl,
    imageUrl: primaryImage,
    marketplacePrice: cabinetPrice,
    marketplaceMinPrice: priceDetails.minPrice || null,
    name: info.name || product.name || product.offer_id || `Ozon ${product.product_id}`,
    marketplaceState,
    ozon: {
      offerId: product.offer_id,
      vendor: cleanText(info.brand || info.vendor || ""),
      name: info.name || product.name || product.offer_id,
      description: info.description || "",
      categoryId: info.description_category_id || info.category_id,
      typeId: info.type_id || info.description_type_id,
      price: hasPrice ? cabinetPrice || undefined : undefined,
      minPrice: hasPrice ? priceDetails.minPrice || undefined : undefined,
      oldPrice: hasPrice ? priceDetails.oldPrice || parseMoneyValue(info.old_price) || undefined : undefined,
      marketingSellerPrice: hasPrice ? priceDetails.marketingSellerPrice || undefined : undefined,
      marketingPrice: hasPrice ? priceDetails.marketingPrice || undefined : undefined,
      retailPrice: hasPrice ? priceDetails.retailPrice || undefined : undefined,
      barcode: (info.barcodes || [])[0] || "",
      barcodes: info.barcodes || [],
      primaryImage,
      images: info.images || [],
      images360: info.images360 || [],
      colorImage: firstImageUrl(info.color_image),
    },
    createdAt: new Date().toISOString(),
  });
}

async function importOzonWarehouseProducts(limit = Number.POSITIVE_INFINITY, existingProducts = [], options = {}) {
  const accounts = getOzonAccounts().filter((account) => account.clientId && account.apiKey && account.importEnabled !== false);
  const imported = [];
  const warnings = [];
  if (!accounts.length) return { imported, warnings };

  const perAccountLimit = Number.isFinite(Number(limit)) && Number(limit) > 0
    ? Math.max(1, Math.ceil(Number(limit) / accounts.length))
    : Number.POSITIVE_INFINITY;

  for (const account of accounts) {
    try {
      const products = await getOzonProducts(perAccountLimit, account);
      const existingByOffer = ozonExistingProductMap(existingProducts, account);
      let infoMap = new Map();
      let stockMap = new Map();
      let priceMap = new Map();
      try {
        const configuredDetailLimit = process.env.OZON_SYNC_DETAIL_LIMIT !== undefined
          ? Number(process.env.OZON_SYNC_DETAIL_LIMIT)
          : Number(process.env.OZON_SYNC_INFO_LIMIT || 800);
        const infoLimit = Math.max(0, Number.isFinite(configuredDetailLimit) ? configuredDetailLimit : 800);
        const infoOfferIds = pickOzonDetailOfferIds(products, existingByOffer, infoLimit);
        logger.info("ozon product list loaded", {
          account: account.id,
          listed: products.length,
          detailRefresh: infoOfferIds.length,
          detailLimit: infoLimit,
        });
        options.onProgress?.({
          percent: 32,
          stage: "Ozon список",
          meta: `Ozon вернул ${formatRuNumber(products.length)} карточек. Детально обновить: ${formatRuNumber(infoOfferIds.length)}.`,
          processed: products.length,
          total: products.length,
        });
        if (infoOfferIds.length) {
          // Independent detail maps load in parallel; the request pool paces the API calls.
          [infoMap, stockMap, priceMap] = await Promise.all([
            getOzonProductInfoMap(infoOfferIds, account, {
              continueOnError: true,
              onProgress: (progress) => options.onProgress?.({
                percent: 32 + Math.round((progress.processed / Math.max(1, progress.total)) * 38),
                stage: progress.stage,
                meta: `Загружаю детали Ozon: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
                processed: progress.processed,
                total: progress.total,
              }),
            }),
            getOzonStockMap(infoOfferIds, account, { continueOnError: true }),
            getOzonPriceMap(infoOfferIds, account, { continueOnError: true }),
          ]);
          const detailOfferSet = new Set(infoOfferIds.map(ozonOfferMapKey));
          const missingProductIds = products
            .filter((product) => detailOfferSet.has(ozonOfferMapKey(product.offer_id || product.offerId)))
            .filter((product) => !getOzonOfferMapValue(infoMap, product.offer_id || product.offerId))
            .map((product) => cleanText(product.product_id || product.productId))
            .filter(Boolean);
          if (missingProductIds.length) {
            const infoByProductId = await getOzonProductInfoMapByProductIds(missingProductIds, account, {
              continueOnError: true,
              onProgress: (progress) => options.onProgress?.({
                percent: 70 + Math.round((progress.processed / Math.max(1, progress.total)) * 4),
                stage: progress.stage,
                meta: `Добираю детали Ozon по product_id: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
                processed: progress.processed,
                total: progress.total,
              }),
            });
            for (const product of products) {
              const productId = cleanText(product.product_id || product.productId);
              const info = infoByProductId.get(productId);
              if (info) setOzonOfferMapValue(infoMap, product.offer_id || product.offerId || info.offer_id || info.offerId, info);
            }
            logger.info("ozon product details recovered by product id", {
              account: account.id,
              requested: missingProductIds.length,
              recovered: infoByProductId.size,
            });
          }
          logger.info("ozon product detail maps loaded", {
            account: account.id,
            requested: infoOfferIds.length,
            info: infoMap.size,
            stock: stockMap.size,
            price: priceMap.size,
          });
        }
      } catch (error) {
        infoMap = new Map();
        stockMap = new Map();
        priceMap = new Map();
        const label = error?.message || error?.code || "ошибка API";
        warnings.push(`Ozon «${account.name || account.id}»: не загружены детали/цены (${label})`);
        logger.warn("ozon info/price batch failed", { account: account.id, detail: label });
      }

      imported.push(...products.map((product) => buildOzonImportedWarehouseProduct(account, product, {
        info: getOzonOfferMapValue(infoMap, product.offer_id) || {},
        stockInfo: getOzonOfferMapValue(stockMap, product.offer_id) || {},
        priceInfo: getOzonOfferMapValue(priceMap, product.offer_id) || {},
      })));
    } catch (error) {
      const label = error?.message || error?.code || "ошибка";
      warnings.push(`Ozon «${account.name || account.id}»: ${label}`);
      logger.warn("ozon account import failed", { account: account.id, detail: label });
    }
  }

  return { imported, warnings };
}
