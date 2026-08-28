function warehouseProductExactMergeKey(product = {}) {
  return [
    cleanText(product.target || product.marketplace || "default").toLowerCase(),
    cleanText(product.offerId || product.offer_id || "").toLowerCase(),
  ].join(":");
}

function warehouseProductLooseMergeKeys(product = {}) {
  const marketplace = cleanText(product.marketplace || "ozon").toLowerCase();
  const offerId = cleanText(product.offerId || product.offer_id || "").toLowerCase();
  const productId = cleanText(product.productId || product.product_id || "").toLowerCase();
  const sku = cleanText(product.sku || "").toLowerCase();
  return [
    productId ? `${marketplace}:product:${productId}` : "",
    sku ? `${marketplace}:sku:${sku}` : "",
    offerId ? `${marketplace}:offer:${offerId}` : "",
  ].filter(Boolean);
}

function mergeOzonDraftForWarehouseProduct(currentProduct = {}, importedProduct = {}, preserveRichFields = false) {
  const current = currentProduct?.ozon || {};
  const imported = importedProduct?.ozon || {};
  const merged = { ...current, ...imported };
  if (!preserveRichFields) return merged;

  const offerId = cleanText(importedProduct.offerId || currentProduct.offerId || imported.offerId || current.offerId);
  const importedNameWeak = isWeakProductName(imported.name || importedProduct.name, offerId);
  if (importedNameWeak && cleanText(current.name)) merged.name = current.name;

  for (const field of [
    "vendor",
    "description",
    "categoryId",
    "typeId",
    "barcode",
    "primaryImage",
    "colorImage",
  ]) {
    if (!cleanText(imported[field]) && cleanText(current[field])) merged[field] = current[field];
  }

  for (const field of ["barcodes", "images", "images360", "attributes", "complexAttributes"]) {
    const importedValue = Array.isArray(imported[field]) ? imported[field] : [];
    const currentValue = Array.isArray(current[field]) ? current[field] : [];
    if (!importedValue.length && currentValue.length) merged[field] = currentValue;
  }

  if (current.extra && typeof current.extra === "object" && (!imported.extra || !Object.keys(imported.extra).length)) {
    merged.extra = current.extra;
  }
  return merged;
}

function mergeYandexDraftForWarehouseProduct(currentProduct = {}, importedProduct = {}) {
  const current = currentProduct?.yandex || {};
  const imported = importedProduct?.yandex || {};
  const merged = { ...current, ...imported };
  const currentHasLivePrice = Boolean(currentProduct?.lastYandexPriceSend?.status || currentProduct?.lastYandexPriceSend?.at);
  if ((Number(imported.price || 0) <= 0 && Number(current.price || 0) > 0) || (currentHasLivePrice && Number(current.price || 0) > 0)) {
    merged.price = current.price;
  }
  if (!cleanText(imported.name) && cleanText(current.name)) merged.name = current.name;
  if (!cleanText(imported.description) && cleanText(current.description)) merged.description = current.description;
  if (!cleanText(imported.vendor) && cleanText(current.vendor)) merged.vendor = current.vendor;
  const importedPictures = Array.isArray(imported.pictures) ? imported.pictures : [];
  const currentPictures = Array.isArray(current.pictures) ? current.pictures : [];
  if (!importedPictures.length && currentPictures.length) merged.pictures = currentPictures;
  const importedBarcodes = Array.isArray(imported.barcodes) ? imported.barcodes : [];
  const currentBarcodes = Array.isArray(current.barcodes) ? current.barcodes : [];
  if (!importedBarcodes.length && currentBarcodes.length) merged.barcodes = currentBarcodes;
  if (current.extra && typeof current.extra === "object") {
    merged.extra = {
      ...current.extra,
      ...(imported.extra && typeof imported.extra === "object" ? imported.extra : {}),
    };
  }
  return merged;
}

function mergeProducts(existingProducts, importedProducts) {
  const map = new Map();
  const looseIndex = new Map();
  const rememberLooseKeys = (product, exactKey) => {
    for (const key of warehouseProductLooseMergeKeys(product)) {
      if (!looseIndex.has(key)) looseIndex.set(key, new Set());
      looseIndex.get(key).add(exactKey);
    }
  };

  for (const product of existingProducts) {
    const normalized = normalizeWarehouseProduct(product);
    const exactKey = warehouseProductExactMergeKey(normalized);
    map.set(exactKey, normalized);
    rememberLooseKeys(normalized, exactKey);
  }

  for (const imported of importedProducts) {
    if (!imported.offerId) continue;
    const importedNormalized = normalizeWarehouseProduct(imported);
    const exactKey = warehouseProductExactMergeKey(importedNormalized);
    let matchedKey = map.has(exactKey) ? exactKey : "";
    if (!matchedKey) {
      for (const looseKey of warehouseProductLooseMergeKeys(importedNormalized)) {
        const candidates = Array.from(looseIndex.get(looseKey) || []);
        if (candidates.length === 1) {
          matchedKey = candidates[0];
          break;
        }
      }
    }
    const current = matchedKey ? map.get(matchedKey) : null;
    if (matchedKey && matchedKey !== exactKey) map.delete(matchedKey);
    const currentState = current?.marketplaceState || {};
    const importedState = importedNormalized.marketplaceState || {};
    const preserveCurrentState = Boolean(
      currentState.code
        && currentState.code !== "unknown"
        && (importedState.partial || importedState.code === "unknown"),
    );
    const offerId = cleanText(importedNormalized.offerId || current?.offerId);
    const importedNameWeak = importedNormalized.marketplace === "ozon" && isWeakProductName(importedNormalized.name, offerId);
    const currentNameWeak = current?.marketplace === "ozon" && isWeakProductName(current?.name, offerId);
    const preserveCurrentRichOzonFields = Boolean(
      current
        && importedNormalized.marketplace === "ozon"
        && (importedState.partial || importedNameWeak || !importedNormalized.imageUrl),
    );
    const preserveCurrentName = Boolean(current?.name && !currentNameWeak && importedNameWeak);
    const preserveCurrentImage = Boolean(current?.imageUrl && !importedNormalized.imageUrl);
    const preserveCurrentProductUrl = Boolean(current?.productUrl && !importedNormalized.productUrl);
    const preserveCurrentSku = Boolean(current?.sku && !importedNormalized.sku);
    const currentHasLiveYandexPrice = Boolean(
      current?.marketplace === "yandex"
        && importedNormalized.marketplace === "yandex"
        && (current?.lastYandexPriceSend?.status || current?.lastYandexPriceSend?.at),
    );
    const preserveCurrentYandexMarkup = Boolean(
      current?.marketplace === "yandex"
        && Number(current.markup || 0) > 0
        && (
          current?.markupSource === "manual"
          || current?.yandex?.extra?.manualMarkup === true
          || currentHasLiveYandexPrice
        ),
    );
    const preserveCurrentPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.marketplacePrice && !importedNormalized.marketplacePrice),
    );
    const preserveCurrentCurrentPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.currentPrice && !importedNormalized.currentPrice),
    );
    const preserveCurrentTargetPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.targetPrice && !importedNormalized.targetPrice),
    );
    const preserveCurrentTargetStock = Boolean(
      current?.targetStock !== null
        && current?.targetStock !== undefined
        && (importedNormalized.targetStock === null || importedNormalized.targetStock === undefined),
    );
    const preserveCurrentMinPrice = Boolean(current?.marketplaceMinPrice && !importedNormalized.marketplaceMinPrice);
    const merged = normalizeWarehouseProduct({
      ...current,
      ...importedNormalized,
      id: pickWarehouseProductMergeId(current, importedNormalized),
      name: preserveCurrentName ? current.name : importedNormalized.name,
      imageUrl: preserveCurrentImage ? current.imageUrl : importedNormalized.imageUrl,
      productUrl: preserveCurrentProductUrl ? current.productUrl : importedNormalized.productUrl,
      sku: preserveCurrentSku ? current.sku : importedNormalized.sku,
      marketplacePrice: preserveCurrentPrice ? current.marketplacePrice : importedNormalized.marketplacePrice,
      currentPrice: preserveCurrentCurrentPrice ? current.currentPrice : importedNormalized.currentPrice,
      targetPrice: preserveCurrentTargetPrice ? current.targetPrice : importedNormalized.targetPrice,
      targetStock: preserveCurrentTargetStock ? current.targetStock : importedNormalized.targetStock,
      marketplaceMinPrice: preserveCurrentMinPrice ? current.marketplaceMinPrice : importedNormalized.marketplaceMinPrice,
      marketplaceState: preserveCurrentState ? currentState : importedState,
      ozon: mergeOzonDraftForWarehouseProduct(current, importedNormalized, preserveCurrentRichOzonFields),
      yandex: mergeYandexDraftForWarehouseProduct(current, importedNormalized),
      keyword: current?.keyword || importedNormalized.keyword,
      markup: preserveCurrentYandexMarkup ? current.markup : (current?.markup || importedNormalized.markup),
      autoPriceEnabled: current?.autoPriceEnabled !== undefined ? current.autoPriceEnabled : importedNormalized.autoPriceEnabled,
      autoPriceMin: current?.autoPriceMin ?? importedNormalized.autoPriceMin,
      autoPriceMax: current?.autoPriceMax ?? importedNormalized.autoPriceMax,
      lastOzonPriceSend: importedNormalized.lastOzonPriceSend || current?.lastOzonPriceSend,
      lastYandexPriceSend: importedNormalized.lastYandexPriceSend || current?.lastYandexPriceSend,
      links: Array.isArray(current?.links) ? current.links : (Array.isArray(importedNormalized.links) ? importedNormalized.links : []),
      createdAt: current?.createdAt || importedNormalized.createdAt,
    });
    const mergedKey = warehouseProductExactMergeKey(merged);
    map.set(mergedKey, merged);
    rememberLooseKeys(merged, mergedKey);
  }

  return Array.from(map.values()).sort((a, b) => a.targetName.localeCompare(b.targetName) || a.name.localeCompare(b.name));
}

