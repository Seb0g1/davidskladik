async function buildWarehouseViewCached(params = {}) {
  if (params.sync || params.refreshPrices) return buildWarehouseView(params);
  const key = warehouseViewCacheKey(params);
  const cached = warehouseViewCache.get(key);
  const ttlMs = warehouseViewCacheMs;
  if (cached && Date.now() - cached.at < ttlMs) return cached.data;
  const existingBuild = warehouseViewBuilds.get(key);
  if (existingBuild) return existingBuild;
  const build = buildWarehouseView(params)
    .then((data) => {
      lastWarehouseViewSnapshot = data;
      warehouseViewCache.set(key, { at: Date.now(), data });
      return data;
    })
    .finally(() => {
      warehouseViewBuilds.delete(key);
    });
  warehouseViewBuilds.set(key, build);
  return build;
}

async function buildFreshWarehouseProductsForWarehouse(warehouse, productIds = [], { refreshPrices = false, persistMutations = false, livePriceMaster = false, batchPriceMaster = false, usdRate, priceMasterTimeoutMs } = {}) {
  const wanted = new Set((productIds || []).map((id) => String(id)));
  if (!wanted.size) return [];
  const appSettings = await readAppSettings();
  const rateSource = appSettings.fixedUsdRate || usdRate || (batchPriceMaster ? process.env.DEFAULT_USD_RATE : (await getUsdRate()).rate);
  const rate = Number(rateSource || process.env.DEFAULT_USD_RATE || 95);
  const productsToBuild = (warehouse.products || []).filter((product) => wanted.has(String(product.id)));
  if (!productsToBuild.length) return [];
  const links = productsToBuild.flatMap((product) => product.links || []);
  const effectivePriceMasterTimeoutMs = Math.max(500, Number(priceMasterTimeoutMs || process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 1500) || 1500);
  let priceMasterSourceError = null;
  let matchMap = new Map();
  if (livePriceMaster) {
    if (batchPriceMaster) {
      matchMap = await Promise.race([
        getBatchPriceMasterMatchesForLinks(links, warehouse.suppliers, rate, { timeoutMs: effectivePriceMasterTimeoutMs }),
        promiseTimeout(effectivePriceMasterTimeoutMs + 100, "warehouse_page_pm_timeout"),
      ]).catch((error) => {
        priceMasterSourceError = error?.message || String(error);
        logger.warn("warehouse page PriceMaster enrichment skipped", { detail: priceMasterSourceError });
        return new Map();
      });
    } else {
      matchMap = await getLivePriceMasterMatchesForLinks(links, warehouse.suppliers, rate).catch((error) => {
        priceMasterSourceError = error?.message || String(error);
        logger.warn("warehouse live PriceMaster enrichment skipped", { detail: priceMasterSourceError });
        return new Map();
      });
    }
  } else {
    matchMap = await getPriceMasterMatchesForLinks(links, warehouse.suppliers, rate);
  }
  const [priceMapResult, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(productsToBuild, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(productsToBuild, { refresh: refreshPrices }),
  ]);
  if (persistMutations && (priceMapResult.mutated || minPriceResult.mutated)) await writeWarehouse(warehouse);
  const priceMap = priceMapResult.map;
  const minPriceMap = minPriceResult.map;

  return productsToBuild.map((product) => {
    const productMarkupOverride = marketplaceProductMarkupOverride(product);
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const buildNow = new Date();
    const rawSuppliers = normalizedLinks.flatMap((link) => {
      if (link.snooze?.snoozedUntil && new Date(link.snooze.snoozedUntil) > buildNow) return [];
      return (matchMap.get(link.id) || []).map((match) => {
        const markupCoefficient = resolveMarkupCoefficient({
          productMarkup: productMarkupOverride,
          marketplace: product.marketplace,
          supplierUsdPrice: match.price,
          supplierPriceCurrency: match.priceCurrency || match.currency,
          usdRate: rate,
          appSettings,
        });
        return {
          ...match,
          markupCoefficient,
          calculatedPrice: calculateRubPrice(match.price, rate, markupCoefficient, match),
        };
      });
    });
    const hasSnoozedLinks = normalizedLinks.some((link) => link.snooze?.snoozedUntil && new Date(link.snooze.snoozedUntil) > buildNow);
    const links = normalizedLinks.map((link) => {
      const matched = matchMap.get(link.id) || [];
      const availableMatches = matched.filter((item) => item.available);
      return {
        ...link,
        matchedCount: matched.length,
        availableCount: availableMatches.length,
        priceEligibleCount: availableMatches.filter((item) => item.priceEligible !== false && item.stockOnly !== true).length,
        stockOnlyCount: availableMatches.filter((item) => item.stockOnly === true || item.priceEligible === false).length,
        stockOnly: availableMatches.some((item) => item.stockOnly === true || item.priceEligible === false),
        priceEligible: availableMatches.some((item) => item.priceEligible !== false && item.stockOnly !== true),
        missingInPriceMaster: availableMatches.length === 0,
        unavailableInPriceMaster: matched.length > 0 && availableMatches.length === 0,
      };
    });
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const availableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && supplier.priceEligible !== false && supplier.stockOnly !== true).length;
    const stockOnlyAvailableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && (supplier.stockOnly === true || supplier.priceEligible === false)).length;
    const suppliers = enrichSupplierPriceCandidates(rawSuppliers, {
      productMarkupOverride,
      marketplace: product.marketplace,
      rate,
      appSettings,
      fallbackMarkup,
      availableSupplierCount,
      stockOnlyAvailableSupplierCount,
    });
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const stockOnlySupplier = selectedSupplier ? null : pickWarehouseStockOnlySupplier(suppliers);
    const stockOnlyFallbackActive = Boolean(!selectedSupplier && stockOnlySupplier);
    const stockOnlyManualPrice = stockOnlyFallbackActive ? stockOnlyManualPriceForProduct(product) : null;
    const baseMarkupCoefficient = Number(productMarkupOverride || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace: product.marketplace,
      availableSupplierCount: availableSupplierCount || stockOnlyAvailableSupplierCount,
      baseMarkup: baseMarkupCoefficient,
      appSettings,
    });
    const markupCoefficient = Number(availabilityPolicy.markupCoefficient || baseMarkupCoefficient);
    const selectedSupplierWithPolicy = selectedSupplier
      ? {
          ...selectedSupplier,
          baseMarkupCoefficient,
          markupCoefficient,
          availabilityRule: availabilityPolicy.rule,
          calculatedPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient, selectedSupplier)),
          effectiveFinalPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient, selectedSupplier)),
        }
      : (stockOnlySupplier
          ? {
              ...stockOnlySupplier,
              baseMarkupCoefficient,
              markupCoefficient,
              availabilityRule: availabilityPolicy.rule,
              calculatedPrice: null,
              manualPrice: stockOnlyManualPrice,
            }
          : null);
    const rawNextPrice = selectedSupplier && selectedSupplierWithPolicy
      ? Number(selectedSupplierWithPolicy.calculatedPrice || calculateRubPrice(selectedSupplierWithPolicy.price, rate, markupCoefficient, selectedSupplierWithPolicy))
      : (stockOnlyManualPrice || 0);
    const minAuto = Number(product.autoPriceMin || 0);
    const maxAuto = Number(product.autoPriceMax || 0);
    const persistedCurrentPrice = Number(product.currentPrice || product.marketplacePrice || 0) || null;
    const persistedNextPrice = Number(product.targetPrice || product.nextPrice || 0) || 0;
    let nextPrice = rawNextPrice;
    const usesStockOnlyPricing = stockOnlyFallbackActive || supplierUsesStockOnlyPricing(null, selectedSupplierWithPolicy || selectedSupplier || {});
    if (!usesStockOnlyPricing && (!Number.isFinite(nextPrice) || nextPrice <= 0) && product.marketplace === "yandex" && persistedNextPrice > 0) {
      nextPrice = persistedNextPrice;
    }
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;
    nextPrice = applyWarehouseNextPriceLimits(nextPrice, { autoPriceMin: minAuto, autoPriceMax: maxAuto, ozonMinPrice });
    const currentPrice = priceMap.get(product.id) || persistedCurrentPrice;
    const lastPriceSend = product.marketplace === "ozon" ? product.lastOzonPriceSend : product.lastYandexPriceSend;

    return {
      ...product,
      brand: resolveWarehouseBrand(product),
      markupCoefficient,
      usdRate: rate,
      priceFormula: {
        marketplace: product.marketplace,
        usdRate: rate,
        selectedSupplierPrice: selectedSupplierWithPolicy && !stockOnlyFallbackActive ? Number(selectedSupplierWithPolicy.price || 0) : null,
        selectedSupplierCurrency: selectedSupplierWithPolicy?.priceCurrency || selectedSupplierWithPolicy?.currency || "",
        baseMarkupCoefficient,
        markupCoefficient,
        calculatedPrice: rawNextPrice || null,
        targetPrice: nextPrice || null,
        currentPrice,
        availabilityRule: availabilityPolicy.rule || null,
        targetStock: availabilityPolicy.targetStock ?? null,
        stockOnlyFallbackActive,
        stockOnlyManualPrice,
        priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || "snapshot"),
        priceSelectionReason: selectedSupplierWithPolicy?.priceSelectionReason || null,
      },
      autoPriceEnabled: normalizedLinks.length > 0 ? true : product.autoPriceEnabled !== false,
      autoPriceMin: minAuto > 0 ? minAuto : null,
      autoPriceMax: maxAuto > 0 ? maxAuto : null,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      lastRequestedPrice: Number(lastPriceSend?.requestedPrice || lastPriceSend?.lastRequestedPrice || 0) || null,
      lastVerifiedMarketplacePrice: Number(lastPriceSend?.verifiedPrice || lastPriceSend?.lastVerifiedPrice || 0) || null,
      lastPriceRequestAt: lastPriceSend?.lastPriceRequestAt || lastPriceSend?.requestedAt || lastPriceSend?.at || null,
      lastPriceVerifiedAt: lastPriceSend?.lastPriceVerifiedAt || lastPriceSend?.verifiedAt || null,
      priceApplyStatus: cleanText(lastPriceSend?.priceApplyStatus || lastPriceSend?.applyStatus || lastPriceSend?.status),
      priceIntentId: cleanText(lastPriceSend?.priceIntentId),
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplierWithPolicy && (nextPrice > 0 || stockOnlyFallbackActive)),
      selectedSupplier: selectedSupplierWithPolicy,
      fallbackSuppliers: suppliers
        .filter((supplier) => supplier.available)
        .slice(0, 3)
        .map((supplier) => ({
          partnerName: supplier.partnerName || supplier.supplierName || "",
          article: supplier.article || "",
          price: supplier.price,
          calculatedPrice: supplier.calculatedPrice,
          stockOnly: Boolean(supplier.stockOnly || supplier.priceEligible === false),
        })),
      stockOnlyFallbackActive,
      supplierAlternatives: supplierAlternativesForDiagnostics(suppliers, 5),
      stockOnlyManualPriceMissing: Boolean(stockOnlyFallbackActive && !stockOnlyManualPrice),
      stockOnlyManualPrices: normalizeStockOnlyManualPrices(product.stockOnlyManualPrices),
      hasSnoozedLinks,
      stockOnlyAvailableSupplierCount,
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной закупочной ценой."
        : "Нет доступного поставщика.",
      priceSelectionReason: selectedSupplier ? "cheapest_supplier_purchase_price" : (stockOnlyFallbackActive ? "stock_only_fallback" : "no_supplier"),
      priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (priceMasterSourceError ? "timeout" : "snapshot")),
      links,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount,
      availabilityRule: availabilityPolicy.rule,
      targetStock: selectedSupplierWithPolicy
        ? resolveWarehouseTargetStock(availabilityPolicy, {
          selectedSupplierWithPolicy,
          availableSupplierCount,
          stockOnlyAvailableSupplierCount,
        })
        : (product.marketplace === "yandex" ? Number(product.targetStock || 0) || null : null),
      hasLinks: links.length > 0,
      everHadLinks: Boolean(product.everHadLinks || links.length > 0),
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : (stockOnlyFallbackActive ? (stockOnlyManualPrice ? "stock_only_fallback" : "stock_only_manual_price_missing") : "no_supplier"),
    };
  });
}

function summarizeWarehouseCounterStats({ totalProducts = 0, linkedProducts = [], builtLinkedProducts = [] } = {}) {
  const linkedCount = Array.isArray(linkedProducts) ? linkedProducts.length : 0;
  const built = Array.isArray(builtLinkedProducts) ? builtLinkedProducts : [];
  const ready = built.filter((product) => product.ready).length;
  const changed = built.filter((product) => product.changed).length;
  return {
    linkedProducts: linkedCount,
    ready,
    changed,
    withoutSupplier: Math.max(0, Number(totalProducts || 0) - linkedCount),
    linkedNotReady: Math.max(0, linkedCount - ready),
  };
}

async function buildWarehouseCounterStatsFromLinkedProducts(products = [], suppliers = [], { totalProducts = 0, usdRate } = {}) {
  const linkedProducts = (Array.isArray(products) ? products : [])
    .map(normalizeWarehouseProduct)
    .filter((product) => Array.isArray(product.links) && product.links.length > 0);
  if (!linkedProducts.length) {
    return summarizeWarehouseCounterStats({ totalProducts, linkedProducts, builtLinkedProducts: [] });
  }
  const builtLinkedProducts = await buildFreshWarehouseProductsForWarehouse(
    { products: linkedProducts, suppliers: Array.isArray(suppliers) ? suppliers : [] },
    linkedProducts.map((product) => product.id),
    { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate },
  );
  return summarizeWarehouseCounterStats({ totalProducts, linkedProducts, builtLinkedProducts });
}

async function buildFreshWarehouseProducts(productIds = [], {
  refreshPrices = false,
  persistMutations = true,
  livePriceMaster = false,
  batchPriceMaster = false,
  usdRate,
  priceMasterTimeoutMs,
} = {}) {
  await hydrateWarehouseProductsForIds(productIds, { expandGroups: false });
  const warehouse = await readWarehouse();
  return buildFreshWarehouseProductsForWarehouse(warehouse, productIds, {
    refreshPrices,
    persistMutations,
    livePriceMaster,
    batchPriceMaster,
    usdRate,
    priceMasterTimeoutMs,
  });
}

async function buildFreshWarehouseProductsFromKnownProducts(warehouse = {}, products = [], options = {}) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return [];
  return buildFreshWarehouseProductsForWarehouse(
    {
      createdAt: warehouse.createdAt || null,
      updatedAt: warehouse.updatedAt || null,
      products: normalizedProducts,
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [],
    },
    normalizedProducts.map((product) => product.id),
    {
      refreshPrices: false,
      persistMutations: false,
      livePriceMaster: false,
      batchPriceMaster: false,
      ...options,
    },
  );
}
