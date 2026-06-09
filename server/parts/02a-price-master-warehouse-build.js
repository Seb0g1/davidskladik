async function buildWarehouseView({ sync = false, usdRate, targetMarkups = {}, limit = Number.POSITIVE_INFINITY, refreshPrices = false, onProgress = null } = {}) {
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || (await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
  let warehouse = await readWarehouse();
  const supplierSync = { ok: false, partners: 0, imported: 0, changed: false, error: null };
  try {
    const partners = await listPriceMasterPartners();
    const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
    supplierSync.ok = true;
    supplierSync.partners = partners.length;
    supplierSync.imported = syncedSuppliers.imported;
    supplierSync.changed = syncedSuppliers.changed;
    if (syncedSuppliers.changed) {
      await writeWarehouse(warehouse);
      logger.info("imported suppliers from PriceMaster", { imported: syncedSuppliers.imported });
    }
  } catch (error) {
    supplierSync.error = error.message;
    logger.warn("supplier import from PriceMaster failed", { detail: error.message });
  }
  const autoReactivated = applySupplierAutoReactivate(warehouse);
  if (autoReactivated.length) {
    await writeWarehouse(warehouse);
    logger.info("supplier auto-reactivated by date", { count: autoReactivated.length, suppliers: autoReactivated });
  }
  let syncWarnings = [];
  let marketplaceSyncChangedProductIds = [];
  if (sync) {
    const beforeSyncProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
    const synced = await syncWarehouseProductsFromMarketplaces(warehouse, limit, { onProgress });
    warehouse = synced.warehouse;
    marketplaceSyncChangedProductIds = changedWarehouseProductIdsByAutomationFingerprint(
      beforeSyncProducts,
      warehouse.products || [],
    );
    syncWarnings = synced.warnings || [];
    onProgress?.({
      percent: 76,
      stage: "Запись склада",
      meta: `Сохраняю ${formatRuNumber(warehouse.products?.length || 0)} карточек склада.`,
      processed: Number(warehouse.products?.length || 0),
      total: Number(warehouse.products?.length || 0),
    });
    await writeWarehouse(warehouse);
    syncWarnings.forEach((detail) => logger.warn("warehouse sync warning", { detail }));
  }

  const links = warehouse.products.flatMap((product) => product.links || []);
  let matchMap = new Map();
  let sourceError = null;
  try {
    matchMap = await getPriceMasterMatchesForLinks(links, warehouse.suppliers, rate);
  } catch (error) {
    sourceError = error.code || error.message;
  }

  const [priceMapResult, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(warehouse.products, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(warehouse.products, { refresh: refreshPrices }),
  ]);
  const priceMap = priceMapResult.map;
  const minPriceMap = minPriceResult.map;
  if (refreshPrices && (priceMapResult.mutated || minPriceResult.mutated)) {
    await writeWarehouse(warehouse);
  }
  const products = warehouse.products.filter(isWarehouseProductTargetEnabled).map((product) => {
    const productMarkupOverride = marketplaceProductMarkupOverride(product);
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const rawSuppliers = normalizedLinks.flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => ({
        ...match,
        markupCoefficient: resolveMarkupCoefficient({
          productMarkup: productMarkupOverride,
          marketplace: product.marketplace,
          supplierUsdPrice: match.price,
          supplierPriceCurrency: match.priceCurrency || match.currency,
          usdRate: rate,
          appSettings: {
            ...appSettings,
            defaultMarkups: {
              ozon: Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7),
              yandex: Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
            },
          },
        }),
        calculatedPrice: calculateRubPrice(
          match.price,
          rate,
          resolveMarkupCoefficient({
            productMarkup: productMarkupOverride,
            marketplace: product.marketplace,
            supplierUsdPrice: match.price,
            supplierPriceCurrency: match.priceCurrency || match.currency,
            usdRate: rate,
            appSettings: {
              ...appSettings,
              defaultMarkups: {
                ozon: Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7),
                yandex: Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
              },
            },
          }),
          match,
        ),
      })),
    );
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
    const availableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && supplier.priceEligible !== false && supplier.stockOnly !== true).length;
    const stockOnlyAvailableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && (supplier.stockOnly === true || supplier.priceEligible === false)).length;
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
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
        priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (sourceError ? "timeout" : "snapshot")),
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
      stockOnlyAvailableSupplierCount,
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной закупочной ценой."
        : "Нет доступного поставщика.",
      priceSelectionReason: selectedSupplier ? "cheapest_supplier_purchase_price" : (stockOnlyFallbackActive ? "stock_only_fallback" : "no_supplier"),
      priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (sourceError ? "timeout" : "snapshot")),
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

  return {
    createdAt: new Date().toISOString(),
    updatedAt: warehouse.updatedAt || warehouse.createdAt || null,
    usdRate: rate,
    sourceError,
    supplierSync,
    priceMaster: await getPriceMasterSnapshotMeta(),
    targets: marketplaceTargets(),
    suppliers: warehouse.suppliers,
    products,
    total: products.length,
    ready: products.filter((product) => product.ready).length,
    changed: products.filter((product) => product.changed).length,
    withoutSupplier: products.filter((product) => !product.selectedSupplier && Number(product.supplierCount || 0) > 0).length,
    linkedArchived: products.filter((product) => product.hasLinks && product.marketplace === "ozon" && product.marketplaceState?.code === "archived").length,
    ozonArchived: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "archived").length,
    ozonInactive: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "inactive").length,
    ozonOutOfStock: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "out_of_stock").length,
    noSupplierAlerts: buildNoSupplierAlerts(products, { limit: 12 }),
    autoArchiveAlerts: products
      .filter((product) => !product.hasLinks)
      .slice(0, 30)
      .map((product) => ({
        id: product.id,
        offerId: product.offerId,
        name: product.name,
        marketplace: product.marketplace,
        action: "Автоархив кандидат",
      })),
    syncWarnings,
    marketplaceSyncChangedProductIds,
    marketplaceSyncChanged: marketplaceSyncChangedProductIds.length,
  };
}

