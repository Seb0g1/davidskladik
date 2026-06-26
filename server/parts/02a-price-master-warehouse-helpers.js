function warehouseSupplierPurchaseRubPrice(supplier = {}, rate = 0) {
  const preset = Number(supplier.purchaseRubPrice);
  if (Number.isFinite(preset) && preset > 0) return preset;
  if (supplierPriceIsRubNative(supplier)) {
    const rubBase = Number(supplier.originalPrice ?? supplier.price ?? 0);
    if (Number.isFinite(rubBase) && rubBase > 0) return rubBase;
  }
  const originalPrice = Number(supplier.originalPrice ?? NaN);
  if (supplierUsesRubPriceMasterPricing(supplier) && supplier.convertedFromRub && Number.isFinite(originalPrice) && originalPrice > 0) {
    return originalPrice;
  }
  const usd = Number(supplier.price || 0);
  if (!Number.isFinite(usd) || usd <= 0) return Number.MAX_SAFE_INTEGER;
  const fx = Number(rate || 0) > 0 ? Number(rate) : Number(process.env.DEFAULT_USD_RATE || 95);
  return Number((usd * fx).toFixed(4));
}

function compareWarehouseSupplierPrices(a = {}, b = {}) {
  const aPurchase = warehouseSupplierPurchaseRubPrice(a);
  const bPurchase = warehouseSupplierPurchaseRubPrice(b);
  const aHasPurchase = Number.isFinite(aPurchase) && aPurchase > 0 && aPurchase < Number.MAX_SAFE_INTEGER;
  const bHasPurchase = Number.isFinite(bPurchase) && bPurchase > 0 && bPurchase < Number.MAX_SAFE_INTEGER;
  if (aHasPurchase || bHasPurchase) {
    const aComparable = aHasPurchase ? aPurchase : Number.MAX_SAFE_INTEGER;
    const bComparable = bHasPurchase ? bPurchase : Number.MAX_SAFE_INTEGER;
    if (aComparable !== bComparable) return aComparable - bComparable;
  }
  const aFinal = Number(a.effectiveFinalPrice);
  const bFinal = Number(b.effectiveFinalPrice);
  const aHasFinal = Number.isFinite(aFinal) && aFinal > 0;
  const bHasFinal = Number.isFinite(bFinal) && bFinal > 0;
  if (aHasFinal || bHasFinal) {
    const aComparable = aHasFinal ? aFinal : Number.MAX_SAFE_INTEGER;
    const bComparable = bHasFinal ? bFinal : Number.MAX_SAFE_INTEGER;
    if (aComparable !== bComparable) return aComparable - bComparable;
  }
  const aCalculated = Number(a.calculatedPrice || 0);
  const bCalculated = Number(b.calculatedPrice || 0);
  if (aCalculated !== bCalculated) return aCalculated - bCalculated;
  const aUsd = Number(a.price || 0);
  const bUsd = Number(b.price || 0);
  if (aUsd !== bUsd) return aUsd - bUsd;
  // Final tiebreak must be a stable identifier, not docDate: docDate can differ between a
  // live PriceMaster fetch and a snapshot/previous fetch for the SAME row (re-touched rows),
  // which flips the "cheapest" pick between two equal-priced suppliers from build to build
  // and causes nextPrice to oscillate forever. rowId is the PriceMaster row primary key and
  // is stable across snapshot/live fetches.
  return String(a.rowId || "").localeCompare(String(b.rowId || ""));
}

function pickWarehouseSupplier(matches) {
  return [...matches]
    .filter((match) => match.available
      && match.priceEligible !== false
      && match.stockOnly !== true
      && !supplierUsesStockOnlyPricing(null, match))
    .sort(compareWarehouseSupplierPrices)[0] || null;
}

function pickWarehouseStockOnlySupplier(matches) {
  return [...matches]
    .filter((match) => match.available && supplierUsesStockOnlyPricing(null, match))
    .sort(
      (a, b) =>
        String(b.docDate).localeCompare(String(a.docDate))
        || String(a.partnerName || a.supplierName || "").localeCompare(String(b.partnerName || b.supplierName || ""))
        // Same rationale as compareWarehouseSupplierPrices: docDate/partnerName ties must
        // resolve to a stable winner across rebuilds, or selectedSupplier (and thus the
        // diagnostics shown to users) flips nondeterministically between equal candidates.
        || String(a.rowId || "").localeCompare(String(b.rowId || "")),
    )[0] || null;
}

function resolveMarkupCoefficient({ productMarkup, marketplace, supplierUsdPrice, supplierPriceCurrency = "USD", usdRate = 0, appSettings }) {
  if (Number(productMarkup) > 0) return Number(productMarkup);
  const defaults = appSettings?.defaultMarkups || {};
  const fallback = marketplace === "ozon"
    ? Number(defaults.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
    : Number(defaults.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
  let usd = Number(supplierUsdPrice || 0);
  const currency = cleanText(supplierPriceCurrency || "USD").toUpperCase();
  if ((currency === "RUB" || currency === "RUR") && usd > 0) {
    const rate = Number(usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    usd = usd / rate;
  }
  const rules = Array.isArray(appSettings?.markupRules) ? appSettings.markupRules : [];
  if (!Number.isFinite(usd) || usd <= 0 || !rules.length) return fallback;
  const scopedRules = rules.filter((rule) => !rule.marketplace || rule.marketplace === "all" || rule.marketplace === marketplace);
  if (!scopedRules.length) return fallback;
  const sorted = [...scopedRules].sort((a, b) => b.minUsd - a.minUsd);
  const matched = sorted.find((rule) => usd >= Number(rule.minUsd || 0));
  return Number(matched?.coefficient || fallback);
}

function resolveAvailabilityPolicy({ marketplace, availableSupplierCount = 0, baseMarkup = 0, appSettings } = {}) {
  const count = Math.max(0, Number(availableSupplierCount || 0));
  let rules = Array.isArray(appSettings?.availabilityRules) ? appSettings.availabilityRules : [];
  if (!rules.length) rules = defaultAppSettings().availabilityRules;
  const scopedRules = rules.filter((rule) => !rule.marketplace || rule.marketplace === "all" || rule.marketplace === marketplace);
  const sorted = [...scopedRules].sort((a, b) => Number(b.minAvailableSuppliers || 0) - Number(a.minAvailableSuppliers || 0));
  const matched = sorted.find((rule) => count >= Number(rule.minAvailableSuppliers || 0)) || null;
  const base = Number(baseMarkup || 0);
  const delta = Number(matched?.coefficientDelta || 0);
  const markupCoefficient = base > 0 ? Math.max(0.0001, Number((base + delta).toFixed(4))) : base;
  const policyStock = matched ? Math.max(0, Math.round(Number(matched.targetStock || 0))) : null;
  const targetStock = policyStock && policyStock > 0 ? policyStock : (count > 0 ? 3 : null);
  return {
    rule: matched,
    baseMarkup: base,
    coefficientDelta: delta,
    markupCoefficient,
    targetStock,
  };
}

function resolveWarehouseTargetStock(availabilityPolicy = {}, {
  selectedSupplierWithPolicy = null,
  availableSupplierCount = 0,
  stockOnlyAvailableSupplierCount = 0,
} = {}) {
  const policyStock = Number(availabilityPolicy?.targetStock);
  if (Number.isFinite(policyStock) && policyStock > 0) return policyStock;
  const supplierCount = Math.max(0, Number(availableSupplierCount || 0)) + Math.max(0, Number(stockOnlyAvailableSupplierCount || 0));
  if (selectedSupplierWithPolicy || supplierCount > 0) return Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
  return null;
}

function enrichSupplierPriceCandidates(suppliers = [], {
  productMarkupOverride = 0,
  marketplace = "",
  rate = 0,
  appSettings = {},
  fallbackMarkup = 0,
  availableSupplierCount = 0,
  stockOnlyAvailableSupplierCount = 0,
} = {}) {
  const policySupplierCount = availableSupplierCount || stockOnlyAvailableSupplierCount;
  return (Array.isArray(suppliers) ? suppliers : []).map((supplier) => {
    const baseMarkupCoefficient = Number(productMarkupOverride || supplier.markupCoefficient || fallbackMarkup || 0);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace,
      availableSupplierCount: policySupplierCount,
      baseMarkup: baseMarkupCoefficient,
      appSettings,
    });
    const markupCoefficient = Number(availabilityPolicy.markupCoefficient || baseMarkupCoefficient);
    const priceEligible = supplier.priceEligible !== false && supplier.stockOnly !== true;
    const purchaseRubPrice = warehouseSupplierPurchaseRubPrice(supplier, rate);
    const effectiveFinalPrice = supplier.available && priceEligible
      ? calculateRubPrice(supplier.price, rate, markupCoefficient, supplier)
      : null;
    return {
      ...supplier,
      baseMarkupCoefficient,
      markupCoefficient,
      effectiveMarkupCoefficient: markupCoefficient,
      availabilityRule: availabilityPolicy.rule,
      purchaseRubPrice,
      prePolicyCalculatedPrice: Number(supplier.calculatedPrice || 0) || null,
      calculatedPrice: effectiveFinalPrice || supplier.calculatedPrice || null,
      effectiveFinalPrice,
      priceSelectionReason: effectiveFinalPrice
        ? "cheapest_supplier_purchase_price"
        : (supplier.stockOnly || supplier.priceEligible === false ? "stock_only_excluded" : "not_available"),
    };
  });
}

function supplierAlternativesForDiagnostics(suppliers = [], limit = 5) {
  return [...(Array.isArray(suppliers) ? suppliers : [])]
    .sort((a, b) =>
      compareWarehouseSupplierPrices(a, b)
      || Number(a.effectiveFinalPrice || a.calculatedPrice || Number.MAX_SAFE_INTEGER)
      - Number(b.effectiveFinalPrice || b.calculatedPrice || Number.MAX_SAFE_INTEGER))
    .slice(0, Math.max(1, Number(limit || 5) || 5))
    .map((supplier) => ({
      partnerName: supplier.partnerName || supplier.supplierName || "",
      supplierName: supplier.supplierName || supplier.partnerName || "",
      partnerId: supplier.partnerId || "",
      article: supplier.article || "",
      name: supplier.name || supplier.nativeName || supplier.keyword || "",
      rowId: supplier.rowId || "",
      available: Boolean(supplier.available),
      active: supplier.active !== false,
      stopped: Boolean(supplier.stopped),
      stockOnly: Boolean(supplier.stockOnly || supplier.priceEligible === false),
      priceEligible: supplier.priceEligible !== false && supplier.stockOnly !== true,
      price: supplier.price,
      originalPrice: supplier.originalPrice,
      priceCurrency: supplier.priceCurrency || supplier.currency || "",
      sourceCurrency: supplier.sourceCurrency || supplier.priceCurrency || "",
      markupCoefficient: supplier.markupCoefficient,
      baseMarkupCoefficient: supplier.baseMarkupCoefficient,
      effectiveFinalPrice: supplier.effectiveFinalPrice || supplier.calculatedPrice || null,
      calculatedPrice: supplier.calculatedPrice || null,
      priceSource: supplier.priceSource || supplier.source || "snapshot",
      exclusionReason: supplier.available
        ? (supplier.stockOnly || supplier.priceEligible === false ? "stock_only_excluded" : null)
        : (supplier.stopped ? "supplier_stopped" : "not_available"),
    }));
}

function applyWarehouseNextPriceLimits(nextPrice, { autoPriceMin = 0, autoPriceMax = 0, ozonMinPrice = null } = {}) {
  let price = Number(nextPrice);
  if (!Number.isFinite(price) || price <= 0) return price;
  const minAuto = Number(autoPriceMin || 0);
  const maxAuto = Number(autoPriceMax || 0);
  if (minAuto > 0 && price < minAuto) price = minAuto;
  if (maxAuto > 0 && price > maxAuto) price = maxAuto;
  const ozonMin = Number(ozonMinPrice || 0);
  if (ozonMin > 0 && price < ozonMin) price = ozonMin;
  return price;
}

function storedMarketplacePrice(product = {}) {
  const ozonPrice = Number(product.ozon?.price || 0);
  const yandexPrice = Number(product.yandex?.price || 0);
  return Number(product.marketplacePrice || 0) || (product.marketplace === "ozon" ? ozonPrice : yandexPrice) || null;
}
