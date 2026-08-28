function yandexTargetOfferKey(target = "", offerId = "") {
  return `${cleanText(target).toLowerCase()}::${cleanText(offerId).toLowerCase()}`;
}

function marketplaceProductMarkupOverride(product = {}) {
  const markup = Number(product?.markup || 0);
  if (!Number.isFinite(markup) || markup <= 0) return 0;
  const marketplace = cleanText(product?.marketplace || product?.target || "").toLowerCase();
  if (marketplace === "yandex") {
    const manual = product?.markupSource === "manual" || product?.yandex?.extra?.manualMarkup === true;
    return manual ? markup : 0;
  }
  return markup;
}

function yandexExportProductMarkup(sourceProduct = {}, yandexProduct = null) {
  const yandexMarkup = marketplaceProductMarkupOverride(yandexProduct);
  const sourceMarkup = marketplaceProductMarkupOverride(sourceProduct);
  if (!Number.isFinite(yandexMarkup) || yandexMarkup <= 0) return 0;
  if (sourceMarkup > 0 && Math.abs(yandexMarkup - sourceMarkup) < 0.0001) return 0;
  return yandexMarkup;
}

function yandexPriceUpdateResultKey(result = {}) {
  return yandexTargetOfferKey(result.target || result.shopId || "", result.offerId || result.offer_id || "");
}

function applyYandexPriceSendToWarehouse(warehouse = {}, shop = {}, row = {}, sentAt = new Date().toISOString()) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const offerId = cleanText(row.offerId || row.offer_id);
  const price = Number(row.price?.value || row.price || 0);
  if (!offerId || !shop?.id || !Number.isFinite(price) || price <= 0) return false;
  const product = products.find((item) => {
    const normalized = normalizeWarehouseProduct(item);
    return normalized.marketplace === "yandex"
      && normalized.target === shop.id
      && cleanText(normalized.offerId).toLowerCase() === offerId.toLowerCase();
  });
  if (!product) return false;
  const cabinetPriceAtSend = Number(product.marketplacePrice || product.currentPrice || 0) || null;
  product.marketplacePrice = roundPrice(price);
  product.currentPrice = roundPrice(price);
  product.targetPrice = roundPrice(price);
  if (Number.isFinite(Number(row.targetStock))) product.targetStock = Math.max(0, Math.round(Number(row.targetStock)));
  product.yandex = { ...(product.yandex || {}), offerId, price: roundPrice(price) };
  product.lastYandexPriceSend = {
    status: "success",
    at: sentAt,
    requestedPrice: roundPrice(price),
    cabinetPriceAtSend: cabinetPriceAtSend ? roundPrice(cabinetPriceAtSend) : null,
    detail: "",
    nextRetryAt: null,
  };
  product.updatedAt = sentAt;
  return true;
}

async function buildYandexPriceOverrideLookup(products = [], shops = [], warehouse = {}, yandexPriceLookup = new Map()) {
  const overrides = new Map();
  const sourceProducts = Array.isArray(products) ? products.map(normalizeWarehouseProduct) : [];
  if (!sourceProducts.length || !shops.length) return overrides;

  let appSettings = { defaultMarkups: { yandex: Number(process.env.DEFAULT_YANDEX_MARKUP || 1.6) }, markupRules: [] };
  try {
    appSettings = await readAppSettings();
  } catch (_error) {
    // Default markup is enough for this fallback path.
  }
  const rateSource = appSettings.fixedUsdRate || (await getUsdRate().catch(() => ({ rate: process.env.DEFAULT_USD_RATE || 95 }))).rate;
  const rate = Number(rateSource || process.env.DEFAULT_USD_RATE || 95);
  const allLinks = [];
  for (const product of sourceProducts) {
    allLinks.push(...(Array.isArray(product.links) ? product.links : []));
    for (const shop of shops) {
      const yandexProduct = yandexPriceLookup.get(yandexTargetOfferKey(shop.id, product.offerId));
      allLinks.push(...(Array.isArray(yandexProduct?.links) ? yandexProduct.links : []));
    }
  }
  let matchMap = new Map();
  if (allLinks.length) {
    try {
      matchMap = await getPriceMasterMatchesForLinks(allLinks, warehouse.suppliers || [], rate);
    } catch (error) {
      logger.warn("yandex import PriceMaster price calculation skipped", { detail: error?.message || String(error) });
    }
  }

  for (const product of sourceProducts) {
    if (!product.offerId) continue;
    for (const shop of shops) {
      const lookupKey = yandexTargetOfferKey(shop.id, product.offerId);
      const yandexProduct = yandexPriceLookup.get(lookupKey) || null;
      const yandexLinks = Array.isArray(yandexProduct?.links) && yandexProduct.links.length ? yandexProduct.links : product.links;
      const markupOverride = yandexExportProductMarkup(product, yandexProduct);
      const suppliers = (Array.isArray(yandexLinks) ? yandexLinks.map(normalizeWarehouseLink) : [])
        .flatMap((link) => (matchMap.get(link.id) || []).map((match) => {
          const markupCoefficient = resolveMarkupCoefficient({
            productMarkup: markupOverride,
            marketplace: "yandex",
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
        }));
      const availableSupplierCount = suppliers.filter((supplier) => supplier.available).length;
      const selectedSupplier = pickWarehouseSupplier(suppliers);
      if (selectedSupplier) {
        const availabilityPolicy = resolveAvailabilityPolicy({
          marketplace: "yandex",
          availableSupplierCount,
          baseMarkup: Number(selectedSupplier.markupCoefficient || 0),
          appSettings,
        });
        const markupCoefficient = Number(availabilityPolicy.markupCoefficient || selectedSupplier.markupCoefficient || 0);
        const price = calculateRubPrice(selectedSupplier.price, rate, markupCoefficient, selectedSupplier);
        if (price > 0) {
          overrides.set(lookupKey, {
            price,
            markupCoefficient,
            targetStock: Number.isFinite(Number(availabilityPolicy.targetStock)) ? availabilityPolicy.targetStock : null,
            source: "pricemaster",
          });
          continue;
        }
      }

      const persistedPrice = Number(
        yandexProduct?.targetPrice ||
          yandexProduct?.nextPrice ||
          yandexProduct?.marketplacePrice ||
          yandexProduct?.currentPrice ||
          0,
      ) || 0;
      if (persistedPrice > 0) {
        overrides.set(lookupKey, {
          price: persistedPrice,
          markupCoefficient: Number(yandexProduct?.markupCoefficient || yandexProduct?.markup || 0) || null,
          targetStock: Number.isFinite(Number(yandexProduct?.targetStock)) ? yandexProduct.targetStock : null,
          source: "yandex_row",
        });
      }
    }
  }

  return overrides;
}
