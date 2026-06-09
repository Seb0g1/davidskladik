async function getWarehousePriceMaps(products, { refresh = false } = {}) {
  const result = new Map();
  let mutated = false;
  for (const product of products) result.set(product.id, storedMarketplacePrice(product));
  if (!refresh) return { map: result, mutated };

  for (const account of getOzonAccounts()) {
    const accountProducts = products.filter((product) => product.target === account.id && product.marketplace === "ozon");
    const ozonOfferIds = accountProducts.map((product) => product.offerId).filter(Boolean);
    if (!ozonOfferIds.length) continue;
    try {
      const priceMap = await getOzonPriceMap(ozonOfferIds, account);
      for (const product of accountProducts) {
        const raw = priceMap.get(product.offerId);
        const details = normalizeOzonPriceDetails(raw || {});
        const listed = pickOzonCabinetListedPrice(details);
        const fallback = storedMarketplacePrice(product);
        const value = listed ?? fallback;
        result.set(product.id, value);
        if (listed != null) {
          if (product.marketplacePrice !== listed) {
            product.marketplacePrice = listed;
            mutated = true;
          }
          const oz = product.ozon || {};
          product.ozon = {
            ...oz,
            price: listed,
            minPrice: details.minPrice ?? oz.minPrice,
            oldPrice: details.oldPrice ?? oz.oldPrice,
            marketingSellerPrice: details.marketingSellerPrice ?? oz.marketingSellerPrice,
            marketingPrice: details.marketingPrice ?? oz.marketingPrice,
            retailPrice: details.retailPrice ?? oz.retailPrice,
          };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  for (const shop of getYandexShops()) {
    const shopProducts = products.filter((product) => product.target === shop.id);
    const offerIds = shopProducts.map((product) => product.offerId).filter(Boolean);
    if (!offerIds.length) continue;
    try {
      const priceMap = await getYandexPriceMap(shop, offerIds);
      for (const product of shopProducts) {
        const yPrice = Number(priceMap.get(product.offerId) || 0) || null;
        const fallback = storedMarketplacePrice(product);
        const value = yPrice ?? fallback;
        result.set(product.id, value);
        if (yPrice != null) {
          if (product.marketplacePrice !== yPrice) {
            product.marketplacePrice = yPrice;
            mutated = true;
          }
          product.yandex = { ...(product.yandex || {}), price: yPrice };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  return { map: result, mutated };
}

async function getWarehouseMinPriceMaps(products, { refresh = false } = {}) {
  const result = new Map();
  let mutated = false;
  for (const product of products) {
    result.set(product.id, Number(product.marketplaceMinPrice || product.ozon?.minPrice || 0) || null);
  }

  if (!refresh) return { map: result, mutated };

  for (const account of getOzonAccounts()) {
    const accountProducts = products.filter((product) => product.target === account.id && product.marketplace === "ozon");
    const ozonOfferIds = accountProducts.map((product) => product.offerId).filter(Boolean);
    if (!ozonOfferIds.length) continue;
    try {
      const priceMap = await getOzonPriceMap(ozonOfferIds, account);
      for (const product of accountProducts) {
        const details = normalizeOzonPriceDetails(priceMap.get(product.offerId) || {});
        const minPrice = details.minPrice || null;
        result.set(product.id, minPrice);
        if (minPrice !== null && product.marketplaceMinPrice !== minPrice) {
          product.marketplaceMinPrice = minPrice;
          product.ozon = { ...(product.ozon || {}), minPrice };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored min prices when Ozon request fails.
    }
  }

  return { map: result, mutated };
}
