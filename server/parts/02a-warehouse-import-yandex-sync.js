async function importYandexWarehouseProducts(limit = Number.POSITIVE_INFINITY) {
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const imported = [];
  const warnings = [];
  if (!shops.length) return { imported, warnings };

  const perShopLimit = Number.isFinite(Number(limit)) && Number(limit) > 0
    ? Math.max(1, Math.ceil(Number(limit) / shops.length))
    : Number.POSITIVE_INFINITY;

  for (const shop of shops) {
    try {
      const mappings = await getYandexOfferMappings(shop, perShopLimit);
      imported.push(
        ...mappings
          .map((item) => normalizeYandexWarehouseProduct(item, shop))
          .filter(Boolean),
      );
    } catch (error) {
      const label = error?.message || error?.code || "ошибка";
      warnings.push(`Yandex «${shop.name || shop.id}»: ${label}`);
      logger.warn("yandex shop import failed", { shop: shop.id, detail: label });
    }
  }

  return { imported, warnings };
}

async function syncWarehouseProductsFromMarketplaces(warehouse, limit = Number.POSITIVE_INFINITY, options = {}) {
  const warnings = [];
  let imported = [];
  try {
    const oz = await importOzonWarehouseProducts(limit, warehouse.products || [], options);
    imported = imported.concat(oz.imported);
    warnings.push(...oz.warnings);
  } catch (error) {
    const label = error?.message || error?.code || "ошибка";
    warnings.push(`Ozon: ${label}`);
    logger.warn("ozon import failed", { detail: label });
  }
  try {
    const ya = await importYandexWarehouseProducts(limit);
    imported = imported.concat(ya.imported);
    warnings.push(...ya.warnings);
  } catch (error) {
    const label = error?.message || error?.code || "ошибка";
    warnings.push(`Yandex Market: ${label}`);
    logger.warn("yandex import failed", { detail: label });
  }
  return {
    warehouse: { ...warehouse, products: mergeProducts(warehouse.products, imported) },
    warnings,
  };
}


