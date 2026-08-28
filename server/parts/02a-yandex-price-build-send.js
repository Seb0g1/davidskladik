function buildYandexPriceUpdateFromOzonProduct(product = {}, {
  yandexProduct = null,
  priceOverride = null,
  allowSourceFallback = true,
} = {}) {
  const normalized = normalizeWarehouseProduct(yandexProduct || product);
  const hasYandexProduct = Boolean(yandexProduct && Object.keys(yandexProduct).length);
  const forcedPrice = Number(priceOverride || 0) || 0;
  const overridePrice = Number(
    forcedPrice ||
      yandexProduct?.nextPrice ||
      yandexProduct?.targetPrice ||
      yandexProduct?.marketplacePrice ||
      yandexProduct?.currentPrice ||
      0,
  ) || 0;
  const built = buildYandexOfferMapping(normalized, overridePrice > 0 ? { price: overridePrice } : {});
  const offerId = cleanText(built.offer?.offerId || normalized.offerId || normalized.offer_id);
  const priceCandidates = [
    forcedPrice,
    yandexProduct?.nextPrice,
    yandexProduct?.targetPrice,
    yandexProduct?.marketplacePrice,
    yandexProduct?.currentPrice,
  ];
  if (allowSourceFallback || hasYandexProduct) {
    priceCandidates.push(
      built.offer?.basicPrice?.value,
      normalized.nextPrice,
      normalized.targetPrice,
      normalized.marketplacePrice,
      normalized.currentPrice,
      normalized.ozon?.price,
      normalized.price,
    );
  }
  const price = priceCandidates
    .map((value) => Number(value))
    .find((value) => Number.isFinite(value) && value > 0) || 0;
  const rounded = roundPrice(price);
  if (!offerId || !Number.isFinite(rounded) || rounded <= 0) return null;
  return { id: normalized.id, offerId, price: { value: rounded, currencyId: "RUR" } };
}

async function sendYandexPricesFromOzonProducts(products = [], options = {}) {
  const dryRun = options.dryRun === true;
  const warnings = [];
  const shops = Array.isArray(options.shops) && options.shops.length
    ? options.shops
    : uniqueYandexShopsByBusiness();
  const sourceProducts = (Array.isArray(products) ? products : []).filter((product) => !isYandexSmallVolumeBlocked(product));
  const sourceWarehouse = options.warehouse || (await readWarehouse().catch((error) => {
    logger.warn("read warehouse before yandex price send failed", { detail: error?.message || String(error) });
    return { products: [] };
  }));
  const yandexPriceLookup = new Map();
  for (const item of Array.isArray(sourceWarehouse.products) ? sourceWarehouse.products : []) {
    const normalized = normalizeWarehouseProduct(item);
    if (normalized.marketplace !== "yandex" || !normalized.offerId || !normalized.target) continue;
    yandexPriceLookup.set(yandexTargetOfferKey(normalized.target, normalized.offerId), normalized);
  }
  const priceOverrides = shops.length
    ? await buildYandexPriceOverrideLookup(sourceProducts, shops, sourceWarehouse, yandexPriceLookup)
    : new Map();
  const requestedPriceRows = sourceProducts.length;
  const rows = sourceProducts
    .map((product) => {
      const normalized = normalizeWarehouseProduct(product);
      const primaryShop = shops[0] || {};
      const lookupKey = yandexTargetOfferKey(primaryShop.id, normalized.offerId);
      const override = priceOverrides.get(lookupKey) || null;
      return buildYandexPriceUpdateFromOzonProduct(product, {
        yandexProduct: yandexPriceLookup.get(lookupKey) || null,
        priceOverride: override?.price || null,
        allowSourceFallback: false,
      });
    })
    .filter(Boolean);

  if (!rows.length || !shops.length) {
    if (shops.length && requestedPriceRows && !rows.length) {
      warnings.push("Yandex: price send skipped, no calculated Yandex price was available.");
    }
    return {
      ok: true,
      dryRun,
      sent: 0,
      failed: 0,
      skipped: shops.length ? requestedPriceRows : rows.length,
      warnings: shops.length ? warnings : ["Yandex Market не настроен."],
      results: [],
    };
  }

  let existingOfferIds = options.existingOfferIds instanceof Set ? options.existingOfferIds : null;
  if (!existingOfferIds) {
    const existingCheckTimeoutMs = Math.max(1000, Number(process.env.OZON_YANDEX_EXISTING_CHECK_TIMEOUT_MS || 8000) || 8000);
    try {
      existingOfferIds = await Promise.race([
        getExistingYandexOfferIdSet(rows.map((row) => row.offerId)),
        promiseTimeout(existingCheckTimeoutMs, "yandex_existing_check_timeout"),
      ]);
    } catch (error) {
      const label = error?.message === "yandex_existing_check_timeout"
        ? `таймаут ${Math.round(existingCheckTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие артикулы перед отправкой цен (${label})`);
      existingOfferIds = new Set();
    }
  }

  const selected = rows.filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  let sourceWarehouseMutated = false;
  if (dryRun) {
    return {
      ok: true,
      dryRun,
      sent: 0,
      failed: 0,
      skipped: Math.max(0, requestedPriceRows - selected.length),
      skippedNoPrice: Math.max(0, requestedPriceRows - rows.length),
      planned: selected.length,
      warnings,
      results: selected.map((row) => ({ ...row, ok: true, stage: "price", dryRun: true })),
    };
  }

  for (const shop of shops) {
    const shopRows = sourceProducts
      .map((product) => {
        const normalized = normalizeWarehouseProduct(product);
        const lookupKey = yandexTargetOfferKey(shop.id, normalized.offerId);
        const override = priceOverrides.get(lookupKey) || null;
        const row = buildYandexPriceUpdateFromOzonProduct(product, {
          yandexProduct: yandexPriceLookup.get(lookupKey) || null,
          priceOverride: override?.price || null,
          allowSourceFallback: false,
        });
        return row ? {
          ...row,
          sourceId: normalized.id,
          targetStock: override?.targetStock ?? null,
          markupCoefficient: override?.markupCoefficient ?? null,
          priceSource: override?.source || "",
        } : null;
      })
      .filter(Boolean)
      .filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));

    for (const chunk of chunkArray(shopRows, 500)) {
      if (!chunk.length) continue;
      try {
        const sentAt = new Date().toISOString();
        await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, {
          offers: chunk.map((item) => ({ offerId: item.offerId, price: item.price })),
        });
        for (const item of chunk) {
          if (applyYandexPriceSendToWarehouse(sourceWarehouse, shop, item, sentAt)) sourceWarehouseMutated = true;
        }
        results.push(...chunk.map((item) => ({
          stage: "price",
          sourceId: item.sourceId,
          offerId: item.offerId,
          target: shop.id,
          targetName: shop.name || "Yandex Market",
          price: item.price?.value || null,
          targetStock: item.targetStock,
          markupCoefficient: item.markupCoefficient,
          priceSource: item.priceSource,
          ok: true,
        })));
      } catch (error) {
        const label = error?.message || error?.code || "ошибка API";
        warnings.push(`Yandex «${shop.name || shop.id}»: цены не отправлены (${label})`);
        results.push(...chunk.map((item) => ({
          stage: "price",
          sourceId: item.sourceId,
          offerId: item.offerId,
          target: shop.id,
          targetName: shop.name || "Yandex Market",
          price: item.price?.value || null,
          targetStock: item.targetStock,
          markupCoefficient: item.markupCoefficient,
          priceSource: item.priceSource,
          ok: false,
          error: label,
        })));
      }
    }
  }

  if (sourceWarehouseMutated) {
    await writeWarehouse(sourceWarehouse);
  }

  const failed = results.filter((item) => !item.ok).length;
  return {
    ok: warnings.length === 0 && failed === 0,
    dryRun,
    sent: results.filter((item) => item.ok).length,
    failed,
    skipped: Math.max(0, requestedPriceRows - selected.length),
    skippedNoPrice: Math.max(0, requestedPriceRows - rows.length),
    planned: selected.length,
    warnings,
    results,
  };
}


