function isExpectedMarketplaceArchiveBlock(detail = "") {
  const value = cleanText(detail).toLowerCase();
  return Boolean(
    value.includes("item has fbo stock")
      || value.includes("fbo stock")
      || value.includes("has stock")
      || value.includes("нельзя архив")
      || value.includes("остат"),
  );
}

async function sendYandexStocksFromOzonProducts(products = [], options = {}) {
  const dryRun = options.dryRun === true;
  const warnings = [];
  const allShops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const shops = yandexStockShops(allShops);
  const missingCampaignShops = allShops.filter((shop) => !shop.campaignId);
  if (missingCampaignShops.length) {
    warnings.push(yandexMissingStockCampaignWarning(missingCampaignShops.length));
  }
  const rows = (Array.isArray(products) ? products : [])
    .filter((product) => !isYandexSmallVolumeBlocked(product))
    .map((product) => ({
      id: product.id,
      offerId: cleanText(product.offerId || product.offer_id),
      productId: cleanText(product.productId || product.product_id),
      stock: pickOzonProductStockForYandex(product),
    }))
    .filter((row) => row.offerId);
  if (!rows.length) {
    return { ok: true, dryRun, sent: 0, failed: 0, skipped: 0, warnings, results: [] };
  }
  if (!shops.length) {
    const detail = warnings.length ? warnings.join(" ") : "Yandex Market не настроен для остатков.";
    return {
      ok: false,
      dryRun,
      sent: 0,
      failed: rows.length,
      skipped: 0,
      planned: rows.length,
      warnings: warnings.length ? warnings : [detail],
      results: rows.map((row) => ({
        stage: "stock",
        offerId: row.offerId,
        stock: row.stock,
        ok: false,
        error: detail,
      })),
    };
  }

  let existingOfferIds = options.existingOfferIds instanceof Set
    ? new Set(Array.from(options.existingOfferIds).map((id) => cleanText(id).toLowerCase()).filter(Boolean))
    : null;
  if (!existingOfferIds) {
    try {
      existingOfferIds = await getExistingYandexOfferIdSet(rows.map((row) => row.offerId));
    } catch (error) {
      const label = error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие артикулы (${label})`);
      existingOfferIds = getLocalYandexExportedOfferIdSet(options.warehouseProducts || products);
      if (existingOfferIds.size) warnings.push(`Yandex: использую локальный каталог для остатков (${existingOfferIds.size} SKU).`);
    }
  }

  const selected = rows.filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  if (dryRun) {
    return { ok: true, dryRun, sent: 0, skipped: rows.length - selected.length, planned: selected.length, warnings, results: selected };
  }
  const restoreActions = await restoreYandexArchivedStocks(selected, allShops);
  const restoreFailed = restoreActions.filter((item) => !item.ok);
  if (restoreFailed.length) {
    warnings.push(`Yandex: не удалось разархивировать ${restoreFailed.length} карточек перед отправкой остатков`);
  }

  const failedStockWarningKeys = new Set();
  for (const shop of shops) {
    const stockChunkSize = Math.max(1, Math.min(500, Number(process.env.YANDEX_STOCK_CHUNK_SIZE || 100) || 100));
    for (const chunk of chunkArray(selected, stockChunkSize)) {
      if (!chunk.length) continue;
      const stockResult = await sendYandexStockRowsWithFallback(shop, chunk);
      results.push(...stockResult.results);
      if (stockResult.failed > 0) {
        const label = stockResult.fallbackError || "ошибка API";
        const warningKey = `${shop.id || shop.name}:${label}`;
        if (!failedStockWarningKeys.has(warningKey)) {
          failedStockWarningKeys.add(warningKey);
          warnings.push(`Yandex «${shop.name || shop.id}»: остатки не отправлены (${label})`);
        }
      }
    }
  }

  const failed = results.filter((item) => item.ok === false).length;
  const sent = results.reduce((total, item) => total + Number(item.sent || 0), 0);
  const partial = failed > 0 && sent > 0;
  return {
    ok: failed === 0,
    partial,
    dryRun,
    sent,
    failed,
    skipped: rows.length - selected.length,
    planned: selected.length,
    warnings,
    results,
  };
}

async function sendYandexStocksForExportedOzonProducts(products = [], options = {}) {
  const warnings = [];
  const allShops = Array.isArray(options.stockShops) && options.stockShops.length
    ? options.stockShops
    : getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const shops = yandexStockShops(allShops);
  const missingCampaignShops = allShops.filter((shop) => shop.apiKey && shop.businessId && !shop.campaignId);
  if (missingCampaignShops.length) {
    warnings.push(yandexMissingStockCampaignWarning(missingCampaignShops.length));
  }
  const existingOfferIds = options.existingOfferIds instanceof Set ? options.existingOfferIds : new Set();
  const rows = (Array.isArray(products) ? products : [])
    .map((product) => ({
      offerId: cleanText(product.offerId || product.offer_id),
      stock: pickOzonProductStockForYandex(product),
    }))
    .filter((row) => row.offerId && existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  if (!rows.length) {
    return { ok: true, sent: 0, failed: 0, skipped: Math.max(0, (products || []).length - rows.length), warnings, results };
  }
  if (!shops.length) {
    const detail = warnings.length ? warnings.join(" ") : "Yandex Market не настроен для остатков.";
    return {
      ok: false,
      sent: 0,
      failed: rows.length,
      skipped: Math.max(0, (products || []).length - rows.length),
      planned: rows.length,
      warnings: warnings.length ? warnings : [detail],
      results: rows.map((row) => ({
        stage: "stock",
        offerId: row.offerId,
        stock: row.stock,
        ok: false,
        error: detail,
      })),
    };
  }

  const preUnarchive = await preUnarchiveYandexOffersBeforeStockSend(products, shops, options);
  if (preUnarchive.warnings.length) warnings.push(...preUnarchive.warnings);

  for (const shop of shops) {
    const stockChunkSize = Math.max(1, Math.min(500, Number(process.env.YANDEX_STOCK_CHUNK_SIZE || 100) || 100));
    for (const chunk of chunkArray(rows, stockChunkSize)) {
      if (!chunk.length) continue;
      const stockResult = await sendYandexStockRowsWithFallback(shop, chunk);
      results.push(...stockResult.results);
      if (stockResult.failed > 0) {
        const label = stockResult.fallbackError || "ошибка API";
        warnings.push(`Yandex «${shop.name || shop.id}»: остатки не отправлены (${label})`);
      }
    }
  }

  const failed = results.filter((item) => !item.ok).length;
  const sent = results.reduce((total, item) => total + Number(item.sent || (item.ok && item.stage === "stock" ? 1 : 0)), 0);
  return {
    ok: warnings.length === 0 && failed === 0,
    sent,
    failed,
    skipped: Math.max(0, (products || []).length - rows.length),
    planned: rows.length,
    warnings,
    results,
  };
}


