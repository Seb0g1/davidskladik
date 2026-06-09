function warehousePriceMarketplaceStats(items = [], failed = [], skipped = []) {
  const failedIds = new Set((Array.isArray(failed) ? failed : []).map((item) => String(item.id || item.productId || "")));
  const successItems = (Array.isArray(items) ? items : []).filter((item) => !failedIds.has(String(item.id || item.productId || "")));
  const count = (rows, marketplace) => rows.filter((item) => cleanText(item.marketplace).toLowerCase() === marketplace).length;
  return {
    ozonSent: count(successItems, "ozon"),
    ozonFailed: count(Array.isArray(failed) ? failed : [], "ozon"),
    ozonSkipped: count(Array.isArray(skipped) ? skipped : [], "ozon"),
    yandexSent: count(successItems, "yandex"),
    yandexFailed: count(Array.isArray(failed) ? failed : [], "yandex"),
    yandexSkipped: count(Array.isArray(skipped) ? skipped : [], "yandex"),
  };
}

function salesAutomationReason(value = "") {
  const reason = cleanText(value || "ok");
  const map = {
    no_pricemaster_link: "no_pricemaster_link",
    not_ready: "no_supplier",
    no_next_price: "no_price",
    ozon_price_delayed: "in_retry",
    ozon_unarchive_daily_limit_queued: "ozon_limit",
    stock_only_manual_price_missing: "stock_only_manual_price_missing",
    unchanged: "unchanged",
    unchanged_verified: "unchanged_verified",
    queued: "queued",
    api_accepted: "api_accepted",
    verification_pending: "verification_pending",
    verified: "ok",
    ozon_price_not_applied: "ozon_price_not_applied",
    pm_live_timeout: "pm_live_timeout",
    send_failed: "api_error",
  };
  return map[reason] || reason || "ok";
}

async function resolveSalesAutomationProductIds(prisma, rows = []) {
  const ids = Array.from(new Set((Array.isArray(rows) ? rows : [])
    .map((row) => cleanText(row.productId || row.id))
    .filter(Boolean)));
  const valid = new Set();
  if (!ids.length) return valid;
  for (const chunk of chunkArray(ids, 500)) {
    const found = await prisma.warehouseProduct.findMany({
      where: { id: { in: chunk } },
      select: { id: true },
    });
    for (const row of found) valid.add(row.id);
  }
  return valid;
}

async function upsertSalesAutomationSkuStates(rows = []) {
  if (!shouldUsePostgresStorage() || !rows.length) return { updated: 0, skipped: true };
  try {
    const prisma = getPrisma();
    let updated = 0;
    let droppedMissingProduct = 0;
    for (const batch of chunkArray(rows, 250)) {
      const validBatch = batch.filter((row) => cleanText(row.offerId));
      const validProductIds = await resolveSalesAutomationProductIds(prisma, validBatch);
      await prisma.$transaction(validBatch.map((row) => {
        const marketplace = cleanText(row.marketplace).toLowerCase() === "yandex" ? "yandex" : "ozon";
        const target = cleanText(row.target) || "default";
        const offerId = cleanText(row.offerId);
        const rawProductId = cleanText(row.productId || row.id) || null;
        const productId = rawProductId && validProductIds.has(rawProductId) ? rawProductId : null;
        if (rawProductId && !productId) droppedMissingProduct += 1;
        updated += 1;
        return prisma.salesAutomationSkuState.upsert({
          where: { marketplace_target_offerId: { marketplace, target, offerId } },
          create: {
            productId,
            marketplace,
            target,
            offerId,
            currentPrice: Number(row.currentPrice ?? row.oldPrice ?? 0) || null,
            targetPrice: Number(row.targetPrice ?? row.price ?? 0) || null,
            targetStock: Number(row.targetStock ?? row.stock ?? 0) || null,
            priceStatus: row.priceStatus || "pending",
            stockStatus: row.stockStatus || "pending",
            unarchiveStatus: row.unarchiveStatus || "pending",
            reason: salesAutomationReason(row.reason || "ok"),
            lastCalculatedAt: toDateOrNull(row.lastCalculatedAt) || new Date(),
            lastPriceSentAt: toDateOrNull(row.lastPriceSentAt),
            lastStockSentAt: toDateOrNull(row.lastStockSentAt),
            lastError: cleanText(row.lastError || row.error) || null,
            raw: row,
          },
          update: {
            productId,
            currentPrice: Number(row.currentPrice ?? row.oldPrice ?? 0) || null,
            targetPrice: Number(row.targetPrice ?? row.price ?? 0) || null,
            targetStock: Number(row.targetStock ?? row.stock ?? 0) || null,
            priceStatus: row.priceStatus || "pending",
            stockStatus: row.stockStatus || "pending",
            unarchiveStatus: row.unarchiveStatus || "pending",
            reason: salesAutomationReason(row.reason || "ok"),
            lastCalculatedAt: toDateOrNull(row.lastCalculatedAt) || new Date(),
            lastPriceSentAt: toDateOrNull(row.lastPriceSentAt),
            lastStockSentAt: toDateOrNull(row.lastStockSentAt),
            lastError: cleanText(row.lastError || row.error) || null,
            raw: row,
          },
        });
      }));
    }
    if (droppedMissingProduct) {
      logger.warn("sales automation skipped stale product ids", { droppedMissingProduct });
    }
    return { updated, droppedMissingProduct };
  } catch (error) {
    if (!jsonFallbackEnabled()) throw error;
    logger.warn("sales automation state upsert failed", { detail: error?.message || String(error) });
    return { updated: 0, error: error?.message || String(error) };
  }
}

async function updateSalesAutomationFromPriceResult({ items = [], failed = [], skipped = [], stockActions = [], sentAt = new Date().toISOString() } = {}) {
  const failedById = new Map((failed || []).map((item) => [cleanText(item.id || item.productId), item]));
  const stockById = new Map((stockActions || []).map((item) => [cleanText(item.id), item]));
  const rows = [];
  for (const item of items || []) {
    const failedItem = failedById.get(cleanText(item.id || item.productId));
    const stock = stockById.get(cleanText(item.id || item.productId));
    const delayedByLimit = failedItem ? isOzonPerItemPriceLimitError({ message: failedItem.error }) : false;
    const verificationNotApplied = failedItem?.errorCode === "ozon_price_not_applied";
    const applyStatus = item.priceApplyStatus
      || (failedItem
        ? (verificationNotApplied ? "ozon_price_not_applied" : (delayedByLimit ? "ozon_price_delayed" : "api_error"))
        : (item.marketplace === "ozon" ? "verified" : "api_accepted"));
    const reason = failedItem
      ? (verificationNotApplied ? "ozon_price_not_applied" : (delayedByLimit ? "in_retry" : "api_error"))
      : "ok";
    rows.push({
      ...item,
      productId: item.productId || item.id,
      currentPrice: item.oldPrice,
      targetPrice: item.price,
      targetStock: stock?.stock ?? null,
      priceStatus: failedItem ? (delayedByLimit ? "delayed" : "failed") : "success",
      stockStatus: stock ? (stock.ok ? "success" : "failed") : "pending",
      unarchiveStatus: stock?.queuedByDailyLimit ? "delayed" : "pending",
      reason,
      lastPriceSentAt: failedItem && !verificationNotApplied ? null : sentAt,
      lastStockSentAt: stock?.ok ? sentAt : null,
      lastError: failedItem?.error || stock?.error || "",
      lastCalculatedAt: sentAt,
      priceIntentId: item.priceIntentId || "",
      priceApplyStatus: applyStatus,
      lastRequestedPrice: item.lastRequestedPrice ?? roundPrice(item.price),
      lastVerifiedPrice: item.lastVerifiedPrice ?? null,
      lastPriceRequestAt: item.lastPriceRequestAt || sentAt,
      lastPriceVerifiedAt: item.lastPriceVerifiedAt || null,
      verificationAttempts: item.verificationAttempts || failedItem?.verificationAttempts || 0,
      selectedSupplier: item.supplier || item.selectedSupplier || null,
      supplierPurchasePrice: Number(item.supplier?.price || item.supplier?.supplierPrice || 0) || null,
      supplierPurchaseCurrency: cleanText(item.supplier?.priceCurrency || item.supplier?.currency) || null,
      effectiveFinalPrice: Number(item.supplier?.effectiveFinalPrice || item.supplier?.calculatedPrice || item.price || 0) || null,
      sourceEvent: item.sourceEvent || "",
      priceSource: item.priceSource || item.supplier?.priceSource || null,
    });
  }
  for (const row of skipped || []) {
    const normalizedReason = salesAutomationReason(row.reason);
    rows.push({
      ...row,
      productId: row.productId || row.id,
      priceStatus: normalizedReason === "unchanged_verified" || normalizedReason === "unchanged" ? "success" : (row.reason === "ozon_price_delayed" ? "delayed" : "pending"),
      stockStatus: "pending",
      unarchiveStatus: row.reason === "ozon_price_delayed" ? "delayed" : "pending",
      reason: row.reason,
      lastError: row.error || "",
      lastCalculatedAt: sentAt,
      priceIntentId: row.priceIntentId || "",
      priceApplyStatus: row.priceApplyStatus || normalizedReason,
      lastRequestedPrice: row.lastRequestedPrice ?? null,
      lastVerifiedPrice: row.lastVerifiedPrice ?? null,
      lastPriceRequestAt: row.lastPriceRequestAt || null,
      lastPriceVerifiedAt: row.lastPriceVerifiedAt || null,
      verificationAttempts: row.verificationAttempts || 0,
    });
  }
  return upsertSalesAutomationSkuStates(rows);
}

