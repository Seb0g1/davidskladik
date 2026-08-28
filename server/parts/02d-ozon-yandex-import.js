async function runOzonYandexImportSend(payload = {}, auditRequest = { session: { username: "system", role: "admin" } }) {
  if (payload?.confirmed !== true) {
    const error = new Error("Yandex import requires confirmed=true.");
    error.statusCode = 400;
    throw error;
  }
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const sendLimit = Math.max(1, Math.min(yandexImportSendLimit, Number(payload?.sendLimit || yandexImportSendLimit) || yandexImportSendLimit));
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) {
    const error = new Error("Yandex Market is not configured.");
    error.statusCode = 400;
    throw error;
  }

  const warehouse = await readWarehouse();
  const products = (warehouse.products || [])
    .filter((product) => product.marketplace === "ozon" && product.target === "ozon")
    .slice(0, limit);
  const initialRows = products.map((product) => buildOzonYandexImportCandidate(product));
  const candidateOfferIds = initialRows
    .filter((row) => !row.blockReasons?.length && row.yandexReady)
    .map((row) => cleanText(row.offerId))
    .filter(Boolean);
  const warnings = [];
  const yandexExistingOfferIds = await getKnownYandexExistingOfferIds(candidateOfferIds, {
    products: warehouse.products || [],
    warnings,
    allowCatalogRefresh: false,
    allowDirectCheck: false,
  });
  const rows = products.map((product) => buildOzonYandexImportCandidate(product, { yandexExistingOfferIds }));
  const eligibleRows = rows.filter((row) => row.eligible);
  const selectedRows = eligibleRows.slice(0, sendLimit);
  const selectedIds = new Set(selectedRows.map((row) => row.id));
  const productsById = new Map(products.map((product) => [product.id, product]));
  const selectedProducts = selectedRows.map((row) => productsById.get(row.id)).filter(Boolean);
  const offers = selectedProducts
    .map((product) => buildYandexOfferMapping(normalizeWarehouseProduct(product)).offer)
    .filter((offer) => offer?.offerId);

  const cardResults = [];
  for (const shop of shops) {
    const sent = await sendYandexOfferMappings(shop, offers);
    cardResults.push(...sent.map((item) => ({
      ...item,
      stage: "card",
      target: shop.id,
      targetName: shop.name || "Yandex Market",
    })));
  }

  const failedRows = cardResults.filter((item) => !item.ok);
  const sentCount = cardResults.filter((item) => item.ok).length;
  const skippedExisting = rows.filter((row) => row.existingInYandex).length;
  const skippedBlocked = rows.filter((row) => row.blockReasons?.length).length;
  const skippedMissing = rows.filter((row) => !row.yandexReady).length;
  const sentOfferIds = new Set(cardResults
    .filter((item) => item.ok)
    .map((item) => cleanText(item.offerId).toLowerCase())
    .filter(Boolean));
  if (sentOfferIds.size) {
    writeYandexExistingOfferIdCache(new Set([...yandexExistingOfferIds, ...sentOfferIds]))
      .catch((error) => logger.warn("write Yandex existing offer cache failed", { detail: error?.message || String(error) }));
  }

  const exportedProducts = selectedProducts
    .filter((product) => sentOfferIds.has(cleanText(product.offerId).toLowerCase()));
  const priceStage = exportedProducts.length
    ? await sendYandexPricesFromOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds, warehouse })
    : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };
  const stockStage = exportedProducts.length
    ? await sendYandexStocksForExportedOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds })
    : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };

  // Cards were accepted by YM but not yet PUBLISHED — enqueue for stock retry later.
  if (sentOfferIds.size) {
    const stockSentIds = new Set((Array.isArray(stockStage.results) ? stockStage.results : [])
      .filter((r) => r.ok)
      .map((r) => cleanText(r.offerId).toLowerCase())
      .filter(Boolean));
    const pendingOfferIds = Array.from(sentOfferIds).filter((id) => !stockSentIds.has(id));
    if (pendingOfferIds.length) {
      const pendingProducts = exportedProducts.filter((p) => pendingOfferIds.includes(cleanText(p.offerId).toLowerCase()));
      // Build one enqueue call per shop; shopId is the first shop if multiple
      const shopId = shops.length ? shops[0].id : "";
      const itemsToQueue = pendingProducts.map((p) => ({ offerId: cleanText(p.offerId), stock: pickOzonProductStockForYandex(p) }));
      addYandexPendingStockItems(itemsToQueue.map((i) => i.offerId), { shopId, stock: itemsToQueue[0]?.stock || 5 })
        .catch((err) => logger.warn("addYandexPendingStockItems failed", { detail: err?.message || String(err) }));
    }
  }

  const stageWarnings = [
    ...warnings,
    ...(Array.isArray(priceStage.warnings) ? priceStage.warnings : []),
    ...(Array.isArray(stockStage.warnings) ? stockStage.warnings : []),
  ];
  const results = [
    ...cardResults,
    ...(Array.isArray(priceStage.results) ? priceStage.results : []),
    ...(Array.isArray(stockStage.results) ? stockStage.results : []),
  ];

  if (sentCount > 0) {
    const now = new Date().toISOString();
    const yandexProducts = [];
    const priceResultByTargetOffer = new Map((Array.isArray(priceStage.results) ? priceStage.results : [])
      .filter((item) => item.ok && item.target && item.offerId)
      .map((item) => [yandexPriceUpdateResultKey(item), item]));
    for (const product of warehouse.products || []) {
      if (!selectedIds.has(product.id) || !sentOfferIds.has(cleanText(product.offerId).toLowerCase())) continue;
      product.exports = product.exports || {};
      const baseExportState = {
        status: "sent",
        targetName: shops.map((shop) => shop.name || shop.id).join(", "),
        sentAt: now,
        priceSent: Number(priceStage.sent || 0),
        stockSent: Number(stockStage.sent || 0),
      };
      for (const shop of shops) {
        const key = yandexTargetOfferKey(shop.id, product.offerId);
        const priceResult = priceResultByTargetOffer.get(key) || null;
        const exportState = {
          ...baseExportState,
          targetName: shop.name || shop.id,
          price: Number(priceResult?.price || 0) || undefined,
          stock: pickOzonProductStockForYandex(product),
          markupCoefficient: Number(priceResult?.markupCoefficient || 0) || undefined,
          priceSource: priceResult?.priceSource || undefined,
        };
        product.exports[shop.id] = exportState;
        yandexProducts.push(buildYandexWarehouseProductFromOzonExport(product, shop, exportState));
      }
      product.exports.yandex = baseExportState;
      product.updatedAt = now;
    }
    if (yandexProducts.length) {
      warehouse.products = mergeProducts(warehouse.products || [], yandexProducts);
    }
    await writeWarehouse(warehouse);
  }

  const responsePayload = {
    ok: failedRows.length === 0 && Number(priceStage.failed || 0) === 0 && Number(stockStage.failed || 0) === 0,
    generatedAt: new Date().toISOString(),
    limit,
    sendLimit,
    targets: shops.map((shop) => ({ id: shop.id, name: shop.name, businessId: shop.businessId })),
    planned: eligibleRows.length,
    plannedNow: selectedRows.length,
    sent: sentCount,
    failed: failedRows.length,
    priceSent: Number(priceStage.sent || 0),
    priceFailed: Number(priceStage.failed || 0),
    priceSkipped: Number(priceStage.skipped || 0),
    priceSkippedNoPrice: Number(priceStage.skippedNoPrice || 0),
    stockSent: Number(stockStage.sent || 0),
    stockFailed: Number(stockStage.failed || 0),
    stockSkipped: Number(stockStage.skipped || 0),
    skippedExisting,
    skippedBlocked,
    skippedMissing,
    skippedByLimit: Math.max(0, eligibleRows.length - selectedRows.length),
    warnings: stageWarnings,
    results,
    summary: summarizeOzonYandexImportPreview(rows),
  };

  await appendAudit(auditRequest, "yandex.import.send", {
    entityType: "yandex_import",
    entityId: "ozon_to_yandex",
    limit,
    sendLimit,
    targets: responsePayload.targets,
    planned: eligibleRows.length,
    plannedNow: selectedRows.length,
    sent: sentCount,
    failed: failedRows.length,
    priceSent: responsePayload.priceSent,
    priceFailed: responsePayload.priceFailed,
    priceSkippedNoPrice: responsePayload.priceSkippedNoPrice,
    stockSent: responsePayload.stockSent,
    stockFailed: responsePayload.stockFailed,
    skippedExisting,
    skippedBlocked,
    skippedMissing,
    warnings: stageWarnings.slice(0, 100),
    failedOfferIds: results.filter((item) => !item.ok).map((item) => item.offerId).filter(Boolean).slice(0, 500),
    newValue: {
      planned: eligibleRows.length,
      plannedNow: selectedRows.length,
      sent: sentCount,
      failed: failedRows.length,
      priceSent: responsePayload.priceSent,
      priceFailed: responsePayload.priceFailed,
      priceSkippedNoPrice: responsePayload.priceSkippedNoPrice,
      stockSent: responsePayload.stockSent,
      stockFailed: responsePayload.stockFailed,
    },
  });

  return responsePayload;
}

