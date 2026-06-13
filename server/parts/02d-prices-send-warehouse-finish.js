  const stockActions = await sendTargetStocksToMarketplace(stockItems);

  const warehouse = await readWarehouse();
  const successIds = new Set(items
    .filter((item) => cleanText(item.marketplace).toLowerCase() !== "ozon" || ozonVerifiedById.has(String(item.id)))
    .map((item) => item.id));
  for (const failedItem of failed) successIds.delete(failedItem.id);
  const postgresPriceHistoryRows = [];
  const touchedProductIds = new Set();
  for (const item of items) {
    const product = warehouse.products.find((entry) => entry.id === item.id);
    if (!product) continue;
    touchedProductIds.add(product.id);
    const success = successIds.has(item.id);
    const failedEntryForItem = failed.find((entry) => entry.id === item.id);
    const delayedByLimitForItem = failedEntryForItem ? isOzonPerItemPriceLimitError({ message: failedEntryForItem.error }) : false;
    const oldPriceAdjustedForItem = failedEntryForItem ? needsOzonOldPriceEscalation({ message: failedEntryForItem.error }) : false;
    const ozonVerification = item.marketplace === "ozon" ? ozonVerifiedById.get(String(item.id)) : null;
    const verificationNotApplied = failedEntryForItem?.errorCode === "ozon_price_not_applied";
    const sendStatus = success ? "success" : (delayedByLimitForItem ? "delayed" : (oldPriceAdjustedForItem ? "pending" : "failed"));
    const retryNextAt = failedEntryForItem
      ? new Date(new Date(sentAt).getTime() + priceRetryDelayMs(Number(failedEntryForItem.attempts || 1), { message: failedEntryForItem.error })).toISOString()
      : null;
    const applyStatus = success
      ? (item.marketplace === "ozon" ? "verified" : "api_accepted")
      : (verificationNotApplied ? "ozon_price_not_applied" : (delayedByLimitForItem ? "ozon_price_delayed" : (oldPriceAdjustedForItem ? "in_retry" : "api_error")));
    const verifiedPrice = success && item.marketplace === "ozon"
      ? roundPrice(ozonVerification?.verifiedPrice || item.price)
      : (success && item.marketplace === "yandex" ? roundPrice(item.price) : null);
    item.priceApplyStatus = applyStatus;
    item.lastRequestedPrice = roundPrice(item.price);
    item.lastPriceRequestAt = sentAt;
    item.lastVerifiedPrice = verifiedPrice;
    item.lastPriceVerifiedAt = verifiedPrice ? (ozonVerification?.verifiedAt || sentAt) : null;
    item.verificationAttempts = Number(ozonVerification?.verificationAttempts || failedEntryForItem?.verificationAttempts || 0) || 0;
    if (success) {
      const sentPrice = verifiedPrice || roundPrice(item.price);
      product.marketplacePrice = sentPrice;
      // Keep the cached cabinet price in sync: the postgres column maps from
      // raw.currentPrice FIRST (marketplacePrice is only a fallback), so without this
      // the column stays stale and the sweep re-sends the same prices forever.
      product.currentPrice = sentPrice;
      // Also advance targetPrice to the price we just sent: productToPostgresData derives
      // the target_price column from normalized.targetPrice (nextPrice is a computed,
      // non-persisted field — see lib/computed-product-fields.js), so without this the
      // column stays at its pre-send value while current_price jumps to sentPrice. That
      // makes current_price <> target_price true again right after a successful send,
      // and price_sweep re-queues the same SKU forever (price oscillation bug class).
      product.targetPrice = sentPrice;
      if (item.marketplace === "ozon") {
        product.ozon = {
          ...(product.ozon || {}),
          price: sentPrice,
        };
      } else if (item.marketplace === "yandex") {
        product.yandex = {
          ...(product.yandex || {}),
          offerId: product.yandex?.offerId || item.offerId,
          price: sentPrice,
        };
      }
    }
    product.priceHistory = Array.isArray(product.priceHistory) ? product.priceHistory : [];
    const previous = product.priceHistory[product.priceHistory.length - 1] || null;
    const reasons = [];
    if (previous?.supplierArticle && previous.supplierArticle !== (item.supplier?.article || null)) reasons.push("смена поставщика");
    if (Number(previous?.usdRate || 0) !== Number(preview.usdRate || 0)) reasons.push("изменение курса");
    if (Number(previous?.usdPrice || 0) !== Number(item.supplier?.price || 0)) reasons.push("изменение прайса поставщика");
    if (!reasons.length) reasons.push("регулярный пересчет");
    const historyEntry = {
      at: sentAt,
      marketplace: item.marketplace,
      target: item.target,
      offerId: item.offerId,
      oldPrice: item.oldPrice || null,
      newPrice: roundPrice(item.price),
      markup: item.markup || null,
      supplierName: item.supplier?.partnerName || item.supplier?.supplierName || null,
      supplierArticle: item.supplier?.article || null,
      usdPrice: item.supplier?.price || null,
      usdRate: Number(preview.usdRate || 0) || null,
      reason: reasons.join(", "),
      status: sendStatus === "pending" ? "pending" : (success ? "success" : (delayedByLimitForItem ? "delayed" : "error")),
      error: success ? null : (failedEntryForItem?.error || "send_failed"),
    };
    const duplicateLocalHistory = product.priceHistory
      .slice(-20)
      .some((entry) => isDuplicatePriceHistoryEntry(entry, historyEntry, { now: new Date(sentAt) }));
    if (!duplicateLocalHistory) product.priceHistory.push(historyEntry);
    product.priceHistory = product.priceHistory.slice(-100);
    postgresPriceHistoryRows.push({
      productId: item.id,
      marketplace: item.marketplace,
      target: item.target,
      offerId: item.offerId,
      oldPrice: item.oldPrice || null,
      newPrice: roundPrice(item.price),
      status: sendStatus,
      error: historyEntry.error || "",
      at: sentAt,
    });
    const lastPriceSendBase = {
      status: sendStatus === "failed" ? "error" : sendStatus,
      at: sentAt,
      requestedPrice: roundPrice(item.price),
      lastRequestedPrice: roundPrice(item.price),
      lastPriceRequestAt: sentAt,
      lastVerifiedPrice: verifiedPrice,
      lastPriceVerifiedAt: verifiedPrice ? (ozonVerification?.verifiedAt || sentAt) : null,
      verifiedPrice,
      verifiedAt: verifiedPrice ? (ozonVerification?.verifiedAt || sentAt) : null,
      verificationAttempts: item.verificationAttempts,
      priceApplyStatus: applyStatus,
      applyStatus,
      priceIntentId,
      sourceEvent: sourceEvent || reason,
      cabinetPriceAtSend: Number(item.oldPrice || 0) || null,
      detail: failedEntryForItem ? failedEntryForItem.error : "ok",
      nextRetryAt: failedEntryForItem ? retryNextAt : null,
    };
    if (item.marketplace === "ozon") {
      product.lastOzonPriceSend = {
        ...lastPriceSendBase,
        oldPriceForRetry: oldPriceAdjustedForItem ? resolveOzonOldPrice(roundPrice(item.price), item) : null,
        detail: oldPriceAdjustedForItem
          ? "Ozon rejected old_price; old_price adjusted to 120% and retry queued."
          : lastPriceSendBase.detail,
      };
    } else if (item.marketplace === "yandex") {
      product.lastYandexPriceSend = {
        ...lastPriceSendBase,
      };
    }
  }
  for (const action of stockActions) {
    const product = warehouse.products.find((entry) => entry.id === action.id);
    if (!product) continue;
    touchedProductIds.add(product.id);
    product.lastStockSend = marketplaceCommandFromAction(action, product, sentAt);
    if (!action.ok) continue;
    const stock = Math.max(0, Math.round(Number(action.stock || 0)));
    const marketplaceState = {
      ...(product.marketplaceState || {}),
      stock,
    };
    if (product.marketplace === "yandex" && stock > 0) {
      marketplaceState.code = "active";
      marketplaceState.status = "active";
      marketplaceState.archived = false;
      product.status = "active";
      product.archived = false;
    }
    product.marketplaceState = {
      ...marketplaceState,
    };
  }
  if (touchedProductIds.size) {
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => touchedProductIds.has(product.id)),
      { reason: "warehouse_price_stock_send", writeLinks: false },
    );
  }
  appendPriceHistoryRows(postgresPriceHistoryRows).catch((error) => logger.warn("price history background append failed", { detail: error?.message || String(error) }));

  const failedQueued = failed.map((item) => buildPriceRetryItem({
    ...item,
    queueKey: `${item.id}:${item.target}`,
    queuedAt: sentAt,
  }, { message: item.error }, new Date(sentAt)));
  const merged = [...(queueState.items || []), ...delayedQueueUpdates, ...failedQueued];
  const deduped = Array.from(new Map(merged.map((item) => [priceRetryQueueKey(item), item])).values()).slice(0, 5000);
  if (failedQueued.length || delayedQueueUpdates.length) await writePriceRetryQueue({ items: deduped });
  schedulePriceRetryItems([...failedQueued, ...delayedQueueUpdates]);

  const stats = warehousePriceMarketplaceStats(items, failed, skipped);
  updateSalesAutomationFromPriceResult({ items, failed, skipped, stockActions, sentAt })
    .catch((error) => logger.warn("sales automation state background update failed", { detail: error?.message || String(error) }));
  return {
    ok: true,
    selected: selected.length,
    sent: items.length - failed.length,
    failed: failed.length,
    stockSent: stockActions.filter((item) => item.ok).length,
    stockFailed: stockActions.filter((item) => !item.ok).length,
    queued: deduped.length,
    delayed: delayedQueueUpdates.length,
    skipped,
    failedItems: failed,
    results,
    stockActions,
    ...stats,
  };
}


