function enqueueMarketplaceJobAccepted(name, data = {}, { priority = QUEUE_PRIORITY.DEFAULT } = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return Promise.resolve(null);
  if (marketplaceQueue) {
    return marketplaceQueue.add(name, data, {
      jobId: marketplaceJobId(name, data),
      priority,
      removeOnComplete: name === "auto-price-push" || name === "ozon-unarchive-queue-process" ? true : 2000,
      removeOnFail: 2000,
    }).catch((error) => {
      const queueError = error?.message || String(error);
      logger.warn("queue add failed", { name, detail: queueError, serverRole });
      if (isApiServer) {
        return { accepted: false, inlineBackground: false, queueError, queueUnavailable: true };
      }
      setTimeout(() => {
        processMarketplaceJob(name, data).catch((jobError) => {
          logger.warn("background marketplace job failed", { name, detail: jobError?.message || String(jobError) });
        });
      }, 0).unref?.();
      return { accepted: true, inlineBackground: true, queueError };
    });
  }
  if (!backgroundJobsEnabled || isApiServer) return Promise.resolve(null);
  setTimeout(() => {
    processMarketplaceJob(name, data).catch((error) => {
      logger.warn("background marketplace job failed", { name, detail: error?.message || String(error) });
    });
  }, 0).unref?.();
  return Promise.resolve({ accepted: true, inlineBackground: true });
}

async function queueAuthoritativePriceReprice({
  productIds,
  marketplace = "all",
  reason = "authoritative_reprice",
  sourceEvent = "",
  force = true,
  onlyChanged = false,
  refreshMarketplacePrices = true,
  livePriceMaster = true,
  verify = true,
  limit = 0,
  priority = QUEUE_PRIORITY.PRICE_IMMEDIATE,
  repriceAttempt = 0,
} = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") {
    return { ok: true, accepted: false, disabled: true, queued: 0, queuedBatches: 0 };
  }
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  invalidateWarehouseViewCache();
  const products = await readLinkedProductsForReprice({ productIds, marketplace, limit });
  const ids = products.map((product) => String(product.id)).filter(Boolean);
  const priceIntentId = crypto.randomUUID();
  if (!ids.length) {
    return { ok: true, accepted: true, priceIntentId, queued: 0, queuedBatches: 0, reason: "no_linked_products" };
  }
  const queuedStateProducts = products.length > 1000 ? products.slice(0, 1000) : products;
  await markSalesAutomationPriceQueued(queuedStateProducts, { priceIntentId, reason, marketplace, sourceEvent: sourceEvent || reason })
    .catch((error) => logger.warn("sales automation queued state update failed", { detail: error?.message || String(error), priceIntentId }));
  if (queuedStateProducts.length < products.length) {
    logger.info("sales automation queued state partially marked", {
      priceIntentId,
      marked: queuedStateProducts.length,
      queued: products.length,
    });
  }
  const batches = chunkArray(ids, authoritativeRepriceBatchSize);
  for (const batch of batches) {
    await enqueueMarketplaceJobAccepted(
      "auto-price-push",
      {
        productIds: batch,
        priceIntentId,
        usdRate: undefined,
        minDiffRub: 0,
        minDiffPct: 0,
        force: force === true,
        reason,
        sourceEvent: sourceEvent || reason,
        marketplace,
        onlyChanged: force === true ? false : onlyChanged === true,
        refreshMarketplacePrices: refreshMarketplacePrices !== false,
        livePriceMaster: livePriceMaster !== false,
        verify: verify !== false,
        repriceAttempt,
        priceMasterTimeoutMs: autoPricePmTimeoutMs,
      },
      { priority },
    );
  }
  return { ok: true, accepted: true, priceIntentId, queued: ids.length, queuedBatches: batches.length };
}

function pickImmediateLinkRecoveryCandidates(products = []) {
  return (Array.isArray(products) ? products : []).filter((product) => {
    if (!product?.id || !productHasSupplierLinks(product) || !product.selectedSupplier) return false;
    if (productLooksArchived(product) || marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true })) return true;
    // Zero stock with an active supplier means a prior deactivation left stock at 0 while the
    // marketplace code stayed "active" (Ozon/Yandex don't flip code on zero-stock without archive).
    // Restore immediately so the user doesn't have to wait for the next scheduled stock sweep.
    const marketplaceStock = Math.round(Number(product.marketplaceState?.stock || 0));
    const targetStock = Math.round(Number(product.targetStock || 0));
    return marketplaceStock <= 0 || targetStock <= 0;
  });
}

