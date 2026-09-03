async function runLinkedProductActivationImmediate(productIds = [], sourceEvent = "link_change_immediate", requestMeta = {}) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : []).map(cleanText).filter(Boolean)));
  if (!ids.length) {
    return {
      activationQueued: false,
      activationImmediate: true,
      affectedProductIds: [],
      ok: true,
    };
  }
  const normalizedSourceEvent = cleanText(sourceEvent) || "link_change_immediate";
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  invalidateWarehouseViewCache();

  const products = await buildFreshWarehouseProducts(ids, {
    refreshPrices: true,
    livePriceMaster: true,
    batchPriceMaster: true,
    priceMasterTimeoutMs: autoPricePmTimeoutMs,
  });
  const withLinks = products.filter((product) => productHasSupplierLinks(product));
  const withoutSupplier = withLinks.filter((product) => !product.selectedSupplier);
  const withSupplier = withLinks.filter((product) => product.selectedSupplier);

  let noSupplierResult = null;
  if (withoutSupplier.length) {
    noSupplierResult = await runNoSupplierMarketplaceAutomation(
      { products: withoutSupplier },
      {
        productIds: withoutSupplier.map((product) => product.id),
        includeNoLinks: false,
        source: "linked_activation_immediate",
        skipLinkedGrace: true,
      },
    );
  }

  let recoveryResult = null;
  const recoveryCandidates = pickImmediateLinkRecoveryCandidates(withSupplier);
  if (recoveryCandidates.length) {
    recoveryResult = await runSupplierRecoveryAutomation(
      { products: recoveryCandidates },
      {
        productIds: recoveryCandidates.map((product) => product.id),
        source: normalizedSourceEvent,
        sourceEvent: normalizedSourceEvent,
        force: true,
        deferOzonUnarchive: true,
      },
    );
  }

  // For stock-only products (no price, supplier provides availability only) that have
  // no marketplace stock yet, send stock directly. These are excluded from priceIds below and
  // may not be caught by pickImmediateLinkRecoveryCandidates when marketplaceState.stock is
  // absent or positive-but-stale. This is a targeted fast path: recovery automation handles
  // the general case, but stock-only products need a dedicated stock push on link save.
  const stockOnlyNeedsStock = withSupplier.filter(
    (product) => warehouseProductUsesStockOnlyPricing(product)
      && !productLooksArchived(product)
      && !product.hasSnoozedLinks
      && !marketplaceHasPositiveStock(product)
      && !recoveryCandidates.some((c) => String(c.id) === String(product.id)),
  );
  if (stockOnlyNeedsStock.length) {
    const defaultStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
    const toSend = stockOnlyNeedsStock.map((p) => ({
      ...p,
      targetStock: Math.max(defaultStock, Math.round(Number(p.targetStock || 0)) || defaultStock),
    }));
    await restoreStocksOnMarketplaces(toSend).catch((err) => {
      logger.warn("link_activation stock-only direct restore failed", { sourceEvent: normalizedSourceEvent, detail: err?.message || String(err) });
    });
  }

  const priceIds = withSupplier.filter((product) => !warehouseProductUsesStockOnlyPricing(product)).map((product) => product.id);
  const priceResult = priceIds.length
    ? await sendWarehousePrices({
      productIds: priceIds,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      reason: normalizedSourceEvent,
      sourceEvent: normalizedSourceEvent,
      marketplace: "all",
    })
    : { sent: 0, failed: 0, skipped: [] };

  return {
    activationQueued: true,
    activationImmediate: true,
    activationPending: false,
    recoveryQueued: false,
    priceIntentId: priceResult.priceIntentId || null,
    affectedProductIds: ids,
    price: priceResult,
    recovery: recoveryResult,
    noSupplier: noSupplierResult,
    ok: true,
    requestedBy: requestMeta.username || requestMeta.user || "system",
  };
}

async function queueLinkedProductActivation(productIds = [], sourceEvent = "link_change_activate_marketplace", requestMeta = {}) {
  const requestedIds = Array.from(new Set((Array.isArray(productIds) ? productIds : [])
    .map((id) => cleanText(id))
    .filter(Boolean)));
  if (!requestedIds.length) {
    return {
      activationQueued: false,
      recoveryQueued: false,
      priceIntentId: null,
      affectedProductIds: requestedIds,
      queued: 0,
      queuedBatches: 0,
    };
  }

  const normalizedSourceEvent = cleanText(sourceEvent) || "link_change_activate_marketplace";
  try {
    priceMasterLinkLookupCache.clear();
    priceMasterSearchCache.clear();
    invalidateWarehouseViewCache();

    const affectedProducts = (await hydrateWarehouseProductsForIds(requestedIds, { expandGroups: true }))
      .filter((product) => product?.id);
    const affectedProductIds = Array.from(new Set(affectedProducts.map((product) => String(product.id))));
    const linkedAffectedIds = affectedProducts
      .filter((product) => Array.isArray(product.links) && product.links.length)
      .map((product) => String(product.id));
    const recoveryIds = affectedProductIds;
    const priceIds = linkedAffectedIds.length ? Array.from(new Set(linkedAffectedIds)) : affectedProductIds;
    const skipImmediateReplay = /(?:^|_)unchanged$/.test(normalizedSourceEvent);

    if (
      !skipImmediateReplay
      && requestMeta.immediate === true
      && affectedProductIds.length <= linkedActivationImmediateMaxScope
      && !isApiServer
    ) {
      if (requestMeta.awaitImmediate === true) {
        return await runLinkedProductActivationImmediate(affectedProductIds, normalizedSourceEvent, requestMeta);
      }
      void runLinkedProductActivationImmediate(affectedProductIds, normalizedSourceEvent, requestMeta).catch((error) => {
        logger.warn("async linked product activation failed", {
          sourceEvent: normalizedSourceEvent,
          detail: error?.message || String(error),
          count: affectedProductIds.length,
        });
      });
      return {
        activationQueued: true,
        activationImmediate: true,
        activationPending: true,
        recoveryQueued: false,
        priceIntentId: null,
        affectedProductIds,
        queued: 0,
        queuedBatches: 0,
      };
    }

    if (!marketplaceJobsCanEnqueue() && isApiServer) {
      return {
        activationQueued: false,
        recoveryQueued: false,
        priceIntentId: null,
        affectedProductIds,
        queued: 0,
        queuedBatches: 0,
        queueUnavailable: true,
        error: "background_queue_unavailable",
        message: "Фоновая очередь недоступна. Привязка сохранена, но цена и остаток не отправятся автоматически.",
      };
    }

    if (!marketplaceJobsCanEnqueue() && backgroundMarketplaceJobsBlocked()) {
      if (
        !skipImmediateReplay
        && affectedProductIds.length <= linkedActivationImmediateMaxScope
        && !isApiServer
      ) {
        if (requestMeta.awaitImmediate === true) {
          return await runLinkedProductActivationImmediate(affectedProductIds, normalizedSourceEvent, requestMeta);
        }
        void runLinkedProductActivationImmediate(affectedProductIds, normalizedSourceEvent, requestMeta).catch((error) => {
          logger.warn("async linked product activation failed (background disabled fallback)", {
            sourceEvent: normalizedSourceEvent,
            detail: error?.message || String(error),
            count: affectedProductIds.length,
          });
        });
        return {
          activationQueued: true,
          activationImmediate: true,
          activationPending: true,
          recoveryQueued: false,
          priceIntentId: null,
          affectedProductIds,
          queued: 0,
          queuedBatches: 0,
          disabled: true,
          fallbackImmediate: true,
        };
      }
      return {
        activationQueued: false,
        recoveryQueued: false,
        priceIntentId: null,
        affectedProductIds,
        queued: 0,
        queuedBatches: 0,
        disabled: true,
      };
    }

    if (!recoveryIds.length) {
      return {
        activationQueued: false,
        recoveryQueued: false,
        priceIntentId: null,
        affectedProductIds,
        queued: 0,
        queuedBatches: 0,
        reason: "no_affected_products",
      };
    }

    const recoveryQueue = enqueueMarketplaceJobAccepted(
      "supplier-recovery-automation",
      {
        productIds: recoveryIds,
        force: true,
        // Bypass our self-imposed daily counter: user-triggered link saves should unarchive
        // immediately. The background queue processes slowly (100/day limit exhausted at 03:00
        // MSK), so without this flag new link activations would wait hours or days. Ozon's
        // actual API limit is still honoured — a rejection falls back to the queue.
        forceOzonDailyLimit: true,
        source: normalizedSourceEvent,
        sourceEvent: normalizedSourceEvent,
        requestedBy: requestMeta.username || requestMeta.user || "system",
      },
      // Below price pushes: bulk recovery jobs must not starve auto-price-push (prio 1-2).
      { priority: QUEUE_PRIORITY.UNARCHIVE },
    ).catch((error) => {
      logger.warn("linked product activation recovery queue failed", {
        sourceEvent: normalizedSourceEvent,
        detail: error?.message || String(error),
        count: recoveryIds.length,
      });
      return { accepted: false, error: error?.message || String(error) };
    });

    const priceQueue = await queueAuthoritativePriceReprice({
      productIds: priceIds,
      marketplace: "all",
      reason: normalizedSourceEvent,
      sourceEvent: normalizedSourceEvent,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      priority: QUEUE_PRIORITY.PRICE_IMMEDIATE,
    });
    const recoveryResult = await recoveryQueue;
    const activationAccepted = Boolean(recoveryResult?.accepted || priceQueue.accepted);
    const queueError = recoveryResult?.queueError || recoveryResult?.error || priceQueue.queueError || null;

    if (!activationAccepted && isApiServer) {
      return {
        activationQueued: false,
        recoveryQueued: false,
        priceIntentId: priceQueue.priceIntentId || null,
        affectedProductIds,
        queued: priceQueue.queued || 0,
        queuedBatches: priceQueue.queuedBatches || 0,
        queueUnavailable: true,
        error: "background_queue_unavailable",
        message: "Фоновая очередь недоступна. Привязка сохранена, но цена и остаток не отправятся автоматически.",
        recoveryQueueError: queueError,
      };
    }

    return {
      activationQueued: activationAccepted,
      recoveryQueued: Boolean(recoveryResult?.accepted),
      priceIntentId: priceQueue.priceIntentId || null,
      affectedProductIds,
      queued: priceQueue.queued || 0,
      queuedBatches: priceQueue.queuedBatches || 0,
      recoveryInlineBackground: Boolean(recoveryResult?.inlineBackground),
      recoveryQueueError: queueError,
    };
  } catch (error) {
    logger.warn("linked product activation failed", {
      sourceEvent: normalizedSourceEvent,
      detail: error?.message || String(error),
      count: requestedIds.length,
    });
    return {
      activationQueued: false,
      recoveryQueued: false,
      priceIntentId: null,
      affectedProductIds: requestedIds,
      queued: 0,
      queuedBatches: 0,
      error: error?.message || String(error),
    };
  }
}

// Targeted stock sync triggered directly after a link create/update or delete.
// Runs asynchronously — never blocks the HTTP response. Handles two cases:
//   1. Products with a resolved supplier and zero (or missing) marketplace stock →
//      immediately restores stock via restoreStocksOnMarketplaces so the marketplace
//      shows the correct available quantity without waiting for the BullMQ recovery job.
//   2. Products with a resolved supplier and a positive targetStock that differs from
//      the current marketplace stock → pushes the correct targetStock via
//      sendTargetStocksToMarketplace (covers the "re-link after supplier change" case).
// This is a belt-and-suspenders safety net on top of the existing queueLinkedProductActivation
// flow: the BullMQ supplier-recovery-automation job handles the general case, but there are
// edge cases where it may run slightly later or be deduplicated — this direct push fills the gap.
async function triggerLinkedProductStockSync(productIds = [], sourceEvent = "link_stock_sync") {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : []).map((id) => cleanText(id)).filter(Boolean)));
  if (!ids.length) return;
  try {
    const products = await buildFreshWarehouseProducts(ids, {
      refreshPrices: false,
      livePriceMaster: true,
      batchPriceMaster: true,
      priceMasterTimeoutMs: autoPricePmTimeoutMs,
    });
    const defaultStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
    // Products that need a stock restore: have a supplier, are not archived, not snoozed,
    // and either have zero marketplace stock or a zero targetStock (first-time activation).
    const needsRestore = products.filter(
      (product) =>
        product?.id
        && product.hasLinks
        && product.selectedSupplier
        && !productLooksArchived(product)
        && !product.hasSnoozedLinks
        && (!marketplaceHasPositiveStock(product) || Math.round(Number(product.targetStock || 0)) <= 0),
    ).map((product) => ({
      ...product,
      targetStock: Math.max(defaultStock, Math.round(Number(product.targetStock || 0)) || defaultStock),
    }));
    if (needsRestore.length) {
      await restoreStocksOnMarketplaces(needsRestore).catch((err) => {
        logger.warn("triggerLinkedProductStockSync restore failed", {
          sourceEvent,
          count: needsRestore.length,
          detail: err?.message || String(err),
        });
      });
      logger.info("triggerLinkedProductStockSync restore sent", {
        sourceEvent,
        count: needsRestore.length,
        productIds: needsRestore.map((p) => p.id),
      });
      return;
    }
    // Products with a positive targetStock that differs from the marketplace — push targetStock.
    const needsTargetPush = pickTargetStockSendProducts(
      products.filter(
        (product) =>
          product?.id
          && product.hasLinks
          && product.selectedSupplier
          && !productLooksArchived(product)
          && !product.hasSnoozedLinks,
      ),
    );
    if (needsTargetPush.length) {
      await sendTargetStocksToMarketplace(needsTargetPush).catch((err) => {
        logger.warn("triggerLinkedProductStockSync target push failed", {
          sourceEvent,
          count: needsTargetPush.length,
          detail: err?.message || String(err),
        });
      });
      logger.info("triggerLinkedProductStockSync target push sent", {
        sourceEvent,
        count: needsTargetPush.length,
        productIds: needsTargetPush.map((p) => p.id),
      });
    }
  } catch (err) {
    logger.warn("triggerLinkedProductStockSync failed", {
      sourceEvent,
      ids,
      detail: err?.message || String(err),
    });
  }
}

