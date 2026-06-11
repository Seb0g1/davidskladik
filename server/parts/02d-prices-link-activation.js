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
        source: normalizedSourceEvent,
        sourceEvent: normalizedSourceEvent,
        requestedBy: requestMeta.username || requestMeta.user || "system",
      },
      // Below price pushes: bulk recovery jobs must not starve auto-price-push (prio 1-2).
      { priority: 4 },
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
      priority: 1,
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

