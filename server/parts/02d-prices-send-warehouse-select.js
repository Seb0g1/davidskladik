async function sendWarehousePrices({
  productIds,
  usdRate,
  minDiffRub = 0,
  minDiffPct = 0,
  dryRun = false,
  force = false,
  refreshMarketplacePrices = false,
  marketplace = "all",
  onlyChanged = false,
  livePriceMaster = false,
  reason = "auto-price-push",
  limit,
  verify = true,
  priceIntentId = "",
  sourceEvent = "",
  repriceAttempt = 0,
  priceMasterTimeoutMs,
} = {}) {
  const ids = Array.isArray(productIds) ? new Set(productIds.map(String)) : null;
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const normalizedMarketplaceFilter = ["ozon", "yandex"].includes(marketplaceFilter) ? marketplaceFilter : "all";
  const normalizedLimit = Math.max(0, Math.round(Number(limit || 0) || 0));
  let preview = null;
  let selected = [];
  if (ids) {
    const settings = await readAppSettings();
    const rate = Number(settings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    preview = { usdRate: rate };
    selected = await buildFreshWarehouseProducts(Array.from(ids), {
      refreshPrices: Boolean(refreshMarketplacePrices),
      usdRate: rate,
      livePriceMaster: Boolean(livePriceMaster),
      batchPriceMaster: Boolean(livePriceMaster),
      priceMasterTimeoutMs: livePriceMaster ? (Number(priceMasterTimeoutMs || 0) || autoPricePmTimeoutMs) : undefined,
    });
  } else {
    if (livePriceMaster && !dryRun) {
      const authoritative = await queueAuthoritativePriceReprice({
        marketplace: normalizedMarketplaceFilter,
        reason,
        sourceEvent: sourceEvent || reason,
        force: force === true,
        onlyChanged,
        refreshMarketplacePrices,
        livePriceMaster,
        verify,
        limit: normalizedLimit,
        repriceAttempt,
      });
      return {
        ok: true,
        accepted: true,
        selected: authoritative.queued,
        sent: 0,
        failed: 0,
        queued: authoritative.queued,
        queuedBatches: authoritative.queuedBatches,
        priceIntentId: authoritative.priceIntentId,
        skipped: [],
        reason: "authoritative_reprice_queued",
      };
    }
    preview = await buildWarehouseView({ usdRate: Number(usdRate || 0) || undefined });
    selected = preview.products;
  }
  if (normalizedMarketplaceFilter !== "all") {
    selected = selected.filter((product) => cleanText(product.marketplace).toLowerCase() === normalizedMarketplaceFilter);
  }
  if (normalizedLimit > 0) selected = selected.slice(0, normalizedLimit);
  const skipped = [];
  const items = [];
  const stockItems = [];
  const sentAt = new Date().toISOString();
  const queueState = dryRun ? { items: [] } : await readPriceRetryQueue().catch((error) => {
    logger.warn("price retry queue read failed before price send", { detail: error?.message || String(error) });
    return { items: [] };
  });
  const delayedQueueUpdates = [];

  for (const product of selected) {
    if (!product.hasLinks) {
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason: "no_pricemaster_link", priceIntentId, priceApplyStatus: "no_pricemaster_link" });
      continue;
    }
    if (shouldSendTargetStockForProduct(product)) stockItems.push(product);
    if (!product.ready) {
      const reason = product.priceSource === "timeout"
        ? "pm_live_timeout"
        : (product.hasLinks && !product.selectedSupplier ? "no_supplier" : "not_ready");
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason, priceSource: product.priceSource || null, priceIntentId, priceApplyStatus: reason });
      continue;
    }
    if (warehouseProductUsesStockOnlyPricing(product)) {
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: "stock_only_excluded_from_price_push",
        supplier: product.selectedSupplier,
        priceIntentId,
        priceApplyStatus: "stock_only_excluded_from_price_push",
      });
      continue;
    }
    if (onlyChanged && !force && product.changed === false) {
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason: "unchanged_verified", priceIntentId, priceApplyStatus: "unchanged_verified" });
      continue;
    }
    const current = Number(product.currentPrice || 0);
    const nextValue = Number(product.nextPrice || 0);
    const skipDecision = shouldSkipWarehousePriceSend({
      currentPrice: current,
      nextPrice: nextValue,
      minDiffRub,
      minDiffPct,
      force,
    });
    if (skipDecision.skip) {
      const reasonCode = skipDecision.reason === "unchanged" ? "unchanged_verified" : skipDecision.reason;
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: reasonCode,
        diffRub: skipDecision.diffRub,
        diffPct: skipDecision.diffPct,
        priceIntentId,
        priceApplyStatus: reasonCode,
      });
      continue;
    }
    const lastOzonSend = product.marketplace === "ozon" ? product.lastOzonPriceSend : null;
    const lastOzonError = lastOzonSend ? (lastOzonSend.detail || lastOzonSend.error || "") : "";
    const quarantineRelease = lastOzonSend ? needsOzonOldPriceEscalation({ message: lastOzonError }) : false;
    const discountQuarantine = lastOzonSend ? isOzonPriceDiscountQuarantineError({ message: lastOzonError }) : false;
    const cabinetPrice = roundPrice(product.currentPrice || 0);
    const finalTargetPrice = roundPrice(product.nextPrice);
    const stepPrice = discountQuarantine && cabinetPrice > finalTargetPrice
      ? computeOzonQuarantineNextPrice(cabinetPrice, finalTargetPrice)
      : finalTargetPrice;
    const priceItem = {
      id: product.id,
      productId: product.id,
      target: product.target,
      offerId: product.offerId,
      price: stepPrice,
      finalTargetPrice: discountQuarantine && stepPrice > finalTargetPrice ? finalTargetPrice : undefined,
      cabinetPrice: discountQuarantine ? cabinetPrice : undefined,
      oldPrice: discountQuarantine ? resolveOzonOldPrice(stepPrice, {}) : cabinetPrice,
      forceOldPrice: quarantineRelease || undefined,
      markup: product.markupCoefficient,
      supplier: product.selectedSupplier,
      marketplace: product.marketplace,
      priceIntentId,
      sourceEvent: sourceEvent || reason,
      priceApplyStatus: "queued",
    };
    const delayedRetry = product.marketplace === "ozon"
      ? findActiveDelayedPriceRetry(queueState.items, priceItem, new Date(sentAt))
      : null;
    if (delayedRetry) {
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: "ozon_price_delayed",
        nextRetryAt: delayedRetry.nextRetryAt,
        error: delayedRetry.error || "ozon_per_item_price_limit",
        priceIntentId,
        priceApplyStatus: "ozon_price_delayed",
      });
      delayedQueueUpdates.push({
        ...delayedRetry,
        id: product.id,
        productId: product.id,
        target: product.target,
        offerId: product.offerId,
        price: product.nextPrice,
        oldPrice: product.currentPrice,
        status: "delayed",
        retryReason: delayedRetry.retryReason || "ozon_per_item_price_limit",
        updatedAt: sentAt,
        priceIntentId,
        sourceEvent: sourceEvent || reason,
      });
      continue;
    }
    items.push(priceItem);
  }

  if (!dryRun) {
    const pmTimeoutIds = skipped
      .filter((item) => item.reason === "pm_live_timeout")
      .map((item) => item.id || item.productId)
      .filter(Boolean);
    schedulePmTimeoutRepriceRetry(pmTimeoutIds, {
      marketplace: normalizedMarketplaceFilter,
      reason,
      sourceEvent: sourceEvent || reason,
      repriceAttempt,
    });

    // Reconcile the cached marketplace price for products the build confirmed are already
    // at target on the marketplace ("unchanged_verified"). The postgres current_price column
    // is derived from raw.marketplacePrice on EVERY write, so a bare column update gets
    // clobbered by the next product patch — we must update raw.marketplacePrice through the
    // normal patch path. Without this the price sweep re-selects the same thousands of
    // products forever and the UI shows "цена ждёт" for prices that are actually live.
    try {
      const prisma = getPrisma();
      if (prisma && shouldUsePostgresStorage()) {
        const unchangedIds = new Set(skipped
          .filter((item) => item.reason === "unchanged_verified")
          .map((item) => String(item.id || item.productId))
          .filter(Boolean));
        const updates = [];
        for (const product of selected) {
          if (!unchangedIds.has(String(product.id))) continue;
          const target = roundPrice(Number(product.nextPrice || product.targetPrice || 0));
          if (target > 0) updates.push({ id: String(product.id), target });
        }
        if (updates.length) {
          // Set BOTH columns AND ALL raw price fields to the SAME verified value, plus a
          // priceVerifiedAt timestamp. Columns are re-derived from raw.* on every product
          // write, so a partial reconcile (e.g. only raw.marketplacePrice) gets clobbered
          // by the next rebuild's freshly-recomputed raw.nextPrice/targetPrice — leaving
          // current_price <> target_price true again and price_sweep re-selecting these
          // SKUs forever. priceVerifiedAt lets price_sweep skip recently-verified SKUs
          // even if the columns cosmetically diverge again before the next reconciler pass.
          await Promise.all(updates.map(({ id, target }) =>
            prisma.$executeRawUnsafe(
              `UPDATE warehouse_products
               SET current_price = $1,
                   target_price = $1,
                   raw = COALESCE(raw, '{}'::jsonb) || jsonb_build_object(
                     'marketplacePrice', $1::int,
                     'currentPrice', $1::int,
                     'nextPrice', $1::int,
                     'targetPrice', $1::int,
                     'priceVerifiedAt', to_jsonb(now())
                   )
               WHERE id = $2`,
              target, id,
            ).catch(() => null)));
          const reconciledIds = updates.map(({ id }) => id);
          await prisma.$executeRawUnsafe(
            `UPDATE sales_automation_sku_states
             SET reason = 'ok', price_status = 'success', updated_at = now()
             WHERE product_id = ANY($1::text[]) AND reason = 'unchanged_verified'`,
            reconciledIds,
          ).catch(() => null);
          logger.info("price current_price reconciled to target (already live)", { count: updates.length, reason: cleanText(reason) || "" });
        }
      }
    } catch (error) {
      logger.warn("current_price reconcile failed", { detail: error?.message || String(error) });
    }
  }

  if (!dryRun && items.length) {
    logger.info("warehouse price push selection", {
      reason: cleanText(reason) || "auto-price-push",
      priceIntentId,
      sourceEvent: sourceEvent || reason,
      selected: selected.length,
      readyToSend: items.length,
      productIds: items.slice(0, 25).map((item) => item.id),
      firstOfferId: items[0]?.offerId || null,
      selectedSupplier: items[0]?.supplier?.partnerName || items[0]?.supplier?.supplierName || null,
      effectiveFinalPrice: Number(items[0]?.supplier?.effectiveFinalPrice || items[0]?.price || 0) || null,
      alternativesCount: Array.isArray(selected[0]?.supplierAlternatives) ? selected[0].supplierAlternatives.length : 0,
      pmSource: selected[0]?.priceSource || items[0]?.supplier?.priceSource || null,
      priceApplyStatus: "queued",
    });
  }

  if (dryRun) {
    const stats = warehousePriceMarketplaceStats(items, [], skipped);
    return {
      ok: true,
      dryRun: true,
      selected: selected.length,
      readyToSend: items.length,
      stockReadyToSend: stockItems.length,
      skipped,
      items,
      ...stats,
    };
  }

