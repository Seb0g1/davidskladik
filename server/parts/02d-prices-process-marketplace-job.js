async function processMarketplaceJob(name, data = {}) {
  if (name === "auto-price-push") {
    return sendWarehousePrices({
      productIds: Array.isArray(data.productIds) ? data.productIds : undefined,
      usdRate: data.usdRate,
      minDiffRub: 0,
      minDiffPct: 0,
      force: data.force === true,
      dryRun: false,
      marketplace: data.marketplace || "all",
      onlyChanged: data.onlyChanged === true,
      refreshMarketplacePrices: data.refreshMarketplacePrices === true,
      livePriceMaster: data.livePriceMaster === true,
      reason: data.reason || "auto-price-push",
      limit: data.limit,
      verify: data.verify !== false,
      priceIntentId: cleanText(data.priceIntentId || ""),
      sourceEvent: data.sourceEvent || data.reason || "auto-price-push",
      repriceAttempt: Number(data.repriceAttempt || 0) || 0,
      priceMasterTimeoutMs: Number(data.priceMasterTimeoutMs || 0) || undefined,
    });
  }
  if (name === "no-supplier-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds);
      return runNoSupplierMarketplaceAutomation({ products }, {
        productIds,
        includeNoLinks: true,
        source: "targeted",
        skipLinkedGrace: data.skipLinkedGrace === true,
      });
    }
    const preview = await buildWarehouseView({ sync: true });
    return runNoSupplierMarketplaceAutomation(preview, { source: "full_sync" });
  }
  if (name === "supplier-recovery-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    const source = cleanText(data.source || data.sourceEvent) || (productIds.length ? "targeted" : "full");
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds, {
        refreshPrices: data.refreshPrices !== false,
        livePriceMaster: data.livePriceMaster !== false,
        batchPriceMaster: data.livePriceMaster !== false,
        priceMasterTimeoutMs: Number(data.priceMasterTimeoutMs || 0) || autoPricePmTimeoutMs,
      });
      // Guard against race: live PM build takes 2-5s during which the API process may have
      // written a new snooze to Postgres (e.g. user immediately re-snoozed after unsnooze).
      // Re-read link state from Postgres and exclude products that are currently snoozed so we
      // don't send stock=5 to a marketplace that should be at 0.
      const pgProducts = await readWarehouseProductsFromPostgresByIds(productIds).catch(() => []);
      const pgById = new Map(pgProducts.map((p) => [String(p.id), p]));
      const nowTs = new Date();
      const eligible = products.filter((p) => {
        const pg = pgById.get(String(p.id));
        if (!pg) return true;
        return !(pg.links || []).some(
          (link) => link.snooze?.snoozedUntil && new Date(link.snooze.snoozedUntil) > nowTs,
        );
      });
      if (!eligible.length) {
        logger.info("supplier_recovery_skipped_all_snoozed", { source, count: productIds.length });
        return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [], source };
      }
      return runSupplierRecoveryAutomation({ products: eligible }, {
        productIds,
        source,
        sourceEvent: data.sourceEvent || source,
        force: data.force === true,
        forceOzonDailyLimit: data.forceOzonDailyLimit === true,
      });
    }
    const preview = await buildWarehouseView({ sync: false });
    return runSupplierRecoveryAutomation(preview, { source });
  }
  if (name === "ozon-unarchive-queue-process") {
    return processOzonUnarchiveQueue({
      source: data.source || "ozon_unarchive_queue_auto",
      limit: data.limit || ozonUnarchiveQueueBatchLimit,
      force: data.force === true,
      queueRunId: cleanText(data.queueRunId || data.runId),
    });
  }
  if (name === "yandex-unarchive-queue-process") {
    return processYandexUnarchiveQueue({
      source: data.source || "yandex_unarchive_queue_auto",
      limit: data.limit || yandexUnarchiveQueueBatchLimit,
      queueRunId: cleanText(data.queueRunId || data.runId),
    });
  }
  if (name === "wb-marketplace-sync") {
    return runWbMarketplaceSync({ source: cleanText(data.source || "queue") });
  }
  return null;
}

