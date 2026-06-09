async function runSalesAutomationOperation(payload = {}) {
  const result = await queueAuthoritativePriceReprice({
    productIds: Array.isArray(payload.productIds) ? payload.productIds : undefined,
    marketplace: payload.marketplace || "all",
    force: payload.force === true,
    onlyChanged: payload.onlyChanged !== false,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
    verify: payload.verify !== false,
    limit: cleanLimit(payload.limit, 1000, 50000),
    reason: payload.reason || "sales_automation_operation",
    sourceEvent: "sales_automation_operation",
  });
  return {
    ok: result.ok !== false,
    ...result,
    summary: `Sales automation queued ${result.queued || 0} SKU in ${result.queuedBatches || 0} batches.`,
  };
}

async function runProblemProductsRepairOperation(payload = {}, request = null, options = {}) {
  const productIds = Array.isArray(payload.productIds) ? payload.productIds.map(cleanText).filter(Boolean).slice(0, 100) : [];
  const results = [];
  let processed = 0;
  for (const id of productIds) {
    processed += 1;
    await options.onProgress?.({
      progress: 5 + (processed / Math.max(1, productIds.length)) * 90,
      summary: `Repairing ${processed} of ${productIds.length} problem products.`,
    });
    try {
      results.push(await repairWarehouseProductGroup(id, request));
    } catch (error) {
      results.push({ ok: false, productId: id, error: error?.message || String(error) });
    }
  }
  return {
    ok: results.every((item) => item.ok !== false),
    repaired: results.filter((item) => item.ok !== false).length,
    failed: results.filter((item) => item.ok === false).length,
    results,
    summary: `Problem products repaired ${results.filter((item) => item.ok !== false).length}; failed ${results.filter((item) => item.ok === false).length}.`,
  };
}

async function runBrandIndexRebuildOperation(payload = {}) {
  if (!shouldUsePostgresStorage()) {
    return { ok: false, error: "postgres_required", summary: "Brand index requires PostgreSQL storage." };
  }
  const limit = cleanLimit(payload.limit, 100000, 200000);
  const result = await rebuildWarehouseBrandIndexPostgres(getPrisma(), { limit });
  return {
    ok: result.ok !== false,
    ...result,
    source: "postgres",
    summary: `Brand index rebuilt: indexed ${result.indexed || result.created || 0}; scanned ${result.scanned || 0}.`,
  };
}

async function runYandexPricePushOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const force = payload?.force === true;
  const onlyChanged = payload?.onlyChanged !== false;
  const result = await sendWarehousePrices({
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
  });
  return {
    ok: result.ok,
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    processed: result.selected || limit,
    sent: result.sent || 0,
    failed: result.failed || 0,
    skipped: Array.isArray(result.skipped) ? result.skipped.length : Number(result.skipped || 0) || 0,
    yandexSent: result.yandexSent || 0,
    yandexFailed: result.yandexFailed || 0,
    yandexSkipped: result.yandexSkipped || 0,
    errors: Array.isArray(result.failedItems) ? result.failedItems : [],
    ...result,
    summary: `Yandex price push sent ${result.yandexSent || result.sent || 0}; failed ${result.yandexFailed || result.failed || 0}; skipped ${result.yandexSkipped || (Array.isArray(result.skipped) ? result.skipped.length : 0)}.`,
  };
}

async function runLinkedSupplierRecoveryOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const warehouse = await readWarehouse();
  const marketplaceFilter = cleanText(payload?.marketplace || "all").toLowerCase();
  const productIdSet = Array.isArray(payload?.productIds) && payload.productIds.length
    ? new Set(payload.productIds.map((id) => cleanText(id)).filter(Boolean))
    : null;
  const offerIdSet = Array.isArray(payload?.offerIds) && payload.offerIds.length
    ? new Set(payload.offerIds.map((id) => cleanText(id).toLowerCase()).filter(Boolean))
    : null;
  const candidateLimit = productIdSet || offerIdSet ? Math.max(limit, (warehouse.products || []).length) : limit;
  const candidates = linkedRecoveryCandidateProducts(warehouse.products || [], candidateLimit)
    .filter((product) => {
      if (marketplaceFilter !== "all" && cleanText(product.marketplace).toLowerCase() !== marketplaceFilter) return false;
      if (productIdSet && !productIdSet.has(String(product.id))) return false;
      if (offerIdSet && !offerIdSet.has(cleanText(product.offerId).toLowerCase())) return false;
      return true;
    })
    .slice(0, limit);

  if (!candidates.length) {
    return {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      recovered: 0,
      restoredStocks: 0,
      unarchived: 0,
      unarchivePending: 0,
      queuedByDailyLimit: 0,
      queueSize: (await readOzonUnarchiveQueue().catch(() => ({ items: [] }))).items.length || 0,
      errors: [],
      summary: "Нет привязанных карточек, которым нужно восстановление.",
    };
  }

  const rebuilt = [];
  for (const chunk of chunkArray(candidates, 200)) {
    const products = await buildFreshWarehouseProductsFromKnownProducts(
      warehouse,
      chunk,
      {
        refreshPrices: false,
        persistMutations: false,
        livePriceMaster: false,
        batchPriceMaster: false,
      },
    );
    rebuilt.push(...products);
  }

  const ready = rebuilt.filter((product) => product.hasLinks && product.selectedSupplier);
  const forceRecovery = payload.force !== false;
  const needsRecovery = forceRecovery
    ? ready
    : ready.filter((product) => (
        marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true })
        || Boolean(product.noSupplierAutomation?.stockZeroAt)
        || Boolean(product.noSupplierAutomation?.archivedAt)
      ));
  const notReady = candidates.length - ready.length;
  const alreadySellable = Math.max(0, ready.length - needsRecovery.length);
  const result = await runSupplierRecoveryAutomation(
    { products: needsRecovery },
    { productIds: needsRecovery.map((product) => product.id), source: "targeted", force: true },
  );
  const sellableRecovered = Number(result.sellableRecovered || 0);
  const unarchiveFailed = Number(result.unarchiveFailed || 0);
  const unarchivePending = Number(result.unarchivePending || 0);
  const queuedByDailyLimit = Number(result.queuedByDailyLimit || 0);
  const stockFailed = Number(result.stockFailed || 0);
  return {
    ok: result.errors?.length ? false : true,
    partial: Boolean(result.errors?.length && (result.recovered || result.restoredStocks || result.unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    ready: ready.length,
    notReady,
    alreadySellable,
    needsRecovery: needsRecovery.length,
    recovered: result.recovered || 0,
    sellableRecovered,
    restoredStocks: result.restoredStocks || 0,
    unarchived: result.unarchived || 0,
    unarchivePending,
    queuedByDailyLimit,
    nextRetryAt: result.nextRetryAt || null,
    queueSize: result.queueSize || 0,
    queuedSamples: result.queuedSamples || [],
    unarchiveFailed,
    stockFailed,
    errors: result.errors || [],
    productStatuses: result.productStatuses || [],
    summary: `Проверено ${candidates.length}; с доступным поставщиком ${ready.length}; уже продавались ${alreadySellable}; нужно восстановить ${needsRecovery.length}; полностью восстановлено ${sellableRecovered}; без поставщика ${notReady}; ошибки разархива ${unarchiveFailed}; ошибки остатков ${stockFailed}.`,
  };
}

