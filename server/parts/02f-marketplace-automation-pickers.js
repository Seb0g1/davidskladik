function marketplaceHasPositiveStock(product = {}) {
  const state = product.marketplaceState || {};
  if (Number(state.stock || 0) > 0 || Number(state.present || 0) > 0) return true;
  return (Array.isArray(state.warehouses) ? state.warehouses : [])
    .some((warehouse) => Number(warehouse.stock || warehouse.present || 0) > 0);
}

function marketplaceOfferAutomationKey(product = {}) {
  const marketplace = cleanText(product.marketplace).toLowerCase();
  const target = cleanText(product.target).toLowerCase();
  const offerId = cleanText(product.offerId).toLowerCase();
  return marketplace && target && offerId ? `${marketplace}:${target}:${offerId}` : "";
}

function shouldSendTargetStockForProduct(product = {}) {
  if (!product?.hasLinks || !product.ready || !product.selectedSupplier) return false;
  const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
  if (targetStock <= 0) return false;
  const currentStock = Math.max(0, Math.round(Number(product.marketplaceState?.stock || 0)));
  return targetStock !== currentStock;
}

function pickTargetStockSendProducts(products = []) {
  return (Array.isArray(products) ? products : [])
    .filter((product) =>
      product?.id
      && product.offerId
      && product.target
      && shouldSendTargetStockForProduct(product)
    )
    .map((product) => ({
      ...product,
      targetStock: Math.max(1, Math.round(Number(product.targetStock || 0))),
    }));
}

function marketplaceProductNeedsSalesRecovery(product = {}, { includeUnknown = true } = {}) {
  const state = product.marketplaceState || {};
  const code = cleanText(state.code || product.status).toLowerCase();
  const archived = Boolean(productLooksArchived(product));
  if (archived || code === "out_of_stock" || code === "inactive") return true;
  if (includeUnknown && (!code || code === "unknown" || state.partial)) return true;
  const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
  return targetStock > 0 && !marketplaceHasPositiveStock(product);
}

function pickNoSupplierAutomationCandidates(products = [], options = {}) {
  const list = Array.isArray(products) ? products : [];
  const protectedOfferKeys = new Set(list
    .filter((product) => product.hasLinks || product.noSupplierAutomation?.manualSellableAt)
    .map(marketplaceOfferAutomationKey)
    .filter(Boolean));
  const noLinkProducts = options.includeNoLinks
    ? list.filter((product) => {
        if (product.hasLinks || product.noSupplierAutomation?.manualSellableAt || !product.everHadLinks) return false;
        const nowMs = options.now ? new Date(options.now).getTime() : Date.now();
        if (product.hasSnoozedLinks) return false;
        const key = marketplaceOfferAutomationKey(product);
        return !key || !protectedOfferKeys.has(key);
      })
    : [];
  // manualSellableAt expires after 48 h — long enough for unarchive/recovery to settle,
  // but short enough that a product whose supplier truly vanished again gets zeroed.
  const manualSellableTtlMs = 48 * 60 * 60 * 1000;
  const linkedNoSupplier = autoZeroStockOnNoSupplier
    ? list.filter((product) => {
        if (!product.hasLinks || product.selectedSupplier) return false;
        const nowMs = options.now ? new Date(options.now).getTime() : Date.now();
        // Skip snoozed products only when marketplace stock is already 0 — if the snooze
        // route's API call failed the marketplace may still show positive stock; in that
        // case the automation acts as a retry so the stock actually gets zeroed.
        if (product.hasSnoozedLinks && !marketplaceHasPositiveStock(product)) return false;
        const manualAt = product.noSupplierAutomation?.manualSellableAt;
        if (manualAt && nowMs - new Date(manualAt).getTime() < manualSellableTtlMs) return false;
        // Skip products already archived by this automation — recovery automation handles them.
        // Without this check, no-supplier automation re-archives right after recovery queues an unarchive.
        if (product.noSupplierAutomation?.archivedAt) return false;
        const key = marketplaceOfferAutomationKey(product);
        if (key && protectedOfferKeys.has(key) && !product.hasLinks) return false;
        const updatedMs = product.updatedAt ? new Date(product.updatedAt).getTime() : 0;
        if (
          !options.skipLinkedGrace
          && linkedNoSupplierZeroGraceMs > 0
          && Number.isFinite(updatedMs)
          && updatedMs > 0
          && nowMs - updatedMs < linkedNoSupplierZeroGraceMs
        ) {
          return false;
        }
        return !product.noSupplierAutomation?.stockZeroAt || marketplaceHasPositiveStock(product);
      })
    : [];
  const noLinkZeroStock = autoZeroStockOnNoSupplier
    ? noLinkProducts.filter((product) => !product.noSupplierAutomation?.stockZeroAt || marketplaceHasPositiveStock(product))
    : [];
  return {
    toZeroStock: [...linkedNoSupplier, ...noLinkZeroStock],
    // noLinkProducts already guarantees product.everHadLinks === true (the filter above excludes
    // products that never had links). The old `!product.everHadLinks` condition was always false
    // for every item in the list, making toArchive permanently empty.
    // Archive only after stock is already zeroed by automation — never archive while stock > 0.
    toArchive: autoArchiveOnNoLinks
      ? noLinkProducts.filter(
          (product) =>
            !productLooksArchived(product)
            && !marketplaceHasPositiveStock(product)
            && Boolean(product.noSupplierAutomation?.stockZeroAt),
        )
      : [],
  };
}

function queueLinkedUnavailableSupplierZeroStock(builtProducts = [], { source = "warehouse_view" } = {}) {
  if (!autoZeroStockOnNoSupplier || !Array.isArray(builtProducts) || !builtProducts.length) return;
  const { toZeroStock } = pickNoSupplierAutomationCandidates(builtProducts, {
    includeNoLinks: false,
    skipLinkedGrace: true,
    now: new Date().toISOString(),
  });
  const productIds = [];
  const now = Date.now();
  for (const product of toZeroStock) {
    if (!product?.id || !marketplaceHasPositiveStock(product)) continue;
    const lastQueuedAt = linkedNoSupplierZeroStockQueuedAt.get(product.id) || 0;
    if (now - lastQueuedAt < linkedNoSupplierZeroStockQueueCooldownMs) continue;
    linkedNoSupplierZeroStockQueuedAt.set(product.id, now);
    productIds.push(product.id);
  }
  if (!productIds.length) return;
  void queueMarketplaceJob("no-supplier-automation", { productIds, source }, { priority: QUEUE_PRIORITY.RECOVERY }).catch((error) => {
    logger.warn("linked unavailable supplier zero-stock queue failed", {
      source,
      count: productIds.length,
      detail: error?.message || String(error),
    });
  });
}

function pickSupplierRecoveryCandidates(products = [], { productIds, force = false } = {}) {
  const idSet = Array.isArray(productIds) && productIds.length
    ? new Set(productIds.map((id) => String(id || "").trim()).filter(Boolean))
    : null;
  return (Array.isArray(products) ? products : []).filter((product) => {
    if (idSet && !idSet.has(String(product.id))) return false;
    if (!product.hasLinks) return false;
    // Allow recovery for products archived by automation even without a supplier —
    // they get unarchived with minimum stock=1 and manualSellableAt set to prevent re-zeroing.
    if (!product.selectedSupplier && !product.noSupplierAutomation?.archivedAt) return false;
    if (force) return true;
    const needsRecovery = marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true });
    if (
      product.noSupplierAutomation?.recoveredAt
      && !product.noSupplierAutomation?.stockZeroAt
      && !product.noSupplierAutomation?.archivedAt
      && !needsRecovery
    ) return false;
    return Boolean(product.noSupplierAutomation?.stockZeroAt)
      || Boolean(product.noSupplierAutomation?.archivedAt)
      || needsRecovery;
  });
}

function summarizeNoSupplierAutomationProducts(products = [], actions = []) {
  const actionsByProduct = new Map();
  for (const action of actions) {
    if (!action?.id) continue;
    if (!actionsByProduct.has(action.id)) actionsByProduct.set(action.id, []);
    actionsByProduct.get(action.id).push(action);
  }
  return products.map((product) => {
    const productActions = actionsByProduct.get(product.id) || [];
    const failed = productActions.find((action) => !action.ok && !action.skipped);
    const skipped = productActions.find((action) => action.skipped);
    return {
      id: product.id,
      offerId: product.offerId || "",
      hasLinks: Boolean(product.hasLinks),
      status: failed ? "error" : (skipped ? "skipped" : (productActions.length ? "processed" : "no_action")),
      actions: Array.from(new Set(productActions.map((action) => action.type).filter(Boolean))),
      error: failed?.error || null,
      skippedReason: skipped?.reason || null,
    };
  });
}

function summarizeSupplierRecoveryProducts(products = [], stockActions = [], unarchiveActions = []) {
  const actionsByProduct = new Map();
  for (const action of [...stockActions, ...unarchiveActions]) {
    if (!action?.id) continue;
    const id = String(action.id);
    if (!actionsByProduct.has(id)) actionsByProduct.set(id, []);
    actionsByProduct.get(id).push(action);
  }
  return (Array.isArray(products) ? products : []).map((product) => {
    const productActions = actionsByProduct.get(String(product.id)) || [];
    const stock = productActions.filter((action) => action.type === "restore_stock");
    const unarchive = productActions.filter((action) => action.type === "unarchive");
    const stockOk = stock.filter((action) => action.ok).length;
    const stockFailed = stock.filter((action) => !action.ok).length;
    const unarchiveOk = unarchive.filter((action) => action.ok).length;
    const unarchiveFailed = unarchive.filter((action) => !action.ok).length;
    const unarchivePending = unarchive.filter((action) => action.ok && action.pending).length;
    const queuedByDailyLimit = unarchive.filter((action) => action.ok && action.queuedByDailyLimit).length;
    const failed = productActions.find((action) => !action.ok);
    const needsUnarchive = productLooksArchived(product);
    const sellable = stockOk > 0
      && stockFailed === 0
      && unarchiveFailed === 0
      && unarchivePending === 0
      && queuedByDailyLimit === 0
      && (!needsUnarchive || unarchiveOk > 0);
    return {
      id: product.id,
      offerId: product.offerId || "",
      marketplace: product.marketplace || "",
      target: product.target || "",
      stockOk,
      stockFailed,
      unarchiveOk,
      unarchiveFailed,
      unarchivePending,
      queuedByDailyLimit,
      nextRetryAt: unarchive.find((action) => action.nextRetryAt)?.nextRetryAt || null,
      sellable,
      status: failed ? "error" : (sellable ? "sellable" : (productActions.length ? "processed" : "no_action")),
      error: failed?.error || null,
      warning: productActions.find((action) => action.warning)?.warning || null,
    };
  });
}
