function normalizeSupplierArticle(input = {}) {
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article: cleanText(input.article),
    keyword: cleanText(input.keyword),
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    createdAt: input.createdAt || new Date().toISOString(),
  };
}

function normalizeSupplierPricingMode(input = {}) {
  const raw = input.raw && typeof input.raw === "object" && !Array.isArray(input.raw) ? input.raw : {};
  const value = cleanText(
    input.pricingMode
    || input.pricing_mode
    || input.priceMode
    || input.price_mode
    || raw.pricingMode
    || raw.pricing_mode
    || raw.priceMode
    || raw.price_mode
  ).toLowerCase().replace(/[-\s]+/g, "_");
  if (["stock_only", "stockonly", "inventory_only", "no_price", "stock_fallback"].includes(value)) {
    return "stock_only";
  }
  const name = normalizeSupplierName(input.name || raw.name || "");
  if (name && stockOnlySupplierNameSet().has(name)) return "stock_only";
  return "normal";
}

function normalizeSupplierTrustFactor(value, fallback = 100) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return Math.max(0, Math.min(100, Number(fallback || 100) || 100));
  return Math.max(0, Math.min(100, Math.round(parsed)));
}

function normalizeSupplierOrderCutoff(value = "") {
  const text = cleanText(value);
  const match = text.match(/^(\d{1,2})(?::?(\d{2}))?$/);
  if (!match) return "";
  const hour = Math.max(0, Math.min(23, Number(match[1]) || 0));
  const minute = Math.max(0, Math.min(59, Number(match[2] || 0) || 0));
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function normalizeManagedSupplier(input = {}) {
  const inactiveUntil = cleanText(input.inactiveUntil || input.inactive_until);
  const stopped = Boolean(input.stopped);
  const name = cleanText(input.name);
  const priceCurrencyInput = cleanText(input.priceCurrency || input.price_currency || input.currency).toUpperCase();
  // Инна always prices in RUB — name detection overrides explicit "USD" saved by form default.
  const priceCurrency = priceCurrencyInput === "RUB" || priceCurrencyInput === "RUR" || isInnaSupplierName(name)
    ? "RUB"
    : "USD";
  const pricingMode = normalizeSupplierPricingMode(input);
  const raw = input.raw && typeof input.raw === "object" && !Array.isArray(input.raw) ? input.raw : {};
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual"),
    name,
    priceCurrency,
    pricingMode,
    stockOnly: pricingMode === "stock_only",
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor ?? input.trust_factor ?? raw.trustFactor ?? raw.trust_factor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time || raw.orderCutoffTime || raw.order_cutoff_time),
    reseller: Boolean(input.reseller ?? raw.reseller),
    stopped,
    note: cleanText(input.note),
    stopReason: cleanText(input.stopReason || input.stop_reason),
    inactiveComment: cleanText(input.inactiveComment || input.inactive_comment),
    inactiveUntil: inactiveUntil || null,
    inactiveUntilUnknown: Boolean(input.inactiveUntilUnknown || input.inactive_until_unknown || (stopped && !inactiveUntil)),
    articles: Array.isArray(input.articles) ? input.articles.map(normalizeSupplierArticle) : [],
    createdAt: input.createdAt || new Date().toISOString(),
    // Preserve the original timestamp: bumping it on every normalize makes a stale
    // in-memory copy (api/worker keep independent caches) look "fresh" and lets a
    // bulk warehouse write roll back newer supplier rows in Postgres.
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
}

function supplierImpactCount(warehouse = {}, supplier = {}) {
  return supplierImpactProductIds(warehouse, supplier).length;
}

function supplierImpactCountMap(warehouse = {}, suppliers = []) {
  const counts = new Map();
  const supplierKeys = new Map();
  for (const supplier of suppliers || []) {
    const keys = [
      normalizeSupplierName(supplier.name),
      cleanText(supplier.partnerId) ? `partner:${cleanText(supplier.partnerId)}` : "",
    ].filter(Boolean);
    for (const key of keys) {
      if (!supplierKeys.has(key)) supplierKeys.set(key, new Set());
      supplierKeys.get(key).add(supplier.id);
    }
    counts.set(supplier.id, 0);
  }
  if (!supplierKeys.size) return counts;
  const productHitsBySupplier = new Map();
  for (const product of warehouse.products || []) {
    const productSupplierIds = new Set();
    for (const link of product.links || []) {
      const keys = [
        normalizeSupplierName(link.supplierName),
        cleanText(link.partnerId) ? `partner:${cleanText(link.partnerId)}` : "",
      ].filter(Boolean);
      for (const key of keys) {
        const ids = supplierKeys.get(key);
        if (!ids) continue;
        ids.forEach((id) => productSupplierIds.add(id));
      }
    }
    productSupplierIds.forEach((id) => {
      if (!productHitsBySupplier.has(id)) productHitsBySupplier.set(id, new Set());
      productHitsBySupplier.get(id).add(product.id);
    });
  }
  for (const [id, productIds] of productHitsBySupplier.entries()) counts.set(id, productIds.size);
  return counts;
}

function supplierImpactProductIds(warehouse = {}, ...suppliers) {
  const matchers = suppliers
    .filter(Boolean)
    .map((supplier) => ({
      name: normalizeSupplierName(supplier.name),
      partnerId: cleanText(supplier.partnerId),
    }))
    .filter((supplier) => supplier.name || supplier.partnerId);
  if (!matchers.length) return [];
  const productIds = new Set();
  for (const product of warehouse.products || []) {
    for (const link of product.links || []) {
      const normalizedLinkSupplier = normalizeSupplierName(link.supplierName);
      const linkPartnerId = cleanText(link.partnerId);
      if (matchers.some((supplier) =>
        (supplier.name && normalizedLinkSupplier === supplier.name)
        || (supplier.partnerId && linkPartnerId === supplier.partnerId),
      )) {
        productIds.add(product.id);
        break;
      }
    }
  }
  return Array.from(productIds);
}

function priceMasterChangedRowMatchesWarehouseLink(row = {}, link = {}) {
  if (!row || !link) return false;
  const fields = priceMasterSnapshotRowFields(row);
  const supplierOk =
    !link.supplierName
    || normalizeSupplierName(fields.partnerName) === normalizeSupplierName(link.supplierName);
  const partnerOk = link.matchType === "article"
    ? true
    : (!link.partnerId || String(fields.partnerId || "") === String(link.partnerId));
  const keywordOk = includesKeyword(fields.name, link.keyword);
  if (!supplierOk || !partnerOk || !keywordOk) return false;
  if (link.matchType === "selected_row") {
    if (link.sourceRowId && String(fields.rowId || "") === String(link.sourceRowId)) return true;
    if (link.exactName) return exactPriceMasterNameMatches(fields.name, link.exactName);
    return false;
  }
  if (link.matchType === "exact_name") {
    return exactPriceMasterNameMatches(fields.name, link.exactName || link.article);
  }
  const article = cleanText(link.article).toLowerCase();
  return Boolean(article && cleanText(fields.article).toLowerCase() === article);
}

function priceMasterChangeImpactProductIds(warehouse = {}, changes = [], options = {}) {
  const maxChanges = Math.max(1, Number(options.maxChanges || priceMasterDeltaMaxChanges) || priceMasterDeltaMaxChanges);
  const maxProducts = Math.max(1, Number(options.maxProducts || priceMasterDeltaMaxProducts) || priceMasterDeltaMaxProducts);
  const relevantTypes = new Set(["price_changed", "inactive", "returned", "missing", "new"]);
  const relevantChanges = (Array.isArray(changes) ? changes : []).filter((change) => relevantTypes.has(change?.type));
  if (!relevantChanges.length) return { productIds: [], scannedChanges: 0, skipped: false, reason: null };
  if (relevantChanges.length > maxChanges) {
    if (options.fullReconcileOnTooMany === true) {
      const reconcileLimit = Math.max(1, Number(options.fullReconcileMaxProducts || autoPriceReconcileMaxProducts) || autoPriceReconcileMaxProducts);
      const products = Array.isArray(warehouse.products) ? warehouse.products : [];
      const linked = products.filter((product) => product?.autoPriceEnabled !== false && Array.isArray(product.links) && product.links.length);
      const expanded = expandWarehouseProductsToGroups(products, linked);
      const ids = Array.from(new Set(expanded.map((product) => cleanText(product.id)).filter(Boolean)));
      const truncated = ids.length > reconcileLimit;
      return {
        productIds: ids.slice(0, reconcileLimit),
        scannedChanges: relevantChanges.length,
        skipped: truncated,
        reason: truncated ? "too_many_pricemaster_changes_full_reconcile_limited" : "too_many_pricemaster_changes_full_reconcile",
        fallbackFullReconcile: true,
        directProducts: linked.length,
        groupExpandedProducts: ids.length,
      };
    }
    return {
      productIds: [],
      scannedChanges: relevantChanges.length,
      skipped: true,
      reason: "too_many_pricemaster_changes",
    };
  }
  const rows = relevantChanges.flatMap((change) => [change.current, change.previous].filter(Boolean));
  if (!rows.length) return { productIds: [], scannedChanges: relevantChanges.length, skipped: false, reason: null };

  const productIds = new Set();
  const matchedProducts = [];
  for (const product of warehouse.products || []) {
    const links = Array.isArray(product.links) ? product.links : [];
    if (!links.length) continue;
    const matched = links.some((link) => rows.some((row) => priceMasterChangedRowMatchesWarehouseLink(row, link)));
    if (!matched) continue;
    productIds.add(product.id);
    matchedProducts.push(product);
    if (productIds.size >= maxProducts) {
      const expanded = expandWarehouseProductsToGroups(warehouse.products || [], matchedProducts);
      const expandedIds = Array.from(new Set([...Array.from(productIds), ...expanded.map((product) => product.id).filter(Boolean)]));
      return {
        productIds: expandedIds.slice(0, maxProducts),
        scannedChanges: relevantChanges.length,
        skipped: true,
        reason: "too_many_impacted_products",
        directProducts: productIds.size,
        groupExpandedProducts: expandedIds.length,
      };
    }
  }
  const expanded = expandWarehouseProductsToGroups(warehouse.products || [], matchedProducts);
  const expandedIds = Array.from(new Set([...Array.from(productIds), ...expanded.map((product) => product.id).filter(Boolean)]));
  return {
    productIds: expandedIds.slice(0, maxProducts),
    scannedChanges: relevantChanges.length,
    skipped: expandedIds.length > maxProducts,
    reason: expandedIds.length > maxProducts ? "too_many_impacted_products" : null,
    directProducts: productIds.size,
    groupExpandedProducts: expandedIds.length,
  };
}

function warehouseProductAutomationFingerprint(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const state = normalized.marketplaceState || {};
  const links = (Array.isArray(normalized.links) ? normalized.links : [])
    .map((link) => ({
      article: cleanText(link.article).toLowerCase(),
      matchType: cleanText(link.matchType || "article"),
      exactName: cleanText(link.exactName).toLowerCase(),
      sourceRowId: cleanText(link.sourceRowId),
      partnerId: cleanText(link.partnerId),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

  return JSON.stringify({
    id: normalized.id,
    target: normalized.target,
    marketplace: normalized.marketplace,
    offerId: cleanText(normalized.offerId).toLowerCase(),
    marketplacePrice: Number(normalized.marketplacePrice || 0) || 0,
    currentPrice: Number(normalized.currentPrice || 0) || 0,
    targetStock: normalized.targetStock === null || normalized.targetStock === undefined ? null : Number(normalized.targetStock),
    state: {
      code: cleanText(state.code),
      active: Boolean(state.active),
      archived: Boolean(state.archived),
      outOfStock: Boolean(state.outOfStock),
      partial: Boolean(state.partial),
    },
    noSupplierAutomation: {
      stockZeroAt: cleanText(normalized.noSupplierAutomation?.stockZeroAt || ""),
      archivedAt: cleanText(normalized.noSupplierAutomation?.archivedAt || ""),
      recoveredAt: cleanText(normalized.noSupplierAutomation?.recoveredAt || ""),
    },
    links,
  });
}

function changedWarehouseProductIdsByAutomationFingerprint(beforeProducts = [], afterProducts = []) {
  const before = new Map();
  for (const product of Array.isArray(beforeProducts) ? beforeProducts : []) {
    if (!product?.id) continue;
    before.set(String(product.id), warehouseProductAutomationFingerprint(product));
  }
  const changed = [];
  for (const product of Array.isArray(afterProducts) ? afterProducts : []) {
    if (!product?.id) continue;
    const id = String(product.id);
    const fingerprint = warehouseProductAutomationFingerprint(product);
    if (!before.has(id) || before.get(id) !== fingerprint) changed.push(product.id);
  }
  return changed;
}

function backgroundAutomationProductIds(priceMaster = {}, warehouse = {}, options = {}) {
  const productIds = new Set(
    (Array.isArray(warehouse.marketplaceSyncChangedProductIds) ? warehouse.marketplaceSyncChangedProductIds : [])
      .map((id) => cleanText(id))
      .filter(Boolean),
  );
  const priceMasterDelta = priceMasterChangeImpactProductIds(warehouse, priceMaster.changedRows || [], {
    maxChanges: options.maxChanges || priceMasterDeltaMaxChanges,
    maxProducts: options.maxProducts || priceMasterDeltaMaxProducts,
  });
  for (const id of priceMasterDelta.productIds || []) {
    const normalizedId = cleanText(id);
    if (normalizedId) productIds.add(normalizedId);
  }
  return {
    productIds: Array.from(productIds),
    marketplaceChanged: Array.isArray(warehouse.marketplaceSyncChangedProductIds) ? warehouse.marketplaceSyncChangedProductIds.length : 0,
    priceMasterDelta,
  };
}
