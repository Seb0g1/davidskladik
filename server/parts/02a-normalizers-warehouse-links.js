function normalizeWarehouseLink(input = {}) {
  const priceCurrency = cleanText(input.priceCurrency || input.price_currency || input.currency).toUpperCase();
  const matchTypeRaw = cleanText(input.matchType || input.match_type);
  let matchType = ["article", "selected_row", "exact_name"].includes(matchTypeRaw) ? matchTypeRaw : "article";
  const exactName = cleanText(input.exactName || input.exact_name || input.nativeName || input.name);
  let sourceRowId = cleanText(input.sourceRowId || input.source_row_id || input.rowId);
  let article = cleanText(input.article || input.offerId || input.nativeId);
  const syntheticNoArticleRowId = parsePriceMasterNoArticleRowId(article);
  if (syntheticNoArticleRowId) {
    article = "";
    sourceRowId = sourceRowId || syntheticNoArticleRowId;
    matchType = "selected_row";
  }
  const preserveSelectedRow = matchType === "selected_row" && sourceRowId;
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article,
    matchType: article && !preserveSelectedRow ? "article" : matchType,
    exactName,
    sourceRowId,
    keyword: cleanText(input.keyword),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    priceCurrency: priceCurrency === "RUB" || priceCurrency === "RUR" ? "RUB" : "USD",
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || input.createdAt || new Date().toISOString(),
    createdBy: cleanText(input.createdBy || input.created_by),
    updatedBy: cleanText(input.updatedBy || input.updated_by || input.createdBy || input.created_by),
    resolvedBy: cleanText(input.resolvedBy || input.resolved_by),
    resolvedPriceMasterRow: input.resolvedPriceMasterRow && typeof input.resolvedPriceMasterRow === "object"
      ? cloneAuditValue(input.resolvedPriceMasterRow)
      : null,
  };
}

function warehouseLinkHasMatchTarget(input = {}) {
  const link = normalizeWarehouseLink(input);
  return Boolean(link.article || link.exactName || link.sourceRowId);
}

function warehouseLinkIdentityKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.matchType === "selected_row" && link.sourceRowId
    ? `row:${link.sourceRowId}`
    : link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    link.partnerId,
    normalizeSupplierName(link.supplierName),
    link.keyword.toLowerCase(),
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkTargetKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.matchType === "selected_row" && link.sourceRowId
    ? `row:${link.sourceRowId}`
    : link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    warehouseLinkSupplierSignature(link),
    link.keyword.toLowerCase(),
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkPrimaryTargetKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.matchType === "selected_row" && link.sourceRowId
    ? `row:${link.sourceRowId}`
    : link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkSupplierKeys(input = {}) {
  const link = normalizeWarehouseLink(input);
  return [
    normalizeSupplierName(link.supplierName),
    link.partnerId ? `partner:${link.partnerId}` : "",
  ].filter(Boolean);
}

function warehouseLinkSupplierSignature(input = {}) {
  const keys = warehouseLinkSupplierKeys(input).sort();
  return keys.length ? keys.join("&") : "manual";
}

function warehouseLinksHaveCompatibleSupplierTarget(existingLink = {}, incomingLink = {}) {
  const left = warehouseLinkSupplierKeys(existingLink);
  const right = warehouseLinkSupplierKeys(incomingLink);
  if (!left.length && !right.length) return true;
  if (!left.length && right.length) return true;
  if (left.length && !right.length) return false;
  const rightSet = new Set(right);
  return left.some((key) => rightSet.has(key));
}

function warehouseLinksEqualForSave(a = {}, b = {}) {
  const left = normalizeWarehouseLink(a);
  const right = normalizeWarehouseLink(b);
  return warehouseLinkPrimaryTargetKey(a) === warehouseLinkPrimaryTargetKey(b)
    && left.keyword.toLowerCase() === right.keyword.toLowerCase()
    && warehouseLinksHaveCompatibleSupplierTarget(left, right);
}

function warehouseLinkMeaningfulSignature(input = {}) {
  const link = normalizeWarehouseLink(input);
  return JSON.stringify({
    primary: warehouseLinkPrimaryTargetKey(link),
    supplierTarget: warehouseLinkSupplierKeys(link).sort().join("|"),
    keyword: link.keyword.toLowerCase(),
    priceCurrency: link.priceCurrency,
    exactName: link.exactName.toLowerCase(),
    sourceRowId: link.sourceRowId,
  });
}

function warehouseProductLinkDetailsSignature(product = {}) {
  return compactWarehouseLinks(product.links || [])
    .map(warehouseLinkMeaningfulSignature)
    .sort()
    .join("||");
}

function mergeWarehouseLinkForSave(existing = {}, incoming = {}, { now = new Date().toISOString(), username = "system" } = {}) {
  const current = normalizeWarehouseLink(existing);
  const next = normalizeWarehouseLink(incoming);
  return normalizeWarehouseLink({
    ...current,
    ...next,
    id: current.id || next.id,
    article: current.article || next.article,
    exactName: current.exactName || next.exactName,
    sourceRowId: current.sourceRowId || next.sourceRowId,
    supplierName: next.supplierName || current.supplierName,
    partnerId: next.partnerId || current.partnerId,
    keyword: next.keyword || current.keyword,
    priceCurrency: next.priceCurrency || current.priceCurrency,
    createdAt: current.createdAt || next.createdAt || now,
    createdBy: current.createdBy || next.createdBy || username,
    updatedAt: now,
    updatedBy: username,
  });
}

function productHasSupplierLinks(product = {}) {
  return Array.isArray(product.links) && product.links.length > 0;
}

function warehouseLinkActivationRequestMeta(productIds = [], requestMeta = {}) {
  const ids = Array.isArray(productIds) ? productIds : [];
  return {
    ...requestMeta,
    immediate: ids.length > 0 && ids.length <= linkedActivationImmediateMaxScope,
  };
}

function stripStaleWarehouseSupplierSnapshot(raw = {}) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  const next = { ...raw };
  for (const key of [
    "selectedSupplier",
    "suppliers",
    "priceFormula",
    "ready",
    "changed",
    "nextPrice",
    "fallbackSuppliers",
    "selectedSupplierReason",
    "priceSource",
    "priceSelectionReason",
  ]) {
    delete next[key];
  }
  return next;
}

function repairWarehouseProductSupplierSnapshot(input = {}) {
  const normalized = normalizeWarehouseProduct(input);
  const links = Array.isArray(normalized.links) ? normalized.links : [];
  if (!links.length) return { product: normalized, repaired: false, reason: "no_links" };
  const raw = normalized.raw && typeof normalized.raw === "object" && !Array.isArray(normalized.raw)
    ? normalized.raw
    : {};
  const hasStaleSnapshot = Boolean(
    raw.selectedSupplier
    || (Array.isArray(raw.suppliers) && raw.suppliers.length)
    || raw.ready
    || raw.priceFormula
    || raw.nextPrice,
  );
  if (!hasStaleSnapshot) return { product: normalized, repaired: false, reason: "no_stale_snapshot" };
  const cleanedRaw = stripStaleWarehouseSupplierSnapshot(raw);
  return {
    product: normalizeWarehouseProduct({
      ...normalized,
      ...cleanedRaw,
      raw: cleanedRaw,
      selectedSupplier: null,
      suppliers: [],
    }),
    repaired: true,
    reason: "stripped_stale_supplier_snapshot",
  };
}

function resolveLightweightSelectedSupplier(product = {}, suppliers = []) {
  const matches = Array.isArray(suppliers) ? suppliers : (Array.isArray(product.suppliers) ? product.suppliers : []);
  return pickWarehouseSupplier(matches);
}

function compactWarehouseLinks(links = []) {
  const result = [];
  for (const input of Array.isArray(links) ? links : []) {
    const link = normalizeWarehouseLink(input);
    if (!warehouseLinkHasMatchTarget(link)) continue;
    const index = result.findIndex((existing) => warehouseLinksEqualForSave(existing, link));
    if (index < 0) {
      result.push(link);
      continue;
    }
    const existing = result[index];
    result[index] = normalizeWarehouseLink({
      ...existing,
      ...link,
      id: existing.id || link.id,
      article: existing.article || link.article,
      exactName: existing.exactName || link.exactName,
      sourceRowId: existing.sourceRowId || link.sourceRowId,
      supplierName: existing.supplierName || link.supplierName,
      partnerId: existing.partnerId || link.partnerId,
      keyword: existing.keyword || link.keyword,
      priceCurrency: existing.priceCurrency || link.priceCurrency,
      createdAt: existing.createdAt || link.createdAt,
      createdBy: existing.createdBy || link.createdBy,
      updatedAt: link.updatedAt || existing.updatedAt,
      updatedBy: link.updatedBy || existing.updatedBy,
    });
  }
  return result;
}

function warehouseProductHasLinks(product = {}, links = []) {
  const existing = compactWarehouseLinks(product.links || []);
  return compactWarehouseLinks(links).every((link) => existing.some((item) => warehouseLinksEqualForSave(item, link)));
}

function warehouseProductLinksSignature(product = {}) {
  return compactWarehouseLinks(product.links || [])
    .map((link) => warehouseLinkTargetKey(link))
    .sort()
    .join("||");
}

function warehouseGroupLinkSignature(products = []) {
  const rows = (Array.isArray(products) ? products : [])
    .map((product) => ({
      productId: product.id || "",
      offerId: product.offerId || "",
      marketplace: normalizeWarehouseProduct(product).marketplace || "",
      linkCount: compactWarehouseLinks(product.links || []).length,
      signature: warehouseProductLinksSignature(product),
    }));
  const signatures = Array.from(new Set(rows.map((row) => row.signature)));
  return {
    ok: signatures.length <= 1,
    signature: signatures[0] || "",
    uniqueCount: signatures.length,
    products: rows,
  };
}

