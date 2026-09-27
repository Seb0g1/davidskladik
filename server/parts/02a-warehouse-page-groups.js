function warehouseProductPageGroupKey(product = {}, groupContext = null) {
  const normalized = normalizeWarehouseProductForGrouping(product);
  const raw = normalized.raw && typeof normalized.raw === "object" && !Array.isArray(normalized.raw) ? normalized.raw : {};
  const manualGroupId = cleanText(normalized.manualGroupId || normalized.manual_group_id || raw.manualGroupId || raw.manual_group_id).toLowerCase();
  if (manualGroupId && !manualGroupId.startsWith("auto-pair-")) return `manual:${manualGroupId}`;

  const offerId = cleanText(normalized.offerId || normalized.offer_id).toLowerCase();
  if (offerId && groupContext?.pairedOfferIds?.has(offerId)) return `offer:${offerId}`;

  const marketplace = cleanText(normalized.marketplace).toLowerCase();
  const ozonId = cleanText(normalized.id).toLowerCase();
  if (marketplace === "ozon" && ozonId && groupContext?.ozonIdsReferencedByYandex?.has(ozonId)) {
    return `pair:${ozonId}`;
  }

  const pairOzonId = resolveWarehouseProductPairOzonId(normalized);
  if (pairOzonId) return `pair:${pairOzonId.toLowerCase()}`;

  if (offerId) return `offer:${offerId}`;
  return "";
}

function addWarehousePageGroupSiblings(sourceProducts = [], pageProducts = []) {
  // Grouping views of every product, built once for this call (see
  // normalizeWarehouseProductForGrouping); the original objects are what gets returned.
  const pageViews = (pageProducts || []).map(normalizeWarehouseProductForGrouping);
  const sourceList = (sourceProducts || []).filter((product) => product?.id);
  const sourceViews = sourceList.map(normalizeWarehouseProductForGrouping);
  const groupContext = buildWarehouseCatalogGroupContext([...pageViews, ...sourceViews]);
  const { groupKeys, pairOzonIds } = collectWarehouseGroupExpansionKeys(pageViews, groupContext);
  if (!groupKeys.size && !pairOzonIds.size) return pageProducts || [];
  const byId = new Map();
  for (const product of pageProducts || []) {
    if (product?.id) byId.set(String(product.id), product);
  }
  for (let index = 0; index < sourceList.length; index += 1) {
    if (warehouseProductSharesGroup(sourceViews[index], groupContext, groupKeys, pairOzonIds)) {
      byId.set(String(sourceList[index].id), sourceList[index]);
    }
  }
  return Array.from(byId.values());
}

// Grouping views of a whole catalog, built once and reused for many seeds (the autocart expands
// every order line). Valid only for the same products array; see supplierCartGroupingIndex.
function buildWarehouseGroupingIndex(sourceProducts = []) {
  const source = Array.isArray(sourceProducts) ? sourceProducts : [];
  const sourceList = source.filter((product) => product?.id);
  const sourceViews = sourceList.map(normalizeWarehouseProductForGrouping);
  return {
    source,
    length: source.length,
    sourceList,
    sourceViews,
    sourceIds: new Set(sourceList.map((product) => String(product.id))),
    groupContext: buildWarehouseCatalogGroupContext(sourceViews),
    builtAt: Date.now(),
  };
}

function expandWarehouseProductsToGroups(sourceProducts = [], seedProducts = [], { index = null } = {}) {
  const seeds = Array.isArray(seedProducts) ? seedProducts : [];
  const seedIds = new Set(seeds.map((product) => String(product?.id || "")).filter(Boolean));
  // The prebuilt index applies when it was built from this very array and already contains the
  // seeds (then its context is exactly the one built from seeds + source below).
  const useIndex = Boolean(index && index.source === sourceProducts && index.length === (sourceProducts || []).length
    && [...seedIds].every((id) => index.sourceIds.has(id)));
  const sourceList = useIndex ? index.sourceList : (Array.isArray(sourceProducts) ? sourceProducts : []).filter((product) => product?.id);
  const seedViews = seeds.map(normalizeWarehouseProductForGrouping);
  const sourceViews = useIndex ? index.sourceViews : sourceList.map(normalizeWarehouseProductForGrouping);
  const groupContext = useIndex ? index.groupContext : buildWarehouseCatalogGroupContext([...seedViews, ...sourceViews]);
  const { groupKeys, pairOzonIds } = collectWarehouseGroupExpansionKeys(seedViews, groupContext);
  const byId = new Map();
  for (let index = 0; index < sourceList.length; index += 1) {
    const id = String(sourceList[index].id);
    if (seedIds.has(id) || warehouseProductSharesGroup(sourceViews[index], groupContext, groupKeys, pairOzonIds)) {
      byId.set(id, sourceList[index]);
    }
  }
  return Array.from(byId.values());
}

function warehouseProductsForGroupKey(sourceProducts = [], groupKey = "") {
  const key = cleanText(groupKey);
  if (!key) return [];
  const list = Array.isArray(sourceProducts) ? sourceProducts : [];
  const views = list.map(normalizeWarehouseProductForGrouping);
  const groupContext = buildWarehouseCatalogGroupContext(views);
  return list.filter((product, index) => warehouseProductPageGroupKey(views[index], groupContext) === key);
}

function syncWarehouseProductGroupLinks(products = [], { now = new Date().toISOString(), username = "system" } = {}) {
  const targetProducts = Array.isArray(products) ? products.filter((product) => product?.id) : [];
  const commonLinks = buildCommonWarehouseGroupLinks(targetProducts, [], { now, username });
  const changedProducts = [];
  const oldValues = [];
  for (const product of targetProducts) {
    const beforeSignature = warehouseProductLinkDetailsSignature(product);
    const beforeValue = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    product.links = commonLinks.map((link) => normalizeWarehouseLink({
      ...link,
      createdAt: link.createdAt || now,
      updatedAt: now,
      createdBy: link.createdBy || username,
      updatedBy: username,
    }));
    if (commonLinks.length) {
      product.autoPriceEnabled = true;
      product.everHadLinks = true;
    }
    if (warehouseProductLinkDetailsSignature(product) !== beforeSignature) {
      product.updatedAt = now;
      changedProducts.push(product);
      oldValues.push(beforeValue);
    }
  }
  return {
    products: targetProducts,
    changedProducts,
    changedIds: changedProducts.map((product) => product.id),
    oldValues,
    commonLinks,
    groupLinkSignature: warehouseGroupLinkSignature(targetProducts),
  };
}

function buildWarehousePageProductGroups(products = []) {
  const groupContext = buildWarehouseCatalogGroupContext(products);
  const groups = new Map();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = mapWarehousePageItemFromProduct(product);
    const groupKey = warehouseProductPageGroupKey(normalized, groupContext) || `id:${normalized.id}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        groupKey,
        offerId: normalized.offerId || "",
        manualGroupId: normalized.manualGroupId || normalized.raw?.manualGroupId || "",
        name: normalized.name || normalized.offerId || normalized.id,
        brand: resolveWarehouseBrand(normalized) || normalized.brand || "",
        imageUrl: normalized.imageUrl || "",
        marketplaces: [],
        products: [],
        links: [],
        statusSummary: {
          total: 0,
          linked: 0,
          archived: 0,
          ready: 0,
          changed: 0,
          withoutSupplier: 0,
          marketplaces: [],
        },
      });
    }
    const group = groups.get(groupKey);
    group.products.push(normalized);
    if (!group.imageUrl && normalized.imageUrl) group.imageUrl = normalized.imageUrl;
    if (!group.brand && (resolveWarehouseBrand(normalized) || normalized.brand)) group.brand = resolveWarehouseBrand(normalized) || normalized.brand;
    const marketplaceRaw = cleanText(normalized.marketplace || normalized.target || "marketplace").toLowerCase();
    const marketplace = marketplaceRaw.includes("ozon") ? "Ozon" : marketplaceRaw.includes("yandex") ? "Yandex" : marketplaceRaw;
    if (marketplace && !group.marketplaces.includes(marketplace)) group.marketplaces.push(marketplace);
    for (const link of normalized.links || []) {
      const linkKey = warehouseLinkTargetKey(link);
      if (!group.links.some((item) => warehouseLinkTargetKey(item) === linkKey)) {
        group.links.push(link);
      }
    }
    const archived = Boolean(productLooksArchived(normalized));
    const linked = (Array.isArray(normalized.links) && normalized.links.length > 0) || group.links.length > 0;
    const hasSupplier = Boolean(normalized.selectedSupplier)
      || Boolean(normalized.stockOnlyFallbackActive)
      || Boolean(displaySupplierFromProductLinks(normalized));
    const sellable = hasSupplier && Number(normalized.targetStock || normalized.stock || normalized.marketplaceState?.stock || 0) > 0;
    const ready = linked && !archived && sellable;
    const changed = Number(normalized.nextPrice || normalized.newPrice || normalized.targetPrice || 0) > 0
      && Number(normalized.marketplacePrice || normalized.currentPrice || 0) !== Number(normalized.nextPrice || normalized.newPrice || normalized.targetPrice || 0);
    group.statusSummary.total += 1;
    if (linked) group.statusSummary.linked += 1;
    if (archived) group.statusSummary.archived += 1;
    if (ready) group.statusSummary.ready += 1;
    if (changed) group.statusSummary.changed += 1;
    if (!hasSupplier && linked) group.statusSummary.withoutSupplier += 1;
    group.statusSummary.marketplaces = group.marketplaces;
  }
  return Array.from(groups.values()).map((group) => ({
    ...group,
    marketplaces: group.marketplaces.sort(),
    products: group.products.sort((a, b) => String(a.marketplace || "").localeCompare(String(b.marketplace || "")) || String(a.target || "").localeCompare(String(b.target || ""))),
  }));
}

function linkedRecoveryCandidateProducts(products = [], limit = 30000) {
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 30000) || 30000)));
  const rows = (Array.isArray(products) ? products : [])
    .filter((product) => product?.id)
    .map(normalizeWarehouseProduct);
  const groupContext = buildWarehouseCatalogGroupContext(rows);
  const linkedByGroup = new Map();
  for (const product of rows) {
    if (!Array.isArray(product.links) || !product.links.length) continue;
    const groupKey = warehouseProductPageGroupKey(product, groupContext) || `id:${product.id}`;
    if (!linkedByGroup.has(groupKey)) linkedByGroup.set(groupKey, product);
  }
  if (!linkedByGroup.size) return [];

  const byId = new Map();
  for (const product of rows) {
    const groupKey = warehouseProductPageGroupKey(product, groupContext) || `id:${product.id}`;
    const donor = linkedByGroup.get(groupKey);
    if (!donor) continue;
    const links = Array.isArray(product.links) && product.links.length
      ? product.links
      : donor.links;
    byId.set(String(product.id), normalizeWarehouseProduct({
      ...product,
      links,
    }));
    if (byId.size >= max) break;
  }

  return Array.from(byId.values());
}
