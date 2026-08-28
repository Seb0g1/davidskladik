function warehouseProductMatchesSearchQuery(product = {}, query = "") {
  const q = cleanText(query || "");
  if (!q) return true;
  if (isWarehouseArticleLikeQuery(q)) {
    const normalizedQuery = normalizeWarehouseSearchToken(q);
    return warehouseProductSearchIdentityTokens(product).some((token) => token === normalizedQuery);
  }
  const qLower = q.toLowerCase();
  const haystack = [
    product.id,
    product.offerId,
    product.name,
    resolveWarehouseBrand(product),
    product.categoryName,
    product.sku,
    product.barcode,
    ...(Array.isArray(product.links)
      ? product.links.flatMap((link) => [link.article, link.supplierName, link.partnerId, link.keyword, link.exactName, link.sourceRowId])
      : []),
  ]
    .map((value) => cleanText(value || "").toLowerCase())
    .join(" ");
  return haystack.includes(qLower);
}

function warehousePageProductMatches(product = {}, filters = {}) {
  if (!isWarehouseProductTargetEnabled(product)) return false;
  // Deletion-marker names ("удалить1212", "дубль" …) never surface in the catalog/search
  // (in-memory path; the Postgres path excludes them via duplicateNameExclusionWhere).
  if (isTrashNameProduct(product)) return false;
  if (!warehouseProductMatchesSearchQuery(product, filters.q || "")) return false;
  const linked = cleanText(filters.linked || "all");
  const hasLinks = Array.isArray(product.links) && product.links.length > 0;
  if (linked === "linked" && !hasLinks) return false;
  if (linked === "ready" && (!hasLinks || !product.ready)) return false;
  if (linked === "unlinked" && hasLinks) return false;
  if (linked === "changed" && (!hasLinks || !product.changed)) return false;
  if (linked === "linked_archived" && (!hasLinks || !productLooksArchived(product))) return false;
  const marketplace = cleanText(filters.marketplace || "all");
  if (marketplace !== "all" && cleanText(product.marketplace) !== marketplace) return false;
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all") {
    if (stateCode === "archived") {
      if (!productLooksArchived(product)) return false;
    } else if (cleanText(product.marketplaceState?.code) !== stateCode) {
      return false;
    }
  }
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter && !warehouseBrandMatches(product, brandFilter)) return false;
  if (filters.autoOnly && product.autoPriceEnabled === false) return false;
  return true;
}
