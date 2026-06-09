function normalizeWarehouseSearchToken(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[\s\-_/\\.:;#№]+/g, "");
}

function isWarehouseArticleLikeQuery(query) {
  const text = cleanText(query);
  if (text.length < 2) return false;
  if (/\s/.test(text)) return false;
  return /\d/.test(text) || /[\-_/\\#№]/.test(text);
}

function isWarehouseStrictIdentitySearch(filters = {}) {
  return isWarehouseArticleLikeQuery(filters.q || "");
}

function warehouseProductSearchIdentityTokens(product = {}) {
  const links = Array.isArray(product.links) ? product.links : [];
  return [
    product.id,
    product.offerId,
    product.productId,
    product.sku,
    product.barcode,
    product.ozon?.offerId,
    product.ozon?.productId,
    product.ozon?.sku,
    product.ozon?.barcode,
    product.yandex?.offerId,
    product.yandex?.productId,
    product.yandex?.sku,
    product.yandex?.barcode,
    ...links.flatMap((link) => [link.article, link.sourceRowId]),
  ]
    .map(normalizeWarehouseSearchToken)
    .filter(Boolean);
}

function warehouseProductSearchRank(product = {}, query = "") {
  const normalizedQuery = normalizeWarehouseSearchToken(query);
  if (!normalizedQuery) return 999;
  const groups = [
    [product.offerId, product.id, product.productId],
    [product.sku, product.barcode, product.ozon?.offerId, product.yandex?.offerId],
    [product.ozon?.productId, product.yandex?.productId, product.ozon?.sku, product.yandex?.sku, product.ozon?.barcode, product.yandex?.barcode],
    ...(Array.isArray(product.links)
      ? product.links.map((link) => [link.article, link.sourceRowId])
      : []),
  ];
  for (let index = 0; index < groups.length; index += 1) {
    if (groups[index].some((value) => normalizeWarehouseSearchToken(value) === normalizedQuery)) return index;
  }
  return 999;
}

function sortWarehouseProductsForSearch(products = [], filters = {}) {
  if (!isWarehouseStrictIdentitySearch(filters)) return products;
  const query = filters.q || "";
  return [...products].sort((left, right) => {
    const rankDiff = warehouseProductSearchRank(left, query) - warehouseProductSearchRank(right, query);
    if (rankDiff) return rankDiff;
    return String(left.offerId || left.name || left.id || "").localeCompare(
      String(right.offerId || right.name || right.id || ""),
      "ru",
      { sensitivity: "base" },
    );
  });
}

function preferWarehousePrimaryIdentityMatches(products = [], filters = {}) {
  const rows = Array.isArray(products) ? products : [];
  if (!isWarehouseStrictIdentitySearch(filters)) return rows;
  const query = filters.q || "";
  const primaryMatches = rows.filter((product) => warehouseProductSearchRank(product, query) <= 2);
  return primaryMatches.length ? primaryMatches : rows;
}

