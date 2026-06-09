async function rebuildWarehouseBrandIndexPostgres(prisma, { limit = 100000 } = {}) {
  if (!prisma?.brandIndexItem) return { ok: false, skipped: true, reason: "brand_index_model_missing" };
  const rows = await prisma.warehouseProduct.findMany({
    where: enabledWarehouseTargetWhere(),
    select: { id: true, marketplace: true, target: true, offerId: true, productId: true, name: true, brand: true, raw: true },
    take: Math.max(100, Math.min(200000, Number(limit || 100000) || 100000)),
    orderBy: [{ updatedAt: "desc" }],
  });
  let indexed = 0;
  await prisma.brandIndexItem.deleteMany({});
  for (const batch of chunkArray(rows, 500)) {
    const data = batch.flatMap((row) => warehouseBrandIndexRowsForProduct(productFromPostgres({ ...row, links: [] })));
    if (!data.length) continue;
    const result = await prisma.brandIndexItem.createMany({ data, skipDuplicates: true });
    indexed += result.count || 0;
  }
  warehouseBrandListCache = null;
  return { ok: true, scanned: rows.length, indexed };
}

async function brandIndexProductIdsForFilterPostgres(prisma, brandFilter = "") {
  const normalizedBrand = normalizedBrandIndexKey(brandFilter);
  if (!normalizedBrand || !prisma?.brandIndexItem) return [];
  let rows = await prisma.brandIndexItem.findMany({
    where: { normalizedBrand: { contains: normalizedBrand, mode: "insensitive" } },
    select: { productId: true },
    take: 50000,
  });
  if (!rows.length) {
    await rebuildWarehouseBrandIndexPostgres(prisma, { limit: Number(process.env.WAREHOUSE_BRAND_INDEX_REBUILD_LIMIT || 100000) || 100000 });
    rows = await prisma.brandIndexItem.findMany({
      where: { normalizedBrand: { contains: normalizedBrand, mode: "insensitive" } },
      select: { productId: true },
      take: 50000,
    });
  }
  return Array.from(new Set(rows.map((row) => row.productId).filter(Boolean)));
}

function warehouseBrandSearchHaystack(product = {}) {
  return [
    ...resolveWarehouseBrandCandidates(product),
    product.name,
    product.ozon?.name,
    product.yandex?.name,
    flattenAttributeText(product.ozon?.attributes),
    flattenAttributeText(product.yandex?.attributes),
  ]
    .map((value) => normalizeSearchText(value))
    .filter(Boolean)
    .join(" ");
}

function warehouseBrandDeepHaystack(product = {}) {
  const parts = [];
  const visit = (value, key = "", depth = 0) => {
    if (depth > 6 || value === null || value === undefined) return;
    const normalizedKey = normalizeSearchText(key);
    const keyLooksLikeBrand =
      normalizedKey.includes("brand")
      || normalizedKey.includes("vendor")
      || normalizedKey.includes("manufacturer")
      || normalizedKey.includes("trademark")
      || normalizedKey.includes("бренд")
      || normalizedKey.includes("производитель");
    if (keyLooksLikeBrand && (typeof value === "string" || typeof value === "number")) {
      parts.push(value);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item) => visit(item, key, depth + 1));
      return;
    }
    if (typeof value === "object") {
      Object.entries(value).forEach(([childKey, childValue]) => visit(childValue, childKey, depth + 1));
    }
  };
  visit(product);
  return parts.map((value) => normalizeSearchText(value)).filter(Boolean).join(" ");
}

function warehouseBrandMatches(product = {}, brandFilter = "") {
  const needle = normalizeSearchText(brandFilter);
  if (!needle) return true;
  if (warehouseBrandSearchHaystack(product).includes(needle)) return true;
  if (process.env.WAREHOUSE_ENABLE_DEEP_BRAND_SCAN !== "true") return false;
  return warehouseBrandDeepHaystack(product).includes(needle);
}
