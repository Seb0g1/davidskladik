function warehousePostgresWriteConcurrency() {
  return Math.max(1, Math.min(2, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CONCURRENCY || 1) || 1));
}

async function replaceProductLinksInPostgres(prisma, products = []) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return { products: 0, links: 0 };
  let linksWritten = 0;
  const chunkSize = Math.max(1, Math.min(100, Number(process.env.WAREHOUSE_POSTGRES_LINK_WRITE_CHUNK_SIZE || 50) || 50));
  const writeConcurrency = warehousePostgresWriteConcurrency();
  for (const productChunk of chunkArray(normalizedProducts, chunkSize)) {
    await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
      await upsertWarehouseProductPostgres(prisma, product);
    });
    const productIds = productChunk.map((product) => product.id).filter(Boolean);
    const linkRows = [];
    for (const product of productChunk) {
      linkRows.push(...(product.links || [])
        .map((link) => linkToPostgresData(product, link))
        .filter((linkData) => linkData.supplierArticle));
    }
    if (productIds.length) {
      await prisma.productLink.deleteMany({ where: { productId: { in: productIds } } });
    }
    for (const linkChunk of chunkArray(dedupeProductLinkRows(linkRows), 1000)) {
      if (!linkChunk.length) continue;
      const result = await prisma.productLink.createMany({ data: linkChunk, skipDuplicates: true });
      linksWritten += result.count || 0;
    }
    markWarehousePostgresProductsWritten(productChunk);
  }
  return { products: normalizedProducts.length, links: linksWritten };
}

async function getWarehousePostgresSuppliers(prisma) {
  if (
    warehousePostgresSuppliersCache
    && Date.now() - warehousePostgresSuppliersCache.at < warehousePostgresDetailCacheTtlMs
  ) {
    return warehousePostgresSuppliersCache.value;
  }
  const suppliers = await prisma.managedSupplier.findMany({ orderBy: { name: "asc" } });
  const normalized = suppliers.map(supplierFromPostgres);
  warehousePostgresSuppliersCache = { at: Date.now(), value: normalized };
  return normalized;
}

function warehousePostgresDetailCachePeek(key) {
  const cacheKey = cleanText(key);
  if (!cacheKey) return null;
  const cached = warehousePostgresDetailCache.get(cacheKey);
  if (cached && Date.now() - cached.at < warehousePostgresDetailCacheTtlMs) return cloneAuditValue(cached.value);
  return null;
}

async function peekWarehouseGroupDetailCached(groupKey, { usdRate, refreshPrices = false } = {}) {
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const cacheKey = `group:${groupKey}:${rate}:all-marketplaces:${refreshPrices ? "live" : "stored"}`;
  return warehousePostgresDetailCachePeek(cacheKey);
}

function warehousePostgresCachedDetail(key, build) {
  const cacheKey = cleanText(key);
  if (!cacheKey) return build();
  const cached = warehousePostgresDetailCachePeek(cacheKey);
  if (cached) return cached;
  const inflight = warehousePostgresDetailInflight.get(cacheKey);
  if (inflight) return inflight;
  const promise = Promise.resolve(build()).then((value) => {
    if (value) warehousePostgresDetailCache.set(cacheKey, { at: Date.now(), value: cloneAuditValue(value) });
    return value;
  }).finally(() => {
    warehousePostgresDetailInflight.delete(cacheKey);
  });
  warehousePostgresDetailInflight.set(cacheKey, promise);
  return promise;
}

