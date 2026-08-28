async function ensureWarehousePostgresLinksBackfilled(prisma) {
  if (warehousePostgresLinkBackfillDone) return { created: 0, skipped: true };
  if (warehousePostgresLinkBackfillPromise) return warehousePostgresLinkBackfillPromise;
  const deferThreshold = Math.max(10000, Number(process.env.WAREHOUSE_LINK_BACKFILL_DEFER_THRESHOLD || 50000) || 50000);
  const productCount = await prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }).catch(() => 0);
  if (productCount > deferThreshold && process.env.WAREHOUSE_LINK_BACKFILL_DEFER !== "false") {
    logger.warn("warehouse postgres link backfill deferred", { productCount, deferThreshold });
    return { created: 0, skipped: true, deferred: true };
  }
  warehousePostgresLinkBackfillPromise = (async () => {
    const productsWithRawLinksPromise = prisma.$queryRaw`
      SELECT id, raw
      FROM "warehouse_products"
      WHERE jsonb_typeof(raw->'links') = 'array'
        AND jsonb_array_length(raw->'links') > 0
    `.catch((error) => {
      logger.warn("warehouse postgres raw-link prefilter failed, using full backfill scan", { detail: error?.message || String(error) });
      return prisma.warehouseProduct.findMany({ select: { id: true, raw: true } });
    });
    const [products, existingLinks] = await Promise.all([
      productsWithRawLinksPromise,
      prisma.productLink.findMany({
        select: {
          productId: true,
          supplierArticle: true,
          supplierName: true,
          partnerId: true,
          keyword: true,
          priceCurrency: true,
        },
      }),
    ]);
    const existingByProduct = new Map();
    for (const link of existingLinks) {
      if (!link.productId) continue;
      if (!existingByProduct.has(link.productId)) existingByProduct.set(link.productId, new Set());
      existingByProduct.get(link.productId).add(warehouseLinkIdentityKey({
        article: link.supplierArticle,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        keyword: link.keyword,
        priceCurrency: link.priceCurrency,
      }));
    }
    const rows = [];
    for (const product of products) {
      const raw = product.raw && typeof product.raw === "object" && !Array.isArray(product.raw) ? product.raw : {};
      const rawLinks = Array.isArray(raw.links) ? raw.links : [];
      const existingKeys = existingByProduct.get(product.id) || new Set();
      for (const rawLink of rawLinks) {
        const row = linkToPostgresData({ id: product.id }, rawLink);
        if (!row.supplierArticle) continue;
        const identity = warehouseLinkIdentityKey({
          article: row.supplierArticle,
          supplierName: row.supplierName,
          partnerId: row.partnerId,
          keyword: row.keyword,
          priceCurrency: row.priceCurrency,
        });
        if (existingKeys.has(identity)) continue;
        existingKeys.add(identity);
        rows.push(row);
      }
    }
    let created = 0;
    const chunkSize = 1000;
    for (let index = 0; index < rows.length; index += chunkSize) {
      const batch = rows.slice(index, index + chunkSize);
      if (!batch.length) continue;
      const result = await prisma.productLink.createMany({ data: batch, skipDuplicates: true });
      created += result.count || 0;
    }
    warehousePostgresLinkBackfillDone = true;
    if (created) logger.info("warehouse postgres links backfilled from raw", { created });
    return { created, skipped: false };
  })().catch((error) => {
    warehousePostgresLinkBackfillPromise = null;
    logger.warn("warehouse postgres links backfill failed", { detail: error?.message || String(error) });
    return { created: 0, skipped: false, error: error?.message || String(error) };
  });
  return warehousePostgresLinkBackfillPromise;
}

function kickWarehousePostgresLinksBackfill(prisma) {
  if (!prisma || warehousePostgresLinkBackfillDone || warehousePostgresLinkBackfillPromise) return;
  void ensureWarehousePostgresLinksBackfilled(prisma).catch((error) => {
    logger.warn("warehouse postgres links backfill background failed", { detail: error?.message || String(error) });
  });
}
