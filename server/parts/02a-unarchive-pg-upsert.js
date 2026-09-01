function productFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const imageState = row.images && typeof row.images === "object" && !Array.isArray(row.images) ? row.images : {};
  const rowName = cleanText(row.name);
  const rawName = cleanText(raw.name || raw.ozon?.name || raw.yandex?.name);
  const rowMarketplace = cleanText(row.marketplace || raw.marketplace || row.target || raw.target).toLowerCase();
  const effectiveName = isWeakProductName(rowName, row.offerId) && rawName && !isWeakProductName(rawName, row.offerId)
    ? rawName
    : rowName;
  const effectiveImageUrl = firstImageUrl(raw.imageUrl || raw.ozon?.primaryImage || raw.ozon?.images || raw.yandex?.pictures || imageState.imageUrl || imageState.images);
  const postgresLinksLoaded = Array.isArray(row.links);
  const postgresLinks = (postgresLinksLoaded ? row.links : []).map((link) => {
    const linkRaw = link.raw && typeof link.raw === "object" && !Array.isArray(link.raw) ? link.raw : {};
    return normalizeWarehouseLink({
      ...linkRaw,
      id: link.id,
      // Prefer typed columns over raw JSON — typed columns are authoritative after migration
      sourceRowId: link.sourceRowId || linkRaw.sourceRowId,
      exactName: link.exactName || linkRaw.exactName,
      article: linkRaw.article || link.supplierArticle,
      supplierName: link.supplierName,
      partnerId: link.partnerId,
      priceCurrency: link.priceCurrency,
      keyword: link.keyword,
      createdAt: link.createdAt ? link.createdAt.toISOString() : undefined,
      updatedAt: link.updatedAt ? link.updatedAt.toISOString() : undefined,
      createdBy: linkRaw.createdBy,
      updatedBy: linkRaw.updatedBy,
    });
  });
  const rawLinks = Array.isArray(raw.links) ? raw.links.map(normalizeWarehouseLink) : [];
  const links = postgresLinksLoaded
    ? (postgresLinks.length > 0 || rowMarketplace !== "yandex" ? postgresLinks : rawLinks)
    : rawLinks;
  const { product } = repairWarehouseProductSupplierSnapshot(normalizeWarehouseProduct({
    ...raw,
    id: row.id,
    marketplace: row.marketplace,
    target: row.target || row.marketplace,
    offerId: row.offerId,
    productId: row.productId,
    name: effectiveName,
    brand: row.brand || raw.brand,
    imageUrl: effectiveImageUrl,
    marketplacePrice: row.currentPrice ?? raw.marketplacePrice,
    currentPrice: row.currentPrice ?? raw.currentPrice ?? raw.marketplacePrice,
    targetPrice: row.targetPrice ?? raw.targetPrice ?? raw.nextPrice,
    targetStock: row.targetStock ?? raw.targetStock,
    marketplaceState: row.marketplaceState || raw.marketplaceState,
    status: row.status || raw.status,
    archived: row.archived ?? raw.archived,
    everHadLinks: Boolean(row.everHadLinks ?? raw.everHadLinks),
    links,
    createdAt: row.createdAt ? row.createdAt.toISOString() : raw.createdAt,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : raw.updatedAt,
  }));
  return product;
}

function warehouseProductPostgresUpdateData(data = {}) {
  // Preserve existing images when the incoming product has no image data — the
  // images column is a persistent cache (populated by backfill from Ozon API),
  // and reconciler/patch writes often lack ozon.images in their in-memory state.
  const hasImages = data.images && typeof data.images === "object" && Object.keys(data.images).length > 0;
  return {
    marketplace: data.marketplace,
    target: data.target,
    offerId: data.offerId,
    productId: data.productId,
    name: data.name,
    brand: data.brand,
    ...(hasImages ? { images: data.images } : {}),
    marketplaceState: data.marketplaceState,
    currentPrice: data.currentPrice,
    targetPrice: data.targetPrice,
    targetStock: data.targetStock,
    status: data.status,
    archived: data.archived,
    everHadLinks: data.everHadLinks,
    raw: data.raw,
    updatedAt: data.updatedAt,
  };
}

async function upsertWarehouseProductPostgres(client, product) {
  const closePrepareMarker = setEventLoopBlockMarker("pg_upsert_prepare");
  let data, imagesJson, marketplaceStateJson, rawJson;
  try {
    data = productToPostgresData(product);
    // Pre-serialize JSONB columns with V8 JSON.stringify so that Prisma's napi
    // layer receives plain strings — napi string-copy is O(n) text bytes vs.
    // O(n) object traversal which is 50-200× slower for nested objects.
    imagesJson = JSON.stringify(data.images || {});
    marketplaceStateJson = JSON.stringify(data.marketplaceState || {});
    rawJson = JSON.stringify(data.raw || {});
  } finally {
    closePrepareMarker();
  }
  const closeCallMarker = setEventLoopBlockMarker("pg_upsert_call");
  try {
    // $executeRawUnsafe keeps JSONB columns as pre-serialized strings.
    // images: only updated when the incoming product carries image data
    // (the images column is a persistent backfill cache).
    // created_at is NOT in the UPDATE SET — preserve the original creation date.
    await client.$executeRawUnsafe(
      `INSERT INTO warehouse_products
         (id, marketplace, target, offer_id, product_id, name, brand,
          images, marketplace_state, current_price, target_price, target_stock,
          status, archived, ever_had_links, raw, created_at, updated_at)
       VALUES
         ($1, $2::"Marketplace", $3, $4, $5, $6, $7,
          $8::jsonb, $9::jsonb, $10, $11, $12,
          $13, $14, $15, $16::jsonb, $17, $18)
       ON CONFLICT (id) DO UPDATE SET
         marketplace      = EXCLUDED.marketplace,
         target           = EXCLUDED.target,
         offer_id         = EXCLUDED.offer_id,
         product_id       = EXCLUDED.product_id,
         name             = EXCLUDED.name,
         brand            = EXCLUDED.brand,
         images           = CASE WHEN $8::jsonb != '{}'::jsonb
                              THEN EXCLUDED.images
                              ELSE warehouse_products.images END,
         marketplace_state = EXCLUDED.marketplace_state,
         current_price    = EXCLUDED.current_price,
         target_price     = EXCLUDED.target_price,
         target_stock     = EXCLUDED.target_stock,
         status           = EXCLUDED.status,
         archived         = EXCLUDED.archived,
         ever_had_links   = EXCLUDED.ever_had_links,
         raw              = EXCLUDED.raw,
         updated_at       = EXCLUDED.updated_at`,
      data.id,
      data.marketplace,
      data.target,
      data.offerId,
      data.productId,
      data.name,
      data.brand,
      imagesJson,
      marketplaceStateJson,
      data.currentPrice,
      data.targetPrice,
      data.targetStock,
      data.status,
      data.archived,
      data.everHadLinks,
      rawJson,
      data.createdAt,
      data.updatedAt,
    );
  } finally {
    closeCallMarker();
  }
}

async function runWithLimitedConcurrency(items = [], concurrency = 1, worker) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  const limit = Math.max(1, Math.min(list.length, Number(concurrency) || 1));
  const results = new Array(list.length);
  let nextIndex = 0;
  await Promise.all(Array.from({ length: limit }, async () => {
    while (nextIndex < list.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await worker(list[currentIndex], currentIndex);
    }
  }));
  return results;
}

