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
      article: linkRaw.article || (linkRaw.matchType ? "" : link.supplierArticle),
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
  return {
    marketplace: data.marketplace,
    target: data.target,
    offerId: data.offerId,
    productId: data.productId,
    name: data.name,
    brand: data.brand,
    images: data.images,
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
  // Микро-маркеры для event_loop_blocked: делят стоимость upsert'а на
  // подготовку данных (normalize) и вызов Prisma (сериализация Json-полей +
  // движок) — бисекция остаточных 7-10 с блокировок worker.
  const closePrepareMarker = setEventLoopBlockMarker("pg_upsert_prepare");
  let data;
  try {
    data = productToPostgresData(product);
  } finally {
    closePrepareMarker();
  }
  const closeCallMarker = setEventLoopBlockMarker("pg_upsert_call");
  try {
    await client.warehouseProduct.upsert({
      where: { id: data.id },
      create: data,
      update: warehouseProductPostgresUpdateData(data),
    });
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

