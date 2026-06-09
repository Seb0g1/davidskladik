function collectWarehouseLinkRepairGroups(products = []) {
  const groupContext = buildWarehouseCatalogGroupContext(products);
  const groups = new Map();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const key = warehouseProductPageGroupKey(normalized, groupContext);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(normalized);
  }
  return groups;
}

function applyOzonYandexPairGroupIds(products = []) {
  const list = Array.isArray(products) ? products.map(normalizeWarehouseProduct) : [];
  const ozon = list.find((product) => cleanText(product.marketplace).toLowerCase() === "ozon");
  const yandex = list.find((product) => cleanText(product.marketplace).toLowerCase() === "yandex");
  if (!ozon || !yandex) return [];
  const groupId = buildOzonYandexAutoPairGroupId(ozon);
  if (!groupId) return [];
  const patches = [];
  for (const product of list) {
    const raw = product.raw && typeof product.raw === "object" && !Array.isArray(product.raw) ? product.raw : {};
    const current = cleanText(product.manualGroupId || raw.manualGroupId);
    if (current !== groupId) patches.push(normalizeWarehouseProduct({ ...product, manualGroupId: groupId }));
  }
  return patches;
}

async function syncOzonYandexLinkPairsPostgres(prisma, { dryRun = false, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, synced: 0, reason: "postgres_unavailable" };
  const [yandexRows, ozonRows] = await Promise.all([
    client.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      include: { links: true },
    }),
    client.warehouseProduct.findMany({
      where: { marketplace: "ozon" },
      include: { links: true },
    }),
  ]);
  const yandexProducts = yandexRows.map(productFromPostgres);
  const ozonProducts = ozonRows.map(productFromPostgres);
  const yandexByOffer = new Map();
  for (const yandex of yandexProducts) {
    const offerId = cleanText(yandex.offerId).toLowerCase();
    if (!offerId || yandexByOffer.has(offerId)) continue;
    yandexByOffer.set(offerId, yandex);
  }
  const ozonIndexes = buildOzonProductLookupIndexes(ozonProducts);
  const linkedOzon = ozonProducts.filter((product) => (product.links || []).length);
  const linkedYandex = yandexProducts.filter((product) => (product.links || []).length);
  const pairs = collectOzonYandexPairsFromProducts(linkedOzon, linkedYandex, { yandexByOffer, ozonIndexes });
  let synced = 0;
  let scanned = pairs.length;
  const updates = [];
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  for (const chunk of chunkArray(pairs, normalizedBatchSize)) {
    for (const [ozon, yandex] of chunk) {
      const groupProducts = [ozon, yandex];
      const pairPatches = applyOzonYandexPairGroupIds(groupProducts);
      const mergedProducts = Array.from(new Map(
        [...groupProducts, ...pairPatches].map((product) => [String(product.id), product]),
      ).values());
      const group = syncWarehouseProductGroupLinks(mergedProducts, { username: "pair_sync" });
      const changed = Array.from(new Map(
        [...pairPatches, ...(group.changedProducts || [])].map((product) => [String(product.id), product]),
      ).values());
      if (!changed.length) continue;
      synced += changed.length;
      updates.push(...changed);
    }
    if (!dryRun && updates.length) {
      for (const writeChunk of chunkArray(updates.splice(0, updates.length), 100)) {
        await replaceProductLinksInPostgres(client, writeChunk);
        mergeWarehouseProductsIntoMemory(writeChunk);
      }
    }
  }
  if (!dryRun && synced > 0) invalidateWarehouseViewCache();
  return { ok: true, dryRun, synced, scanned, pairs: pairs.length };
}

