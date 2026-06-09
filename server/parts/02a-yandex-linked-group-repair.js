async function syncLinkedWarehouseGroupLinksPostgres(prisma, {
  dryRun = false,
  batchSize = 500,
  limit = 0,
  onProgress,
} = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, reason: "postgres_unavailable" };
  const linkedRows = await client.warehouseProduct.findMany({
    where: { links: { some: {} } },
    select: { id: true },
    orderBy: { id: "asc" },
  });
  let linkedIds = linkedRows.map((row) => cleanText(row.id)).filter(Boolean);
  if (limit > 0) linkedIds = linkedIds.slice(0, limit);
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  const processedGroupKeys = new Set();
  const changedProducts = [];
  let processedGroups = 0;
  let repairedGroups = 0;
  let skippedGroups = 0;

  for (let index = 0; index < linkedIds.length; index += normalizedBatchSize) {
    const idChunk = linkedIds.slice(index, index + normalizedBatchSize);
    const seedRows = await client.warehouseProduct.findMany({
      where: { id: { in: idChunk } },
      include: { links: true },
    });
    const seeds = seedRows.map(productFromPostgres);
    const siblings = await readWarehouseGroupSiblingsFromPostgres(seeds);
    const allProducts = Array.from(new Map(
      [...seeds, ...siblings].map((product) => [String(product.id), product]),
    ).values());
    const groups = collectWarehouseLinkRepairGroups(allProducts);

    for (const [groupKey, products] of groups) {
      if (processedGroupKeys.has(groupKey)) continue;
      processedGroupKeys.add(groupKey);
      processedGroups += 1;
      const hasLinks = products.some((product) => (product.links || []).length > 0);
      if (!hasLinks) {
        skippedGroups += 1;
        continue;
      }
      const before = warehouseGroupLinkSignature(products);
      const pairPatches = applyOzonYandexPairGroupIds(products);
      const mergedProducts = Array.from(new Map(
        [...products, ...pairPatches].map((product) => [String(product.id), product]),
      ).values());
      const syncResult = syncWarehouseProductGroupLinks(mergedProducts, { username: "catalog_repair" });
      const updates = Array.from(new Map(
        [...pairPatches, ...(syncResult.changedProducts || [])].map((product) => [String(product.id), product]),
      ).values());
      const after = warehouseGroupLinkSignature(mergedProducts);
      if (!updates.length && before.ok) {
        skippedGroups += 1;
        continue;
      }
      if (!updates.length && !before.ok) {
        skippedGroups += 1;
        continue;
      }
      repairedGroups += 1;
      changedProducts.push(...updates);
      logger.info("warehouse linked group repaired", {
        groupKey,
        productIds: products.map((product) => product.id),
        beforeOk: before.ok,
        afterOk: after.ok,
        changed: updates.length,
      });
    }

    await onProgress?.({
      progress: 10 + ((index + idChunk.length) / Math.max(1, linkedIds.length)) * 85,
      summary: `Checked ${Math.min(index + idChunk.length, linkedIds.length)} of ${linkedIds.length} linked products; repaired groups ${repairedGroups}.`,
    });
  }

  const uniqueChanged = Array.from(new Map(changedProducts.map((product) => [String(product.id), product])).values());
  if (!dryRun && uniqueChanged.length) {
    for (const chunk of chunkArray(uniqueChanged, 100)) {
      await replaceProductLinksInPostgres(client, chunk);
      mergeWarehouseProductsIntoMemory(chunk);
    }
    await queueLinkedProductActivation(uniqueChanged.map((product) => product.id), "link_catalog_repair", { username: "catalog_repair" });
    invalidateWarehouseViewCache();
  }

  return {
    ok: true,
    dryRun,
    linkedProducts: linkedIds.length,
    processedGroups,
    repairedGroups,
    skippedGroups,
    changedProducts: uniqueChanged.length,
    changedProductIds: uniqueChanged.map((product) => product.id),
  };
}

async function repairLinkedWarehouseCatalogPostgres(prisma, {
  dryRun = false,
  batchSize = 500,
  limit = 0,
  onProgress,
} = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, reason: "postgres_unavailable" };

  await onProgress?.({ progress: 5, summary: "Backfilling Ozon/Yandex pair groups..." });
  const pairGroups = await backfillOzonYandexManualGroupsPostgres(client, { dryRun, batchSize });
  const ozonGroups = await syncOzonManualGroupsFromYandexPostgres(client, { dryRun, batchSize });

  await onProgress?.({ progress: 20, summary: "Syncing PriceMaster links across marketplace pairs..." });
  const linkPairs = await syncOzonYandexLinkPairsPostgres(client, { dryRun, batchSize });

  await onProgress?.({ progress: 35, summary: "Repairing linked warehouse catalog groups..." });
  const linkSync = await syncLinkedWarehouseGroupLinksPostgres(client, { dryRun, batchSize, limit, onProgress: async (progress = {}) => {
    await onProgress?.({
      progress: 35 + (Number(progress.progress || 0) * 0.6),
      summary: progress.summary || "Repairing linked warehouse catalog groups...",
    });
  } });

  const summary = [
    `Pairs grouped: ${pairGroups.updated || 0}`,
    `Ozon groups synced: ${ozonGroups.updated || 0}`,
    `Pair link sync: ${linkPairs.synced || 0}`,
    `Catalog groups repaired: ${linkSync.repairedGroups || 0}`,
    `Changed products: ${linkSync.changedProducts || 0}`,
  ].join("; ");

  return {
    ok: true,
    dryRun,
    pairGroups,
    ozonGroups,
    linkPairs,
    linkSync,
    processedGroups: linkSync.processedGroups || 0,
    repairedGroups: linkSync.repairedGroups || 0,
    changedProducts: linkSync.changedProducts || 0,
    changedProductIds: linkSync.changedProductIds || [],
    skippedGroups: linkSync.skippedGroups || 0,
    summary,
  };
}

