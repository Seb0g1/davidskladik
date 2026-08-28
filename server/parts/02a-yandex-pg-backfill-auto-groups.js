async function backfillUnpairedYandexAutoGroupsPostgres(prisma, { dryRun = true, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, paired: 0, updated: 0, scanned: 0, reason: "postgres_unavailable" };
  const ozonRows = await client.warehouseProduct.findMany({
    where: { marketplace: "ozon" },
    include: { links: true },
  });
  const ozonIndexes = buildOzonProductLookupIndexes(ozonRows.map(productFromPostgres));
  let paired = 0;
  let updated = 0;
  let scanned = 0;
  let skipped = 0;
  let cursorId = "";
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  const patches = [];

  while (true) {
    const yandexRows = await client.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      include: { links: true },
      orderBy: { id: "asc" },
      take: normalizedBatchSize,
      ...(cursorId ? { skip: 1, cursor: { id: cursorId } } : {}),
    });
    if (!yandexRows.length) break;
    cursorId = yandexRows[yandexRows.length - 1].id;

    for (const row of yandexRows) {
      scanned += 1;
      const yandex = productFromPostgres(row);
      const groupId = cleanText(yandex.manualGroupId || yandex.raw?.manualGroupId);
      if (groupId.startsWith("auto-pair-")) {
        skipped += 1;
        continue;
      }
      const ozon = findOzonMatchForYandexProduct(yandex, ozonIndexes);
      if (!ozon) {
        skipped += 1;
        continue;
      }
      paired += 1;
      const pairGroupId = buildOzonYandexAutoPairGroupId(ozon);
      if (!pairGroupId) {
        skipped += 1;
        continue;
      }
      const yandexGroup = cleanText(yandex.manualGroupId || yandex.raw?.manualGroupId);
      const ozonGroup = cleanText(ozon.manualGroupId || ozon.raw?.manualGroupId);
      if (yandexGroup === pairGroupId && ozonGroup === pairGroupId) {
        skipped += 1;
        continue;
      }
      if (!dryRun) {
        if (yandexGroup !== pairGroupId) patches.push(normalizeWarehouseProduct({ ...yandex, manualGroupId: pairGroupId }));
        if (ozonGroup !== pairGroupId) patches.push(normalizeWarehouseProduct({ ...ozon, manualGroupId: pairGroupId }));
      }
      updated += 1;
    }

    if (!dryRun && patches.length) {
      for (const chunk of chunkArray(patches.splice(0, patches.length), 100)) {
        await replaceProductLinksInPostgres(client, chunk);
        mergeWarehouseProductsIntoMemory(chunk);
      }
    }

    if (yandexRows.length < normalizedBatchSize) break;
  }

  if (!dryRun && updated > 0) invalidateWarehouseViewCache();
  return { ok: true, dryRun, paired, updated, skipped, scanned };
}


