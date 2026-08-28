async function backfillOzonYandexManualGroupsPostgres(prisma, { dryRun = true, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, paired: 0, updated: 0, reason: "postgres_unavailable" };
  const ozonRows = await client.warehouseProduct.findMany({
    where: { marketplace: "ozon" },
    include: { links: true },
  });
  const indexes = buildOzonProductLookupIndexes(ozonRows.map(productFromPostgres));
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
      const ozon = findOzonMatchForYandexProduct(yandex, indexes);
      if (!ozon) {
        skipped += 1;
        continue;
      }
      paired += 1;
      const groupId = buildOzonYandexAutoPairGroupId(ozon);
      if (!groupId) {
        skipped += 1;
        continue;
      }
      const yandexGroup = cleanText(yandex.manualGroupId || yandex.raw?.manualGroupId);
      const ozonGroup = cleanText(ozon.manualGroupId || ozon.raw?.manualGroupId);
      if (yandexGroup === groupId && ozonGroup === groupId) {
        skipped += 1;
        continue;
      }
      if (!dryRun) {
        if (yandexGroup !== groupId) patches.push(normalizeWarehouseProduct({ ...yandex, manualGroupId: groupId }));
        if (ozonGroup !== groupId) patches.push(normalizeWarehouseProduct({ ...ozon, manualGroupId: groupId }));
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

async function syncOzonManualGroupsFromYandexPostgres(prisma, { dryRun = true, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, updated: 0, reason: "postgres_unavailable" };
  const ozonRows = await client.warehouseProduct.findMany({
    where: { marketplace: "ozon" },
    include: { links: true },
  });
  const ozonIndexes = buildOzonProductLookupIndexes(ozonRows.map(productFromPostgres));
  let updated = 0;
  let scanned = 0;
  let skipped = 0;
  let skippedNoGroup = 0;
  let skippedNoOzon = 0;
  let skippedAlreadySynced = 0;
  let cursorId = "";
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  const pending = [];

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
      if (!groupId.startsWith("auto-pair-")) {
        skipped += 1;
        skippedNoGroup += 1;
        continue;
      }
      const ozonId = extractOzonIdFromAutoPairGroupId(groupId) || extractYandexSourceProductId(yandex);
      let ozon = ozonId && ozonIndexes.byId?.get(ozonId) ? ozonIndexes.byId.get(ozonId) : null;
      if (!ozon) ozon = findOzonMatchForYandexProduct(yandex, ozonIndexes);
      if (!ozon) {
        skipped += 1;
        skippedNoOzon += 1;
        continue;
      }
      if (cleanText(ozon.manualGroupId || ozon.raw?.manualGroupId) === groupId) {
        skipped += 1;
        skippedAlreadySynced += 1;
        continue;
      }
      updated += 1;
      if (!dryRun) pending.push(normalizeWarehouseProduct({ ...ozon, manualGroupId: groupId }));
    }

    if (!dryRun && pending.length) {
      for (const chunk of chunkArray(pending.splice(0, pending.length), 100)) {
        await replaceProductLinksInPostgres(client, chunk);
        mergeWarehouseProductsIntoMemory(chunk);
      }
    }

    if (yandexRows.length < normalizedBatchSize) break;
  }

  if (!dryRun && updated > 0) invalidateWarehouseViewCache();
  return {
    ok: true,
    dryRun,
    updated,
    skipped,
    scanned,
    skippedNoGroup,
    skippedNoOzon,
    skippedAlreadySynced,
  };
}
