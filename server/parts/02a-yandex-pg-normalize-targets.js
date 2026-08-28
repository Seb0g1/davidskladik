async function normalizeYandexWarehouseTargetsPostgres(prisma, { dryRun = true, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, updated: 0, scanned: 0, reason: "postgres_unavailable" };
  let updated = 0;
  let scanned = 0;
  let skipped = 0;
  let cursorId = "";
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  const pending = [];

  while (true) {
    const rows = await client.warehouseProduct.findMany({
      where: { marketplace: "yandex" },
      include: { links: true },
      orderBy: { id: "asc" },
      take: normalizedBatchSize,
      ...(cursorId ? { skip: 1, cursor: { id: cursorId } } : {}),
    });
    if (!rows.length) break;
    cursorId = rows[rows.length - 1].id;

    for (const row of rows) {
      scanned += 1;
      const product = productFromPostgres(row);
      const shop = getYandexShopByTarget(product.target);
      if (!shop?.id || cleanText(product.target) === cleanText(shop.id)) {
        skipped += 1;
        continue;
      }
      updated += 1;
      if (!dryRun) {
        pending.push(normalizeWarehouseProduct({
          ...product,
          target: shop.id,
          targetName: shop.name || product.targetName || "Yandex Market",
        }));
      }
    }

    if (!dryRun && pending.length) {
      for (const chunk of chunkArray(pending.splice(0, pending.length), 100)) {
        await replaceProductLinksInPostgres(client, chunk);
        mergeWarehouseProductsIntoMemory(chunk);
      }
    }

    if (rows.length < normalizedBatchSize) break;
  }

  if (!dryRun && updated > 0) invalidateWarehouseViewCache();
  return { ok: true, dryRun, updated, skipped, scanned };
}
