async function materializeYandexExportedProductsForPostgres(prisma, { dryRun = false, limit = 0, batchSize = 500 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, added: 0, scanned: 0, skipped: 0, reason: "postgres_unavailable" };
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { ok: true, added: 0, scanned: 0, skipped: 0, reason: "no_yandex_shops" };

  const shop = shops[0];
  const yandexRows = await client.warehouseProduct.findMany({
    where: { marketplace: "yandex" },
    select: { target: true, offerId: true },
  });
  const existingKeys = new Set(yandexRows.map((row) => warehouseProductExactMergeKey({
    target: row.target,
    marketplace: "yandex",
    offerId: row.offerId,
  })));
  const yandexOfferIds = new Set(yandexRows.map((row) => cleanText(row.offerId).toLowerCase()).filter(Boolean));
  const yandexCacheOfferIds = await readYandexExistingOfferIdCache({ maxAgeMs: 7 * 24 * 60 * 60 * 1000 });

  let added = 0;
  let scanned = 0;
  let skipped = 0;
  let cursorId = "";
  const normalizedBatchSize = Math.max(50, Math.min(2000, Number(batchSize) || 500));
  const normalizedLimit = Math.max(0, Number(limit) || 0);

  while (true) {
    const ozonRows = await client.warehouseProduct.findMany({
      where: { marketplace: "ozon" },
      include: { links: true },
      orderBy: { id: "asc" },
      take: normalizedBatchSize,
      ...(cursorId ? { skip: 1, cursor: { id: cursorId } } : {}),
    });
    if (!ozonRows.length) break;
    cursorId = ozonRows[ozonRows.length - 1].id;

    const additions = [];
    const ozonPairPatches = [];
    for (const row of ozonRows) {
      scanned += 1;
      const product = productFromPostgres(row);
      if (!ozonProductShouldMaterializeYandexSibling(product, shop, { yandexOfferIds, yandexCacheOfferIds })) {
        skipped += 1;
        continue;
      }
      const exactKey = warehouseProductExactMergeKey({
        target: shop.id,
        marketplace: "yandex",
        offerId: product.offerId,
      });
      if (existingKeys.has(exactKey)) {
        skipped += 1;
        continue;
      }
      const exports = product.exports || {};
      const exportState = exports[shop.id] || exports.yandex || { status: "sent", targetName: shop.name || "Yandex Market" };
      const pairGroupId = product.manualGroupId || buildOzonYandexAutoPairGroupId(product);
      const yandexProduct = buildYandexWarehouseProductFromOzonExport(
        { ...product, manualGroupId: pairGroupId },
        shop,
        exportState,
      );
      yandexProduct.links = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
      additions.push(yandexProduct);
      if (!cleanText(product.manualGroupId) && pairGroupId) {
        ozonPairPatches.push(normalizeWarehouseProduct({ ...product, manualGroupId: pairGroupId }));
      }
      existingKeys.add(exactKey);
      const offerKey = cleanText(product.offerId).toLowerCase();
      if (offerKey) yandexOfferIds.add(offerKey);
      added += 1;
      if (normalizedLimit > 0 && added >= normalizedLimit) break;
    }

    if (!dryRun && (additions.length || ozonPairPatches.length)) {
      const writes = [...additions, ...ozonPairPatches];
      await replaceProductLinksInPostgres(client, writes);
      mergeWarehouseProductsIntoMemory(writes);
    }

    if (normalizedLimit > 0 && added >= normalizedLimit) break;
    if (ozonRows.length < normalizedBatchSize) break;
  }

  if (!dryRun && added > 0) invalidateWarehouseViewCache();
  return { ok: true, dryRun, added, scanned, skipped, shopId: shop.id };
}
