async function reassignWarehouseProductReferences(tx, fromIds = [], toId = "") {
  const ids = (Array.isArray(fromIds) ? fromIds : []).filter((id) => id && id !== toId);
  if (!ids.length || !toId) return;
  await tx.priceHistory.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.priceRetryQueueItem.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.salesAutomationSkuState.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.ozonUnarchiveQueueItem.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.brandIndexItem.deleteMany({ where: { productId: { in: ids } } });
}

function warehouseProductsShareIdentity(left = {}, right = {}) {
  return cleanText(left.marketplace).toLowerCase() === cleanText(right.marketplace).toLowerCase()
    && cleanText(left.offerId).toLowerCase() === cleanText(right.offerId).toLowerCase()
    && cleanText(left.target || left.marketplace) === cleanText(right.target || right.marketplace);
}

async function removeOrphanWarehouseProductPostgres(client, orphan = {}, canonical = {}) {
  const orphanProduct = normalizeWarehouseProduct(orphan);
  const canonicalProduct = normalizeWarehouseProduct(canonical);
  const mergedLinks = buildCommonWarehouseGroupLinks([canonicalProduct, orphanProduct]);
  const patchedCanonical = normalizeWarehouseProduct({ ...canonicalProduct, links: mergedLinks });
  await client.$transaction(async (tx) => {
    await reassignWarehouseProductReferences(tx, [orphanProduct.id], canonicalProduct.id);
    await replaceProductLinksInPostgres(tx, patchedCanonical);
    await tx.productLink.deleteMany({ where: { productId: orphanProduct.id } });
    await tx.warehouseProduct.delete({ where: { id: orphanProduct.id } });
  }, { timeout: 60_000 });
}

async function migrateWarehouseProductCanonicalIdsPostgres(prisma, { dryRun = true, limit = 5000 } = {}) {
  const client = prisma || getPrisma();
  if (!client) return { ok: false, migrated: 0, deleted: 0, scanned: 0, reason: "postgres_unavailable" };
  const rows = await client.warehouseProduct.findMany({
    include: { links: true },
    orderBy: { updatedAt: "desc" },
    take: Math.max(1, Math.min(50000, Number(limit) || 5000)),
  });
  let migrated = 0;
  let deleted = 0;
  let skipped = 0;
  for (const row of rows) {
    const product = productFromPostgres(row);
    const canonicalId = warehouseProductCanonicalId(product);
    if (!canonicalId || product.id === canonicalId) continue;
    const conflictRow = await client.warehouseProduct.findUnique({
      where: { id: canonicalId },
      include: { links: true },
    });
    if (conflictRow) {
      const conflict = productFromPostgres(conflictRow);
      if (!warehouseProductsShareIdentity(product, conflict)) {
        skipped += 1;
        continue;
      }
      if (!dryRun) await removeOrphanWarehouseProductPostgres(client, product, conflict);
      deleted += 1;
      continue;
    }
    if (dryRun) {
      migrated += 1;
      continue;
    }
    const merged = normalizeWarehouseProduct({ ...product, id: canonicalId });
    await client.$transaction(async (tx) => {
      await upsertWarehouseProductPostgres(tx, merged);
      await reassignWarehouseProductReferences(tx, [product.id], canonicalId);
      await tx.productLink.deleteMany({ where: { productId: { in: [product.id, canonicalId] } } });
      await tx.warehouseProduct.delete({ where: { id: product.id } });
      await replaceProductLinksInPostgres(tx, merged);
    }, { timeout: 60_000 });
    migrated += 1;
  }
  if (!dryRun && (migrated > 0 || deleted > 0)) invalidateWarehouseViewCache();
  return { ok: true, dryRun, migrated, deleted, skipped, scanned: rows.length };
}


