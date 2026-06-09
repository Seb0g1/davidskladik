async function fetchCrossMarketplaceSiblingRows(prisma, baseWhere, seedRows = []) {
  const client = prisma || getPrisma();
  if (!client || !Array.isArray(seedRows) || !seedRows.length) return [];
  const siblingWhere = warehousePageSiblingWhere(baseWhere);
  const seen = new Set(seedRows.map((row) => cleanText(row.id)).filter(Boolean));
  const found = [];
  const products = seedRows.map((row) => productFromPostgres(row));
  const ozonIds = products.filter((product) => cleanText(product.marketplace) === "ozon").map((product) => cleanText(product.id)).filter(Boolean);
  const sourceIds = products
    .filter((product) => cleanText(product.marketplace) === "yandex")
    .map((product) => extractYandexSourceProductId(product))
    .filter(Boolean);
  const productIds = Array.from(new Set(products.map((product) => cleanText(product.productId)).filter(Boolean)));

  for (const chunk of chunkArray(ozonIds, 100)) {
    if (!chunk.length) continue;
    const rows = await client.warehouseProduct.findMany({
      where: {
        AND: [
          siblingWhere,
          { marketplace: "yandex" },
          {
            OR: chunk.map((ozonId) => ({
              raw: { path: ["yandex", "extra", "sourceProductId"], equals: ozonId },
            })),
          },
        ],
      },
      include: { links: true },
    });
    for (const row of rows) {
      if (!seen.has(row.id)) {
        seen.add(row.id);
        found.push(row);
      }
    }
  }

  if (sourceIds.length) {
    const ozonMatches = await client.warehouseProduct.findMany({
      where: {
        AND: [siblingWhere, { marketplace: "ozon" }, { id: { in: sourceIds } }],
      },
      include: { links: true },
    });
    for (const row of ozonMatches) {
      if (!seen.has(row.id)) {
        seen.add(row.id);
        found.push(row);
      }
    }
  }

  if (productIds.length) {
    const crossRows = await client.warehouseProduct.findMany({
      where: {
        AND: [siblingWhere, { productId: { in: productIds } }],
      },
      include: { links: true },
    });
    for (const row of crossRows) {
      if (!seen.has(row.id)) {
        seen.add(row.id);
        found.push(row);
      }
    }
  }

  const manualGroupIds = Array.from(new Set(products
    .map((product) => cleanText(product.manualGroupId || product.raw?.manualGroupId))
    .filter((groupId) => groupId && groupId.startsWith("auto-pair-"))));
  if (manualGroupIds.length) {
    for (const chunk of chunkArray(manualGroupIds, 100)) {
      const rows = await client.warehouseProduct.findMany({
        where: {
          AND: [
            siblingWhere,
            {
              OR: chunk.map((groupId) => ({
                raw: { path: ["manualGroupId"], equals: groupId },
              })),
            },
          ],
        },
        include: { links: true },
      });
      for (const row of rows) {
        if (!seen.has(row.id)) {
          seen.add(row.id);
          found.push(row);
        }
      }
    }
  }

  return found;
}
