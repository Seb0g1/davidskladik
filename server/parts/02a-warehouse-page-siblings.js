function mergeWarehousePostgresRows(...rowLists) {
  const byId = new Map();
  for (const rows of rowLists) {
    for (const row of rows || []) {
      if (row?.id) byId.set(String(row.id), row);
    }
  }
  return Array.from(byId.values());
}

function warehousePageSiblingWhere(baseWhere = {}) {
  const clauses = Array.isArray(baseWhere.AND) ? baseWhere.AND : [];
  return {
    AND: clauses.filter((clause) => !(clause && typeof clause === "object" && "marketplace" in clause)),
  };
}

async function addWarehousePostgresPageGroupSiblings(prisma, baseWhere, pageRows = []) {
  if (!pageRows?.length) return pageRows || [];
  if (warehousePostgresWhereRequiresUnlinkedOnly(baseWhere)) return pageRows;
  const crossSiblings = await fetchCrossMarketplaceSiblingRows(prisma, baseWhere, pageRows);
  const pairedOfferIds = new Set();
  for (const row of pageRows) {
    const product = productFromPostgres(row);
    const groupId = cleanText(product.manualGroupId || product.raw?.manualGroupId);
    const offerId = cleanText(product.offerId).toLowerCase();
    if (groupId.startsWith("auto-pair-") && offerId) pairedOfferIds.add(offerId);
  }
  const offerIds = Array.from(new Set((pageRows || []).map((row) => cleanText(row.offerId)).filter(Boolean)))
    .filter((offerId) => !pairedOfferIds.has(offerId.toLowerCase()));
  if (!offerIds.length) return mergeWarehousePostgresRows(pageRows, crossSiblings);
  const siblings = await prisma.warehouseProduct.findMany({
    where: {
      AND: [
        warehousePageSiblingWhere(baseWhere),
        {
          OR: offerIds.map((offerId) => ({
            offerId: { equals: offerId, mode: "insensitive" },
          })),
        },
      ],
    },
    include: { links: true },
    orderBy: warehousePagePostgresOrderBy(),
  });
  return mergeWarehousePostgresRows(pageRows, siblings, crossSiblings);
}


