function buildEnabledWarehouseProductsSql(alias = "w") {
  const { Prisma } = require("@prisma/client");
  const col = (field) => Prisma.raw(`${alias}.${field}`);
  const parts = [];
  const ozonAccounts = getOzonAccounts();
  if (ozonAccounts.length) {
    for (const account of ozonAccounts) {
      const targetId = cleanText(account.id);
      parts.push(Prisma.sql`(${col("marketplace")} = 'ozon' AND (${col("target")} = ${targetId} OR ${col("target")} = 'ozon'))`);
    }
  } else {
    parts.push(Prisma.sql`${col("marketplace")} = 'ozon'`);
  }
  const yandexShops = getYandexShops({ includeSyncDisabled: true });
  if (yandexShops.length <= 1) {
    parts.push(Prisma.sql`${col("marketplace")} = 'yandex'`);
  } else {
    parts.push(Prisma.sql`${col("marketplace")} = 'yandex'`);
  }
  return parts.length === 1 ? parts[0] : Prisma.sql`(${Prisma.join(parts, " OR ")})`;
}

function warehouseGroupCountLinkSql(filters = {}) {
  const { Prisma } = require("@prisma/client");
  const linked = cleanText(filters.linked || "all");
  if (linked === "unlinked") {
    return Prisma.sql`NOT EXISTS (SELECT 1 FROM "product_links" pl WHERE pl."product_id" = w.id)`;
  }
  if (linked === "linked") {
    return Prisma.sql`EXISTS (SELECT 1 FROM "product_links" pl WHERE pl."product_id" = w.id)`;
  }
  return null;
}

async function countWarehouseGroupTotalPostgresSql(prisma, filters = {}) {
  if (!prisma || !warehouseGroupCountUsesSqlFastPath(filters)) return 0;
  const linkClause = warehouseGroupCountLinkSql(filters);
  if (!linkClause) return 0;
  try {
    const enabledClause = buildEnabledWarehouseProductsSql("w");
    const rows = await prisma.$queryRaw`
      SELECT COUNT(DISTINCT LOWER(w.offer_id))::int AS cnt
      FROM warehouse_products w
      WHERE ${enabledClause}
        AND ${linkClause}
    `;
    return Math.max(0, Number(rows[0]?.cnt || 0));
  } catch (error) {
    logger.warn("warehouse group count sql fast-path failed, using batched fallback", {
      detail: error?.message || String(error),
      linked: cleanText(filters.linked || "all"),
    });
    const where = warehousePagePostgresWhere(filters);
    const distinctOffers = new Set();
    const batchSize = Math.max(1000, Number(process.env.WAREHOUSE_GROUP_COUNT_BATCH_SIZE || 5000) || 5000);
    let cursorId = "";
    while (true) {
      const batch = await prisma.warehouseProduct.findMany({
        where,
        select: { id: true, offerId: true },
        orderBy: { id: "asc" },
        take: batchSize,
        ...(cursorId ? { skip: 1, cursor: { id: cursorId } } : {}),
      });
      if (!batch.length) break;
      for (const row of batch) {
        const offerId = cleanText(row.offerId).toLowerCase();
        if (offerId) distinctOffers.add(offerId);
      }
      cursorId = batch[batch.length - 1].id;
      if (batch.length < batchSize) break;
    }
    return distinctOffers.size;
  }
}

function warehousePostgresWhereRequiresUnlinkedOnly(baseWhere = {}) {
  const and = Array.isArray(baseWhere.AND) ? baseWhere.AND : [];
  return and.some((clause) => clause && typeof clause === "object" && clause.links && clause.links.none !== undefined);
}

