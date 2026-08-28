"use strict";
// Remote: диагностика outOfStock по Postgres
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  // Загружаем avito-listings.json
  const fs = require("fs");
  const listings = JSON.parse(fs.readFileSync("data/avito-listings.json", "utf8"));
  const items = listings.items || [];
  const oosItems = items.filter((i) => i.outOfStock);
  const sourceIds = [...new Set(oosItems.map((i) => i.sourceProductId).filter(Boolean))];

  console.log(`OOS items: ${oosItems.length}, unique sourceIds: ${sourceIds.length}`);

  if (!sourceIds.length) { console.log("No sourceIds in OOS items"); return; }

  // Берём первые 10000
  const chunk = sourceIds.slice(0, 10000);
  const rows = await prisma.$queryRaw`
    SELECT
      id,
      target_stock,
      archived,
      raw->'selectedSupplier' AS supplier
    FROM warehouse_products
    WHERE id = ANY(${chunk})
  `;

  let zeroStock = 0, noSupplier = 0, supplierStopped = 0, archived = 0, hasStockHasSupplier = 0;
  for (const r of rows) {
    const stock = Number(r.target_stock || 0);
    const supp = r.supplier && typeof r.supplier === "object" ? r.supplier : null;
    const isArchived = Boolean(r.archived);
    const isStopped = Boolean(supp?.stopped);
    if (stock <= 0) zeroStock++;
    if (!supp) noSupplier++;
    if (isStopped) supplierStopped++;
    if (isArchived) archived++;
    if (stock > 0 && supp && !isStopped) hasStockHasSupplier++;
  }

  console.log(JSON.stringify({
    total: rows.length,
    zeroStock,
    noSupplier,
    supplierStopped,
    archived,
    hasStockHasSupplierButOOS: hasStockHasSupplier,
  }, null, 2));
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
