"use strict";
// Проверяем несколько OOS листингов — что за продукты в warehouse
const { PrismaClient } = require("@prisma/client");
const fs = require("fs");
const prisma = new PrismaClient();

async function main() {
  const listings = JSON.parse(fs.readFileSync("data/avito-listings.json", "utf8"));
  const items = listings.items || [];
  const oos = items.filter(i => i.outOfStock).slice(0, 20);
  const sourceIds = oos.map(i => i.sourceProductId).filter(Boolean);

  const rows = await prisma.$queryRaw`
    SELECT id, name, target_stock, target_price, archived,
           raw->'selectedSupplier' AS supplier,
           raw->'ozonOfferId' AS ozon_offer_id,
           raw->'stock' AS pm_stock
    FROM warehouse_products
    WHERE id = ANY(${sourceIds})
    LIMIT 20
  `;

  for (const r of rows) {
    const listing = oos.find(i => i.sourceProductId === r.id);
    console.log(JSON.stringify({
      id: r.id,
      name: (r.name || "").slice(0, 50),
      adId: listing?.adId,
      targetStock: r.target_stock,
      targetPrice: r.target_price,
      archived: r.archived,
      hasSupplier: !!r.supplier,
      ozonOfferId: r.ozon_offer_id,
      pmStock: r.pm_stock,
    }));
  }
}

main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
