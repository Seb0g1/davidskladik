#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function tryFindMany(take, skip = 0) {
  try {
    await prisma.warehouseProduct.findMany({
      where: { links: { some: {} }, archived: false },
      include: { links: true },
      orderBy: { updatedAt: "desc" },
      take,
      skip,
    });
    return true;
  } catch (e) {
    return false;
  }
}

async function main() {
  // Count linked non-archived products
  const total = await prisma.warehouseProduct.count({
    where: { links: { some: {} }, archived: false },
  });
  console.log("linked non-archived products:", total);

  // Binary search: find the max take that works
  let lo = 1, hi = total, lastGood = 1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    process.stdout.write(`  trying take:${mid} skip:0 ... `);
    const ok = await tryFindMany(mid);
    console.log(ok ? "OK" : "FAIL");
    if (ok) { lastGood = mid; lo = mid + 1; }
    else hi = mid - 1;
  }
  console.log(`\nMax working take: ${lastGood}`);

  // Now narrow down: which row at position lastGood..lastGood+10 fails?
  if (lastGood < total) {
    console.log("\nSearching for offending row around position", lastGood + 1);
    for (let skip = lastGood; skip <= Math.min(lastGood + 50, total - 1); skip++) {
      process.stdout.write(`  take:1 skip:${skip} ... `);
      const ok = await tryFindMany(1, skip);
      console.log(ok ? "OK" : "FAIL");
      if (!ok) {
        // Get the ID of this product
        const rows = await prisma.$queryRawUnsafe(
          `SELECT id, offer_id, marketplace, name FROM warehouse_products
           WHERE id NOT IN (SELECT id FROM warehouse_products WHERE archived = true)
             AND EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = warehouse_products.id)
           ORDER BY updated_at DESC LIMIT 1 OFFSET ${skip}`
        );
        if (rows[0]) {
          console.log("  OFFENDING PRODUCT:", JSON.stringify({ id: rows[0].id, offerId: rows[0].offer_id, marketplace: rows[0].marketplace, name: rows[0].name?.slice(0, 80) }));
        }
        break;
      }
    }
  }
}

main()
  .catch((e) => { console.error(e); process.exit(1); })
  .finally(() => prisma.$disconnect());
