#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const prisma = new PrismaClient();

async function main() {
  // Find product by offerId or id containing 234123
  const rows = await prisma.$queryRawUnsafe(`
    SELECT
      wp.id, wp.offer_id, wp.name, wp.markup, wp.yandex_markup,
      wp.marketplace, wp.data
    FROM warehouse_products wp
    WHERE wp.offer_id LIKE '%234123%' OR wp.id LIKE '%234123%'
    LIMIT 5
  `);
  console.log("=== PRODUCT 234123 ===");
  for (const r of rows) {
    console.log(JSON.stringify({
      id: r.id,
      offer_id: r.offer_id,
      name: r.name,
      markup: r.markup,
      yandex_markup: r.yandex_markup,
      marketplace: r.marketplace,
    }, null, 2));
  }

  // Also look at recent price history for this offer
  const hist = await prisma.$queryRawUnsafe(`
    SELECT marketplace::text, target, offer_id, old_price, new_price, status::text, created_at
    FROM price_history
    WHERE offer_id LIKE '%234123%'
    ORDER BY created_at DESC LIMIT 10
  `);
  console.log("\n=== PRICE HISTORY 234123 ===");
  for (const r of hist) {
    console.log(`  ${r.created_at?.toISOString?.() || r.created_at} [${r.marketplace}/${r.target}] ${r.offer_id}: old=${r.old_price} new=${r.new_price} ${r.status}`);
  }
}

main().catch(e => { console.error(e.message); process.exit(1); }).finally(() => prisma.$disconnect());
