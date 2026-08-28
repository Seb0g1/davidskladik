#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  const rows = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id,
           current_price, target_price, target_stock,
           raw
    FROM warehouse_products
    WHERE offer_id = 'K18001'
    ORDER BY marketplace
  `);

  console.log(`\n=== K18001 raw JSON fields (autoPriceMin/Max, markup etc.) ===\n`);
  for (const r of rows) {
    const raw = typeof r.raw === "string" ? JSON.parse(r.raw) : (r.raw || {});
    console.log(`  [${r.marketplace}] id=${r.id}`);
    console.log(`    currentPrice=${r.current_price} targetPrice=${r.target_price} targetStock=${r.target_stock}`);
    console.log(`    raw.autoPriceMin=${raw.autoPriceMin} raw.autoPriceMax=${raw.autoPriceMax} ← CLAMPING CHECK`);
    console.log(`    raw.markup=${raw.markup} raw.autoPriceEnabled=${raw.autoPriceEnabled}`);
    console.log(`    raw keys: ${Object.keys(raw).join(", ")}`);
  }

  // ЮК345754
  const other = await prisma.$queryRawUnsafe(`
    SELECT offer_id, marketplace, current_price, target_price, raw
    FROM warehouse_products
    WHERE offer_id = 'ЮК345754'
    ORDER BY marketplace
  `);
  console.log(`\n=== ЮК345754 raw fields ===\n`);
  for (const r of other) {
    const raw = typeof r.raw === "string" ? JSON.parse(r.raw) : (r.raw || {});
    console.log(`  ${r.offer_id} [${r.marketplace}]: current=${r.current_price} target=${r.target_price}`);
    console.log(`    raw.autoPriceMin=${raw.autoPriceMin} raw.autoPriceMax=${raw.autoPriceMax}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
