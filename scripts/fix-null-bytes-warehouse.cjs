#!/usr/bin/env node
"use strict";
// One-off: find and strip null bytes (chr(0)) from warehouse_products text columns.
// Prisma's Rust napi layer throws "Failed to convert rust String into napi string"
// when it encounters null bytes in a TEXT column returned from Postgres.
require("dotenv").config();
const { PrismaClient } = require("@prisma/client");

const prisma = new PrismaClient();

async function main() {
  // Find affected rows first
  const affected = await prisma.$queryRaw`
    SELECT id, offer_id, name, marketplace
    FROM warehouse_products
    WHERE position(chr(0) in name) > 0
       OR position(chr(0) in offer_id) > 0
       OR position(chr(0) in coalesce(target, '')) > 0
       OR position(chr(0) in coalesce(brand, '')) > 0
       OR position(chr(0) in coalesce(status, '')) > 0
       OR position(chr(0) in coalesce(product_id, '')) > 0
  `;

  console.log(`Found ${affected.length} products with null bytes:`);
  for (const row of affected) {
    console.log(`  id=${row.id}  offer_id=${row.offer_id}  marketplace=${row.marketplace}  name=${JSON.stringify(row.name)}`);
  }

  if (!affected.length) {
    console.log("No null bytes found — nothing to clean.");
    return;
  }

  // Strip null bytes from all text columns
  const result = await prisma.$executeRaw`
    UPDATE warehouse_products
    SET
      name        = replace(name,                      chr(0), ''),
      offer_id    = replace(offer_id,                  chr(0), ''),
      target      = replace(coalesce(target,    ''),   chr(0), ''),
      brand       = replace(coalesce(brand,     ''),   chr(0), ''),
      status      = replace(coalesce(status,    ''),   chr(0), ''),
      product_id  = replace(coalesce(product_id,''),   chr(0), '')
    WHERE position(chr(0) in name) > 0
       OR position(chr(0) in offer_id) > 0
       OR position(chr(0) in coalesce(target, '')) > 0
       OR position(chr(0) in coalesce(brand, '')) > 0
       OR position(chr(0) in coalesce(status, '')) > 0
       OR position(chr(0) in coalesce(product_id, '')) > 0
  `;

  console.log(`\nCleaned ${result} rows.`);

  // Verify
  const remaining = await prisma.$queryRaw`
    SELECT count(*)::int AS cnt
    FROM warehouse_products
    WHERE position(chr(0) in name) > 0
       OR position(chr(0) in offer_id) > 0
       OR position(chr(0) in coalesce(target, '')) > 0
       OR position(chr(0) in coalesce(brand, '')) > 0
       OR position(chr(0) in coalesce(status, '')) > 0
       OR position(chr(0) in coalesce(product_id, '')) > 0
  `;
  console.log(`Remaining null bytes: ${remaining[0].cnt}`);
}

main()
  .catch((e) => { console.error(e); process.exit(1); })
  .finally(() => prisma.$disconnect());
