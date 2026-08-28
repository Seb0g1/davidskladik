#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  // Check recent repair jobs
  const jobs = await prisma.$queryRawUnsafe(`
    SELECT id, type, status, progress,
           result::text as result_text,
           created_at::text, updated_at::text
    FROM operations
    WHERE type = 'problem-products-repair'
    ORDER BY created_at DESC LIMIT 5
  `);
  console.log("=== Recent repair jobs ===");
  for (const j of jobs) {
    let res = null;
    try { res = JSON.parse(j.result_text || "null"); } catch {}
    console.log(JSON.stringify({
      id: String(j.id).substring(0, 8),
      status: j.status,
      progress: j.progress,
      repaired: res?.repaired,
      failed: res?.failed,
      updatedAt: j.updated_at,
      summary: res?.summary,
    }));
  }

  // Check SAAL051new current state
  const prods = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace,
           raw->>'offerId'         AS offer_id,
           raw->>'targetStock'     AS target_stock,
           raw->>'selectedSupplier' AS sel_sup
    FROM warehouse_products
    WHERE raw->>'offerId' = 'SAAL051new'
  `);
  console.log("\n=== SAAL051new current state ===");
  console.log(JSON.stringify(prods, null, 2));

  // Check a few zero-stock products from the list
  const zeroStockSamples = await prisma.$queryRawUnsafe(`
    SELECT wp.id, wp.marketplace,
           wp.raw->>'offerId'         AS offer_id,
           wp.raw->>'targetStock'     AS target_stock,
           wp.raw->>'selectedSupplier' AS sel_sup
    FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id
    WHERE wp.raw->>'targetStock' = '0'
      AND pl.raw->>'matchType' = 'selected_row'
    LIMIT 5
  `);
  console.log("\n=== Zero-stock linked products sample ===");
  console.log(JSON.stringify(zeroStockSamples, null, 2));
}

main().catch((e) => console.error("FAILED:", e.message)).finally(() => prisma.$disconnect());
