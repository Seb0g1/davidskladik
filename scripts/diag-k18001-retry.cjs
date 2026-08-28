#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Check price_retry_queue_items for K18001
  const retryItems = await prisma.$queryRawUnsafe(`
    SELECT ri.id, ri.product_id, ri.marketplace, ri.offer_id, ri.status,
           ri.price, ri.old_price, ri.attempts, ri.next_retry_at, ri.error,
           ri.created_at, ri.updated_at, ri.payload
    FROM price_retry_queue ri
    WHERE ri.offer_id = 'K18001'
    ORDER BY ri.created_at DESC
  `);

  console.log(`\n=== price_retry_queue for K18001 (${retryItems.length} items) ===\n`);
  for (const r of retryItems) {
    const payload = typeof r.payload === "string" ? JSON.parse(r.payload) : (r.payload || {});
    console.log(`  id=${r.id} marketplace=${r.marketplace} status=${r.status}`);
    console.log(`    price=${r.price} oldPrice=${r.old_price} attempts=${r.attempts}`);
    console.log(`    nextRetryAt=${r.next_retry_at}`);
    console.log(`    error="${r.error}"`);
    console.log(`    payload.retryReason=${payload.retryReason} payload.finalTargetPrice=${payload.finalTargetPrice}`);
    console.log(`    createdAt=${r.created_at} updatedAt=${r.updated_at}`);
  }

  // Also check ЮК345754
  const retryItems2 = await prisma.$queryRawUnsafe(`
    SELECT ri.id, ri.marketplace, ri.offer_id, ri.status, ri.price, ri.attempts, ri.next_retry_at
    FROM price_retry_queue ri
    WHERE ri.offer_id = 'ЮК345754'
    ORDER BY ri.created_at DESC
  `);

  console.log(`\n=== price_retry_queue_items for ЮК345754 (${retryItems2.length} items) ===\n`);
  for (const r of retryItems2) {
    console.log(`  marketplace=${r.marketplace} status=${r.status} price=${r.price} attempts=${r.attempts}`);
  }

  // Check the full price history for K18001 - all, sorted by date
  const hist = await prisma.$queryRawUnsafe(`
    SELECT ph.new_price, ph.status, ph.marketplace, ph.created_at,
           (ph.response->>'pmPriceUsd') as pm_usd,
           (ph.response->>'usdRate') as usd_rate,
           (ph.response->>'markup') as markup
    FROM price_history ph
    JOIN warehouse_products wp ON wp.id = ph.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY ph.created_at ASC
  `);

  console.log(`\n=== price_history K18001 (ALL, ${hist.length} entries) ===\n`);
  let lastPrice = null;
  for (const h of hist) {
    const marker = lastPrice !== null && h.new_price !== lastPrice ? " ← PRICE CHANGED" : "";
    console.log(`  [${h.marketplace}] newPrice=${h.new_price} status=${h.status} rate=${h.usd_rate} markup=${h.markup} pmUsd=${h.pm_usd} at=${String(h.created_at).substring(0,24)}${marker}`);
    lastPrice = h.new_price;
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
