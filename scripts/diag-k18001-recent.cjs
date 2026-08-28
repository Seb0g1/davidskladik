#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Last 20 price history entries for K18001 (newest first)
  const hist = await prisma.$queryRawUnsafe(`
    SELECT ph.new_price, ph.status, ph.marketplace, ph.created_at,
           (ph.response->>'pmPriceUsd') as pm_usd,
           (ph.response->>'usdRate') as usd_rate,
           (ph.response->>'markup') as markup,
           (ph.response->>'error') as error_detail,
           ph.response
    FROM price_history ph
    JOIN warehouse_products wp ON wp.id = ph.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY ph.created_at DESC
    LIMIT 20
  `);
  console.log(`\n=== Last 20 price_history entries for K18001 (newest first) ===\n`);
  for (const h of hist) {
    const resp = typeof h.response === "object" ? h.response : {};
    const errMsg = h.error_detail || resp.error || resp.message || (Array.isArray(resp.errors) ? JSON.stringify(resp.errors?.[0]) : "") || "";
    console.log(`  [${h.marketplace}] newPrice=${h.new_price} status=${h.status} at=${String(h.created_at).substring(0,24)}`);
    if (errMsg) console.log(`    error: ${String(errMsg).substring(0,120)}`);
  }

  // Current warehouse_products state for K18001
  const wh = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, current_price, target_price, status, updated_at
    FROM warehouse_products WHERE offer_id = 'K18001' ORDER BY marketplace
  `);
  console.log(`\n=== warehouse_products for K18001 ===\n`);
  for (const w of wh) {
    console.log(`  [${w.marketplace}] id=${w.id} currentPrice=${w.current_price} targetPrice=${w.target_price} status=${w.status} updatedAt=${w.updated_at}`);
  }

  // Retry queue current state
  const retryItems = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, price, status, attempts, next_retry_at, error, updated_at
    FROM price_retry_queue WHERE offer_id = 'K18001' ORDER BY updated_at DESC
  `);
  console.log(`\n=== price_retry_queue for K18001 (${retryItems.length}) ===\n`);
  for (const r of retryItems) {
    console.log(`  [${r.marketplace}] price=${r.price} status=${r.status} attempts=${r.attempts} error="${r.error}" nextRetryAt=${r.next_retry_at}`);
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
