#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  const DRY_RUN = process.argv.includes("--dry-run");
  console.log(`\n=== K18001 stale-clear fix (${DRY_RUN ? "DRY RUN" : "LIVE"}) ===\n`);

  // Show current state
  const rows = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, target, target_price, current_price,
           raw->>'currentPrice' as raw_current_price,
           raw->>'marketplacePrice' as raw_marketplace_price,
           raw->>'targetPrice' as raw_target_price,
           raw->>'nextPrice' as raw_next_price
    FROM warehouse_products WHERE offer_id = 'K18001' ORDER BY marketplace
  `);
  console.log("Current state:");
  for (const r of rows) {
    console.log(`  [${r.marketplace}] target_price=${r.target_price} current_price=${r.current_price}`);
    console.log(`    raw.targetPrice=${r.raw_target_price} raw.nextPrice=${r.raw_next_price}`);
    console.log(`    raw.currentPrice=${r.raw_current_price} raw.marketplacePrice=${r.raw_marketplace_price}`);
  }

  if (!DRY_RUN) {
    // 1. Clear ALL stale price fields from yandex K18001 raw JSON + columns
    //    Also remove the null-valued targetPrice key that was set in a previous fix
    const updatedYandex = await prisma.$executeRawUnsafe(`
      UPDATE warehouse_products
      SET
        target_price = NULL,
        current_price = NULL,
        raw = raw
          - 'targetPrice'
          - 'nextPrice'
          - 'currentPrice'
          - 'marketplacePrice'
          - 'selectedSupplier'
          - 'suppliers'
          - 'ready'
          - 'priceFormula'
          - 'priceSource',
        updated_at = NOW()
      WHERE offer_id = 'K18001' AND marketplace = 'yandex'
    `);
    console.log(`\nYandex: cleared ${updatedYandex} row(s) — all stale price + supplier fields wiped`);

    // 2. Clear ALL stale price fields from ozon K18001 raw JSON + columns
    const updatedOzon = await prisma.$executeRawUnsafe(`
      UPDATE warehouse_products
      SET
        target_price = NULL,
        current_price = NULL,
        raw = raw
          - 'targetPrice'
          - 'nextPrice'
          - 'currentPrice'
          - 'marketplacePrice'
          - 'selectedSupplier'
          - 'suppliers'
          - 'ready'
          - 'priceFormula'
          - 'priceSource',
        updated_at = NOW()
      WHERE offer_id = 'K18001' AND marketplace = 'ozon'
    `);
    console.log(`Ozon: cleared ${updatedOzon} row(s) — all stale price + supplier fields wiped`);

    // 3. Delete all K18001 price_retry_queue items (bad 36/61₽ prices)
    const deletedRetry = await prisma.$executeRawUnsafe(`
      DELETE FROM price_retry_queue WHERE offer_id = 'K18001'
    `);
    console.log(`Retry queue: deleted ${deletedRetry} K18001 item(s)`);

    // 4. Verify
    const after = await prisma.$queryRawUnsafe(`
      SELECT id, marketplace, target_price, current_price,
             raw->>'currentPrice' as raw_current_price,
             raw->>'targetPrice' as raw_target_price
      FROM warehouse_products WHERE offer_id = 'K18001' ORDER BY marketplace
    `);
    console.log("\nAfter fix:");
    for (const r of after) {
      console.log(`  [${r.marketplace}] target_price=${r.target_price} current_price=${r.current_price} raw.currentPrice=${r.raw_current_price} raw.targetPrice=${r.raw_target_price}`);
    }
  } else {
    console.log("\nDRY RUN — rerun without --dry-run to apply");
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
