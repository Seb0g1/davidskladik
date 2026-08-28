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
  console.log(`\n=== Archive ozon K18001 products (${DRY_RUN ? "DRY RUN" : "LIVE"}) ===\n`);

  const ozonRows = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, target, target_price, current_price, status
    FROM warehouse_products WHERE offer_id = 'K18001' AND marketplace = 'ozon'
  `);
  console.log(`Ozon K18001 products (${ozonRows.length}):`);
  for (const r of ozonRows) {
    console.log(`  id=${r.id} target="${r.target}" status=${r.status}`);
  }

  if (!DRY_RUN && ozonRows.length > 0) {
    const ozonIds = ozonRows.map((r) => r.id);

    // Delete ozon K18001 product_links — these offers don't exist in Ozon so no supplier
    // computation is needed. The yandex K18001 has its own separate link record.
    for (const id of ozonIds) {
      const delLinks = await prisma.$executeRawUnsafe(
        `DELETE FROM product_links WHERE product_id = $1`, id,
      );
      console.log(`  Deleted ${delLinks} link(s) for ozon product ${id}`);
    }

    // Mark ozon products as archived so automation ignores them
    const updated = await prisma.$executeRawUnsafe(`
      UPDATE warehouse_products
      SET
        raw = jsonb_set(
          jsonb_set(raw, '{marketplaceState}', '{"code":"archived","reason":"product_not_found_in_ozon"}'),
          '{archived}', 'true'
        ),
        target_price = NULL,
        current_price = NULL,
        updated_at = NOW()
      WHERE offer_id = 'K18001' AND marketplace = 'ozon'
    `);
    console.log(`Archived ${updated} ozon K18001 product(s)`);

    // Clear any retry queue entries
    const deleted = await prisma.$executeRawUnsafe(`DELETE FROM price_retry_queue WHERE offer_id = 'K18001'`);
    console.log(`Deleted ${deleted} K18001 retry queue item(s)`);

    // Verify links removed
    const linksAfter = await prisma.$queryRawUnsafe(`
      SELECT pl.id, pl.product_id FROM product_links pl
      JOIN warehouse_products wp ON wp.id = pl.product_id
      WHERE wp.offer_id = 'K18001'
    `);
    console.log(`\nRemaining K18001 links: ${linksAfter.length}`);
    for (const l of linksAfter) {
      console.log(`  linkId=${l.id} productId=${l.product_id}`);
    }
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
