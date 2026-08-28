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
  console.log(`\n=== K18001 price fix (${DRY_RUN ? "DRY RUN" : "LIVE"}) ===\n`);

  // 1. Show current state
  const retryItems = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, price, status, attempts, next_retry_at, error, created_at, updated_at
    FROM price_retry_queue WHERE offer_id = 'K18001' ORDER BY marketplace, updated_at DESC
  `);
  console.log(`price_retry_queue for K18001 (${retryItems.length} items):`);
  for (const r of retryItems) {
    console.log(`  [${r.marketplace}] id=${r.id} price=${r.price} status=${r.status} attempts=${r.attempts} error="${r.error}"`);
  }

  const products = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, current_price, target_price, status, updated_at
    FROM warehouse_products WHERE offer_id = 'K18001' ORDER BY marketplace
  `);
  console.log(`\nwarehouse_products for K18001 (${products.length}):`);
  for (const p of products) {
    console.log(`  [${p.marketplace}] id=${p.id} currentPrice=${p.current_price} targetPrice=${p.target_price} status=${p.status}`);
  }

  // Show K18001 product_links to understand why PM match may fail
  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.product_id, pl.partner_id, pl.price_currency, pl.supplier_article, pl.raw,
           wp.marketplace, wp.offer_id, wp.status as wp_status
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY wp.marketplace, pl.partner_id
  `);
  console.log(`\nproduct_links for K18001 (${links.length}):`);
  for (const l of links) {
    const raw = typeof l.raw === "object" ? l.raw : {};
    console.log(`  [${l.marketplace}] wpId=${l.product_id} partnerId=${l.partner_id} linkId=${l.id} priceCurrency=${l.price_currency}`);
    console.log(`    supplierArticle=${l.supplier_article} raw.article=${raw.article} raw.matchType=${raw.matchType}`);
  }

  // 2. Delete bad K18001 ozon retry items (price=61, status=failed)
  const badOzonRetry = retryItems.filter((r) => r.marketplace === "ozon" && Number(r.price) < 200);
  console.log(`\n=> Will delete ${badOzonRetry.length} bad ozon retry item(s) (price<200₽)`);
  for (const r of badOzonRetry) {
    console.log(`   DELETE price_retry_queue id=${r.id} price=${r.price} status=${r.status}`);
  }
  if (!DRY_RUN && badOzonRetry.length > 0) {
    const ids = badOzonRetry.map((r) => r.id);
    const deleted = await prisma.$executeRawUnsafe(
      `DELETE FROM price_retry_queue WHERE id = ANY($1::text[])`,
      ids,
    );
    console.log(`   Deleted ${deleted} row(s) from price_retry_queue`);
  }

  // 3. Reset yandex K18001 stale prices in both column AND raw JSON so the fallback stops.
  //    productFromPostgres falls to raw.targetPrice when column is NULL, so we must wipe both.
  const yandexProduct = products.find((p) => p.marketplace === "yandex");
  if (yandexProduct) {
    const colPrice = Number(yandexProduct.target_price || yandexProduct.current_price || 0);
    if (colPrice === 0 || colPrice >= 200) {
      console.log(`\n=> Yandex column prices already ok (targetPrice=${yandexProduct.target_price}, currentPrice=${yandexProduct.current_price}) — still clearing raw JSON`);
    } else {
      console.log(`\n=> Will reset yandex stale prices (targetPrice=${yandexProduct.target_price}, currentPrice=${yandexProduct.current_price})`);
    }
    console.log(`   Wiping target_price, current_price columns AND raw.targetPrice/nextPrice/currentPrice JSON fields`);
    if (!DRY_RUN) {
      const updated = await prisma.$executeRawUnsafe(`
        UPDATE warehouse_products
        SET
          target_price = NULL,
          current_price = NULL,
          raw = raw
            - 'targetPrice'
            - 'nextPrice'
            - 'currentPrice'
            - 'marketplacePrice',
          updated_at = NOW()
        WHERE offer_id = 'K18001' AND marketplace = 'yandex'
      `);
      console.log(`   Updated ${updated} yandex row(s) — columns+raw price fields wiped`);
    }
  } else {
    console.log(`\n=> No yandex K18001 warehouse_product found`);
  }

  // 4. Show last 10 price_history for K18001 after fix
  const hist = await prisma.$queryRawUnsafe(`
    SELECT ph.new_price, ph.status, ph.marketplace, ph.created_at,
           (ph.response->>'error') as error_detail
    FROM price_history ph
    JOIN warehouse_products wp ON wp.id = ph.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY ph.created_at DESC
    LIMIT 10
  `);
  console.log(`\nLast 10 price_history for K18001 (newest first):`);
  for (const h of hist) {
    const errMsg = h.error_detail || "";
    console.log(`  [${h.marketplace}] price=${h.new_price} status=${h.status} at=${String(h.created_at).substring(0, 24)}${errMsg ? ` | ${errMsg.substring(0, 80)}` : ""}`);
  }

  if (DRY_RUN) {
    console.log("\n=> DRY RUN complete — rerun without --dry-run to apply changes\n");
  } else {
    console.log("\n=> Fix applied. K18001 ozon retry items deleted. Yandex targetPrice reset.\n");
    console.log("   Next steps:");
    console.log("   - Ozon: K18001 'product not found' means the offer ID may not exist in those Ozon cabinets.");
    console.log("     Check if K18001 is mapped to correct Ozon offer IDs and set ozon warehouse_products to archived if not.");
    console.log("   - Yandex: both column AND raw JSON price fields wiped. Next auto-sync will compute fresh from PM (~3022₽).");
    console.log("     If the warehouse page shows ~3022 for K18001, manually send the price to Yandex from Operations page.");
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
