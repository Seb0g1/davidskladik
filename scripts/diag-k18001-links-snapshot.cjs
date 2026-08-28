#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // 1. product_links for K18001
  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.product_id, pl.supplier_article, pl.supplier_name,
           pl.partner_id, pl.price_currency, pl.keyword, pl.raw,
           wp.marketplace, wp.offer_id
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY wp.marketplace
  `);
  console.log(`\n=== product_links for K18001 (${links.length} rows) ===\n`);
  for (const l of links) {
    const raw = typeof l.raw === "object" && l.raw ? l.raw : {};
    console.log(`  [${l.marketplace}] link.id=${l.id}`);
    console.log(`    article="${l.supplier_article}" supplierName="${l.supplier_name}" partnerId=${l.partner_id}`);
    console.log(`    priceCurrency=${l.price_currency} keyword=${l.keyword}`);
    console.log(`    raw.matchType=${raw.matchType} raw.sourceRowId=${raw.sourceRowId} raw.snooze=${JSON.stringify(raw.snooze)}`);
    console.log(`    raw.priceCurrency=${raw.priceCurrency} raw.currency=${raw.currency} raw.partnerId=${raw.partnerId}`);
  }

  // 2. PM snapshot rows for article K18001
  const snapshotRows = await prisma.$queryRawUnsafe(`
    SELECT s.id, s.row_id, s.article, s.name, s.partner_id, s.partner_name,
           s.price, s.currency, s.doc_date, s.active,
           s.raw
    FROM pm_snapshot_items s
    WHERE s.article = 'K18001'
    ORDER BY s.active DESC, s.doc_date DESC
  `);
  console.log(`\n=== pm_snapshot_items for K18001 (${snapshotRows.length} rows) ===\n`);
  for (const r of snapshotRows) {
    const raw = typeof r.raw === "object" ? r.raw : {};
    console.log(`  rowId=${r.row_id} partnerId=${r.partner_id} partnerName="${r.partner_name}"`);
    console.log(`    price=${r.price} currency=${r.currency} active=${r.active} docDate=${r.doc_date}`);
    console.log(`    raw.currency=${raw.currency} raw.Currency=${raw.Currency} raw.priceCurrency=${raw.priceCurrency}`);
    console.log(`    raw.partnerId=${raw.partnerId} raw.PartnerID=${raw.PartnerID}`);
  }

  // 3. managed_supplier for Тимофей (partner 278)
  const timofey = await prisma.$queryRawUnsafe(`
    SELECT id, partner_id, name, default_currency, active, raw
    FROM managed_suppliers
    WHERE partner_id = '278'
  `);
  console.log(`\n=== managed_supplier partnerId=278 ===\n`);
  for (const s of timofey) {
    const raw = typeof s.raw === "object" ? s.raw : {};
    console.log(`  id=${s.id} name="${s.name}" defaultCurrency=${s.default_currency} active=${s.active}`);
    console.log(`  raw.priceCurrency=${raw.priceCurrency} raw.currency=${raw.currency} raw.defaultCurrency=${raw.defaultCurrency}`);
  }

  // 4. warehouse_products raw for K18001 (check product links & supplier data stored in raw)
  const wh = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, current_price, target_price, raw
    FROM warehouse_products
    WHERE offer_id = 'K18001'
    ORDER BY marketplace
  `);
  console.log(`\n=== warehouse_products raw for K18001 ===\n`);
  for (const w of wh) {
    const raw = typeof w.raw === "object" ? w.raw : {};
    const links = Array.isArray(raw.links) ? raw.links : [];
    console.log(`  [${w.marketplace}] id=${w.id} currentPrice=${w.current_price} targetPrice=${w.target_price}`);
    for (const lk of links) {
      console.log(`    raw.link: matchType=${lk.matchType} article="${lk.article}" supplierName="${lk.supplierName}" partnerId=${lk.partnerId} priceCurrency=${lk.priceCurrency}`);
    }
    if (links.length === 0) console.log(`    (raw.links: ${Array.isArray(raw.links) ? 'empty array' : 'not an array'})`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
