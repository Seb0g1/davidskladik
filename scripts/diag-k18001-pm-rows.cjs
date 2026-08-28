#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // All PM snapshot rows for K18001 across all partners
  const rows = await prisma.$queryRawUnsafe(`
    SELECT row_id, partner_id, partner_name, article, price, currency, active
    FROM pm_snapshot_items
    WHERE article = 'K18001'
    ORDER BY partner_name
  `);

  console.log(`\n=== pm_snapshot_items for article=K18001 (${rows.length} rows) ===\n`);
  for (const r of rows) {
    console.log(`  rowId=${r.row_id} partnerId=${r.partner_id} partner="${r.partner_name}" price=${r.price} currency=${r.currency} active=${r.active}`);
  }

  // Also check K18001 warehouse products and their product_links
  const wh = await prisma.$queryRawUnsafe(`
    SELECT wp.id, wp.marketplace, wp.offer_id
    FROM warehouse_products wp
    WHERE wp.offer_id = 'K18001'
  `);

  console.log(`\n=== warehouse_products K18001 (${wh.length} entries) ===\n`);
  for (const p of wh) {
    console.log(`  id=${p.id} marketplace=${p.marketplace} offerId=${p.offer_id}`);
    const links = await prisma.$queryRawUnsafe(`
      SELECT partner_id, supplier_name, supplier_article, price_currency, keyword, raw
      FROM product_links
      WHERE product_id = $1
    `, p.id);
    for (const l of links) {
      console.log(`    link: partnerId=${l.partner_id} supplierName="${l.supplier_name}" article="${l.supplier_article}" priceCurrency=${l.price_currency} keyword="${l.keyword}"`);
    }
  }

  // Read the price_history response for K18001 to see actual breakdown
  const hist = await prisma.$queryRawUnsafe(`
    SELECT ph.new_price, ph.status, ph.response, ph.created_at
    FROM price_history ph
    JOIN warehouse_products wp ON wp.id = ph.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY ph.created_at DESC
    LIMIT 5
  `);

  console.log(`\n=== price_history K18001 (last 5) ===\n`);
  for (const h of hist) {
    const resp = typeof h.response === "string" ? JSON.parse(h.response) : (h.response || {});
    console.log(`  newPrice=${h.new_price} status=${h.status} at=${h.created_at}`);
    console.log(`    response: pmPriceUsd=${resp.pmPriceUsd} usdRate=${resp.usdRate} markup=${resp.markup} rubNative=${resp.rubNative} priceCurrency=${resp.priceCurrency} supplierCurrency=${resp.supplierPriceCurrency}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
