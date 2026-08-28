#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  const products = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, target, target_price, current_price, status, updated_at
    FROM warehouse_products WHERE offer_id = 'K18001' ORDER BY marketplace
  `);
  console.log(`\nwarehouse_products for K18001 (${products.length}):`);
  for (const p of products) {
    console.log(`  [${p.marketplace}] id=${p.id}`);
    console.log(`    target="${p.target}" targetPrice=${p.target_price} currentPrice=${p.current_price} status=${p.status}`);
  }

  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.product_id, pl.partner_id, pl.price_currency, pl.supplier_article, pl.raw,
           wp.marketplace, wp.offer_id
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE wp.offer_id = 'K18001'
    ORDER BY wp.marketplace, pl.partner_id
  `);
  console.log(`\nproduct_links for K18001 (${links.length}):`);
  for (const l of links) {
    const raw = typeof l.raw === "object" ? l.raw : {};
    console.log(`  [${l.marketplace}] linkId=${l.id} partnerId=${l.partner_id} priceCurrency=${l.price_currency}`);
    console.log(`    supplierArticle=${l.supplier_article} raw.article=${raw.article} raw.matchType=${raw.matchType} raw.id=${raw.id}`);
  }

  // Check YANDEX_SHOPS_JSON from env
  let shops = [];
  try { shops = JSON.parse(process.env.YANDEX_SHOPS_JSON || "[]"); } catch (_) {}
  console.log(`\nYandex shops (${shops.length}):`);
  for (const s of shops) {
    console.log(`  id=${s.id} name="${s.name}" businessId=${s.businessId}`);
  }

  // Compare K18001 yandex product.target vs shop IDs
  const yandexProduct = products.find((p) => p.marketplace === "yandex");
  if (yandexProduct) {
    const shopIds = shops.map((s) => String(s.id));
    const matches = shopIds.filter((id) => id === String(yandexProduct.target));
    console.log(`\nK18001 yandex target="${yandexProduct.target}" — matches shop IDs: ${matches.length > 0 ? matches.join(",") : "NONE (lookup will fail!)"}`);
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
