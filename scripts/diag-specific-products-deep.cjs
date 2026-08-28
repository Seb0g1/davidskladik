#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

const OFFER_IDS = (process.argv[2] || "ЮК345754,K18001").split(",").map((s) => s.trim());

async function main() {
  const prisma = getPrisma();

  // Full raw for price send errors + selected supplier info
  const products = await prisma.$queryRawUnsafe(`
    SELECT
      wp.id::text AS id, wp.offer_id, wp.marketplace,
      wp.current_price, wp.target_price, wp.target_stock,
      wp.raw->'lastOzonPriceSend'    AS last_ozon_send,
      wp.raw->'lastYandexPriceSend'  AS last_ym_send,
      wp.raw->'noSupplierAutomation' AS no_supplier,
      wp.raw->'links'                AS ym_links
    FROM warehouse_products wp
    WHERE wp.offer_id = ANY($1)
    ORDER BY wp.offer_id, wp.marketplace
  `, OFFER_IDS);

  console.log(`\n=== Deep product raw data ===\n`);
  for (const p of products) {
    console.log(`[${p.offer_id}] [${p.marketplace}] current=${p.current_price}₽ target=${p.target_price}₽ stock=${p.target_stock}`);
    if (p.last_ozon_send) {
      const s = typeof p.last_ozon_send === "string" ? JSON.parse(p.last_ozon_send) : p.last_ozon_send;
      console.log(`  Ozon send: status=${s.status} price=${s.price} error=${JSON.stringify(s.error || s.errors || s.failReason || "")}`);
    }
    if (p.last_ym_send) {
      const s = typeof p.last_ym_send === "string" ? JSON.parse(p.last_ym_send) : p.last_ym_send;
      console.log(`  YM send: status=${s.status} price=${s.price} error=${JSON.stringify(s.error || s.errors || "")}`);
    }
    if (p.no_supplier) {
      const ns = typeof p.no_supplier === "string" ? JSON.parse(p.no_supplier) : p.no_supplier;
      console.log(`  noSupplierAutomation: ${JSON.stringify(ns)}`);
    }
    console.log();
  }

  // Duplicate links check for K18001
  console.log(`=== Duplicate links check ===\n`);
  const dupLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.product_id, pl.supplier_name, pl.partner_id, COUNT(*) AS n,
           array_agg(pl.id::text) AS ids
    FROM product_links pl
    WHERE pl.product_id IN (
      SELECT id::text FROM warehouse_products WHERE offer_id = ANY($1)
    )
    GROUP BY pl.product_id, pl.supplier_name, pl.partner_id
    HAVING COUNT(*) > 1
    ORDER BY n DESC
  `, OFFER_IDS);

  if (dupLinks.length) {
    console.log(`Found ${dupLinks.length} duplicate supplier groups:`);
    for (const d of dupLinks) {
      console.log(`  product=${d.product_id} supplier=${d.supplier_name} x${d.n}: ids=${d.ids.join(", ")}`);
    }
  } else {
    console.log("No duplicate links found");
  }

  // Check if ЮК345754 appears in sales_automation_sku_states
  console.log(`\n=== salesAutomationSkuState ===\n`);
  const sas = await prisma.$queryRawUnsafe(`
    SELECT s.product_id, s.price_status, s.reason, s.updated_at
    FROM sales_automation_sku_states s
    WHERE s.product_id IN (
      SELECT id FROM warehouse_products WHERE offer_id = ANY($1)
    )
  `, OFFER_IDS).catch(() => []);
  for (const s of sas) {
    console.log(`  product=${s.product_id} price_status=${s.price_status} reason=${s.reason} updated=${s.updated_at}`);
  }
  if (!sas.length) console.log("  (none found or table doesn't exist)");

  // Yandex entry for ЮК345754?
  console.log(`\n=== All entries for ЮК345754 ===\n`);
  const uk = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id, marketplace, current_price, target_price, target_stock, archived
    FROM warehouse_products WHERE offer_id = 'ЮК345754'
  `);
  for (const r of uk) {
    console.log(`  [${r.marketplace}] id=${r.id} current=${r.current_price}₽ target=${r.target_price}₽ stock=${r.target_stock} archived=${r.archived}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
