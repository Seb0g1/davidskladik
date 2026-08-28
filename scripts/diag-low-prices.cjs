#!/usr/bin/env node
"use strict";
// Diagnoses products with suspiciously low prices (< 500 RUB with stock > 0).

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  const lowPrices = await prisma.$queryRawUnsafe(`
    SELECT
      wp.offer_id, wp.marketplace, wp.current_price,
      wp.target_stock,
      wp.raw->>'selectedSupplier' AS selected_supplier_json,
      wp.raw->'autoPriceMin' AS auto_price_min,
      wp.raw->'autoPriceMax' AS auto_price_max,
      wp.raw->'lastYandexPriceSend' AS last_ym_send,
      wp.raw->'lastOzonPriceSend'   AS last_ozon_send,
      -- Active supplier links and their PM prices
      (
        SELECT json_agg(json_build_object(
          'supplier', pl.supplier_name,
          'matchType', pl.raw->>'matchType',
          'pmPrice', pm.price,
          'pmActive', pm.active,
          'pmName', pm.native_name
        ))
        FROM product_links pl
        LEFT JOIN pm_snapshot_items pm ON (
          (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = (pl.raw->>'sourceRowId'))
          OR
          (pl.raw->>'matchType' = 'article' AND pm.partner_id::text = pl.partner_id::text
            AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
        )
        WHERE pl.product_id = wp.id::text
      ) AS links
    FROM warehouse_products wp
    WHERE wp.target_stock > 0
      AND wp.current_price > 0
      AND wp.current_price < 500
    ORDER BY wp.current_price ASC
    LIMIT 30
  `);

  console.log(`\n=== Products with price < 500 RUB and stock > 0 (${lowPrices.length}) ===\n`);

  for (const r of lowPrices) {
    console.log(`art=${r.offer_id} [${r.marketplace}] price=${r.current_price}₽ stock=${r.target_stock}`);
    const links = r.links || [];
    for (const l of links) {
      const pmInfo = l.pmPrice ? ` PM=$${parseFloat(l.pmPrice).toFixed(2)} (${l.pmActive ? "active" : "INACTIVE"}) "${(l.pmName||"").slice(0,50)}"` : " (no PM row)";
      console.log(`  └─ ${l.supplier} [${l.matchType}]${pmInfo}`);
    }
    if (r.auto_price_min || r.auto_price_max) {
      console.log(`  ⚠ autoPriceMin=${r.auto_price_min} autoPriceMax=${r.auto_price_max}`);
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
