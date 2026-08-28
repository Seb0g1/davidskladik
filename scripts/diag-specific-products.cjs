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

  const products = await prisma.$queryRawUnsafe(`
    SELECT
      wp.id::text AS id, wp.offer_id, wp.marketplace,
      wp.current_price, wp.target_price, wp.target_stock,
      wp.archived,
      wp.raw->>'yandexMarkup'  AS ym_markup,
      wp.raw->>'ozonMarkup'    AS oz_markup,
      wp.raw->>'markup'        AS markup,
      wp.raw->'marketplaceState'->>'stock' AS mp_stock,
      wp.raw->'marketplaceState'->>'code'  AS mp_code,
      wp.raw->'lastYandexPriceSend'->>'status' AS ym_send_status,
      wp.raw->'lastYandexPriceSend'->>'price'  AS ym_send_price,
      wp.raw->'lastOzonPriceSend'->>'status'   AS oz_send_status,
      wp.raw->'lastOzonPriceSend'->>'price'    AS oz_send_price,
      wp.updated_at
    FROM warehouse_products wp
    WHERE wp.offer_id = ANY($1)
    ORDER BY wp.offer_id, wp.marketplace
  `, OFFER_IDS);

  console.log(`\n=== Products (${products.length} entries) ===\n`);
  for (const p of products) {
    console.log(`[${p.offer_id}] [${p.marketplace}] id=${p.id}`);
    console.log(`  current=${p.current_price}₽  target=${p.target_price}₽  stock=${p.target_stock}  mp_stock=${p.mp_stock}  mp_code=${p.mp_code}  archived=${p.archived}`);
    console.log(`  markup: ym=${p.ym_markup} oz=${p.oz_markup} generic=${p.markup}`);
    console.log(`  ym_send: status=${p.ym_send_status} price=${p.ym_send_price}₽`);
    console.log(`  oz_send: status=${p.oz_send_status} price=${p.oz_send_price}₽`);
    console.log(`  updated: ${p.updated_at}`);
    console.log();
  }

  // Get product_links
  const links = await prisma.$queryRawUnsafe(`
    SELECT
      pl.id, pl.supplier_name, pl.partner_id,
      pl.raw->>'matchType'   AS match_type,
      pl.raw->>'sourceRowId' AS source_row_id,
      pl.raw->>'article'     AS article,
      pl.updated_at,
      pm.price               AS pm_price,
      pm.active              AS pm_active,
      pm.native_name         AS pm_name,
      pm.currency            AS pm_currency,
      -- check if sourceRowId still exists in PM
      CASE WHEN pl.raw->>'matchType' = 'selected_row'
        THEN EXISTS(SELECT 1 FROM pm_snapshot_items x WHERE x.row_id = (pl.raw->>'sourceRowId'))
        ELSE NULL
      END AS row_still_exists
    FROM product_links pl
    LEFT JOIN pm_snapshot_items pm ON (
      (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = (pl.raw->>'sourceRowId'))
      OR
      (pl.raw->>'matchType' = 'article'
       AND pm.partner_id::text = pl.partner_id::text
       AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
    )
    WHERE pl.product_id = ANY(
      SELECT id::text FROM warehouse_products WHERE offer_id = ANY($1)
    )
    ORDER BY pl.supplier_name
  `, OFFER_IDS);

  console.log(`=== Product links (${links.length}) ===\n`);
  for (const l of links) {
    const staleFlag = l.match_type === "selected_row" && l.row_still_exists === false ? " ⚠ STALE" : "";
    const pmInfo = l.pm_price
      ? ` PM=${l.pm_currency}$${parseFloat(l.pm_price).toFixed(2)} active=${l.pm_active} "${(l.pm_name || "").slice(0, 50)}"`
      : " (no PM match)";
    console.log(`  ${l.supplier_name} [${l.match_type}]${staleFlag}${pmInfo}`);
    if (l.article) console.log(`    article=${l.article}`);
    if (l.source_row_id) console.log(`    sourceRowId=${l.source_row_id} exists=${l.row_still_exists}`);
  }

  // Check PM snapshot for these offer IDs (by article search)
  console.log(`\n=== PM snapshot search for articles ===\n`);
  for (const offerId of OFFER_IDS) {
    const pmRows = await prisma.$queryRawUnsafe(`
      SELECT row_id, partner_id, partner_name, article, price, currency, active, native_name
      FROM pm_snapshot_items
      WHERE active = true AND (
        article ILIKE $1
        OR native_name ILIKE $2
      )
      ORDER BY price::float ASC
      LIMIT 10
    `, `%${offerId}%`, `%${offerId}%`);
    if (pmRows.length) {
      console.log(`${offerId} — PM rows found:`);
      for (const r of pmRows) {
        console.log(`  rowId=${r.row_id} partner=${r.partner_name} article=${r.article} price=${r.currency}$${parseFloat(r.price || 0).toFixed(2)} "${(r.native_name || "").slice(0, 50)}"`);
      }
    } else {
      console.log(`${offerId} — no PM rows found by article/name`);
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
