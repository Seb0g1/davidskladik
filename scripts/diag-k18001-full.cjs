#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // All K18001 and ЮК345754 warehouse entries with full details
  const products = await prisma.$queryRawUnsafe(`
    SELECT
      id::text AS id, offer_id, marketplace,
      current_price, target_price, target_stock, archived,
      raw->>'ozonProductId'        AS ozon_product_id,
      raw->>'ozonFboSku'           AS ozon_fbo_sku,
      raw->>'ozonFbsSku'           AS ozon_fbs_sku,
      raw->>'yandexSku'            AS yandex_sku,
      raw->'marketplaceState'->>'stock'    AS mp_stock,
      raw->'marketplaceState'->>'price'    AS mp_price,
      raw->'marketplaceState'->>'code'     AS mp_code,
      raw->'lastOzonPriceSend'->>'status'  AS oz_status,
      raw->'lastOzonPriceSend'->>'requestedPrice' AS oz_requested,
      raw->'lastOzonPriceSend'->>'detail'  AS oz_detail,
      raw->>'ozonMarkup'           AS oz_markup,
      raw->>'yandexMarkup'         AS ym_markup,
      raw->>'markup'               AS generic_markup,
      raw->>'priceVerifiedAt'      AS price_verified_at,
      updated_at
    FROM warehouse_products
    WHERE offer_id IN ('K18001','ЮК345754')
    ORDER BY offer_id, marketplace, archived, updated_at DESC
  `);

  console.log(`\n=== All entries (${products.length}) ===\n`);
  for (const p of products) {
    console.log(`[${p.offer_id}][${p.marketplace}] id=${p.id}`);
    console.log(`  archived=${p.archived} current=${p.current_price}₽ target=${p.target_price}₽ stock=${p.target_stock}`);
    console.log(`  mp_stock=${p.mp_stock} mp_price=${p.mp_price} mp_code=${p.mp_code}`);
    console.log(`  ozon_id=${p.ozon_product_id} fbo=${p.ozon_fbo_sku} fbs=${p.ozon_fbs_sku} ym_sku=${p.yandex_sku}`);
    console.log(`  oz_send: status=${p.oz_status} price=${p.oz_requested} detail=${p.oz_detail}`);
    console.log(`  markup: oz=${p.oz_markup} ym=${p.ym_markup} generic=${p.generic_markup}`);
    console.log(`  priceVerifiedAt=${p.price_verified_at}`);
    console.log(`  updated=${p.updated_at}`);
    console.log();
  }

  // PM snapshot for these offer_ids - what price PM has
  const pmRows = await prisma.$queryRawUnsafe(`
    SELECT pm.row_id, pm.partner_id, pm.partner_name, pm.article, pm.price, pm.currency,
           pm.active, pm.native_name
    FROM pm_snapshot_items pm
    WHERE pm.article IN ('K18001','ЮК345754','23216')
       OR pm.native_name ILIKE '%K18001%'
       OR pm.native_name ILIKE '%345754%'
    ORDER BY pm.active DESC, pm.partner_name
    LIMIT 20
  `);
  console.log(`\n=== PM snapshot rows ===\n`);
  for (const r of pmRows) {
    console.log(`  rowId=${r.row_id} partner=${r.partner_name} article=${r.article} price=${r.currency}$${parseFloat(r.price||0).toFixed(2)} active=${r.active} "${(r.native_name||"").slice(0,50)}"`);
  }

  // Check product_links for these products
  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id::text, pl.product_id, pl.supplier_name, pl.partner_id,
           pl.raw->>'matchType' AS match_type,
           pl.raw->>'sourceRowId' AS source_row_id,
           pl.raw->>'article' AS article,
           pm.price AS pm_price, pm.currency AS pm_currency, pm.active AS pm_active,
           wp.offer_id, wp.marketplace, wp.archived AS wp_archived
    FROM product_links pl
    LEFT JOIN pm_snapshot_items pm ON (
      (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = (pl.raw->>'sourceRowId'))
      OR (pl.raw->>'matchType' = 'article'
          AND pm.partner_id::text = pl.partner_id::text
          AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
    )
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    WHERE wp.offer_id IN ('K18001','ЮК345754')
    ORDER BY wp.offer_id, wp.marketplace
  `);
  console.log(`\n=== Product links (${links.length}) ===\n`);
  for (const l of links) {
    console.log(`  [${l.offer_id}][${l.marketplace}] archived=${l.wp_archived}`);
    console.log(`    supplier=${l.supplier_name} matchType=${l.match_type}`);
    console.log(`    PM: price=${l.pm_currency}$${parseFloat(l.pm_price||0).toFixed(2)} active=${l.pm_active}`);
    if (l.source_row_id) console.log(`    sourceRowId=${l.source_row_id}`);
    if (l.article) console.log(`    article=${l.article}`);
    console.log();
  }

  // How the price is computed: fetch app settings fixedUsdRate + markupRules
  const appSetting = await prisma.$queryRawUnsafe(`SELECT value FROM app_settings WHERE key = 'app'`).catch(() => []);
  if (appSetting.length) {
    const parsed = typeof appSetting[0].value === "string" ? JSON.parse(appSetting[0].value) : appSetting[0].value;
    const rate = parsed.fixedUsdRate || 85;
    console.log(`\n=== Price simulation (fixedUsdRate=${rate}) ===\n`);
    // K18001: $16 USD
    const k18001_pm = 16;
    // minUsd=15 ozon coefficient=3.83
    const k18001_coeff_ozon = 3.83;
    const k18001_coeff_ym = 2.222;
    console.log(`K18001 PM=$${k18001_pm}`);
    console.log(`  Ozon coeff=${k18001_coeff_ozon}: formula1=PM×coeff=${(k18001_pm*k18001_coeff_ozon).toFixed(0)}₽ | formula2=PM×rate×coeff=${(k18001_pm*rate*k18001_coeff_ozon).toFixed(0)}₽`);
    console.log(`  YM  coeff=${k18001_coeff_ym}: formula1=PM×coeff=${(k18001_pm*k18001_coeff_ym).toFixed(0)}₽ | formula2=PM×rate×coeff=${(k18001_pm*rate*k18001_coeff_ym).toFixed(0)}₽`);
    console.log(`  → DB shows ozon=61₽, ym=36₽ (matches formula1: PM×coeff without rate)`);
    const uk_pm = 3;
    const uk_coeff_ozon = 5.65; // minUsd=3 ozon
    console.log(`\nЮК345754 PM=$${uk_pm}`);
    console.log(`  Ozon coeff=${uk_coeff_ozon}: formula1=PM×coeff=${(uk_pm*uk_coeff_ozon).toFixed(0)}₽ | formula2=PM×rate×coeff=${(uk_pm*rate*uk_coeff_ozon).toFixed(0)}₽`);
    console.log(`  → DB shows current=1428₽ (matches formula2: PM×rate×coeff)`);
    console.log(`\nConclusion: K18001 was computed WITHOUT rate, ЮК345754 WITH rate — different code paths or timing`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
