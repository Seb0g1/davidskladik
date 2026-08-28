#!/usr/bin/env node
"use strict";
// Checks what product.links the warehouse object has for 10825 YM.
// The "общие привязки" label suggests group links — verify links are in the product.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

async function main() {
  console.log("=== 10825 YM warehouse product.links check ===\n");

  // Read the warehouse from PostgreSQL (same as server)
  const { getPrisma } = require("../lib/postgres.js");
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // Get the raw warehouse_products row for 10825 YM
  const ym = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id, marketplace, current_price, target_price,
           raw->>'nextPrice' AS next_price,
           raw->>'groupId' AS group_id,
           raw->'links' AS links_json,
           jsonb_array_length(CASE WHEN raw->'links' IS NOT NULL AND jsonb_typeof(raw->'links') = 'array' THEN raw->'links' ELSE '[]'::jsonb END) AS links_count
    FROM warehouse_products WHERE offer_id ILIKE '10825' AND marketplace = 'yandex'
  `);
  if (!ym.length) { console.log("Not found"); return; }
  const w = ym[0];
  console.log(`id=${w.id} mp=${w.marketplace} currentPrice=${w.current_price} targetPrice=${w.target_price} nextPrice=${w.next_price}`);
  console.log(`groupId=${w.group_id || "(none)"} linksCount=${w.links_count}`);

  // Show the links stored IN the raw JSON
  if (w.links_json) {
    const links = typeof w.links_json === "string" ? JSON.parse(w.links_json) : (Array.isArray(w.links_json) ? w.links_json : []);
    console.log(`\nLinks in raw JSON (${links.length}):`);
    for (const l of links.slice(0, 10)) {
      const rpmRowId = l.resolvedPriceMasterRow?.rowId;
      const rpmPrice = l.resolvedPriceMasterRow?.price;
      console.log(`  id=${l.id || "(no-id)"} supplier=${l.supplierName || "(none)"} art=${l.article || l.supplierArticle || "(none)"} sourceRowId=${l.sourceRowId || "(none)"}`);
      console.log(`    rpm: rowId=${rpmRowId || "(none)"} price=${rpmPrice || "(none)"}`);
    }
  }

  // Also check if there are product_links rows for this product
  const wpId = String(w.id).replace(/[^a-zA-Z0-9_-]/g, "");
  const plCount = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM product_links WHERE product_id = '${wpId}'
  `);
  console.log(`\nproduct_links rows for this product: ${plCount[0]?.cnt || 0}`);

  // Check if the product is part of a group and if the group has links
  if (w.group_id) {
    const groupLinks = await prisma.$queryRawUnsafe(`
      SELECT COUNT(*) AS cnt FROM product_links WHERE product_id = '${String(w.group_id).replace(/[^a-zA-Z0-9_-]/g, "")}'
    `);
    console.log(`product_links rows for group ${w.group_id}: ${groupLinks[0]?.cnt || 0}`);
  }

  // Read what readWarehouseFromPostgres would give for this product's links
  // Check how the product is read in the warehouse build
  const fullProduct = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id, marketplace,
           jsonb_array_length(CASE WHEN raw->'links' IS NOT NULL AND jsonb_typeof(raw->'links') = 'array' THEN raw->'links' ELSE '[]'::jsonb END) AS raw_links_count,
           (SELECT COUNT(*) FROM product_links pl WHERE pl.product_id = wp.id::text) AS pg_links_count
    FROM warehouse_products wp WHERE offer_id ILIKE '10825'
  `);
  console.log("\nAll 10825 products — links comparison:");
  for (const p of fullProduct) {
    console.log(`  ${p.marketplace}: rawLinksCount=${p.raw_links_count} pgLinksCount=${p.pg_links_count}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
