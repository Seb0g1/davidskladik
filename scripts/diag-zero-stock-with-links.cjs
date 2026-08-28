#!/usr/bin/env node
"use strict";
// Finds warehouse products that have product_links but resolve to 0 stock in PM snapshot.
// Groups results by root cause.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Products that have at least one link but all their linked PM rows are either
  // missing or inactive. We check selected_row links by sourceRowId and article
  // links by partner_id+article.

  console.log("=== Zero-stock products with links — diagnosis ===\n");

  // 1. selected_row links whose sourceRowId row is MISSING from pm_snapshot_items
  const missingRowId = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock,
           pl.supplier_name, pl.raw->>'sourceRowId' AS source_row_id
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (pl.raw->>'sourceRowId') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM pm_snapshot_items pm WHERE pm.row_id = (pl.raw->>'sourceRowId')
      )
    ORDER BY wp.offer_id, pl.supplier_name
    LIMIT 100
  `);

  // 2. selected_row links whose PM row IS in snapshot but active=false
  const inactiveRowId = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock,
           pl.supplier_name, pl.raw->>'sourceRowId' AS source_row_id,
           pm.price::float AS pm_price
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    JOIN pm_snapshot_items pm ON pm.row_id = (pl.raw->>'sourceRowId')
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND pm.active = false
    ORDER BY wp.offer_id, pl.supplier_name
    LIMIT 100
  `);

  // 3. article links whose best PM row (partner+article) is missing or inactive
  const articleNoMatch = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock,
           pl.supplier_name, pl.supplier_article AS article, pl.partner_id::text AS partner_id,
           COUNT(pm.row_id) FILTER (WHERE pm.active = true) AS active_rows,
           COUNT(pm.row_id) AS total_rows
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id::text = pl.product_id
    LEFT JOIN pm_snapshot_items pm
      ON pm.partner_id::text = pl.partner_id::text
      AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
    WHERE pl.raw->>'matchType' = 'article'
      AND pl.partner_id IS NOT NULL
    GROUP BY wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock,
             pl.supplier_name, pl.supplier_article, pl.partner_id
    HAVING COUNT(pm.row_id) FILTER (WHERE pm.active = true) = 0
    ORDER BY wp.offer_id, pl.supplier_name
    LIMIT 100
  `);

  // 4. Products with links where ALL linked rows (any type) are inactive/missing
  // These are the ones showing as "нет в наличии" with 0 stock
  const allLinksInactive = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock,
           COUNT(DISTINCT pl.id) AS link_count,
           COUNT(DISTINCT pm.row_id) FILTER (WHERE pm.active = true AND pm.price::float > 0) AS active_pm_rows
    FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id::text
    LEFT JOIN pm_snapshot_items pm ON (
      (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = (pl.raw->>'sourceRowId'))
      OR
      (pl.raw->>'matchType' = 'article'
        AND pm.partner_id::text = pl.partner_id::text
        AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
    )
    GROUP BY wp.offer_id, wp.marketplace, wp.current_price, wp.target_stock
    HAVING COUNT(DISTINCT pl.id) > 0
       AND COUNT(DISTINCT pm.row_id) FILTER (WHERE pm.active = true AND pm.price::float > 0) = 0
    ORDER BY wp.offer_id, wp.marketplace
    LIMIT 200
  `);

  console.log(`[A] selected_row links with MISSING sourceRowId in snapshot: ${missingRowId.length}`);
  const missingByOffer = new Map();
  for (const r of missingRowId) {
    const key = r.offer_id;
    if (!missingByOffer.has(key)) missingByOffer.set(key, []);
    missingByOffer.get(key).push(r);
  }
  let shown = 0;
  for (const [offerId, rows] of missingByOffer) {
    if (shown++ >= 10) { console.log(`  ... (${missingByOffer.size - 10} more)`); break; }
    const suppliers = rows.map((r) => `${r.supplier_name}(rowId=${r.source_row_id})`).join(", ");
    console.log(`  art=${offerId} [${rows[0].marketplace}]: ${suppliers}`);
  }

  console.log(`\n[B] selected_row links with INACTIVE PM row: ${inactiveRowId.length}`);
  const inactiveByOffer = new Map();
  for (const r of inactiveRowId) {
    const key = r.offer_id;
    if (!inactiveByOffer.has(key)) inactiveByOffer.set(key, []);
    inactiveByOffer.get(key).push(r);
  }
  shown = 0;
  for (const [offerId, rows] of inactiveByOffer) {
    if (shown++ >= 10) { console.log(`  ... (${inactiveByOffer.size - 10} more)`); break; }
    const suppliers = rows.map((r) => `${r.supplier_name}(${r.pm_price}$)`).join(", ");
    console.log(`  art=${offerId} [${rows[0].marketplace}]: ${suppliers}`);
  }

  console.log(`\n[C] article links with NO active PM match: ${articleNoMatch.length}`);
  shown = 0;
  for (const r of articleNoMatch.slice(0, 10)) {
    console.log(`  art=${r.offer_id} [${r.marketplace}]: ${r.supplier_name} art="${r.article}" partner=${r.partner_id} (total_rows=${r.total_rows})`);
  }
  if (articleNoMatch.length > 10) console.log(`  ... (${articleNoMatch.length - 10} more)`);

  console.log(`\n[D] Products where ALL links have no active PM match (shown as 0 stock): ${allLinksInactive.length}`);
  shown = 0;
  for (const r of allLinksInactive.slice(0, 20)) {
    console.log(`  art=${r.offer_id} [${r.marketplace}] links=${r.link_count} currentPrice=${r.current_price} targetStock=${r.target_stock}`);
  }
  if (allLinksInactive.length > 20) console.log(`  ... (${allLinksInactive.length - 20} more)`);

  // Summary
  console.log(`\n=== Summary ===`);
  console.log(`A: Missing rowId (stale selected_row): ${missingRowId.length} links across ${missingByOffer.size} products`);
  console.log(`B: Inactive PM row (selected_row to inactive supplier): ${inactiveRowId.length} links across ${inactiveByOffer.size} products`);
  console.log(`C: No active PM match for article link: ${articleNoMatch.length} links`);
  console.log(`D: Products with ALL links → 0 stock: ${allLinksInactive.length} products`);

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
