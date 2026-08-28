#!/usr/bin/env node
/**
 * Finds ProductLinks where the pinned sourceRowId has price=0 (or doesn't exist) in
 * pm_snapshot_items, but another row from the same supplier with the same article has
 * price>0. Updates the sourceRowId to point to the newest active row.
 *
 * Usage:
 *   node scripts/fix-stale-row-id-links.cjs          # dry run — report only
 *   node scripts/fix-stale-row-id-links.cjs --apply  # apply fixes
 */
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");

const APPLY = process.argv.includes("--apply");

async function main() {
  const prisma = new PrismaClient();
  try {
    // Step 1: find all selected_row links that have a sourceRowId
    const staleLinks = await prisma.$queryRawUnsafe(`
      SELECT
        pl.id                            AS link_id,
        pl.product_id,
        pl.supplier_article,
        pl.partner_id,
        pl.supplier_name,
        pl.raw->>'sourceRowId'          AS pinned_row_id,
        pl.raw->>'matchType'            AS match_type,
        pm_old.row_id                   AS old_row_id,
        pm_old.price::float             AS old_price,
        pm_old.active                   AS old_active,
        pm_old.native_name              AS old_name,
        pm_new.row_id                   AS new_row_id,
        pm_new.price::float             AS new_price,
        pm_new.native_name              AS new_name,
        pm_new.doc_date                 AS new_doc_date,
        wp.raw->>'offerId'              AS offer_id,
        wp.marketplace
      FROM product_links pl
      JOIN warehouse_products wp ON wp.id = pl.product_id
      -- The currently-pinned PM row (may have price=0 or not exist)
      LEFT JOIN pm_snapshot_items pm_old
        ON pm_old.row_id = (pl.raw->>'sourceRowId')
        AND pm_old.partner_id::text = pl.partner_id::text
      -- The best available row: same article + same partner, active, price > 0, newest doc
      JOIN LATERAL (
        SELECT pm2.row_id, pm2.price, pm2.native_name, pm2.doc_date
        FROM pm_snapshot_items pm2
        WHERE pm2.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article)
          AND pm2.partner_id::text = pl.partner_id::text
          AND pm2.active = true
          AND pm2.price IS NOT NULL AND pm2.price > 0
        ORDER BY pm2.doc_date DESC, pm2.row_id DESC
        LIMIT 1
      ) pm_new ON true
      WHERE pl.raw->>'matchType' = 'selected_row'
        AND pl.raw->>'sourceRowId' IS NOT NULL
        AND pl.raw->>'sourceRowId' != ''
        -- Only when the new best row is DIFFERENT from the pinned row
        AND pm_new.row_id != (pl.raw->>'sourceRowId')
        -- Only when the pinned row has price=0, price null, or row doesn't exist
        AND (pm_old.row_id IS NULL OR pm_old.price IS NULL OR pm_old.price = 0)
      ORDER BY wp.raw->>'offerId', pl.partner_id
    `);

    if (!staleLinks.length) {
      console.log("No stale pinned-rowId links found. All good.");
      return;
    }

    console.log(`Found ${staleLinks.length} stale link(s):\n`);
    for (const row of staleLinks) {
      const oldStatus = row.old_row_id
        ? `active=${row.old_active} price=${row.old_price ?? "null"} name="${row.old_name}"`
        : "NOT IN SNAPSHOT";
      console.log(
        `[${row.marketplace}] ${row.offer_id} | partner=${row.partner_id} | article=${row.supplier_article}\n` +
        `  pinned row   : ${row.pinned_row_id} → ${oldStatus}\n` +
        `  best new row : ${row.new_row_id}  price=${row.new_price}  name="${row.new_name}"  doc=${String(row.new_doc_date).slice(0,10)}\n`,
      );
    }

    if (!APPLY) {
      console.log(`\nDRY RUN — run with --apply to fix ${staleLinks.length} link(s).`);
      return;
    }

    // Apply: update sourceRowId for each stale link
    let fixed = 0;
    for (const row of staleLinks) {
      await prisma.$executeRawUnsafe(`
        UPDATE product_links
        SET raw = jsonb_set(
              jsonb_set(raw, '{sourceRowId}', $1::jsonb),
              '{resolvedBy}', '"stale_row_id_fix"'
            ),
            updated_at = now()
        WHERE id = $2
      `, JSON.stringify(row.new_row_id), row.link_id);
      fixed++;
      console.log(`Fixed link ${row.link_id}: ${row.pinned_row_id} → ${row.new_row_id} (${row.offer_id})`);
    }

    console.log(`\nDone. Fixed ${fixed}/${staleLinks.length} link(s).`);
    console.log("Run a warehouse refresh to pick up the new prices.");

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
