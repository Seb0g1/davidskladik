// Fix Dalik selected_row links after bulk article renumbering in PriceMaster.
// Dalik periodically renumbers ALL their NativeIDs (article codes).
// Our product_links store source_row_id (PM RowID) which is permanent,
// but the NativeName at that RowID may have changed after renumbering.
//
// This script:
//   1. Queries each Dalik selected_row link's source_row_id against live PM
//   2. If NativeName at that RowID diverges from our warehouse product name → reset the pin
//   3. Also updates supplier_article to reflect current NativeID for that RowID
//
// Run with --dry-run first to preview changes.
"use strict";

const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = "true";

const mysql2 = require("mysql2/promise");
const { getPrisma } = require("../lib/postgres.js");

const EXCLUDE_TOKENS = new Set(["edp", "edt", "parfum", "perfume", "extrait", "de", "ml", "eau", "100", "50", "200"]);

function tokens(str) {
  return [...new Set(
    (str || "").toLowerCase()
      .replace(/[^a-z0-9а-яё\s]/gi, " ")
      .split(/\s+/)
      .filter((t) => t.length >= 2 && !EXCLUDE_TOKENS.has(t)),
  )];
}

function overlap(a, b) {
  const setB = new Set(b);
  return a.filter((t) => setB.has(t)).length;
}

function nameDiverged(pmName, warehouseName) {
  const pmT = tokens(pmName);
  const wpT = tokens(warehouseName);
  const shared = overlap(pmT, wpT);
  const uniqueToPm = pmT.filter((t) => !new Set(wpT).has(t));
  const uniqueToWp = wpT.filter((t) => !new Set(pmT).has(t));
  // Diverged if: shared tokens < 2 OR (both sides have exclusive tokens AND penalty >= 40)
  const divergencePenalty = (uniqueToPm.length > 0 && uniqueToWp.length > 0) ? Math.min(uniqueToPm.length, 4) * 20 : 0;
  return { diverged: shared < 2 || divergencePenalty >= 40, shared, divergencePenalty, uniqueToPm, uniqueToWp };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  console.log(`\n=== Dalik article migration fix ${dryRun ? "[DRY RUN]" : "[LIVE]"} ===\n`);

  const prisma = getPrisma();
  const pool = mysql2.createPool({
    host: process.env.PM_DB_HOST || "localhost",
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME,
    waitForConnections: true,
    connectionLimit: 3,
  });

  // 1. Load all Dalik selected_row links that have a source_row_id
  const links = await prisma.$queryRawUnsafe(`
    SELECT
      pl.id        AS link_id,
      pl.supplier_article,
      pl.supplier_name,
      pl.source_row_id,
      pl.exact_name AS pinned_pm_name,
      wp.id        AS product_id,
      wp.offer_id,
      wp.name      AS warehouse_name
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND pl.source_row_id IS NOT NULL
      AND (
        LOWER(pl.supplier_name) LIKE '%далик%'
        OR LOWER(pl.supplier_name) LIKE '%dalik%'
      )
    ORDER BY wp.offer_id
  `);

  console.log(`Found ${links.length} Dalik selected_row links to check\n`);

  let resetCount = 0;
  let articleUpdatedCount = 0;
  let okCount = 0;

  for (const link of links) {
    const rowId = link.source_row_id;

    // 2. Query live PM for current NativeID and NativeName at this RowID
    const [pmRows] = await pool.query(
      `SELECT r.NativeID, r.NativeName, r.Active, r.Ignored
       FROM OfferRows r WHERE r.RowID = ? LIMIT 1`,
      [rowId],
    );

    if (!pmRows.length) {
      console.log(`  ⚠️  ${link.offer_id}: RowID=${rowId} NOT FOUND in PM → resetting pin`);
      if (!dryRun) {
        await prisma.$executeRawUnsafe(`
          UPDATE product_links
          SET source_row_id = NULL, exact_name = NULL,
              raw = jsonb_set(jsonb_set(jsonb_set(raw,'{matchType}','"article"'),'{sourceRowId}','null'),'{exactName}','null'),
              updated_at = now()
          WHERE id = $1
        `, link.link_id);
      }
      resetCount++;
      continue;
    }

    const pm = pmRows[0];
    const currentPmName = pm.NativeName || "";
    const currentNativeId = String(pm.NativeID || "").trim();

    // 3. Check if NativeName at this RowID still matches our warehouse product
    const { diverged, shared, divergencePenalty } = nameDiverged(currentPmName, link.warehouse_name);

    const articleChanged = currentNativeId !== String(link.supplier_article || "").trim();
    const status = diverged ? "RESET" : articleChanged ? "UPDATE_ARTICLE" : "OK";

    if (status === "RESET" || status === "UPDATE_ARTICLE") {
      console.log(`  ${status === "RESET" ? "🔴" : "🟡"} ${link.offer_id}`);
      console.log(`     warehouse: "${(link.warehouse_name || "").slice(0, 55)}"`);
      console.log(`     pm now:   "${currentPmName.slice(0, 55)}" (RowID=${rowId}, NativeID=${currentNativeId})`);
      console.log(`     stored:   article="${link.supplier_article}" pinned="${(link.pinned_pm_name || "").slice(0, 40)}"`);
      console.log(`     shared=${shared} penalty=${divergencePenalty} → ${status}`);
    } else {
      console.log(`  ✅ ${link.offer_id}: RowID=${rowId} NativeID=${currentNativeId} OK (shared=${shared})`);
    }

    if (!dryRun) {
      if (diverged) {
        // Name completely changed → reset pin, let autocart re-resolve
        await prisma.$executeRawUnsafe(`
          UPDATE product_links
          SET source_row_id = NULL, exact_name = NULL,
              raw = jsonb_set(jsonb_set(jsonb_set(raw,'{matchType}','"article"'),'{sourceRowId}','null'),'{exactName}','null'),
              updated_at = now()
          WHERE id = $1
        `, link.link_id);
        resetCount++;
      } else if (articleChanged) {
        // NativeName OK but article renumbered → update supplier_article only
        await prisma.$executeRawUnsafe(`
          UPDATE product_links
          SET supplier_article = $1, updated_at = now()
          WHERE id = $2
        `, currentNativeId, link.link_id);
        articleUpdatedCount++;
      } else {
        okCount++;
      }
    } else {
      if (diverged) resetCount++;
      else if (articleChanged) articleUpdatedCount++;
      else okCount++;
    }
  }

  console.log(`\n${dryRun ? "[DRY RUN] Would:" : "Done:"}`);
  console.log(`  Reset (name diverged):    ${resetCount} links`);
  console.log(`  Update article only:      ${articleUpdatedCount} links`);
  console.log(`  OK (no change needed):    ${okCount} links`);
  console.log(`  Total checked:            ${links.length} links`);

  await pool.end();
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error("Error:", err.message);
  process.exit(1);
});
