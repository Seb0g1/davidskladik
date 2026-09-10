// One-time fix: reset Dalik selected_row links where pinned PM row name
// diverges from warehouse product name (caused by volume-match bonus bug).
// Safe to run multiple times — idempotent.
"use strict";

const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = "true";

const { getPrisma } = require("../lib/postgres.js");
const prisma = getPrisma();

// Simple tokenizer matching priceMasterNameTokens() in 02a-price-master-match-helpers.js
function tokens(str) {
  const EXCLUDE = new Set(["edp", "edt", "parfum", "perfume", "extrait", "de", "ml"]);
  return [...new Set(
    (str || "").toLowerCase()
      .replace(/[^a-z0-9а-яё\s]/gi, " ")
      .split(/\s+/)
      .filter((t) => t.length >= 2 && !EXCLUDE.has(t))
  )];
}

function overlap(a, b) {
  const setB = new Set(b);
  return a.filter((t) => setB.has(t)).length;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  // Find all selected_row links where supplier is Dalik
  const rows = await prisma.$queryRawUnsafe(`
    SELECT
      pl.id AS link_id,
      pl.supplier_article,
      pl.supplier_name,
      pl.source_row_id,
      pl.exact_name AS pinned_name,
      wp.name AS product_name,
      wp.offer_id
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (
        LOWER(pl.supplier_name) LIKE '%далик%'
        OR LOWER(pl.supplier_name) LIKE '%dalik%'
      )
    ORDER BY wp.offer_id
  `);

  console.log(`Found ${rows.length} Dalik selected_row links`);

  let resetCount = 0;
  for (const row of rows) {
    const prodTokens = tokens(row.product_name);
    const pinTokens = tokens(row.pinned_name);
    const sharedCount = overlap(prodTokens, pinTokens);
    const uniqueToPin = pinTokens.filter((t) => !new Set(prodTokens).has(t));
    const uniqueToProd = prodTokens.filter((t) => !new Set(pinTokens).has(t));
    const hasDivergence = uniqueToPin.length > 0 && uniqueToProd.length > 0;
    const divergencePenalty = hasDivergence ? Math.min(uniqueToPin.length, 4) * 20 : 0;

    const needsReset = divergencePenalty >= 40 || sharedCount < 2;

    console.log(
      `${row.offer_id}: prod="${(row.product_name || "").slice(0, 35)}" ` +
      `pinned="${(row.pinned_name || "").slice(0, 35)}" ` +
      `shared=${sharedCount} penalty=${divergencePenalty} ` +
      (needsReset ? "→ RESET" : "→ OK")
    );

    if (needsReset && !dryRun) {
      await prisma.$executeRawUnsafe(`
        UPDATE product_links
        SET
          source_row_id = NULL,
          exact_name    = NULL,
          raw           = jsonb_set(
                            jsonb_set(
                              jsonb_set(raw, '{matchType}',   '"article"'),
                              '{sourceRowId}', 'null'
                            ),
                            '{exactName}', 'null'
                          ),
          updated_at    = now()
        WHERE id = $1
      `, row.link_id);
      resetCount++;
    } else if (needsReset) {
      resetCount++;
    }
  }

  console.log(`\n${dryRun ? "[DRY RUN] Would reset" : "Reset"} ${resetCount} of ${rows.length} links`);
  await prisma.$disconnect();
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
