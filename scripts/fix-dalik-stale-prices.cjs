// Fix stale target_prices for Dalik products whose linked PM row changed.
// Strategy: for each product where the DB target_price doesn't match the price
// from the currently-pinned PM row, copy the correct price from the row.
"use strict";

const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = "true";
process.env.DB_MODE = "postgres";

const { getPrisma } = require("../lib/postgres.js");
const prisma = getPrisma();

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  // Find warehouse products that:
  // 1. Have a Dalik selected_row link
  // 2. target_price doesn't match the correct price (i.e. was pinned to wrong row)
  // We detect by comparing target_price across variants of same offer_id.
  // Products within same offer_id should converge to the same price.
  const staleRows = await prisma.$queryRawUnsafe(`
    WITH offer_prices AS (
      SELECT offer_id, MIN(target_price) as min_price, MAX(target_price) as max_price,
             COUNT(DISTINCT target_price) as price_variants
      FROM warehouse_products
      WHERE offer_id IN (
        SELECT DISTINCT wp.offer_id
        FROM warehouse_products wp
        JOIN product_links pl ON pl.product_id = wp.id
        WHERE pl.raw->>'matchType' = 'selected_row'
          AND (LOWER(pl.supplier_name) LIKE '%далик%' OR LOWER(pl.supplier_name) LIKE '%dalik%')
      )
      GROUP BY offer_id
      HAVING COUNT(DISTINCT target_price) > 1  -- only where prices differ across variants
    )
    SELECT wp.id, wp.offer_id, wp.name, wp.target_price,
           op.min_price, op.max_price
    FROM warehouse_products wp
    JOIN offer_prices op ON op.offer_id = wp.offer_id
    WHERE wp.target_price = op.max_price  -- the higher (wrong) price
    ORDER BY wp.offer_id
  `);

  console.log(`Found ${staleRows.length} products with stale prices (Dalik supplier group)`);
  for (const r of staleRows) {
    const correctPrice = r.min_price;
    console.log(
      `  ${r.offer_id}: current=${r.target_price} → correct=${correctPrice} ` +
      `("${(r.name || "").slice(0, 40)}")`
    );
    if (!dryRun && correctPrice && correctPrice !== r.target_price) {
      await prisma.$executeRawUnsafe(
        `UPDATE warehouse_products SET target_price = $1, updated_at = now() WHERE id = $2`,
        Number(correctPrice),
        r.id
      );
    }
  }

  if (!staleRows.length) {
    console.log("All prices look consistent — nothing to fix.");
  } else if (!dryRun) {
    console.log(`\nFixed ${staleRows.length} products. Prices updated to the lower (correct) value.`);
    console.log("Note: prices will sync to marketplaces on the next manual price push.");
  } else {
    console.log("[DRY RUN] No changes made.");
  }

  await prisma.$disconnect();
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
