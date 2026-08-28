#!/usr/bin/env node
"use strict";

// Broadfix: clears bad manualMarkup on all Yandex products linked to Инна suppliers.
// restore-yandex-markups treated Инна's RUB prices as USD (rubEquiv = price * rate),
// producing a wildly wrong markup that got stored with manualMarkup:true and overrode
// normal pricing rules. Now that the code fix (Инна always RUB) is deployed, we just
// need to clear the stored markup so normal rules apply on the next reprice.

process.env.DISABLE_BACKGROUND_JOBS = "true";

require("dotenv").config();
const { getPrisma } = require("../lib/postgres.js");

const DRY_RUN = process.argv.includes("--dry-run") || !process.argv.includes("--confirm");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // Find all Yandex products that:
  //   1. have manualMarkup:true stored in raw JSON
  //   2. have at least one link to an Инна supplier (contains "инна", not "иванна"/"ivanna")
  const affectedRows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT wp.id, wp.raw->'yandex'->'extra'->>'manualMarkup' AS manual_markup,
           (wp.raw->>'markup')::numeric AS stored_markup,
           wp.offer_id
    FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id
    WHERE wp.marketplace = 'yandex'
      AND (wp.raw -> 'yandex' -> 'extra' ->> 'manualMarkup')::boolean = true
      AND lower(pl.supplier_name) LIKE '%инна%'
      AND lower(pl.supplier_name) NOT LIKE '%иванна%'
      AND lower(pl.supplier_name) NOT LIKE '%ivanna%'
    ORDER BY wp.offer_id
  `);

  console.log(`Found ${affectedRows.length} Yandex products with bad Инна manualMarkup`);
  if (affectedRows.length) {
    console.log("Sample (first 10):", JSON.stringify(
      affectedRows.slice(0, 10).map((r) => ({ id: r.id, offerId: r.offer_id, storedMarkup: r.stored_markup })),
      null, 2
    ));
  }

  if (DRY_RUN) {
    console.log("\nDRY RUN — pass --confirm to apply. No changes made.");
    return;
  }

  if (!affectedRows.length) {
    console.log("Nothing to fix.");
    return;
  }

  const ids = affectedRows.map((r) => String(r.id));

  // Bulk update: set markup=0 and manualMarkup=false for all affected products.
  // Using jsonb merge so other raw fields are preserved.
  const updated = await prisma.$executeRawUnsafe(`
    UPDATE warehouse_products
    SET raw = raw
          || jsonb_build_object('markup', 0)
          || jsonb_build_object('yandex',
               COALESCE(raw->'yandex', '{}'::jsonb)
               || jsonb_build_object('extra',
                    COALESCE(raw->'yandex'->'extra', '{}'::jsonb)
                    || '{"manualMarkup": false}'::jsonb
                  )
             ),
        updated_at = NOW()
    WHERE id = ANY($1)
  `, ids);

  console.log(`\nUpdated ${updated} products. markup → 0, manualMarkup → false.`);
  console.log("Reprice Yandex to recalculate prices using normal rules.");
}

main().catch((e) => { console.error(e.message); process.exitCode = 1; });
