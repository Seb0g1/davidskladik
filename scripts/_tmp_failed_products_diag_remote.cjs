#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Get some failed Ozon products and look at their marketplaceState
    console.log("=== marketplaceState for failed Ozon products ===");
    const failedProducts = await prisma.$queryRawUnsafe(`
      SELECT
        wp.id,
        wp.raw->>'offerId' AS offer_id,
        wp.raw->>'target' AS target,
        wp.raw->>'marketplaceState' AS mp_state_json,
        wp.raw->>'archived' AS archived,
        wp.raw->>'status' AS status,
        wp.raw->>'marketplacePrice' AS marketplace_price,
        s.last_error,
        s.price_status,
        s.target_stock,
        s.updated_at AS state_updated_at
      FROM warehouse_products wp
      JOIN sales_automation_sku_states s
        ON s.offer_id = (wp.raw->>'offerId')
        AND s.marketplace = 'ozon'
        AND s.target = 'ozon'
      WHERE s.price_status = 'failed'
        AND s.target_stock > 0
      LIMIT 10
    `);

    for (const p of failedProducts) {
      console.log(`\n  offerId=${p.offer_id} target=${p.target}`);
      console.log(`  archived=${p.archived}  status=${p.status}`);
      console.log(`  marketplace_price=${p.marketplace_price}₽`);
      console.log(`  state_updated: ${p.state_updated_at}`);
      try {
        const state = JSON.parse(p.mp_state_json || "{}");
        const stateKeys = Object.keys(state);
        if (stateKeys.length === 0) {
          console.log(`  marketplaceState: empty`);
        } else {
          console.log(`  marketplaceState keys: ${stateKeys.join(", ")}`);
          const relevant = ["archived", "isArchived", "isAutoArchived", "visibility", "code", "stock", "status", "active"];
          for (const k of relevant) {
            if (state[k] !== undefined) console.log(`    ${k}: ${JSON.stringify(state[k])}`);
          }
        }
      } catch (e) {
        console.log(`  marketplaceState (raw): ${String(p.mp_state_json || "").slice(0, 100)}`);
      }
    }

    // 2. What marketplaceState.archived looks like for SUCCESS products vs FAILED
    console.log("\n=== marketplaceState.archived for Ozon products (target=ozon) ===");
    const stateBreakdown = await prisma.$queryRawUnsafe(`
      SELECT
        s.price_status,
        (wp.raw->'marketplaceState'->>'archived')::text AS archived,
        (wp.raw->'marketplaceState'->>'isArchived')::text AS is_archived,
        (wp.raw->'marketplaceState'->>'visibility')::text AS visibility,
        COUNT(*) AS count
      FROM warehouse_products wp
      JOIN sales_automation_sku_states s
        ON s.offer_id = (wp.raw->>'offerId')
        AND s.marketplace = 'ozon'
        AND s.target = 'ozon'
      GROUP BY s.price_status, archived, is_archived, visibility
      ORDER BY count DESC
      LIMIT 20
    `);
    for (const r of stateBreakdown) {
      console.log(`  [${r.price_status}] archived=${r.archived} is_archived=${r.is_archived} visibility=${r.visibility}: ${r.count}`);
    }

    // 3. Count products in warehouse with archived marketplace state
    console.log("\n=== Products with archived marketplaceState (warehouse_products table) ===");
    const warehouseArchived = await prisma.$queryRawUnsafe(`
      SELECT
        wp.raw->>'target' AS target,
        (wp.raw->'marketplaceState'->>'archived')::text AS archived,
        (wp.raw->'marketplaceState'->>'isArchived')::text AS is_archived,
        (wp.raw->'marketplaceState'->>'visibility')::text AS visibility,
        COUNT(*) AS count
      FROM warehouse_products wp
      WHERE wp.marketplace = 'ozon' AND wp.raw IS NOT NULL
      GROUP BY target, archived, is_archived, visibility
      ORDER BY target, count DESC
      LIMIT 20
    `);
    for (const r of warehouseArchived) {
      console.log(`  [${r.target}] archived=${r.archived} is_archived=${r.is_archived} visibility=${r.visibility}: ${r.count}`);
    }

    // 4. Check notFoundOnMarketplace field in warehouse_products
    console.log("\n=== notFoundOnMarketplace in warehouse_products ===");
    const notFound = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'target' AS target,
        (raw->>'notFoundOnMarketplace')::text AS not_found,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND raw IS NOT NULL
      GROUP BY raw->>'target', (raw->>'notFoundOnMarketplace')::text
      ORDER BY count DESC
    `);
    for (const r of notFound) {
      console.log(`  [${r.target}] notFoundOnMarketplace=${r.not_found}: ${r.count}`);
    }

    // 5. Look at what fields exist in marketplaceState
    console.log("\n=== marketplaceState keys sample ===");
    const stateSample = await prisma.$queryRawUnsafe(`
      SELECT DISTINCT jsonb_object_keys(raw->'marketplaceState') AS key, COUNT(*) OVER () AS total
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND raw->'marketplaceState' IS NOT NULL AND raw->'marketplaceState' != '{}'::jsonb
      LIMIT 20
    `);
    if (stateSample.length === 0) {
      console.log("  marketplaceState is empty for all Ozon products");
    } else {
      console.log("  Keys in marketplaceState: " + stateSample.map((r) => r.key).join(", "));
    }

    // 6. How many ozon products have no marketplaceState
    console.log("\n=== Ozon products with empty/null marketplaceState ===");
    const noState = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'target' AS target,
        COUNT(*) AS no_state
      FROM warehouse_products
      WHERE
        marketplace = 'ozon'
        AND (raw->'marketplaceState' IS NULL OR raw->'marketplaceState' = '{}'::jsonb)
        AND raw IS NOT NULL
      GROUP BY raw->>'target'
      ORDER BY no_state DESC
    `);
    for (const r of noState) {
      console.log(`  [${r.target}] no marketplaceState: ${r.no_state}`);
    }

    // 7. Recent price history to see when last success
    console.log("\n=== Recent Ozon price history (last 5 successes) ===");
    const priceHist = await prisma.$queryRawUnsafe(`
      SELECT
        ph.offer_id,
        ph.target,
        ph.price_rub,
        ph.sent_at
      FROM price_history ph
      WHERE ph.marketplace = 'ozon' AND ph.target = 'ozon'
      ORDER BY ph.sent_at DESC
      LIMIT 5
    `).catch((e) => {
      console.log(`  Error: ${e.message}`);
      return [];
    });
    for (const r of priceHist) {
      console.log(`  ${r.offer_id} [${r.target}] ${r.price_rub}₽ at ${r.sent_at}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
