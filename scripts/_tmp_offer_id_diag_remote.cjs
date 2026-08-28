#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Sample failed products - look at raw.ozon to understand their Ozon structure
    console.log("=== raw.ozon fields for never-priced FAILED ozon/ozon products ===");
    const failedSample = await prisma.$queryRawUnsafe(`
      SELECT
        wp.id,
        wp.raw->>'offerId' AS offer_id,
        wp.raw->>'target' AS target,
        wp.raw->'ozon' AS ozon_data,
        wp.raw->'marketplaceState' AS mp_state,
        wp.raw->>'productId' AS product_id,
        wp.raw->>'source' AS source,
        wp.raw->>'createdAt' AS created_at,
        wp.raw->>'updatedAt' AS updated_at
      FROM warehouse_products wp
      JOIN sales_automation_sku_states s
        ON s.marketplace = wp.marketplace
        AND s.offer_id = (wp.raw->>'offerId')
        AND s.target = (wp.raw->>'target')
      WHERE wp.marketplace = 'ozon'
        AND (wp.raw->>'target') = 'ozon'
        AND s.price_status = 'failed'
        AND s.last_price_sent_at IS NULL
      LIMIT 5
    `);
    for (const r of failedSample) {
      console.log(`\n  offerId=${r.offer_id} productId=${r.product_id} source=${r.source}`);
      console.log(`  created: ${r.created_at}  updated: ${r.updated_at}`);
      const ozon = r.ozon_data || {};
      console.log(`  ozon keys: ${Object.keys(ozon).join(", ")}`);
      const mpState = r.mp_state || {};
      const relevant = ["code","archived","isAutoArchived","stock","visibility","present"];
      for (const k of relevant) {
        if (mpState[k] !== undefined) console.log(`  mktState.${k}: ${JSON.stringify(mpState[k])}`);
      }
      // ozon sub-fields
      const ozonRelevant = ["account","clientId","target","productId","skuFbo","skuFbs","fboStock","fbsStock","status","archivalStatus","visibility"];
      for (const k of ozonRelevant) {
        if (ozon[k] !== undefined) console.log(`  ozon.${k}: ${JSON.stringify(ozon[k])}`);
      }
    }

    // 2. Sample SUCCESS products for comparison
    console.log("\n=== raw.ozon fields for SUCCESS ozon/ozon products (comparison) ===");
    const successSample = await prisma.$queryRawUnsafe(`
      SELECT
        wp.raw->>'offerId' AS offer_id,
        wp.raw->>'productId' AS product_id,
        wp.raw->>'source' AS source,
        wp.raw->'ozon' AS ozon_data,
        wp.raw->'marketplaceState' AS mp_state,
        wp.raw->>'createdAt' AS created_at
      FROM warehouse_products wp
      JOIN sales_automation_sku_states s
        ON s.marketplace = wp.marketplace
        AND s.offer_id = (wp.raw->>'offerId')
        AND s.target = (wp.raw->>'target')
      WHERE wp.marketplace = 'ozon'
        AND (wp.raw->>'target') = 'ozon'
        AND s.price_status = 'success'
      LIMIT 5
    `);
    for (const r of successSample) {
      console.log(`\n  offerId=${r.offer_id} productId=${r.product_id} source=${r.source} created=${r.created_at}`);
      const ozon = r.ozon_data || {};
      const ozonRelevant = ["account","clientId","target","productId","skuFbo","skuFbs","fboStock","fbsStock","status"];
      for (const k of ozonRelevant) {
        if (ozon[k] !== undefined) console.log(`  ozon.${k}: ${JSON.stringify(ozon[k])}`);
      }
    }

    // 3. Check if the failed products have productId set (needed for Ozon API calls)
    console.log("\n=== productId presence for failed vs success (ozon/ozon) ===");
    const productIdCheck = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.price_status,
          CASE WHEN (wp.raw->>'productId') IS NOT NULL AND (wp.raw->>'productId') != '' AND (wp.raw->>'productId') != '0' THEN 'has_product_id' ELSE 'no_product_id' END AS has_product_id,
          CASE WHEN (wp.raw->>'ozon'->'productId') IS NOT NULL THEN 'has_ozon_product_id' ELSE 'no_ozon_product_id' END AS has_ozon_pid
        FROM sales_automation_sku_states s
        JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon'
      )
      SELECT price_status, has_product_id, COUNT(*) AS count
      FROM joined
      GROUP BY price_status, has_product_id
      ORDER BY price_status, count DESC
    `);
    for (const r of productIdCheck) {
      console.log(`  [${r.price_status}] ${r.has_product_id}: ${r.count}`);
    }

    // 4. Check ozon account clientId distribution for failed vs success
    console.log("\n=== Ozon clientId/account breakdown for failed (target=ozon) ===");
    const clientIdCheck = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.price_status,
          COALESCE(wp.raw->'ozon'->>'account', 'null') AS ozon_account,
          COALESCE(wp.raw->'ozon'->>'clientId', 'null') AS ozon_client_id
        FROM sales_automation_sku_states s
        JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon'
      )
      SELECT price_status, ozon_account, ozon_client_id, COUNT(*) AS count
      FROM joined
      GROUP BY price_status, ozon_account, ozon_client_id
      ORDER BY count DESC
    `);
    for (const r of clientIdCheck) {
      console.log(`  [${r.price_status}] account=${r.ozon_account} clientId=${r.ozon_client_id}: ${r.count}`);
    }

    // 5. Source breakdown for failed products (where did these products come from?)
    console.log("\n=== Source/import origin for failed never-priced products ===");
    const sourceCheck = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.target,
          COALESCE(wp.raw->>'source', 'null') AS source,
          s.target_stock
        FROM sales_automation_sku_states s
        JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.price_status = 'failed' AND s.last_price_sent_at IS NULL
      )
      SELECT target, source, COUNT(*) AS count, SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM joined
      GROUP BY target, source
      ORDER BY count DESC
    `);
    for (const r of sourceCheck) {
      console.log(`  [${r.target}] source=${r.source}: ${r.count} (stock: ${r.with_stock})`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
