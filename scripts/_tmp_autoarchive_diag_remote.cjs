#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Check isAutoArchived for failed vs success products (ozon/ozon target)
    console.log("=== isAutoArchived breakdown: failed vs success (target=ozon) ===");
    const autoArchive = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.price_status,
          COALESCE(wp.raw->'marketplaceState'->>'isAutoArchived', 'null') AS is_auto_archived,
          COALESCE(wp.raw->'marketplaceState'->>'archived', 'null') AS archived,
          COALESCE(wp.raw->'marketplaceState'->>'code', 'null') AS code,
          s.target_stock
        FROM sales_automation_sku_states s
        LEFT JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon'
      )
      SELECT
        price_status,
        is_auto_archived,
        archived,
        code,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM joined
      GROUP BY price_status, is_auto_archived, archived, code
      ORDER BY price_status, count DESC
    `);

    for (const r of autoArchive) {
      console.log(`  [${r.price_status}] isAutoArchived=${r.is_auto_archived} archived=${r.archived} code=${r.code}: ${r.count} (stock: ${r.with_stock})`);
    }

    // 2. Same for ozon-3d10ec43
    console.log("\n=== isAutoArchived breakdown: account 2 (ozon-3d10ec43) ===");
    const autoArchive2 = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.price_status,
          COALESCE(wp.raw->'marketplaceState'->>'isAutoArchived', 'null') AS is_auto_archived,
          COALESCE(wp.raw->'marketplaceState'->>'archived', 'null') AS archived,
          COALESCE(wp.raw->'marketplaceState'->>'code', 'null') AS code,
          s.target_stock
        FROM sales_automation_sku_states s
        LEFT JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon-3d10ec43'
      )
      SELECT
        price_status,
        is_auto_archived,
        archived,
        code,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM joined
      GROUP BY price_status, is_auto_archived, archived, code
      ORDER BY price_status, count DESC
    `);

    for (const r of autoArchive2) {
      console.log(`  [${r.price_status}] isAutoArchived=${r.is_auto_archived} archived=${r.archived} code=${r.code}: ${r.count} (stock: ${r.with_stock})`);
    }

    // 3. When was the last time these products were SUCCESSFULLY priced?
    console.log("\n=== Last successful price send for failed products ===");
    const lastSuccess = await prisma.$queryRawUnsafe(`
      SELECT
        s.target,
        ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - s.last_price_sent_at))/3600)) AS avg_hours_since_last_price,
        MIN(s.last_price_sent_at) AS oldest_success,
        MAX(s.last_price_sent_at) AS newest_success,
        SUM(CASE WHEN s.last_price_sent_at IS NULL THEN 1 ELSE 0 END) AS never_priced,
        COUNT(*) AS total
      FROM sales_automation_sku_states s
      WHERE s.marketplace = 'ozon' AND s.price_status = 'failed'
      GROUP BY s.target
    `);
    for (const r of lastSuccess) {
      console.log(`  [${r.target}] avg_hours_since_last=${r.avg_hours_since_last_price}h  oldest=${r.oldest_success}  newest=${r.newest_success}  never_priced=${r.never_priced}/${r.total}`);
    }

    // 4. Count products by their actual Ozon-code for failed products
    console.log("\n=== Ozon code distribution for failed products (ALL targets) ===");
    const codeDist = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.target,
          COALESCE(wp.raw->'marketplaceState'->>'code', 'no_state') AS code,
          COALESCE(wp.raw->'marketplaceState'->>'isAutoArchived', 'null') AS is_auto_archived
        FROM sales_automation_sku_states s
        LEFT JOIN warehouse_products wp
          ON s.offer_id = (wp.raw->>'offerId')
          AND s.marketplace = wp.marketplace
          AND s.target = (wp.raw->>'target')
        WHERE s.marketplace = 'ozon' AND s.price_status = 'failed'
      )
      SELECT target, code, is_auto_archived, COUNT(*) AS count
      FROM joined
      GROUP BY target, code, is_auto_archived
      ORDER BY target, count DESC
    `);
    for (const r of codeDist) {
      console.log(`  [${r.target}] code=${r.code} isAutoArchived=${r.is_auto_archived}: ${r.count}`);
    }

    // 5. Products in success that are isAutoArchived (sanity check)
    console.log("\n=== isAutoArchived=true for SUCCESS products (control group) ===");
    const successAutoArch = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.target,
          COALESCE(wp.raw->'marketplaceState'->>'isAutoArchived', 'null') AS is_auto_archived
        FROM sales_automation_sku_states s
        LEFT JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.price_status = 'success'
      )
      SELECT target, is_auto_archived, COUNT(*) AS count
      FROM joined
      GROUP BY target, is_auto_archived
      ORDER BY target, count DESC
    `);
    for (const r of successAutoArch) {
      console.log(`  [${r.target}] isAutoArchived=${r.is_auto_archived}: ${r.count} success products`);
    }

    // 6. What percentage of failed products have no warehouse_products record (offer_id mismatch)?
    console.log("\n=== Failed products with no matching warehouse_products record ===");
    const noMatch = await prisma.$queryRawUnsafe(`
      SELECT
        s.target,
        COUNT(*) AS failed_no_match
      FROM sales_automation_sku_states s
      LEFT JOIN warehouse_products wp
        ON s.offer_id = (wp.raw->>'offerId')
        AND s.marketplace = wp.marketplace
        AND s.target = (wp.raw->>'target')
      WHERE s.marketplace = 'ozon' AND s.price_status = 'failed' AND wp.id IS NULL
      GROUP BY s.target
    `);
    for (const r of noMatch) {
      console.log(`  [${r.target}] failed records without matching warehouse product: ${r.failed_no_match}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
