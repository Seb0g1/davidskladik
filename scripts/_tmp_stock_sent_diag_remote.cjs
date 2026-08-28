#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Check stock status for failed vs success products
    console.log("=== stock_status for failed vs success (target=ozon) ===");
    const stockStatus = await prisma.$queryRawUnsafe(`
      SELECT
        price_status,
        stock_status,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock,
        SUM(CASE WHEN last_stock_sent_at IS NOT NULL THEN 1 ELSE 0 END) AS stock_ever_sent
      FROM sales_automation_sku_states
      WHERE marketplace = 'ozon' AND target = 'ozon'
      GROUP BY price_status, stock_status
      ORDER BY price_status, count DESC
    `);
    for (const r of stockStatus) {
      console.log(`  [${r.price_status}] stock_status=${r.stock_status}: ${r.count} total, with_stock=${r.with_stock}, stock_ever_sent=${r.stock_ever_sent}`);
    }

    // 2. Check lastStockSend field in warehouse_products for failed products
    console.log("\n=== lastStockSend for sample FAILED ozon/ozon products ===");
    const stockSend = await prisma.$queryRawUnsafe(`
      SELECT
        wp.raw->>'offerId' AS offer_id,
        wp.raw->'lastStockSend' AS last_stock_send,
        s.stock_status,
        s.last_stock_sent_at,
        s.target_stock
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
    for (const r of stockSend) {
      console.log(`\n  offerId=${r.offer_id} stock_status=${r.stock_status} target_stock=${r.target_stock}`);
      console.log(`  last_stock_sent_at: ${r.last_stock_sent_at}`);
      const ls = r.last_stock_send;
      if (ls) {
        console.log(`  lastStockSend: ok=${ls.ok} error=${ls.error} sentAt=${ls.sentAt}`);
      } else {
        console.log(`  lastStockSend: null`);
      }
    }

    // 3. Check the warehouse lastArchiveSend for failed products
    console.log("\n=== lastArchiveSend for FAILED products (unarchive history) ===");
    const archiveSend = await prisma.$queryRawUnsafe(`
      SELECT
        wp.raw->>'offerId' AS offer_id,
        wp.raw->'lastArchiveSend' AS last_archive_send,
        (wp.raw->>'notFoundOnMarketplace')::text AS not_found
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
    for (const r of archiveSend) {
      console.log(`\n  offerId=${r.offer_id} notFoundOnMarketplace=${r.not_found}`);
      const la = r.last_archive_send;
      if (la) {
        console.log(`  lastArchiveSend: ${JSON.stringify(la).slice(0, 200)}`);
      } else {
        console.log(`  lastArchiveSend: null`);
      }
    }

    // 4. How many of these products have successful stock send (confirming they're in seller catalog)
    console.log("\n=== Stock send history: failed products that had stock sent ===");
    const stockSentBreakdown = await prisma.$queryRawUnsafe(`
      SELECT
        s.target,
        SUM(CASE WHEN s.last_stock_sent_at IS NOT NULL THEN 1 ELSE 0 END) AS stock_was_sent,
        SUM(CASE WHEN s.last_stock_sent_at IS NULL THEN 1 ELSE 0 END) AS stock_never_sent,
        COUNT(*) AS total
      FROM sales_automation_sku_states s
      WHERE s.marketplace = 'ozon' AND s.price_status = 'failed'
      GROUP BY s.target
    `);
    for (const r of stockSentBreakdown) {
      console.log(`  [${r.target}] stock_was_sent=${r.stock_was_sent} stock_never_sent=${r.stock_never_sent} total=${r.total}`);
    }

    // 5. What are the raw.ozon keys for SUCCESS products?
    console.log("\n=== raw.ozon structure for SUCCESS products (ozon/ozon) ===");
    const successOzon = await prisma.$queryRawUnsafe(`
      SELECT
        wp.raw->>'offerId' AS offer_id,
        CASE WHEN wp.raw->'ozon' IS NULL THEN 'null' WHEN wp.raw->'ozon' = '{}'::jsonb THEN 'empty' ELSE 'has_data' END AS ozon_status,
        CASE WHEN wp.raw->'ozon' IS NOT NULL AND wp.raw->'ozon' != '{}'::jsonb THEN jsonb_object_keys(wp.raw->'ozon') ELSE NULL END AS ozon_key
      FROM warehouse_products wp
      JOIN sales_automation_sku_states s
        ON s.marketplace = wp.marketplace
        AND s.offer_id = (wp.raw->>'offerId')
        AND s.target = (wp.raw->>'target')
      WHERE wp.marketplace = 'ozon'
        AND (wp.raw->>'target') = 'ozon'
        AND s.price_status = 'success'
      LIMIT 10
    `).catch((e) => {
      // fallback simpler query
      return prisma.$queryRawUnsafe(`
        SELECT
          wp.raw->>'offerId' AS offer_id,
          CASE WHEN wp.raw->'ozon' IS NULL THEN 'null' WHEN wp.raw->'ozon' = '{}'::jsonb THEN 'empty' ELSE 'has_data' END AS ozon_status
        FROM warehouse_products wp
        JOIN sales_automation_sku_states s
          ON s.marketplace = wp.marketplace
          AND s.offer_id = (wp.raw->>'offerId')
          AND s.target = (wp.raw->>'target')
        WHERE wp.marketplace = 'ozon'
          AND (wp.raw->>'target') = 'ozon'
          AND s.price_status = 'success'
        LIMIT 10
      `);
    });
    const ozonStatusCounts = {};
    for (const r of successOzon) {
      ozonStatusCounts[r.ozon_status] = (ozonStatusCounts[r.ozon_status] || 0) + 1;
    }
    console.log(`  ozon_status distribution: ${JSON.stringify(ozonStatusCounts)}`);

    // 6. Count by ozon_status for all failed vs success
    console.log("\n=== raw.ozon status: failed vs success (target=ozon) ===");
    const ozonStatusAll = await prisma.$queryRawUnsafe(`
      WITH joined AS (
        SELECT
          s.price_status,
          CASE WHEN wp.raw->'ozon' IS NULL OR wp.raw->'ozon' = '{}'::jsonb THEN 'null_or_empty' ELSE 'has_data' END AS ozon_status
        FROM sales_automation_sku_states s
        JOIN warehouse_products wp
          ON wp.marketplace = s.marketplace
          AND (wp.raw->>'offerId') = s.offer_id
          AND (wp.raw->>'target') = s.target
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon'
      )
      SELECT price_status, ozon_status, COUNT(*) AS count
      FROM joined
      GROUP BY price_status, ozon_status
      ORDER BY price_status, count DESC
    `);
    for (const r of ozonStatusAll) {
      console.log(`  [${r.price_status}] raw.ozon=${r.ozon_status}: ${r.count}`);
    }

    // 7. Check PM2 logs or the automation config
    console.log("\n=== How many of these have PM links? ===");
    const pmLinks = await prisma.$queryRawUnsafe(`
      WITH failed_pids AS (
        SELECT DISTINCT wp.id
        FROM warehouse_products wp
        JOIN sales_automation_sku_states s
          ON s.marketplace = wp.marketplace
          AND s.offer_id = (wp.raw->>'offerId')
          AND s.target = (wp.raw->>'target')
        WHERE s.marketplace = 'ozon' AND s.target = 'ozon' AND s.price_status = 'failed'
      )
      SELECT
        COUNT(DISTINCT fp.id) AS total_failed,
        COUNT(DISTINCT pl.product_id) AS with_pm_link,
        COUNT(DISTINCT fp.id) - COUNT(DISTINCT pl.product_id) AS without_pm_link
      FROM failed_pids fp
      LEFT JOIN product_links pl ON pl.product_id = fp.id
    `);
    for (const r of pmLinks) {
      console.log(`  total_failed_pids=${r.total_failed} with_pm_link=${r.with_pm_link} without_pm_link=${r.without_pm_link}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
