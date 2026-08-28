#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Price status by marketplace + target
    console.log("=== sales_automation_sku_states: price_status breakdown ===");
    const byStatus = await prisma.$queryRawUnsafe(`
      SELECT
        marketplace,
        target,
        price_status,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock,
        ROUND(AVG(NULLIF(current_price, 0))) AS avg_current_price,
        ROUND(AVG(NULLIF(target_price, 0))) AS avg_target_price
      FROM sales_automation_sku_states
      GROUP BY marketplace, target, price_status
      ORDER BY marketplace, target, count DESC
    `);
    for (const r of byStatus) {
      const stockNote = Number(r.with_stock) > 0 ? ` [stock: ${r.with_stock}]` : "";
      console.log(`  [${r.marketplace}/${r.target || 'default'}] ${r.price_status}: ${r.count}${stockNote}  curr=${r.avg_current_price}₽ tgt=${r.avg_target_price}₽`);
    }

    // 2. Failed on Ozon — error messages
    console.log("\n=== Ozon FAILED products — error messages ===");
    const ozonErrors = await prisma.$queryRawUnsafe(`
      SELECT
        target,
        last_error,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM sales_automation_sku_states
      WHERE marketplace = 'ozon' AND price_status = 'failed'
      GROUP BY target, last_error
      ORDER BY count DESC
      LIMIT 20
    `);
    if (ozonErrors.length === 0) {
      console.log("  No failed Ozon products!");
    } else {
      for (const r of ozonErrors) {
        const err = String(r.last_error || "no error text").slice(0, 150);
        console.log(`  [${r.target}] ${r.count}x (stock=${r.with_stock}): ${err}`);
      }
    }

    // 3. Failed on Yandex
    console.log("\n=== Yandex FAILED products — error messages ===");
    const ymErrors = await prisma.$queryRawUnsafe(`
      SELECT
        target,
        last_error,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM sales_automation_sku_states
      WHERE marketplace = 'yandex' AND price_status = 'failed'
      GROUP BY target, last_error
      ORDER BY count DESC
      LIMIT 20
    `);
    if (ymErrors.length === 0) {
      console.log("  No failed Yandex products!");
    } else {
      for (const r of ymErrors) {
        const err = String(r.last_error || "no error text").slice(0, 150);
        console.log(`  [${r.target}] ${r.count}x (stock=${r.with_stock}): ${err}`);
      }
    }

    // 4. Products with stock > 0 but status is not success
    console.log("\n=== Products WITH STOCK but NOT priced (not success) — by marketplace ===");
    const stockedBad = await prisma.$queryRawUnsafe(`
      SELECT
        marketplace,
        target,
        price_status,
        reason,
        COUNT(*) AS count,
        SUM(target_stock) AS total_stock
      FROM sales_automation_sku_states
      WHERE target_stock > 0 AND price_status != 'success'
      GROUP BY marketplace, target, price_status, reason
      ORDER BY count DESC
      LIMIT 30
    `);
    if (stockedBad.length === 0) {
      console.log("  All products with stock have success price status ✓");
    } else {
      for (const r of stockedBad) {
        console.log(`  [${r.marketplace}/${r.target}] status=${r.price_status} reason=${r.reason}: ${r.count} products, stock=${r.total_stock}`);
      }
    }

    // 5. Reason breakdown for all products
    console.log("\n=== Full reason breakdown ===");
    const reasons = await prisma.$queryRawUnsafe(`
      SELECT
        marketplace,
        target,
        reason,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock
      FROM sales_automation_sku_states
      GROUP BY marketplace, target, reason
      ORDER BY marketplace, target, count DESC
    `);
    let lastMkt = "";
    for (const r of reasons) {
      const key = `${r.marketplace}/${r.target}`;
      if (key !== lastMkt) {
        console.log(`\n  [${key}]`);
        lastMkt = key;
      }
      const stockNote = Number(r.with_stock) > 0 ? ` (stock=${r.with_stock})` : "";
      console.log(`    ${r.reason}: ${r.count}${stockNote}`);
    }

    // 6. Products with stock, reason=send_failed - sample
    console.log("\n=== Sample failed Ozon products WITH STOCK ===");
    const sampleFailed = await prisma.$queryRawUnsafe(`
      SELECT
        s.offer_id,
        s.target,
        s.current_price,
        s.target_price,
        s.target_stock,
        s.last_error,
        s.updated_at
      FROM sales_automation_sku_states s
      WHERE s.marketplace = 'ozon' AND s.price_status = 'failed' AND s.target_stock > 0
      ORDER BY s.updated_at DESC
      LIMIT 10
    `);
    if (sampleFailed.length === 0) {
      console.log("  No failed Ozon products with stock");
    } else {
      for (const r of sampleFailed) {
        console.log(`  ${r.offer_id} [${r.target}] curr=${r.current_price}₽ tgt=${r.target_price}₽ stock=${r.target_stock}`);
        console.log(`    error: ${String(r.last_error || "").slice(0, 150)}`);
        console.log(`    updated: ${r.updated_at}`);
      }
    }

    // 7. Ozon account 2 breakdown
    console.log("\n=== Ozon account 2 (ozon-3d10ec43) — full breakdown ===");
    const ozon2 = await prisma.$queryRawUnsafe(`
      SELECT
        price_status,
        reason,
        COUNT(*) AS count,
        SUM(CASE WHEN target_stock > 0 THEN 1 ELSE 0 END) AS with_stock,
        ROUND(AVG(NULLIF(current_price, 0))) AS avg_price
      FROM sales_automation_sku_states
      WHERE marketplace = 'ozon' AND target = 'ozon-3d10ec43'
      GROUP BY price_status, reason
      ORDER BY count DESC
    `);
    for (const r of ozon2) {
      const stockNote = Number(r.with_stock) > 0 ? ` (stock=${r.with_stock})` : "";
      console.log(`  ${r.price_status} / ${r.reason}: ${r.count}${stockNote} avg=${r.avg_price}₽`);
    }

    // 8. Products in retry queue
    console.log("\n=== PriceRetryQueueItem count by marketplace ===");
    const retryQueue = await prisma.$queryRawUnsafe(`
      SELECT marketplace, COUNT(*) AS count
      FROM "PriceRetryQueueItem"
      GROUP BY marketplace
      ORDER BY count DESC
    `).catch(() => prisma.$queryRawUnsafe(`
      SELECT marketplace, COUNT(*) AS count
      FROM price_retry_queue_items
      GROUP BY marketplace
      ORDER BY count DESC
    `));
    for (const r of retryQueue) {
      console.log(`  ${r.marketplace}: ${r.count}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
