#!/usr/bin/env node
"use strict";
// Comprehensive critical audit: prices, stock, data quality, BullMQ, PM sync.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");
const { Queue } = require("bullmq");
const Redis = require("ioredis");

async function main() {
  const prisma = getPrisma();
  const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: null, lazyConnect: true });
  await redis.connect();

  // ── 1. BullMQ health ────────────────────────────────────────────────────────
  console.log("=== 1. BullMQ ===");
  const q = new Queue("marketplace-tasks", { connection: redis });
  const [w, a, f, d] = await Promise.all([q.getWaitingCount(), q.getActiveCount(), q.getFailedCount(), q.getDelayedCount()]);
  console.log(`  marketplace-tasks: waiting=${w} active=${a} failed=${f} delayed=${d}`);
  if (f > 0) {
    const failedJobs = await q.getFailed(0, Math.min(f - 1, 4));
    for (const j of failedJobs) console.log(`    ⚠ ${j.name}: ${(j.failedReason || "?").slice(0, 100)}`);
  }
  await q.close();
  await redis.quit();

  // ── 2. PM snapshot freshness ─────────────────────────────────────────────────
  console.log("\n=== 2. PM snapshot freshness ===");
  const pmAge = await prisma.$queryRawUnsafe(`
    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) FILTER (WHERE active = true) AS active_rows,
      MAX(updated_at) AS last_updated,
      EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/3600 AS age_hours
    FROM pm_snapshot_items
  `);
  const pm = pmAge[0];
  const ageH = parseFloat(pm.age_hours || 0).toFixed(1);
  const stale = parseFloat(ageH) > 6;
  console.log(`  total=${pm.total_rows} active=${pm.active_rows} last_updated=${new Date(pm.last_updated).toISOString()} age=${ageH}h ${stale ? "⚠ STALE" : "✓"}`);

  // ── 3. Price anomalies ────────────────────────────────────────────────────────
  console.log("\n=== 3. Price anomalies ===");

  // 3a. Products with price=0 or null but active links
  const zeroPrices = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id::text
    WHERE (wp.current_price IS NULL OR wp.current_price = 0)
      AND wp.target_stock > 0
  `);
  console.log(`  Products with price=0/null but target_stock>0: ${zeroPrices[0].cnt}`);

  // 3b. Products with autoPriceMin/Max clamping active
  const clamped = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products
    WHERE (raw->>'autoPriceMin' IS NOT NULL AND (raw->>'autoPriceMin')::float > 0)
       OR (raw->>'autoPriceMax' IS NOT NULL AND (raw->>'autoPriceMax')::float > 0)
  `);
  console.log(`  Products with autoPriceMin/Max clamp set: ${clamped[0].cnt}`);

  // 3c. Very suspicious prices: > 500000 RUB or < 100 RUB with stock > 0
  const suspiciousPrices = await prisma.$queryRawUnsafe(`
    SELECT offer_id, marketplace, current_price, target_stock
    FROM warehouse_products
    WHERE target_stock > 0
      AND (current_price > 500000 OR (current_price < 100 AND current_price > 0))
    ORDER BY current_price DESC
    LIMIT 15
  `);
  if (suspiciousPrices.length) {
    console.log(`  Suspicious prices (${suspiciousPrices.length}):`);
    for (const r of suspiciousPrices) {
      console.log(`    art=${r.offer_id} [${r.marketplace}] price=${r.current_price} stock=${r.target_stock}`);
    }
  } else {
    console.log(`  Suspicious prices: none ✓`);
  }

  // ── 4. Stock issues ───────────────────────────────────────────────────────────
  console.log("\n=== 4. Stock issues ===");

  // 4a. Products showing stock>0 on marketplace but 0 targetStock in DB (dangerous: oversell)
  const oversellRisk = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products
    WHERE target_stock = 0
      AND (raw->'marketplaceState'->>'stock')::int > 0
  `);
  console.log(`  ⚠ Products with targetStock=0 but marketplace shows stock>0 (oversell risk): ${oversellRisk[0].cnt}`);

  // 4b. Total products with links but 0 target stock
  const zeroStockLinked = await prisma.$queryRawUnsafe(`
    SELECT COUNT(DISTINCT wp.id) AS cnt FROM warehouse_products wp
    JOIN product_links pl ON pl.product_id = wp.id::text
    WHERE wp.target_stock = 0
  `);
  console.log(`  Products with links but targetStock=0: ${zeroStockLinked[0].cnt}`);

  // 4c. Products with target_stock > 0 (healthy)
  const withStock = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products WHERE target_stock > 0
  `);
  console.log(`  Products with targetStock>0 (healthy): ${withStock[0].cnt}`);

  // ── 5. Retry queue depth ──────────────────────────────────────────────────────
  console.log("\n=== 5. Price retry queue ===");
  const retryQ = await prisma.$queryRawUnsafe(`
    SELECT
      marketplace,
      COUNT(*) AS total,
      COUNT(*) FILTER (WHERE attempts >= 3) AS exhausted,
      MIN(next_retry_at) AS oldest_next_retry
    FROM price_retry_queue_items
    GROUP BY marketplace
    ORDER BY marketplace
  `).catch(() => null);
  if (retryQ === null) {
    console.log(`  (table not found — likely removed)`);
  } else if (retryQ.length) {
    for (const r of retryQ) {
      console.log(`  ${r.marketplace}: total=${r.total} exhausted=${r.exhausted}`);
    }
  } else {
    console.log(`  Empty ✓`);
  }

  // ── 6. Supplier link quality ──────────────────────────────────────────────────
  console.log("\n=== 6. Supplier link data quality ===");

  // 6a. Still-stale selected_row links (after our fixes)
  const staleRemaining = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM product_links
    WHERE raw->>'matchType' = 'selected_row'
      AND (raw->>'sourceRowId') IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM pm_snapshot_items pm WHERE pm.row_id = (pl.raw->>'sourceRowId'))
    FROM product_links pl
  `).catch(() => prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM product_links pl
    WHERE pl.raw->>'matchType' = 'selected_row'
      AND (pl.raw->>'sourceRowId') IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM pm_snapshot_items pm WHERE pm.row_id = (pl.raw->>'sourceRowId'))
  `));
  console.log(`  Stale selected_row links (sourceRowId missing from PM): ${staleRemaining[0].cnt}`);

  // 6b. Products with NO links at all
  const noLinks = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products wp
    WHERE NOT EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id::text)
      AND wp.target_stock = 0
  `);
  console.log(`  Products with no links and no stock: ${noLinks[0].cnt}`);

  // 6c. Duplicate links (same product + same partner)
  const dupLinks = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM (
      SELECT product_id, partner_id, COUNT(*) AS n
      FROM product_links
      WHERE partner_id IS NOT NULL
      GROUP BY product_id, partner_id
      HAVING COUNT(*) > 2
    ) dupes
  `);
  console.log(`  Product+partner combos with >2 links (potential dupes): ${dupLinks[0].cnt}`);

  // ── 7. Marketplace sync lag ───────────────────────────────────────────────────
  console.log("\n=== 7. Marketplace price send lag ===");
  const sendLag = await prisma.$queryRawUnsafe(`
    SELECT
      marketplace,
      COUNT(*) AS products,
      COUNT(*) FILTER (WHERE raw->>'lastYandexPriceSend' IS NOT NULL OR raw->>'lastOzonPriceSend' IS NOT NULL) AS ever_sent,
      COUNT(*) FILTER (WHERE
        CASE WHEN marketplace='yandex' THEN (raw->'lastYandexPriceSend'->>'sentAt')::timestamptz
             ELSE (raw->'lastOzonPriceSend'->>'sentAt')::timestamptz END
        < NOW() - INTERVAL '7 days'
      ) AS not_sent_7d,
      COUNT(*) FILTER (WHERE
        CASE WHEN marketplace='yandex' THEN (raw->'lastYandexPriceSend'->>'sentAt')::timestamptz
             ELSE (raw->'lastOzonPriceSend'->>'sentAt')::timestamptz END IS NULL
      ) AS never_sent
    FROM warehouse_products
    WHERE target_stock > 0
    GROUP BY marketplace
  `);
  for (const r of sendLag) {
    console.log(`  ${r.marketplace}: products=${r.products} never_sent=${r.never_sent} not_sent_7d=${r.not_sent_7d}`);
  }

  // ── 8. Products with target_stock > 0 but no supplier found ──────────────────
  console.log("\n=== 8. Products with stock but broken supplier ===");
  const stockNoSupplier = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products
    WHERE target_stock > 0
      AND (raw->>'selectedSupplier' IS NULL OR raw->>'selectedSupplier' = 'null')
      AND EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = warehouse_products.id::text)
  `);
  console.log(`  Products showing stock>0 but no selectedSupplier (ghost stock): ${stockNoSupplier[0].cnt}`);

  // ── 9. Recent errors in warehouse_products ────────────────────────────────────
  console.log("\n=== 9. Recent price send errors ===");
  const recentErrors = await prisma.$queryRawUnsafe(`
    SELECT marketplace, raw->'lastYandexPriceSend'->>'status' AS ym_status,
           raw->'lastOzonPriceSend'->>'status' AS ozon_status,
           COUNT(*) AS cnt
    FROM warehouse_products
    WHERE raw->'lastYandexPriceSend'->>'status' = 'error'
       OR raw->'lastOzonPriceSend'->>'status' = 'error'
    GROUP BY marketplace, ym_status, ozon_status
    LIMIT 10
  `);
  if (recentErrors.length) {
    for (const r of recentErrors) console.log(`  ${r.marketplace}: ym_status=${r.ym_status} ozon_status=${r.ozon_status} cnt=${r.cnt}`);
  } else {
    console.log(`  No price send errors in DB ✓`);
  }

  await prisma.$disconnect();
  console.log("\n=== Done ===");
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
