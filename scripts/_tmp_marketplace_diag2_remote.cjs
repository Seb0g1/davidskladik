#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

async function main() {
  const prisma = new PrismaClient();
  try {
    // 1. Look at actual raw field keys from a sample of products
    console.log("=== Sample raw field keys (ozon) ===");
    const sample = await prisma.$queryRawUnsafe(`
      SELECT id, marketplace, raw
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND raw IS NOT NULL
      LIMIT 3
    `);
    for (const r of sample) {
      console.log(`\n[${r.id}] raw keys: ${Object.keys(r.raw || {}).join(", ")}`);
      const raw = r.raw || {};
      // Show key fields
      const fields = ["sellable","stock","status","archived","hidden","marketplacePrice","targetPrice","priceStatus","lastError","offerId","target","isArchived","isHidden","active"];
      for (const f of fields) {
        if (raw[f] !== undefined) console.log(`  ${f}: ${JSON.stringify(raw[f])}`);
      }
    }

    // 2. Count by priceStatus per marketplace/target
    console.log("\n\n=== Price status breakdown from 'raw' (priceStatus field) ===");
    const byStatus = await prisma.$queryRawUnsafe(`
      SELECT
        marketplace,
        raw->>'target' AS target,
        raw->>'priceStatus' AS price_status,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE marketplace IN ('ozon','yandex') AND raw IS NOT NULL
      GROUP BY marketplace, raw->>'target', raw->>'priceStatus'
      ORDER BY marketplace, target, count DESC
    `);
    for (const r of byStatus) {
      console.log(`  [${r.marketplace}/${r.target}] ${r.price_status || 'null'}: ${r.count}`);
    }

    // 3. What errors do the failed products have?
    console.log("\n\n=== API error samples (Ozon, priceStatus=failed) ===");
    const errorSamples = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'offerId' AS offer_id,
        raw->>'target' AS target,
        raw->>'lastError' AS last_error,
        raw->>'marketplacePrice' AS marketplace_price,
        raw->>'targetPrice' AS target_price,
        (raw->>'updatedAt') AS updated_at
      FROM warehouse_products
      WHERE
        marketplace = 'ozon'
        AND raw->>'priceStatus' = 'failed'
        AND raw IS NOT NULL
      LIMIT 10
    `);
    for (const r of errorSamples) {
      console.log(`\n  [${r.target}] ${r.offer_id}`);
      console.log(`    price: marketplace=${r.marketplace_price}₽  target=${r.target_price}₽`);
      console.log(`    error: ${r.last_error}`);
      console.log(`    updatedAt: ${r.updated_at}`);
    }

    // 4. Error frequency - group by error text
    console.log("\n\n=== Error message frequency (Ozon, priceStatus=failed) ===");
    const errFreq = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'target' AS target,
        raw->>'lastError' AS last_error,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE
        marketplace = 'ozon'
        AND raw->>'priceStatus' = 'failed'
        AND raw IS NOT NULL
      GROUP BY raw->>'target', raw->>'lastError'
      ORDER BY count DESC
      LIMIT 20
    `);
    for (const r of errFreq) {
      const err = String(r.last_error || "").slice(0, 120);
      console.log(`  [${r.target}] ${r.count}x: ${err}`);
    }

    // 5. Yandex pending breakdown
    console.log("\n\n=== Yandex pending analysis ===");
    const ymPending = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'target' AS target,
        raw->>'lastError' AS last_error,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE
        marketplace = 'yandex'
        AND raw->>'priceStatus' IN ('pending', 'failed')
        AND raw IS NOT NULL
      GROUP BY raw->>'target', raw->>'lastError'
      ORDER BY count DESC
      LIMIT 15
    `);
    for (const r of ymPending) {
      const err = String(r.last_error || "none").slice(0, 120);
      console.log(`  [${r.target}] ${r.count}x: ${err}`);
    }

    // 6. Check if ozon-3d10ec43 (account 2) has failures
    console.log("\n\n=== Ozon account 2 (ozon-3d10ec43) status ===");
    const ozon2 = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'priceStatus' AS price_status,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE
        marketplace = 'ozon'
        AND raw->>'target' = 'ozon-3d10ec43'
        AND raw IS NOT NULL
      GROUP BY raw->>'priceStatus'
      ORDER BY count DESC
    `);
    for (const r of ozon2) {
      console.log(`  ${r.price_status || 'null'}: ${r.count}`);
    }

    // 7. Archival status check (actual fields in raw)
    console.log("\n\n=== Ozon archive status breakdown ===");
    const archivalStatus = await prisma.$queryRawUnsafe(`
      SELECT
        raw->>'target' AS target,
        raw->>'archived' AS archived,
        raw->>'hidden' AS hidden,
        COUNT(*) AS count
      FROM warehouse_products
      WHERE marketplace = 'ozon' AND raw IS NOT NULL
      GROUP BY raw->>'target', raw->>'archived', raw->>'hidden'
      ORDER BY count DESC
      LIMIT 20
    `);
    for (const r of archivalStatus) {
      console.log(`  [${r.target}] archived=${r.archived} hidden=${r.hidden}: ${r.count}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
