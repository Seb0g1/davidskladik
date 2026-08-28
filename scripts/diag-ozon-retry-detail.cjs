#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const prisma = new PrismaClient();

async function main() {
  // By target
  const byTarget = await prisma.$queryRawUnsafe(`
    SELECT target, status::text, COUNT(*) as cnt
    FROM price_retry_queue
    WHERE marketplace::text = 'ozon'
    GROUP BY target, status ORDER BY cnt DESC
  `);
  console.log("=== OZON RETRY QUEUE BY TARGET ===");
  for (const r of byTarget) console.log(`  target:${r.target || "(null)"} | ${r.status}: ${Number(r.cnt)}`);

  // By error
  const byError = await prisma.$queryRawUnsafe(`
    SELECT COALESCE(NULLIF(error, ''), '(no error)') as err, COUNT(*) as cnt
    FROM price_retry_queue
    WHERE marketplace::text = 'ozon'
    GROUP BY err ORDER BY cnt DESC LIMIT 10
  `);
  console.log("\n=== OZON RETRY ERRORS ===");
  for (const r of byError) console.log(`  ${r.err}: ${Number(r.cnt)}`);

  // Sample of "product not found" offers
  const sample = await prisma.$queryRawUnsafe(`
    SELECT offer_id, target, attempts, error, updated_at
    FROM price_retry_queue
    WHERE marketplace::text = 'ozon' AND error LIKE '%not found%'
    LIMIT 20
  `);
  console.log("\n=== SAMPLE 'product not found' ITEMS ===");
  for (const r of sample) console.log(`  ${r.offer_id} | target:${r.target} | att:${r.attempts} | ${r.updated_at?.toISOString?.() || r.updated_at}`);

  // Price history recent ozon failures
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const recentFailed = await prisma.$queryRawUnsafe(`
    SELECT offer_id, target, error, created_at
    FROM price_history
    WHERE marketplace::text = 'ozon' AND status::text = 'failed' AND created_at >= $1
    ORDER BY created_at DESC LIMIT 10
  `, todayStart);
  console.log("\n=== RECENT OZON FAILED PRICE HISTORY ===");
  for (const r of recentFailed) {
    console.log(`  ${r.created_at?.toISOString?.() || r.created_at} | target:${r.target} | ${r.offer_id} | ${String(r.error || "").slice(0, 80)}`);
  }
}

main().catch(e => { console.error(e.message); process.exit(1); }).finally(() => prisma.$disconnect());
