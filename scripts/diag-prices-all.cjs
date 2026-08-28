#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const prisma = new PrismaClient();

async function main() {
  const now = new Date();
  const todayStart = new Date(now);
  todayStart.setHours(0, 0, 0, 0);

  // --- Price history counts today ---
  const [totalToday] = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) as cnt FROM price_history WHERE created_at >= $1
  `, todayStart);
  console.log("=== PRICE HISTORY TODAY ===");
  console.log("Total sends today:", Number(totalToday.cnt));

  const byMarket = await prisma.$queryRawUnsafe(`
    SELECT marketplace, status, COUNT(*) as cnt
    FROM price_history WHERE created_at >= $1
    GROUP BY marketplace, status ORDER BY marketplace, cnt DESC
  `, todayStart);
  for (const r of byMarket) {
    console.log(`  [${r.marketplace}] ${r.status}: ${Number(r.cnt)}`);
  }

  // --- Recent 20 entries per marketplace ---
  console.log("\n=== RECENT PRICE HISTORY (last 5 per marketplace) ===");
  for (const mp of ["ozon", "yandex", "avito", "wildberries"]) {
    const rows = await prisma.$queryRawUnsafe(`
      SELECT marketplace::text, status::text, offer_id, created_at, error
      FROM price_history
      WHERE marketplace::text = $1 AND created_at >= $2
      ORDER BY created_at DESC LIMIT 5
    `, mp, todayStart);
    if (!rows.length) { console.log(`  [${mp}] no entries today`); continue; }
    console.log(`  [${mp}]:`);
    for (const r of rows) {
      const msg = r.error ? ` | err: ${String(r.error).slice(0, 80)}` : "";
      console.log(`    ${r.created_at?.toISOString?.() || r.created_at} ${r.status} ${r.offer_id}${msg}`);
    }
  }

  // --- priceRetryQueueItem counts ---
  console.log("\n=== PRICE RETRY QUEUE ===");
  const retryRows = await prisma.$queryRawUnsafe(`
    SELECT marketplace, status, COUNT(*) as cnt
    FROM price_retry_queue
    GROUP BY marketplace, status ORDER BY marketplace, cnt DESC
  `);
  if (!retryRows.length) {
    console.log("  (empty)");
  } else {
    for (const r of retryRows) {
      console.log(`  [${r.marketplace}] ${r.status}: ${Number(r.cnt)}`);
    }
  }

  // --- Pending retries with errors ---
  console.log("\n=== RETRY QUEUE PENDING (sample) ===");
  const pending = await prisma.$queryRawUnsafe(`
    SELECT marketplace::text, offer_id, status::text, attempts, error, updated_at
    FROM price_retry_queue
    WHERE status::text IN ('pending','failed')
    ORDER BY updated_at DESC LIMIT 10
  `);
  if (!pending.length) {
    console.log("  (none)");
  } else {
    for (const r of pending) {
      const err = r.error ? String(r.error).slice(0, 100) : "-";
      console.log(`  [${r.marketplace}] ${r.offer_id} | ${r.status} | attempts:${r.attempts} | ${r.updated_at?.toISOString?.() || r.updated_at} | ${err}`);
    }
  }
}

main().catch(e => { console.error(e.message); process.exit(1); }).finally(() => prisma.$disconnect());
