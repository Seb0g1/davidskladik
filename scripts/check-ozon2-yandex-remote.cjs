#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

const remoteRoot = "/var/www/davidsklad/davidskladik";

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve());
    });
  });
}

async function connect() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password,
      readyTimeout: 30000, keepaliveInterval: 5000,
    });
  });
  return conn;
}

async function main() {
  const conn = await connect();
  try {
    // Check second Ozon account errors
    console.log("=== OZON-2 ERRORS (worker log) ===");
    await exec(conn, `pm2 logs davidsklad-worker --lines 500 --nostream 2>/dev/null | grep -i "ozon-3d10ec43\\|ozon2\\|2533393\\|second" | tail -30`);

    console.log("\n=== ALL ACCOUNTS — price push results (worker log last 300) ===");
    await exec(conn, `pm2 logs davidsklad-worker --lines 300 --nostream 2>/dev/null | grep -E "auto_price_push_heartbeat|price_send|ozon_price|yandex_price" | tail -20`);

    console.log("\n=== YANDEX PRICE UPDATE — price_history last entries ===");
    await exec(conn, `cd ${remoteRoot} && node -e "
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
async function main() {
  // Check most recent Yandex price history entries
  const recent = await prisma.priceHistory.findMany({
    where: { marketplace: 'yandex' },
    orderBy: { createdAt: 'desc' },
    take: 10,
    select: { offerId: true, price: true, createdAt: true, reason: true }
  });
  console.log('Recent Yandex price_history (' + recent.length + '):');
  for (const r of recent) {
    console.log('  ' + r.offerId + ' = ' + r.price + ' RUB  ' + new Date(r.createdAt).toISOString() + '  reason=' + (r.reason || 'null'));
  }

  // Count how many Yandex products have price history updated today
  const today = new Date(); today.setHours(0,0,0,0);
  const todayCount = await prisma.priceHistory.count({
    where: { marketplace: 'yandex', createdAt: { gte: today } }
  });
  console.log('Yandex price_history entries today: ' + todayCount);

  // Sample of Yandex products with current prices
  const yandexProducts = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'yandex', archived: false },
    orderBy: { updatedAt: 'desc' },
    take: 10,
    select: { offerId: true, updatedAt: true, raw: true }
  });
  console.log('\nSample Yandex products (latest 10 by updatedAt):');
  for (const p of yandexProducts) {
    const raw = p.raw || {};
    const price = raw.yandex?.price || raw.price || null;
    console.log('  ' + p.offerId + ' price=' + price + '  updated=' + new Date(p.updatedAt).toISOString());
  }
  await prisma.\$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
"`);

    console.log("\n=== OZON-2 — price_history last entries ===");
    await exec(conn, `cd ${remoteRoot} && node -e "
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
async function main() {
  // Check if there's a second ozon account identifier in price_history
  // ozon-2 account id is ozon-3d10ec43
  const recent = await prisma.priceHistory.findMany({
    where: { marketplace: 'ozon', accountId: { contains: '3d10ec43' } },
    orderBy: { createdAt: 'desc' },
    take: 10,
    select: { offerId: true, price: true, createdAt: true, reason: true, accountId: true }
  });
  console.log('Ozon-2 price_history (' + recent.length + '):');
  for (const r of recent) {
    console.log('  ' + r.offerId + ' = ' + r.price + '  ' + new Date(r.createdAt).toISOString() + '  account=' + r.accountId);
  }

  // Check ozon-2 products
  const ozon2Products = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'ozon', archived: false, accountId: { contains: '3d10ec43' } },
    orderBy: { updatedAt: 'desc' },
    take: 5,
    select: { offerId: true, updatedAt: true, raw: true, accountId: true }
  });
  console.log('\nOzon-2 products sample (' + ozon2Products.length + '):');
  for (const p of ozon2Products) {
    const raw = p.raw || {};
    const price = raw.ozon?.price || raw.price || null;
    console.log('  ' + p.offerId + ' price=' + price + '  account=' + p.accountId + '  updated=' + new Date(p.updatedAt).toISOString());
  }
  await prisma.\$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
"`);

    console.log("\n=== RETRY QUEUE — how many pending ===");
    await exec(conn, `cd ${remoteRoot} && node -e "
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();
async function main() {
  const counts = await prisma.priceRetryQueueItem.groupBy({
    by: ['marketplace', 'status'],
    _count: true
  });
  console.log('priceRetryQueueItem counts:');
  for (const c of counts) console.log('  ' + c.marketplace + ' / ' + c.status + ': ' + c._count);

  // Check latest worker errors about ozon-2
  await prisma.\$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
"`);

    console.log("\n=== WORKER ERRORS — ozon price failures ===");
    await exec(conn, `pm2 logs davidsklad-worker --lines 500 --nostream 2>/dev/null | grep -iE "(price.*fail|fail.*price|ozon.*error|error.*ozon|ResourceExhausted|rate.limit|account)" | grep -v "reviews\\|ReviewList\\|telegram\\|yandex.*fail\\|avito" | tail -20`);

  } finally {
    conn.end();
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
