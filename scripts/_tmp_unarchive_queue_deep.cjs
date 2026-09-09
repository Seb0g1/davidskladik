#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

if (!privateKey && !password) { console.error("DEPLOY_PASSWORD or SSH key required"); process.exit(1); }

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve(out));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      ...(privateKey ? { privateKey } : { password }),
      readyTimeout: 60000,
    });
  });
  try {
    await exec(conn, `cd /var/www/davidsklad/davidskladik && node --no-warnings -e "
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
(async () => {
  try {
    // 1. Кол-во позиций в очереди по статусам
    const queueStats = await p.ozonUnarchiveQueueItem.groupBy({
      by: ['status'],
      _count: { id: true },
    });
    console.log('=== Queue stats by status ===');
    for (const s of queueStats) console.log(JSON.stringify(s));

    // 2. Кол-во due-позиций (nextRetryAt < now)
    const dueCount = await p.ozonUnarchiveQueueItem.count({
      where: {
        status: { in: ['pending', 'processing', 'failed', 'delayed'] },
        OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }],
      },
    });
    console.log('=== Due items count ===');
    console.log(JSON.stringify({ due: dueCount }));

    // 3. Первые 5 due-позиций + их warehouse product state
    const dueItems = await p.ozonUnarchiveQueueItem.findMany({
      where: {
        status: { in: ['pending', 'processing', 'failed', 'delayed'] },
        OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }],
      },
      orderBy: [{ nextRetryAt: 'asc' }],
      take: 5,
    });
    console.log('=== Sample due items ===');
    for (const item of dueItems) {
      const raw = item.raw && typeof item.raw === 'object' ? item.raw : {};
      console.log(JSON.stringify({
        id: item.id,
        productId: item.productId,
        offerId: item.offerId,
        target: item.target,
        status: item.status,
        attempts: item.attempts,
        warning: item.warning,
        nextRetryAt: item.nextRetryAt,
        rawId: raw.id,
        rawWpId: raw.warehouseProductId,
      }));
    }

    // 4. Проверка — есть ли соответствующие warehouse_products для первых 5
    const wpIds = dueItems.map(i => i.productId).filter(Boolean);
    console.log('=== Warehouse products for sample due items ===');
    if (wpIds.length) {
      const wps = await p.warehouseProduct.findMany({
        where: { id: { in: wpIds } },
        select: {
          id: true, offerId: true, marketplace: true, target: true, archived: true,
          raw: true,
        },
      });
      for (const wp of wps) {
        const raw = wp.raw && typeof wp.raw === 'object' ? wp.raw : {};
        console.log(JSON.stringify({
          id: wp.id, offerId: wp.offerId, marketplace: wp.marketplace, target: wp.target,
          archived: wp.archived,
          msCode: raw.marketplaceState?.code,
          msArchived: raw.marketplaceState?.archived,
          isAutoArchived: raw.marketplaceState?.isAutoArchived,
          noSupplierArchivedAt: raw.noSupplierAutomation?.archivedAt,
          hasLinks: Array.isArray(raw.links) ? raw.links.length : 'n/a',
        }));
      }
      // Ищем по offerId+target если productId не совпало
      const foundIds = new Set(wps.map(w => w.id));
      const missing = wpIds.filter(id => !foundIds.has(id));
      if (missing.length) {
        console.log('NOT FOUND in warehouse_products by id:', JSON.stringify(missing));
        // Попробуем найти по offerId
        const missingItems = dueItems.filter(i => missing.includes(i.productId));
        for (const mi of missingItems) {
          const byOffer = await p.warehouseProduct.findFirst({
            where: { offerId: mi.offerId, marketplace: 'ozon' },
            select: { id: true, offerId: true, marketplace: true, target: true, archived: true },
          });
          console.log('By offerId lookup:', JSON.stringify({ queueProductId: mi.productId, offerId: mi.offerId, found: byOffer }));
        }
      }
    }

    // 5. Проверка enabledWarehouseTargetWhere - смотрим есть ли отключенные таргеты
    const targetStats = await p.warehouseProduct.groupBy({
      by: ['marketplace', 'target'],
      where: { marketplace: 'ozon' },
      _count: { id: true },
    });
    console.log('=== Ozon targets in warehouse ===');
    for (const t of targetStats) console.log(JSON.stringify({ mp: t.marketplace, target: t.target, count: Number(t._count.id) }));

    // 6. Кол-во архивных Ozon товаров вообще
    const archivedCount = await p.warehouseProduct.count({
      where: {
        marketplace: 'ozon',
        OR: [
          { archived: true },
          { raw: { path: ['marketplaceState', 'archived'], equals: true } },
          { raw: { path: ['marketplaceState', 'code'], equals: 'archived' } },
        ],
      },
    });
    console.log('=== Archived ozon products total ===');
    console.log(JSON.stringify({ archivedCount }));

    // 7. Смотрим dailyUsed для сегодня
    const today = new Date();
    const moscowOffset = 3 * 60 * 60 * 1000;
    const dateKey = new Date(today.getTime() + moscowOffset).toISOString().slice(0, 10);
    console.log('=== Daily state check ===');
    console.log(JSON.stringify({ todayKey: dateKey }));
    // Читаем файл состояния
    const dailyPath = '/var/www/davidsklad/davidskladik/data/ozon_unarchive_daily_state.json';
    const fs2 = require('fs');
    try {
      const dailyText = fs2.readFileSync(dailyPath, 'utf8');
      console.log('daily state:', dailyText.slice(0, 500));
    } catch(e) {
      console.log('daily state file not found or error:', e.message);
    }

  } finally {
    await p.\$disconnect();
  }
})().catch(e => console.error('ERR:', e.message, e.stack));
" 2>&1`);

  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
