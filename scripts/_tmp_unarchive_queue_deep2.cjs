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

const DIAG_SCRIPT = `
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
const fss = require('fs');
(async () => {
  try {
    const queueStats = await p.ozonUnarchiveQueueItem.groupBy({ by: ['status'], _count: { id: true } });
    console.log('=== Queue stats ===');
    for (const s of queueStats) console.log(JSON.stringify({ status: s.status, count: Number(s._count.id) }));

    const dueCount = await p.ozonUnarchiveQueueItem.count({
      where: { status: { in: ['pending','processing','failed','delayed'] }, OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }] },
    });
    console.log('=== Due items ===');
    console.log(JSON.stringify({ due: dueCount }));

    const dueItems = await p.ozonUnarchiveQueueItem.findMany({
      where: { status: { in: ['pending','processing','failed','delayed'] }, OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }] },
      orderBy: [{ nextRetryAt: 'asc' }], take: 5,
    });
    console.log('=== Sample due items ===');
    for (const item of dueItems) {
      const raw = item.raw && typeof item.raw === 'object' ? item.raw : {};
      console.log(JSON.stringify({ id: item.id, productId: item.productId, offerId: item.offerId, target: item.target, status: item.status, attempts: item.attempts, warning: item.warning, nextRetryAt: item.nextRetryAt ? item.nextRetryAt.toISOString() : null, rawId: raw.id, rawWpId: raw.warehouseProductId }));
    }

    const wpIds = dueItems.map(i => i.productId).filter(Boolean);
    console.log('=== WP IDs to look up ===', JSON.stringify(wpIds));
    if (wpIds.length) {
      const wps = await p.warehouseProduct.findMany({
        where: { id: { in: wpIds } },
        select: { id: true, offerId: true, marketplace: true, target: true, archived: true, raw: true },
      });
      console.log('=== Found warehouse products ===');
      for (const wp of wps) {
        const raw = wp.raw && typeof wp.raw === 'object' ? wp.raw : {};
        console.log(JSON.stringify({ id: wp.id, offerId: wp.offerId, marketplace: wp.marketplace, target: wp.target, archived: wp.archived, msCode: raw.marketplaceState ? raw.marketplaceState.code : null, msArchived: raw.marketplaceState ? raw.marketplaceState.archived : null, isAutoArchived: raw.marketplaceState ? raw.marketplaceState.isAutoArchived : null, noSupArchivedAt: raw.noSupplierAutomation ? raw.noSupplierAutomation.archivedAt : null, linksCount: Array.isArray(wp.links) ? wp.links.length : (raw.links ? raw.links.length : 0) }));
      }
      const foundIds = new Set(wps.map(w => w.id));
      const missing = wpIds.filter(id => !foundIds.has(id));
      if (missing.length) console.log('NOT FOUND in warehouse_products:', JSON.stringify(missing));
    }

    const tgStats = await p.warehouseProduct.groupBy({ by: ['marketplace','target'], where: { marketplace: 'ozon' }, _count: { id: true } });
    console.log('=== Ozon targets ===');
    for (const t of tgStats) console.log(JSON.stringify({ mp: t.marketplace, target: t.target, count: Number(t._count.id) }));

    const arcCount = await p.warehouseProduct.count({ where: { marketplace: 'ozon', archived: true } });
    console.log('=== Archived ozon (archived=true) ===', JSON.stringify({ count: arcCount }));

    try {
      const dailyPath = '/var/www/davidsklad/davidskladik/data/ozon_unarchive_daily_state.json';
      const dailyText = fss.readFileSync(dailyPath, 'utf8');
      console.log('=== Daily state ===');
      console.log(dailyText.slice(0, 800));
    } catch(e) { console.log('daily state file error:', e.message); }

  } finally {
    await p.disconnect();
  }
})().catch(e => console.error('ERR:', e.message));
`;

function exec(conn, command) {
  return new Promise((resolve) => {
    conn.exec(command, (err, stream) => {
      if (err) { console.error("exec err:", err.message); return resolve(""); }
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
    // Write script to temp file on server
    const script = DIAG_SCRIPT.replace(/\$/g, "\\$").replace(/`/g, "\\`");
    await exec(conn, `cat > /tmp/diag_unarchive.js << 'ENDOFSCRIPT'\n${DIAG_SCRIPT}\nENDOFSCRIPT`);
    await exec(conn, "cp /tmp/diag_unarchive.js /var/www/davidsklad/davidskladik/_diag_unarchive.js && cd /var/www/davidsklad/davidskladik && node _diag_unarchive.js 2>&1; rm -f _diag_unarchive.js");
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
