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

function exec(conn, command, { silent = false } = {}) {
  return new Promise((resolve) => {
    conn.exec(command, (err, stream) => {
      if (err) { if (!silent) console.error("exec err:", err.message); return resolve(""); }
      let out = "";
      stream.on("data", (d) => { out += d; if (!silent) process.stdout.write(d); });
      stream.stderr.on("data", (d) => { if (!silent) process.stderr.write(d); });
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
    console.log("=== 1. Последние лог-строки по unarchive (supplier_recovery_run, ozon unarchive queue processed, supplier recovery automation complete) ===");
    await exec(conn, `tail -n 20000 /root/.pm2/logs/davidsklad-worker-out-0.log 2>/dev/null | grep -E 'supplier_recovery_run|ozon unarchive queue processed|supplier recovery automation complete|ozon_unarchive_queue_ghost_purge|ozon_unarchive_queue_daily_defer|ozon unarchive queue auto|already_running_or_queued' | tail -50`);

    console.log("\n=== 2. Ошибки в worker за последние часы ===");
    await exec(conn, `tail -n 5000 /root/.pm2/logs/davidsklad-worker-err-0.log 2>/dev/null | grep -iE 'unarchive|supplier.recov' | tail -30`);

    console.log("\n=== 3. daily state файл ===");
    await exec(conn, `cat /var/www/davidsklad/davidskladik/data/ozon-unarchive-daily.json 2>/dev/null || echo 'ФАЙЛ ОТСУТСТВУЕТ'`);

    console.log("\n=== 4. ozon-unarchive-queue.json (первые 2 позиции) ===");
    await exec(conn, `node -e "
const fs = require('fs');
try {
  const q = JSON.parse(fs.readFileSync('/var/www/davidsklad/davidskladik/data/ozon-unarchive-queue.json','utf8'));
  console.log('updatedAt:', q.updatedAt);
  console.log('daily keys:', JSON.stringify(q.daily));
  console.log('items count:', q.items?.length);
  const sample = (q.items||[]).slice(0,3).map(i=>({id:i.id,wid:i.warehouseProductId,status:i.status,nextRetryAt:i.nextRetryAt,warning:i.warning}));
  console.log('sample items:', JSON.stringify(sample, null, 2));
} catch(e) { console.log('ERR:', e.message); }
"`);

    console.log("\n=== 5. PG: daily state из БД (ozon_unarchive_daily) ===");
    await exec(conn, `cd /var/www/davidsklad/davidskladik && node -e "
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
(async() => {
  try {
    const rows = await p.\\$queryRawUnsafe(\`SELECT key, LEFT(value::text, 500) as val FROM app_kv_store WHERE key ILIKE '%unarchive%daily%' OR key ILIKE '%ozon%daily%' LIMIT 10\`).catch(()=>[]);
    console.log('KV daily rows:', JSON.stringify(rows, null, 2));
    const stats = await p.ozonUnarchiveQueueItem.groupBy({ by: ['status'], _count: { id: true } });
    console.log('Queue stats:', JSON.stringify(stats));
    const due = await p.ozonUnarchiveQueueItem.count({
      where: { status: { in: ['pending','processing','failed','delayed'] }, OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }] }
    });
    console.log('Due count:', due);
    // Check a few due items
    const items = await p.ozonUnarchiveQueueItem.findMany({
      where: { status: { in: ['pending','processing','failed','delayed'] }, OR: [{ nextRetryAt: null }, { nextRetryAt: { lte: new Date() } }] },
      orderBy: [{ nextRetryAt: 'asc' }], take: 3
    });
    for (const item of items) {
      const raw = item.raw && typeof item.raw === 'object' ? item.raw : {};
      const wpId = raw.id || raw.warehouseProductId || item.productId;
      // Check if warehouse product exists
      const wp = wpId ? await p.warehouseProduct.findUnique({ where: { id: wpId }, select: { id: true, archived: true, offerId: true } }) : null;
      console.log('Item:', JSON.stringify({ qItem: { id: item.id, productId: item.productId, offerId: item.offerId, target: item.target, warning: item.warning, attempts: item.attempts }, rawId: raw.id, rawWpId: raw.warehouseProductId, wp }));
    }
  } finally {
    await p.\\$disconnect();
  }
})().catch(e => console.error('ERR:', e.message));
" 2>&1`);

    console.log("\n=== 6. API status /api/system (ozonUnarchiveQueue + lastAutoarchiveRun) ===");
    await exec(conn, `curl -s 'http://localhost:3000/api/system' 2>/dev/null | node -e "
const chunks=[];process.stdin.on('data',d=>chunks.push(d));process.stdin.on('end',()=>{
  try {
    const d = JSON.parse(Buffer.concat(chunks).toString());
    const q = d.ozonUnarchiveQueue || {};
    const last = d.lastAutoarchiveRun || {};
    console.log('ozonUnarchiveQueue:', JSON.stringify(q, null, 2));
    console.log('lastAutoarchiveRun:', JSON.stringify(last, null, 2));
  } catch(e) { console.log('parse err', e.message); }
})" || echo 'curl failed'`);

  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
