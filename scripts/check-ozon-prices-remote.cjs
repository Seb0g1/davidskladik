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
    let out = "";
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve(out));
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
    await exec(conn, `cd ${remoteRoot} && node -e "
const { Queue } = require('bullmq');
async function main() {
  const q = new Queue('marketplace', { connection: { host: '127.0.0.1', port: 6379 } });
  const [failed, active, waiting] = await Promise.all([q.getFailed(0, 30), q.getActive(), q.getWaiting(0, 5)]);
  console.log('=== FAILED JOBS (' + failed.length + ') ===');
  const grouped = {};
  for (const j of failed) {
    const key = (j.name || 'unknown') + ' | ' + (j.failedReason || '').slice(0, 120);
    grouped[key] = (grouped[key] || 0) + 1;
  }
  for (const [k, n] of Object.entries(grouped).sort((a,b) => b[1]-a[1])) {
    console.log('  x' + n + ' — ' + k);
  }
  console.log('');
  console.log('=== ACTIVE (' + active.length + ') ===');
  for (const j of active.slice(0,5)) console.log('  ' + j.name + ' | data:' + JSON.stringify(j.data || {}).slice(0,100));
  console.log('');
  console.log('=== WAITING (' + waiting.length + ') ===');
  for (const j of waiting.slice(0,5)) console.log('  ' + j.name + ' | data:' + JSON.stringify(j.data || {}).slice(0,100));
  await q.close();
}
main().catch(e => console.error('ERR:', e.message));
"`);

    console.log("\n=== API ERROR LOG (last 40 lines with ozon/price/stock) ===");
    await exec(conn, `pm2 logs davidsklad-api --lines 200 --nostream 2>/dev/null | grep -iE "ozon|price_send|stock_send|error|failed|warn" | grep -v "event_loop_blocked\\|slow request\\|warehouse_postgres" | tail -40`);

    console.log("\n=== WORKER ERROR LOG (last 30 lines Ozon-related) ===");
    await exec(conn, `pm2 logs davidsklad-worker --lines 200 --nostream 2>/dev/null | grep -iE "ozon|price_send|stock_send|error|failed" | grep -v "event_loop_blocked\\|link_save_pm_timeout\\|telegram\\|yandex\\|avito\\|wb_" | tail -30`);
  } finally {
    conn.end();
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
