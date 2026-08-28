#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function exec(conn, command, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { resolve(""); }, timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      let out = "";
      stream.on("data", (d) => { out += d; });
      stream.stderr.on("data", (d) => { out += d; });
      stream.on("close", () => { clearTimeout(timer); resolve(out); });
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((res, rej) => conn.on("ready", res).on("error", rej).connect({
    host: "81.17.154.153", username: "root", password, readyTimeout: 30000,
    keepaliveInterval: 10000, keepaliveCountMax: 60,
  }));
  try {
    // 1. Check PM2 logs for price errors
    console.log("=== PM2 Worker logs: last 200 lines (filtering for price/product errors) ===");
    const logs = await exec(conn, "pm2 logs davidsklad-worker --lines 200 --nostream 2>&1 | grep -i 'product.not.found\\|price.*failed\\|import.*price\\|offer_id.*error' | tail -30", 30000);
    console.log(logs || "(no matching log lines)");

    console.log("\n=== PM2 Worker logs: raw last 50 lines ===");
    const rawLogs = await exec(conn, "pm2 logs davidsklad-worker --lines 50 --nostream 2>&1 | tail -50", 30000);
    console.log(rawLogs || "(no logs)");

    // 2. Check Ozon price API directly for one specific offer_id using the app API
    console.log("\n=== System environment check ===");
    const envCheck = await exec(conn, "cd /var/www/davidsklad/davidskladik && node -e \"require('dotenv').config(); console.log('OZON_CLIENT_ID:', process.env.OZON_CLIENT_ID); console.log('OZON2_CLIENT_ID:', process.env.OZON2_CLIENT_ID);\" 2>&1", 15000);
    console.log(envCheck);

  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
