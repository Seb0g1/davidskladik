#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const remoteRoot = "/var/www/davidsklad/davidskladik";

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve()));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 60000,
    });
  });
  try {
    const script = [
      `cd ${remoteRoot}`,
      "cp .env .env.bak-ozon-batch-$(date +%Y%m%d%H%M%S)",
      "sed -i 's/^OZON_PRICE_BATCH_SIZE=.*/OZON_PRICE_BATCH_SIZE=100/' .env",
      "sed -i 's/^OZON_PRICE_BATCH_DELAY_MS=.*/OZON_PRICE_BATCH_DELAY_MS=400/' .env",
      "grep -E '^OZON_PRICE_BATCH' .env",
      "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "sleep 8",
      "node scripts/_prod-monitor-progress.cjs",
    ].join(" && ");
    await exec(conn, script);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
