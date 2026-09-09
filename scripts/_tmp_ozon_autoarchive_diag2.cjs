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

if (!privateKey && !password) {
  console.error("Either DEPLOY_PASSWORD or SSH key at ~/.ssh/davidsklad_deploy is required");
  process.exit(1);
}

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => resolve(out));
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
    console.log("=== 1. PM2 статус ===");
    await exec(conn, "pm2 list 2>/dev/null | head -30");

    console.log("\n=== 2. Логи по автоархиву Ozon (последние 200 строк) ===");
    await exec(conn, [
      "tail -n 3000 /root/.pm2/logs/davidsklad-worker-out-0.log",
      "tail -n 3000 /root/.pm2/logs/davidsklad-out-0.log 2>/dev/null || true",
    ].join(" | cat; ") + " | grep -iE 'unarchive|autoarchiv|auto.archiv|rebuild.queue|ozon.unarchive' | tail -n 200");

    console.log("\n=== 3. Ошибки unarchive за сегодня ===");
    await exec(conn, [
      "tail -n 5000 /root/.pm2/logs/davidsklad-worker-out-0.log",
      "tail -n 5000 /root/.pm2/logs/davidsklad-out-0.log 2>/dev/null || true",
    ].join(" | cat; ") + " | grep -iE 'unarchive.*(error|fail|warn)|error.*unarchive|fail.*unarchive' | tail -n 100");

    console.log("\n=== 4. Состояние очереди в PG (ozon_unarchive_queue) ===");
    await exec(conn, `cd /var/www/davidsklad/davidskladik && node -e "
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
(async () => {
  try {
    // Ищем в app_kv_store
    const rows = await p.\\$queryRawUnsafe(\\\`SELECT key, length(value::text) AS len, LEFT(value::text, 500) AS preview FROM app_kv_store WHERE key ILIKE '%unarchive%' ORDER BY key LIMIT 20\\\`).catch(() => []);
    console.log('KV store unarchive rows:', JSON.stringify(rows, null, 2));

    // Смотрим кол-во архивных Ozon товаров
    const archived = await p.\\$queryRawUnsafe(\\\`
      SELECT
        wp.target,
        COUNT(*) AS total,
        SUM(CASE WHEN wp.raw->>'archived' = 'true' OR wp.raw->'marketplaceState'->>'archived' = 'true' OR wp.raw->'marketplaceState'->>'code' = 'archived' THEN 1 ELSE 0 END) AS archived_count,
        SUM(CASE WHEN wp.raw->'marketplaceState'->>'isAutoArchived' = 'true' THEN 1 ELSE 0 END) AS auto_archived_count,
        SUM(CASE WHEN EXISTS(SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id) THEN 1 ELSE 0 END) AS with_links
      FROM warehouse_products wp
      WHERE wp.marketplace = 'ozon'
      GROUP BY wp.target
      ORDER BY wp.target
    \\\`).catch((e) => [{ error: e.message }]);
    console.log('Ozon warehouse stats:', JSON.stringify(archived, null, 2));

    // Смотрим кол-во архивных с линками (должны быть в очереди)
    const archivedLinked = await p.\\$queryRawUnsafe(\\\`
      SELECT
        wp.target,
        COUNT(*) AS archived_with_links,
        STRING_AGG(DISTINCT wp.raw->'marketplaceState'->>'code', ', ') AS codes
      FROM warehouse_products wp
      WHERE wp.marketplace = 'ozon'
        AND EXISTS(SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id)
        AND (
          wp.raw->>'archived' = 'true'
          OR wp.raw->'marketplaceState'->>'archived' = 'true'
          OR wp.raw->'marketplaceState'->>'code' = 'archived'
          OR wp.raw->'noSupplierAutomation'->>'archivedAt' IS NOT NULL
        )
      GROUP BY wp.target
    \\\`).catch((e) => [{ error: e.message }]);
    console.log('Ozon archived WITH links (unarchive candidates):', JSON.stringify(archivedLinked, null, 2));
  } finally {
    await p.\\$disconnect();
  }
})().catch(e => console.error('ERR:', e.message));
" 2>&1`);

    console.log("\n=== 5. API: состояние очереди unarchive ===");
    await exec(conn, "curl -s -b 'connect.sid=SKIP' http://localhost:3000/api/ozon/unarchive-queue 2>/dev/null | head -c 2000 || echo 'curl failed'");

    console.log("\n=== 6. Последние записи в логах worker про unarchive ===");
    await exec(conn, "tail -n 10000 /root/.pm2/logs/davidsklad-worker-out-0.log | grep -iE 'unarchive|autoarchiv' | tail -n 100");

  } finally {
    conn.end();
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
