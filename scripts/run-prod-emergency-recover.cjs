#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const root = path.resolve(__dirname, "..");
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

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(localPath)
        .pipe(sftp.createWriteStream(remotePath))
        .on("close", resolve)
        .on("error", reject);
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
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    });
  });
  try {
    console.log("=== EMERGENCY: kill all heavy scripts ===");
    await exec(conn, [
      "pkill -9 -f 'node scripts/fix-ozon-quarantine-prices' || true",
      "pkill -9 -f 'node scripts/audit-and-repush-prices-remote' || true",
      "pkill -9 -f 'node scripts/repair-yandex-media-from-ozon' || true",
      "pkill -9 -f 'node scripts/delete-yandex-small-volume' || true",
      "pkill -9 -f 'node scripts/repair-linked-warehouse-catalog' || true",
      "pkill -9 -f 'node scripts/audit-warehouse-catalog-health' || true",
      "pkill -9 -f 'node scripts/run-prod-complete-pipeline' || true",
      "rm -f data/delete-yandex-small-volume.lock data/repair-yandex-media-from-ozon.lock || true",
      "sleep 2",
      "echo '--- processes after kill ---'",
      "ps aux --sort=-%cpu | head -8",
      "free -h | head -2",
    ].join(" && "));

    console.log("=== deploy server.js + api/worker + locks ===");
    await exec(conn, `mkdir -p ${remoteRoot}/scripts ${remoteRoot}/data`);
    for (const rel of [
      "server.js",
      "api-entry.js",
      "worker-entry.js",
      "ecosystem.config.cjs",
      "scripts/repair-yandex-media-from-ozon.cjs",
      "scripts/delete-yandex-small-volume.cjs",
      "scripts/run-prod-complete-pipeline.cjs",
      "scripts/prod-post-deploy-check.cjs",
    ]) {
      const local = path.join(root, rel);
      if (fs.existsSync(local)) await sftpPut(conn, local, `${remoteRoot}/${rel}`);
    }

    console.log("=== restart pm2 api+worker ===");
    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 delete davidsklad 2>/dev/null || true",
      "pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env || pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "pm2 save",
      "sleep 14",
      "pm2 list",
      "curl -s -o /dev/null -w 'health_api:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3000/api/live-status || true",
      "curl -s -o /dev/null -w 'worker_health:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3001/health || true",
      "curl -s -o /dev/null -w 'login_page:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3000/login.html || true",
    ].join(" && "));

    console.log("=== post-deploy check (blocking) ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
