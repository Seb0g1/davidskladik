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
    });
  });
  try {
    console.log("=== kill stuck maintenance scripts ===");
    await exec(conn, [
      "pkill -9 -f 'node scripts/delete-yandex-small-volume' || true",
      "pkill -9 -f 'node scripts/repair-yandex-media-from-ozon' || true",
      "pkill -f 'node scripts/repair-linked-warehouse-catalog' || true",
      "pkill -f 'node scripts/audit-warehouse-catalog-health' || true",
      "pkill -f 'node scripts/run-prod-complete-pipeline' || true",
      "rm -f /var/www/davidsklad/davidskladik/data/delete-yandex-small-volume.lock /var/www/davidsklad/davidskladik/data/repair-yandex-media-from-ozon.lock || true",
      "sleep 2",
      "ps aux | grep -E 'node scripts/(delete|repair-yandex|repair-linked|audit-warehouse|run-prod)' | grep -v grep || echo 'no stuck scripts'",
    ].join(" && "));

    console.log("=== deploy server + frontend ===");
    await exec(conn, `mkdir -p ${remoteRoot}/scripts ${remoteRoot}/public/app-modern/assets`);
    for (const rel of [
      "server.js",
      "scripts/delete-yandex-small-volume.cjs",
      "scripts/repair-yandex-media-from-ozon.cjs",
      "scripts/run-prod-complete-pipeline.cjs",
      "scripts/prod-post-deploy-check.cjs",
      "public/app-modern/index.html",
      "public/app-modern/assets/index-Dr3-HBhG.css",
      "public/app-modern/assets/index-Cx8c83Z1.js",
    ]) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }

    console.log("=== restart pm2 ===");
    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 restart davidsklad --update-env",
      "sleep 15",
      "pm2 list",
      "free -h | head -2",
    ].join(" && "));

    console.log("=== health check ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
