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
      const read = fs.createReadStream(localPath);
      const write = sftp.createWriteStream(remotePath);
      write.on("close", resolve);
      write.on("error", reject);
      read.on("error", reject);
      read.pipe(write);
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
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    const files = [
      "server.js",
      "scripts/materialize-yandex-exported-products-postgres.cjs",
      "scripts/sync-ozon-yandex-link-pairs-postgres.cjs",
      "scripts/backfill-ozon-yandex-pair-groups.cjs",
      "scripts/migrate-warehouse-canonical-ids.cjs",
      "scripts/audit-warehouse-catalog-health.cjs",
      "scripts/audit-auto-pair-full.cjs",
      "scripts/sync-ozon-manual-groups-from-yandex.cjs",
    ];
    for (const rel of files) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    await exec(conn, [
      `cd ${remoteRoot}`,
      "node scripts/sync-ozon-yandex-link-pairs-postgres.cjs --dry-run",
      "node scripts/sync-ozon-yandex-link-pairs-postgres.cjs --apply",
      "node scripts/backfill-ozon-yandex-pair-groups.cjs --dry-run",
      "node scripts/backfill-ozon-yandex-pair-groups.cjs --apply",
      "node scripts/materialize-yandex-exported-products-postgres.cjs --dry-run",
      "node scripts/materialize-yandex-exported-products-postgres.cjs --apply",
      "node scripts/sync-ozon-manual-groups-from-yandex.cjs --dry-run",
      "node scripts/materialize-yandex-exported-products-postgres.cjs --apply",
      "node scripts/audit-auto-pair-full.cjs",
      "node scripts/audit-warehouse-catalog-health.cjs --limit=35316",
      "pm2 restart davidsklad --update-env",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
