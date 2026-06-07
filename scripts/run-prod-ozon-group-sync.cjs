#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

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
      fs.createReadStream(localPath).pipe(sftp.createWriteStream(remotePath)).on("close", resolve).on("error", reject);
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
    for (const rel of [
      "server.js",
      "scripts/sync-ozon-manual-groups-from-yandex.cjs",
      "scripts/audit-warehouse-catalog-health.cjs",
      "scripts/audit-auto-pair-full.cjs",
    ]) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    await exec(conn, [
      `cd ${remoteRoot}`,
      "node scripts/sync-ozon-manual-groups-from-yandex.cjs --dry-run",
      "node scripts/sync-ozon-manual-groups-from-yandex.cjs --apply",
      "node scripts/audit-auto-pair-full.cjs",
      "node scripts/audit-warehouse-catalog-health.cjs --limit=35316",
      "pm2 restart davidsklad --update-env",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
