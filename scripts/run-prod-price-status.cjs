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
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    for (const rel of ["scripts/_prod-price-status.cjs", "scripts/_tmp-yandex-db-counts.cjs"]) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    console.log("=== price sync status ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/_prod-price-status.cjs`);
    console.log("\n=== yandex breakdown ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/_tmp-yandex-db-counts.cjs`);
    console.log("\n=== last worker price batches ===");
    await exec(conn, "grep -E 'immediate auto price push complete|auto_price_push_heartbeat|auto_price_push_stall_guard' /root/.pm2/logs/davidsklad-worker-out-*.log 2>/dev/null | tail -n 12");
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
