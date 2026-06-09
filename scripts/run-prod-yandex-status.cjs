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
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve(out)));
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
    await sftpPut(conn, path.join(root, "scripts/_tmp-yandex-db-counts.cjs"), `${remoteRoot}/scripts/_tmp-yandex-db-counts.cjs`);
    await sftpPut(conn, path.join(root, "scripts/_prod-monitor-progress.cjs"), `${remoteRoot}/scripts/_prod-monitor-progress.cjs`);
    console.log("=== yandex db counts ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/_tmp-yandex-db-counts.cjs`);
    console.log("\n=== recent batches (ozon/yandex sends) ===");
    await exec(conn, "grep 'immediate auto price push complete' /root/.pm2/logs/davidsklad-worker-out-3.log | tail -n 15");
    console.log("\n=== monitor ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/_prod-monitor-progress.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
