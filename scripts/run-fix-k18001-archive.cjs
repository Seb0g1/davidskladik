#!/usr/bin/env node
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
function exec(conn, command, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("exec timeout")), timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => { clearTimeout(timer); resolve(); });
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
    conn.on("ready", resolve).on("error", reject).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
  });
  try {
    await sftpPut(conn, path.join(root, "scripts/fix-k18001-archive-ozon.cjs"), `${remoteRoot}/scripts/fix-k18001-archive-ozon.cjs`);
    await exec(conn, `cd ${remoteRoot} && node scripts/fix-k18001-archive-ozon.cjs`, 60000);
  } finally { conn.end(); }
}
main().catch((e) => { console.error(e.message); process.exit(1); });
