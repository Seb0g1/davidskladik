#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(localPath).pipe(sftp.createWriteStream(remotePath))
        .on("close", resolve).on("error", reject);
    });
  });
}

function exec(conn, command, timeoutMs = 120000) {
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

async function main() {
  const conn = new Client();
  await new Promise((res, rej) => conn.on("ready", res).on("error", rej).connect({
    host: "81.17.154.153", username: "root", password, readyTimeout: 30000,
    keepaliveInterval: 10000, keepaliveCountMax: 60,
  }));
  try {
    const localScript = path.join(__dirname, "_tmp_stock_sent_diag_remote.cjs");
    const remoteScript = `${remoteRoot}/scripts/_tmp_stock_sent_diag_remote.cjs`;
    console.log("Uploading...");
    await sftpPut(conn, localScript, remoteScript);
    console.log("Running...\n");
    await exec(conn, `cd ${remoteRoot} && node scripts/_tmp_stock_sent_diag_remote.cjs`, 120000);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
