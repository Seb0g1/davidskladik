#!/usr/bin/env node
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => resolve());
    });
  });
}
function sftpPut(conn, local, remote) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(local).pipe(sftp.createWriteStream(remote)).on("close", resolve).on("error", reject);
    });
  });
}
async function connect() {
  const conn = new Client();
  await new Promise((r, j) => conn.on("ready", r).on("error", j).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 }));
  return conn;
}
async function main() {
  const conn = await connect();
  try {
    await sftpPut(conn, path.join(__dirname, "diag-supplier-cart2.cjs"), `${remoteRoot}/scripts/diag-supplier-cart2.cjs`);
    await exec(conn, `cd ${remoteRoot} && node scripts/diag-supplier-cart2.cjs`);
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message); process.exit(1); });
