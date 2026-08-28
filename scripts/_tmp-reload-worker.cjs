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
    await sftpPut(conn, path.join(root, "ecosystem.config.cjs"), `${remoteRoot}/ecosystem.config.cjs`);
    console.log("=== pm2 reload worker ===");
    await exec(conn, `cd ${remoteRoot} && pm2 reload ecosystem.config.cjs --only davidsklad-worker --update-env && sleep 12`);
    console.log("\n=== worker env check ===");
    await exec(conn, `cd ${remoteRoot} && pm2 env 1 | grep BACKGROUND_JOBS || true`);
    await exec(conn, `cd ${remoteRoot} && pm2 env 1 | grep SERVER_ROLE || true`);
    console.log("\n=== worker log tail ===");
    await exec(conn, "tail -n 8 /root/.pm2/logs/davidsklad-worker-out-1.log");
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
