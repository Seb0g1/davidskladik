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
const remoteScript = "run-prod-linked-full-reprice-remote.cjs";

const uploadFiles = [
  "server.js",
  "ecosystem.config.cjs",
  "scripts/fix-ozon-quarantine-prices.cjs",
  "scripts/run-prod-linked-full-reprice-remote.cjs",
];

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

async function withConn(run) {
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
    return await run(conn);
  } finally {
    conn.end();
  }
}

async function main() {
  await withConn(async (conn) => {
    await exec(conn, [
      "pkill -9 -f 'node scripts/run-all-sync-remote' || true",
      "pkill -9 -f 'node scripts/run-prod-linked-full-reprice-remote' || true",
      "pkill -9 -f 'node scripts/fix-ozon-quarantine-prices' || true",
      "pkill -9 -f 'node scripts/audit-and-repush-prices-remote' || true",
      "pkill -9 -f 'node scripts/run-prod-linked-full-reprice.cjs' || true",
    ].join(" && "));
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    for (const rel of uploadFiles) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    await exec(conn, `cd ${remoteRoot} && pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env && sleep 12`);
    console.log("\n=== linked full reprice + quarantine release ===\n");
    await exec(conn, `cd ${remoteRoot} && NODE_OPTIONS='--max-old-space-size=4096' node scripts/${remoteScript}`);
  });
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
