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
      fs.createReadStream(localPath).pipe(sftp.createWriteStream(remotePath)).on("close", resolve).on("error", reject);
    });
  });
}

async function main() {
  const scripts = [
    "server.js",
    "scripts/audit-auto-pair-full.cjs",
    "scripts/audit-warehouse-catalog-health.cjs",
    "scripts/audit-linked-missing-pm.cjs",
    "scripts/audit-marketplace-labels.cjs",
    "scripts/prod-catalog-pair-check.cjs",
  ];
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
    for (const rel of scripts) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    await exec(conn, [
      `cd ${remoteRoot}`,
      "echo '=== AUTO PAIR FULL ==='",
      "node scripts/audit-auto-pair-full.cjs",
      "echo '=== CATALOG HEALTH FULL ==='",
      "node scripts/audit-warehouse-catalog-health.cjs --limit=35316",
      "echo '=== LINKED MISSING PM ==='",
      "node scripts/audit-linked-missing-pm.cjs --limit=5000",
      "echo '=== MARKETPLACE LABELS ==='",
      "node scripts/audit-marketplace-labels.cjs --limit=500",
      "echo '=== SIBLING FETCH ==='",
      "node scripts/prod-catalog-pair-check.cjs",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
