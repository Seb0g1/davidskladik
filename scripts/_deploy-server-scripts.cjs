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

const deployFiles = [
  "server.js",
  "scripts/backfill-ozon-yandex-pair-groups.cjs",
  "scripts/sync-ozon-yandex-link-pairs-postgres.cjs",
  "scripts/delete-yandex-small-volume.cjs",
  "scripts/repair-yandex-media-from-ozon.cjs",
  "scripts/audit-auto-pair-full.cjs",
  "scripts/audit-warehouse-catalog-health.cjs",
  "scripts/audit-marketplace-labels.cjs",
  "scripts/prod-catalog-pair-check.cjs",
  "scripts/prod-post-deploy-check.cjs",
  "scripts/repair-linked-warehouse-catalog.cjs",
  "scripts/dedupe-warehouse-products.cjs",
  "scripts/fix-offer-zero-stock.cjs",
  "scripts/repair-yandex-media-from-ozon.cjs",
];

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}: ${command}`)) : resolve()));
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

async function connect() {
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
  return conn;
}

async function uploadOne(rel) {
  const local = path.join(root, rel);
  if (!fs.existsSync(local)) {
    console.warn(`skip missing: ${rel}`);
    return;
  }
  const conn = await connect();
  try {
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    console.log(`upload ${rel}`);
    await sftpPut(conn, local, `${remoteRoot}/${rel}`);
  } finally {
    conn.end();
  }
}

async function main() {
  const restart = process.argv.includes("--restart");
  const onlyScripts = deployFiles.filter((rel) => rel !== "server.js" || !restart);
  for (const rel of onlyScripts) {
    await uploadOne(rel);
  }
  if (restart) {
    const conn = await connect();
    try {
      console.log("pm2 restart davidsklad...");
      await exec(conn, [
        `cd ${remoteRoot}`,
        "pm2 restart davidsklad --update-env",
        "sleep 12",
        "pm2 list",
      ].join(" && "));
    } finally {
      conn.end();
    }
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
