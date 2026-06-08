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

const apply = process.argv.includes("--apply");
const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const mode = apply ? "--apply" : "--dry-run";

const deployFiles = [
  "server.js",
  "api-entry.js",
  "worker-entry.js",
  "ecosystem.config.cjs",
  "scripts/backfill-ozon-yandex-pair-groups.cjs",
  "scripts/sync-ozon-yandex-link-pairs-postgres.cjs",
  "scripts/delete-yandex-small-volume.cjs",
  "scripts/repair-yandex-media-from-ozon.cjs",
  "scripts/audit-auto-pair-full.cjs",
  "scripts/audit-warehouse-catalog-health.cjs",
  "scripts/audit-marketplace-labels.cjs",
  "scripts/prod-catalog-pair-check.cjs",
  "scripts/prod-post-deploy-check.cjs",
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

async function step(conn, title, command) {
  console.log(`\n=== ${title} ===`);
  await exec(conn, command);
}

async function withConnection(run) {
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
  await withConnection(async (conn) => {
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    for (const rel of deployFiles) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
  });

  const steps = [
    ["restart pm2", `cd ${remoteRoot} && pm2 delete davidsklad 2>/dev/null || true && pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env && sleep 12`],
    ["post-deploy check", `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`],
    ["audit before", `cd ${remoteRoot} && node scripts/audit-auto-pair-full.cjs && node scripts/audit-warehouse-catalog-health.cjs --limit=50000 && node scripts/prod-catalog-pair-check.cjs`],
    ["backfill ozon-yandex pair groups", `cd ${remoteRoot} && node scripts/backfill-ozon-yandex-pair-groups.cjs ${mode}`],
    ["sync ozon-yandex link pairs", `cd ${remoteRoot} && node scripts/sync-ozon-yandex-link-pairs-postgres.cjs ${mode}`],
    ["delete yandex <20ml", `cd ${remoteRoot} && (test ! -f data/delete-yandex-small-volume.lock && node scripts/delete-yandex-small-volume.cjs ${mode} || echo "skip delete: lock exists")`],
    ["repair yandex photo/brand/dims", `cd ${remoteRoot} && (test ! -f data/repair-yandex-media-from-ozon.lock && node scripts/repair-yandex-media-from-ozon.cjs ${apply ? "--apply --push" : "--dry-run"} || echo "skip repair-yandex: lock exists")`],
    ["audit after", `cd ${remoteRoot} && node scripts/audit-auto-pair-full.cjs && node scripts/audit-warehouse-catalog-health.cjs --limit=50000 && node scripts/prod-catalog-pair-check.cjs && node scripts/audit-marketplace-labels.cjs --limit=200`],
  ];

  for (const [title, command] of steps) {
    await withConnection(async (conn) => {
      await step(conn, title, command);
    });
  }
  console.log("\n=== pipeline complete ===");
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
