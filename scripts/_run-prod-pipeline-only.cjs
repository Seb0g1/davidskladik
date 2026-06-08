#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const remoteRoot = "/var/www/davidsklad/davidskladik";

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

async function step(title, command) {
  console.log(`\n=== ${title} ===`);
  await withConnection((conn) => exec(conn, command));
}

async function main() {
  const steps = [
    ["audit before", `cd ${remoteRoot} && node scripts/audit-auto-pair-full.cjs && node scripts/audit-warehouse-catalog-health.cjs --limit=50000 && node scripts/prod-catalog-pair-check.cjs`],
    ["backfill ozon-yandex pair groups", `cd ${remoteRoot} && node scripts/backfill-ozon-yandex-pair-groups.cjs --apply`],
    ["sync ozon-yandex link pairs", `cd ${remoteRoot} && node scripts/sync-ozon-yandex-link-pairs-postgres.cjs --apply`],
    ["delete yandex <20ml", `cd ${remoteRoot} && node scripts/delete-yandex-small-volume.cjs --apply`],
    ["repair yandex photo/brand/dims", `cd ${remoteRoot} && node scripts/repair-yandex-media-from-ozon.cjs --apply --push`],
    ["audit after", `cd ${remoteRoot} && node scripts/audit-auto-pair-full.cjs && node scripts/audit-warehouse-catalog-health.cjs --limit=50000 && node scripts/prod-catalog-pair-check.cjs && node scripts/audit-marketplace-labels.cjs --limit=200`],
  ];
  for (const [title, command] of steps) {
    await step(title, command);
  }
  console.log("\n=== pipeline complete ===");
}

main().catch((e) => { console.error(e.message); process.exit(1); });
