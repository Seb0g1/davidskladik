#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

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
    await exec(conn, [
      "cd /var/www/davidsklad/davidskladik",
      "echo '=== recent pm2 out (pipeline keywords) ==='",
      "pm2 logs davidsklad --lines 200 --nostream --out 2>/dev/null | grep -E 'backfill|delete-yandex|repair-yandex|pipeline complete|audit-auto-pair' | tail -20 || true",
      "echo '=== script mtimes ==='",
      "ls -lt scripts/backfill-ozon-yandex-pair-groups.cjs scripts/delete-yandex-small-volume.cjs scripts/repair-yandex-media-from-ozon.cjs 2>/dev/null || true",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
