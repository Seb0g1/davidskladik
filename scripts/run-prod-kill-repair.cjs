#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

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
      "echo 'before:'",
      "ps aux | grep -E 'node scripts/(repair-yandex|delete-yandex)' | grep -v grep || true",
      "pkill -9 -f 'node scripts/repair-yandex-media-from-ozon' || true",
      "pkill -9 -f 'node scripts/delete-yandex-small-volume' || true",
      "sleep 2",
      "rm -f /var/www/davidsklad/davidskladik/data/repair-yandex-media-from-ozon.lock",
      "rm -f /var/www/davidsklad/davidskladik/data/delete-yandex-small-volume.lock",
      "echo 'after:'",
      "ps aux | grep -E 'node scripts/(repair-yandex|delete-yandex)' | grep -v grep || echo 'all killed'",
      "free -h | head -2",
      "cd /var/www/davidsklad/davidskladik && pm2 restart davidsklad --update-env",
      "sleep 8",
      "pm2 list",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
