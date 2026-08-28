#!/usr/bin/env node
"use strict";

// Read-only мониторинг прода после деплоя: ошибки pm2, health, состояние
// очередей разархивации и алертов. Ничего не меняет на сервере.

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", () => resolve());
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
    const sections = [
      ["=== pm2 list ===", "pm2 list | head -12"],
      ["=== api errors (last 30) ===", "pm2 logs davidsklad-api --lines 30 --nostream --err | tail -35"],
      ["=== worker errors (last 30) ===", "pm2 logs davidsklad-worker --lines 30 --nostream --err | tail -35"],
      ["=== worker out: unarchive/probe/avito/sweep (last 40) ===", "pm2 logs davidsklad-worker --lines 800 --nostream --out 2>/dev/null | grep -iE 'quota_probe|unarchive queue|avito feed|stock_sweep_complete|health_alert_check|daily sync' | tail -40"],
      ["=== alert state ===", `cat ${remoteRoot}/data/last-prod-alert.json`],
      ["=== ozon unarchive daily ===", `cat ${remoteRoot}/data/ozon-unarchive-daily.json`],
      ["=== health log tail ===", "tail -6 /var/log/davidsklad-health.log | grep -E 'failures|ok' || tail -3 /var/log/davidsklad-health.log"],
    ];
    for (const [title, cmd] of sections) {
      console.log(`\n${title}`);
      await exec(conn, cmd);
    }
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
