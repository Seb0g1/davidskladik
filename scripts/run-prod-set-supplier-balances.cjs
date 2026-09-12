#!/usr/bin/env node
"use strict";
// Загружает скрипт корректировки балансов на прод-сервер и запускает его.
// Использование:
//   node scripts/run-prod-set-supplier-balances.cjs          # dry-run (только показывает)
//   node scripts/run-prod-set-supplier-balances.cjs --run    # применить
//   node scripts/run-prod-set-supplier-balances.cjs --skip-zero --run
// Требует SSH-ключ: ~/.ssh/davidsklad_deploy (или DEPLOY_SSH_KEY env).

const { execSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const defaultKey = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKey = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKey) ? defaultKey : null);
if (!sshKey) {
  console.error("SSH key not found at ~/.ssh/davidsklad_deploy — set DEPLOY_SSH_KEY");
  process.exit(1);
}

const SSH_HOST = "root@81.17.154.153";
const REMOTE_APP = "/var/www/davidsklad/davidskladik";
const LOCAL_SCRIPT = path.join(__dirname, "_set-supplier-balances.cjs");

if (!fs.existsSync(LOCAL_SCRIPT)) {
  console.error("Скрипт не найден:", LOCAL_SCRIPT);
  process.exit(1);
}

const applyFlag = process.argv.includes("--run") ? "--run" : "";
const skipZeroFlag = process.argv.includes("--skip-zero") ? "--skip-zero" : "";

const SSH_OPTS = `-i "${sshKey}" -o StrictHostKeyChecking=no -o ConnectTimeout=30`;

function run(cmd) {
  execSync(cmd, { stdio: "inherit" });
}

try {
  // 1. Копируем скрипт на сервер
  console.log(`\n▶ Копируем скрипт на сервер...`);
  run(`scp ${SSH_OPTS} "${LOCAL_SCRIPT}" ${SSH_HOST}:/tmp/_set-supplier-balances.cjs`);
  console.log("✓ Скрипт загружен\n");

  // 2. Запускаем
  const cmd = `cd ${REMOTE_APP} && node /tmp/_set-supplier-balances.cjs ${applyFlag} ${skipZeroFlag}`.trim();
  console.log(`▶ Запускаем на сервере: ${cmd}\n`);
  run(`ssh ${SSH_OPTS} ${SSH_HOST} "${cmd.replace(/"/g, '\\"')}"`);

  // 3. Чистим
  run(`ssh ${SSH_OPTS} ${SSH_HOST} "rm -f /tmp/_set-supplier-balances.cjs"`);
} catch (err) {
  console.error("\n❌ Ошибка:", err.message || err);
  process.exit(1);
}
