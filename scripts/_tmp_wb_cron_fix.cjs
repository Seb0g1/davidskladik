#!/usr/bin/env node
"use strict";

// Чинит ночной cron цепочки WB (одобрено пользователем 2026-07-17):
// старая строка в crontab никогда не запускалась — pgrep-guard матчил cmdline
// собственного `sh -c` (в нём есть незаэкранированное имя скрипта в хвосте
// команды). Guard переехал в scripts/wb-chain-cron.sh; расписание — 03:20 и
// 15:20 МСК (лимит 1000 карточек/сутки НЕ сбрасывается в полночь МСК:
// в 01:09 МСК 17.07 WB всё ещё отвечал «daily limit used up»).
// Использование: DEPLOY_PASSWORD=... node scripts/_tmp_wb_cron_fix.cjs

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";
const cronScript = `${remoteRoot}/scripts/wb-chain-cron.sh`;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve(out)));
    });
  });
}

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      const read = fs.createReadStream(localPath);
      const write = sftp.createWriteStream(remotePath);
      write.on("close", resolve);
      write.on("error", reject);
      read.on("error", reject);
      read.pipe(write);
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
    await sftpPut(conn, path.join(__dirname, "wb-chain-cron.sh"), cronScript);
    await exec(conn, `chmod +x ${cronScript} && echo 'uploaded + chmod ok'`);
    // Старую строку (20 0) убираем, новые (03:20 и 15:20 МСК) добавляем.
    const script = [
      `(crontab -l 2>/dev/null | grep -v 'prod-wb-chain.cjs chain-nomedia'; ` +
      `echo "20 3 * * * /bin/sh ${cronScript}"; ` +
      `echo "20 15 * * * /bin/sh ${cronScript}") | crontab -`,
      "echo '--- crontab wb entries now:'",
      "crontab -l | grep -n 'wb-chain\\|prod-wb-chain'",
    ].join(" && ");
    await exec(conn, script);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
