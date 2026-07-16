#!/usr/bin/env node
"use strict";

// Загружает scripts/prod-wb-chain.cjs на прод и запускает его там отдельным
// процессом с большим heap (скан каталога не влезает в память процесса api).
// Использование: DEPLOY_PASSWORD=... node scripts/run-prod-wb-chain.cjs <mode> [args]
//
// Запуск через nohup: обрыв SSH не убивает цепочку. Прогресс пишется на сервере
// в data/wb-chain-result.json (его же отдаёт GET /api/wb/chain/result), здесь
// поллим файл до finishedAt.

const path = require("node:path");
const fs = require("node:fs");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";
const args = process.argv.slice(2).map((value) => `'${String(value).replace(/'/g, "'\\''")}'`).join(" ");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function exec(conn, command, { quiet = false } = {}) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let output = "";
      stream.on("data", (d) => {
        output += d;
        if (!quiet) process.stdout.write(d);
      });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve(output)));
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
      keepaliveInterval: 10000,
      keepaliveCountMax: 60,
    });
  });
  try {
    await sftpPut(conn, path.join(__dirname, "prod-wb-chain.cjs"), `${remoteRoot}/scripts/prod-wb-chain.cjs`);
    // Один запуск за раз: старый процесс цепочки (например, зависший media)
    // конкурировал бы за лимиты WB API. Отдельный exec + [n]-паттерн, чтобы
    // pkill не убил собственный шелл (его cmdline содержит имя скрипта), и вне
    // команды запуска — `& echo` уводит весь &&-список в фоновый сабшелл.
    await exec(conn, "pkill -f 'prod-wb-chai[n].cjs' || true; sleep 1");
    await exec(conn, [
      `cd ${remoteRoot}`,
      "mkdir -p data",
      "rm -f data/wb-chain-result.json",
      `nohup env MALLOC_ARENA_MAX=2 NODE_OPTIONS='--max-old-space-size=4096' WB_REQUEST_MAX_ATTEMPTS=4 node scripts/prod-wb-chain.cjs ${args} > data/wb-chain.log 2>&1 & echo "started pid=$!"`,
    ].join(" && "));

    // Поллим результат до finishedAt (или 110 минут).
    const startedAt = Date.now();
    let lastSteps = 0;
    while (Date.now() - startedAt < 110 * 60 * 1000) {
      await sleep(30000);
      let stateRaw = "";
      try {
        stateRaw = await exec(conn, `cat ${remoteRoot}/data/wb-chain-result.json 2>/dev/null || true`, { quiet: true });
      } catch {
        continue;
      }
      let state = null;
      try { state = JSON.parse(stateRaw); } catch { /* файл пишется в этот момент */ }
      if (!state) {
        // Процесс мог упасть до первой записи — проверяем, жив ли он.
        const alive = await exec(conn, "pgrep -f 'prod-wb-chai[n].cjs' || true", { quiet: true });
        if (!alive.trim()) {
          await exec(conn, `tail -50 ${remoteRoot}/data/wb-chain.log || true`);
          throw new Error("Процесс цепочки не запустился (нет результата и нет процесса)");
        }
        continue;
      }
      if (state.steps.length > lastSteps) {
        for (const step of state.steps.slice(lastSteps)) {
          console.log(`[${step.at}] step=${step.step} ${JSON.stringify(step).slice(0, 600)}`);
        }
        lastSteps = state.steps.length;
      }
      if (state.finishedAt) {
        if (state.error) {
          console.error("Цепочка завершилась с ошибкой:\n" + state.error);
          process.exitCode = 1;
        } else {
          console.log(`Цепочка завершена: ${state.finishedAt} (шагов: ${state.steps.length})`);
        }
        return;
      }
      // Результат есть, но не завершён — проверяем, что процесс ещё жив.
      const alive = await exec(conn, "pgrep -f 'prod-wb-chai[n].cjs' || true", { quiet: true });
      if (!alive.trim()) {
        await exec(conn, `tail -50 ${remoteRoot}/data/wb-chain.log || true`);
        throw new Error("Процесс цепочки умер, не дописав результат (возможно OOM)");
      }
    }
    throw new Error("Таймаут ожидания цепочки (110 минут) — процесс продолжает работать на сервере");
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
