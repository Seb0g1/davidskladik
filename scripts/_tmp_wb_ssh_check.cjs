#!/usr/bin/env node
"use strict";

// Read-only проверка состояния цепочки WB на проде: процессы, state-файл, лог.
// Использование: DEPLOY_PASSWORD=... node scripts/_tmp_wb_ssh_check.cjs [extra-command]

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
    const extra = process.argv[2];
    const sections = [
      ["=== chain processes ===", "pgrep -af 'prod-wb-chain' || echo 'none'"],
      ["=== state file ===", `head -c 3000 ${remoteRoot}/data/wb-chain-result.json 2>/dev/null || echo 'no file'`],
      ["=== chain log tail ===", `tail -30 ${remoteRoot}/data/wb-chain.log 2>/dev/null || echo 'no log'`],
    ];
    for (const [title, cmd] of sections) {
      console.log(`\n${title}`);
      await exec(conn, cmd);
    }
    if (extra) {
      console.log(`\n=== extra: ${extra} ===`);
      await exec(conn, extra);
    }
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
