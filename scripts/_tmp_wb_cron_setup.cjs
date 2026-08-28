#!/usr/bin/env node
"use strict";

// Ставит на проде ежедневный cron цепочки WB: лимит WB — 1000 новых карточек
// в сутки, поэтому досылка ~8k кандидатов идёт порциями раз в день после
// полуночи МСК (сервер в Europe/Moscow). Идемпотентно: не дублирует запись.
// Использование: DEPLOY_PASSWORD=... node scripts/_tmp_wb_cron_setup.cjs

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";
const cronLine = [
  "20 0 * * * ",
  "pgrep -f 'prod-wb-chai[n].cjs' > /dev/null || ",
  `(cd ${remoteRoot} && MALLOC_ARENA_MAX=2 NODE_OPTIONS='--max-old-space-size=4096' WB_REQUEST_MAX_ATTEMPTS=4 `,
  "/usr/bin/node scripts/prod-wb-chain.cjs chain-nomedia >> data/wb-chain-cron.log 2>&1)",
].join("");

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
    const script = [
      "if crontab -l 2>/dev/null | grep -q 'prod-wb-chain.cjs'; then",
      "  echo 'cron already installed:';",
      "else",
      `  (crontab -l 2>/dev/null; echo "${cronLine.replace(/"/g, '\\"')}") | crontab -;`,
      "  echo 'cron installed:';",
      "fi;",
      "crontab -l | grep 'prod-wb-chain.cjs'",
    ].join(" ");
    await exec(conn, script);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
