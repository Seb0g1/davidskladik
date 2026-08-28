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
    await exec(conn, [
      "pm2 list",
      "echo '=== ERROR LOG (today) ==='",
      "grep '2026-06-07' /root/.pm2/logs/davidsklad-error-0.log | tail -n 50",
      "echo '=== ERROR LOG (last 50 lines) ==='",
      "tail -n 50 /root/.pm2/logs/davidsklad-error-0.log",
      "echo '=== OUT LOG errors/warns (last 300) ==='",
      "tail -n 500 /root/.pm2/logs/davidsklad-out-0.log | grep -E '\"level\":\"(error|warn)\"|Error:|error|failed|ECONN|timeout|Unhandled' | tail -n 150",
      "echo '=== RECENT REQUEST ERRORS ==='",
      "tail -n 1000 /root/.pm2/logs/davidsklad-out-0.log | grep -E 'request error|slow request|failed' | tail -n 80",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
