#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const since = process.argv[2] || new Date(Date.now() - 10 * 60_000).toISOString().slice(0, 16);

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
      `echo since=${since}`,
      `grep '${since}' /root/.pm2/logs/davidsklad-error-0.log | grep -E '"level":"(error|warn)"|request error' | tail -n 100`,
      `tail -n 500 /root/.pm2/logs/davidsklad-error-0.log /root/.pm2/logs/davidsklad-out-0.log | grep '${since}' | grep -E '"level":"(error|warn)"|request error|slow request' | tail -n 100`,
    ].join(" ; "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
