#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const remoteRoot = "/var/www/davidsklad/davidskladik";

function exec(conn, command, timeoutMs = 540000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("exec timeout")), timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => {
        clearTimeout(timer);
        code ? reject(new Error(`exit ${code}`)) : resolve();
      });
    });
  });
}

async function withConn(run) {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 30000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 60,
    });
  });
  try { return await run(conn); } finally { conn.end(); }
}

async function main() {
  await withConn(async (conn) => {
    // Kill any running reprice scripts
    await exec(conn, "pkill -9 -f 'run-prod-linked-full-reprice-remote' || true");
    console.log("Running linked full reprice (no pm2 reload, code already deployed)...\n");
    await exec(conn,
      `cd ${remoteRoot} && NODE_OPTIONS='--max-old-space-size=4096' node scripts/run-prod-linked-full-reprice-remote.cjs`,
      540000
    );
  });
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
