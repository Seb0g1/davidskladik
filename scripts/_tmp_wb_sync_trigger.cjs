#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const DEPLOY_PASSWORD = process.env.DEPLOY_PASSWORD;
if (!DEPLOY_PASSWORD) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD;
if (!APP_PASSWORD) { console.error("APP_PASSWORD required"); process.exit(1); }

const conn = new Client();

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => { process.stdout.write(d); out += d.toString(); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => code ? reject(new Error(`exit ${code}`)) : resolve(out));
    });
  });
}

async function main() {
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root",
      password: DEPLOY_PASSWORD, readyTimeout: 30000,
    })
  );

  try {
    console.log("\n🚀 Running WB sync...");
    await exec(conn, `
      curl -sc /tmp/wb_jar.txt -X POST http://localhost:3000/api/login \
        -H 'Content-Type: application/json' \
        -d '{"username":"${APP_USER}","password":"${APP_PASSWORD}"}' -o /dev/null -w "login: %{http_code}\\n"
      echo "Triggering sync..."
      curl -sb /tmp/wb_jar.txt -X POST http://localhost:3000/api/wb/sync/run \
        -H 'Content-Type: application/json' \
        -w "\\nHTTP %{http_code}\\n"
      echo ""
      echo "Status after:"
      curl -sb /tmp/wb_jar.txt http://localhost:3000/api/wb/sync/status \
        -w "\\nHTTP %{http_code}\\n"
      rm -f /tmp/wb_jar.txt
    `);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
