#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const pw = process.env.DEPLOY_PASSWORD;
if (!pw) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", d => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", code => code ? reject(new Error("exit " + code)) : resolve(out));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password: pw,
      readyTimeout: 30000,
    })
  );
  try {
    console.log("=== PM2 status ===");
    await exec(conn, "pm2 list");

    console.log("\n=== API logs (last 60) ===");
    await exec(conn, "pm2 logs davidsklad-api --nostream --lines 60 2>&1 || true");

    console.log("\n=== nginx status ===");
    await exec(conn, "systemctl is-active nginx && curl -sk -o /dev/null -w '%{http_code}' http://localhost:3000/api/health || echo 'port 3000 down'");
  } finally {
    conn.end();
  }
}
main().catch(e => { console.error(e.message); process.exit(1); });
