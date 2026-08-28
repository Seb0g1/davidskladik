#!/usr/bin/env node
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(localPath).pipe(sftp.createWriteStream(remotePath)).on("close", resolve).on("error", reject);
    });
  });
}

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
      host: "81.17.154.153", username: "root", password, readyTimeout: 60000,
    });
  });
  try {
    for (const rel of ["api-entry.js", "worker-entry.js", "scripts/deploy-prod.cjs", "scripts/prod-post-deploy-check.cjs"]) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "pm2 save",
      "sleep 20",
      "pm2 list",
      "curl -s -o /dev/null -w 'api:%{http_code} %{time_total}s\\n' --max-time 15 http://127.0.0.1:3000/api/live-status",
      "curl -s -o /dev/null -w 'worker:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3001/health",
      "node scripts/prod-post-deploy-check.cjs",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
