#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const files = [
  "server.js",
  "test/smoke.test.cjs",
  "public/app.js",
  "scripts/_prod-trigger-reprice-remote.cjs",
  "scripts/_prod-monitor-progress.cjs",
];

function collectBuiltFrontendFiles() {
  const modernRoot = path.join(root, "public", "app-modern");
  const out = [];
  const walk = (dir, prefix = "") => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const rel = path.posix.join(prefix, entry.name);
      const abs = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(abs, rel);
      else out.push({ local: abs, remote: `${remoteRoot}/public/app-modern/${rel.replace(/\\/g, "/")}` });
    }
  };
  if (fs.existsSync(modernRoot)) walk(modernRoot);
  return out;
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

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(localPath)
        .pipe(sftp.createWriteStream(remotePath))
        .on("close", resolve)
        .on("error", reject);
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
    await exec(conn, `mkdir -p ${remoteRoot}/scripts ${remoteRoot}/test`);
    for (const rel of files) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }
    for (const item of collectBuiltFrontendFiles()) {
      await exec(conn, `mkdir -p ${path.posix.dirname(item.remote)}`);
      await sftpPut(conn, item.local, item.remote);
    }
    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "sleep 12",
      "pm2 env davidsklad-worker 2>/dev/null | grep BACKGROUND_JOBS || true",
      "node scripts/_prod-trigger-reprice-remote.cjs",
      "node scripts/_prod-monitor-progress.cjs",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
