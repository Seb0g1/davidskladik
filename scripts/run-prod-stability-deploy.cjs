#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const files = [
  "server.js",
  "api-entry.js",
  "worker-entry.js",
  "ecosystem.config.cjs",
  "package.json",
  "package-lock.json",
  "routes/auth-session.js",
  "routes/marketplaces.js",
  "routes/operations.js",
  "routes/settings.js",
  "routes/static-app.js",
  "routes/system-media.js",
  "routes/users.js",
  "lib/logger.js",
  "lib/postgres.js",
  "lib/static-app.js",
  "scripts/prod-post-deploy-check.cjs",
  "scripts/prod-alert-on-failure.cjs",
  "scripts/inspect-bullmq-failed-jobs.cjs",
  "scripts/setup-prod-monitoring.cjs",
];

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

function openSftp(conn) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => (err ? reject(err) : resolve(sftp)));
  });
}

function sftpPut(sftp, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    const read = fs.createReadStream(localPath);
    const write = sftp.createWriteStream(remotePath);
    write.on("close", resolve);
    write.on("error", reject);
    read.on("error", reject);
    read.pipe(write);
  });
}

async function withConn(run) {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 90000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    });
  });
  try {
    return await run(conn);
  } finally {
    conn.end();
  }
}

async function main() {
  await withConn(async (conn) => {
    await exec(conn, `mkdir -p ${remoteRoot}/routes ${remoteRoot}/lib ${remoteRoot}/scripts`);
    const sftp = await openSftp(conn);
    for (const rel of files) {
      const local = path.join(root, rel);
      if (!fs.existsSync(local)) throw new Error(`Missing ${rel}`);
      console.log(`upload ${rel}`);
      await sftpPut(sftp, local, `${remoteRoot}/${rel}`);
    }
  });

  await withConn(async (conn) => {
    await exec(conn, [
      `cd ${remoteRoot}`,
      "npm ci --omit=dev",
      "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "pm2 save",
      "sleep 25",
      "pm2 list",
      "node scripts/prod-post-deploy-check.cjs",
    ].join(" && "));
  });
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
