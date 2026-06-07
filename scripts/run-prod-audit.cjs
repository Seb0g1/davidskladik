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
      const read = fs.createReadStream(localPath);
      const write = sftp.createWriteStream(remotePath);
      write.on("close", resolve);
      write.on("error", reject);
      read.on("error", reject);
      read.pipe(write);
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
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    });
  });
  try {
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    await sftpPut(conn, path.join(root, "server.js"), `${remoteRoot}/server.js`);
    await sftpPut(
      conn,
      path.join(root, "scripts/audit-warehouse-catalog-health.cjs"),
      `${remoteRoot}/scripts/audit-warehouse-catalog-health.cjs`,
    );
    await sftpPut(
      conn,
      path.join(root, "scripts/audit-marketplace-labels.cjs"),
      `${remoteRoot}/scripts/audit-marketplace-labels.cjs`,
    );
    await exec(conn, [
      `cd ${remoteRoot}`,
      "node scripts/audit-warehouse-catalog-health.cjs --limit=19000",
      "node scripts/audit-marketplace-labels.cjs --limit=400",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
