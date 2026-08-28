#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

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
      fs.createReadStream(localPath).pipe(sftp.createWriteStream(remotePath))
        .on("close", resolve).on("error", reject);
    });
  });
}

async function connect() {
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
  return conn;
}

async function main() {
  console.log("Uploading and running Yandex markup analysis (read-only, no server changes)...\n");
  const conn = await connect();
  try {
    const localScript = path.join(root, "scripts/analyze-yandex-markups-direct.cjs");
    const remoteScript = `${remoteRoot}/scripts/analyze-yandex-markups-direct.cjs`;
    console.log("Uploading script...");
    await sftpPut(conn, localScript, remoteScript);
    console.log("Running...\n");
    await exec(conn, `cd ${remoteRoot} && node scripts/analyze-yandex-markups-direct.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
