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

const remoteRoot = "/var/www/davidsklad/davidskladik";
const mode = process.argv.includes("--apply") ? "--apply" : "--dry-run";
const scripts = [
  "delete-yandex-small-volume.cjs",
  "repair-yandex-media-from-ozon.cjs",
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
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    for (const script of scripts) {
      await sftpPut(conn, path.join(__dirname, script), `${remoteRoot}/scripts/${script}`);
    }
    console.log("=== delete yandex <20ml ===");
    await exec(conn, `cd ${remoteRoot} && node scripts/delete-yandex-small-volume.cjs ${mode}`);
    console.log("=== repair yandex photo/brand from ozon ===");
    const repairMode = process.argv.includes("--apply")
      ? "--apply --push"
      : "--dry-run";
    await exec(conn, `cd ${remoteRoot} && node scripts/repair-yandex-media-from-ozon.cjs ${repairMode}`);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
