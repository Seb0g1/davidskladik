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
const remoteScriptName = "run-all-sync-remote.cjs";
const localRunner = path.join(__dirname, "run-all-sync-remote.cjs");

function exec(conn, command, { allowFail = false } = {}) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => {
        if (code && !allowFail) return reject(new Error(`exit ${code}`));
        resolve(code);
      });
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
  if (!fs.existsSync(localRunner)) {
    console.error(`Missing ${localRunner}`);
    process.exit(1);
  }

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
    await sftpPut(conn, localRunner, `${remoteRoot}/scripts/${remoteScriptName}`);
    console.log("Starting all production synchronizations...\n");
    await exec(conn, `cd ${remoteRoot} && node scripts/${remoteScriptName}`);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
