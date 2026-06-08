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
const withRetry = process.argv.includes("--retry");
const withRemove = process.argv.includes("--remove-failed");
const limit = process.argv.find((arg) => arg.startsWith("--limit="))?.split("=")[1] || "120";

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
    await sftpPut(
      conn,
      path.join(root, "scripts/inspect-bullmq-failed-jobs.cjs"),
      `${remoteRoot}/scripts/inspect-bullmq-failed-jobs.cjs`,
    );
    const flags = [withRetry ? "--retry" : "", withRemove ? "--remove-failed" : ""].filter(Boolean).join(" ");
    await exec(conn, `cd ${remoteRoot} && node scripts/inspect-bullmq-failed-jobs.cjs --limit=${limit} ${flags}`.trim());
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
