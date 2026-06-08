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
const withDedupe = process.argv.includes("--with-dedupe");
const withRepairLinked = process.argv.includes("--repair-linked");

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
    console.log("Deploying server.js...");
    await sftpPut(conn, path.join(root, "server.js"), `${remoteRoot}/server.js`);

    if (withDedupe) {
      console.log("Deploying dedupe script...");
      await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
      await sftpPut(
        conn,
        path.join(root, "scripts/dedupe-warehouse-products.cjs"),
        `${remoteRoot}/scripts/dedupe-warehouse-products.cjs`,
      );
      await sftpPut(
        conn,
        path.join(root, "scripts/audit-marketplace-labels.cjs"),
        `${remoteRoot}/scripts/audit-marketplace-labels.cjs`,
      );
    }

    console.log("Deploying frontend bundle...");
    const files = [
      "public/app-modern/index.html",
      "public/app-modern/assets/index-Dr3-HBhG.css",
      "public/app-modern/assets/index-BmIx0D13.js",
    ];
    for (const rel of files) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }

    console.log("Deploying post-deploy check script...");
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    await sftpPut(
      conn,
      path.join(root, "scripts/prod-post-deploy-check.cjs"),
      `${remoteRoot}/scripts/prod-post-deploy-check.cjs`,
    );

    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 restart davidsklad --update-env",
      "sleep 12",
      "pm2 list",
      "free -h | head -2",
      "echo '=== pm2 error log (last 40 lines) ==='",
      "pm2 logs davidsklad --lines 40 --nostream --err || true",
      "echo '=== post-deploy check ==='",
      "node scripts/prod-post-deploy-check.cjs",
    ].join(" && "));

    if (withRepairLinked) {
      console.log("Running linked warehouse catalog repair on server...");
      await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
      await sftpPut(
        conn,
        path.join(root, "scripts/repair-linked-warehouse-catalog.cjs"),
        `${remoteRoot}/scripts/repair-linked-warehouse-catalog.cjs`,
      );
      await exec(conn, `cd ${remoteRoot} && node scripts/repair-linked-warehouse-catalog.cjs --apply`);
    }

    if (withDedupe) {
      console.log("Running warehouse dedupe on server...");
      await exec(conn, [
        `cd ${remoteRoot}`,
        "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=30",
        "for pass in $(seq 1 25); do echo \"Dedupe apply pass $pass/25...\"; node scripts/dedupe-warehouse-products.cjs --apply --limit=3000 || exit 1; done",
        "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=100000",
        "node scripts/audit-marketplace-labels.cjs --limit=400",
      ].join(" && "));
    }
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
