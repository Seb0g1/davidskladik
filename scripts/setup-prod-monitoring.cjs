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
const cronLine = "*/5 * * * * cd /var/www/davidsklad/davidskladik && /usr/bin/node scripts/prod-post-deploy-check.cjs >> /var/log/davidsklad-health.log 2>&1";

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
      path.join(root, "scripts/prod-post-deploy-check.cjs"),
      `${remoteRoot}/scripts/prod-post-deploy-check.cjs`,
    );

    await exec(conn, [
      "touch /var/log/davidsklad-health.log",
      `(crontab -l 2>/dev/null | grep -v 'prod-post-deploy-check.cjs'; echo '${cronLine}') | crontab -`,
      "crontab -l | grep prod-post-deploy-check || true",
      "pm2 install pm2-logrotate || true",
      "pm2 set pm2-logrotate:max_size 50M || true",
      "pm2 set pm2-logrotate:retain 14 || true",
      "pm2 set pm2-logrotate:compress true || true",
      "pm2 save || true",
    ].join(" && "));
    console.log("Monitoring cron + pm2-logrotate configured.");
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
