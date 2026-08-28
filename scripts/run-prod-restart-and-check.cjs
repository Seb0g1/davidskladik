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
    await exec(conn, [
      `cd ${remoteRoot}`,
      "node -e \"const fs=require('fs');const p='data/daily-sync.json';try{const s=JSON.parse(fs.readFileSync(p,'utf8'));if(s.status==='running'){s.status='ok';s.running=false;fs.writeFileSync(p,JSON.stringify(s,null,2));console.log('daily-sync state reset');}else{console.log('daily-sync status',s.status);}}catch(e){console.log('daily-sync skip',e.message);}\"",
      "pm2 restart davidsklad --update-env",
      "sleep 12",
      "pm2 list",
    ].join(" && "));
    await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
    await sftpPut(conn, path.join(__dirname, "prod-post-deploy-check.cjs"), `${remoteRoot}/scripts/prod-post-deploy-check.cjs`);
    await exec(conn, `cd ${remoteRoot} && node scripts/prod-post-deploy-check.cjs`);
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
