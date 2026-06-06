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
    });
  });

  try {
    console.log("Deploying server.js...");
    await sftpPut(conn, path.join(root, "server.js"), `${remoteRoot}/server.js`);

    console.log("Deploying frontend bundle...");
    const files = [
      "public/app-modern/index.html",
      "public/app-modern/assets/index-CzH8Z1Ju.css",
      "public/app-modern/assets/index-CN5d-JyS.js",
    ];
    for (const rel of files) {
      await sftpPut(conn, path.join(root, rel), `${remoteRoot}/${rel}`);
    }

    await exec(conn, [
      `cd ${remoteRoot}`,
      "pm2 restart davidsklad --update-env",
      "sleep 8",
      "pm2 list",
      "free -h | head -2",
      "curl -sS -m 30 'http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=40' | node -e \"let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>{const j=JSON.parse(s);const linked=(j.items||[]).filter(i=>(i.links||[]).length>0);const withSupplier=linked.filter(i=>i.selectedSupplier||i.stockOnlyFallbackActive);console.log(JSON.stringify({partial:j.partial,items:j.items?.length,linked:linked.length,withSupplier:withSupplier.length,sample:withSupplier.slice(0,2).map(i=>({id:i.id,supplier:!!i.selectedSupplier,ready:i.ready}))},null,2));});\"",
    ].join(" && "));
  } finally {
    conn.end();
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
