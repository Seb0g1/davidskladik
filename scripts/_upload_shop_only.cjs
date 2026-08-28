#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const fs = require("fs");
const path = require("path");

const password = process.env.DEPLOY_PASSWORD || "pm^e7-jVL_gAyM";
const shopDist = path.join(__dirname, "../shop/dist");
const remoteBase = "/var/www/magicvibes";

function openSftp(conn) {
  return new Promise((resolve, reject) => conn.sftp((e, s) => (e ? reject(e) : resolve(s))));
}
function sftpPut(sftp, local, remote) {
  return new Promise((resolve, reject) => {
    const r = fs.createReadStream(local);
    const w = sftp.createWriteStream(remote);
    w.on("close", resolve); w.on("error", reject); r.on("error", reject);
    r.pipe(w);
  });
}
function walkDir(dir) {
  const result = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) result.push(...walkDir(full)); else result.push(full);
  }
  return result;
}
function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", d => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", () => {});
      stream.on("close", code => code ? reject(new Error("exit " + code)) : resolve(out));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password,
      readyTimeout: 30000,
    })
  );
  try {
    const files = walkDir(shopDist);
    const dirs = new Set([remoteBase]);
    for (const f of files) {
      const rel = path.relative(shopDist, path.dirname(f)).split(path.sep).join("/");
      if (rel && rel !== ".") dirs.add(remoteBase + "/" + rel);
    }
    await exec(conn, "mkdir -p " + Array.from(dirs).join(" "));
    console.log("✓ dirs ok");
    const sftp = await openSftp(conn);
    for (const local of files) {
      const rel = path.relative(shopDist, local).split(path.sep).join("/");
      await sftpPut(sftp, local, remoteBase + "/" + rel);
      process.stdout.write(".");
    }
    console.log("\n✓ uploaded " + files.length + " files");
    console.log("✓ DONE — nginx not touched");
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message); process.exit(1); });
