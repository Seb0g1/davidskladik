#!/usr/bin/env node
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { Client } = require("ssh2");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const ROOT = path.join(__dirname, "..");
const REMOTE_ROOT = "/var/www/davidsklad/davidskladik";
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const privateKey = fs.existsSync(defaultKeyPath) ? fs.readFileSync(defaultKeyPath) : null;
const sshPassword = process.env.DEPLOY_PASSWORD;
if (!privateKey && !sshPassword) { console.error("No SSH key or DEPLOY_PASSWORD"); process.exit(1); }
function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      let out = ""; let errOut = "";
      stream.on("data", d => out += d);
      stream.stderr.on("data", d => errOut += d);
      stream.on("close", () => resolve({ out, errOut }));
    });
  });
}
function openSftp(conn) {
  return new Promise((resolve, reject) => conn.sftp((err, sftp) => err ? reject(err) : resolve(sftp)));
}
function uploadFile(sftp, local, remote) {
  return new Promise((resolve, reject) => sftp.fastPut(local, remote, err => err ? reject(err) : resolve()));
}
async function uploadDir(sftp, conn, localDir, remoteDir) {
  await exec(conn, `mkdir -p ${remoteDir}`);
  for (const entry of fs.readdirSync(localDir, { withFileTypes: true })) {
    const lp = path.join(localDir, entry.name);
    const rp = `${remoteDir}/${entry.name}`;
    if (entry.isDirectory()) await uploadDir(sftp, conn, lp, rp);
    else await uploadFile(sftp, lp, rp);
  }
}
const SERVER_FILES = [
  "server/parts/02f-supplier-picking-routes.js",
  "server/parts/02d-routes-catalog-report.js",
];

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) cfg.privateKey = privateKey; else cfg.password = sshPassword;
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });
  console.log("SSH connected");
  try {
    const sftp = await openSftp(conn);
    process.stdout.write("  upload public/app-modern ... ");
    await uploadDir(sftp, conn, path.join(ROOT, "public/app-modern"), `${REMOTE_ROOT}/public/app-modern`);
    console.log("OK");
    for (const rel of SERVER_FILES) {
      process.stdout.write(`  upload ${rel} ... `);
      await uploadFile(sftp, path.join(ROOT, rel), `${REMOTE_ROOT}/${rel}`);
      console.log("OK");
    }
    const r = await exec(conn, `cd ${REMOTE_ROOT} && pm2 reload ecosystem.config.cjs --only davidsklad-api 2>&1 | tail -3`);
    console.log("API reload:", r.out.trim() || r.errOut.trim() || "done");
    console.log("Deployed.");
    const st = await exec(conn, "pm2 jlist 2>/dev/null");
    let procs = []; try { procs = JSON.parse(st.out); } catch {}
    for (const p of procs) {
      if (!["davidsklad-api","davidsklad-worker"].includes(p.name)) continue;
      const mem = Math.round((p.monit?.memory || 0) / 1024 / 1024);
      const uptime = Math.round((Date.now() - (p.pm2_env?.pm_uptime || 0)) / 1000);
      console.log(`  ${p.name}: ${p.pm2_env?.status} restarts=${p.pm2_env?.restart_time} uptime=${uptime}s mem=${mem}MB`);
    }
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message || e); process.exit(1); });
