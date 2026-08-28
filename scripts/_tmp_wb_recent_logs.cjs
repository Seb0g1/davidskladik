#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_recent_logs_run.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { execSync } = require("node:child_process");

async function main() {
  // Последние 50 строк любых логов worker
  try {
    const logs = execSync("pm2 logs davidsklad-worker --nostream --lines 100 2>&1").toString();
    const lines = logs.split("\\n").filter(l => l.trim());
    console.log("=== Последние 50 строк worker (без фильтра) ===");
    lines.slice(-50).forEach(l => console.log(l.slice(0, 200)));
  } catch (e) {
    // Попробуем по индексу 2 (worker обычно второй процесс)
    try {
      const logs = execSync("pm2 logs 2 --nostream --lines 100 2>&1").toString();
      const lines = logs.split("\\n").filter(l => l.trim());
      console.log("=== Worker (процесс 2) последние 50 строк ===");
      lines.slice(-50).forEach(l => console.log(l.slice(0, 200)));
    } catch (e2) {
      console.log("pm2 logs error:", e2.message.slice(0, 100));
    }
  }

  // pm2 status
  try {
    const status = execSync("pm2 jlist 2>&1").toString();
    const procs = JSON.parse(status);
    console.log("\\n=== PM2 процессы ===");
    for (const p of procs) {
      console.log("name:", p.name, "| status:", p.pm2_env?.status, "| pid:", p.pid, "| restarts:", p.pm2_env?.restart_time, "| uptime:", Math.round((Date.now() - p.pm2_env?.pm_uptime)/1000), "сек");
    }
  } catch (e) { console.log("pm2 status error:", e.message.slice(0, 100)); }
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(script);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
