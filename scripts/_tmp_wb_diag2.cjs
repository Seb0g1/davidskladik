#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_diag2_run.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { execSync } = require("node:child_process");
const { Queue } = require("bullmq");

async function main() {
  // Последние 20 строк worker без фильтра
  try {
    const logs = execSync("pm2 logs davidsklad-worker --nostream --lines 50 2>&1").toString();
    const lines = logs.split("\\n").filter(l => l.trim());
    console.log("=== Последние 20 строк worker ===");
    lines.slice(-20).forEach(l => console.log(l.slice(0, 250)));
  } catch (e) {
    console.log("pm2 logs error:", e.message.slice(0, 100));
  }

  // PM2 статус
  try {
    const st = JSON.parse(execSync("pm2 jlist 2>&1").toString());
    const w = st.find(p => p.name.includes("worker"));
    if (w) {
      const heapMb = Math.round((w.monit?.memory || 0) / 1024 / 1024);
      console.log("\\n=== Worker status ===");
      console.log("status:", w.pm2_env?.status, "| restarts:", w.pm2_env?.restart_time, "| uptime:", Math.round((Date.now() - w.pm2_env?.pm_uptime) / 1000), "сек | RAM:", heapMb, "MB");
    }
  } catch (e) { console.log("pm2 jlist error:", e.message); }

  // BullMQ active
  const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const url = new URL(redisUrl);
  const connection = { host: url.hostname, port: Number(url.port) || 6379, password: url.password || undefined };
  const queue = new Queue("marketplace-tasks", { connection });
  try {
    const counts = await queue.getJobCounts("waiting", "active", "completed", "failed");
    console.log("\\n=== BullMQ counts ===", JSON.stringify(counts));
    const active = await queue.getActive();
    for (const job of active) {
      console.log("active:", job.name, "| id:", job.id, "| data.source:", job.data?.source, "| data.productIds:", job.data?.productIds?.length);
    }
  } finally {
    await queue.close();
  }
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
