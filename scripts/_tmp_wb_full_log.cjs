#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_full_log_run.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { execSync } = require("node:child_process");
const { Queue } = require("bullmq");

async function main() {
  // 1. PM2 логи - более широкий фильтр
  try {
    const logs = execSync("pm2 logs --nostream --lines 1000 2>&1").toString();
    const lines = logs.split("\\n").filter(l =>
      l.includes("wb_sync") || l.includes("wb sync") || l.includes("wb-marketplace-sync") ||
      l.includes("wbSet") || l.includes("wb_prices") || l.includes("wb prices") ||
      l.includes("wb stalled") || l.includes("job failed")
    );
    console.log("=== WB-связанные логи (все, последние 30) ===");
    lines.slice(-30).forEach(l => console.log(l.slice(0, 300)));
  } catch (e) { console.log("pm2 logs error:", e.message.slice(0, 100)); }

  // 2. BullMQ - статус очереди marketplace-tasks
  const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const url = new URL(redisUrl);
  const connection = {
    host: url.hostname,
    port: Number(url.port) || 6379,
    password: url.password || undefined,
    tls: url.protocol === "rediss:" ? {} : undefined,
  };
  const queue = new Queue("marketplace-tasks", { connection });
  try {
    const counts = await queue.getJobCounts("waiting", "active", "completed", "failed", "delayed");
    console.log("\\n=== BullMQ marketplace-tasks счётчики ===");
    console.log(JSON.stringify(counts, null, 2));

    // Последние failed jobs
    const failed = await queue.getFailed(0, 10);
    if (failed.length) {
      console.log("\\n=== Failed jobs (последние 10) ===");
      for (const job of failed) {
        console.log("name:", job.name, "| failedReason:", job.failedReason?.slice(0, 100), "| data:", JSON.stringify(job.data));
      }
    }

    // Active jobs
    const active = await queue.getActive();
    console.log("\\n=== Active jobs (" + active.length + ") ===");
    for (const job of active) {
      console.log("name:", job.name, "| data:", JSON.stringify(job.data), "| id:", job.id);
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
