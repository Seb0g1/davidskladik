#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const fs = require("fs");
const path = require("path");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_diag3_run.js";

const localScript = path.join(__dirname, "_tmp_diag3_body.js");
fs.writeFileSync(localScript, `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { execSync } = require("node:child_process");
const { Queue } = require("bullmq");
const { PrismaClient } = require("@prisma/client");

async function main() {
  // PM2 статус processes
  try {
    const st = JSON.parse(execSync("pm2 jlist 2>&1").toString());
    for (const p of st) {
      const heapMb = Math.round((p.monit?.memory || 0) / 1024 / 1024);
      const uptime = Math.round((Date.now() - p.pm2_env?.pm_uptime) / 1000 / 60);
      console.log("PM2 [" + p.name + "] status=" + p.pm2_env?.status + " restarts=" + p.pm2_env?.restart_time + " uptime=" + uptime + "min RAM=" + heapMb + "MB");
    }
  } catch (e) { console.log("pm2 jlist error:", e.message); }

  // Worker последние логи (только WB-related)
  try {
    const logs = execSync("pm2 logs davidsklad-worker --nostream --lines 100 2>&1").toString();
    const lines = logs.split("\\n").filter(l => l.includes("wb") || l.includes("sync") || l.includes("deferred") || l.includes("429") || l.includes("rate"));
    console.log("\\n=== Worker WB/sync logs (last 100 lines filtered) ===");
    lines.slice(-30).forEach(l => console.log(l.slice(0, 300)));
  } catch (e) { console.log("pm2 logs error:", e.message.slice(0, 100)); }

  // WB sync status file
  try {
    const f = require("fs").readFileSync("/tmp/wb-sync-status.json", "utf8");
    console.log("\\n=== WB sync status file ===");
    console.log(f);
  } catch (e) { console.log("No WB sync status file:", e.message); }

  // BullMQ queue state
  const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const url = new URL(redisUrl);
  const connection = { host: url.hostname, port: Number(url.port) || 6379, password: url.password || undefined };
  const queue = new Queue("marketplace-tasks", { connection });
  try {
    const counts = await queue.getJobCounts("waiting", "active", "completed", "failed");
    console.log("\\n=== BullMQ counts ===", JSON.stringify(counts));
    const active = await queue.getActive();
    for (const job of active) {
      const age = Math.round((Date.now() - job.timestamp) / 1000 / 60);
      console.log("ACTIVE job: name=" + job.name + " id=" + job.id + " age=" + age + "min source=" + job.data?.source);
    }
    const waiting = await queue.getWaiting(0, 5);
    for (const job of waiting) {
      console.log("WAITING job: name=" + job.name + " id=" + job.id + " source=" + job.data?.source);
    }
  } finally {
    await queue.close();
  }

  // DB: Как продукты в складе соотносятся с WB vendor codes
  const prisma = new PrismaClient();
  try {
    const total = await prisma.warehouseProduct.count({ where: { marketplace: "ozon" } });
    const archived = await prisma.warehouseProduct.count({ where: { marketplace: "ozon", archived: true } });
    console.log("\\n=== DB stats ===");
    console.log("Total Ozon products:", total, "| archived:", archived, "| active:", total - archived);

    // Посмотрим на createdAt - когда добавлялись товары
    const byDate = await prisma.$queryRawUnsafe(
      "SELECT DATE(created_at) as day, COUNT(*) as cnt FROM warehouse_products WHERE marketplace = 'ozon' GROUP BY DATE(created_at) ORDER BY day DESC LIMIT 10"
    );
    console.log("\\nProducts added by date (last 10 days):");
    for (const row of byDate) {
      console.log("  " + row.day + ": " + row.cnt + " products");
    }
  } finally {
    await prisma.$disconnect();
  }
}
main().catch(e => { console.error("ERROR:", e.message); process.exit(1); });
`);

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const rs = sftp.createWriteStream(remoteScript);
    rs.on("close", () => {
      fs.unlinkSync(localScript);
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => {
          conn.exec("rm -f " + remoteScript, () => conn.end());
        });
      });
    });
    sftp.fastPut(localScript, remoteScript, {}, (err3) => {
      if (err3) { console.error("sftp error:", err3); conn.end(); }
    });
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
