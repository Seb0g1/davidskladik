#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_trigger_run.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { Queue } = require("bullmq");
const fs = require("node:fs/promises");
const path = require("node:path");

async function main() {
  const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";
  const url = new URL(redisUrl);
  const connection = { host: url.hostname, port: Number(url.port) || 6379, password: url.password || undefined };
  const queue = new Queue("marketplace-tasks", { connection });
  try {
    const counts = await queue.getJobCounts("waiting", "active", "completed", "failed", "delayed");
    console.log("BullMQ counts:", JSON.stringify(counts));

    const active = await queue.getActive();
    console.log("Active jobs:", active.length);
    for (const j of active) {
      console.log("  ", j.name, "| id:", j.id, "| productIds:", j.data?.productIds?.length, "| reason:", j.data?.reason);
    }

    // Добавляем wb-marketplace-sync если нет активного
    const wbActive = active.find(j => j.name === "wb-marketplace-sync");
    if (!wbActive) {
      const result = await queue.add("wb-marketplace-sync", { source: "manual_trigger", reason: "manual" }, {
        jobId: "wb-marketplace-sync:manual:" + Date.now(),
        priority: 10,
        removeOnComplete: 2000,
        removeOnFail: 2000,
      });
      console.log("WB sync job queued, id:", result?.id);
    } else {
      console.log("WB sync already active, id:", wbActive.id);
    }

    // Статус файл
    try {
      const statusPath = path.join(__dirname, "..", "data", "wb-sync-status.json");
      const raw = await fs.readFile(statusPath, "utf8");
      const s = JSON.parse(raw);
      const r = s.lastResult || {};
      console.log("\\nLast WB sync:", r.status, "| at:", r.at, "| pricesSent:", r.pricesSent, "| inStock:", r.inStock);
    } catch(e) { console.log("status file:", e.message); }
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
