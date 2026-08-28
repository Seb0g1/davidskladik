#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_sync_enqueue.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { Queue } = require("bullmq");

const redisUrl = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const url = new URL(redisUrl);
const connection = {
  host: url.hostname,
  port: Number(url.port) || 6379,
  password: url.password || undefined,
  tls: url.protocol === "rediss:" ? {} : undefined,
};

const queue = new Queue("marketplace-tasks", { connection });

async function main() {
  await queue.add("wb-marketplace-sync", { source: "manual_script" }, { priority: 2 });
  console.log("WB sync job добавлен в очередь marketplace-tasks");
  console.log("Worker подхватит через несколько секунд.");

  // Ждём 10 секунд и проверяем статус очереди
  await new Promise(r => setTimeout(r, 10000));
  const counts = await queue.getJobCounts("waiting", "active", "completed", "failed");
  console.log("Счётчики очереди:", JSON.stringify(counts));
  await queue.close();
}
main().catch(e => { console.error("error:", e.message); process.exit(1); });
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
