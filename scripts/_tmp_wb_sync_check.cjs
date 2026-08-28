#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_wb_sync_check_run.js";

const script = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const fs = require("node:fs/promises");
const path = require("node:path");
const { execSync } = require("node:child_process");

async function main() {
  // Правильный путь: /var/www/davidsklad/davidskladik/data/wb-sync-status.json
  const statusPath = path.join(__dirname, "..", "data", "wb-sync-status.json");
  try {
    const raw = await fs.readFile(statusPath, "utf8");
    const s = JSON.parse(raw);
    const r = s.lastResult || {};
    console.log("=== WB Sync последний результат ===");
    console.log("status:", r.status);
    console.log("at:", r.at);
    console.log("source:", r.source);
    console.log("pricesSent:", r.pricesSent);
    console.log("stocksSent:", r.stocksSent);
    console.log("inStock:", r.inStock);
    console.log("zeroed:", r.zeroed);
    console.log("pricesError:", r.pricesError || null);
    console.log("elapsedMs:", r.elapsedMs);
    console.log("running:", s.running);
    console.log("updatedAt:", s.updatedAt);
  } catch (e) {
    console.log("Статус файл:", e.message);
  }

  // pm2 логи
  try {
    const logs = execSync("pm2 logs --nostream --lines 500 2>&1").toString();
    const lines = logs.split("\\n").filter(l =>
      l.includes("wb_sync") || l.includes("wb sync") || l.includes("wb-marketplace-sync")
    );
    console.log("\\n=== WB логи (последние 15) ===");
    lines.slice(-15).forEach(l => {
      try {
        const p = JSON.parse(l.replace(/^[^{]*/, ""));
        const extra = { pricesSent: p.pricesSent, inStock: p.inStock, zeroed: p.zeroed, status: p.status, detail: p.detail };
        console.log("[" + p.level + "]", p.msg, JSON.stringify(extra));
      } catch { if (l.trim()) console.log(l.slice(0, 200)); }
    });
  } catch (e) { console.log("pm2 logs error:", e.message.slice(0, 100)); }
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
