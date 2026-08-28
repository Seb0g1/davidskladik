#!/usr/bin/env node
"use strict";
const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD || "";

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1", port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch {}
        resolve({ status: res.statusCode, body: parsed, headers: res.headers });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = [].concat(headers["set-cookie"] || []);
  const s = list.find((x) => String(x).startsWith("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

async function login() {
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const ck = sessionCookie(res.headers);
  if (!ck || res.status !== 200) throw new Error(`Login failed: HTTP ${res.status}`);
  return ck;
}

async function main() {
  const cookie = await login();

  // Check unarchive queue status
  console.log("=== Ozon unarchive queue status ===");
  const queueRes = await request("GET", "/api/ozon/unarchive-queue", { cookie });
  const q = queueRes.body;
  console.log(`  status: HTTP ${queueRes.status}`);
  if (queueRes.status === 200) {
    console.log(`  total in queue: ${q.total || q.items?.length || 0}`);
    console.log(`  due (ready to process): ${q.due}`);
    console.log(`  future (deferred): ${q.future}`);
    console.log(`  dailyUsed: ${JSON.stringify(q.dailyUsed || q.daily || {})}`);
    console.log(`  autoEnabled: ${q.autoEnabled}`);
    console.log(`  autoRunning: ${q.autoRunning}`);
    console.log(`  lastResult: ${JSON.stringify(q.lastResult || {}).slice(0, 300)}`);
    // Sample items
    const items = q.items || [];
    console.log(`  sample items (first 5):`);
    for (const item of items.slice(0, 5)) {
      console.log(`    offerId=${item.offerId} target=${item.target} due=${item.due} warning=${item.warning}`);
    }
  }

  // Check if there's a linked-reprice runner that shows products to unarchive
  console.log("\n=== Warehouse diagnostics (archived count) ===");
  const diagRes = await request("GET", "/api/warehouse/diagnostics?include=archival,linkage", { cookie });
  if (diagRes.status === 200) {
    const d = diagRes.body;
    console.log(JSON.stringify(d, null, 2).slice(0, 1000));
  } else {
    console.log(`  HTTP ${diagRes.status}: ${JSON.stringify(diagRes.body).slice(0, 200)}`);
  }

  // Check live-status more deeply
  console.log("\n=== Live status expanded ===");
  const liveRes = await request("GET", "/api/live-status", { cookie });
  const ls = liveRes.body;
  // Look for warehouse stats
  const wh = ls?.warehouse || {};
  console.log(`  warehouse products: ${wh.products}`);
  console.log(`  ozonArchived: ${wh.ozonArchived}`);
  console.log(`  linkedArchived: ${wh.linkedArchived}`);
  console.log(`  warehouse full state keys: ${Object.keys(wh).join(", ")}`);

  // Check warehouse summary
  console.log("\n=== Warehouse postgres summary ===");
  const summaryRes = await request("GET", "/api/warehouse/summary", { cookie });
  if (summaryRes.status === 200) {
    const s = summaryRes.body;
    console.log(`  linkedArchived: ${s.linkedArchived}`);
    console.log(`  ozonArchived: ${s.ozonArchived}`);
    console.log(JSON.stringify(s, null, 2).slice(0, 800));
  } else {
    console.log(`  HTTP ${summaryRes.status}: ${JSON.stringify(summaryRes.body).slice(0, 200)}`);
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
