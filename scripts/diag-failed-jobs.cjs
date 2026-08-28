#!/usr/bin/env node
"use strict";
require("dotenv").config();
const http = require("http");
const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";
let sessionCookie = "";

async function rawReq(method, path, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : undefined;
    const req = http.request({ hostname: "127.0.0.1", port: 3000, path, method,
      headers: { Cookie: sessionCookie, "Content-Type": "application/json", ...(payload ? { "Content-Length": Buffer.byteLength(payload) } : {}) },
      timeout: 30000,
    }, (res) => {
      const sc = res.headers["set-cookie"];
      if (sc) { const p = sc.find((c) => c.startsWith("pm_session=")); if (p) sessionCookie = p.split(";")[0]; }
      const chunks = []; res.on("data", (d) => chunks.push(d));
      res.on("end", () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch { resolve({}); } });
    });
    req.on("error", reject); req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    if (payload) req.write(payload); req.end();
  });
}

async function main() {
  const lr = await rawReq("POST", "/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (!lr.ok) throw new Error("login failed");

  const status = await rawReq("GET", "/health/deep");
  const queue = status?.queue || status?.health?.queue || {};
  console.log("Queue counts:", JSON.stringify(queue.counts || {}));

  // Get failed jobs details
  const failed = await rawReq("GET", "/api/system/queue/failed?limit=20");
  if (failed.jobs) {
    console.log(`\nFailed jobs (${failed.jobs.length}):`);
    for (const job of failed.jobs) {
      console.log(`  [${job.id}] ${job.name} — ${String(job.failedReason || "").slice(0, 120)}`);
      console.log(`    data: ${JSON.stringify(job.data || {}).slice(0, 150)}`);
    }
  } else {
    console.log("Failed jobs response:", JSON.stringify(failed).slice(0, 500));
  }
}
main().catch((e) => { console.error(e.message); process.exit(1); });
