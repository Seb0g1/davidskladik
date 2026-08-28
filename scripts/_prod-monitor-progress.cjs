#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      path: urlPath,
      method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* keep string */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const setCookie = headers["set-cookie"] || [];
  const list = Array.isArray(setCookie) ? setCookie : [setCookie];
  const session = list.find((item) => String(item).startsWith("pm_session="));
  return session ? String(session).split(";")[0] : "";
}

async function login() {
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(res.headers);
  if (!cookie || res.status !== 200) throw new Error(`login failed: ${res.status}`);
  return cookie;
}

async function main() {
  const cookie = await login();
  const [live, summary, daily, whStatus] = await Promise.all([
    request("GET", "/api/live-status", { cookie }),
    request("GET", "/api/sales-automation/summary", { cookie }),
    request("GET", "/api/daily-sync", { cookie }),
    request("GET", "/api/warehouse/sync/status", { cookie }),
  ]);

  const queue = live.body?.queue || live.body?.bullmq || {};
  const counts = queue.counts || {};
  const automation = live.body?.automation || {};
  const reasons = summary.body?.reasons || {};
  const statuses = summary.body?.statuses || [];

  const ozonPending = statuses
    .filter((s) => s.marketplace === "ozon" && s.priceStatus === "pending")
    .reduce((n, s) => n + Number(s.count || 0), 0);
  const ozonSuccess = statuses
    .filter((s) => s.marketplace === "ozon" && s.priceStatus === "success")
    .reduce((n, s) => n + Number(s.count || 0), 0);
  const ozonFailed = statuses
    .filter((s) => s.marketplace === "ozon" && s.priceStatus === "failed")
    .reduce((n, s) => n + Number(s.count || 0), 0);

  console.log(JSON.stringify({
    at: new Date().toISOString(),
    bullmq: {
      waiting: counts.waiting ?? null,
      active: counts.active ?? null,
      delayed: counts.delayed ?? null,
      failed: counts.failed ?? null,
      completed: counts.completed ?? null,
    },
    pricePush: automation.pricePush || null,
    maintenance: live.body?.marketplaceMaintenance || null,
    salesAutomation: {
      total: summary.body?.total,
      retryTotal: summary.body?.retryTotal,
      ozonUnarchiveQueued: summary.body?.ozonUnarchiveQueued,
      reasons: {
        queued: reasons.queued,
        ok: reasons.ok,
        api_error: reasons.api_error,
        verification_pending: reasons.verification_pending,
        in_retry: reasons.in_retry,
        ozon_price_not_applied: reasons.ozon_price_not_applied,
      },
      ozonPrice: { pending: ozonPending, success: ozonSuccess, failed: ozonFailed },
    },
    dailySync: {
      status: daily.body?.status,
      running: daily.body?.running,
    },
    warehouseSync: {
      status: whStatus.body?.status,
      running: whStatus.body?.running,
      progress: whStatus.body?.progress || null,
    },
  }, null, 2));
}

main().catch((error) => {
  console.error("MONITOR_FAILED:", error.message);
  process.exit(1);
});
