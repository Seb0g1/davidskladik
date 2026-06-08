#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      path: urlPath,
      method,
      headers: {
        ...(payload
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) }
          : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try {
          parsed = JSON.parse(data);
        } catch {
          parsed = data;
        }
        resolve({
          status: res.statusCode,
          headers: res.headers,
          body: parsed,
        });
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
  if (!appPassword) throw new Error("APP_PASSWORD missing in server .env");
  const res = await request("POST", "/api/login", {
    body: { username, password: appPassword },
  });
  const cookie = sessionCookie(res.headers);
  if (!cookie || res.status !== 200) {
    throw new Error(`Login failed for ${username}: HTTP ${res.status}`);
  }
  console.log(`Logged in as ${username}`);
  return cookie;
}

async function trigger(cookie, label, urlPath) {
  console.log(`\n=== ${label} ===`);
  const res = await request("POST", urlPath, { cookie, body: {} });
  console.log(`HTTP ${res.status}`, typeof res.body === "object" ? JSON.stringify(res.body, null, 2) : res.body);
  if (res.status >= 400) throw new Error(`${label} failed: HTTP ${res.status}`);
  return res.body;
}

async function waitForWarehouseSync(cookie, maxMinutes = 180) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  while (Date.now() < deadline) {
    const res = await request("GET", "/api/warehouse/sync/status", { cookie });
    const status = res.body || {};
    const progress = status.progress || {};
    console.log(
      `[warehouse] ${status.status || "unknown"} ${progress.percent || 0}%`
      + ` ${progress.stage || ""} — ${progress.meta || ""}`,
    );
    if (status.status === "ok") {
      console.log("Warehouse sync finished OK:", JSON.stringify(status.result || {}, null, 2));
      return status;
    }
    if (status.status === "error") {
      throw new Error(`Warehouse sync failed: ${status.error || "unknown"}`);
    }
    if (!status.running && status.status !== "running") return status;
    await sleep(15000);
  }
  throw new Error("Warehouse sync timeout");
}

async function waitForDailySync(cookie, maxMinutes = 120) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  while (Date.now() < deadline) {
    const res = await request("GET", "/api/daily-sync", { cookie });
    const status = res.body || {};
    console.log(`[daily] ${status.status || "unknown"} running=${Boolean(status.running)} last=${status.lastRunAt || "-"}`);
    if (status.status === "ok" && !status.running) {
      console.log("Daily sync finished OK");
      return status;
    }
    if (status.status === "error" && !status.running) {
      throw new Error(`Daily sync failed: ${status.error || "unknown"}`);
    }
    await sleep(15000);
  }
  throw new Error("Daily sync timeout");
}

async function waitForMaintenance(cookie, maxMinutes = 120) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  let seenRunning = false;
  while (Date.now() < deadline) {
    const res = await request("GET", "/api/live-status", { cookie });
    const maintenance = res.body?.marketplaceMaintenance || {};
    console.log(
      `[maintenance] running=${Boolean(maintenance.running)}`
      + ` next=${maintenance.nextRunAt || "-"}`,
    );
    if (maintenance.running) seenRunning = true;
    if (seenRunning && !maintenance.running) {
      console.log("Marketplace maintenance finished");
      return maintenance;
    }
    await sleep(10000);
  }
  if (!seenRunning) console.log("Maintenance may have completed before first poll");
  return null;
}

async function printSummary(cookie) {
  const [live, pm] = await Promise.all([
    request("GET", "/api/live-status", { cookie }),
    request("GET", "/api/health?deep=1", { cookie }).catch(() => request("GET", "/api/health", { cookie })),
  ]);
  console.log("\n=== Summary ===");
  console.log(JSON.stringify({
    liveStatus: live.body,
    health: pm.body,
  }, null, 2));
}

async function main() {
  const cookie = await login();

  await trigger(cookie, "PriceMaster snapshot sync", "/api/sync");
  await sleep(3000);

  await trigger(cookie, "Full warehouse sync (PM + Ozon + Yandex + automation)", "/api/warehouse/sync/run");
  await waitForWarehouseSync(cookie);

  await trigger(cookie, "Daily sync (delta prices + automation)", "/api/daily-sync/run");
  await waitForDailySync(cookie);

  await trigger(cookie, "Marketplace maintenance (PM verify + zero stock)", "/api/marketplace/maintenance/run");
  await waitForMaintenance(cookie);

  await trigger(cookie, "Ozon unarchive queue rebuild", "/api/ozon/unarchive-queue/rebuild");
  await sleep(5000);

  await printSummary(cookie);
  console.log("\nAll synchronizations triggered and completed.");
}

main().catch((error) => {
  console.error("SYNC_FAILED:", error.message);
  process.exit(1);
});
