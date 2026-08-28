#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");
const { spawn } = require("node:child_process");

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
  if (!appPassword) throw new Error("APP_PASSWORD missing in server .env");
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(res.headers);
  if (!cookie || res.status !== 200) throw new Error(`Login failed: HTTP ${res.status}`);
  console.log(`Logged in as ${username}`);
  return cookie;
}

async function getJson(cookie, urlPath) {
  const res = await request("GET", urlPath, { cookie });
  if (res.status >= 400) throw new Error(`${urlPath} failed: HTTP ${res.status}`);
  return res.body;
}

async function postJson(cookie, urlPath, body) {
  const res = await request("POST", urlPath, { cookie, body });
  if (res.status >= 400) throw new Error(`${urlPath} failed: HTTP ${res.status} ${JSON.stringify(res.body)}`);
  return res.body;
}

async function waitForPricePush(cookie, maxMinutes = 240) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  const stallMs = Math.max(120000, Number(process.env.REPRICE_WAIT_STALL_MS || 300000) || 300000);
  let idleSince = 0;
  while (Date.now() < deadline) {
    const live = await getJson(cookie, "/api/live-status");
    const push = live?.automation?.pricePush || {};
    const queue = live?.queue || live?.bullmq || {};
    const counts = queue.counts || {};
    const waiting = Number(counts.waiting || 0);
    const active = Number(counts.active || 0);
    const pendingScope = Number(push.pendingScope || 0);
    const pushIdle = !push.running && !push.scheduled && pendingScope === 0 && waiting === 0;
    console.log(
      `[price-push] running=${Boolean(push.running)} pending=${pendingScope}`
      + ` scheduled=${Boolean(push.scheduled)} bullmq waiting=${waiting} active=${active}`
      + ` pushIdle=${pushIdle}`,
    );
    if (pushIdle && active === 0) return live;
    if (pushIdle && active > 0) {
      if (!idleSince) idleSince = Date.now();
      if (Date.now() - idleSince >= stallMs) {
        console.log(`[price-push] queue idle with active=${active} for ${stallMs}ms — continuing`);
        return live;
      }
    } else {
      idleSince = 0;
    }
    await sleep(15000);
  }
  throw new Error("price push wait timeout");
}

function runFixQuarantine() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["scripts/fix-ozon-quarantine-prices.cjs"], {
      cwd: path.join(__dirname, ".."),
      stdio: "inherit",
      env: {
        ...process.env,
        DISABLE_BACKGROUND_JOBS: "true",
        NODE_OPTIONS: process.env.NODE_OPTIONS || "--max-old-space-size=4096",
      },
    });
    child.on("close", (code) => (code ? reject(new Error(`quarantine fix exit ${code}`)) : resolve()));
  });
}

async function main() {
  const cookie = await login();

  console.log("\n=== PriceMaster snapshot sync ===");
  const pm = await postJson(cookie, "/api/sync", {});
  console.log(JSON.stringify(pm, null, 2));
  await sleep(3000);

  console.log("\n=== Linked products: reprice (Ozon + Yandex, linked only, no force) ===");
  const reprice = await postJson(cookie, "/api/sales-automation/run", {
    force: false,
    onlyChanged: true,
    marketplace: "all",
    verify: true,
    limit: 50000,
    reason: "linked_full_reprice",
  });
  console.log(JSON.stringify(reprice, null, 2));

  console.log("\n=== Waiting for marketplace price push queue ===");
  const liveAfter = await waitForPricePush(cookie);
  console.log(JSON.stringify({
    automation: liveAfter?.automation,
    queue: liveAfter?.queue || liveAfter?.bullmq,
  }, null, 2));

  const summary = await getJson(cookie, "/api/sales-automation/summary");
  console.log("\n=== Sales automation summary ===");
  console.log(JSON.stringify(summary, null, 2));

  console.log("\n=== Ozon quarantine staged release ===");
  await runFixQuarantine();

  const afterSummary = await getJson(cookie, "/api/sales-automation/summary");
  console.log("\n=== Final summary ===");
  console.log(JSON.stringify(afterSummary, null, 2));
  console.log("\nLinked full reprice complete.");
}

main().catch((error) => {
  console.error("LINKED_REPRICE_FAILED:", error.message);
  process.exit(1);
});
