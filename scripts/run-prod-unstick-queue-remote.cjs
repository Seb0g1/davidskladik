#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");
const { execSync } = require("node:child_process");

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
        try { parsed = JSON.parse(data); } catch { /* keep */ }
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

async function post(cookie, urlPath, body) {
  const res = await request("POST", urlPath, { cookie, body });
  if (res.status >= 400) throw new Error(`${urlPath} HTTP ${res.status}`);
  return res.body;
}

async function main() {
  console.log("=== restart worker (unstick stalled bullmq) ===");
  try {
    execSync("pm2 reload ecosystem.config.cjs --only davidsklad-worker --update-env", { stdio: "inherit" });
  } catch (error) {
    console.warn("worker restart warn:", error.message);
  }
  await new Promise((r) => setTimeout(r, 15000));

  console.log("\n=== bullmq failed cleanup ===");
  try {
    execSync("node scripts/inspect-bullmq-failed-jobs.cjs --limit=50 --remove-failed", {
      cwd: path.join(__dirname, ".."),
      stdio: "inherit",
    });
  } catch (error) {
    console.warn("bullmq cleanup warn:", error.message);
  }

  const cookie = await login();

  console.log("\n=== re-queue linked full reprice ===");
  const reprice = await post(cookie, "/api/sales-automation/run", {
    force: true,
    onlyChanged: false,
    marketplace: "all",
    verify: true,
    limit: 50000,
    reason: "unstick_reprice",
  });
  console.log(JSON.stringify(reprice, null, 2));

  console.log("\n=== ozon unarchive queue rebuild ===");
  try {
    const unarchive = await post(cookie, "/api/ozon/unarchive-queue/rebuild", {});
    console.log(JSON.stringify(unarchive, null, 2));
  } catch (error) {
    console.warn("unarchive rebuild warn:", error.message);
  }

  console.log("\n=== trigger unarchive queue process ===");
  try {
    const proc = await post(cookie, "/api/ozon/unarchive-queue/process", { confirmed: true });
    console.log(JSON.stringify(proc, null, 2));
  } catch (error) {
    console.warn("unarchive process warn:", error.message);
  }

  const live = await request("GET", "/api/live-status", { cookie });
  console.log("\n=== live-status after unstick ===");
  console.log(JSON.stringify({
    queue: live.body?.queue || live.body?.bullmq,
    automation: live.body?.automation,
  }, null, 2));
}

main().catch((error) => {
  console.error("UNSTICK_FAILED:", error.message);
  process.exit(1);
});
