#!/usr/bin/env node
"use strict";

require("dotenv").config();

const http = require("node:http");
const { execSync } = require("node:child_process");

const failures = [];
const workerHealthPort = Number(process.env.WORKER_HEALTH_PORT || 3001) || 3001;
const slowRequestAlertThreshold = Math.max(3, Number(process.env.SLOW_REQUEST_ALERT_COUNT || 3) || 3);
const slowRequestMs = Math.max(5000, Number(process.env.SLOW_REQUEST_ALERT_MS || 10000) || 10000);
const loginMaxMs = Math.max(2000, Number(process.env.LOGIN_ALERT_MAX_MS || 5000) || 5000);
const heapPressureMax = Math.max(0.5, Math.min(0.95, Number(process.env.HEAP_PRESSURE_ALERT_RATIO || 0.8) || 0.8));

function request(method, urlPath, body, { port = 3000, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      path: urlPath,
      method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...headers,
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* keep text */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function timedRequest(method, urlPath, body, options = {}) {
  const startedAt = Date.now();
  const response = await request(method, urlPath, body, options);
  return { ...response, elapsedMs: Date.now() - startedAt };
}

function sessionCookie(headers = {}) {
  const setCookie = headers["set-cookie"] || [];
  const list = Array.isArray(setCookie) ? setCookie : [setCookie];
  const session = list.find((item) => String(item).startsWith("pm_session="));
  return session ? String(session).split(";")[0] : "";
}

function pm2ProcessOnline(name) {
  try {
    const raw = execSync(`pm2 jlist`, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
    const list = JSON.parse(raw);
    const proc = list.find((item) => item.name === name);
    return proc?.pm2_env?.status === "online";
  } catch {
    return null;
  }
}

function fail(message) {
  failures.push(message);
  console.error(`FAIL: ${message}`);
}

async function main() {
  const apiOnline = pm2ProcessOnline("davidsklad-api");
  const workerOnline = pm2ProcessOnline("davidsklad-worker");
  if (apiOnline === false) fail("davidsklad-api is not online");
  if (workerOnline === false) fail("davidsklad-worker is not online");
  if (pm2ProcessOnline("davidsklad") === true) fail("legacy monolith davidsklad is still running");

  const loginPage = await timedRequest("GET", "/login.html");
  if (loginPage.status !== 200) fail(`login.html HTTP ${loginPage.status}`);
  if (loginPage.elapsedMs > loginMaxMs) fail(`login.html slow: ${loginPage.elapsedMs}ms > ${loginMaxMs}ms`);

  const login = await timedRequest("POST", "/api/login", {
    username: process.env.APP_USER || "david",
    password: process.env.APP_PASSWORD || "",
  });
  const cookie = sessionCookie(login.headers);
  if (!cookie) fail(`API login failed: HTTP ${login.status}`);
  if (login.elapsedMs > loginMaxMs) fail(`API login slow: ${login.elapsedMs}ms > ${loginMaxMs}ms`);
  const hdr = { Cookie: cookie };

  const workerHealth = await timedRequest("GET", "/health", null, { port: workerHealthPort });
  if (workerHealth.status !== 200) fail(`worker health HTTP ${workerHealth.status} on port ${workerHealthPort}`);

  const daily = await timedRequest("GET", "/api/daily-sync", null, hdr);
  const page = await timedRequest("GET", "/api/warehouse/products/page?page=1&pageSize=8&linked=linked", null, hdr);
  const pageAll = await timedRequest("GET", "/api/warehouse/products/page?page=1&pageSize=8", null, hdr);
  const pageGrouped = await timedRequest("GET", "/api/warehouse/products/page?page=1&pageSize=40&grouped=true", null, hdr);
  const pageUnlinkedGrouped = await timedRequest(
    "GET",
    "/api/warehouse/products/page?page=1&pageSize=40&linked=unlinked&grouped=true",
    null,
    hdr,
  );
  const [live, health] = await Promise.all([
    timedRequest("GET", "/api/live-status", null, hdr),
    timedRequest("GET", "/api/health?deep=true", null, hdr),
  ]);

  const items = Array.isArray(page.body?.items) ? page.body.items : [];
  const unlinkedGroups = Array.isArray(pageUnlinkedGrouped.body?.items) ? pageUnlinkedGrouped.body.items : [];
  const unlinkedOk = pageUnlinkedGrouped.status === 200
    && unlinkedGroups.length > 0
    && !pageUnlinkedGrouped.body?.sourceError
    && pageUnlinkedGrouped.elapsedMs < 15000;
  if (!unlinkedOk) {
    fail(`unlinked grouped catalog check failed: status=${pageUnlinkedGrouped.status} items=${unlinkedGroups.length} elapsed=${pageUnlinkedGrouped.elapsedMs}ms error=${pageUnlinkedGrouped.body?.sourceError || ""}`);
  }

  const heapRatio = Number(health.body?.memory?.heapPressureRatio || health.body?.heapPressureRatio || 0);
  if (heapRatio > heapPressureMax) {
    fail(`api heap pressure ${heapRatio.toFixed(2)} > ${heapPressureMax}`);
  }

  const slowRecent = Array.isArray(health.body?.recentSlowRequests) ? health.body.recentSlowRequests : [];
  const slowCount = slowRecent.filter((entry) => Number(entry?.durationMs || entry?.elapsedMs || 0) >= slowRequestMs).length;
  if (slowCount >= slowRequestAlertThreshold) {
    fail(`slow request alert: ${slowCount} requests >= ${slowRequestMs}ms in recent window`);
  }

  const report = {
    pm2: { apiOnline, workerOnline },
    loginPage: { status: loginPage.status, elapsedMs: loginPage.elapsedMs },
    apiLogin: { status: login.status, elapsedMs: login.elapsedMs },
    workerHealth: { status: workerHealth.status, elapsedMs: workerHealth.elapsedMs, port: workerHealthPort },
    daily: { status: daily.body?.status, running: daily.body?.running, lastRunAt: daily.body?.lastRunAt },
    warehousePageLinked: {
      total: page.body?.total,
      items: items.length,
      partial: page.body?.partial,
      elapsedMs: page.elapsedMs,
      linkedProducts: page.body?.linkedProducts,
      sample: items.slice(0, 4).map((item) => ({
        id: item.id,
        offerId: item.offerId,
        marketplace: item.marketplace,
        links: (item.links || []).length,
        manualGroupId: item.manualGroupId || item.raw?.manualGroupId || "",
      })),
    },
    warehousePageAll: {
      total: pageAll.body?.total,
      items: Array.isArray(pageAll.body?.items) ? pageAll.body.items.length : 0,
      partial: pageAll.body?.partial,
      elapsedMs: pageAll.elapsedMs,
    },
    warehousePageUnlinkedGrouped: {
      status: pageUnlinkedGrouped.status,
      elapsedMs: pageUnlinkedGrouped.elapsedMs,
      grouped: pageUnlinkedGrouped.body?.grouped,
      groupTotal: pageUnlinkedGrouped.body?.groupTotal ?? pageUnlinkedGrouped.body?.total,
      items: unlinkedGroups.length,
      partial: pageUnlinkedGrouped.body?.partial,
      sourceError: pageUnlinkedGrouped.body?.sourceError || "",
      ok: unlinkedOk,
    },
    warehousePageGrouped: {
      grouped: pageGrouped.body?.grouped,
      groupTotal: pageGrouped.body?.groupTotal ?? pageGrouped.body?.total,
      items: Array.isArray(pageGrouped.body?.items) ? pageGrouped.body.items.length : 0,
      elapsedMs: pageGrouped.elapsedMs,
      sourceError: pageGrouped.body?.sourceError || "",
    },
    liveStatus: live.body,
    health: {
      ok: health.body?.ok,
      serverRole: health.body?.serverRole,
      heapPressureRatio: heapRatio,
      recentSlowRequests: slowRecent.slice(-5),
      queue: health.body?.queue || health.body?.bullmq || null,
    },
    failures,
    ok: failures.length === 0,
  };

  console.log(JSON.stringify(report, null, 2));
  if (failures.length) process.exit(1);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
