#!/usr/bin/env node
"use strict";

require("dotenv").config();

const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");

const failures = [];
const workerHealthPort = Number(process.env.WORKER_HEALTH_PORT || 3001) || 3001;
const slowRequestAlertThreshold = Math.max(3, Number(process.env.SLOW_REQUEST_ALERT_COUNT || 3) || 3);
const slowRequestMs = Math.max(5000, Number(process.env.SLOW_REQUEST_ALERT_MS || 10000) || 10000);
const loginMaxMs = Math.max(2000, Number(process.env.LOGIN_ALERT_MAX_MS || 5000) || 5000);
const heapPressureMax = Math.max(0.5, Math.min(0.95, Number(process.env.HEAP_PRESSURE_ALERT_RATIO || 0.8) || 0.8));
const bullmqFailedMax = Math.max(1, Number(process.env.BULLMQ_FAILED_ALERT_MAX || 500) || 500);
const unlinkedRetryAttempts = Math.max(1, Number(process.env.POSTDEPLOY_UNLINKED_RETRIES || 3) || 3);
const unlinkedRetryDelayMs = Math.max(1000, Number(process.env.POSTDEPLOY_UNLINKED_RETRY_MS || 5000) || 5000);
const requestTimeoutMs = Math.max(5000, Number(process.env.POSTDEPLOY_REQUEST_TIMEOUT_MS || 90000) || 90000);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
    req.setTimeout(requestTimeoutMs, () => {
      req.destroy(new Error(`request timeout after ${requestTimeoutMs}ms: ${method} ${urlPath}`));
    });
    if (payload) req.write(payload);
    req.end();
  });
}

function normalizeRequestOptions(options = {}) {
  if (options.headers || options.port) return options;
  return { headers: options };
}

async function timedRequest(method, urlPath, body, options = {}) {
  const startedAt = Date.now();
  const response = await request(method, urlPath, body, normalizeRequestOptions(options));
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
    const raw = execSync("pm2 jlist", { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
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

function unlinkedOk(response) {
  const groups = Array.isArray(response.body?.items) ? response.body.items : [];
  return response.status === 200
    && groups.length > 0
    && !response.body?.sourceError
    && response.elapsedMs < 15000;
}

async function fetchUnlinkedGrouped(hdr) {
  let last = null;
  for (let attempt = 1; attempt <= unlinkedRetryAttempts; attempt += 1) {
    last = await timedRequest(
      "GET",
      "/api/warehouse/products/page?page=1&pageSize=40&linked=unlinked&grouped=true",
      null,
      hdr,
    );
    if (unlinkedOk(last)) return { response: last, attempt };
    if (attempt < unlinkedRetryAttempts) await sleep(unlinkedRetryDelayMs);
  }
  return { response: last, attempt: unlinkedRetryAttempts };
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

  let workerHealth = { status: 0, elapsedMs: 0, body: {}, skipped: false };
  const workerHealthTimeoutMs = Math.min(15000, requestTimeoutMs);
  try {
    workerHealth = await Promise.race([
      timedRequest("GET", "/health", null, { port: workerHealthPort }),
      new Promise((_, reject) => setTimeout(
        () => reject(new Error(`worker health timeout after ${workerHealthTimeoutMs}ms`)),
        workerHealthTimeoutMs,
      )),
    ]);
    if (workerHealth.status !== 200) fail(`worker health HTTP ${workerHealth.status} on port ${workerHealthPort}`);
    if (workerHealth.body?.serverRole !== "worker") fail(`worker health serverRole=${workerHealth.body?.serverRole || "unknown"}`);
    if (workerHealth.body?.queue?.consumerReady !== true) fail("worker BullMQ consumer is not ready");
  } catch (error) {
    if (workerOnline !== true) {
      fail(`worker health check failed: ${error.message}`);
    } else {
      workerHealth.skipped = true;
      workerHealth.error = error.message;
      console.error(`WARN: worker health skipped (pm2 online): ${error.message}`);
    }
  }

  const daily = await timedRequest("GET", "/api/daily-sync", null, hdr);
  const page = await timedRequest("GET", "/api/warehouse/products/page?page=1&pageSize=8&linked=linked", null, hdr);
  const pageAll = await timedRequest("GET", "/api/warehouse/products/page?page=1&pageSize=8", null, hdr);
  const unlinkedResult = await fetchUnlinkedGrouped(hdr);
  const pageUnlinkedGrouped = unlinkedResult.response;
  const [live, health] = await Promise.all([
    timedRequest("GET", "/api/live-status", null, hdr),
    timedRequest("GET", "/api/health?deep=true", null, hdr),
  ]);

  if (health.body?.serverRole !== "api") fail(`api health serverRole=${health.body?.serverRole || "unknown"}`);
  if (health.body?.components?.redis?.ok === false) {
    fail(`redis/BullMQ unhealthy: ${health.body?.components?.redis?.error || "unknown"}`);
  }
  if (health.body?.queue?.degraded === true) {
    fail(`queue degraded: ${health.body?.queue?.error || "producer or redis unavailable"}`);
  }
  if (health.body?.queue?.producerReady !== true) {
    fail("api BullMQ producer is not ready");
  }

  const failedJobs = Number(health.body?.bullmq?.counts?.failed ?? health.body?.queue?.counts?.failed ?? 0);
  if (failedJobs > bullmqFailedMax) {
    fail(`BullMQ failed jobs ${failedJobs} > ${bullmqFailedMax}`);
  }

  const items = Array.isArray(page.body?.items) ? page.body.items : [];
  const unlinkedGroups = Array.isArray(pageUnlinkedGrouped.body?.items) ? pageUnlinkedGrouped.body.items : [];
  const unlinkedPass = unlinkedOk(pageUnlinkedGrouped);
  if (!unlinkedPass) {
    fail(`unlinked grouped catalog check failed after ${unlinkedResult.attempt} attempts: status=${pageUnlinkedGrouped.status} items=${unlinkedGroups.length} elapsed=${pageUnlinkedGrouped.elapsedMs}ms error=${pageUnlinkedGrouped.body?.sourceError || ""}`);
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
    workerHealth: {
      status: workerHealth.status,
      elapsedMs: workerHealth.elapsedMs,
      port: workerHealthPort,
      consumerReady: workerHealth.body?.queue?.consumerReady,
      failedJobs: workerHealth.body?.bullmq?.counts?.failed ?? workerHealth.body?.queue?.counts?.failed ?? null,
    },
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
      attempts: unlinkedResult.attempt,
      grouped: pageUnlinkedGrouped.body?.grouped,
      groupTotal: pageUnlinkedGrouped.body?.groupTotal ?? pageUnlinkedGrouped.body?.total,
      items: unlinkedGroups.length,
      partial: pageUnlinkedGrouped.body?.partial,
      sourceError: pageUnlinkedGrouped.body?.sourceError || "",
      ok: unlinkedPass,
    },
    liveStatus: live.body,
    health: {
      ok: health.body?.ok,
      serverRole: health.body?.serverRole,
      heapPressureRatio: heapRatio,
      recentSlowRequests: slowRecent.slice(-5),
      queue: health.body?.queue || null,
      bullmqFailed: failedJobs,
    },
    failures,
    ok: failures.length === 0,
  };

  console.log(JSON.stringify(report, null, 2));
  if (!failures.length) resetAlertCounter();
  if (failures.length) process.exit(1);
}

// Cron вызывает prod-alert-on-failure.cjs только при фейле, поэтому счётчик
// consecutiveFailures без этого сброса рос вечно (наблюдалось 3976) и терял смысл.
function resetAlertCounter() {
  const alertPath = path.join(__dirname, "..", "data", "last-prod-alert.json");
  try {
    const state = JSON.parse(fs.readFileSync(alertPath, "utf8"));
    if (!state || !Number(state.consecutiveFailures)) return;
    fs.writeFileSync(alertPath, `${JSON.stringify({
      ...state,
      consecutiveFailures: 0,
      lastSuccessAt: new Date().toISOString(),
    }, null, 2)}\n`, "utf8");
  } catch {
    // нет файла состояния — сбрасывать нечего
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
