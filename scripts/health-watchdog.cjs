#!/usr/bin/env node
"use strict";

// PLAN-HARDENING.md 2.1: PM2 does not restart a process whose event loop is pinned —
// it still reports "online" because the process never exits. This script polls
// /health on the api and worker ports; after N consecutive unhealthy checks for a
// process it `pm2 restart`s that process and records an incident.
//
// Run continuously (every HEALTH_WATCHDOG_INTERVAL_MS, ~30-60s) via:
//   node scripts/health-watchdog.cjs --loop
// or once per invocation from cron.

require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const statePath = path.join(root, "data", "health-watchdog-state.json");
const incidentsPath = path.join(root, "data", "health-watchdog-incidents.jsonl");

const TARGETS = [
  { name: "davidsklad-api", port: Number(process.env.PORT || 3000) || 3000 },
  { name: "davidsklad-worker", port: Number(process.env.WORKER_HEALTH_PORT || 3001) || 3001 },
];

const requestTimeoutMs = Math.max(1000, Number(process.env.HEALTH_WATCHDOG_TIMEOUT_MS || 8000) || 8000);
const failureThreshold = Math.max(1, Number(process.env.HEALTH_WATCHDOG_FAILURE_THRESHOLD || 3) || 3);
const intervalMs = Math.max(15_000, Math.min(120_000, Number(process.env.HEALTH_WATCHDOG_INTERVAL_MS || 45_000) || 45_000));

function fetchHealth(port) {
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const req = http.get(
      { hostname: "127.0.0.1", port, path: "/health", timeout: requestTimeoutMs },
      (res) => {
        let data = "";
        res.on("data", (chunk) => { data += chunk; });
        res.on("end", () => {
          let body = null;
          try { body = JSON.parse(data); } catch { body = null; }
          resolve({ status: res.statusCode, body, elapsedMs: Date.now() - startedAt, error: null });
        });
      },
    );
    req.on("error", (error) => {
      resolve({ status: 0, body: null, elapsedMs: Date.now() - startedAt, error: error.message });
    });
    req.on("timeout", () => {
      req.destroy();
      resolve({ status: 0, body: null, elapsedMs: Date.now() - startedAt, error: `timeout after ${requestTimeoutMs}ms` });
    });
  });
}

// Pure: classifies a /health probe result. `liveness` (from PLAN-HARDENING.md 2.1's
// event-loop heartbeat) catches a process that is so lagged it can still answer HTTP
// but its background timers have stalled.
function evaluateHealthResponse({ status, body, error }) {
  const reasons = [];
  if (error) {
    reasons.push(`request error: ${error}`);
  } else if (status !== 200) {
    reasons.push(`http ${status}`);
  } else {
    if (body?.ok === false) reasons.push("health ok=false");
    const liveness = body?.liveness;
    if (liveness && liveness.healthy === false) {
      reasons.push(`event loop heartbeat stale (${liveness.ageMs}ms > ${liveness.maxAgeMs}ms)`);
    }
  }
  return { healthy: reasons.length === 0, reasons };
}

// Pure: consecutive-failure counter, reset to 0 on a healthy check.
function nextConsecutiveFailures(state, name, healthy) {
  const previous = Number(state?.[name]?.consecutiveFailures || 0);
  return healthy ? 0 : previous + 1;
}

// Pure: whether enough consecutive failures have accumulated to restart.
function shouldRestart(consecutiveFailures, threshold = failureThreshold) {
  return consecutiveFailures >= threshold;
}

function readState() {
  try {
    return JSON.parse(fs.readFileSync(statePath, "utf8"));
  } catch {
    return {};
  }
}

function writeState(state) {
  fs.mkdirSync(path.dirname(statePath), { recursive: true });
  fs.writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function recordIncident(entry) {
  fs.mkdirSync(path.dirname(incidentsPath), { recursive: true });
  fs.appendFileSync(incidentsPath, `${JSON.stringify(entry)}\n`, "utf8");
}

function restartProcess(name) {
  execFileSync("pm2", ["restart", name], { stdio: "inherit" });
}

function alert(message) {
  try {
    // --immediate: the watchdog only calls this after FAILURE_THRESHOLD consecutive failures
    // AND a restart, so the incident is already confirmed — alert on the first one (debounced),
    // don't wait for a second consecutive failure inside prod-alert-on-failure.cjs.
    execFileSync("node", [path.join(__dirname, "prod-alert-on-failure.cjs"), "--immediate", message], { stdio: "inherit" });
  } catch {
    // best effort; the incident is already recorded on disk.
  }
}

async function checkOnce() {
  const state = readState();
  const results = [];

  for (const target of TARGETS) {
    const probe = await fetchHealth(target.port);
    const evaluation = evaluateHealthResponse(probe);
    const consecutiveFailures = nextConsecutiveFailures(state, target.name, evaluation.healthy);
    state[target.name] = {
      consecutiveFailures,
      lastCheckAt: new Date().toISOString(),
      healthy: evaluation.healthy,
      reasons: evaluation.reasons,
    };

    if (shouldRestart(consecutiveFailures, failureThreshold)) {
      const incident = {
        at: new Date().toISOString(),
        process: target.name,
        port: target.port,
        consecutiveFailures,
        reasons: evaluation.reasons,
      };
      console.error(`health-watchdog: restarting ${target.name}`, incident);
      recordIncident(incident);
      try {
        restartProcess(target.name);
        state[target.name].consecutiveFailures = 0;
      } catch (error) {
        console.error(`health-watchdog: pm2 restart ${target.name} failed: ${error.message}`);
      }
      alert(`health-watchdog restarted ${target.name}: ${evaluation.reasons.join(", ") || "unhealthy"}`);
    }

    results.push({ target, probe, evaluation, consecutiveFailures });
  }

  writeState(state);
  return results;
}

async function main() {
  if (process.argv.includes("--loop")) {
    for (;;) {
      await checkOnce().catch((error) => console.error(`health-watchdog: check failed: ${error.message}`));
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }

  const results = await checkOnce();
  if (results.some((result) => !result.evaluation.healthy)) process.exitCode = 1;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  evaluateHealthResponse,
  nextConsecutiveFailures,
  shouldRestart,
  checkOnce,
};
