"use strict";

const path = require("path");

// Single source of truth for "key runtime tunables" (concurrency, sweep intervals,
// limits): ecosystem.config.cjs's per-app env objects ARE that source — PM2 sets these
// on process.env before dotenv loads .env, and dotenv does not override an
// already-set process.env value, so any .env value for one of these keys is silently
// ignored on a PM2-managed process.
//
// Consumed by server/parts/02a-runtime-config-keys.js (startup config logging) and
// scripts/check-env-ecosystem-divergence.cjs (PLAN-HARDENING.md 2.2).
const ecosystemConfig = require(path.join(__dirname, "..", "ecosystem.config.cjs"));

// Not "tunables" in the sense this module cares about: NODE_ENV is constant across
// apps, SERVER_ROLE/WORKER_HEALTH_PORT intentionally differ per app by design.
const IGNORED_KEYS = new Set(["NODE_ENV", "SERVER_ROLE", "WORKER_HEALTH_PORT"]);

function ecosystemEnvByApp() {
  const result = {};
  for (const app of ecosystemConfig.apps || []) {
    result[app.name] = { ...(app.env || {}) };
  }
  return result;
}

function runtimeConfigKeys() {
  const keys = new Set();
  for (const env of Object.values(ecosystemEnvByApp())) {
    for (const key of Object.keys(env)) {
      if (!IGNORED_KEYS.has(key)) keys.add(key);
    }
  }
  return Array.from(keys).sort();
}

// Snapshot of the env values that actually take effect for this process right now —
// logged at startup so a mismatch between .env and ecosystem.config.cjs is visible
// immediately instead of surfacing later as unexplained behavior.
function effectiveRuntimeConfigSnapshot(env = process.env) {
  const snapshot = {};
  for (const key of runtimeConfigKeys()) {
    snapshot[key] = env[key] !== undefined ? env[key] : null;
  }
  return snapshot;
}

// Pure: compares an .env file's raw values (e.g. from dotenv.parse) against
// ecosystem.config.cjs's per-app env for the same keys. Returns one entry per
// (key, app) pair where both sides define the key with a different value.
function findEnvEcosystemDivergences(envFileVars = {}, keys = runtimeConfigKeys(), byApp = ecosystemEnvByApp()) {
  const divergences = [];
  for (const key of keys) {
    if (!(key in envFileVars)) continue;
    const envValue = String(envFileVars[key]);
    for (const [appName, appEnv] of Object.entries(byApp)) {
      if (!(key in appEnv)) continue;
      const ecosystemValue = String(appEnv[key]);
      if (envValue !== ecosystemValue) {
        divergences.push({ key, app: appName, envValue, ecosystemValue });
      }
    }
  }
  return divergences;
}

module.exports = {
  ecosystemEnvByApp,
  runtimeConfigKeys,
  effectiveRuntimeConfigSnapshot,
  findEnvEcosystemDivergences,
  IGNORED_KEYS,
};
