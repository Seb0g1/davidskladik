#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");

function run(command, args, options = {}) {
  process.stdout.write(`\n$ ${[command, ...args].join(" ")}\n`);
  const result = spawnSync(command, args, {
    cwd: root,
    shell: process.platform === "win32",
    stdio: "inherit",
    env: { ...process.env, DISABLE_BACKGROUND_JOBS: "true", ...(options.env || {}) },
  });
  if (result.status !== 0) {
    process.stderr.write(`\npredeploy-check failed at: ${[command, ...args].join(" ")}\n`);
    process.exit(result.status || 1);
  }
}

run("node", ["--check", "server.js"]);
for (const file of fs.readdirSync(path.join(root, "routes")).filter((name) => name.endsWith(".js")).sort()) {
  run("node", ["--check", path.join("routes", file)]);
}
run("npx", ["prisma", "validate"]);
run("npx", ["prisma", "migrate", "status"]);
run("npx", ["prisma", "migrate", "deploy"]);
run("npx", ["prisma", "generate"]);
run("node", ["scripts/migrate-json-state-to-postgres.cjs"]);
run("node", ["scripts/ops-diagnose.cjs", "--json", "--deep", "--log-lines=80"]);
// PLAN-HARDENING.md 2.2: surface .env / ecosystem.config.cjs divergence at deploy time
// without blocking the deploy on it (--warn-only).
run("node", ["scripts/check-env-ecosystem-divergence.cjs", "--warn-only"]);

process.stdout.write("\npredeploy-check complete\n");
