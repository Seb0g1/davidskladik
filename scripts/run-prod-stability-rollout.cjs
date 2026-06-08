#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const path = require("node:path");

const root = path.resolve(__dirname, "..");

function run(script, args = []) {
  console.log(`\n=== ${script} ${args.join(" ")} ===`);
  const result = spawnSync("node", [path.join(root, "scripts", script), ...args], {
    cwd: root,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status) process.exit(result.status);
}

run("deploy-prod.cjs", ["--skip-local-checks"]);
run("setup-prod-monitoring.cjs");
run("run-prod-bullmq-triage.cjs", ["--limit=120"]);
