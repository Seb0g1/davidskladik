#!/usr/bin/env node
"use strict";

const path = require("node:path");
const { execSync } = require("node:child_process");

async function main() {
  console.log("=== reload worker (ecosystem) ===");
  execSync("pm2 reload ecosystem.config.cjs --only davidsklad-worker --update-env", { stdio: "inherit" });
  await new Promise((r) => setTimeout(r, 15000));

  console.log("\n=== bullmq failed cleanup ===");
  try {
    execSync("node scripts/inspect-bullmq-failed-jobs.cjs --limit=80 --remove-failed", {
      cwd: path.join(__dirname, ".."),
      stdio: "inherit",
    });
  } catch (error) {
    console.warn("bullmq cleanup warn:", error.message);
  }

  console.log("\n=== worker env check ===");
  try {
    execSync("pm2 env davidsklad-worker 2>/dev/null | grep -E 'BACKGROUND_JOBS|BULLMQ|SERVER_ROLE' || true", { stdio: "inherit" });
  } catch { /* optional */ }

  console.log("\n=== monitor snapshot ===");
  execSync("node scripts/_prod-monitor-progress.cjs", {
    cwd: path.join(__dirname, ".."),
    stdio: "inherit",
  });
}

main().catch((error) => {
  console.error("UNSTICK_WORKER_FAILED:", error.message);
  process.exit(1);
});
