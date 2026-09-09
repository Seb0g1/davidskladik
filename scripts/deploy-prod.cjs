#!/usr/bin/env node
"use strict";
require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");
const os = require("node:os");

const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);

if (!sshKeyPath) {
  console.error("SSH key not found at ~/.ssh/davidsklad_deploy — set DEPLOY_SSH_KEY env var");
  process.exit(1);
}

const SSH_HOST = "root@81.17.154.153";
const remoteRoot = "/var/www/davidsklad/davidskladik";
const root = path.resolve(__dirname, "..");

const withDedupe = process.argv.includes("--with-dedupe");
const withRepairLinked = process.argv.includes("--repair-linked");
const skipLocalChecks = process.argv.includes("--skip-local-checks");
const skipPush = process.argv.includes("--skip-push");

function run(cmd, opts = {}) {
  execSync(cmd, { cwd: root, stdio: "inherit", ...opts });
}

function ssh(remoteCmd) {
  run(
    `ssh -i "${sshKeyPath}" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ${SSH_HOST} "${remoteCmd.replace(/"/g, '\\"')}"`,
  );
}

// ── 1. Local checks ───────────────────────────────────────────────────────────
if (!skipLocalChecks) {
  console.log("\n▶ npm test...");
  run("npm test");
  console.log("\n▶ npm run build (frontend)...");
  run("npm run build");
  console.log("\n▶ npm run build (shop)...");
  run("npm run build", { cwd: path.join(root, "shop") });
}

// ── 2. Tag + push to GitHub ───────────────────────────────────────────────────
const tag = `prod-${new Date().toISOString().slice(0, 10)}`;
try { run(`git tag -f ${tag}`); } catch {}

if (!skipPush) {
  console.log("\n▶ git push origin main...");
  run("git push origin main");
  console.log("✓ Pushed to GitHub");
}

// ── 3. Server: pull + install + migrate + reload ──────────────────────────────
console.log("\n▶ Server: git pull + npm ci + prisma + pm2 reload...");

const serverCmd = [
  `cd ${remoteRoot}`,
  "git pull origin main 2>&1",
  "echo '✓ git pull'",
  "npm ci --omit=dev 2>&1 | tail -5",
  "echo '✓ npm ci'",
  "node node_modules/prisma/build/index.js generate 2>&1 | tail -3",
  "node node_modules/prisma/build/index.js migrate deploy 2>&1 | tail -5",
  "echo '✓ prisma'",
  "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env || pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
  "pm2 save",
  "echo '✓ pm2 reloaded'",
  "sleep 10",
  "pm2 list",
  "echo '=== api errors (last 15) ==='",
  "pm2 logs davidsklad-api --lines 15 --nostream --err 2>/dev/null || true",
  "echo '=== post-deploy check ==='",
  "node scripts/prod-post-deploy-check.cjs",
].join(" && ");

ssh(serverCmd);

// ── 4. Optional extras ────────────────────────────────────────────────────────
if (withRepairLinked) {
  console.log("\n▶ repair-linked-warehouse-catalog...");
  ssh(`cd ${remoteRoot} && node scripts/repair-linked-warehouse-catalog.cjs --apply`);
}

if (withDedupe) {
  console.log("\n▶ dedupe...");
  ssh([
    `cd ${remoteRoot}`,
    "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=30",
    "for pass in $(seq 1 25); do echo \"pass $pass/25\"; node scripts/dedupe-warehouse-products.cjs --apply --limit=3000 || exit 1; done",
    "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=100000",
  ].join(" && "));
}

console.log("\n✅ Deploy complete!");
