#!/usr/bin/env node
"use strict";
require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");
const { Client } = require("ssh2");
const os = require("node:os");

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

if (!privateKey && !password) {
  console.error("Either DEPLOY_PASSWORD or SSH key at ~/.ssh/davidsklad_deploy is required");
  process.exit(1);
}
if (privateKey) {
  console.log(`Using SSH key: ${sshKeyPath}`);
} else {
  console.log("Using password auth");
}

const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const withDedupe = process.argv.includes("--with-dedupe");
const withRepairLinked = process.argv.includes("--repair-linked");
const skipLocalChecks = process.argv.includes("--skip-local-checks");
const skipPush = process.argv.includes("--skip-push");

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`Remote command failed (exit ${code}): ${command.slice(0, 120)}`)) : resolve()));
    });
  });
}

function runLocalPreDeploy() {
  if (skipLocalChecks) {
    console.log("Skipping local npm test + build (--skip-local-checks)");
    return;
  }
  console.log("Running npm test...");
  execSync("npm test", { cwd: root, stdio: "inherit" });
  console.log("Running npm run build...");
  execSync("npm run build", { cwd: root, stdio: "inherit" });
  console.log("Building shop (magicvibes.ru)...");
  execSync("npm run build", { cwd: path.join(root, "shop"), stdio: "inherit" });
}

function gitPushToGithub() {
  if (skipPush) {
    console.log("Skipping git push (--skip-push)");
    return;
  }
  console.log("Pushing to GitHub...");
  execSync("git push origin main", { cwd: root, stdio: "inherit" });
  console.log("✓ Pushed to GitHub");
}

function tagProdRelease() {
  const tag = `prod-${new Date().toISOString().slice(0, 10)}`;
  try {
    execSync(`git tag -f ${tag}`, { cwd: root, stdio: "inherit" });
    console.log(`Tagged ${tag} (local rollback marker)`);
  } catch (error) {
    console.warn(`Could not create tag ${tag}: ${error.message}`);
  }
}

async function main() {
  runLocalPreDeploy();
  tagProdRelease();
  gitPushToGithub();

  const conn = new Client();
  await new Promise((resolve, reject) => {
    const connectConfig = {
      host: "81.17.154.153",
      username: "root",
      readyTimeout: 60000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    };
    if (privateKey) {
      connectConfig.privateKey = privateKey;
    } else {
      connectConfig.password = password;
    }
    conn.on("ready", resolve).on("error", reject).connect(connectConfig);
  });

  try {
    console.log("Pulling latest code on server...");
    await exec(conn, [
      `cd ${remoteRoot}`,
      // Pull from GitHub — all files including public/app-modern and server/parts
      "git pull origin main 2>&1",
      "echo '✓ git pull done'",
      // Install only production deps; skip if package.json unchanged (npm ci is idempotent)
      "npm ci --omit=dev 2>&1 | tail -5",
      "echo '✓ npm ci done'",
      // Prisma generate + migrate
      "node node_modules/prisma/build/index.js generate 2>&1 | tail -5",
      "node node_modules/prisma/build/index.js migrate deploy 2>&1 | tail -10",
      "echo '✓ prisma done'",
      // Reload PM2 (zero-downtime reload; falls back to restart if config changed)
      "pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env || pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "pm2 save",
      "echo '✓ pm2 reloaded'",
      // Wait for workers to settle then check status
      "sleep 20",
      "pm2 list",
      "free -h | head -2",
      "echo '=== api errors (last 20) ==='",
      "pm2 logs davidsklad-api --lines 20 --nostream --err || true",
      "echo '=== worker errors (last 20) ==='",
      "pm2 logs davidsklad-worker --lines 20 --nostream --err || true",
      "echo '=== post-deploy check ==='",
      "node scripts/prod-post-deploy-check.cjs",
    ].join(" && "));

    if (withRepairLinked) {
      console.log("Running linked warehouse catalog repair...");
      await exec(conn, `cd ${remoteRoot} && node scripts/repair-linked-warehouse-catalog.cjs --apply`);
    }

    if (withDedupe) {
      console.log("Running warehouse dedupe...");
      await exec(conn, [
        `cd ${remoteRoot}`,
        "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=30",
        "for pass in $(seq 1 25); do echo \"Dedupe apply pass $pass/25...\"; node scripts/dedupe-warehouse-products.cjs --apply --limit=3000 || exit 1; done",
        "node scripts/dedupe-warehouse-products.cjs --dry-run --limit=100000",
        "node scripts/audit-marketplace-labels.cjs --limit=400",
      ].join(" && "));
    }
  } finally {
    conn.end();
  }

  console.log("\n✅ Deploy complete!");
}

main().catch((error) => {
  console.error("Deploy failed:", error.message);
  process.exit(1);
});
