#!/usr/bin/env node
/**
 * One-time setup for magicvibes.ru on the production server.
 *
 * Assumes:
 *  - The repo is already cloned at /var/www/davidsklad/davidskladik
 *  - Node.js ≥ 18 and PM2 are installed on the server
 *  - certbot/Let's Encrypt cert for magicvibes.ru already exists
 *  - SSH key at ~/.ssh/davidsklad_deploy (or DEPLOY_SSH_KEY env var)
 *
 * Run once:
 *   node scripts/setup-magicvibes.cjs
 *
 * Subsequent deploys use the normal deploy script:
 *   DEPLOY_PASSWORD=... node scripts/deploy-prod.cjs --update-nginx
 */
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

function run(cmd, opts = {}) {
  execSync(cmd, { cwd: root, stdio: "inherit", ...opts });
}

function ssh(remoteCmd) {
  run(
    `ssh -i "${sshKeyPath}" -o StrictHostKeyChecking=no -o ConnectTimeout=30 ${SSH_HOST} "${remoteCmd.replace(/"/g, '\\"')}"`,
  );
}

function scp(localPath, remotePath) {
  run(`scp -i "${sshKeyPath}" -o StrictHostKeyChecking=no "${localPath}" ${SSH_HOST}:"${remotePath}"`);
}

console.log("\n▶ Step 1 — Build shop-next locally to verify it compiles...");
run("npm run build", { cwd: path.join(root, "shop-next") });
console.log("✓ Local build OK\n");

console.log("▶ Step 2 — Server: install shop-next deps + build + start PM2 process...");
ssh([
  `cd ${remoteRoot}/shop-next`,
  "npm ci --omit=dev 2>&1 | tail -5",
  "echo '✓ npm ci'",
  "npm run build 2>&1 | tail -15",
  "echo '✓ build'",
  `cd ${remoteRoot}`,
  "pm2 start ecosystem.config.cjs --only shop-next --update-env",
  "pm2 save",
  "sleep 5",
  "pm2 show shop-next",
].join(" && "));
console.log("✓ shop-next started\n");

console.log("▶ Step 3 — Deploy nginx config...");
const nginxConf = path.join(__dirname, "magicvibes_nginx.conf");
const remoteConf = "/etc/nginx/sites-available/magicvibes";
scp(nginxConf, remoteConf);
ssh([
  `ln -sf ${remoteConf} /etc/nginx/sites-enabled/magicvibes`,
  // Remove old static-file config if it still exists
  "rm -f /etc/nginx/sites-enabled/magicvibes.ru 2>/dev/null || true",
  "nginx -t",
  "systemctl reload nginx",
  "echo '✓ nginx reloaded'",
].join(" && "));
console.log("✓ nginx config deployed\n");

console.log("▶ Step 4 — Smoke check...");
ssh(`curl -sI https://magicvibes.ru/ | head -5`);

console.log("\n✅ magicvibes.ru setup complete!");
console.log("   Next deploys: node scripts/deploy-prod.cjs  (or with --update-nginx to also update nginx)");
