#!/usr/bin/env node
"use strict";
require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
const os = require("node:os");
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

const deployFiles = [
  "server.js",
  "server/assemble.js",
  "server/source.js",
  "api-entry.js",
  "worker-entry.js",
  "ecosystem.config.cjs",
  "package.json",
  "package-lock.json",
  "routes/auth-session.js",
  "routes/auth-yandex.js",
  "routes/marketplaces.js",
  "routes/operations.js",
  "routes/settings.js",
  "routes/static-app.js",
  "routes/system-media.js",
  "routes/users.js",
  "lib/logger.js",
  "lib/postgres.js",
  "lib/static-app.js",
  // Legacy public app (вкладка «Кабинеты» с формой Avito живёт здесь)
  "public/index.html",
  "public/app.js",
  "public/styles.css",
  "scripts/prod-post-deploy-check.cjs",
  "scripts/prod-alert-on-failure.cjs",
  // Цепочка WB: её запускает ежедневный cron (лимит WB — 1000 карточек/сутки)
  "scripts/prod-wb-chain.cjs",
  "scripts/wb-chain-cron.sh",
  "scripts/inspect-bullmq-failed-jobs.cjs",
  "scripts/setup-prod-monitoring.cjs",
  "scripts/run-prod-bullmq-triage.cjs",
  // prisma schema + migrations needed for migrate deploy
  "prisma/schema.prisma",
  ...(() => {
    const migrationsRoot = path.join(path.resolve(__dirname, ".."), "prisma/migrations");
    const files = [];
    for (const dir of fs.readdirSync(migrationsRoot)) {
      const migDir = path.join(migrationsRoot, dir);
      if (!fs.statSync(migDir).isDirectory()) continue;
      for (const file of fs.readdirSync(migDir)) {
        files.push(`prisma/migrations/${dir}/${file}`);
      }
    }
    return files;
  })(),
  // server/parts: the actual business logic assembled at runtime
  ...fs.readdirSync(path.join(path.resolve(__dirname, ".."), "server/parts"))
    .filter((f) => f.endsWith(".js"))
    .map((f) => `server/parts/${f}`),
];

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve()));
    });
  });
}

function openSftp(conn) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => (err ? reject(err) : resolve(sftp)));
  });
}

function sftpPut(sftp, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    const read = fs.createReadStream(localPath);
    const write = sftp.createWriteStream(remotePath);
    write.on("close", resolve);
    write.on("error", reject);
    read.on("error", reject);
    read.pipe(write);
  });
}

async function uploadRelativeFiles(conn, sftp, relativeFiles) {
  for (const rel of relativeFiles) {
    const local = path.join(root, rel);
    if (!fs.existsSync(local)) throw new Error(`Missing deploy file: ${rel}`);
    await sftpPut(sftp, local, `${remoteRoot}/${rel.replace(/\\/g, "/")}`);
  }
}

function readFrontendBundleFiles() {
  const assetsDir = path.join(root, "public/app-modern/assets");
  const assets = fs.readdirSync(assetsDir).map((f) => `public/app-modern/assets/${f}`);
  return ["public/app-modern/index.html", ...assets];
}

const shopDistDir = path.join(root, "shop/dist");
const shopRemoteRoot = "/var/www/magicvibes";

function walkDir(dir) {
  const result = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) result.push(...walkDir(full));
    else result.push(full);
  }
  return result;
}

async function deployShop(conn, sftp) {
  const files = walkDir(shopDistDir);
  const dirs = new Set([shopRemoteRoot]);
  for (const f of files) {
    const rel = path.relative(shopDistDir, path.dirname(f)).split(path.sep).join("/");
    if (rel && rel !== ".") dirs.add(`${shopRemoteRoot}/${rel}`);
  }
  await exec(conn, `mkdir -p ${Array.from(dirs).join(" ")}`);
  for (const local of files) {
    const rel = path.relative(shopDistDir, local).split(path.sep).join("/");
    await sftpPut(sftp, local, `${shopRemoteRoot}/${rel}`);
  }
  console.log(`✓ shop: uploaded ${files.length} files → ${shopRemoteRoot}`);
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
    const remoteDirs = new Set([`${remoteRoot}/scripts`, `${remoteRoot}/routes`, `${remoteRoot}/lib`, `${remoteRoot}/public/app-modern/assets`]);
    for (const rel of [...deployFiles, ...readFrontendBundleFiles()]) {
      remoteDirs.add(path.posix.dirname(`${remoteRoot}/${rel.replace(/\\/g, "/")}`));
    }
    await exec(conn, `mkdir -p ${Array.from(remoteDirs).join(" ")}`);
    const sftp = await openSftp(conn);

    console.log("Deploying backend manifest...");
    await uploadRelativeFiles(conn, sftp, deployFiles);

    if (withDedupe) {
      console.log("Deploying dedupe script...");
      await uploadRelativeFiles(conn, sftp, [
        "scripts/dedupe-warehouse-products.cjs",
        "scripts/audit-marketplace-labels.cjs",
      ]);
    }

    console.log("Deploying frontend bundle...");
    await uploadRelativeFiles(conn, sftp, readFrontendBundleFiles());

    console.log("Deploying shop (magicvibes.ru)...");
    await deployShop(conn, sftp);

    await exec(conn, [
      `cd ${remoteRoot}`,
      "npm ci --omit=dev",
      "node node_modules/prisma/build/index.js generate 2>&1 | tail -5",
      "node node_modules/prisma/build/index.js migrate deploy 2>&1 | tail -10",
      "pm2 delete davidsklad 2>/dev/null || true",
      "pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env || pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env",
      "pm2 save",
      "sleep 25",
      "pm2 describe davidsklad-api | grep -E 'max memory|node args|status|restarts' || true",
      "pm2 describe davidsklad-worker | grep -E 'max memory|node args|status|restarts' || true",
      "pm2 list",
      "free -h | head -2",
      "echo '=== pm2 error log api (last 25 lines) ==='",
      "pm2 logs davidsklad-api --lines 25 --nostream --err || true",
      "echo '=== pm2 error log worker (last 25 lines) ==='",
      "pm2 logs davidsklad-worker --lines 25 --nostream --err || true",
      "echo '=== post-deploy check (blocking) ==='",
      "node scripts/prod-post-deploy-check.cjs",
    ].join(" && "));

    if (withRepairLinked) {
      console.log("Running linked warehouse catalog repair on server...");
      await exec(conn, `mkdir -p ${remoteRoot}/scripts`);
      const repairSftp = await openSftp(conn);
      await sftpPut(
        repairSftp,
        path.join(root, "scripts/repair-linked-warehouse-catalog.cjs"),
        `${remoteRoot}/scripts/repair-linked-warehouse-catalog.cjs`,
      );
      await exec(conn, `cd ${remoteRoot} && node scripts/repair-linked-warehouse-catalog.cjs --apply`);
    }

    if (withDedupe) {
      console.log("Running warehouse dedupe on server...");
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
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
