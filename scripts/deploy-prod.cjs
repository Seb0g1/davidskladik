#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { execSync } = require("node:child_process");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
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
  "routes/marketplaces.js",
  "routes/operations.js",
  "routes/settings.js",
  "routes/static-app.js",
  "routes/system-media.js",
  "routes/users.js",
  "lib/logger.js",
  "lib/postgres.js",
  "lib/static-app.js",
  "scripts/prod-post-deploy-check.cjs",
  "scripts/prod-alert-on-failure.cjs",
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
  const indexHtml = fs.readFileSync(path.join(root, "public/app-modern/index.html"), "utf8");
  const assets = [];
  for (const match of indexHtml.matchAll(/\/app-modern\/(assets\/[^"']+)/g)) {
    assets.push(`public/app-modern/${match[1]}`);
  }
  return ["public/app-modern/index.html", ...Array.from(new Set(assets))];
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
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 60000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    });
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
