#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { Client } = require("ssh2");

require("dotenv").config();

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = process.env.DEPLOY_SSH_KEY || (fs.existsSync(defaultKeyPath) ? defaultKeyPath : null);
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

if (!privateKey && !password) {
  console.error("Either DEPLOY_PASSWORD or SSH key at ~/.ssh/davidsklad_deploy is required");
  process.exit(1);
}

const dryRun = !process.argv.includes("--run");
const RATE = 90;
const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";

if (!APP_PASSWORD) {
  console.error("APP_PASSWORD missing in .env");
  process.exit(1);
}

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      let errOut = "";
      stream.on("data", (d) => { out += d; });
      stream.stderr.on("data", (d) => { errOut += d; });
      stream.on("close", () => resolve({ out, errOut }));
    });
  });
}

async function main() {
  console.log(`Mode: ${dryRun ? "DRY-RUN (pass --run to apply)" : "REAL — will update DB"}`);
  console.log(`Rate: ${RATE} ₽/$`);

  const conn = new Client();
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) cfg.privateKey = privateKey;
    else cfg.password = password;
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });

  try {
    // 1. Login
    const loginCmd = `curl -s -c /tmp/mig_cookie.txt -X POST http://127.0.0.1:3000/api/login \
      -H 'Content-Type: application/json' \
      -H 'Origin: http://127.0.0.1:3000' \
      -d '{"username":"${APP_USER}","password":"${APP_PASSWORD}"}'`;
    const { out: loginOut } = await exec(conn, loginCmd);
    let loginData;
    try { loginData = JSON.parse(loginOut); } catch { loginData = {}; }
    if (loginData.ok !== true) {
      console.error("Login failed:", loginOut);
      process.exit(1);
    }
    console.log("Logged in OK");

    // 2. Call migration endpoint
    const dryParam = dryRun ? "?dry=true" : "";
    const migrateCmd = `curl -s -b /tmp/mig_cookie.txt -X POST "http://127.0.0.1:3000/api/supplier-ledger/migrate-usd-payments${dryParam}" \
      -H 'Content-Type: application/json' \
      -H 'Origin: http://127.0.0.1:3000' \
      -d '{"dry":${dryRun},"rate":${RATE}}'`;
    const { out: migOut } = await exec(conn, migrateCmd);
    let data;
    try { data = JSON.parse(migOut); } catch { data = {}; }

    if (!data.ok) {
      console.error("Migration failed (raw):", migOut.slice(0, 2000));
      process.exit(1);
    }

    console.log(`\nRate used: ${data.rate} ₽/$`);
    console.log(`New (RUB→USD): ${data.migrated ?? 0} | Remigrated: ${data.remigrated ?? 0} | Skipped: ${data.skipped ?? 0}`);
    if (Array.isArray(data.details) && data.details.length) {
      console.log("\nEntries:");
      for (const d of data.details) {
        const tag = d.phase === "remigrate" ? `[пересчёт: ${d.oldUsd}$ → ${d.amountUsd}$]` : `[${d.amountRub}₽ → ${d.amountUsd}$]`;
        console.log(`  ${d.supplierName} | ${d.entryType} | ${tag}`);
      }
    }

    if (dryRun) {
      console.log("\n✓ Dry-run complete. Run with --run to apply.");
    } else {
      console.log("\n✓ Migration applied.");
    }

    // Cleanup
    await exec(conn, "rm -f /tmp/mig_cookie.txt");
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
