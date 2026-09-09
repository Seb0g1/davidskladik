#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const host = process.env.DEPLOY_HOST || "127.0.0.1";
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";
const dryRun = !process.argv.includes("--run");
const rateArg = process.argv.find((a) => a.startsWith("--rate="));
const rateOverride = rateArg ? Number(rateArg.split("=")[1]) : 0;

if (!appPassword) { console.error("APP_PASSWORD missing in .env"); process.exit(1); }

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: host, port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
        "Origin": `http://${host}:${port}`,
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* raw */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = Array.isArray(headers["set-cookie"]) ? headers["set-cookie"] : [headers["set-cookie"]].filter(Boolean);
  const s = list.find((item) => String(item).includes("connect.sid=") || String(item).includes("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

async function main() {
  console.log(`Mode: ${dryRun ? "DRY-RUN (pass --run to apply)" : "REAL — will update DB"}`);
  console.log(`Connecting to ${host}:${port} ...`);

  const lr = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(lr.headers);
  if (!cookie || lr.status !== 200) {
    console.error("Login failed:", lr.status, lr.body);
    process.exit(1);
  }
  console.log("Logged in OK");

  const url = `/api/supplier-ledger/migrate-usd-payments${dryRun ? "?dry=true" : ""}`;
  const body = { dry: dryRun, ...(rateOverride > 0 ? { rate: rateOverride } : {}) };
  if (rateOverride > 0) console.log(`Rate override: ${rateOverride} ₽/$`);
  const res = await request("POST", url, { cookie, body });
  const data = res.body;

  if (!data.ok) {
    console.error("Migration failed:", res.status, data);
    process.exit(1);
  }

  console.log(`\nRate used: ${data.rate} ₽/$`);
  console.log(`New (RUB→USD): ${data.migrated} | Remigrated: ${data.remigrated ?? 0} | Skipped: ${data.skipped}`);
  if (data.details?.length) {
    console.log("\nEntries:");
    for (const d of data.details) {
      console.log(`  ${d.supplierName} | ${d.entryType} | ${d.amountRub} ₽ → ${d.amountUsd} $`);
    }
  }
  if (dryRun) {
    console.log("\n✓ Dry-run complete. Run with --run to apply changes.");
  } else {
    console.log("\n✓ Migration applied.");
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
