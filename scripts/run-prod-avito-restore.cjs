"use strict";
// One-time script: restore warehouse_products.images from Ozon API for all
// products where the column is empty, then re-apply avito import.
const { Client } = require("ssh2");
require("dotenv").config();

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, { timeout = 180000 } = {}) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    let output = "";
    const timer = setTimeout(() => { client.end(); reject(new Error("SSH timeout")); }, timeout);
    client.on("ready", () => {
      client.exec(cmd, (err, stream) => {
        if (err) { clearTimeout(timer); client.end(); reject(err); return; }
        stream.on("data", (d) => { output += d; });
        stream.stderr.on("data", (d) => { output += d; });
        stream.on("close", () => { clearTimeout(timer); client.end(); resolve(output); });
      });
    }).on("error", (e) => { clearTimeout(timer); reject(e); })
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

const appUser = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD;
if (!appPassword) { console.error("APP_PASSWORD required"); process.exit(1); }

let sessionReady = false;

async function ensureSession() {
  if (sessionReady) return;
  const loginOut = await sshExec(
    `curl -s -c /tmp/ds-restore.txt -X POST http://localhost:3000/api/login -H 'Content-Type: application/json' -d '{"username":"${appUser}","password":"${appPassword}"}'`,
  );
  if (!loginOut.includes('"ok"') && !loginOut.includes('"user"')) {
    throw new Error("Login failed: " + loginOut.slice(0, 300));
  }
  sessionReady = true;
  console.log("  logged in:", loginOut.slice(0, 80));
}

async function apiCall(endpoint, body = {}) {
  await ensureSession();
  const bodyStr = JSON.stringify(body).replace(/'/g, "'\\''");
  const result = await sshExec(
    `curl -s -b /tmp/ds-restore.txt -X POST http://localhost:3000${endpoint} -H 'Content-Type: application/json' -d '${bodyStr}'`,
    { timeout: 120000 },
  );
  return result;
}

async function main() {
  console.log("=== Avito listings restore ===\n");

  // Step 1: Restore DB images in batches
  const totalBatches = 30; // 30 × 500 = 15000 products max
  let totalUpdated = 0;
  let remaining = Infinity;
  for (let i = 0; i < totalBatches && remaining > 0; i++) {
    console.log(`[${i + 1}/${totalBatches}] Restoring DB images (batch 500)...`);
    const raw = await apiCall("/api/avito/images/db-restore", { limit: 500 });
    let result;
    try { result = JSON.parse(raw); } catch (e) { console.error("Parse error:", raw.slice(0, 200)); break; }
    console.log(`  updated=${result.updated} remaining=${result.remaining} apiErrors=${result.apiErrors || 0}`);
    if (result.lastError) console.log("  lastError:", result.lastError);
    totalUpdated += result.updated || 0;
    remaining = result.remaining || 0;
    if (result.status === "done" || result.updated === 0) break;
  }
  console.log(`\nTotal DB images restored: ${totalUpdated}, remaining: ${remaining}`);

  // Step 2: Re-apply avito import to restore listings
  console.log("\n=== Re-applying Avito import ===");
  const applyRaw = await apiCall("/api/avito/import/apply", {});
  let applyResult;
  try { applyResult = JSON.parse(applyRaw); } catch (e) { console.error("Parse error:", applyRaw.slice(0, 200)); return; }
  console.log("Apply result:", JSON.stringify({
    matched: applyResult.matchedCount,
    skipped: applyResult.skippedCount,
    created: applyResult.created,
    updated: applyResult.updated,
    totalListings: applyResult.totalListings,
    by_reason: applyResult.skippedByReason,
  }));
}

main().catch((e) => { console.error(e.message); process.exit(1); });
