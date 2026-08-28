#!/usr/bin/env node
"use strict";
// Runs on prod: diagnoses + repairs Yandex card issues via local API.
// Usage: node scripts/diag-yandex-repair.cjs [--confirm] [--repair] [--all]
//   --confirm  : confirm price quarantine (otherwise just reports)
//   --repair   : run content/descriptions repair
//   --all      : both confirm + repair

require("dotenv").config();

const http = require("http");
const args = new Set(process.argv.slice(2));
const doConfirm = args.has("--confirm") || args.has("--all");
const doRepair  = args.has("--repair")  || args.has("--all");

const APP_USER     = process.env.APP_USER     || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";

let sessionCookie = "";

async function rawRequest(method, path, body, timeoutMs = 300000) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : undefined;
    const req = http.request({
      hostname: "127.0.0.1",
      port: 3000,
      path,
      method,
      headers: {
        Cookie: sessionCookie,
        "Content-Type": "application/json",
        ...(payload ? { "Content-Length": Buffer.byteLength(payload) } : {}),
      },
      timeout: timeoutMs,
    }, (res) => {
      // Capture Set-Cookie on first login
      const setCookie = res.headers["set-cookie"];
      if (setCookie) {
        const pm = setCookie.find((c) => c.startsWith("pm_session="));
        if (pm) sessionCookie = pm.split(";")[0];
      }
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString();
        try { resolve({ status: res.statusCode, body: JSON.parse(text) }); }
        catch { resolve({ status: res.statusCode, body: { _raw: text.slice(0, 200) } }); }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error(`timeout: ${method} ${path}`)); });
    if (payload) req.write(payload);
    req.end();
  });
}

async function api(method, path, body, timeoutMs = 300000) {
  const res = await rawRequest(method, path, body, timeoutMs);
  if (res.status === 401 || res.status === 403) throw new Error(`Auth failed on ${method} ${path}: ${JSON.stringify(res.body)}`);
  return res.body;
}

async function login() {
  const res = await rawRequest("POST", "/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (!res.body?.ok) throw new Error(`Login failed: ${JSON.stringify(res.body)}`);
  console.log(`Logged in as ${res.body.username} (${res.body.role})`);
}

function sep(label) { console.log(`\n${"─".repeat(60)}\n${label}\n${"─".repeat(60)}`); }

async function main() {
  await login();

  // ── 1. Price quarantine ───────────────────────────────────────────────────
  sep("1. PRICE QUARANTINE");
  const qInfo = await api("GET", "/api/yandex/price-quarantine");
  console.log(`Total in quarantine: ${qInfo.total ?? 0}`);
  for (const s of qInfo.byShop || []) {
    console.log(`  Shop ${s.shopId}: ${s.count} items`);
    if (s.sample?.length) {
      console.log("  Sample:", s.sample.slice(0, 3).map((i) => `${i.offerId}→${i.price?.value}`).join(", "));
    }
  }

  if ((qInfo.total ?? 0) > 0) {
    if (doConfirm) {
      console.log("Confirming all quarantined prices...");
      const cRes = await api("POST", "/api/yandex/price-quarantine/confirm", {});
      console.log(`✓ Confirmed: ${cRes.total ?? "?"}`);
      for (const s of cRes.byShop || []) {
        console.log(`  Shop ${s.shopId}: confirmed ${s.confirmed}`);
      }
    } else {
      console.log("  → pass --confirm to release from quarantine");
    }
  }

  if (!doRepair) {
    console.log("\n→ pass --repair to run content/description repairs");
    return;
  }

  // ── 2. repair-yandex-content ──────────────────────────────────────────────
  sep("2. REPAIR CONTENT (vendor / dims / pictures)");
  const dryContent = await api("POST", "/api/ozon-yandex-import/repair-yandex-content", { dryRun: true, limit: 50000 });
  console.log(`Total Yandex products: ${dryContent.total ?? "?"}, candidates: ${dryContent.candidates ?? "?"}`);
  if ((dryContent.candidates ?? 0) > 0) {
    console.log("Running real content repair...");
    const realContent = await api("POST", "/api/ozon-yandex-import/repair-yandex-content", { dryRun: false, limit: 50000 }, 600000);
    console.log(`Sent: ${realContent.sent ?? "?"}, skipped: ${realContent.skipped ?? "?"}`);
  } else {
    console.log("No content candidates — all good.");
  }

  // ── 3. repair-yandex-descriptions ────────────────────────────────────────
  sep("3. REPAIR DESCRIPTIONS");
  const dryDesc = await api("POST", "/api/ozon-yandex-import/repair-yandex-descriptions", { dryRun: true, limit: 50000 });
  console.log(`Description candidates: ${dryDesc.candidates ?? "?"}`);
  if ((dryDesc.candidates ?? 0) > 0) {
    console.log("Running real description repair (slow — fetches from Ozon API)...");
    const realDesc = await api("POST", "/api/ozon-yandex-import/repair-yandex-descriptions", { dryRun: false, limit: 5000 }, 600000);
    console.log(`Updated: ${realDesc.updated ?? "?"}, apiCalls: ${realDesc.apiCalls ?? "?"}, errors: ${realDesc.apiErrors ?? "?"}`);
  } else {
    console.log("No description candidates — all good.");
  }

  // ── 4. fix-yandex-categories: error report ───────────────────────────────
  sep("4. CARD ERROR REPORT + CATEGORY FIX");
  console.log("Fetching card error report (pulls offer-cards from Yandex — may take a minute)...");
  const catReport = await api("POST", "/api/ozon-yandex-import/fix-yandex-categories", { dryRun: true, errorsReport: true }, 600000);
  console.log(`Total offers: ${catReport.totalOffers ?? "?"}, cards with errors: ${catReport.cardsWithErrors ?? "?"}`);
  if ((catReport.topErrors || []).length > 0) {
    console.log("Top errors:");
    for (const e of catReport.topErrors.slice(0, 15)) {
      console.log(`  ${e.count}×  ${e.message}`);
    }
  }
  if ((catReport.topWarnings || []).length > 0) {
    console.log("Top warnings (top 5):");
    for (const w of catReport.topWarnings.slice(0, 5)) {
      console.log(`  ${w.count}×  ${w.message}`);
    }
  }
  if ((catReport.fixesByTransition || []).length > 0) {
    console.log("\nCategory fixes planned:");
    for (const f of catReport.fixesByTransition.slice(0, 10)) {
      console.log(`  ${f.count}×  ${f.transition}`);
    }
    console.log("Running category fix...");
    const catFix = await api("POST", "/api/ozon-yandex-import/fix-yandex-categories", { dryRun: false, errorsReport: false }, 600000);
    console.log(`Category fix done:`, JSON.stringify(catFix).slice(0, 300));
  } else {
    console.log("No category fixes needed.");
  }

  // ── 5. Fix "Цена сильно снизилась" (price-drop quarantine via card errors) ──
  sep("5. CONFIRM PRICE-DROP QUARANTINE (\"Цена сильно снизилась\")");
  console.log("Scanning all card errors for price-drop flag (slow — may take 5-10 min)...");
  const pdRes = await api("POST", "/api/yandex/price-quarantine/confirm-price-drop-errors", {}, 900000);
  for (const s of pdRes.byShop || []) {
    console.log(`  Shop ${s.shopId}: scanned ${s.scanned}, price-drop errors: ${s.priceDropErrors ?? 0}, confirmed: ${s.confirmed}`);
  }
  console.log(`Total confirmed: ${pdRes.totalConfirmed ?? 0}`);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
