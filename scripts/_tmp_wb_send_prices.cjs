#!/usr/bin/env node
"use strict";
// Отправка цен WB через HTTP API на production
// Usage: DEPLOY_PASSWORD=... node scripts/_tmp_wb_send_prices.cjs
const https = require("https");

const BASE = "https://davidsklad.ru";
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function request(method, path, body, cookie) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const req = https.request(`${BASE}${path}`, {
      method,
      headers: {
        "Content-Type": "application/json",
        ...(cookie ? { Cookie: cookie } : {}),
        ...(payload ? { "Content-Length": Buffer.byteLength(payload) } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => data += c);
      res.on("end", () => {
        const cookies = [].concat(res.headers["set-cookie"] || []).join("; ");
        try { resolve({ status: res.statusCode, body: JSON.parse(data), cookies }); }
        catch { resolve({ status: res.statusCode, body: data, cookies }); }
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function main() {
  // Login
  const login = await request("POST", "/api/login", { password });
  if (login.status !== 200) { console.error("Login failed:", login.body); process.exit(1); }
  const cookie = login.cookies;
  console.log("✓ Login OK");

  // Check sync status first
  const status = await request("GET", "/api/wb/sync/status", null, cookie);
  console.log("\n=== WB Sync Status ===");
  const s = status.body;
  if (s.lastResult) {
    console.log("Last sync:", s.lastResult.at);
    console.log("Status:", s.lastResult.status);
    console.log("Cards:", s.lastResult.cards);
    console.log("Prices sent:", s.lastResult.pricesSent);
    console.log("Skipped manual:", s.lastResult.skippedManual);
    if (s.lastResult.pricesError) console.log("Prices error:", s.lastResult.pricesError.slice(0, 100));
  }
  console.log("Next sync:", s.nextRunAt);
  console.log("Running:", s.running);

  // Send prices
  console.log("\n=== Sending WB Prices ===");
  const send = await request("POST", "/api/wb/prices/send", { dryRun: false }, cookie);
  console.log("Response status:", send.status);
  console.log("Response:", JSON.stringify(send.body, null, 2));

  if (send.status === 202) {
    console.log("\n✓ Price send started in background. Monitor /api/wb/prices/status");
  } else if (send.status === 409) {
    console.log("\n⚠ Price send already running");
  } else {
    console.log("\n✗ Unexpected status:", send.status);
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
