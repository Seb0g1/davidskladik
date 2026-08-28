#!/usr/bin/env node
"use strict";
// Runs on prod: confirms prices for all Yandex cards with "Цена сильно снизилась" error.
// This is a targeted script — only runs the price-drop quarantine fix.

require("dotenv").config();

const http = require("http");
const APP_USER     = process.env.APP_USER     || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";
let sessionCookie = "";

async function rawRequest(method, path, body, timeoutMs = 600000) {
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

async function api(method, path, body, timeoutMs = 600000) {
  const res = await rawRequest(method, path, body, timeoutMs);
  if (res.status === 401 || res.status === 403) throw new Error(`Auth failed: ${JSON.stringify(res.body)}`);
  return res.body;
}

async function main() {
  // Login
  const loginRes = await rawRequest("POST", "/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (!loginRes.body?.ok) throw new Error(`Login failed: ${JSON.stringify(loginRes.body)}`);
  console.log(`Logged in as ${loginRes.body.username} (${loginRes.body.role})`);

  console.log("\nScanning card errors for \"Цена сильно снизилась\"...");
  console.log("(This scans all 12k+ offer-cards from Yandex — may take 10-20 minutes)");

  const res = await api("POST", "/api/yandex/price-quarantine/confirm-price-drop-errors", {}, 1800000);
  if (res.error) { console.error("Error:", res.error); process.exit(1); }

  for (const s of res.byShop || []) {
    console.log(`Shop ${s.shopId}: scanned ${s.scanned}, price-drop errors: ${s.priceDropErrors ?? 0}, confirmed: ${s.confirmed}`);
  }
  console.log(`\nTotal confirmed: ${res.totalConfirmed ?? 0}`);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
