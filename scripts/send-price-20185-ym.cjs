#!/usr/bin/env node
"use strict";
// Sends updated YM price for product 20185 (yandex-9a297bbf37eaa53c61a4ba39).
// Reads credentials from .env, makes HTTP calls to localhost:3000.

require("dotenv").config();
const http = require("http");

const PRODUCT_ID = "yandex-9a297bbf37eaa53c61a4ba39";
const HOST = "localhost";
const PORT = 3000;
const USER = process.env.APP_USER;
const PASS = process.env.APP_PASSWORD;

if (!USER || !PASS) throw new Error("APP_USER/APP_PASSWORD not set in .env");

function post(path, body, cookieHeader) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const headers = { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) };
    if (cookieHeader) headers["Cookie"] = cookieHeader;
    const req = http.request({ host: HOST, port: PORT, path, method: "POST", headers }, (res) => {
      let raw = "";
      res.on("data", (d) => { raw += d; });
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body: raw }));
    });
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  // 1. Login
  console.log("Logging in as", USER);
  const login = await post("/api/login", { username: USER, password: PASS });
  if (login.status !== 200) throw new Error(`Login failed: ${login.status} ${login.body}`);
  const setCookie = login.headers["set-cookie"] || [];
  const sessionCookie = setCookie.map((c) => c.split(";")[0]).join("; ");
  console.log("Login OK, session cookie obtained");

  // 2. Send price
  console.log(`\nSending YM price for ${PRODUCT_ID} (force=true, livePriceMaster=false)...`);
  const send = await post(
    "/api/warehouse/prices/send",
    { confirmed: true, productIds: [PRODUCT_ID], marketplace: "yandex", force: true, livePriceMaster: false },
    sessionCookie,
  );
  console.log(`Response status: ${send.status}`);
  try {
    const result = JSON.parse(send.body);
    const summary = {
      sent: result.sent,
      skipped: result.skipped,
      errors: result.errors,
      results: (result.results || []).map((r) => ({
        id: r.id,
        marketplace: r.marketplace,
        supplierName: r.supplierName,
        oldPrice: r.oldPrice,
        newPrice: r.newPrice,
        status: r.status,
        error: r.error,
      })),
    };
    console.log(JSON.stringify(summary, null, 2));
  } catch {
    console.log(send.body.slice(0, 1000));
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
