#!/usr/bin/env node
"use strict";
// Sends the corrected price for art 10825 on Yandex Market.
// The DB target_price was already updated to 6188₽ by fix-10825-ym-price.cjs.
// This script calls /api/warehouse/prices/send to actually push it to YM.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const http = require("http");
const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";
let sessionCookie = "";

async function rawRequest(method, path, body, timeoutMs = 120000) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : undefined;
    const req = http.request({
      hostname: "127.0.0.1", port: 3000, path, method,
      headers: {
        Cookie: sessionCookie, "Content-Type": "application/json",
        ...(payload ? { "Content-Length": Buffer.byteLength(payload) } : {}),
      },
      timeout: timeoutMs,
    }, (res) => {
      const sc = res.headers["set-cookie"];
      if (sc) { const p = sc.find((c) => c.startsWith("pm_session=")); if (p) sessionCookie = p.split(";")[0]; }
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString()) }); }
        catch { resolve({ status: res.statusCode, body: {} }); }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    if (payload) req.write(payload); req.end();
  });
}

const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // Get 10825 YM product
  const yp = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, current_price, target_price
    FROM warehouse_products WHERE offer_id ILIKE '10825' AND marketplace = 'yandex'
  `);
  if (!yp.length) { console.log("10825 YM not found"); return; }
  const ymProduct = yp[0];
  console.log(`10825 YM: id=${ymProduct.id} currentPrice=${ymProduct.current_price} targetPrice=${ymProduct.target_price}`);

  if (ymProduct.target_price < 1000) {
    console.error(`target_price=${ymProduct.target_price} looks wrong — aborting`);
    process.exit(1);
  }

  // Log in
  const loginRes = await rawRequest("POST", "/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (!loginRes.body?.ok) throw new Error(`Login failed: ${JSON.stringify(loginRes.body)}`);
  console.log(`Logged in as ${loginRes.body.username}`);

  // Get warehouse view of 10825 to get the product IDs and confirm state
  const whRes = await rawRequest("GET", `/api/warehouse?offerId=10825&limit=10`, undefined, 30000);
  const allProducts = whRes.body?.products || [];
  const ymProd = allProducts.find((p) => p.marketplace === "yandex" || p.marketplace === "YANDEX");
  if (!ymProd) { console.log("10825 YM not in warehouse response"); return; }
  console.log(`Warehouse: offerId=${ymProd.offerId} marketplace=${ymProd.marketplace} currentPrice=${ymProd.currentPrice} nextPrice=${ymProd.nextPrice}`);

  // Send price for just this YM product
  const productIds = [ymProd.id];
  console.log(`\nSending price for product id=${ymProd.id} (nextPrice=${ymProd.nextPrice})...`);
  const sendRes = await rawRequest("POST", "/api/warehouse/prices/send", {
    confirmed: true,
    productIds,
    skipStockUpdate: true,
  }, 120000);
  console.log(`Send status: ${sendRes.status}`);
  console.log(`Result: ${JSON.stringify(sendRes.body, null, 2)}`);

  // Re-check the price after send
  await new Promise((r) => setTimeout(r, 2000));
  const whRes2 = await rawRequest("GET", `/api/warehouse?offerId=10825&limit=10`, undefined, 30000);
  const ymProd2 = (whRes2.body?.products || []).find((p) => p.marketplace === "yandex" || p.marketplace === "YANDEX");
  if (ymProd2) {
    console.log(`\nAfter send: currentPrice=${ymProd2.currentPrice} nextPrice=${ymProd2.nextPrice} targetPrice=${ymProd2.targetPrice}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
