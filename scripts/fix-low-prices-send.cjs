#!/usr/bin/env node
"use strict";
// Sends correct prices for products with suspiciously low current_price.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");
const http = require("http");

const HOST = "localhost";
const PORT = 3000;
const USER = process.env.APP_USER;
const PASS = process.env.APP_PASSWORD;

function post(p, body, cookieHeader) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const headers = { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) };
    if (cookieHeader) headers["Cookie"] = cookieHeader;
    const req = http.request({ host: HOST, port: PORT, path: p, method: "POST", headers }, (res) => {
      let raw = "";
      res.on("data", (d) => { raw += d; });
      res.on("end", () => resolve({ status: res.statusCode, body: raw, headers: res.headers }));
    });
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function main() {
  const prisma = getPrisma();

  // Find all products with current_price < 500 and stock > 0
  const lowPriceProducts = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id, marketplace, current_price
    FROM warehouse_products
    WHERE target_stock > 0 AND current_price > 0 AND current_price < 500
    ORDER BY marketplace, current_price
  `);

  console.log(`Found ${lowPriceProducts.length} low-price products:`);
  for (const r of lowPriceProducts) {
    console.log(`  art=${r.offer_id} [${r.marketplace}] price=${r.current_price}₽`);
  }

  const ymIds = lowPriceProducts.filter((r) => r.marketplace === "yandex").map((r) => r.id);
  const ozonIds = lowPriceProducts.filter((r) => r.marketplace === "ozon").map((r) => r.id);

  await prisma.$disconnect();

  console.log("\nLogging in...");
  const login = await post("/api/login", { username: USER, password: PASS });
  if (login.status !== 200) throw new Error(`Login failed: ${login.status} ${login.body.slice(0, 200)}`);
  const sessionCookie = (login.headers["set-cookie"] || []).map((c) => c.split(";")[0]).join("; ");
  console.log("OK\n");

  for (const [marketplace, ids] of [["yandex", ymIds], ["ozon", ozonIds]]) {
    if (!ids.length) continue;
    console.log(`Sending ${marketplace} prices for ${ids.length} products...`);
    const resp = await post(
      "/api/warehouse/prices/send",
      { confirmed: true, productIds: ids, marketplace, force: true, livePriceMaster: true },
      sessionCookie,
    );
    if (resp.status !== 200) {
      console.error(`  Failed: ${resp.status} ${resp.body.slice(0, 300)}`);
      continue;
    }
    try {
      const result = JSON.parse(resp.body);
      console.log(`  sent=${result.sent} failed=${result.failed} stockSent=${result.stockSent}`);
      const errSample = (result.failedItems || []).slice(0, 3).map((f) => `${f.offerId}: ${f.error}`).join(" | ");
      if (errSample) console.log(`  errors: ${errSample}`);
    } catch {
      console.log(`  response: ${resp.body.slice(0, 400)}`);
    }
  }

  // After send — show new prices
  console.log("\nVerifying new prices...");
  const prisma2 = getPrisma();
  const updated = await prisma2.$queryRawUnsafe(`
    SELECT offer_id, marketplace, current_price, target_stock
    FROM warehouse_products
    WHERE target_stock > 0 AND current_price > 0
      AND offer_id = ANY(ARRAY['K18001','10825','13214','8574345','2362','НФ-00005048'])
    ORDER BY offer_id, marketplace
  `);
  for (const r of updated) {
    console.log(`  art=${r.offer_id} [${r.marketplace}] price=${r.current_price}₽ stock=${r.target_stock}`);
  }
  await prisma2.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
