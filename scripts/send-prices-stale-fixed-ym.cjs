#!/usr/bin/env node
"use strict";
// Sends YM prices for products whose stale selected_row links were just fixed to article type.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");
const http = require("http");

const HOST = "localhost";
const PORT = 3000;
const USER = process.env.APP_USER;
const PASS = process.env.APP_PASSWORD;
const WINDOW_MIN = parseInt(process.argv[2] || "30", 10);
const MARKETPLACE = process.argv[3] || "yandex";

function post(path2, body, cookieHeader) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const headers = { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) };
    if (cookieHeader) headers["Cookie"] = cookieHeader;
    const req = http.request({ host: HOST, port: PORT, path: path2, method: "POST", headers }, (res) => {
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

  // Find product_ids from recently fixed links (any matchType, updated recently)
  const recentlyFixed = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT product_id FROM product_links
    WHERE updated_at >= NOW() - INTERVAL '${WINDOW_MIN} minutes'
  `);
  const affectedProductIds = new Set(recentlyFixed.map((r) => String(r.product_id)));
  console.log(`Found ${affectedProductIds.size} recently fixed product_ids (window: ${WINDOW_MIN} min)\n`);

  if (!affectedProductIds.size) { console.log("No recently fixed products found."); await prisma.$disconnect(); return; }

  const placeholders = [...affectedProductIds].map((id) => `'${id.replace(/[^a-zA-Z0-9_-]/g, "")}'`).join(",");
  const products = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id FROM warehouse_products
    WHERE id::text IN (${placeholders}) AND marketplace = '${MARKETPLACE}'
  `);

  const ids = products.map((r) => String(r.id));
  console.log(`Found ${ids.length} ${MARKETPLACE} products to reprice`);
  await prisma.$disconnect();

  if (!ids.length) { console.log("Nothing to send."); return; }

  console.log("Logging in...");
  const login = await post("/api/login", { username: USER, password: PASS });
  if (login.status !== 200) throw new Error(`Login failed: ${login.status} ${login.body.slice(0, 200)}`);
  const setCookie = login.headers["set-cookie"] || [];
  const sessionCookie = setCookie.map((c) => c.split(";")[0]).join("; ");
  console.log("OK\n");

  const BATCH = 50;
  let totalSent = 0, totalErrors = 0;

  for (let i = 0; i < ids.length; i += BATCH) {
    const batch = ids.slice(i, i + BATCH);
    console.log(`Sending batch ${Math.floor(i / BATCH) + 1}: ${batch.length} products...`);
    const resp = await post(
      "/api/warehouse/prices/send",
      { confirmed: true, productIds: batch, marketplace: MARKETPLACE, force: true, livePriceMaster: false },
      sessionCookie,
    );
    if (resp.status !== 200) {
      console.error(`  Batch failed: ${resp.status} ${resp.body.slice(0, 300)}`);
      totalErrors += batch.length;
      continue;
    }
    try {
      const result = JSON.parse(resp.body);
      const errSample = (result.failedItems || []).slice(0, 3).map((f) => `${f.offerId}: ${f.error}`).join(" | ");
      console.log(`  sent=${result.sent} failed=${result.failed} stockSent=${result.stockSent}`);
      if (errSample) console.log(`  sample errors: ${errSample.slice(0, 300)}`);
      totalSent += Number(result.sent ?? 0);
      totalErrors += Number(result.failed ?? result.errors ?? result.errorCount ?? 0);
    } catch {
      console.log(`  response: ${resp.body.slice(0, 400)}`);
    }
  }

  console.log(`\nTotal: sent=${totalSent} errors=${totalErrors}`);
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
