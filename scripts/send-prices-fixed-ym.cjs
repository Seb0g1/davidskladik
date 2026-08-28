#!/usr/bin/env node
"use strict";
// Sends updated YM prices for all products fixed by fix-pinned-over-cheaper.
// Repeats the same diff >= $30 query to get the affected offer_ids,
// then resolves their yandex warehouse_products IDs and POSTs to /api/warehouse/prices/send.

require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");
const http = require("http");

const MIN_USD = 3;
const MAX_USD = 3000;
const MIN_DIFF_USD = 30;
const HOST = "localhost";
const PORT = 3000;
const USER = process.env.APP_USER;
const PASS = process.env.APP_PASSWORD;

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

  // 1. Find product_ids recently updated by fix-pinned-over-cheaper (last 30 min)
  //    These are product_links that were just promoted from article → selected_row.
  const recentlyFixed = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT product_id FROM product_links
    WHERE updated_at >= NOW() - INTERVAL '30 minutes'
      AND raw->>'matchType' = 'selected_row'
  `);
  const affectedProductIds = new Set(recentlyFixed.map((r) => String(r.product_id)));
  console.log(`Found ${affectedProductIds.size} recently fixed product_ids\n`);

  if (!affectedProductIds.size) { console.log("No recently fixed products found."); await prisma.$disconnect(); return; }

  // 2. Get yandex warehouse_products IDs for affected product_ids
  const placeholders = [...affectedProductIds].map((id) => `'${id.replace(/[^a-zA-Z0-9_-]/g, "")}'`).join(",");
  const ymProducts = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id FROM warehouse_products
    WHERE id::text IN (${placeholders}) AND marketplace = 'ozon'
  `);

  const ymIds = ymProducts.map((r) => String(r.id));
  console.log(`Found ${ymIds.length} Yandex Market products to reprice:`);
  for (const r of ymProducts) console.log(`  ${r.offer_id} → ${r.id}`);
  console.log();

  await prisma.$disconnect();

  if (!ymIds.length) { console.log("Nothing to send."); return; }

  // 3. Login
  console.log("Logging in...");
  const login = await post("/api/login", { username: USER, password: PASS });
  if (login.status !== 200) throw new Error(`Login failed: ${login.status} ${login.body.slice(0, 200)}`);
  const setCookie = login.headers["set-cookie"] || [];
  const sessionCookie = setCookie.map((c) => c.split(";")[0]).join("; ");
  console.log("OK\n");

  // 4. Send in batches of 50 to avoid timeouts
  const BATCH = 50;
  let totalSent = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  for (let i = 0; i < ymIds.length; i += BATCH) {
    const batch = ymIds.slice(i, i + BATCH);
    console.log(`Sending batch ${Math.floor(i / BATCH) + 1}: ${batch.length} products...`);
    const resp = await post(
      "/api/warehouse/prices/send",
      { confirmed: true, productIds: batch, marketplace: "ozon", force: true, livePriceMaster: false },
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
      totalSent += Number(result.sent ?? result.updatedCount ?? 0);
      totalSkipped += Number(result.skipped?.length ?? result.skippedCount ?? 0);
      totalErrors += Number(result.errors ?? result.errorCount ?? 0);
    } catch {
      console.log(`  response: ${resp.body.slice(0, 400)}`);
    }
  }

  console.log(`\nTotal: sent=${totalSent} skipped=${totalSkipped} errors=${totalErrors}`);
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
