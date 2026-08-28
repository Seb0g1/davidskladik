#!/usr/bin/env node
"use strict";
// Zero out Ozon stock for K18001 (Collistar Eye Shadow).
// Ozon blocks price above 400 RUB — cannot sell at a profit. Must stop sales immediately.

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

function httpReq(method, p, body, cookieHeader) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const headers = {};
    if (payload) { headers["Content-Type"] = "application/json"; headers["Content-Length"] = Buffer.byteLength(payload); }
    if (cookieHeader) headers["Cookie"] = cookieHeader;
    const req = http.request({ host: HOST, port: PORT, path: p, method, headers }, (res) => {
      let raw = "";
      res.on("data", (d) => { raw += d; });
      res.on("end", () => resolve({ status: res.statusCode, body: raw, headers: res.headers }));
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function main() {
  const prisma = getPrisma();

  const products = await prisma.$queryRawUnsafe(`
    SELECT id::text AS id, offer_id, marketplace, current_price, target_stock,
           raw->'marketplaceState'->>'stock' AS mp_stock
    FROM warehouse_products
    WHERE offer_id = 'K18001' AND marketplace = 'ozon'
  `);

  console.log(`K18001 Ozon entries (${products.length}):`);
  for (const p of products) {
    console.log(`  id=${p.id} price=${p.current_price}₽ target_stock=${p.target_stock} mp_stock=${p.mp_stock}`);
  }

  if (!products.length) { await prisma.$disconnect(); return; }

  // Set target_stock=0 in DB (prevents stock sweep from restoring)
  const ids = products.map((p) => p.id);
  await prisma.$executeRawUnsafe(
    `UPDATE warehouse_products SET target_stock = 0, updated_at = now() WHERE id::text = ANY(ARRAY[${ids.map((id) => `'${id}'`).join(",")}])`
  );
  console.log(`\nSet target_stock=0 in DB for ${ids.length} entries`);
  await prisma.$disconnect();

  // Login and trigger stock send via price send endpoint (which also sends stock=0)
  const login = await httpReq("POST", "/api/login", { username: USER, password: PASS });
  if (login.status !== 200) throw new Error(`Login failed: ${login.status}`);
  const cookie = (login.headers["set-cookie"] || []).map((c) => c.split(";")[0]).join("; ");
  console.log("Logged in");

  // Use the price send with force=true — it will also push stock (which is now 0)
  const resp = await httpReq("POST", "/api/warehouse/prices/send", {
    confirmed: true,
    productIds: ids,
    marketplace: "ozon",
    force: true,
    livePriceMaster: true,
  }, cookie);

  console.log(`\nPrice/stock send response: ${resp.status}`);
  try {
    const r = JSON.parse(resp.body);
    console.log(`  stockSent=${r.stockSent} sent=${r.sent} failed=${r.failed}`);
    const errs = (r.failedItems || []).slice(0, 5);
    for (const e of errs) console.log(`  err: ${e.offerId}: ${e.error}`);
  } catch {
    console.log(resp.body.slice(0, 400));
  }

  // Verify
  const prisma2 = getPrisma();
  const after = await prisma2.$queryRawUnsafe(`
    SELECT id::text AS id, current_price, target_stock FROM warehouse_products
    WHERE offer_id = 'K18001' AND marketplace = 'ozon'
  `);
  console.log(`\nAfter fix:`);
  for (const p of after) console.log(`  id=${p.id} price=${p.current_price}₽ target_stock=${p.target_stock}`);
  await prisma2.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
