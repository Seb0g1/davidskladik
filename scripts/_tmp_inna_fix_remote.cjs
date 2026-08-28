#!/usr/bin/env node
"use strict";
// Runs on the REMOTE server (localhost:3000 is the app)
// 1. Finds all warehouse products whose PM snapshot items have native_name containing "inna"
//    but whose managed supplier is NOT marked as RUB (i.e. the bug affected them)
// 2. Triggers targeted reprice for those products in small batches

const http = require("node:http");
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD || "";

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1", port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch {}
        resolve({ status: res.statusCode, body: parsed, headers: res.headers });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = [].concat(headers["set-cookie"] || []);
  const s = list.find((x) => String(x).startsWith("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

async function login() {
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(res.headers);
  if (!cookie || res.status !== 200) throw new Error(`Login failed: HTTP ${res.status}`);
  console.log(`Logged in as ${username}`);
  return cookie;
}

async function findAffectedProductIds(prisma) {
  // PriceMasterSnapshotItem has no @@map so table is "PriceMasterSnapshotItem" (quoted)
  // product_links has @@map("product_links"), field supplier_article joins to pmsi.article
  const rows = await prisma.$queryRawUnsafe(`
    SELECT DISTINCT pl.product_id AS id
    FROM product_links pl
    JOIN pm_snapshot_items pmsi
      ON pmsi.article = pl.supplier_article
      AND pl.supplier_article IS NOT NULL
      AND pl.supplier_article <> ''
    WHERE
      LOWER(pmsi.native_name) LIKE '%inna%'
      AND pmsi.currency = 'USD'
    LIMIT 2000
  `);
  return rows.map((r) => String(r.id));
}

async function repriceInBatches(cookie, productIds, batchSize = 50) {
  let total = 0;
  for (let i = 0; i < productIds.length; i += batchSize) {
    const batch = productIds.slice(i, i + batchSize);
    console.log(`Batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(productIds.length / batchSize)}: ${batch.length} products`);
    const res = await request("POST", "/api/sales-automation/run", {
      cookie,
      body: {
        marketplace: "all",
        productIds: batch,
        force: true,
        reason: "inna_bug_fix_targeted",
      },
    });
    if (res.status >= 400) {
      console.error(`  Batch failed: HTTP ${res.status}`, JSON.stringify(res.body).slice(0, 200));
    } else {
      const q = res.body?.queued || 0;
      total += q;
      console.log(`  queued=${q} total=${total}`);
    }
    if (i + batchSize < productIds.length) await sleep(2000);
  }
  return total;
}

async function main() {
  const prisma = new PrismaClient();
  let productIds;
  try {
    console.log("Querying postgres for affected product IDs...");
    productIds = await findAffectedProductIds(prisma);
    console.log(`Found ${productIds.length} potentially affected products`);
    if (!productIds.length) {
      console.log("No products found — nothing to do.");
      return;
    }
    console.log("Sample IDs:", productIds.slice(0, 5).join(", "));
  } finally {
    await prisma.$disconnect();
  }

  const cookie = await login();
  console.log(`\nRepricing ${productIds.length} products in batches of 50...`);
  const totalQueued = await repriceInBatches(cookie, productIds, 50);
  console.log(`\nDone! Total queued: ${totalQueued}`);
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
