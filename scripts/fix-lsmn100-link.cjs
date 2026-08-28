#!/usr/bin/env node
"use strict";
// Fixes LSMN100 (Далика) link: sourceRowId=2331708 points to art=178 (wrong product).
// Correct PM row for art=163 (Далик partnerId=123) is now RowID=2331694, price=10 USD.
// Also fixes 10825 Yandex price (67₽ → correct ~6400₽).

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const http = require("http");
const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";
let sessionCookie = "";

async function rawRequest(method, path, body, timeoutMs = 60000) {
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
    req.on("error", reject); req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    if (payload) req.write(payload); req.end();
  });
}

const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // ── 1. Fix LSMN100 link: update resolvedPriceMasterRow to correct row ─────
  console.log("=== Fix LSMN100 (Далика) link ===");

  // Find LSMN100 warehouse products
  const lsmn100Products = await prisma.$queryRawUnsafe(`
    SELECT wp.id, wp.marketplace, wp.current_price, wp.target_price
    FROM warehouse_products wp
    WHERE wp.offer_id ILIKE 'LSMN100'
  `);
  console.log(`Found ${lsmn100Products.length} LSMN100 products`);

  for (const wp of lsmn100Products) {
    // Find the Далик link (wp.id is a CUID from our own DB — safe to interpolate)
    const wpId = String(wp.id).replace(/[^a-zA-Z0-9_-]/g, "");
    const links = await prisma.$queryRawUnsafe(`
      SELECT id, supplier_article, partner_id, raw
      FROM product_links
      WHERE product_id = '${wpId}' AND partner_id = '123'
    `);
    console.log(`  ${wp.marketplace} links from Далик: ${links.length}`);

    for (const link of links) {
      const currentRaw = link.raw && typeof link.raw === "object" ? link.raw : {};
      const currentSourceRowId = currentRaw.sourceRowId;
      const currentRpm = currentRaw.resolvedPriceMasterRow;
      console.log(`    link id=${link.id} art=${link.supplier_article} sourceRowId=${currentSourceRowId} rpmRowId=${currentRpm?.rowId} rpmPrice=${currentRpm?.price}`);

      // The correct row for art=163 from Далик is now RowID=2331694, price=10 USD
      // Update the raw field to fix the sourceRowId and resolvedPriceMasterRow
      const newRaw = {
        ...currentRaw,
        sourceRowId: "2331694",
        resolvedPriceMasterRow: {
          rowId: "2331694",
          article: "163",
          name: "LATTAFA QAED AL FURSAN UNTAMED (M) EDP 9",
          partnerId: "123",
          partnerName: "Далик",
          price: 10,
          currency: "USD",
          priceCurrency: "USD",
          active: true,
        },
      };

      const linkId = String(link.id).replace(/[^a-zA-Z0-9_-]/g, "");
      const rawJson = JSON.stringify(newRaw).replace(/'/g, "''");
      await prisma.$queryRawUnsafe(`
        UPDATE product_links SET raw = '${rawJson}'::jsonb WHERE id = '${linkId}'
      `);
      console.log(`    ✓ Updated link ${link.id}: sourceRowId 2331708 → 2331694 (price 26 USD → 10 USD)`);
    }
  }

  // ── 2. Fix 10825 YM price (67₽ → correct price) ──────────────────────────
  console.log("\n=== Fix 10825 Yandex price (67₽ → correct ~6400₽) ===");

  // Get the current YM product for 10825
  const p10825 = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, current_price, target_price
    FROM warehouse_products WHERE offer_id ILIKE '10825'
  `);
  for (const wp of p10825) {
    console.log(`  ${wp.marketplace}: currentPrice=${wp.current_price} targetPrice=${wp.target_price}`);
  }

  // Log in and trigger a price re-computation for the YM 10825 product
  // The proper fix is to trigger a sync which will recompute nextPrice using the correct formula.
  const loginRes = await rawRequest("POST", "/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (!loginRes.body?.ok) throw new Error("Login failed");
  console.log(`  Logged in as ${loginRes.body.username}`);

  // Trigger a manual sync to recompute prices
  console.log("  Triggering warehouse sync to recompute prices...");
  const syncRes = await rawRequest("POST", "/api/sync", {}, 120000);
  console.log(`  Sync result: ${syncRes.status} ok=${syncRes.body?.ok} items=${syncRes.body?.items}`);

  // After sync, check the new computed price for 10825 YM
  const warehouseRes = await rawRequest("GET", "/api/warehouse?limit=5&offerId=10825", undefined, 30000);
  if (warehouseRes.body?.products?.length) {
    for (const p of warehouseRes.body.products) {
      console.log(`  10825 ${p.marketplace}: currentPrice=${p.currentPrice} nextPrice=${p.nextPrice} selectedSupplier=${p.selectedSupplier?.supplierName || "none"}`);
    }
  }

  console.log("\nDone. Check the warehouse page for LSMN100 and 10825 to verify.");
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
