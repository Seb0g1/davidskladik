#!/usr/bin/env node
"use strict";
const path = require("node:path");
const https = require("node:https");
const http = require("node:http");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const { PrismaClient } = require("@prisma/client");

const TARGET_OFFER_IDS = ["HF-THOOS37010", "CO-MO", "UG000015104", "16608", "AR0002"];

async function main() {
  const prisma = new PrismaClient();
  try {
    // Step 1: login
    const loginRes = await fetch("http://localhost:3000/api/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: process.env.APP_USER, password: process.env.APP_PASSWORD }),
    });
    const setCookie = loginRes.headers.get("set-cookie") || "";
    const sessionCookie = setCookie.split(";")[0];
    const loginBody = await loginRes.json();
    console.log("Login:", loginBody.ok ? "OK" : JSON.stringify(loginBody));

    if (!loginBody.ok) { console.error("Auth failed"); process.exit(1); }

    // Step 2: trigger warehouse sync (with PM snapshot refresh, no marketplace import — just PM prices)
    console.log("Triggering warehouse refresh (refreshPrices=true, no PM import)...");
    const syncRes = await fetch("http://localhost:3000/api/warehouse?refreshPrices=true", {
      headers: { Cookie: sessionCookie },
      signal: AbortSignal.timeout(290000),
    });
    const syncData = await syncRes.json();
    console.log(`Sync done: ${syncData.products?.length ?? "?"} products`);

    // Step 3: check the specific products
    console.log("\n===== Проверка остатков =====");
    const placeholders = TARGET_OFFER_IDS.map((_, i) => `$${i + 1}`).join(", ");
    const rows = await prisma.$queryRawUnsafe(`
      SELECT
        wp.id,
        wp.marketplace,
        wp.raw->>'offerId'              AS offer_id,
        wp.raw->>'target'               AS target,
        wp.raw->>'targetStock'          AS target_stock,
        wp.raw->'marketplaceState'->>'code'  AS mp_code,
        (SELECT COUNT(*) FROM product_links pl WHERE pl.product_id = wp.id) AS link_count,
        s.stock_status,
        s.target_stock                  AS sku_target_stock,
        s.last_stock_sent_at,
        s.last_error
      FROM warehouse_products wp
      LEFT JOIN sales_automation_sku_states s
        ON s.marketplace = wp.marketplace
        AND s.offer_id   = (wp.raw->>'offerId')
        AND s.target     = (wp.raw->>'target')
      WHERE wp.raw->>'offerId' IN (${placeholders})
      ORDER BY wp.marketplace, wp.raw->>'offerId'
    `, ...TARGET_OFFER_IDS);

    for (const r of rows) {
      console.log(`\n[${r.marketplace}/${r.target}] ${r.offer_id}`);
      console.log(`  links:        ${r.link_count}`);
      console.log(`  targetStock:  ${r.target_stock ?? "(null)"}`);
      console.log(`  sku_stock:    ${r.sku_target_stock ?? "(null)"}`);
      console.log(`  stock_status: ${r.stock_status ?? "(none)"}`);
      console.log(`  last_sent_at: ${r.last_stock_sent_at ?? "(never)"}`);
      console.log(`  last_error:   ${r.last_error ?? "(none)"}`);
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message, e.stack); process.exit(1); });
