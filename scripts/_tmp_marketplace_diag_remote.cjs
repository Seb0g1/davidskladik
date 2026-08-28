#!/usr/bin/env node
"use strict";
// Remote diagnostic: checks product counts, stock, prices across Ozon (both) and YM
const http = require("node:http");
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "david";
const appPassword = process.env.APP_PASSWORD || "";

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
  const ck = sessionCookie(res.headers);
  if (!ck || res.status !== 200) throw new Error(`Login failed: HTTP ${res.status}`);
  return ck;
}

async function dbDiag(prisma) {
  console.log("\n=== PostgreSQL: product counts by marketplace + target ===");

  // Count by marketplace and target
  const counts = await prisma.$queryRawUnsafe(`
    SELECT
      marketplace,
      raw->>'target' AS target,
      COUNT(*) AS total,
      SUM(CASE WHEN (raw->>'sellable')::boolean = true THEN 1 ELSE 0 END) AS sellable,
      SUM(CASE WHEN (raw->>'stock')::int > 0 THEN 1 ELSE 0 END) AS with_stock,
      SUM(CASE WHEN (raw->>'stock')::int > 0 AND (raw->>'sellable')::boolean = true THEN 1 ELSE 0 END) AS sellable_with_stock,
      SUM(CASE WHEN (raw->>'stock')::int > 0 AND (raw->>'sellable')::boolean != true THEN 1 ELSE 0 END) AS stocked_not_sellable,
      ROUND(AVG(NULLIF((raw->>'marketplacePrice')::numeric, 0)), 0) AS avg_marketplace_price,
      ROUND(AVG(NULLIF((raw->>'targetPrice')::numeric, 0)), 0) AS avg_target_price,
      SUM(CASE WHEN (raw->>'marketplacePrice')::numeric < 500 AND (raw->>'stock')::int > 0 AND (raw->>'sellable')::boolean = true THEN 1 ELSE 0 END) AS low_price_under_500
    FROM warehouse_products
    WHERE marketplace IN ('ozon','yandex') AND raw IS NOT NULL
    GROUP BY marketplace, raw->>'target'
    ORDER BY marketplace, target
  `);

  for (const r of counts) {
    console.log(`\n[${r.marketplace?.toUpperCase()} / ${r.target || 'default'}]`);
    console.log(`  total=${r.total}  sellable=${r.sellable}  with_stock=${r.with_stock}`);
    console.log(`  sellable+stock=${r.sellable_with_stock}  stocked_but_NOT_sellable=${r.stocked_not_sellable}`);
    console.log(`  avg_marketplace_price=${r.avg_marketplace_price}₽  avg_target_price=${r.avg_target_price}₽`);
    if (Number(r.low_price_under_500) > 0) {
      console.log(`  ⚠️  LOW PRICE <500₽ with stock: ${r.low_price_under_500} products`);
    }
  }

  // Products with stock but price too low (< 500 ₽) - likely still-bugged
  console.log("\n=== Products with stock + price < 500 ₽ (suspicious) ===");
  const lowPriced = await prisma.$queryRawUnsafe(`
    SELECT
      id, marketplace, raw->>'target' AS target,
      raw->>'offerId' AS offer_id,
      (raw->>'marketplacePrice')::numeric AS marketplace_price,
      (raw->>'targetPrice')::numeric AS target_price,
      (raw->>'stock')::int AS stock
    FROM warehouse_products
    WHERE
      marketplace IN ('ozon','yandex')
      AND (raw->>'stock')::int > 0
      AND (raw->>'sellable')::boolean = true
      AND (raw->>'marketplacePrice')::numeric > 0
      AND (raw->>'marketplacePrice')::numeric < 500
    ORDER BY (raw->>'marketplacePrice')::numeric ASC
    LIMIT 30
  `);

  if (lowPriced.length === 0) {
    console.log("  None found — all active products with stock have price >= 500 ₽ ✓");
  } else {
    console.log(`  Found ${lowPriced.length} suspicious products:`);
    for (const p of lowPriced) {
      console.log(`  [${p.marketplace}/${p.target}] ${p.offer_id} — marketplace=${p.marketplace_price}₽ target=${p.target_price}₽ stock=${p.stock}`);
    }
  }

  // Stocked but not sellable - products with stock that aren't active
  console.log("\n=== Products with stock but NOT sellable (archived/hidden) ===");
  const stockedNotSellable = await prisma.$queryRawUnsafe(`
    SELECT
      marketplace,
      raw->>'target' AS target,
      COUNT(*) AS count,
      SUM((raw->>'stock')::int) AS total_stock
    FROM warehouse_products
    WHERE
      marketplace IN ('ozon','yandex')
      AND (raw->>'stock')::int > 0
      AND (raw->>'sellable')::boolean != true
      AND raw IS NOT NULL
    GROUP BY marketplace, raw->>'target'
    ORDER BY count DESC
  `);

  if (stockedNotSellable.length === 0) {
    console.log("  None — all stocked products are sellable ✓");
  } else {
    for (const r of stockedNotSellable) {
      console.log(`  [${r.marketplace}/${r.target}] ${r.count} products with stock=${r.total_stock} BUT not sellable`);
    }
  }

  // Linked products with no price sent recently
  console.log("\n=== Linked products without any marketplace price (never priced) ===");
  const noPriced = await prisma.$queryRawUnsafe(`
    SELECT
      wp.marketplace,
      raw->>'target' AS target,
      COUNT(*) AS count
    FROM warehouse_products wp
    WHERE
      wp.marketplace IN ('ozon','yandex')
      AND (raw->>'sellable')::boolean = true
      AND (raw->>'marketplacePrice')::numeric = 0
      AND raw IS NOT NULL
      AND EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id = wp.id)
    GROUP BY wp.marketplace, raw->>'target'
  `);
  if (noPriced.length === 0) {
    console.log("  None — all sellable linked products have a price ✓");
  } else {
    for (const r of noPriced) {
      console.log(`  [${r.marketplace}/${r.target}] ${r.count} sellable linked products with price=0`);
    }
  }
}

async function main() {
  const prisma = new PrismaClient();
  try {
    await dbDiag(prisma);
  } finally {
    await prisma.$disconnect();
  }

  // Check live status
  const cookie = await login();
  const live = await request("GET", "/api/live-status", { cookie });
  const ls = live.body;
  console.log("\n=== Live status ===");
  console.log(`  warehouse: ${ls?.warehouse?.products} products, updated ${ls?.warehouse?.updatedAt}`);
  console.log(`  priceMaster: ${ls?.priceMaster?.items} items, updated ${ls?.priceMaster?.updatedAt}`);
  console.log(`  queue: waiting=${ls?.queue?.counts?.waiting} active=${ls?.queue?.counts?.active} failed=${ls?.queue?.counts?.failed}`);

  // Check automation summary
  const summary = await request("GET", "/api/sales-automation/summary", { cookie });
  if (summary.status === 200) {
    const s = summary.body;
    console.log("\n=== Sales automation summary ===");
    console.log(JSON.stringify(s, null, 2));
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
