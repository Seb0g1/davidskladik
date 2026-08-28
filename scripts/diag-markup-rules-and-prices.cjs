#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // appSettings with markupRules
  const settings = await prisma.$queryRawUnsafe(`
    SELECT key, value FROM app_settings WHERE key IN ('markupRules','availabilityRules','defaultMarkups','fixedUsdRate')
    ORDER BY key
  `).catch(async () => {
    // Try app_setting (singular)
    return prisma.$queryRawUnsafe(`
      SELECT key, value FROM app_setting WHERE key IN ('markupRules','availabilityRules','defaultMarkups','fixedUsdRate')
      ORDER BY key
    `).catch(() => []);
  });

  console.log("\n=== App Settings ===\n");
  for (const s of settings) {
    console.log(`${s.key}:`, JSON.stringify(s.value, null, 2));
  }

  // Also check via raw SQL for all settings
  const allSettings = await prisma.$queryRawUnsafe(`
    SELECT * FROM app_settings LIMIT 50
  `).catch(async () => {
    return prisma.$queryRawUnsafe(`SELECT * FROM app_setting LIMIT 50`).catch(() => []);
  });

  if (settings.length === 0 && allSettings.length > 0) {
    console.log("\nAll settings keys:", allSettings.map((s) => s.key).join(", "));
  }

  // Check ozon_min_price for K18001 specifically
  const ozonMinPrice = await prisma.$queryRawUnsafe(`
    SELECT offer_id, marketplace, raw->>'ozonMinPrice' AS ozon_min_price,
           raw->>'autoPriceMin' AS auto_price_min,
           raw->>'autoPriceMax' AS auto_price_max,
           raw->'links' AS ym_links_json
    FROM warehouse_products
    WHERE offer_id IN ('K18001','ЮК345754')
    ORDER BY offer_id, marketplace
  `);
  console.log("\n=== Price limits for K18001 / ЮК345754 ===\n");
  for (const r of ozonMinPrice) {
    console.log(`[${r.offer_id}][${r.marketplace}] ozonMinPrice=${r.ozon_min_price} autoPriceMin=${r.auto_price_min} autoPriceMax=${r.auto_price_max}`);
    if (r.ym_links_json) {
      const links = typeof r.ym_links_json === "string" ? JSON.parse(r.ym_links_json) : r.ym_links_json;
      if (Array.isArray(links) && links.length) {
        console.log(`  YM links (${links.length}):`);
        for (const l of links.slice(0, 3)) console.log(`    supplier=${l.supplierName} markup=${l.markup} matchType=${l.matchType}`);
      }
    }
  }

  // Check environment markups from .env
  console.log("\n=== Environment markups ===");
  console.log(`DEFAULT_OZON_MARKUP = ${process.env.DEFAULT_OZON_MARKUP}`);
  console.log(`DEFAULT_YANDEX_MARKUP = ${process.env.DEFAULT_YANDEX_MARKUP}`);
  console.log(`DEFAULT_USD_RATE = ${process.env.DEFAULT_USD_RATE}`);

  // Check price_history for K18001 and ЮК345754
  const history = await prisma.$queryRawUnsafe(`
    SELECT ph.offer_id, ph.marketplace, ph.created_at,
           ph.response->>'price' AS price,
           ph.response->>'usdRate' AS rate,
           ph.response->>'markupCoefficient' AS markup_coeff,
           ph.status
    FROM price_history ph
    WHERE ph.offer_id IN ('K18001','ЮК345754')
    ORDER BY ph.created_at DESC
    LIMIT 10
  `).catch(() => []);

  console.log(`\n=== Price history (${history.length} records) ===\n`);
  for (const h of history) {
    console.log(`[${h.offer_id}][${h.marketplace}] ${h.created_at} status=${h.status} price=${h.price}₽ rate=${h.rate} markup=${h.markup_coeff}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
