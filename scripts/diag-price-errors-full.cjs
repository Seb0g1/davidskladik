#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // App settings 'app' key
  const appSetting = await prisma.$queryRawUnsafe(`
    SELECT key, value FROM app_settings WHERE key = 'app'
  `).catch(() => []);

  if (appSetting.length) {
    const v = appSetting[0].value;
    const parsed = typeof v === "string" ? JSON.parse(v) : v;
    console.log("\n=== App settings (key=app) relevant fields ===\n");
    console.log("defaultMarkups:", JSON.stringify(parsed.defaultMarkups, null, 2));
    console.log("markupRules:", JSON.stringify(parsed.markupRules, null, 2));
    console.log("fixedUsdRate:", parsed.fixedUsdRate);
    console.log("availabilityRules:", JSON.stringify(parsed.availabilityRules, null, 2));
  } else {
    console.log("\n(no app setting found)\n");
  }

  // Full price_history response for K18001 and ЮК345754
  const history = await prisma.$queryRawUnsafe(`
    SELECT ph.id, ph.offer_id, ph.marketplace, ph.created_at,
           ph.response, ph.status
    FROM price_history ph
    WHERE ph.offer_id IN ('K18001','ЮК345754')
      AND ph.status = 'failed'
    ORDER BY ph.created_at DESC
    LIMIT 5
  `).catch(() => []);

  console.log(`\n=== Failed price_history (${history.length}) ===\n`);
  for (const h of history) {
    const resp = h.response ? (typeof h.response === "string" ? JSON.parse(h.response) : h.response) : {};
    console.log(`[${h.offer_id}][${h.marketplace}] ${h.created_at}`);
    console.log(`  price=${resp.price} rate=${resp.usdRate} markup=${resp.markupCoefficient}`);
    console.log(`  error=${JSON.stringify(resp.error || resp.errors || resp.failReason || resp.message || resp)}`);
    console.log();
  }

  // Check the actual Ozon error from lastOzonPriceSend for ЮК345754
  const detail = await prisma.$queryRawUnsafe(`
    SELECT offer_id, marketplace,
           raw->'lastOzonPriceSend' AS last_oz,
           raw->'lastYandexPriceSend' AS last_ym
    FROM warehouse_products
    WHERE offer_id IN ('K18001','ЮК345754')
    ORDER BY offer_id, marketplace
  `);

  console.log("\n=== Full last send JSON ===\n");
  for (const d of detail) {
    console.log(`[${d.offer_id}][${d.marketplace}]`);
    if (d.last_oz) {
      const oz = typeof d.last_oz === "string" ? JSON.parse(d.last_oz) : d.last_oz;
      console.log("  Ozon:", JSON.stringify(oz));
    }
    if (d.last_ym) {
      const ym = typeof d.last_ym === "string" ? JSON.parse(d.last_ym) : d.last_ym;
      console.log("  YM:", JSON.stringify(ym));
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
