#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  const appRow = await prisma.appSetting.findUnique({ where: { key: "app" } });
  const app = appRow?.value || {};
  const rate = Number(app.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);
  console.log("rate=" + rate);

  const prods = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id, marketplace, current_price, target_price, raw->>'markup' AS markup
    FROM warehouse_products WHERE offer_id ILIKE '20185'
  `);
  console.log("\n=== 20185 products ===");
  for (const p of prods) console.log("  id=" + p.id + " mp=" + p.marketplace + " cur=" + p.current_price + " target=" + p.target_price + " markup=" + p.markup);

  const ids = prods.map((p) => "'" + String(p.id).replace(/[^a-zA-Z0-9_-]/g, "") + "'").join(",");
  const pgLinks = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.product_id, pl.supplier_name, pl.supplier_article, pl.partner_id, pl.price_currency,
           pl.raw->>'matchType' AS match_type, pl.raw->>'sourceRowId' AS source_row_id, pl.raw->>'article' AS raw_article,
           (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id,
           (pl.raw->'resolvedPriceMasterRow'->>'price') AS rpm_price,
           (pl.raw->'resolvedPriceMasterRow'->>'active') AS rpm_active,
           wp.marketplace
    FROM product_links pl JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE pl.product_id IN (${ids}) ORDER BY wp.marketplace, pl.supplier_name
  `);
  console.log("\n=== product_links for 20185 ===");
  for (const l of pgLinks) {
    console.log("  [" + l.marketplace + "] " + l.supplier_name + ": art=" + l.supplier_article + " partnerId=" + l.partner_id + " priceCurrency=" + l.price_currency);
    console.log("    matchType=" + l.match_type + " sourceRowId=" + l.source_row_id + " rawArticle=" + (l.raw_article || "(none)"));
    console.log("    rpm.rowId=" + l.rpm_row_id + " rpm.price=" + l.rpm_price + " rpm.active=" + l.rpm_active);
  }

  const rowIds = [...new Set(pgLinks.map((l) => l.rpm_row_id || l.source_row_id).filter(Boolean))];
  if (!rowIds.length) { console.log("No rowIds"); await prisma.$disconnect(); return; }
  const inList = rowIds.map((r) => "'" + String(r).replace(/[^0-9]/g, "") + "'").join(",");
  const pmRows = await prisma.$queryRawUnsafe(`
    SELECT row_id, article, partner_id, partner_name, price, currency, active, native_name
    FROM pm_snapshot_items WHERE row_id IN (${inList}) ORDER BY price::float ASC
  `);
  console.log("\n=== pm_snapshot_items for linked rowIds ===");
  for (const r of pmRows) {
    const rub = Math.round(Number(r.price) * rate);
    console.log("  rowId=" + r.row_id + " " + r.partner_name + "(" + r.partner_id + ") price=" + r.price + " " + r.currency + " active=" + r.active + " base=" + rub + "rub");
  }

  const partnerIds = [...new Set(pmRows.map((r) => r.partner_id).filter(Boolean))];
  console.log("\n=== managed_suppliers for these partners ===");
  for (const pid of partnerIds) {
    const ms = await prisma.$queryRawUnsafe(`
      SELECT id, name, partner_id, default_currency, active, stop_reason,
             raw->>'pricingMode' AS pricing_mode, raw->>'stopped' AS raw_stopped
      FROM managed_suppliers WHERE partner_id = '${pid}'
    `);
    for (const s of ms) {
      console.log("  pid=" + s.partner_id + " name=" + s.name + " default_currency=" + s.default_currency + " active=" + s.active + " stop_reason=" + (s.stop_reason || "(none)") + " pricingMode=" + (s.pricing_mode || "normal") + " rawStopped=" + (s.raw_stopped || "false"));
    }
    if (!ms.length) console.log("  pid=" + pid + " NOT IN managed_suppliers");
  }

  // Markup simulation for YM
  const ymProd = prods.find((p) => p.marketplace === "yandex");
  if (ymProd) {
    console.log("\n=== YM 20185 markup simulation ===");
    const productMarkup = Number(ymProd.markup) || 0;
    const yandexFallback = Number(app.defaultMarkups?.yandex || 1.6);
    const ymRules = (app.markupRules || []).filter((r) => !r.marketplace || r.marketplace === "all" || r.marketplace === "yandex");
    const ymLinkRowIds = [...new Set(pgLinks.filter((l) => l.product_id === String(ymProd.id)).map((l) => l.rpm_row_id || l.source_row_id).filter(Boolean))];
    const ymPm = pmRows.filter((r) => ymLinkRowIds.includes(r.row_id)).sort((a, b) => Number(a.price) - Number(b.price));
    for (const r of ymPm) {
      const pmPrice = Number(r.price);
      const currency = String(r.currency || "USD").toUpperCase();
      const usd = currency === "USD" ? pmPrice : pmPrice / rate;
      let markup = productMarkup;
      if (!(markup > 0)) {
        const sorted = [...ymRules].sort((a, b) => b.minUsd - a.minUsd);
        const matched = sorted.find((rule) => usd >= Number(rule.minUsd || 0));
        markup = Number(matched?.coefficient || yandexFallback);
      }
      const rubNative = currency === "RUB";
      const calcPrice = rubNative ? Math.round(pmPrice * markup) : Math.round(pmPrice * rate * markup);
      const isActive = r.active === true || r.active === "true";
      console.log("  rowId=" + r.row_id + " " + r.partner_name + ": " + pmPrice + " " + r.currency + " active=" + isActive + " markup=" + markup + " -> " + calcPrice + "rub");
    }
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
