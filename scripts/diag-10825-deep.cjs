#!/usr/bin/env node
"use strict";
// Deep diagnosis of 10825 YM price=67₽ issue.
// Shows every field that affects price computation.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // 1. Full raw JSON of 10825 YM product
  console.log("=== 10825 YM product raw JSON ===\n");
  const ym = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id, marketplace, current_price, target_price, target_stock,
           raw->>'markup' AS markup,
           raw->>'autoPriceMin' AS auto_price_min,
           raw->>'autoPriceMax' AS auto_price_max,
           raw->>'nextPrice' AS next_price,
           raw->>'targetPrice' AS raw_target_price,
           raw->>'priceSource' AS price_source,
           raw->>'fixedUsdRate' AS fixed_usd_rate,
           raw->'links' AS links_json,
           raw->'lastYandexPriceSend' AS last_ym_send
    FROM warehouse_products WHERE offer_id ILIKE '10825' AND marketplace = 'yandex'
  `);
  if (!ym.length) { console.log("Not found"); return; }
  const w = ym[0];
  console.log(`id=${w.id} currentPrice=${w.current_price} targetPrice=${w.target_price}`);
  console.log(`markup=${w.markup} autoPriceMin=${w.auto_price_min} autoPriceMax=${w.auto_price_max}`);
  console.log(`nextPrice(raw)=${w.next_price} targetPrice(raw)=${w.raw_target_price} priceSource=${w.price_source}`);
  console.log(`fixedUsdRate(in raw)=${w.fixed_usd_rate}`);

  // Show Дима америка link details
  const links = typeof w.links_json === "string" ? JSON.parse(w.links_json) : (Array.isArray(w.links_json) ? w.links_json : []);
  const dimaLink = links.find((l) => /дима/i.test(l.supplierName || ""));
  if (dimaLink) {
    console.log(`\nДима америка link in raw.links:`);
    console.log(`  id=${dimaLink.id}`);
    console.log(`  article=${dimaLink.article || "(empty)"}`);
    console.log(`  sourceRowId=${dimaLink.sourceRowId || "(empty)"}`);
    console.log(`  matchType=${dimaLink.matchType}`);
    console.log(`  partnerId=${dimaLink.partnerId || "(empty)"}`);
    console.log(`  priceCurrency=${dimaLink.priceCurrency || "(empty)"}`);
    if (dimaLink.resolvedPriceMasterRow) {
      const rpm = dimaLink.resolvedPriceMasterRow;
      console.log(`  resolvedPriceMasterRow.rowId=${rpm.rowId}`);
      console.log(`  resolvedPriceMasterRow.price=${rpm.price}`);
      console.log(`  resolvedPriceMasterRow.priceCurrency=${rpm.priceCurrency || "(empty)"}`);
      console.log(`  resolvedPriceMasterRow.partnerName=${rpm.partnerName}`);
    }
  }

  // 2. product_links rows for this product — check priceCurrency column
  console.log("\n=== product_links for 10825 YM ===\n");
  const pgLinks = await prisma.$queryRawUnsafe(`
    SELECT id, supplier_name, supplier_article, partner_id, price_currency,
           raw->>'matchType' AS match_type,
           raw->>'sourceRowId' AS source_row_id,
           raw->>'article' AS raw_article,
           raw->>'priceCurrency' AS raw_price_currency,
           raw->'resolvedPriceMasterRow' AS rpm
    FROM product_links WHERE product_id = '${String(w.id).replace(/[^a-zA-Z0-9_-]/g, "")}'
    ORDER BY supplier_name ASC
  `);
  for (const l of pgLinks) {
    const rpm = l.rpm && typeof l.rpm === "object" ? l.rpm : {};
    console.log(`  ${l.supplier_name}: article_col=${l.supplier_article} partnerId_col=${l.partner_id} priceCurrency_col=${l.price_currency}`);
    console.log(`    raw.matchType=${l.match_type} raw.sourceRowId=${l.source_row_id} raw.article=${l.raw_article || "(empty)"} raw.priceCurrency=${l.raw_price_currency || "(empty)"}`);
    console.log(`    rpm.rowId=${rpm.rowId} rpm.price=${rpm.price} rpm.priceCurrency=${rpm.priceCurrency || "(none)"}`);
  }

  // 3. Дима америка in managed_suppliers — check raw JSON for priceCurrency
  console.log("\n=== Дима америка in managed_suppliers ===\n");
  const dima = await prisma.$queryRawUnsafe(`
    SELECT id, name, partner_id, default_currency, active, note, stop_reason,
           raw->>'priceCurrency' AS raw_price_currency,
           raw->>'defaultCurrency' AS raw_default_currency,
           raw->>'pricingMode' AS raw_pricing_mode,
           raw->>'stopped' AS raw_stopped
    FROM managed_suppliers WHERE name ILIKE '%дима%' OR name ILIKE '%america%' OR partner_id = '99'
  `);
  for (const s of dima) {
    console.log(`  id=${s.id} name=${s.name} partnerId=${s.partner_id}`);
    console.log(`  default_currency(col)=${s.default_currency} active=${s.active}`);
    console.log(`  raw.priceCurrency=${s.raw_price_currency || "(empty)"} raw.defaultCurrency=${s.raw_default_currency || "(empty)"}`);
    console.log(`  raw.pricingMode=${s.raw_pricing_mode || "(empty)"} raw.stopped=${s.raw_stopped || "(false)"}`);
  }

  // 4. AppSettings
  console.log("\n=== AppSettings ===\n");
  const appRow = await prisma.appSetting.findUnique({ where: { key: "app" } });
  const app = appRow?.value || {};
  console.log(`  fixedUsdRate=${app.fixedUsdRate}`);
  console.log(`  defaultMarkups=${JSON.stringify(app.defaultMarkups || {})}`);
  const rules = app.markupRules || [];
  console.log(`  markupRules count=${rules.length}`);
  for (const r of rules) {
    console.log(`    rule: marketplace=${r.marketplace} minUsd=${r.minUsd} coefficient=${r.coefficient}`);
  }

  // 5. pm_snapshot_items for rowId=2038752
  console.log("\n=== pm_snapshot_items for rowId=2038752 ===\n");
  const pmRow = await prisma.$queryRawUnsafe(`
    SELECT row_id, article, partner_id, partner_name, price, currency, active, native_name
    FROM pm_snapshot_items WHERE row_id = '2038752'
  `);
  if (pmRow.length) {
    const r = pmRow[0];
    console.log(`  rowId=${r.row_id} article="${r.article}" partner=${r.partner_name}(${r.partner_id})`);
    console.log(`  price=${r.price} currency="${r.currency}" active=${r.active}`);
    console.log(`  name=${String(r.native_name || "").slice(0, 50)}`);
  } else {
    console.log("  NOT FOUND");
  }

  // 6. Simulate the price computation with actual values
  console.log("\n=== Price computation simulation ===\n");
  const rate = Number(app.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);
  const pmPrice = Number(pmRow[0]?.price || 0);
  const pmCurrency = String(pmRow[0]?.currency || "USD").toUpperCase();
  const dimaDefaultCurrency = String(dima[0]?.default_currency || "USD").toUpperCase();
  const dimaRawPriceCurrency = String(dima[0]?.raw_price_currency || "").toUpperCase();
  // managedSupplierPriceCurrency logic
  const explicit = (dimaDefaultCurrency || dimaRawPriceCurrency).toUpperCase();
  const effectivePriceCurrency = explicit === "RUB" || explicit === "RUR" ? "RUB" : "USD";
  const rubNative = effectivePriceCurrency === "RUB";

  console.log(`  rate=${rate} pmPrice=${pmPrice} pmCurrency=${pmCurrency}`);
  console.log(`  dimaDefaultCurrency=${dimaDefaultCurrency} dimaRawPriceCurrency=${dimaRawPriceCurrency}`);
  console.log(`  effectivePriceCurrency=${effectivePriceCurrency} rubNative=${rubNative}`);

  // Product markup
  const productMarkup = Number(w.markup) || 0;
  const yandexMarkup = Number(app.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);

  console.log(`  productMarkup=${productMarkup} yandexMarkup=${yandexMarkup}`);

  // resolveMarkupCoefficient
  let markup;
  if (productMarkup > 0) {
    markup = productMarkup;
    console.log(`  productMarkup > 0 → markup = ${markup}`);
  } else {
    let usd = pmPrice;
    if ((pmCurrency === "RUB" || pmCurrency === "RUR") && usd > 0) usd = usd / rate;
    const yandexRules = rules.filter((r) => !r.marketplace || r.marketplace === "all" || r.marketplace === "yandex");
    if (yandexRules.length && usd > 0) {
      const sorted = [...yandexRules].sort((a, b) => b.minUsd - a.minUsd);
      const matched = sorted.find((r) => usd >= Number(r.minUsd || 0));
      markup = Number(matched?.coefficient || yandexMarkup);
      console.log(`  markup from rules: usd=${usd} → matched minUsd=${matched?.minUsd} coeff=${matched?.coefficient} → markup=${markup}`);
    } else {
      markup = yandexMarkup;
      console.log(`  no rules/usd, fallback yandexMarkup=${markup}`);
    }
  }

  let calculatedPrice;
  if (rubNative) {
    calculatedPrice = Math.round(pmPrice * markup);
    console.log(`  rubNative=TRUE: ${pmPrice} * ${markup} = ${calculatedPrice}₽`);
  } else {
    calculatedPrice = Math.round(pmPrice * rate * markup);
    console.log(`  rubNative=FALSE: ${pmPrice} * ${rate} * ${markup} = ${calculatedPrice}₽`);
  }

  const autoPriceMax = Number(w.auto_price_max || 0);
  const autoPriceMin = Number(w.auto_price_min || 0);
  let nextPrice = calculatedPrice;
  if (autoPriceMin > 0 && nextPrice < autoPriceMin) nextPrice = autoPriceMin;
  if (autoPriceMax > 0 && nextPrice > autoPriceMax) nextPrice = autoPriceMax;
  console.log(`  autoPriceMin=${autoPriceMin} autoPriceMax=${autoPriceMax}`);
  console.log(`  → rawNextPrice=${calculatedPrice} nextPrice=${nextPrice}₽`);

  // Yandex fallback: if nextPrice=0, uses persistedNextPrice
  const persistedNextPrice = Number(w.target_price || w.next_price || 0);
  if (nextPrice <= 0 && persistedNextPrice > 0) {
    console.log(`  ⚠ nextPrice=0, YM fallback: persistedNextPrice=${persistedNextPrice}₽`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
