#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Read the app settings
  const appSetting = await prisma.$queryRawUnsafe(`SELECT value FROM app_settings WHERE key = 'app'`).catch(() => []);
  const appSettings = appSetting.length
    ? (typeof appSetting[0].value === "string" ? JSON.parse(appSetting[0].value) : appSetting[0].value)
    : {};
  const fixedUsdRate = appSettings.fixedUsdRate || 85;

  // Read the managed supplier for Тимофей Косметика
  const timSupplier = await prisma.$queryRawUnsafe(`
    SELECT id, name, partner_id AS "partnerId", default_currency AS "defaultCurrency",
           raw
    FROM managed_suppliers
    WHERE name ILIKE '%тимоф%'
  `).catch(() => []);

  console.log("\n=== Тимофей Косметика managed supplier ===\n");
  for (const s of timSupplier) {
    console.log(`  id=${s.id} name=${s.name} partnerId=${s.partnerId} defaultCurrency=${s.defaultCurrency}`);
    if (s.raw) {
      const rawJson = typeof s.raw === "string" ? JSON.parse(s.raw) : s.raw;
      console.log(`  raw.priceCurrency=${rawJson?.priceCurrency} raw.currency=${rawJson?.currency}`);
    }
  }

  // Get K18001 PM snapshot row
  const pmRow = await prisma.$queryRawUnsafe(`
    SELECT row_id, partner_id, partner_name, article, price, currency, active
    FROM pm_snapshot_items
    WHERE row_id = '2226806'
  `).catch(() => []);

  console.log("\n=== K18001 PM snapshot row ===\n");
  for (const r of pmRow) {
    console.log(`  rowId=${r.row_id} partner=${r.partner_name} partnerId=${r.partner_id} currency=${r.currency} price=${r.price}`);
  }

  // Now simulate the price calculation
  console.log("\n=== Price calculation simulation ===\n");
  const pmPrice = 16;
  const pmCurrency = "USD";
  const rate = fixedUsdRate;
  const markupRules = appSettings.markupRules || [];

  // managedSupplierPriceCurrency logic
  const timRow = timSupplier[0];
  const supplierPriceCurrency = timRow
    ? (timRow.defaultCurrency || "USD")  // what Prisma returns
    : "USD";
  console.log(`supplierPriceCurrency (from managed supplier): ${supplierPriceCurrency}`);

  // resolvePriceMasterRowCurrency would return this
  const priceCurrency = supplierPriceCurrency; // simplified (isInna check skipped)
  console.log(`priceCurrency for K18001: ${priceCurrency}`);

  // normalizePriceMasterPrice
  const isRub = priceCurrency === "RUB" || priceCurrency === "RUR";
  const normalizedPrice = {
    price: Number(pmPrice),
    originalPrice: Number(pmPrice),
    sourceCurrency: isRub ? "RUB" : "USD",
    convertedFromRub: isRub,
    priceCurrency: isRub ? "RUB" : "USD",
  };
  console.log(`normalizedPrice: price=${normalizedPrice.price} priceCurrency=${normalizedPrice.priceCurrency} convertedFromRub=${normalizedPrice.convertedFromRub}`);

  // resolveMarkupCoefficient
  let usd = normalizedPrice.price;
  if (normalizedPrice.priceCurrency === "RUB" && usd > 0) {
    usd = usd / rate; // convert to USD for rule lookup
  }
  const ozRules = markupRules
    .filter((r) => !r.marketplace || r.marketplace === "ozon")
    .sort((a, b) => b.minUsd - a.minUsd);
  const ozMatched = ozRules.find((r) => usd >= Number(r.minUsd || 0));
  const ozCoefficient = ozMatched?.coefficient || appSettings.defaultMarkups?.ozon || 1.666;
  console.log(`resolveMarkupCoefficient(ozon, usd=${usd}): coefficient=${ozCoefficient} from rule minUsd=${ozMatched?.minUsd}`);

  const ymRules = markupRules
    .filter((r) => !r.marketplace || r.marketplace === "yandex")
    .sort((a, b) => b.minUsd - a.minUsd);
  const ymMatched = ymRules.find((r) => usd >= Number(r.minUsd || 0));
  const ymCoefficient = ymMatched?.coefficient || appSettings.defaultMarkups?.yandex || 1.5837;
  console.log(`resolveMarkupCoefficient(yandex, usd=${usd}): coefficient=${ymCoefficient} from rule minUsd=${ymMatched?.minUsd}`);

  // calculateRubPrice
  // ctx = match object (which has priceCurrency from normalizedPrice)
  const rubNative = normalizedPrice.priceCurrency === "RUB"; // simplified supplierPriceIsRubNative
  if (rubNative) {
    const rubBase = normalizedPrice.originalPrice;
    const ozPrice = Math.round(rubBase * ozCoefficient);
    const ymPrice = Math.round(rubBase * ymCoefficient);
    console.log(`\ncalculateRubPrice (rubNative=TRUE):`);
    console.log(`  Ozon: rubBase=${rubBase} × coef=${ozCoefficient} = ${ozPrice}₽`);
    console.log(`  YM:   rubBase=${rubBase} × coef=${ymCoefficient} = ${ymPrice}₽`);
  } else {
    const ozPrice = Math.round(normalizedPrice.price * rate * ozCoefficient);
    const ymPrice = Math.round(normalizedPrice.price * rate * ymCoefficient);
    console.log(`\ncalculateRubPrice (rubNative=FALSE):`);
    console.log(`  Ozon: price=${normalizedPrice.price} × rate=${rate} × coef=${ozCoefficient} = ${ozPrice}₽`);
    console.log(`  YM:   price=${normalizedPrice.price} × rate=${rate} × coef=${ymCoefficient} = ${ymPrice}₽`);
  }

  console.log(`\n→ DB shows new_price=61 (ozon) and new_price=36 (yandex)`);
  console.log(`→ 16 × ${ozCoefficient} = ${(16 * ozCoefficient).toFixed(1)}₽ (without rate)`);
  console.log(`→ 16 × ${ymCoefficient} = ${(16 * ymCoefficient).toFixed(1)}₽ (without rate)`);

  // Check if pm_snapshot_items.partner_id matches the managed supplier's partner_id
  const pmPartnerId = pmRow[0]?.partner_id;
  const managedPartnerId = timRow?.partnerId;
  console.log(`\nPM partnerId=${pmPartnerId}, managedSupplier partnerId=${managedPartnerId}`);
  if (pmPartnerId !== managedPartnerId?.toString()) {
    console.log("  WARNING: partnerId MISMATCH! findManagedSupplierForPriceMasterRow byPartnerId lookup will fail!");
    console.log("  The managed supplier will be looked up by NAME instead.");
  } else {
    console.log("  partnerId matches — managed supplier found correctly");
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
