#!/usr/bin/env node
"use strict";
// Runs on prod: loads server code and directly calls buildFreshWarehouseProducts for K18001
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
process.env.SERVER_ROLE = "worker";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

// Load the full server stack (same as server.js does)
require("../server/source.js");

const { buildFreshWarehouseProducts } = require("../server/parts/03-lifecycle-exports.js");

async function main() {
  // K18001 product IDs
  const productIds = [
    "ozon-b114947e336e516250621929",
    "ozon-2f380604566bfa419239dc9f",
    "yandex-2611810083b012fda1896c11",
  ];

  console.log("\n=== Running buildFreshWarehouseProducts for K18001 ===\n");
  console.log(`productIds: ${productIds.join(", ")}`);

  const products = await buildFreshWarehouseProducts(productIds, {
    refreshPrices: true,
    livePriceMaster: false,
  });

  console.log(`\nResult: ${products.length} products\n`);
  for (const p of products) {
    console.log(`  [${p.marketplace}] offerId=${p.offerId}`);
    console.log(`    nextPrice=${p.nextPrice} rawNextPrice=${p.calculatedPrice || "N/A"}`);
    console.log(`    markupCoefficient=${p.markupCoefficient} baseMarkupCoefficient=${p.baseMarkupCoefficient}`);
    console.log(`    ready=${p.ready} changed=${p.changed}`);
    console.log(`    status=${p.status}`);
    if (p.selectedSupplier) {
      const s = p.selectedSupplier;
      console.log(`    supplier: name="${s.partnerName || s.supplierName}" price=${s.price} priceCurrency=${s.priceCurrency} effectiveFinalPrice=${s.effectiveFinalPrice} calculatedPrice=${s.calculatedPrice}`);
      console.log(`    supplier.rubNative=${s.rubNative} supplier.convertedFromRub=${s.convertedFromRub} supplier.originalPrice=${s.originalPrice}`);
    }
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
