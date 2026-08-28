#!/usr/bin/env node
"use strict";
const path = require("node:path");
const fs = require("node:fs");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const warehousePath = path.join(__dirname, "..", "data", "personal-warehouse.json");
let warehouse;
try {
  warehouse = JSON.parse(fs.readFileSync(warehousePath, "utf8"));
} catch (e) {
  console.error("Cannot read warehouse:", e.message);
  process.exit(1);
}

const products = Array.isArray(warehouse.products) ? warehouse.products : warehouse;
const q = process.argv[2] || "234123";

const found = products.filter(p =>
  String(p.offerId || p.id || "").includes(q) ||
  String(p.id || "").includes(q)
);

if (!found.length) {
  console.log(`Product ${q} not found. Trying partial match on name...`);
  const byName = products.filter(p => String(p.name || "").includes(q));
  console.log(`By name: ${byName.length} results`);
  for (const p of byName.slice(0, 3)) {
    console.log(JSON.stringify({ id: p.id, offerId: p.offerId, name: p.name, markup: p.markup, markupSource: p.markupSource, marketplace: p.marketplace }, null, 2));
  }
  process.exit(0);
}

for (const p of found) {
  const yandex = p.yandex || {};
  console.log(JSON.stringify({
    id: p.id,
    offerId: p.offerId,
    name: p.name,
    marketplace: p.marketplace,
    target: p.target,
    markup: p.markup,
    markupSource: p.markupSource,
    yandex_manualMarkup: yandex?.extra?.manualMarkup,
    yandex_markup: yandex?.markup,
    currentPrice: p.currentPrice,
    targetPrice: p.targetPrice,
    markupCoefficient: p.markupCoefficient,
  }, null, 2));
}
