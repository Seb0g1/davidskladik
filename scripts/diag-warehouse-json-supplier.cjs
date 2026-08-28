#!/usr/bin/env node
"use strict";
require("dotenv").config();
const path = require("path");
const fs = require("node:fs/promises");
process.chdir(path.resolve(__dirname, ".."));

async function main() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");

  // Check personal-warehouse.json suppliers
  const warehousePath = path.join(dataDir, "personal-warehouse.json");
  console.log(`\nReading from: ${warehousePath}`);

  let warehouse;
  try {
    const text = await fs.readFile(warehousePath, "utf8");
    warehouse = JSON.parse(text);
  } catch (e) {
    console.log(`ERROR reading warehouse: ${e.message}`);
    return;
  }

  const suppliers = Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [];
  console.log(`\nwarehouse.suppliers count: ${suppliers.length}`);
  console.log(`warehouse.createdAt: ${warehouse.createdAt}`);
  console.log(`warehouse.updatedAt: ${warehouse.updatedAt}`);
  console.log(`warehouse.postgresOnly: ${warehouse.postgresOnly}`);

  // Find Тимофей
  const timofey = suppliers.filter((s) => (s.name || "").toLowerCase().includes("тимоф") || (s.partnerId === "278") || String(s.partnerId) === "278");
  console.log(`\nТимофей in warehouse.json (${timofey.length}):`);
  for (const s of timofey) {
    console.log(`  id=${s.id} partnerId=${s.partnerId} name="${s.name}" priceCurrency=${s.priceCurrency} stopped=${s.stopped} source=${s.source}`);
  }

  // Show all RUB suppliers in warehouse.json
  const rubSuppliers = suppliers.filter((s) => s.priceCurrency === "RUB");
  console.log(`\nAll RUB suppliers in warehouse.json (${rubSuppliers.length}):`);
  for (const s of rubSuppliers) {
    console.log(`  id=${s.id} partnerId=${s.partnerId} name="${s.name}"`);
  }

  // Also check price-retry-queue.json
  const retryPath = path.join(dataDir, "price-retry-queue.json");
  console.log(`\nReading from: ${retryPath}`);
  try {
    const text = await fs.readFile(retryPath, "utf8");
    const queue = JSON.parse(text);
    const items = Array.isArray(queue.items) ? queue.items : [];
    const k18001 = items.filter((i) => i.offerId === "K18001");
    console.log(`\nprice-retry-queue.json K18001 items (${k18001.length}):`);
    for (const i of k18001) {
      console.log(`  marketplace=${i.marketplace} price=${i.price} status=${i.status} finalTargetPrice=${i.finalTargetPrice} nextRetryAt=${i.nextRetryAt}`);
    }
    console.log(`\nTotal items in queue: ${items.length}`);
  } catch (e) {
    console.log(`price-retry-queue.json: ${e.message}`);
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
