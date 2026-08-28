#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
function exec(conn, command, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("exec timeout")), timeoutMs);
    conn.exec(command, (err, stream) => {
      if (err) { clearTimeout(timer); return reject(err); }
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => { clearTimeout(timer); resolve(); });
    });
  });
}
async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
  });
  try {
    const remoteRoot = "/var/www/davidsklad/davidskladik";

    console.log("\n=== calculateRubPrice in prod code ===\n");
    await exec(conn, `grep -n "calculateRubPrice\\|roundPrice(price.*usdRate\\|rubBase.*markup" ${remoteRoot}/server/parts/02a-brand-index-core.js | head -20`, 10000);

    console.log("\n=== resolveMarkupCoefficient body in prod ===\n");
    await exec(conn, `grep -n "resolveMarkupCoefficient\\|coefficient\\|matched\\.coefficient" ${remoteRoot}/server/parts/02a-price-master-warehouse-helpers.js | head -20`, 10000);

    console.log("\n=== Checking if prod code uses different formula ===\n");
    await exec(conn, `grep -n "price.*markup\\|markup.*price\\|pmPrice.*coefficient" ${remoteRoot}/server/parts/02a-price-master-warehouse-helpers.js | head -30`, 10000);

    // Check the actual fresh products code on prod
    console.log("\n=== buildFreshWarehouseProductsForWarehouse rate logic ===\n");
    await exec(conn, `grep -n "rateSource\\|fixedUsdRate\\|getUsdRate\\|rate = Number" ${remoteRoot}/server/parts/02a-price-master-fresh-products.js | head -20`, 10000);

    // Check what enrichSupplierPriceCandidates does with markup
    console.log("\n=== enrichSupplierPriceCandidates effectiveFinalPrice ===\n");
    await exec(conn, `grep -n "effectiveFinalPrice\\|calculateRubPrice" ${remoteRoot}/server/parts/02a-price-master-warehouse-helpers.js | head -20`, 10000);
  } finally { conn.end(); }
}
main().catch((e) => { console.error(e.message); process.exit(1); });
