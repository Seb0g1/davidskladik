#!/usr/bin/env node
"use strict";
require("dotenv").config();
const path = require("path");
const fs = require("node:fs/promises");
process.chdir(path.resolve(__dirname, ".."));

async function main() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");

  // List data dir to find snapshot file
  const files = await fs.readdir(dataDir).catch(() => []);
  const pmFiles = files.filter(f => f.includes("price") || f.includes("pm-") || f.includes("snapshot") || f.includes("pricemaster"));
  console.log(`\nData dir PM-related files: ${pmFiles.join(", ")}`);

  // Try common snapshot paths
  let snapshot = null;
  let snapshotFile = null;
  for (const fname of ["pricemaster-snapshot.json","pm-snapshot.json","pm-state.json","pricemaster-state.json","pricemaster-data.json"]) {
    try {
      const raw = await fs.readFile(path.join(dataDir, fname), "utf8");
      snapshot = JSON.parse(raw);
      snapshotFile = fname;
      break;
    } catch (_) {}
  }

  if (!snapshot) {
    console.log("No PM snapshot file found");
    return;
  }
  console.log(`Using: ${snapshotFile}`);

  const rows = Array.isArray(snapshot.rows) ? snapshot.rows
    : Array.isArray(snapshot) ? snapshot
    : Array.isArray(snapshot.data) ? snapshot.data : [];
  console.log(`Total rows: ${rows.length}, createdAt: ${snapshot.createdAt || snapshot.updatedAt || "?"}`);

  // First row for structure
  if (rows[0]) {
    console.log(`\nFirst row keys: ${Object.keys(rows[0]).join(", ")}`);
  }

  // Find K18001
  const k18001 = rows.filter(r => {
    const id = String(r.NativeID || r.nativeId || r.article || r.Article || "").trim();
    return id.toUpperCase() === "K18001";
  });
  console.log(`\nK18001 rows (${k18001.length}):`);
  for (const r of k18001) {
    console.log(`  RowID=${r.RowID||r.rowId} Article=${r.NativeID||r.article} PartnerID=${r.PartnerID||r.partnerId} PartnerName=${r.PartnerName||r.partnerName}`);
    console.log(`  Price=${r.Price||r.price} Currency=${r.Currency||r.currency||"not_stored"} Active=${r.Active||r.active} Ignored=${r.Ignored||r.ignored}`);
  }

  // Warehouse check
  const wh = JSON.parse(await fs.readFile(path.join(dataDir, "personal-warehouse.json"), "utf8"));
  const tim = (wh.suppliers||[]).find(s => String(s.partnerId)==="278");
  console.log(`\nТимофей warehouse.json: priceCurrency=${tim?.priceCurrency} pricingMode=${tim?.pricingMode}`);

  // Simulate
  if (k18001[0] && tim) {
    const rawPrice = Number(k18001[0].Price || k18001[0].price || 0);
    const cur = tim.priceCurrency || "USD";
    const rate = 85;
    const mk = 2.222;
    console.log(`\nSimulation: rawPrice=${rawPrice} currency=${cur} rate=${rate} markup=${mk}`);
    if (cur === "RUB") {
      const usd = rawPrice / rate;
      console.log(`  RUB-native path (convertFromRub): ${rawPrice} / ${rate} = ${usd.toFixed(6)} USD then ${usd.toFixed(6)} × ${rate} × ${mk} = ${Math.round(usd * rate * mk)}`);
    }
    console.log(`  USD path: ${rawPrice} × ${rate} × ${mk} = ${Math.round(rawPrice * rate * mk)}`);
    console.log(`  rubNative path (16 × markup): ${rawPrice} × ${mk} = ${Math.round(rawPrice * mk)}`);
  }
}

main().catch(e => { console.error(e.stack||e.message); process.exit(1); });
