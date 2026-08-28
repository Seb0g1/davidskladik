#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
const fs = require("node:fs/promises");
process.chdir(path.resolve(__dirname, ".."));

async function main() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");
  const snapshotPath = path.join(dataDir, "snapshot.json");

  console.log(`\nReading snapshot from: ${snapshotPath}\n`);
  let snapshot;
  try {
    const text = await fs.readFile(snapshotPath, "utf8");
    snapshot = JSON.parse(text);
  } catch (e) {
    console.error(`Failed to read snapshot: ${e.message}`);
    process.exit(1);
  }

  console.log(`Snapshot createdAt: ${snapshot.createdAt}`);
  console.log(`Snapshot syncId: ${snapshot.syncId}`);
  console.log(`Snapshot items count: ${Object.keys(snapshot.items || {}).length}`);

  // Search for K18001 in snapshot items
  const items = snapshot.items || {};
  const k18001Rows = [];
  for (const [key, val] of Object.entries(items)) {
    const rows = Array.isArray(val) ? val : [val];
    for (const row of rows) {
      const article = String(row.NativeID || row.article || row.nativeId || "");
      if (article.toUpperCase() === "K18001" || article === "K18001") {
        k18001Rows.push({ key, row });
      }
    }
  }

  // Also check 'rows' array if it exists
  const allRows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
  for (const row of allRows) {
    const article = String(row.NativeID || row.article || row.nativeId || row.offerId || "");
    if (article.toUpperCase() === "K18001" || article === "K18001") {
      k18001Rows.push({ key: "rows[]", row });
    }
  }

  console.log(`\n=== K18001 entries in snapshot.json (${k18001Rows.length} found) ===\n`);
  for (const { key, row } of k18001Rows) {
    console.log(`  key="${key}"`);
    console.log(`    NativeID=${row.NativeID} article=${row.article}`);
    console.log(`    PartnerID=${row.PartnerID} partnerId=${row.partnerId} PartnerName=${row.PartnerName}`);
    console.log(`    NativePrice=${row.NativePrice} price=${row.price}`);
    console.log(`    Currency=${row.Currency} currency=${row.currency} priceCurrency=${row.priceCurrency}`);
    console.log(`    Active=${row.Active} active=${row.active} Ignored=${row.Ignored}`);
    console.log(`    RowID=${row.RowID} rowId=${row.rowId}`);
    console.log(`    raw keys: ${Object.keys(row).join(", ")}`);
  }

  if (k18001Rows.length === 0) {
    // Try to find by checking first 5 items to understand structure
    console.log("\n  NOT FOUND. Showing first 2 items for structure reference:");
    let count = 0;
    for (const [key, val] of Object.entries(items)) {
      if (count >= 2) break;
      const rows = Array.isArray(val) ? val : [val];
      console.log(`  key="${key}" val=${JSON.stringify(rows[0]).substring(0, 200)}`);
      count++;
    }
    if (allRows.length > 0) {
      console.log(`\n  snapshot.rows[0]: ${JSON.stringify(allRows[0]).substring(0, 300)}`);
    }
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
