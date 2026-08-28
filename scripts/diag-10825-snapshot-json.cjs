#!/usr/bin/env node
"use strict";
// Checks the actual JSON snapshot file for rowId=2038752 (Дима америка / 10825).
// Also reads the product's links from the in-memory warehouse (PostgreSQL version).

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const path = require("path");
const fs = require("fs").promises;

// Snapshot path matches server config
const dataDir = path.resolve(process.cwd(), "data");
const snapshotPath = path.join(dataDir, "snapshot.json");

async function main() {
  console.log("=== JSON snapshot check for 10825 / Дима америка ===\n");
  console.log("Snapshot path:", snapshotPath);

  try {
    const stat = await fs.stat(snapshotPath);
    console.log(`Snapshot file size: ${(stat.size / 1024 / 1024).toFixed(1)} MB, modified: ${stat.mtime.toISOString()}`);
  } catch (e) {
    console.log("Snapshot file NOT FOUND:", e.message);
    return;
  }

  let snapshot;
  try {
    const text = await fs.readFile(snapshotPath, "utf8");
    snapshot = JSON.parse(text);
    console.log(`Snapshot createdAt=${snapshot.createdAt} syncId=${snapshot.syncId}`);
    const items = snapshot.items || {};
    const count = Object.keys(items).length;
    console.log(`Total snapshot items: ${count}`);
  } catch (e) {
    console.log("Error reading/parsing snapshot:", e.message);
    return;
  }

  // Search for rowId=2038752 in snapshot items
  const items = snapshot.items || {};
  const target = "2038752";
  let found = null;
  for (const [stableId, row] of Object.entries(items)) {
    const rowId = String(row.rowId || row.RowID || "");
    if (rowId === target) {
      found = { stableId, ...row };
      break;
    }
  }

  if (found) {
    console.log(`\nFound rowId=${target} in JSON snapshot:`);
    console.log(`  stableId=${found.stableId}`);
    console.log(`  RowID=${found.RowID || found.rowId}`);
    console.log(`  NativeID=${found.NativeID || found.article || found.nativeId || "(empty)"}`);
    console.log(`  NativeName=${String(found.NativeName || found.name || "").slice(0, 60)}`);
    console.log(`  NativePrice=${found.NativePrice || found.price}`);
    console.log(`  PartnerID=${found.PartnerID || found.partnerId}`);
    console.log(`  PartnerName=${found.PartnerName || found.partnerName}`);
    console.log(`  Active=${found.Active !== undefined ? found.Active : found.active}`);
    console.log(`  DocDate=${found.DocDate || found.docDate}`);
  } else {
    console.log(`\nrowId=${target} NOT FOUND in JSON snapshot!`);
    // Show a few other rows to see the format
    const sample = Object.values(items).slice(0, 2);
    if (sample.length) {
      console.log("\nSample row format:");
      console.log(JSON.stringify(sample[0], null, 2).slice(0, 500));
    }
  }

  // Also check how many active rows with partnerId=99 (Дима америка) are in the snapshot
  const dimaRows = Object.values(items).filter((r) => String(r.PartnerID || r.partnerId || "") === "99");
  console.log(`\nAll rows with PartnerID=99 (Дима америка) in snapshot: ${dimaRows.length}`);
  for (const r of dimaRows) {
    const rowId = r.rowId || r.RowID;
    const active = r.active !== false && r.Active !== false && r.Active !== 0;
    const price = r.NativePrice || r.price;
    const art = r.NativeID || r.article || r.nativeId || "(empty)";
    console.log(`  rowId=${rowId} art="${art}" price=${price} active=${active} name=${String(r.NativeName || r.name || "").slice(0, 40)}`);
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
