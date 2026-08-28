#!/usr/bin/env node
"use strict";
require("dotenv").config();
const path = require("path");
const fs = require("fs");
process.chdir(path.resolve(__dirname, ".."));
const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");

const s = JSON.parse(fs.readFileSync(path.join(dataDir, "snapshot.json"), "utf8"));
const items = s.items || {};
const all = Object.values(items);
console.log("Total snapshot items:", all.length);

// K18001 rows
const k18 = all.filter(r => String(r.article || "").trim().toUpperCase() === "K18001");
console.log("K18001 entries:", k18.length);
for (const row of k18) {
  console.log("  partnerId=" + row.partnerId + " partnerName=" + row.partnerName + " price=" + row.price + " active=" + row.active);
}

// Check Тимофей (278)
const tim = k18.find(r => String(r.partnerId) === "278");
if (tim) {
  const price = Number(tim.price || 0);
  const rate = 85;
  const markup = 2.222;
  console.log("\nТимофей K18001:");
  console.log("  price=" + price + " active=" + tim.active + " ignored=" + tim.ignored);
  console.log("  USD path: " + price + " × " + rate + " × " + markup + " = " + Math.round(price * rate * markup));
  console.log("  RUB native: " + price + " × " + markup + " = " + Math.round(price * markup));

  // Check link sourceRowId
  const wh = JSON.parse(fs.readFileSync(path.join(dataDir, "personal-warehouse.json"), "utf8"));
  const tim_supp = (wh.suppliers || []).find(s => String(s.partnerId) === "278");
  console.log("\nТимофей in warehouse.json: priceCurrency=" + tim_supp?.priceCurrency + " stopped=" + tim_supp?.stopped);
} else {
  console.log("K18001 not found for partnerId=278");
}

// Check byArticle index key format (cleanText does trim() only, no lowercase)
const articleKey = "K18001"; // cleanText("K18001") = "K18001"
const inIndexByKey = all.filter(r => String(r.article || "").trim() === articleKey);
console.log("\nRows with article trim='K18001' (byArticle index):", inIndexByKey.length);
for (const r of inIndexByKey) {
  console.log("  partnerId=" + r.partnerId + " price=" + r.price + " active=" + r.active);
}
