#!/usr/bin/env node
"use strict";
// Direct price trace: reads snapshot + supplier data and computes K18001 price step by step
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
process.env.SERVER_ROLE = "worker";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

// Patch console.log to add prefix so we can distinguish our output
const _orig = console.log.bind(console);
console.log = (...a) => _orig("[TRACE]", ...a);

// Load only what we need (NOT the full server)
const { getPrisma } = require("../lib/postgres.js");
const fs = require("node:fs/promises");
const crypto = require("node:crypto");

async function main() {
  const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");
  const snapshotPath = path.join(dataDir, "snapshot.json");

  // 1. Load snapshot
  const snapshotText = await fs.readFile(snapshotPath, "utf8");
  const snapshot = JSON.parse(snapshotText);
  const snapshotRows = Object.values(snapshot.items || {});
  console.log(`Snapshot items: ${snapshotRows.length}, createdAt: ${snapshot.createdAt}`);

  // 2. Find K18001 in snapshot
  const k18001Rows = snapshotRows.filter((r) => {
    const article = String(r.article || r.NativeID || r.nativeId || "");
    return article.toUpperCase() === "K18001" || article === "K18001";
  });
  console.log(`K18001 snapshot rows (${k18001Rows.length}):`);
  for (const r of k18001Rows) {
    console.log(`  rowId=${r.rowId} partnerId=${r.partnerId} partnerName="${r.partnerName}" price=${r.price} currency="${r.currency}" priceCurrency="${r.priceCurrency}" active=${r.active}`);
    console.log(`    All keys: ${Object.keys(r).join(", ")}`);
  }

  // 3. Get Тимофей supplier from DB
  const prisma = getPrisma();
  const timofey = await prisma.$queryRawUnsafe(`SELECT id, partner_id, name, default_currency, raw FROM managed_suppliers WHERE partner_id = '278'`);
  const timofeyRec = timofey[0] || null;
  console.log(`Тимофей DB: name="${timofeyRec?.name}" defaultCurrency=${timofeyRec?.default_currency} raw.priceCurrency=${timofeyRec?.raw?.priceCurrency}`);

  // 4. Simulate normalizeManagedSupplier → supplierFromPostgres
  const supplierPriceCurrency = timofeyRec?.default_currency || "USD";
  console.log(`Тимофей.priceCurrency (from supplierFromPostgres): ${supplierPriceCurrency}`);

  // 5. Simulate managedSupplierPriceCurrency for the active row
  const activeRow = k18001Rows.find((r) => r.active && String(r.partnerId) === "278");
  if (!activeRow) { console.log("ERROR: No active K18001 row for partnerId=278"); return; }

  // fields.currency from priceMasterSnapshotRowFields
  const raw2 = activeRow.raw && typeof activeRow.raw === "object" ? activeRow.raw : {};
  const currencyRaw = (activeRow.currency || activeRow.priceCurrency || activeRow.sourceCurrency || raw2.currency || raw2.priceCurrency || raw2.Currency || "").toUpperCase();
  const currency = currencyRaw === "RUB" || currencyRaw === "RUR" ? "RUB" : currencyRaw === "USD" ? "USD" : "";
  console.log(`fields.currency (from priceMasterSnapshotRowFields): "${currency}" (currencyRaw="${currencyRaw}")`);

  // managedSupplierPriceCurrency
  const explicit = (supplierPriceCurrency || "").toUpperCase();
  console.log(`managedSupplierPriceCurrency: explicit="${explicit}"`);
  let resolvedCurrency;
  if (explicit === "RUB" || explicit === "RUR") {
    resolvedCurrency = "RUB";
  } else {
    const partnerName = (activeRow.partnerName || "").toLowerCase().trim();
    const isInna = partnerName.includes("инна") && !partnerName.includes("иванна");
    console.log(`  isInna("${partnerName}")=${isInna}`);
    if (isInna) {
      resolvedCurrency = "RUB";
    } else if (explicit === "USD") {
      resolvedCurrency = "USD";
    } else {
      resolvedCurrency = "USD";
    }
  }
  console.log(`resolvePriceMasterRowCurrency: "${resolvedCurrency}"`);

  // 6. Compute price
  const price = Number(activeRow.price || 0);
  const fixedRate = 85; // from app_settings
  const isRub = resolvedCurrency === "RUB";

  // markupCoefficient for ozon at $16 (from tiered rules) = 3.83
  const markup = 3.83;

  let computedPrice;
  if (isRub) {
    computedPrice = Math.round((price * markup) / 5) * 5;
    console.log(`rubNative=TRUE: ${price} × ${markup} = ${price * markup} → rounded=${computedPrice}`);
  } else {
    computedPrice = Math.round((price * fixedRate * markup) / 5) * 5;
    console.log(`rubNative=FALSE: ${price} × ${fixedRate} × ${markup} = ${price * fixedRate * markup} → rounded=${computedPrice}`);
  }

  console.log(`\n=== RESULT ===`);
  console.log(`K18001/ozon computed price: ${computedPrice}₽`);
  console.log(`Expected: ~5200₽ (if USD), got 61₽ in production (if RUB)`);
  console.log(`ruNative: ${isRub}`);

  // 7. Also read the product_links from PG and show link IDs
  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.supplier_article, pl.partner_id, pl.price_currency, pl.raw
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE wp.offer_id = 'K18001'
  `);
  console.log(`\nProduct links K18001 (${links.length}):`);
  for (const l of links) {
    const raw = typeof l.raw === "object" ? l.raw : {};
    console.log(`  id=${l.id} article=${l.supplier_article} partnerId=${l.partner_id} priceCurrency=${l.price_currency}`);
    console.log(`    raw.matchType=${raw.matchType} raw.sourceRowId=${raw.sourceRowId} raw.priceCurrency=${raw.priceCurrency}`);
    console.log(`    raw.supplierName=${raw.supplierName} raw.partnerId=${raw.partnerId}`);

    // Simulate matchMap lookup: what is the byArticle key?
    const linkArticle = raw.article || (raw.matchType ? "" : l.supplier_article);
    console.log(`    link.article="${linkArticle}" link.matchType="${raw.matchType || "article"}" link.id="${l.id}"`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
