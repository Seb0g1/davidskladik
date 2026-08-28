#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
const fs = require("fs");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

function cleanText(v) { return String(v || "").trim(); }
function normalizeSupplierName(v) { return cleanText(v).toLowerCase().replace(/\s+/g, " "); }

async function main() {
  const prisma = getPrisma();
  const dataDir = process.env.DATA_DIR || path.join(__dirname, "..", "data");

  // ── 1. Full raw JSON for K18001 yandex ────────────────────────────────────
  const [yRow] = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, target, target_price, current_price, raw
    FROM warehouse_products
    WHERE offer_id = 'K18001' AND marketplace = 'yandex'
  `);
  if (!yRow) { console.log("K18001 yandex not found"); process.exit(0); }
  const raw = typeof yRow.raw === "object" ? yRow.raw : (yRow.raw ? JSON.parse(yRow.raw) : {});
  console.log(`\n=== K18001 yandex DB state ===`);
  console.log(`  target_price=${yRow.target_price} current_price=${yRow.current_price}`);
  console.log(`  raw.targetPrice=${raw.targetPrice} raw.nextPrice=${raw.nextPrice}`);
  console.log(`  raw.currentPrice=${raw.currentPrice} raw.marketplacePrice=${raw.marketplacePrice}`);
  if (raw.selectedSupplier) console.log(`  raw.selectedSupplier.calculatedPrice=${raw.selectedSupplier?.calculatedPrice}`);

  // ── 2. Full raw link JSON ─────────────────────────────────────────────────
  const [linkRow] = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.partner_id, pl.price_currency, pl.supplier_article, pl.raw
    FROM product_links pl
    WHERE pl.product_id = $1
  `, yRow.id);
  if (!linkRow) { console.log("No link for K18001 yandex"); process.exit(0); }
  const lr = typeof linkRow.raw === "object" ? linkRow.raw : (linkRow.raw ? JSON.parse(linkRow.raw) : {});
  console.log(`\n=== K18001 yandex link ===`);
  console.log(`  partnerId=${linkRow.partner_id} priceCurrency=${linkRow.price_currency}`);
  console.log(`  supplierArticle=${linkRow.supplier_article}`);
  console.log(`  raw.article=${lr.article} raw.matchType=${lr.matchType}`);
  console.log(`  raw.sourceRowId=${lr.sourceRowId || "(empty)"}`);
  console.log(`  raw.exactName=${lr.exactName || "(empty)"}`);
  console.log(`  raw.supplierName=${lr.supplierName || "(empty)"}`);
  console.log(`  raw.partnerId=${lr.partnerId || "(empty)"}`);
  console.log(`  raw.priceCurrency=${lr.priceCurrency || "(empty)"}`);

  // ── 3. Normalise the link (same logic as normalizeWarehouseLink) ──────────
  const matchTypeRaw = cleanText(lr.matchType || "article");
  let matchType = ["article", "selected_row", "exact_name"].includes(matchTypeRaw) ? matchTypeRaw : "article";
  const article = cleanText(lr.article || linkRow.supplier_article || "");
  const sourceRowId = cleanText(lr.sourceRowId || "");
  const exactName = cleanText(lr.exactName || "");
  const preserveSelectedRow = matchType === "selected_row" && sourceRowId;
  const effectiveMatchType = article && !preserveSelectedRow ? "article" : matchType;
  const priceCurrencyRaw = cleanText(lr.priceCurrency || linkRow.price_currency || "USD").toUpperCase();
  const priceCurrency = priceCurrencyRaw === "RUB" || priceCurrencyRaw === "RUR" ? "RUB" : "USD";
  console.log(`\n=== After normalizeWarehouseLink ===`);
  console.log(`  article="${article}" sourceRowId="${sourceRowId}" exactName="${exactName}"`);
  console.log(`  raw matchType="${matchTypeRaw}" → effectiveMatchType="${effectiveMatchType}"`);
  console.log(`  priceCurrency="${priceCurrency}"`);
  if (matchTypeRaw === "selected_row" && effectiveMatchType === "article") {
    console.log(`  *** matchType DOWNGRADED to "article" because sourceRowId is empty! ***`);
  }

  // ── 4. PM snapshot lookup ─────────────────────────────────────────────────
  const snapshotPath = path.join(dataDir, "snapshot.json");
  if (!fs.existsSync(snapshotPath)) { console.log("No snapshot.json"); process.exit(0); }
  const snap = JSON.parse(fs.readFileSync(snapshotPath, "utf8"));
  const snapshotItems = Object.values(snap.items || {});

  // Build byArticle index (article key is cleanText = trim)
  const byArticle = new Map();
  for (const item of snapshotItems) {
    const key = cleanText(item.article);
    if (!byArticle.has(key)) byArticle.set(key, []);
    byArticle.get(key).push(item);
  }
  const byRowId = new Map();
  for (const item of snapshotItems) {
    const key = cleanText(String(item.rowId || ""));
    if (key) byRowId.set(key, item);
  }

  console.log(`\n=== PM snapshot lookup for article="${article}" ===`);
  const candidates = byArticle.get(article) || [];
  console.log(`  byArticle.get("${article}"): ${candidates.length} rows`);
  for (const c of candidates) {
    console.log(`  partnerId=${c.partnerId} partnerName="${c.partnerName}" price=${c.price} active=${c.active} rowId=${c.rowId}`);
    console.log(`    currency=${c.currency || c.priceCurrency || "(none)"}`);
    const lPartnerOk = effectiveMatchType === "article" ? true : (!lr.partnerId || String(c.partnerId || "") === String(lr.partnerId));
    const supplierName = cleanText(lr.supplierName || "");
    const supplierOk = !supplierName || normalizeSupplierName(c.partnerName) === normalizeSupplierName(supplierName);
    console.log(`    partnerOk=${lPartnerOk} supplierOk=${supplierOk}`);
  }

  // Simulate priceCurrency resolution per candidate
  const [timRow] = await prisma.$queryRawUnsafe(`SELECT partner_id, name, default_currency, raw FROM managed_suppliers WHERE partner_id = '278'`);
  const allSuppliers = await prisma.$queryRawUnsafe(`SELECT partner_id, name, default_currency FROM managed_suppliers LIMIT 100`);
  console.log(`\n=== Currency resolution per candidate ===`);
  for (const c of candidates) {
    // findManagedSupplierForPriceMasterRow
    const found = allSuppliers.find((s) => String(s.partner_id) === String(c.partnerId));
    const suppPriceCurrency = cleanText(found?.default_currency || "").toUpperCase();
    let resolvedCurrency;
    if (suppPriceCurrency === "RUB" || suppPriceCurrency === "RUR") {
      resolvedCurrency = "RUB";
    } else {
      const pname = normalizeSupplierName(found?.name || c.partnerName || "");
      const isInna = pname.includes("инна") || pname.includes("inna");
      if (isInna) resolvedCurrency = "RUB";
      else if (suppPriceCurrency === "USD") resolvedCurrency = "USD";
      else resolvedCurrency = "USD";
    }
    const price = Number(c.price || 0);
    const rate = Number(process.env.DEFAULT_USD_RATE || 85);
    const markup = 2.222;
    const rubNative = resolvedCurrency === "RUB";
    const calcPrice = rubNative ? price * markup : price * rate * markup;
    console.log(`  partner=${c.partnerName} suppCurrency=${suppPriceCurrency} → resolvedCurrency=${resolvedCurrency} rubNative=${rubNative}`);
    console.log(`    price=${price} → calculateRubPrice ≈ ${Math.round(calcPrice)}`);
  }

  // ── 5. Show currentPrice propagation ─────────────────────────────────────
  console.log(`\n=== persistedNextPrice / currentPrice fallback ===`);
  const targetPrice = yRow.target_price ?? raw.targetPrice ?? raw.nextPrice;
  const currentPrice = raw.currentPrice ?? raw.marketplacePrice;
  console.log(`  product.targetPrice = ${targetPrice}`);
  console.log(`  product.currentPrice (for display) = ${currentPrice}`);
  console.log(`  persistedNextPrice = ${Number(targetPrice || 0)}`);
  if (Number(targetPrice || 0) > 0) {
    console.log(`  *** persistedNextPrice=${Number(targetPrice||0)} > 0 — would be used as yandex fallback if no PM match ***`);
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
