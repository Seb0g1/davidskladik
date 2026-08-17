#!/usr/bin/env node
"use strict";
// Diagnostic: what PM row is LSMN100 actually bound to, and why?
// Run ON THE SERVER: node scripts/run-prod-diag-lsmn100.cjs
require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const mysql = require("mysql2/promise");

const OFFER_ID = process.argv[2] || "LSMN100";

async function main() {
  const prisma = new PrismaClient();

  console.log(`\n=== Diagnostic for offerId="${OFFER_ID}" ===\n`);

  // 1. Find the product in PostgreSQL
  const product = await prisma.warehouseProduct.findFirst({
    where: { offerId: OFFER_ID },
    include: { links: true },
  });

  if (!product) {
    console.log(`✗ Product "${OFFER_ID}" not found in warehouseProducts`);
    await prisma.$disconnect();
    return;
  }

  console.log(`✓ Product found: id=${product.id}`);
  console.log(`  name: ${product.name || "(none)"}`);
  console.log(`  priceUsd: ${product.priceUsd ?? "(none)"}`);
  console.log(`  priceRub: ${product.priceRub ?? "(none)"}`);
  console.log(`  priceSource: ${product.priceSource || "(none)"}`);
  console.log(`  archived: ${product.archived}`);
  console.log(`  inStock: ${product.inStock}`);
  console.log(`  links (${product.links.length}):`);

  for (const link of product.links) {
    console.log(`\n  ── Link id=${link.id} ──`);
    console.log(`    matchType:    ${link.matchType}`);
    console.log(`    article:      ${link.article || "(empty)"}`);
    console.log(`    sourceRowId:  ${link.sourceRowId || "(none)"}`);
    console.log(`    exactName:    ${link.exactName || "(none)"}`);
    console.log(`    supplierName: ${link.supplierName || "(none)"}`);
    console.log(`    partnerId:    ${link.partnerId || "(none)"}`);
    console.log(`    keyword:      ${link.keyword || "(none)"}`);
    console.log(`    resolvedBy:   ${link.resolvedBy || "(none)"}`);
  }

  // 2. Connect to PriceMaster MySQL and check what PM has for this article and name
  const conn = await mysql.createConnection({
    host: process.env.PM_DB_HOST,
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME,
    connectTimeout: 10000,
  });

  // 3. What does PM have for article = LSMN100 (and variants)?
  console.log(`\n=== PriceMaster rows with NativeID LIKE "LSMN100" ===`);
  const [byArticle] = await conn.query(`
    SELECT r.RowID, r.NativeID, r.NativeName, r.NativePrice, r.Active, r.Ignored,
           d.DocDate, d.PartnerID, p.PartnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE r.NativeID LIKE ? AND r.Ignored = 0
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 20
  `, [`%LSMN100%`]);

  if (byArticle.length) {
    for (const r of byArticle) {
      console.log(`  RowID=${r.RowID} Article="${r.NativeID}" Name="${r.NativeName}" Price=${r.NativePrice} Active=${r.Active} Partner="${r.PartnerName}" Date=${String(r.DocDate).slice(0,10)}`);
    }
  } else {
    console.log("  (none found)");
  }

  // 4. What does PM have for "SUR MESURE NOIRE" or "LATTAFA SUR"?
  console.log(`\n=== PriceMaster rows with name LIKE "%SUR MESURE NOIRE%" ===`);
  const [byName] = await conn.query(`
    SELECT r.RowID, r.NativeID, r.NativeName, r.NativePrice, r.Active, r.Ignored,
           d.DocDate, d.PartnerID, p.PartnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE r.NativeName LIKE ? AND r.Ignored = 0
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 20
  `, [`%SUR MESURE NOIRE%`]);

  if (byName.length) {
    for (const r of byName) {
      console.log(`  RowID=${r.RowID} Article="${r.NativeID}" Name="${r.NativeName}" Price=${r.NativePrice} Active=${r.Active} Partner="${r.PartnerName}" Date=${String(r.DocDate).slice(0,10)}`);
    }
  } else {
    console.log("  (none found)");
  }

  // 5. Check the actual linked sourceRowId if present
  for (const link of product.links) {
    if (link.sourceRowId) {
      console.log(`\n=== PriceMaster row for sourceRowId=${link.sourceRowId} ===`);
      const [pinned] = await conn.query(`
        SELECT r.RowID, r.NativeID, r.NativeName, r.NativePrice, r.Active, r.Ignored,
               d.DocDate, d.PartnerID, p.PartnerName
        FROM OfferRows r
        JOIN OfferDocs d ON d.DocID = r.DocID
        LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
        WHERE r.RowID = ?
      `, [link.sourceRowId]);
      if (pinned.length) {
        for (const r of pinned) {
          console.log(`  RowID=${r.RowID} Article="${r.NativeID}" Name="${r.NativeName}" Price=${r.NativePrice} Active=${r.Active} Partner="${r.PartnerName}"`);
        }
      } else {
        console.log(`  ✗ RowID ${link.sourceRowId} NOT FOUND in PriceMaster (staleness fallback will activate)`);
      }
    }
    // If link has an article, show what that article resolves to
    if (link.article && link.article !== OFFER_ID) {
      console.log(`\n=== PriceMaster rows for link.article="${link.article}" ===`);
      const [artRows] = await conn.query(`
        SELECT r.RowID, r.NativeID, r.NativeName, r.NativePrice, r.Active,
               d.DocDate, d.PartnerID, p.PartnerName
        FROM OfferRows r
        JOIN OfferDocs d ON d.DocID = r.DocID
        LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
        WHERE BINARY TRIM(r.NativeID) = ? AND r.Ignored = 0
        ORDER BY d.DocDate DESC LIMIT 10
      `, [link.article]);
      if (artRows.length) {
        for (const r of artRows) {
          console.log(`  RowID=${r.RowID} Article="${r.NativeID}" Name="${r.NativeName}" Price=${r.NativePrice} Active=${r.Active} Partner="${r.PartnerName}"`);
        }
      } else {
        console.log(`  (no PM row with NativeID="${link.article}")`);
      }
    }
  }

  await conn.end();
  await prisma.$disconnect();
  console.log("\n=== Done ===");
  console.log(`
HOW TO FIX:
  1. Open the warehouse product card for "${OFFER_ID}"
  2. Delete the wrong PM link (see link details above)
  3. Use the PM search to find "SUR MESURE NOIRE 100" at ~$26
  4. Select and save the correct row

If the article in PM for the $26 product is different from "${OFFER_ID}":
  → Set matchType=selected_row and pin to the exact RowID shown above
`);
}

main().catch((e) => { console.error("Error:", e.message); process.exit(1); });
