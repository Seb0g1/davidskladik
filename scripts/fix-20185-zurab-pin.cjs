#!/usr/bin/env node
"use strict";
// Fixes art 20185: Зураб link matchType article→selected_row with sourceRowId=2211366
// so it enters the pinned pool and beats Родина (110 USD) on price.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

const ZURAB_ROW_ID = "2211366";
const PARTNER_ID_ZURAB = "1"; // partnerId=1 for Зураб (подвал)

async function main() {
  const prisma = getPrisma();

  // Get 20185 products
  const prods = await prisma.$queryRawUnsafe(`
    SELECT id FROM warehouse_products WHERE offer_id ILIKE '20185'
  `);
  const prodIds = prods.map((p) => String(p.id));
  console.log("Products to fix:", prodIds.join(", "));

  // 1. Update product_links for Зураб rows
  for (const prodId of prodIds) {
    const zurabLinks = await prisma.$queryRaw`
      SELECT id, raw->>'matchType' AS match_type, raw->>'sourceRowId' AS source_row_id
      FROM product_links
      WHERE product_id = ${prodId} AND partner_id = ${PARTNER_ID_ZURAB}
    `;
    for (const l of zurabLinks) {
      console.log(`  product_links ${l.id}: matchType=${l.match_type} sourceRowId=${l.source_row_id}`);
      if (l.match_type === "selected_row" && l.source_row_id === ZURAB_ROW_ID) {
        console.log("    → already selected_row with correct sourceRowId, skipping");
        continue;
      }
      await prisma.$queryRaw`
        UPDATE product_links
        SET raw = raw || ${`{"matchType":"selected_row","sourceRowId":"${ZURAB_ROW_ID}"}`}::jsonb,
            updated_at = NOW()
        WHERE id = ${l.id}
      `;
      console.log(`    → updated to selected_row, sourceRowId=${ZURAB_ROW_ID}`);
    }
  }

  // 2. Update warehouse_products.raw.links[] for Зураб entries
  for (const prodId of prodIds) {
    const wpRows = await prisma.$queryRaw`
      SELECT id, raw->'links' AS links_json FROM warehouse_products WHERE id = ${prodId}
    `;
    if (!wpRows.length) continue;
    const links = wpRows[0].links_json;
    if (!Array.isArray(links)) continue;

    let changed = false;
    const newLinks = links.map((l) => {
      const isZurab = String(l.partnerId || "") === PARTNER_ID_ZURAB
        || /зураб/i.test(l.supplierName || "");
      if (!isZurab) return l;
      if (l.matchType === "selected_row" && l.sourceRowId === ZURAB_ROW_ID) return l;
      changed = true;
      console.log(`  raw.links[${l.id}] (${l.supplierName}): matchType ${l.matchType}→selected_row, sourceRowId ${l.sourceRowId}→${ZURAB_ROW_ID}`);
      return { ...l, matchType: "selected_row", sourceRowId: ZURAB_ROW_ID };
    });

    if (changed) {
      const newLinksJson = JSON.stringify(newLinks);
      await prisma.$queryRaw`
        UPDATE warehouse_products
        SET raw = jsonb_set(raw, '{links}', ${newLinksJson}::jsonb),
            updated_at = NOW()
        WHERE id = ${prodId}
      `;
      console.log(`  → warehouse_products.raw.links updated for ${prodId}`);
    } else {
      console.log(`  → warehouse_products.raw.links unchanged for ${prodId}`);
    }
  }

  // Verify
  console.log("\n=== Verification ===");
  for (const prodId of prodIds) {
    const links = await prisma.$queryRaw`
      SELECT raw->>'matchType' AS match_type, raw->>'sourceRowId' AS source_row_id, supplier_name
      FROM product_links
      WHERE product_id = ${prodId} AND supplier_name ILIKE '%зураб%'
    `;
    for (const l of links) {
      const ok = l.match_type === "selected_row" && l.source_row_id === ZURAB_ROW_ID;
      console.log(`  [${prodId.slice(0, 25)}] ${l.supplier_name}: matchType=${l.match_type} sourceRowId=${l.source_row_id} ${ok ? "✓ OK" : "✗ WRONG"}`);
    }
  }

  await prisma.$disconnect();
  console.log("\nDone. Зураб is now selected_row → enters pinned pool → wins over Родина (110 USD) on price.");
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
