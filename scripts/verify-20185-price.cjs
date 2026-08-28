#!/usr/bin/env node
"use strict";
// Verifies product 20185 supplier selection by simulating pickWarehouseSupplier
// using live pm_snapshot_items (not stale resolvedPriceMasterRow).

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

function normalizeSupplierName(n) {
  return (n || "").trim().toLowerCase().replace(/\s+/g, " ");
}

async function simProduct(prisma, app, p) {
  const rate = Number(app.fixedUsdRate || 85);
  const rules = app.markupRules || [];

  // Load links from product_links
  const links = await prisma.$queryRaw`
    SELECT pl.supplier_name, pl.partner_id,
           pl.raw->>'matchType' AS match_type,
           pl.raw->>'sourceRowId' AS source_row_id,
           pl.raw->>'article' AS raw_article
    FROM product_links pl
    WHERE pl.product_id = ${String(p.id)}
  `;

  // Load all relevant pm_snapshot_items for these links
  const allRowIds = [...new Set(links.map((l) => l.source_row_id).filter(Boolean))];
  const allPartnerIds = [...new Set(links.map((l) => l.partner_id).filter(Boolean))];

  let snapshotRows = [];
  if (allRowIds.length || allPartnerIds.length) {
    snapshotRows = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_id::text AS partner_id, partner_name, price, currency, active
      FROM pm_snapshot_items
      WHERE row_id = ANY(ARRAY[${allRowIds.map((r) => `'${r}'`).join(",") || "''"}]::text[])
         OR partner_id::text = ANY(ARRAY[${allPartnerIds.map((r) => `'${r}'`).join(",") || "''"}]::text[])
    `);
  }

  // Build byRowId and byPartnerId indexes
  const byRowId = new Map();
  const byPartnerId = new Map();
  for (const row of snapshotRows) {
    byRowId.set(String(row.row_id), row);
    const pid = String(row.partner_id);
    if (!byPartnerId.has(pid)) byPartnerId.set(pid, []);
    byPartnerId.get(pid).push(row);
  }

  // Simulate getPriceMasterMatchesForLinks + pinnedRow
  const rawSuppliers = [];
  for (const link of links) {
    const pid = String(link.partner_id || "");
    let match = null;

    if (link.match_type === "selected_row" && link.source_row_id) {
      match = byRowId.get(String(link.source_row_id)) || null;
    } else if (link.match_type === "article" && link.partner_id) {
      // find row from partnerId with matching article
      const rows = byPartnerId.get(pid) || [];
      match = rows.find((r) => r.article === link.raw_article) || rows[0] || null;
    } else if (link.partner_id) {
      const rows = byPartnerId.get(pid) || [];
      match = rows[0] || null;
    }

    if (!match) continue;

    const price = Number(match.price || 0);
    const active = match.active !== false && match.active !== "false";
    const available = active && price > 0;
    const pinnedRow = link.match_type === "selected_row"
      && link.source_row_id
      && String(match.row_id) === String(link.source_row_id);

    // resolveMarkupCoefficient
    const scopedRules = rules.filter((r) => !r.marketplace || r.marketplace === "all" || r.marketplace === p.marketplace);
    const sorted = [...scopedRules].sort((a, b) => b.minUsd - a.minUsd);
    const matchedRule = sorted.find((r) => price >= Number(r.minUsd || 0));
    const markup = matchedRule?.coefficient || app.defaultMarkups?.[p.marketplace] || 1.6;
    const rubPrice = available ? Math.round(price * rate * markup) : 0;

    rawSuppliers.push({ link, match, price, active, available, pinnedRow, markup, rubPrice });
  }

  // pickWarehouseSupplier logic
  const eligible = rawSuppliers.filter((s) => s.available).sort((a, b) => a.price - b.price);
  const pinned = eligible.filter((s) => s.pinnedRow);
  const pool = pinned.length ? pinned : eligible;
  const selected = pool[0] || null;

  console.log(`=== ${p.marketplace.toUpperCase()} (currentPrice=${p.current_price}₽) ===`);
  for (const s of rawSuppliers) {
    const tag = [];
    if (!s.available) tag.push("unavailable");
    if (s.pinnedRow) tag.push("PINNED");
    if (selected && s.link.supplier_name === selected.link.supplier_name) tag.push("→ SELECTED");
    console.log(`  ${s.link.supplier_name}: ${s.price} USD active=${s.active} pinned=${s.pinnedRow} → ${s.rubPrice}₽ [${tag.join(", ") || "eligible"}]`);
  }
  if (selected) {
    console.log(`  ✓ Winner: ${selected.link.supplier_name} ${selected.price} USD → ${selected.rubPrice}₽`);
  } else {
    console.log(`  ✗ No eligible supplier found`);
  }
  console.log();
}

async function main() {
  const prisma = getPrisma();
  const appRow = await prisma.appSetting.findUnique({ where: { key: "app" } });
  const app = appRow?.value || {};

  const prods = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, current_price, target_price FROM warehouse_products
    WHERE offer_id ILIKE '20185' ORDER BY marketplace
  `);
  console.log(`rate=${app.fixedUsdRate}\n`);
  for (const p of prods) await simProduct(prisma, app, p);

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
