#!/usr/bin/env node
"use strict";
// Diagnoses:
// 1. Дима америка priceCurrency in AppSettings (should be USD, not RUB)
// 2. pm_snapshot_items article for rowId=2038752 (10825 / Дима америка)
// 3. 20185 Зураб availability — why Родина is picked over cheaper Зураб

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  try {
    // ── 1. managed_suppliers table — find Дима америка ───────────────────────
    console.log("=== managed_suppliers (default_currency) ===\n");
    const suppliers = await prisma.$queryRawUnsafe(`
      SELECT id, name, partner_id, default_currency, stop_reason, active, note, raw
      FROM managed_suppliers ORDER BY name ASC
    `);
    console.log(`Total managed suppliers: ${suppliers.length}`);
    for (const s of suppliers) {
      const dc = s.default_currency || "(none)";
      const active = s.active ? "" : " [inactive]";
      if (/дима|dima|америка|america/i.test(s.name || "") || dc === "RUB") {
        console.log(`  *** ${s.name} — default_currency=${dc} partnerId=${s.partner_id || "(none)"}${active}`);
      } else {
        console.log(`  ${s.name} — default_currency=${dc} partnerId=${s.partner_id || "(none)"}${active}`);
      }
    }

    // ── 2. PM snapshot: article column for rowId=2038752 (Дима америка 10825) ─
    console.log("\n=== PM snapshot item rowId=2038752 (Дима америка / 10825) ===\n");
    const dmRow = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_id, partner_name, price, currency, active, native_name
      FROM pm_snapshot_items WHERE row_id = '2038752'
    `);
    if (dmRow.length) {
      const r = dmRow[0];
      console.log(`  rowId=${r.row_id} partner=${r.partner_name} partnerId=${r.partner_id}`);
      console.log(`  article="${r.article}" price=${r.price} currency=${r.currency} active=${r.active}`);
      console.log(`  name=${String(r.native_name||"").slice(0,60)}`);
    } else {
      console.log("  NOT FOUND in pm_snapshot_items");
    }

    // ── 3. byArticle index simulation for "2038752" in snapshot ──────────────
    console.log('\n=== pm_snapshot_items WHERE article = "2038752" ===\n');
    const byArt2038752 = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_name, price, currency, active
      FROM pm_snapshot_items WHERE article = '2038752'
    `);
    console.log(`  Rows with article='2038752': ${byArt2038752.length}`);
    for (const r of byArt2038752) {
      console.log(`    rowId=${r.row_id} partner=${r.partner_name} price=${r.price} ${r.currency} active=${r.active}`);
    }

    // ── 4. 20185 — all linked rowIds in snapshot ──────────────────────────────
    console.log("\n=== Art 20185 — PM snapshot rows for linked rowIds ===\n");
    const links20185 = await prisma.$queryRawUnsafe(`
      SELECT
        pl.id, pl.supplier_article, pl.supplier_name, pl.partner_id,
        pl.raw->>'sourceRowId' AS source_row_id,
        (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id,
        (pl.raw->'resolvedPriceMasterRow'->>'price') AS rpm_price,
        (pl.raw->'resolvedPriceMasterRow'->>'active') AS rpm_active,
        (pl.raw->'resolvedPriceMasterRow'->>'partnerName') AS rpm_partner
      FROM product_links pl
      JOIN warehouse_products wp ON wp.id = pl.product_id
      WHERE wp.offer_id ILIKE '20185'
      ORDER BY (pl.raw->'resolvedPriceMasterRow'->>'price')::float ASC NULLS LAST
    `);
    console.log(`  Links for 20185: ${links20185.length}`);
    for (const l of links20185) {
      console.log(`  supplier=${l.supplier_name} art=${l.supplier_article} partnerId=${l.partner_id}`);
      console.log(`    sourceRowId=${l.source_row_id} rpmRowId=${l.rpm_row_id} rpmPrice=${l.rpm_price} rpmActive=${l.rpm_active}`);
    }

    const rowIds20185 = [...new Set(links20185.map((l) => l.rpm_row_id || l.source_row_id).filter(Boolean))];
    if (rowIds20185.length) {
      const inList = rowIds20185.map((r) => `'${String(r).replace(/[^0-9]/g, "")}'`).join(",");
      const snRows = await prisma.$queryRawUnsafe(`
        SELECT row_id, article, partner_id, partner_name, price, currency, active
        FROM pm_snapshot_items WHERE row_id IN (${inList})
        ORDER BY CASE WHEN active THEN 0 ELSE 1 END, price::float ASC
      `);
      console.log(`\n  PM snapshot for 20185 linked rowIds (${rowIds20185.join(",")}): ${snRows.length}`);
      for (const r of snRows) {
        const rub = Number(r.price) * 85 * 1.74;
        console.log(`    rowId=${r.row_id} partner=${r.partner_name}(id=${r.partner_id}) art=${r.article} price=${r.price} ${r.currency} active=${r.active} → ~${Math.round(rub)}₽`);
      }
    }

    // Also check by article "20185" in snapshot
    console.log('\n=== pm_snapshot_items WHERE article = "20185" ===\n');
    const byArt20185 = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_id, partner_name, price, currency, active
      FROM pm_snapshot_items WHERE article = '20185'
      ORDER BY CASE WHEN active THEN 0 ELSE 1 END, price::float ASC
    `);
    console.log(`  Rows with article='20185': ${byArt20185.length}`);
    for (const r of byArt20185) {
      const rub = Number(r.price) * 85 * 1.74;
      console.log(`    rowId=${r.row_id} partner=${r.partner_name}(id=${r.partner_id}) price=${r.price} ${r.currency} active=${r.active} → ~${Math.round(rub)}₽`);
    }

    // ── 5. Check if Зураб is in managed_suppliers ────────────────────────────
    console.log("\n=== Зураб in managed_suppliers ===\n");
    for (const s of suppliers) {
      if (/зураб|zurab/i.test(s.name || "")) {
        const raw = s.raw && typeof s.raw === "object" ? s.raw : {};
        console.log(`  *** Зураб: default_currency=${s.default_currency} active=${s.active} note=${s.note||"(none)"} pricingMode=${raw.pricingMode||"(none)"} trustFactor=${raw.trustFactor||"(none)"} stopped=${!s.active}`);
      }
    }

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
