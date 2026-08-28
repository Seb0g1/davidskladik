#!/usr/bin/env node
"use strict";
// Runs the actual fresh-products computation for 10825 YM and shows why price = 67.
// Reuses the same logic as the server's buildFreshWarehouseProductsForWarehouse.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
process.env.SERVER_ROLE = "api";

// Bootstrap enough of the app to use warehouse functions
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));

// Load the server source partially (just enough for snapshot + warehouse)
// We'll do it manually to avoid side effects.
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  console.log("=== 10825 YM price computation diagnosis ===\n");

  // 1. Get 10825 YM product and its links from PostgreSQL
  const products10825 = await prisma.$queryRawUnsafe(`
    SELECT id, offer_id, marketplace, current_price, target_price,
           raw->>'markup' AS markup,
           raw->>'autoPriceMin' AS auto_price_min,
           raw->>'autoPriceMax' AS auto_price_max
    FROM warehouse_products WHERE offer_id ILIKE '10825'
  `);
  console.log("Warehouse products for 10825:");
  for (const p of products10825) {
    console.log(`  id=${p.id} mp=${p.marketplace} currentPrice=${p.current_price} targetPrice=${p.target_price} markup=${p.markup}`);
  }

  const ym10825 = products10825.find((p) => p.marketplace === "yandex");
  if (!ym10825) { console.log("No YM 10825"); return; }
  const wpId = String(ym10825.id).replace(/[^a-zA-Z0-9_-]/g, "");

  // 2. Get all links for 10825 YM
  const links = await prisma.$queryRawUnsafe(`
    SELECT
      id, supplier_article, supplier_name, partner_id,
      pl.raw->>'sourceRowId' AS source_row_id,
      pl.raw->>'matchType' AS match_type,
      pl.raw->>'priceCurrency' AS price_currency,
      (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id,
      (pl.raw->'resolvedPriceMasterRow'->>'price') AS rpm_price,
      (pl.raw->'resolvedPriceMasterRow'->>'active') AS rpm_active,
      (pl.raw->'resolvedPriceMasterRow'->>'partnerName') AS rpm_partner
    FROM product_links pl
    WHERE pl.product_id = '${wpId}'
    ORDER BY (pl.raw->'resolvedPriceMasterRow'->>'price')::float ASC NULLS LAST
  `);
  console.log(`\n10825 YM links: ${links.length}`);
  for (const l of links) {
    console.log(`  id=${l.id} supplier=${l.supplier_name} art=${l.supplier_article} partnerId=${l.partner_id}`);
    console.log(`    matchType=${l.match_type} sourceRowId=${l.source_row_id} priceCurrency=${l.price_currency}`);
    console.log(`    rpm: partner=${l.rpm_partner} price=${l.rpm_price} active=${l.rpm_active} rowId=${l.rpm_row_id}`);
  }

  // 3. Get managed suppliers
  const manaSuppliers = await prisma.$queryRawUnsafe(`
    SELECT id, name, partner_id, default_currency, active, note, raw
    FROM managed_suppliers WHERE active = true ORDER BY name ASC
  `);
  console.log(`\nActive managed suppliers: ${manaSuppliers.length}`);

  // Find Дима америка
  const dimaAmerika = manaSuppliers.find((s) => /дима\s+америка/i.test(s.name || ""));
  if (dimaAmerika) {
    console.log(`  Дима америка: id=${dimaAmerika.id} partnerId=${dimaAmerika.partner_id} default_currency=${dimaAmerika.default_currency}`);
  } else {
    console.log("  Дима америка NOT FOUND in active suppliers!");
  }

  // 4. Check what's in pm_snapshot_items for the linked rowIds
  const rowIds = [...new Set(links.map((l) => l.rpm_row_id || l.source_row_id).filter(Boolean))];
  if (rowIds.length) {
    const inList = rowIds.map((r) => `'${String(r).replace(/[^0-9A-Za-z_-]/g, "")}'`).join(",");
    const snRows = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_id, partner_name, price, currency, active
      FROM pm_snapshot_items WHERE row_id IN (${inList})
      ORDER BY CASE WHEN active THEN 0 ELSE 1 END, price::float ASC
    `);
    console.log(`\nPM snapshot rows for 10825 YM links (${rowIds.join(",")}): ${snRows.length}`);
    for (const r of snRows) {
      const rub = Number(r.price) * 85 * 1.79;
      console.log(`  rowId=${r.row_id} partner=${r.partner_name}(id=${r.partner_id}) art="${r.article}" price=${r.price} ${r.currency} active=${r.active} → ~${Math.round(rub)}₽`);
    }

    // Simulate match logic for each link
    console.log("\n--- Match simulation for each link ---");
    for (const link of links) {
      const linkRowId = link.source_row_id;
      const linkArticle = link.supplier_article;
      const linkPartnerId = String(link.partner_id || "");
      const linkMatchType = link.match_type;

      // Find candidates in snapshot
      let candidates = [];
      if (linkMatchType === "selected_row" && linkArticle) {
        // byArticle first, then byRowId
        const byArt = snRows.filter((r) => r.article === linkArticle);
        if (byArt.length) {
          candidates = byArt;
          console.log(`  Link ${link.supplier_name} (art=${linkArticle}): byArticle found ${byArt.length} candidates`);
        } else {
          const byRowId = snRows.filter((r) => r.row_id === linkRowId);
          candidates = byRowId;
          console.log(`  Link ${link.supplier_name} (art=${linkArticle}): byArticle empty, byRowId(${linkRowId}) found ${byRowId.length} candidates`);
        }
      } else if (linkMatchType === "selected_row" && linkRowId) {
        candidates = snRows.filter((r) => r.row_id === linkRowId);
        console.log(`  Link ${link.supplier_name}: byRowId(${linkRowId}) found ${candidates.length} candidates`);
      }

      // Apply priceMasterRowMatchesLink filter
      const matched = candidates.filter((r) => {
        // partnerOk
        const partnerOk = !linkPartnerId || String(r.partner_id || "") === linkPartnerId;
        if (!partnerOk) { console.log(`    row ${r.row_id} FILTERED: partnerId ${r.partner_id} != ${linkPartnerId}`); return false; }
        // sourceRowId check (selected_row)
        if (linkMatchType === "selected_row") {
          if (linkRowId && String(r.row_id || "") === String(linkRowId)) return true;
          // article fallback
          if (linkArticle && r.article === linkArticle) return true;
          console.log(`    row ${r.row_id} FILTERED: neither sourceRowId nor article match`);
          return false;
        }
        return true;
      });

      for (const r of matched) {
        const active = r.active;
        const priceOk = Number(r.price) > 0;
        const available = active && priceOk;
        const rub = available ? Math.round(Number(r.price) * 85 * 1.79) : 0;
        console.log(`    MATCHED: rowId=${r.row_id} partner=${r.partner_name} price=${r.price} active=${active} available=${available} → ${available ? rub + "₽" : "N/A"}`);
      }
      if (matched.length === 0) {
        console.log(`    NO MATCH for link ${link.supplier_name}`);
      }
    }
  }

  // 5. Check AppSettings for fixedUsdRate
  const appSettingsRow = await prisma.appSetting.findUnique({ where: { key: "app" } });
  const appSettings = appSettingsRow?.value || {};
  console.log(`\nAppSettings: fixedUsdRate=${appSettings.fixedUsdRate} yandexMarkup=${appSettings.defaultMarkups?.yandex}`);

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
