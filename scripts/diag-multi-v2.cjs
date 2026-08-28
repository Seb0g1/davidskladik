#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const { getPrisma } = require("../lib/postgres.js");
const mysql = require("mysql2/promise");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const pool = mysql.createPool({
    host: process.env.PM_DB_HOST, port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER, password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME, waitForConnections: true, connectionLimit: 2,
  });

  try {
    // ── 1. SA.AL&CO 051: what does PM search actually return? ────────────────
    console.log("═══════════════════════════════════════════════════════");
    console.log("1. PM search for 'SA.AL&CO 051' (active=true only)");
    const srch1 = await prisma.$queryRawUnsafe(`
      SELECT id, row_id, article, partner_name, native_name, price, currency, active
      FROM pm_snapshot_items
      WHERE active = true
        AND (
          native_name ILIKE '%SA.AL%051%'
          OR native_name ILIKE '%natural spray deodorant%'
          OR article ILIKE '%SA.AL%051%'
          OR article ILIKE '%SAAL051%'
        )
      ORDER BY price ASC LIMIT 10
    `);
    console.log(`Search results (active only): ${srch1.length} rows`);
    for (const r of srch1) {
      console.log(`  rowId=${r.row_id} art=${r.article} partner=${r.partner_name} price=${r.price} ${r.currency} active=${r.active} name=${String(r.native_name).slice(0,60)}`);
    }

    // Check what Инна items are linked to warehouse products
    const innaLinked = await prisma.$queryRawUnsafe(`
      SELECT pl.supplier_article, pl.supplier_name, pl.partner_id,
             wp.offer_id, wp.marketplace, wp.current_price
      FROM "product_links" pl
      JOIN "warehouse_products" wp ON wp.id = pl.product_id
      WHERE pl.supplier_name ILIKE '%инн%'
      LIMIT 20
    `);
    console.log(`\nИнна links in warehouse: ${innaLinked.length}`);
    for (const r of innaLinked) console.log(`  art=${r.supplier_article} offerId=${r.offer_id} mp=${r.marketplace} price=${r.current_price}`);

    // Show Инна partner IDs in PM snapshot
    const innaPids = await prisma.$queryRawUnsafe(`
      SELECT DISTINCT partner_id, partner_name, COUNT(*) AS n
      FROM pm_snapshot_items WHERE partner_name ILIKE '%инн%'
      GROUP BY partner_id, partner_name
    `);
    console.log("\nИнна partnerIds in PM snapshot:");
    for (const r of innaPids) console.log(`  partnerId=${r.partner_id} name=${r.partner_name} count=${r.n}`);

    // ── 2. Art 20185 — direct PM snapshot lookup for linked articles ──────────
    console.log("\n═══════════════════════════════════════════════════════");
    console.log("2. Art 20185 — linked PM snapshot rows (direct SQL)");
    const links20185 = await prisma.$queryRawUnsafe(`
      SELECT pl.supplier_article, pl.supplier_name, pl.partner_id,
             pl.raw->>'sourceRowId' AS source_row_id,
             (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id
      FROM "product_links" pl
      JOIN "warehouse_products" wp ON wp.id = pl.product_id
      WHERE wp.offer_id ILIKE '20185'
      ORDER BY pl.supplier_name
    `);
    console.log(`Links for 20185: ${links20185.length}`);

    // Collect row IDs to look up
    const rowIds20185 = [];
    for (const l of links20185) {
      const rid = l.rpm_row_id || l.source_row_id;
      if (rid) rowIds20185.push(String(rid));
      console.log(`  supplier=${l.supplier_name} art=${l.supplier_article} partnerId=${l.partner_id} rpmRowId=${l.rpm_row_id} srcRowId=${l.source_row_id}`);
    }

    if (rowIds20185.length) {
      const snRows20185 = await prisma.$queryRawUnsafe(`
        SELECT row_id, article, partner_id, partner_name, price, currency, active
        FROM pm_snapshot_items
        WHERE row_id = ANY($1::text[])
        ORDER BY CASE WHEN active THEN 0 ELSE 1 END, price ASC
      `, rowIds20185);
      console.log("\nPM snapshot rows for 20185 links:");
      for (const r of snRows20185) {
        console.log(`  rowId=${r.row_id} partner=${r.partner_name} price=${r.price} ${r.currency} active=${r.active}`);
      }
    }

    // Also look up by article
    const [pm20185] = await pool.query(`
      SELECT o.RowID, o.NativeName, o.NativePrice, o.NativeID, o.Active, r.PartnerID, r.DocDate
      FROM OfferRows o
      JOIN OfferDocs r ON r.DocID = o.DocID
      WHERE o.RowID = 2211366
      LIMIT 5
    `);
    console.log("\nMySQL PM row for art 20185 (Зураб rowId=2211366):");
    for (const r of pm20185) console.log(`  RowID=${r.RowID} name=${String(r.NativeName || "").slice(0,40)} price=${r.NativePrice} art=${r.NativeID} active=${r.Active} partner=${r.PartnerID}`);

    // ── 3. LSMN100 — direct PM snapshot lookup ────────────────────────────────
    console.log("\n═══════════════════════════════════════════════════════");
    console.log("3. LSMN100 (Далика) — PM snapshot + price history");
    const linksLSMN = await prisma.$queryRawUnsafe(`
      SELECT pl.supplier_article, pl.supplier_name, pl.partner_id,
             pl.raw->>'sourceRowId' AS source_row_id,
             (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id,
             (pl.raw->'resolvedPriceMasterRow'->>'price') AS rpm_price,
             (pl.raw->'resolvedPriceMasterRow'->>'currency') AS rpm_currency,
             wp.marketplace, wp.offer_id, wp.current_price, wp.target_price
      FROM "product_links" pl
      JOIN "warehouse_products" wp ON wp.id = pl.product_id
      WHERE wp.offer_id ILIKE 'LSMN100'
    `);
    console.log(`LSMN100 links: ${linksLSMN.length}`);
    for (const l of linksLSMN) {
      console.log(`  mp=${l.marketplace} supplier=${l.supplier_name} art=${l.supplier_article} rpm_price=${l.rpm_price} ${l.rpm_currency || "USD"} rpmRowId=${l.rpm_row_id} srcRowId=${l.source_row_id} currentPrice=${l.current_price} targetPrice=${l.target_price}`);
    }

    // Look up Далика art 163 in PM MySQL directly
    const [pmDalika] = await pool.query(`
      SELECT o.RowID, o.NativeID, o.NativeName, o.NativePrice, o.Active, r.PartnerID, r.DocDate
      FROM OfferRows o
      JOIN OfferDocs r ON r.DocID = o.DocID
      WHERE o.RowID = 2331708 OR (o.NativeID = '163' AND r.PartnerID = 123)
      LIMIT 5
    `);
    console.log("\nMySQL Далика art 163:");
    for (const r of pmDalika) console.log(`  RowID=${r.RowID} art=${r.NativeID} name=${String(r.NativeName||"").slice(0,40)} price=${r.NativePrice} active=${r.Active} partner=${r.PartnerID}`);

    // ── 4. 10825 — price history to understand 67₽ ───────────────────────────
    console.log("\n═══════════════════════════════════════════════════════");
    console.log("4. 10825 — price history on Yandex (last 5 sends)");
    const wp10825 = await prisma.$queryRawUnsafe(`
      SELECT id, marketplace, current_price, target_price, raw->>'markup' AS markup
      FROM warehouse_products WHERE offer_id ILIKE '10825'
    `);
    for (const w of wp10825) {
      console.log(`  wp id=${w.id} mp=${w.marketplace} currentPrice=${w.current_price} targetPrice=${w.target_price} markup=${w.markup}`);
    }
    const ids10825 = wp10825.map((w) => w.id);
    if (ids10825.length) {
      const hist10825 = await prisma.$queryRawUnsafe(`
        SELECT ph.product_id, ph.marketplace, ph.new_price AS price, ph.status,
               ph.created_at,
               ph.response->>'pmPriceUsd' AS pm_usd,
               ph.response->>'usdRate' AS rate,
               ph.response->>'markup' AS markup,
               ph.response->>'selectedSupplier' AS supplier
        FROM "price_history" ph
        WHERE ph.product_id = ANY($1::text[])
        ORDER BY ph.created_at DESC LIMIT 10
      `, ids10825);
      console.log("\nPrice history for 10825:");
      for (const h of hist10825) {
        console.log(`  mp=${h.marketplace} price=${h.price} status=${h.status} pmUsd=${h.pm_usd} rate=${h.rate} markup=${h.markup} supplier=${h.supplier} at=${String(h.created_at).slice(0,16)}`);
      }
    }

    // Check the actual targetPrice stored for yandex product
    const ym10825 = wp10825.find((w) => w.marketplace === "yandex" || w.marketplace === "YANDEX");
    if (ym10825) {
      const pmRows10825 = await prisma.$queryRawUnsafe(`
        SELECT pl.supplier_article, pl.supplier_name, pl.partner_id,
               (pl.raw->'resolvedPriceMasterRow'->>'price') AS rpm_price,
               (pl.raw->'resolvedPriceMasterRow'->>'priceRub') AS rpm_price_rub,
               (pl.raw->'resolvedPriceMasterRow'->>'currency') AS rpm_currency,
               (pl.raw->'resolvedPriceMasterRow'->>'active') AS rpm_active
        FROM "product_links" pl
        WHERE pl.product_id = $1
        ORDER BY (pl.raw->'resolvedPriceMasterRow'->>'price')::float ASC NULLS LAST
      `, [ym10825.id]);
      console.log("\nYandex 10825 links with PM data:");
      for (const l of pmRows10825) {
        console.log(`  supplier=${l.supplier_name} art=${l.supplier_article} rpmPrice=${l.rpm_price} ${l.rpm_currency || "USD"} priceRub=${l.rpm_price_rub} active=${l.rpm_active}`);
      }
    }

    // ── 5. Cart — check RequestRows schema then find duplicates ──────────────
    console.log("\n═══════════════════════════════════════════════════════");
    console.log("5. Cart — check RequestRows columns");
    const [cols] = await pool.query(`DESCRIBE RequestRows`);
    console.log("RequestRows columns:", cols.map((c) => c.Field).join(", "));

    const [colsDocs] = await pool.query(`DESCRIBE RequestDocs`);
    console.log("RequestDocs columns:", colsDocs.map((c) => c.Field).join(", "));

    // Recent DavidSklad docs
    const [recentDocs] = await pool.query(`
      SELECT rd.DocID, rd.PartnerID, rd.Sended, rd.Comment, rd.DocDate,
             COUNT(rr.RowID) AS rowCount
      FROM RequestDocs rd
      LEFT JOIN RequestRows rr ON rr.DocID = rd.DocID
      WHERE rd.Comment LIKE 'ДавидСклад%'
      GROUP BY rd.DocID ORDER BY rd.DocDate DESC LIMIT 10
    `);
    console.log("\nRecent DavidSklad docs:");
    for (const d of recentDocs) {
      console.log(`  DocID=${d.DocID} PartnerID=${d.PartnerID} Sended=${d.Sended} rows=${d.rowCount} date=${String(d.DocDate).slice(0,10)}`);
    }

    // Look for items that appear in both pending (Sended=0) and sent (Sended=1) docs
    const [dupCheck] = await pool.query(`
      SELECT rr1.OfferRowID, COUNT(DISTINCT rd1.DocID) AS pendingDocs, COUNT(DISTINCT rd2.DocID) AS sentDocs
      FROM RequestRows rr1
      JOIN RequestDocs rd1 ON rd1.DocID = rr1.DocID AND rd1.Sended = 0 AND rd1.Comment LIKE 'ДавидСклад%'
      JOIN RequestRows rr2 ON rr2.OfferRowID = rr1.OfferRowID
      JOIN RequestDocs rd2 ON rd2.DocID = rr2.DocID AND rd2.Sended = 1 AND rd2.Comment LIKE 'ДавидСклад%'
      WHERE rr1.OfferRowID != 0
      GROUP BY rr1.OfferRowID HAVING pendingDocs > 0
      LIMIT 10
    `);
    console.log(`\nOfferRows in pending docs that also exist in sent docs: ${dupCheck.length}`);
    for (const d of dupCheck) console.log(`  OfferRowID=${d.OfferRowID} pendingDocs=${d.pendingDocs} sentDocs=${d.sentDocs}`);

    // Check if the cart's "ready to ship" orders are being re-included
    // "Ready to ship" in PriceMaster = Sended=1 on the doc itself
    const [readyOrders] = await pool.query(`
      SELECT DocID, PartnerID, Sended, Comment, DocDate,
             (SELECT COUNT(*) FROM RequestRows rr WHERE rr.DocID = rd.DocID) AS rowCount
      FROM RequestDocs rd
      WHERE rd.Comment LIKE 'ДавидСклад%' AND rd.Sended = 1
      ORDER BY rd.DocDate DESC LIMIT 5
    `);
    console.log("\nAlready-sent (Sended=1) DavidSklad docs (should not appear in cart):");
    for (const d of readyOrders) console.log(`  DocID=${d.DocID} PartnerID=${d.PartnerID} Sended=${d.Sended} rows=${d.rowCount} date=${String(d.DocDate).slice(0,10)}`);

    // What does the cart API query? Let's look at what "pending" means in the server
    // The cart query fetches Sended=0 docs. If the user says "ready to ship" items appear,
    // it means there are Sended=0 docs that contain items that were already ordered+shipped.
    // The OfferRows.Active field (if exists) or quantity might be the indicator.
    const colNames = cols.map((c) => c.Field);
    console.log("\nRequestRows key columns:", colNames.filter(c => ['OfferRowID','DocID','Quantity','Price','Active','Status'].some(k => c.includes(k))).join(", "));

  } finally {
    await pool.end();
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
