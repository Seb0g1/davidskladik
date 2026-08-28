#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const {
  getPrisma,
} = require("../lib/postgres.js");

const {
  normalizeWarehouseProduct,
  buildFreshWarehouseProducts,
  productFromPostgres,
} = require("../server.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const usdRate = Number(process.env.DEFAULT_USD_RATE || 88);
  console.log(`USD rate (env): ${usdRate}`);
  console.log(`USD rate: ${usdRate}`);

  // ── 1. SA.AL&CO 051 — найти все строки в снапшоте (включая active=false) ──
  console.log("\n─────────────────────────────────────────────");
  console.log("1. SA.AL&CO 051 — PM snapshot (including inactive)");
  const snRows = await prisma.$queryRawUnsafe(`
    SELECT id, row_id, article, partner_name, native_name, price, currency, active, doc_date
    FROM pm_snapshot_items
    WHERE native_name ILIKE '%SA.AL%051%' OR native_name ILIKE '%natural spray deodorant%'
       OR article ILIKE '%SA.AL%051%'
    ORDER BY active DESC, doc_date DESC
    LIMIT 20
  `);
  console.log(`Found ${snRows.length} rows:`);
  for (const r of snRows) {
    console.log(`  [${r.active ? "active" : "INACTIVE"}] rowId=${r.row_id} art=${r.article} partner=${r.partner_name} price=${r.price} ${r.currency} date=${String(r.doc_date).slice(0,10)} name=${String(r.native_name).slice(0,60)}`);
  }
  // Also check how Инна rows look in general for 0-price/inactive
  const innaInactive = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*)::int AS n FROM pm_snapshot_items
    WHERE partner_name ILIKE '%инн%' AND active = false
  `);
  const innaActive = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*)::int AS n FROM pm_snapshot_items
    WHERE partner_name ILIKE '%инн%' AND active = true
  `);
  console.log(`Инна total: active=${innaActive[0]?.n}, inactive=${innaInactive[0]?.n}`);

  // ── 2. Art 20185 (Nasomatto Baraonda 30) — supplier selection ─────────────
  console.log("\n─────────────────────────────────────────────");
  console.log("2. Art 20185 — supplier diagnosis");
  const p20185 = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: "20185", mode: "insensitive" } },
    include: { links: true },
  });
  for (const row of p20185) {
    const p = normalizeWarehouseProduct(productFromPostgres(row));
    console.log(`  marketplace=${p.marketplace} id=${p.id} links=${row.links.length}`);
    if (row.links.length) {
      for (const link of row.links) {
        const raw = link.raw && typeof link.raw === "object" ? link.raw : {};
        const rpm = raw.resolvedPriceMasterRow || {};
        console.log(`    link: art=${link.supplierArticle} supplier=${link.supplierName} partnerId=${link.partnerId} rowId=${raw.sourceRowId || rpm.rowId} price=${rpm.price || "?"} currency=${rpm.currency || "?"}`);
      }
    }
    const built = await buildFreshWarehouseProducts([p], { usdRate }).catch(() => [p]);
    const fresh = built[0] || p;
    const selected = fresh.selectedSupplier;
    console.log(`  selectedSupplier: ${JSON.stringify(selected?.partnerName || selected?.supplierName || "none")}, price=${selected?.purchasePrice || selected?.priceRub || "?"}`);
    console.log(`  nextPrice=${fresh.nextPrice}, markup=${fresh.markup}`);
  }

  // ── 3. LSMN100 — Далика, price on Ozon ────────────────────────────────────
  console.log("\n─────────────────────────────────────────────");
  console.log("3. LSMN100 — Далика supplier/price diagnosis");
  const lsmn = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: "LSMN100", mode: "insensitive" } },
    include: { links: true },
  });
  for (const row of lsmn) {
    const p = normalizeWarehouseProduct(productFromPostgres(row));
    console.log(`  marketplace=${p.marketplace} id=${p.id}`);
    for (const link of row.links) {
      const raw = link.raw && typeof link.raw === "object" ? link.raw : {};
      const rpm = raw.resolvedPriceMasterRow || {};
      console.log(`    link: art=${link.supplierArticle} supplier=${link.supplierName} partnerId=${link.partnerId} rowId=${raw.sourceRowId || rpm.rowId} price=${rpm.price} ${rpm.currency || "USD"}`);
    }
    const built = await buildFreshWarehouseProducts([p], { usdRate }).catch(() => [p]);
    const fresh = built[0] || p;
    const selected = fresh.selectedSupplier;
    console.log(`  selected: ${selected?.partnerName || "none"} price=${selected?.purchasePrice || "?"} markup=${fresh.markup} nextPrice=${fresh.nextPrice}`);
    // Show all alternatives
    if (fresh.supplierAlternatives?.length) {
      console.log("  alternatives:");
      for (const alt of fresh.supplierAlternatives) {
        console.log(`    ${alt.partnerName || alt.supplierName}: purchasePrice=${alt.purchasePrice} priceRub=${alt.priceRub}`);
      }
    }
  }

  // ── 4. 10825 — price on YM ────────────────────────────────────────────────
  console.log("\n─────────────────────────────────────────────");
  console.log("4. 10825 — price diagnosis");
  const p10825 = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: "10825", mode: "insensitive" } },
    include: { links: true },
  });
  for (const row of p10825) {
    const p = normalizeWarehouseProduct(productFromPostgres(row));
    console.log(`  marketplace=${p.marketplace} id=${p.id} currentPrice=${p.currentPrice} targetPrice=${p.targetPrice}`);
    for (const link of row.links) {
      const raw = link.raw && typeof link.raw === "object" ? link.raw : {};
      const rpm = raw.resolvedPriceMasterRow || {};
      console.log(`    link: art=${link.supplierArticle} supplier=${link.supplierName} price=${rpm.price} ${rpm.currency || "USD"} date=${String(rpm.docDate || "").slice(0,10)}`);
    }
    const built = await buildFreshWarehouseProducts([p], { usdRate }).catch(() => [p]);
    const fresh = built[0] || p;
    console.log(`  selected: ${fresh.selectedSupplier?.partnerName || "none"} purchasePrice=${fresh.selectedSupplier?.purchasePrice} markup=${fresh.markup} nextPrice=${fresh.nextPrice}`);
  }

  // ── 5. Cart: check if it orders already-shipped items ────────────────────
  console.log("\n─────────────────────────────────────────────");
  console.log("5. Cart duplicate check: Sended=1 rows that might be re-ordered");
  // Check last 10 docs to understand Sended status
  const mysql = require("mysql2/promise");
  const pool = mysql.createPool({
    host: process.env.PM_DB_HOST, port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER, password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME, waitForConnections: true, connectionLimit: 1,
  });
  try {
    const [recentDocs] = await pool.query(`
      SELECT rd.DocID, rd.PartnerID, rd.Sended, rd.Comment, rd.DocDate,
             COUNT(rr.RowID) AS rowCount,
             SUM(CASE WHEN rr.Sended = 1 THEN 1 ELSE 0 END) AS sentRows
      FROM RequestDocs rd
      LEFT JOIN RequestRows rr ON rr.DocID = rd.DocID
      WHERE rd.Comment LIKE 'ДавидСклад%'
      GROUP BY rd.DocID ORDER BY rd.DocDate DESC LIMIT 10
    `);
    console.log("Recent DavidSklad docs:");
    for (const d of recentDocs) {
      console.log(`  DocID=${d.DocID} PartnerID=${d.PartnerID} Sended=${d.Sended} rows=${d.rowCount} sentRows=${d.sentRows} date=${String(d.DocDate).slice(0,10)} comment=${String(d.Comment).slice(0,60)}`);
    }
    // Check if any offer rows in pending docs also appear in sent docs
    const [dupCheck] = await pool.query(`
      SELECT rr1.OfferRowID, COUNT(DISTINCT rd1.DocID) AS pendingDocs, COUNT(DISTINCT rd2.DocID) AS sentDocs
      FROM RequestRows rr1
      JOIN RequestDocs rd1 ON rd1.DocID = rr1.DocID AND rd1.Sended = 0 AND rd1.Comment LIKE 'ДавидСклад%'
      JOIN RequestRows rr2 ON rr2.OfferRowID = rr1.OfferRowID AND rr2.DocID != rr1.DocID
      JOIN RequestDocs rd2 ON rd2.DocID = rr2.DocID AND rd2.Sended = 1 AND rd2.Comment LIKE 'ДавидСклад%'
      GROUP BY rr1.OfferRowID HAVING pendingDocs > 0 AND sentDocs > 0
      LIMIT 10
    `);
    console.log(`Rows in pending AND sent docs: ${dupCheck.length}`);
    for (const d of dupCheck) console.log(`  OfferRowID=${d.OfferRowID} pending=${d.pendingDocs} sent=${d.sentDocs}`);
  } finally {
    await pool.end();
  }
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
