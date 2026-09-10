"use strict";
// Diagnostic: show what PM rows Dalik article 4222 and 179 resolve to,
// and what price each LTDC100 variant would compute.
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = "true";

const mysql2 = require("mysql2/promise");

async function main() {
  const pool = mysql2.createPool({
    host: process.env.PM_DB_HOST || "localhost",
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME,
    waitForConnections: true,
    connectionLimit: 2,
  });

  const articles = ["4222", "179"];
  for (const art of articles) {
    console.log(`\n=== Dalik article ${art} ===`);
    const [rows] = await pool.query(`
      SELECT r.RowID, r.NativeID, r.NativeName, r.Price, r.Active, r.Ignored,
             d.PartnerID, p.PartnerName
      FROM OfferRows r
      JOIN OfferDocs d ON d.DocID = r.DocID
      JOIN Partners p ON p.PartnerID = d.PartnerID
      WHERE BINARY TRIM(r.NativeID) = ?
        AND (p.PartnerName LIKE '%алик%' OR p.PartnerName LIKE '%alik%')
        AND r.Ignored = 0
      ORDER BY r.Active DESC, r.RowID DESC
      LIMIT 20
    `, [art]);

    if (!rows.length) {
      console.log("  No rows found");
    } else {
      for (const r of rows) {
        console.log(`  RowID=${r.RowID} active=${r.Active} price=${r.Price} name="${r.NativeName}" partner="${r.PartnerName}"`);
      }
    }
  }

  // Also show what LTDC100 links look like in DB
  const { getPrisma } = require("../lib/postgres.js");
  const prisma = getPrisma();
  const links = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.name, wp.target_price, wp.current_price,
           pl.supplier_article, pl.source_row_id, pl.exact_name,
           pl.raw->>'matchType' as match_type,
           pl.raw->>'resolvedBy' as resolved_by
    FROM warehouse_products wp
    LEFT JOIN product_links pl ON pl.product_id = wp.id
    WHERE wp.offer_id = 'LTDC100'
      AND (LOWER(pl.supplier_name) LIKE '%далик%' OR LOWER(pl.supplier_name) LIKE '%dalik%')
    ORDER BY pl.supplier_article
  `);

  console.log("\n=== LTDC100 Dalik links in DB ===");
  for (const r of links) {
    console.log(`  article=${r.supplier_article} matchType=${r.match_type} sourceRowId=${r.source_row_id}`);
    console.log(`  exactName=${r.exact_name}`);
    console.log(`  target_price=${r.target_price} current_price=${r.current_price}`);
  }

  await pool.end();
  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
