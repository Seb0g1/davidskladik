#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  const offerId = process.argv[2] || "SAAL051new";

  const prods = await prisma.$queryRawUnsafe(`
    SELECT wp.id, wp.marketplace,
           wp.raw->>'offerId' as offer_id,
           wp.raw->>'targetStock' as target_stock,
           wp.raw->>'priceStatus' as price_status,
           wp.raw->>'selectedSupplier' as selected_supplier
    FROM warehouse_products wp
    WHERE wp.raw->>'offerId' = $1
    ORDER BY wp.marketplace
  `, offerId);
  console.log("=== Products ===");
  console.log(JSON.stringify(prods, null, 2));

  if (!prods.length) { console.log("No product found"); return; }
  const productIds = prods.map((p) => String(p.id));

  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.id, pl.supplier_article, pl.partner_id, pl.supplier_name,
           pl.raw->>'matchType'   as match_type,
           pl.raw->>'sourceRowId' as source_row_id,
           pl.raw->>'article'     as article,
           pl.raw->>'exactName'   as exact_name
    FROM product_links pl
    WHERE pl.product_id = ANY($1)
  `, productIds);
  console.log("\n=== Links ===");
  console.log(JSON.stringify(links, null, 2));

  const pmRows = await prisma.$queryRawUnsafe(`
    SELECT row_id, article, partner_id, native_name,
           price::float as price, active, doc_date::text
    FROM pm_snapshot_items
    WHERE row_id IN ('2237266', '2290296')
    ORDER BY doc_date
  `);
  console.log("\n=== PM snapshot rows 2237266 & 2290296 ===");
  console.log(JSON.stringify(pmRows, null, 2));

  if (links.length > 0) {
    const article = links[0].article || links[0].supplier_article;
    const partnerId = links[0].partner_id;
    if (article && partnerId) {
      const byArticle = await prisma.$queryRawUnsafe(`
        SELECT row_id, article, partner_id, native_name,
               price::float as price, active, doc_date::text
        FROM pm_snapshot_items
        WHERE article = $1 AND partner_id::text = $2
        ORDER BY doc_date DESC
        LIMIT 10
      `, article, partnerId);
      console.log(`\n=== PM by article="${article}" partner="${partnerId}" ===`);
      console.log(JSON.stringify(byArticle, null, 2));
    }
  }
}

main().catch((e) => console.error("FAILED:", e.message, e.stack)).finally(() => prisma.$disconnect());
