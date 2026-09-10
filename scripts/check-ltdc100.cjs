"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = "true";
const { getPrisma } = require("../lib/postgres.js");
const prisma = getPrisma();

(async () => {
  const rows = await prisma.$queryRawUnsafe(`
    SELECT wp.offer_id, wp.name, wp.target_price,
           pl.id as link_id, pl.supplier_article, pl.source_row_id, pl.exact_name,
           pl.raw->>'matchType' as match_type,
           pl.raw->>'resolvedBy' as resolved_by
    FROM warehouse_products wp
    LEFT JOIN product_links pl ON pl.product_id = wp.id
    WHERE wp.offer_id = 'LTDC100'
  `);
  console.log(`LTDC100 links: ${rows.length}`);
  for (const r of rows) {
    console.log(`  matchType=${r.match_type} resolvedBy=${r.resolved_by}`);
    console.log(`  article=${r.supplier_article} sourceRowId=${r.source_row_id} exactName=${r.exact_name}`);
    console.log(`  target_price=${r.target_price} ozon=${r.target_price_ozon} yandex=${r.target_price_yandex}`);
  }
  await prisma.$disconnect();
})().catch((e) => { console.error(e.message); process.exit(1); });
