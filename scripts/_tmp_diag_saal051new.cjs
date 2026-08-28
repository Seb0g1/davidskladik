"use strict";
require("dotenv").config({ path: require("node:path").join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
(async () => {
  const links = await p.$queryRawUnsafe(`
    SELECT pl.id, pl.partner_id, pl.supplier_article,
           pl.raw->>'sourceRowId' AS source_row_id,
           pl.raw->>'article' AS article,
           pl.raw->>'matchType' AS match_type,
           wp.raw->>'offerId' AS offer_id, wp.id AS product_id
    FROM product_links pl
    JOIN warehouse_products wp ON wp.id = pl.product_id
    WHERE wp.raw->>'offerId' ILIKE '%SAAL051%'
    LIMIT 20
  `);
  console.log("LINKS:", JSON.stringify(links, null, 2));

  const rows = await p.$queryRawUnsafe(`
    SELECT row_id, article, partner_id, price::float, active, native_name
    FROM pm_snapshot_items
    WHERE row_id IN ('2237266','2290296')
  `);
  console.log("PM ROWS:", JSON.stringify(rows, null, 2));

  await p.$disconnect();
})().catch((e) => { console.error(e.message); process.exit(1); });
