#!/usr/bin/env node
"use strict";
// Fixes 10825 Yandex price (currently 67₽, needs to be ~6400₽).
// Checks snapshot for linked supplier prices and sends correct price.

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  // Get 10825 YM product
  const yp = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, current_price, target_price
    FROM warehouse_products WHERE offer_id ILIKE '10825' AND marketplace = 'yandex'
  `);
  console.log("10825 YM products:", yp.length);
  for (const p of yp) console.log(`  id=${p.id} currentPrice=${p.current_price} targetPrice=${p.target_price}`);

  if (!yp.length) { console.log("Not found"); return; }

  // Get linked articles and their current PM snapshot prices
  const yId = String(yp[0].id).replace(/[^a-zA-Z0-9_-]/g, "");
  const links = await prisma.$queryRawUnsafe(`
    SELECT pl.supplier_article, pl.supplier_name, pl.partner_id,
           (pl.raw->>'sourceRowId') AS source_row_id,
           (pl.raw->'resolvedPriceMasterRow'->>'rowId') AS rpm_row_id
    FROM product_links pl
    WHERE pl.product_id = '${yId}'
  `);
  console.log(`\n10825 YM links: ${links.length}`);

  // Get all linked rowIds to look up PM snapshot
  const rowIds = [...new Set(links.map((l) => l.rpm_row_id || l.source_row_id).filter(Boolean))];
  console.log("Row IDs to check:", rowIds.join(", "));

  if (rowIds.length) {
    const inList = rowIds.map((r) => `'${String(r).replace(/[^0-9]/g, "")}'`).join(",");
    const snRows = await prisma.$queryRawUnsafe(`
      SELECT row_id, article, partner_id, partner_name, price, currency, active
      FROM pm_snapshot_items
      WHERE row_id IN (${inList})
      ORDER BY CASE WHEN active THEN 0 ELSE 1 END, price ASC
    `);
    console.log("\nPM snapshot rows:");
    for (const r of snRows) {
      console.log(`  rowId=${r.row_id} partner=${r.partner_name} price=${r.price} ${r.currency} active=${r.active}`);
    }

    // Find cheapest active row
    const activeRows = snRows.filter((r) => r.active);
    const cheapestRow = activeRows.sort((a, b) => Number(a.price) - Number(b.price))[0];

    if (cheapestRow) {
      const rate = Number(process.env.DEFAULT_USD_RATE || 95);
      const ymMarkup = 1.79;
      const pmPrice = Number(cheapestRow.price);
      const correctPrice = Math.round(pmPrice * rate * ymMarkup);
      console.log(`\nCheapest active: ${cheapestRow.partner_name} price=${pmPrice} USD`);
      console.log(`Correct YM price = ${pmPrice} × ${rate} × ${ymMarkup} = ${correctPrice}₽`);
      console.log(`Current YM price = ${yp[0].current_price}₽ (WRONG)`);

      // Update target_price in DB (does NOT send to YM automatically)
      console.log(`\nUpdating target_price from ${yp[0].target_price} to ${correctPrice}...`);
      await prisma.$queryRawUnsafe(`
        UPDATE warehouse_products SET target_price = ${correctPrice} WHERE id = '${yId}'
      `);
      console.log("✓ Updated target_price in DB");
      console.log("→ Now use the warehouse UI to send prices for 10825 YM, or run price sync.");
    } else {
      console.log("\nNo active PM rows found for 10825 YM links — product out of stock.");
      console.log("The 67₽ price won't be shown to buyers until stock is added.");
      console.log("Consider setting a fallback price manually via the warehouse UI.");
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
