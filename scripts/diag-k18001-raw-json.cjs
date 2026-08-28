#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  const rows = await prisma.$queryRawUnsafe(`
    SELECT id, marketplace, offer_id, target,
           target_price, current_price,
           raw
    FROM warehouse_products
    WHERE offer_id = 'K18001'
    ORDER BY marketplace
  `);

  for (const row of rows) {
    const raw = typeof row.raw === "object" ? row.raw : (row.raw ? JSON.parse(row.raw) : {});
    console.log(`\n[${row.marketplace}] id=${row.id}`);
    console.log(`  target_price=${row.target_price} current_price=${row.current_price}`);
    console.log(`  raw keys: ${Object.keys(raw).join(", ")}`);

    // Show price-related raw fields
    const priceFields = ["targetPrice", "nextPrice", "currentPrice", "marketplacePrice", "selectedSupplier", "suppliers", "ready", "priceFormula", "managedSupplierPriceCurrency", "priceSource"];
    for (const f of priceFields) {
      if (raw[f] !== undefined) {
        const val = raw[f];
        const str = typeof val === "object" ? JSON.stringify(val).substring(0, 200) : String(val);
        console.log(`  raw.${f} = ${str}`);
      }
    }

    // Show product_links with full raw
    const links = await prisma.$queryRawUnsafe(`
      SELECT id, partner_id, price_currency, supplier_article, raw
      FROM product_links WHERE product_id = $1
    `, row.id);
    for (const link of links) {
      const linkRaw = typeof link.raw === "object" ? link.raw : {};
      console.log(`  link: id=${link.id} partnerId=${link.partner_id} priceCurrency=${link.price_currency}`);
      console.log(`    raw keys: ${Object.keys(linkRaw).join(", ")}`);
      console.log(`    raw.id=${linkRaw.id} raw.matchType=${linkRaw.matchType} raw.sourceRowId=${linkRaw.sourceRowId} raw.article=${linkRaw.article}`);
    }
  }

  // Also show the Тимофей supplier from managed_suppliers
  const [tim] = await prisma.$queryRawUnsafe(`
    SELECT id, partner_id, name, default_currency, raw
    FROM managed_suppliers WHERE partner_id = '278'
  `);
  if (tim) {
    const r = typeof tim.raw === "object" ? tim.raw : {};
    console.log(`\nТимофей: id=${tim.id} partnerId=${tim.partner_id} name="${tim.name}" defaultCurrency=${tim.default_currency}`);
    console.log(`  raw.priceCurrency=${r.priceCurrency} raw.defaultCurrency=${r.defaultCurrency}`);
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
