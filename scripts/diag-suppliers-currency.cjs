#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Overall counts
  const counts = await prisma.$queryRawUnsafe(`
    SELECT default_currency, COUNT(*) as cnt
    FROM managed_suppliers
    GROUP BY default_currency
    ORDER BY cnt DESC
  `);
  console.log("\n=== managed_suppliers default_currency breakdown ===");
  for (const c of counts) {
    console.log(`  "${c.default_currency || "(null)"}" → ${c.cnt} suppliers`);
  }

  // List all RUB suppliers
  const rubSuppliers = await prisma.$queryRawUnsafe(`
    SELECT partner_id, name, default_currency, active
    FROM managed_suppliers
    WHERE default_currency = 'RUB'
    ORDER BY name
  `);
  console.log(`\n=== RUB suppliers (${rubSuppliers.length}) ===`);
  for (const s of rubSuppliers) {
    const isInnaSuspect = s.name.toLowerCase().includes("инна") || s.name.toLowerCase().includes("inna");
    console.log(`  partnerId=${s.partner_id} name="${s.name}" active=${s.active} isInnaSuspect=${isInnaSuspect}`);
  }

  // List USD suppliers
  const usdSuppliers = await prisma.$queryRawUnsafe(`
    SELECT partner_id, name FROM managed_suppliers
    WHERE default_currency = 'USD' ORDER BY name
  `);
  console.log(`\n=== USD suppliers (${usdSuppliers.length}) ===`);
  for (const s of usdSuppliers) {
    console.log(`  partnerId=${s.partner_id} name="${s.name}"`);
  }

  // Check specific Cinnabar suppliers
  const specific = await prisma.$queryRawUnsafe(`
    SELECT partner_id, name, default_currency FROM managed_suppliers
    WHERE name ILIKE '%дима%' OR name ILIKE '%allscent%' OR name ILIKE '%олег%'
       OR name ILIKE '%юля%' OR name ILIKE '%борик%' OR name ILIKE '%вишняков%'
    ORDER BY name
  `);
  console.log(`\n=== Filtered suppliers (Дима/Allscent/Олег/Юля/Борики) ===`);
  for (const s of specific) {
    console.log(`  partnerId=${s.partner_id} name="${s.name}" currency=${s.default_currency}`);
  }

  await prisma.$disconnect();
}
main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
