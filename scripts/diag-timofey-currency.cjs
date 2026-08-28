#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  const suppliers = await prisma.$queryRawUnsafe(`
    SELECT id, partner_id, name, default_currency, active, stop_reason, updated_at
    FROM managed_suppliers
    WHERE LOWER(name) LIKE '%тимоф%' OR LOWER(name) LIKE '%timof%'
    ORDER BY name
  `);

  console.log(`\n=== managed_suppliers Тимофей (${suppliers.length} rows) ===\n`);
  for (const s of suppliers) {
    console.log(`  id=${s.id} partnerId=${s.partner_id} name="${s.name}"`);
    console.log(`  defaultCurrency=${s.default_currency} active=${s.active}`);
    console.log(`  stopReason=${s.stop_reason}`);
    console.log(`  updatedAt=${s.updated_at}`);
  }

  // Also show all suppliers with default_currency = RUB
  const rubSuppliers = await prisma.$queryRawUnsafe(`
    SELECT id, partner_id, name, default_currency, active, updated_at
    FROM managed_suppliers
    WHERE default_currency = 'RUB'
    ORDER BY name
  `);
  console.log(`\n=== ALL managed_suppliers with defaultCurrency=RUB (${rubSuppliers.length} rows) ===\n`);
  for (const s of rubSuppliers) {
    console.log(`  id=${s.id} partnerId=${s.partner_id} name="${s.name}" active=${s.active} updatedAt=${s.updated_at}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
