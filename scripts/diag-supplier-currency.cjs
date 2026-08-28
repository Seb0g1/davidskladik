#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Check managed_suppliers for Тимофей Косметика and other RUB suppliers
  const suppliers = await prisma.$queryRawUnsafe(`
    SELECT id, name, partner_id, default_currency, active, stop_reason, note,
           raw->>'priceCurrency' AS raw_price_currency,
           raw->>'defaultCurrency' AS raw_default_currency
    FROM managed_suppliers
    ORDER BY name
  `).catch(() => []);

  console.log(`\n=== Managed suppliers (${suppliers.length}) ===\n`);
  for (const s of suppliers) {
    const currency = s.default_currency || s.raw_price_currency || s.raw_default_currency || "USD";
    const flag = currency === "RUB" ? " ← RUB SUPPLIER" : "";
    console.log(`  [${currency}]${flag} ${s.name} (partnerId=${s.partner_id}) active=${s.active}`);
  }

  // Focus on Тимофей Косметика
  const tim = suppliers.find((s) => s.name && s.name.toLowerCase().includes("тимоф"));
  if (tim) {
    console.log(`\n=== Тимофей Косметика FULL ===\n`);
    console.log(`  id=${tim.id}`);
    console.log(`  name=${tim.name}`);
    console.log(`  partner_id=${tim.partner_id}`);
    console.log(`  default_currency=${tim.default_currency}`);
    console.log(`  raw_price_currency=${tim.raw_price_currency}`);
    console.log(`  raw_default_currency=${tim.raw_default_currency}`);
    console.log(`  active=${tim.active}`);
    console.log(`  stop_reason=${tim.stop_reason}`);
  } else {
    console.log(`\nТимофей Косметика NOT FOUND in managed_suppliers!`);
  }

  // Also check the actual price_history new_price for K18001
  const history = await prisma.$queryRawUnsafe(`
    SELECT ph.offer_id, ph.marketplace, ph.created_at, ph.new_price, ph.status,
           ph.response->>'markup' AS markup,
           ph.response->>'usdRate' AS usd_rate,
           ph.response->>'pmPriceUsd' AS pm_price_usd
    FROM price_history ph
    WHERE ph.offer_id IN ('K18001','ЮК345754')
    ORDER BY ph.created_at DESC
    LIMIT 10
  `).catch(async () => {
    // maybe column is named differently
    return prisma.$queryRawUnsafe(`
      SELECT offer_id, marketplace, created_at, status, response
      FROM price_history
      WHERE offer_id IN ('K18001','ЮК345754')
      ORDER BY created_at DESC
      LIMIT 10
    `);
  });

  console.log(`\n=== Price history (actual prices) ===\n`);
  for (const h of history) {
    const resp = h.response ? (typeof h.response === "string" ? JSON.parse(h.response) : h.response) : {};
    console.log(`[${h.offer_id}][${h.marketplace}] ${h.created_at} status=${h.status}`);
    console.log(`  new_price=${h.new_price} markup=${resp.markup || h.markup} rate=${resp.usdRate || h.usd_rate} pm_usd=${resp.pmPriceUsd || h.pm_price_usd}`);
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
