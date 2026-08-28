#!/usr/bin/env node
"use strict";
require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  // Check target_price vs current_price for the low-price products
  const rows = await prisma.$queryRawUnsafe(`
    SELECT
      offer_id, marketplace,
      current_price, target_price,
      raw->>'yandexMarkup' AS yandex_markup,
      raw->>'ozonMarkup'   AS ozon_markup,
      raw->>'markup'       AS markup,
      raw->'lastYandexPriceSend'->>'status' AS ym_status,
      raw->'lastYandexPriceSend'->>'sentAt' AS ym_sent_at,
      raw->'lastYandexPriceSend'->>'price'  AS ym_sent_price,
      raw->'lastOzonPriceSend'->>'status'   AS ozon_status,
      raw->'lastOzonPriceSend'->>'price'    AS ozon_sent_price,
      target_stock
    FROM warehouse_products
    WHERE offer_id = ANY(ARRAY['K18001','10825','13214','8574345','2362','НФ-00005048'])
    ORDER BY offer_id, marketplace
  `);

  console.log("\n=== target_price vs current_price for low-price products ===\n");
  for (const r of rows) {
    const markup = r.yandex_markup || r.ozon_markup || r.markup || "(default)";
    console.log(
      `${r.offer_id} [${r.marketplace}]  current=${r.current_price}₽  target=${r.target_price}₽  markup=${markup}  stock=${r.target_stock}`
    );
    if (r.marketplace === "yandex") {
      console.log(`  YM last send: status=${r.ym_status} price=${r.ym_sent_price}₽ at=${r.ym_sent_at}`);
    } else {
      console.log(`  Ozon last send: status=${r.ozon_status} price=${r.ozon_sent_price}₽`);
    }
  }

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.stack || e.message); process.exit(1); });
