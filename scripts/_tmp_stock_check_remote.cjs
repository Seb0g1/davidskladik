#!/usr/bin/env node
"use strict";
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const TARGET_OFFER_IDS = ["HF-THOOS37010", "CO-MO", "UG000015104", "16608", "AR0002"];

async function main() {
  const prisma = new PrismaClient();
  try {
    const placeholders = TARGET_OFFER_IDS.map((_, i) => `$${i + 1}`).join(", ");
    const rows = await prisma.$queryRawUnsafe(`
      SELECT
        wp.id,
        wp.marketplace,
        wp.raw->>'offerId'              AS offer_id,
        wp.raw->>'target'               AS target,
        wp.raw->>'targetStock'          AS target_stock,
        wp.raw->'marketplaceState'->>'code'  AS mp_code,
        wp.raw->'marketplaceState'->'warehouses' AS mp_warehouses,
        wp.raw->'lastStockSend'         AS last_stock_send,
        (SELECT COUNT(*) FROM product_links pl WHERE pl.product_id = wp.id) AS link_count,
        s.price_status,
        s.stock_status,
        s.target_stock                  AS sku_target_stock,
        s.last_stock_sent_at,
        s.last_error
      FROM warehouse_products wp
      LEFT JOIN sales_automation_sku_states s
        ON s.marketplace = wp.marketplace
        AND s.offer_id   = (wp.raw->>'offerId')
        AND s.target     = (wp.raw->>'target')
      WHERE wp.raw->>'offerId' IN (${placeholders})
      ORDER BY wp.marketplace, wp.raw->>'offerId'
    `, ...TARGET_OFFER_IDS);

    if (!rows.length) {
      console.log("❌ Ни один из артикулов не найден в warehouse_products");
      return;
    }

    for (const r of rows) {
      const warehouses = r.mp_warehouses;
      const hasWarehouses = Array.isArray(warehouses) ? warehouses.length > 0
        : warehouses && warehouses !== "null" && warehouses !== "[]";
      const ls = r.last_stock_send;

      console.log(`\n===== ${r.offer_id} [${r.marketplace}/${r.target}] =====`);
      console.log(`  links:         ${r.link_count}`);
      console.log(`  mp_code:       ${r.mp_code}`);
      console.log(`  targetStock:   ${r.target_stock ?? "(null)"}`);
      console.log(`  sku_stock:     ${r.sku_target_stock ?? "(null)"}`);
      console.log(`  price_status:  ${r.price_status ?? "(none)"}`);
      console.log(`  stock_status:  ${r.stock_status ?? "(none)"}`);
      console.log(`  last_stock_at: ${r.last_stock_sent_at ?? "(never)"}`);
      console.log(`  last_error:    ${r.last_error ?? "(none)"}`);
      console.log(`  warehouses:    ${hasWarehouses ? JSON.stringify(warehouses).slice(0, 120) : "❌ ПУСТО — warehouse_id не знаем"}`);
      if (ls) {
        console.log(`  lastStockSend: ok=${ls.ok} sentAt=${ls.sentAt} err=${ls.error ?? "-"}`);
      } else {
        console.log(`  lastStockSend: (null)`);
      }
    }

    // Check what OZON_STOCK_WAREHOUSE_IDS is set to
    console.log("\n===== ENV: OZON_STOCK_WAREHOUSE_IDS =====");
    const ids = process.env.OZON_STOCK_WAREHOUSE_IDS || process.env.OZON_STOCK_WAREHOUSE_ID || "";
    console.log(`  ${ids || "(не задан — будет дискавери через API)"}`);
    console.log(`  OZON_WAREHOUSE_LIST_ENABLED=${process.env.OZON_WAREHOUSE_LIST_ENABLED || "false (default)"}`);

  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
