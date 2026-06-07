#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  const prisma = new PrismaClient();
  try {
    const [row] = await prisma.$queryRaw`
      WITH yandex AS (
        SELECT id, LOWER(offer_id) AS offer_id, raw->>'manualGroupId' AS group_id
        FROM warehouse_products
        WHERE marketplace = 'yandex'
      ),
      ozon AS (
        SELECT id, LOWER(offer_id) AS offer_id, raw->>'manualGroupId' AS group_id
        FROM warehouse_products
        WHERE marketplace = 'ozon'
      ),
      yandex_pairs AS (
        SELECT id, group_id
        FROM yandex
        WHERE group_id LIKE 'auto-pair-%'
      ),
      ozon_pairs AS (
        SELECT id, group_id
        FROM ozon
        WHERE group_id LIKE 'auto-pair-%'
      ),
      complete AS (
        SELECT y.group_id
        FROM yandex_pairs y
        INNER JOIN ozon_pairs o ON o.group_id = y.group_id
        GROUP BY y.group_id
      )
      SELECT
        (SELECT COUNT(*)::int FROM yandex) AS yandex_total,
        (SELECT COUNT(*)::int FROM ozon) AS ozon_total,
        (SELECT COUNT(*)::int FROM yandex_pairs) AS yandex_auto_pair,
        (SELECT COUNT(*)::int FROM ozon_pairs) AS ozon_auto_pair,
        (SELECT COUNT(*)::int FROM complete) AS complete_groups,
        (SELECT COUNT(*)::int FROM yandex_pairs y WHERE NOT EXISTS (
          SELECT 1 FROM ozon_pairs o WHERE o.group_id = y.group_id
        )) AS yandex_without_ozon_group,
        (SELECT COUNT(*)::int FROM ozon_pairs o WHERE NOT EXISTS (
          SELECT 1 FROM yandex_pairs y WHERE y.group_id = o.group_id
        )) AS ozon_without_yandex_group
    `;
    console.log(JSON.stringify({ ok: true, ...row }, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
