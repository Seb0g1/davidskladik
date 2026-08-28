#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

async function timedFetch(url, options = {}) {
  const startedAt = Date.now();
  const response = await fetch(url, options);
  const elapsedMs = Date.now() - startedAt;
  let body = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  return { response, body, elapsedMs };
}

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  const prisma = new PrismaClient();
  const baseUrl = process.env.PROD_HEALTH_BASE_URL || "http://127.0.0.1:3000";
  try {
    const [total, ozonCount, yandexCount, linkedCount, duplicateGroups, fullPairs] = await Promise.all([
      prisma.warehouseProduct.count(),
      prisma.warehouseProduct.count({ where: { marketplace: "ozon" } }),
      prisma.warehouseProduct.count({ where: { marketplace: "yandex" } }),
      prisma.warehouseProduct.count({ where: { links: { some: {} } } }),
      prisma.$queryRaw`
        SELECT COUNT(*)::int AS cnt FROM (
          SELECT marketplace, COALESCE(target, ''), LOWER(offer_id)
          FROM warehouse_products
          GROUP BY 1, 2, 3
          HAVING COUNT(*) > 1
        ) d
      `,
      prisma.$queryRaw`
        SELECT COUNT(*)::int AS cnt FROM (
          SELECT LOWER(offer_id) AS offer_id
          FROM warehouse_products
          WHERE offer_id IS NOT NULL AND offer_id <> ''
          GROUP BY 1
          HAVING COUNT(DISTINCT marketplace) > 1
        ) p
      `,
    ]);

    const health = await timedFetch(`${baseUrl}/health`);
    const page = await timedFetch(`${baseUrl}/api/warehouse/products/page?page=1&pageSize=40`);
    const pageItems = Array.isArray(page.body?.items) ? page.body.items : [];
    const withBoth = pageItems.filter((item) => {
      const marketplaces = new Set((item.marketplaceRows || []).map((row) => row.marketplace));
      return marketplaces.has("ozon") && marketplaces.has("yandex");
    });

    const catalogAuthRequired = page.response.status === 401;
    const report = {
      ok: health.response.ok && (page.response.ok || catalogAuthRequired),
      database: {
        total,
        ozon: ozonCount,
        yandex: yandexCount,
        linked: linkedCount,
        duplicateOfferGroups: duplicateGroups[0]?.cnt || 0,
        offerIdOzonYandexPairs: fullPairs[0]?.cnt || 0,
      },
      api: {
        health: {
          status: health.response.status,
          elapsedMs: health.elapsedMs,
          body: health.body,
        },
        catalogPage: {
          status: page.response.status,
          elapsedMs: page.elapsedMs,
          authRequired: catalogAuthRequired,
          items: pageItems.length,
          total: page.body?.total ?? null,
          partial: page.body?.partial ?? null,
          source: page.body?.source ?? null,
          withOzonAndYandex: withBoth.length,
        },
      },
    };

    console.log(JSON.stringify(report, null, 2));
    if (!report.ok) process.exitCode = 1;
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
