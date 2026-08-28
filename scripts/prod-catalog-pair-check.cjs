#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const {
  productFromPostgres,
  warehouseProductPageGroupKey,
  fetchCrossMarketplaceSiblingRows,
} = require("../server.js");

async function run() {
  const prisma = new PrismaClient();
  try {
    const baseWhere = {};
    const sampleRows = await prisma.$queryRaw`
      SELECT id FROM warehouse_products
      WHERE marketplace = 'ozon'
        AND raw->>'manualGroupId' LIKE 'auto-pair-%'
      ORDER BY updated_at DESC
      LIMIT 5
    `;
    const ids = (Array.isArray(sampleRows) ? sampleRows : []).map((row) => row.id).filter(Boolean);
    const rows = await prisma.warehouseProduct.findMany({
      where: { id: { in: ids.length ? ids : ["__none__"] } },
      include: { links: true },
      take: 5,
      orderBy: { updatedAt: "desc" },
    });
    const pageProducts = rows.map(productFromPostgres);
    const cross = await fetchCrossMarketplaceSiblingRows(prisma, baseWhere, rows);
    const crossProducts = cross.map(productFromPostgres);
    const groups = new Map();
    for (const product of [...pageProducts, ...crossProducts]) {
      const key = warehouseProductPageGroupKey(product);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push({ id: product.id, marketplace: product.marketplace, offerId: product.offerId });
    }
    const paired = Array.from(groups.entries())
      .filter(([, items]) => new Set(items.map((i) => i.marketplace)).size > 1)
      .slice(0, 3);

    const pageRes = await fetch("http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=40");
    const pageJson = await pageRes.json();
    const pageItems = pageJson.items || [];
    const withBoth = pageItems.filter((item) => {
      const mps = new Set((item.marketplaceRows || []).map((r) => r.marketplace));
      return mps.has("ozon") && mps.has("yandex");
    });

    console.log(JSON.stringify({
      ok: true,
      sampleOzonWithPair: rows.length,
      crossSiblingsFetched: cross.length,
      pairedGroupsInSample: paired.length,
      pairedExamples: paired.map(([key, items]) => ({ key, items })),
      catalogPage: {
        partial: pageJson.partial,
        items: pageItems.length,
        withOzonAndYandex: withBoth.length,
        example: withBoth[0] ? {
          id: withBoth[0].id,
          offerId: withBoth[0].offerId,
          marketplaces: (withBoth[0].marketplaceRows || []).map((r) => r.marketplace),
        } : null,
      },
    }, null, 2));
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
