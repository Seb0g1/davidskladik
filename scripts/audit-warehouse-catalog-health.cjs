#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const {
  buildWarehouseCatalogGroupContext,
  countWarehouseProductGroups,
  productFromPostgres,
  warehouseProductPageGroupKey,
  warehouseProductCanonicalId,
} = require("../server.js");

function argValue(name, fallback = "") {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : fallback;
}

const limit = Math.max(100, Number(argValue("--limit", "5000")) || 5000);

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  const prisma = new PrismaClient();
  try {
    const [total, ozonCount, yandexCount, linkedCount, duplicateGroups] = await Promise.all([
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
    ]);

    const rows = await prisma.warehouseProduct.findMany({
      include: { links: true },
      orderBy: { updatedAt: "desc" },
      take: limit,
    });
    const products = rows.map(productFromPostgres);
    const groupContext = buildWarehouseCatalogGroupContext(products);
    const catalogGroupTotal = countWarehouseProductGroups(products);
    const groups = new Map();
    for (const product of products) {
      const key = warehouseProductPageGroupKey(product, groupContext);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(product);
    }

    let ozonYandexPairs = 0;
    let autoPairGroups = 0;
    let autoPairComplete = 0;
    let missingYandexSibling = 0;
    let missingOzonSibling = 0;
    let linkedWithoutSupplier = 0;
    let archivedLinked = 0;

    for (const [groupKey, items] of groups) {
      const marketplaces = new Set(items.map((item) => item.marketplace));
      if (marketplaces.has("ozon") && marketplaces.has("yandex")) ozonYandexPairs += 1;
      if (String(groupKey).startsWith("manual:auto-pair-")) {
        autoPairGroups += 1;
        if (marketplaces.has("ozon") && marketplaces.has("yandex")) autoPairComplete += 1;
      }
      const offerId = items[0]?.offerId;
      if (offerId && marketplaces.size === 1) {
        if (marketplaces.has("ozon")) missingYandexSibling += 1;
        if (marketplaces.has("yandex")) missingOzonSibling += 1;
      }
      for (const item of items) {
        const hasLinks = Array.isArray(item.links) && item.links.length > 0;
        if (hasLinks && !item.selectedSupplier && !item.stockOnlyFallbackActive) linkedWithoutSupplier += 1;
        if (hasLinks && (item.archived || item.marketplaceState?.code === "archived")) archivedLinked += 1;
      }
    }

    const canonicalMismatches = products.filter((product) => {
      const canonical = warehouseProductCanonicalId(product);
      return canonical && product.id !== canonical;
    }).length;

    console.log(JSON.stringify({
      ok: true,
      scanned: products.length,
      totals: {
        all: total,
        ozon: ozonCount,
        yandex: yandexCount,
        linked: linkedCount,
        duplicateOfferGroups: duplicateGroups[0]?.cnt || 0,
      },
      catalogGroupTotal,
      sampleGroups: {
        total: groups.size,
        ozonYandexPairs,
        autoPairGroups,
        autoPairComplete,
        missingYandexSibling,
        missingOzonSibling,
      },
      quality: {
        linkedWithoutSupplierInSample: linkedWithoutSupplier,
        linkedWithoutSupplierNote: "raw postgres read without PriceMaster enrichment; use audit-linked-missing-pm.cjs for live supplier resolution",
        archivedLinkedInSample: archivedLinked,
        canonicalIdMismatchesInSample: canonicalMismatches,
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
