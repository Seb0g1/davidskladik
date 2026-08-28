#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const {
  productFromPostgres,
  warehouseProductCanonicalId,
  warehouseProductPageGroupKey,
  findOzonMatchForYandexProduct,
  buildOzonProductLookupIndexes,
  ozonProductShouldMaterializeYandexSibling,
  ozonProductHasYandexExport,
} = require("../server.js");

async function run() {
  const prisma = new PrismaClient();
  try {
    const [ozonRows, yandexRows, shopRows] = await Promise.all([
      prisma.warehouseProduct.findMany({ where: { marketplace: "ozon" }, include: { links: true } }),
      prisma.warehouseProduct.findMany({ where: { marketplace: "yandex" }, include: { links: true } }),
      prisma.$queryRaw`
        SELECT target, COUNT(*)::int AS cnt
        FROM warehouse_products
        WHERE marketplace = 'yandex'
        GROUP BY target
        ORDER BY cnt DESC
        LIMIT 20
      `,
    ]);
    const ozonProducts = ozonRows.map(productFromPostgres);
    const yandexProducts = yandexRows.map(productFromPostgres);
    const ozonIndexes = buildOzonProductLookupIndexes(ozonProducts);
    const yandexOfferIds = new Set(yandexProducts.map((p) => String(p.offerId || "").toLowerCase()).filter(Boolean));
    const shop = { id: "yandex-06c2112c", name: "Yandex Market" };

    let ozonNoYandexPair = 0;
    let ozonCouldMaterialize = 0;
    let ozonWithExportNoYandex = 0;
    let ozonLinkedNoYandex = 0;
    let yandexNoAutoPair = 0;
    let yandexNoOzonMatch = 0;
    let yandexParfumeriusTarget = 0;
    let canonicalTargetMismatch = 0;
    const materializeSamples = [];
    const unpairedYandexSamples = [];

    for (const ozon of ozonProducts) {
      const groupId = ozon.manualGroupId || ozon.raw?.manualGroupId || "";
      const offerKey = String(ozon.offerId || "").toLowerCase();
      const hasYandexSibling = yandexOfferIds.has(offerKey)
        || yandexProducts.some((y) => (y.manualGroupId || y.raw?.manualGroupId) === groupId && groupId.startsWith("auto-pair-"));
      if (!hasYandexSibling) {
        ozonNoYandexPair += 1;
        const could = ozonProductShouldMaterializeYandexSibling(ozon, shop, { yandexOfferIds, yandexCacheOfferIds: new Set() });
        if (could) {
          ozonCouldMaterialize += 1;
          if (materializeSamples.length < 5) materializeSamples.push({ id: ozon.id, offerId: ozon.offerId, hasExport: ozonProductHasYandexExport(ozon, shop), links: (ozon.links || []).length });
        }
        if (ozonProductHasYandexExport(ozon, shop)) ozonWithExportNoYandex += 1;
        if ((ozon.links || []).length > 0) ozonLinkedNoYandex += 1;
      }
    }

    for (const yandex of yandexProducts) {
      const groupId = clean(yandex.manualGroupId || yandex.raw?.manualGroupId);
      if (!groupId.startsWith("auto-pair-")) {
        yandexNoAutoPair += 1;
        const ozon = findOzonMatchForYandexProduct(yandex, ozonIndexes);
        if (!ozon) yandexNoOzonMatch += 1;
        else if (unpairedYandexSamples.length < 5) unpairedYandexSamples.push({ id: yandex.id, offerId: yandex.offerId, target: yandex.target, ozonId: ozon.id });
      }
      if (clean(yandex.target) === "parfumerius") yandexParfumeriusTarget += 1;
      const canonical = warehouseProductCanonicalId(yandex);
      const canonicalFromShop = warehouseProductCanonicalId({ ...yandex, target: "yandex-06c2112c" });
      if (canonical && canonicalFromShop && canonical !== canonicalFromShop && yandex.id === canonical) {
        canonicalTargetMismatch += 1;
      }
    }

    console.log(JSON.stringify({
      ok: true,
      ozonTotal: ozonProducts.length,
      yandexTotal: yandexProducts.length,
      ozonNoYandexPair,
      ozonCouldMaterialize,
      ozonWithExportNoYandex,
      ozonLinkedNoYandex,
      yandexNoAutoPair,
      yandexNoOzonMatch,
      yandexParfumeriusTarget,
      canonicalTargetMismatch,
      yandexTargets: shopRows,
      materializeSamples,
      unpairedYandexSamples,
    }, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

function clean(value) {
  return String(value || "").trim();
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
