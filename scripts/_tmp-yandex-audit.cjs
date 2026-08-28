#!/usr/bin/env node
"use strict";

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const {
  getYandexShopByTarget,
  getYandexOfferMappingsByOfferIds,
  getYandexPriceMap,
  yandexOfferIdFromMapping,
  cleanText,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

const SAMPLE_LIMIT = Number(process.argv[2] || 600);

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const totalLinked = await prisma.warehouseProduct.count({
    where: { marketplace: "yandex", archived: false, links: { some: {} } },
  });

  const rows = await prisma.warehouseProduct.findMany({
    where: { marketplace: "yandex", archived: false, links: { some: {} } },
    select: { id: true, offerId: true, target: true, currentPrice: true, targetPrice: true },
    take: SAMPLE_LIMIT,
    orderBy: { updatedAt: "desc" },
  });

  console.log(`total linked non-archived yandex products: ${totalLinked}`);
  console.log(`sampled: ${rows.length}`);

  const byTarget = new Map();
  for (const row of rows) {
    if (!byTarget.has(row.target)) byTarget.set(row.target, []);
    byTarget.get(row.target).push(row);
  }

  let notFound = 0;
  let priceMismatch = 0;
  let checkedForMismatch = 0;
  const notFoundSamples = [];
  const mismatchSamples = [];

  for (const [target, items] of byTarget) {
    const shop = getYandexShopByTarget(target);
    if (!shop) {
      console.log(`no shop config for target=${target} (count=${items.length})`);
      continue;
    }
    const offerIds = items.map((r) => r.offerId);

    for (const idChunk of chunk(offerIds, 100)) {
      const subset = items.filter((r) => idChunk.includes(r.offerId));
      let mappings = [];
      try {
        mappings = await getYandexOfferMappingsByOfferIds(shop, idChunk);
      } catch (e) {
        console.log(`mapping fetch failed target=${target}: ${e.message}`);
        continue;
      }
      const found = new Set(mappings.map((m) => cleanText(yandexOfferIdFromMapping(m)).toLowerCase()));
      for (const row of subset) {
        if (!found.has(cleanText(row.offerId).toLowerCase())) {
          notFound += 1;
          if (notFoundSamples.length < 15) notFoundSamples.push({ id: row.id, offerId: row.offerId, target });
        }
      }
    }

    for (const priceChunk of chunk(offerIds, 200)) {
      const subset = items.filter((r) => priceChunk.includes(r.offerId));
      let priceMap = new Map();
      try {
        priceMap = await getYandexPriceMap(shop, priceChunk);
      } catch (e) {
        console.log(`price fetch failed target=${target}: ${e.message}`);
        continue;
      }
      for (const row of subset) {
        const live = Number(priceMap.get(row.offerId) || 0);
        const cached = Number(row.currentPrice || 0);
        if (live > 0 && cached > 0) {
          checkedForMismatch += 1;
          const tolerance = Math.max(1, cached * 0.01);
          if (Math.abs(live - cached) > tolerance) {
            priceMismatch += 1;
            if (mismatchSamples.length < 15) {
              mismatchSamples.push({
                id: row.id,
                offerId: row.offerId,
                target,
                liveYandexPrice: live,
                cachedCurrentPrice: cached,
                cachedTargetPrice: row.targetPrice,
              });
            }
          }
        }
      }
    }
  }

  console.log(JSON.stringify({
    totalLinked,
    sampled: rows.length,
    notFoundOnYandex: notFound,
    notFoundSamples,
    checkedForPriceMismatch: checkedForMismatch,
    priceMismatch,
    mismatchSamples,
  }, null, 2));

  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
