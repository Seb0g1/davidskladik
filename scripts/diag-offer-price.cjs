#!/usr/bin/env node
"use strict";

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = "true";

const query = cleanText(process.argv[2] || "");
if (!query) {
  console.error("Usage: node scripts/diag-offer-price.cjs <offerId>");
  process.exit(1);
}

const {
  buildFreshWarehouseProductsFromKnownProducts,
  calculateRubPrice,
  normalizePriceMasterPrice,
  productFromPostgres,
  resolvePriceMasterRowCurrency,
  supplierFromPostgres,
  managedSupplierMaps,
  getOzonAccountByTarget,
  getOzonPriceMap,
  normalizeOzonPriceDetails,
  pickOzonCabinetListedPrice,
  getYandexShopByTarget,
  getYandexPriceMap,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

function cleanText(value) {
  return String(value || "").trim();
}

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const rows = await prisma.warehouseProduct.findMany({
    where: {
      OR: [
        { offerId: { equals: query, mode: "insensitive" } },
        { offerId: { contains: query, mode: "insensitive" } },
      ],
    },
    include: { links: true },
    take: 10,
  });
  const suppliers = (await prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }))
    .map(supplierFromPostgres);
  const maps = managedSupplierMaps(suppliers);
  const appSettings = await prisma.appSetting.findUnique({ where: { key: "app" } }).catch(() => null);
  const usdRate = Number(appSettings?.value?.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95);

  let pmRows = [];
  if (!rows.length) {
    pmRows = await prisma.priceMasterSnapshotItem.findMany({
      where: { article: { contains: query, mode: "insensitive" } },
      take: 5,
    });
    const linkRows = await prisma.productLink.findMany({
      where: {
        OR: [
          { supplierArticle: { contains: query, mode: "insensitive" } },
          { keyword: { contains: query, mode: "insensitive" } },
        ],
      },
      take: 20,
    });
    const extraIds = [...new Set(linkRows.map((row) => row.productId).filter(Boolean))];
    if (extraIds.length) {
      const extra = await prisma.warehouseProduct.findMany({
        where: { id: { in: extraIds } },
        include: { links: true },
      });
      rows.push(...extra);
    }
  }

  const known = rows.map(productFromPostgres);
  const built = known.length
    ? await buildFreshWarehouseProductsFromKnownProducts(
      { suppliers },
      known,
      { persistMutations: false, livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: 8000 },
    )
    : [];

  const automation = await prisma.salesAutomationSkuState.findMany({
    where: {
      OR: [
        { offerId: { equals: query, mode: "insensitive" } },
        { offerId: { contains: query, mode: "insensitive" } },
      ],
    },
    take: 10,
  });

  const liveOzon = {};
  const liveYandex = {};
  for (const product of built) {
    const offerId = cleanText(product.offerId);
    if (!offerId) continue;
    if (String(product.marketplace).toLowerCase() === "ozon") {
      const account = getOzonAccountByTarget(product.target || "ozon");
      if (account) {
        try {
          const map = await getOzonPriceMap([offerId], account);
          const raw = map.get(offerId) || {};
          const details = normalizeOzonPriceDetails(raw);
          liveOzon[offerId] = {
            ...details,
            cabinetListed: pickOzonCabinetListedPrice(details),
            rawPrice: raw?.price || null,
          };
        } catch (error) {
          liveOzon[offerId] = { error: error?.message || String(error) };
        }
      }
    }
    if (String(product.marketplace).toLowerCase() === "yandex") {
      const shop = getYandexShopByTarget(product.target || "yandex");
      if (shop) {
        try {
          const map = await getYandexPriceMap(shop, [offerId]);
          liveYandex[offerId] = Number(map.get(offerId) || 0) || null;
        } catch (error) {
          liveYandex[offerId] = { error: error?.message || String(error) };
        }
      }
    }
  }

  console.log(JSON.stringify({
    query,
    usdRate,
    liveOzon,
    liveYandex,
    products: built.map((product) => {
      const sel = product.selectedSupplier || {};
      const currency = resolvePriceMasterRowCurrency(
        { partnerName: sel.partnerName, price: sel.price },
        {},
        maps,
        usdRate,
      );
      const normalized = normalizePriceMasterPrice(sel.price, usdRate, currency);
      const markup = Number(product.targetMarkup || product.markup || 1.7);
      const calc = calculateRubPrice(normalized.price, usdRate, markup, {
        ...normalized,
        partnerName: sel.partnerName,
      });
      return {
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        name: product.name?.slice(0, 100),
        currentPrice: product.currentPrice,
        targetPrice: product.targetPrice,
        marketplacePrice: product.marketplacePrice,
        selectedSupplier: sel.partnerName,
        supplierPrice: sel.price,
        supplierCurrency: currency,
        supplierPriceCurrency: sel.priceCurrency,
        calculatedRub: calc,
        markup,
        links: (product.links || []).map((l) => ({
          article: l.article,
          supplierName: l.supplierName,
          priceCurrency: l.priceCurrency,
        })),
        lastOzonPriceSend: product.lastOzonPriceSend || null,
        lastYandexPriceSend: product.lastYandexPriceSend || null,
      };
    }),
    pmMatches: pmRows.map((row) => ({
      article: row.article,
      partnerName: row.partnerName,
      price: row.price,
      currency: row.currency,
      nativeName: row.nativeName?.slice?.(0, 80) || row.nativeName,
    })),
    automation: automation.map((row) => ({
      productId: row.productId,
      offerId: row.offerId,
      marketplace: row.marketplace,
      priceStatus: row.priceStatus,
      reason: row.reason,
      targetPrice: row.targetPrice,
      lastError: row.lastError?.slice?.(0, 200) || row.lastError,
      raw: row.raw,
    })),
  }, null, 2));

  await prisma.$disconnect();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
