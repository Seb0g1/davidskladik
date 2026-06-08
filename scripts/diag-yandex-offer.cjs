#!/usr/bin/env node
"use strict";

require("dotenv").config();
process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const offerId = process.argv[2] || "6021";
const {
  buildYandexOfferMapping,
  getYandexOfferCardsContentStatus,
  getYandexShopByTarget,
  getYandexShops,
  isWeakYandexWarehouseCard,
  normalizeWarehouseProduct,
  patchYandexWarehouseProductFromOzonDonor,
  productFromPostgres,
  resolveWarehouseBrand,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");
  const rows = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: offerId, mode: "insensitive" } },
    include: { links: true },
  });
  const products = rows.map(productFromPostgres);
  const ozon = products.find((p) => String(p.marketplace).toLowerCase() === "ozon");
  const yandex = products.find((p) => String(p.marketplace).toLowerCase() === "yandex");
  const patched = ozon && yandex ? patchYandexWarehouseProductFromOzonDonor(yandex, ozon) : null;
  const built = yandex ? buildYandexOfferMapping(normalizeWarehouseProduct(yandex)) : null;
  const builtPatched = patched ? buildYandexOfferMapping(patched) : null;

  let cardStatus = null;
  if (yandex) {
    const shop = getYandexShopByTarget(yandex.target) || getYandexShops()[0];
    if (shop) {
      const rowsStatus = await getYandexOfferCardsContentStatus(shop, [yandex.offerId], { withRecommendations: true });
      cardStatus = rowsStatus[0] || null;
    }
  }

  console.log(JSON.stringify({
    offerId,
    ozon: ozon ? {
      id: ozon.id,
      name: ozon.name,
      brand: ozon.brand,
      imageUrl: ozon.imageUrl,
      vendor: ozon.ozon?.vendor,
      resolvedBrand: resolveWarehouseBrand(ozon),
      depth: ozon.ozon?.depth,
      width: ozon.ozon?.width,
      height: ozon.ozon?.height,
      weight: ozon.ozon?.weight,
      images: (ozon.ozon?.images || []).slice(0, 3),
      pictures: (ozon.ozon?.images || []).length,
    } : null,
    yandex: yandex ? {
      id: yandex.id,
      weak: isWeakYandexWarehouseCard(yandex),
      name: yandex.name,
      brand: yandex.brand,
      imageUrl: yandex.imageUrl,
      vendor: yandex.yandex?.vendor,
      resolvedBrand: resolveWarehouseBrand(yandex),
      pictures: (yandex.yandex?.pictures || []).slice(0, 3),
      picturesCount: (yandex.yandex?.pictures || []).length,
      builtOffer: built?.offer,
      builtMissing: built?.missing,
    } : null,
    patched: patched ? {
      vendor: patched.yandex?.vendor,
      resolvedBrand: resolveWarehouseBrand(patched),
      imageUrl: patched.imageUrl,
      pictures: (patched.yandex?.pictures || []).slice(0, 3),
      builtOffer: builtPatched?.offer,
      builtMissing: builtPatched?.missing,
    } : null,
    yandexCardStatus: cardStatus,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
