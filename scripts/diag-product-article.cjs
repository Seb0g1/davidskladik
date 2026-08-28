#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const query = process.argv[2] || "45352437";
const {
  buildFreshWarehouseProductsFromKnownProducts,
  pickNoSupplierAutomationCandidates,
  productFromPostgres,
  supplierFromPostgres,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const linkRows = await prisma.productLink.findMany({
    where: {
      OR: [
        { supplierArticle: { equals: query, mode: "insensitive" } },
        { supplierName: { equals: query, mode: "insensitive" } },
        { keyword: { equals: query, mode: "insensitive" } },
      ],
    },
    include: { product: { include: { links: true } } },
  });

  const linkedProductIds = [...new Set(linkRows.map((row) => row.productId).filter(Boolean))];
  const offerRows = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: query, mode: "insensitive" } },
    include: { links: true },
  });
  const productIds = [...new Set([...linkedProductIds, ...offerRows.map((row) => row.id)])];
  const [products, supplierRows, pmRows] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where: { id: { in: productIds.length ? productIds : ["__none__"] } },
      include: { links: true },
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
    prisma.priceMasterSnapshotItem.findMany({
      where: { article: { equals: query, mode: "insensitive" } },
      take: 10,
    }),
  ]);

  const known = products.map(productFromPostgres);
  const builtSnapshot = await buildFreshWarehouseProductsFromKnownProducts(
    { suppliers: supplierRows.map(supplierFromPostgres) },
    known,
    { persistMutations: false, livePriceMaster: false, batchPriceMaster: false },
  );
  const builtLive = await buildFreshWarehouseProductsFromKnownProducts(
    { suppliers: supplierRows.map(supplierFromPostgres) },
    known,
    { persistMutations: false, livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: 5000 },
  );

  const candidates = pickNoSupplierAutomationCandidates(builtSnapshot, {
    includeNoLinks: false,
    now: new Date().toISOString(),
    skipLinkedGrace: true,
  });

  console.log(JSON.stringify({
    query,
    productIds,
    pmSnapshotRows: pmRows.length,
    products: builtSnapshot.map((product, index) => ({
      id: product.id,
      offerId: product.offerId,
      marketplace: product.marketplace,
      target: product.target,
      hasLinks: product.hasLinks,
      links: (product.links || []).map((link) => ({
        id: link.id,
        article: link.article,
        exactName: link.exactName,
        partnerId: link.partnerId,
        matchedCount: link.matchedCount,
        missingInPriceMaster: link.missingInPriceMaster,
      })),
      suppliers: (product.suppliers || []).map((supplier) => ({
        partnerName: supplier.partnerName,
        article: supplier.article,
        price: supplier.price,
        active: supplier.active,
        available: supplier.available,
        stopped: supplier.stopped,
        priceEligible: supplier.priceEligible,
        stockOnly: supplier.stockOnly,
        priceSource: supplier.priceSource,
      })),
      selectedSupplier: product.selectedSupplier ? {
        partnerName: product.selectedSupplier.partnerName,
        article: product.selectedSupplier.article,
        price: product.selectedSupplier.price,
        priceSource: product.selectedSupplier.priceSource,
      } : null,
      stockOnlyFallbackActive: product.stockOnlyFallbackActive,
      ready: product.ready,
      targetStock: product.targetStock,
      marketplaceStock: product.marketplaceState?.stock,
      stockZeroAt: product.noSupplierAutomation?.stockZeroAt,
      rawStockZeroAt: products.find((row) => row.id === product.id)?.raw?.noSupplierAutomation?.stockZeroAt || null,
      rawMarketplaceStock: products.find((row) => row.id === product.id)?.raw?.marketplaceState?.stock ?? null,
      inZeroStockCandidates: candidates.toZeroStock.some((item) => item.id === product.id),
      liveSelectedSupplier: builtLive[index]?.selectedSupplier?.partnerName || null,
      liveMissingAllLinks: (builtLive[index]?.links || []).every((link) => link.missingInPriceMaster),
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
