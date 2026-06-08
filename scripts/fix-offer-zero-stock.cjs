#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const offerId = process.argv[2] || "";
if (!offerId) {
  console.error("Usage: node scripts/fix-offer-zero-stock.cjs <offerId>");
  process.exit(1);
}

const {
  buildFreshWarehouseProductsFromKnownProducts,
  pickNoSupplierAutomationCandidates,
  runNoSupplierMarketplaceAutomation,
  productFromPostgres,
  supplierFromPostgres,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const [rows, supplierRows] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where: { offerId: { equals: offerId, mode: "insensitive" } },
      include: { links: true },
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
  ]);
  if (!rows.length) throw new Error(`No products for offerId ${offerId}`);

  const built = await buildFreshWarehouseProductsFromKnownProducts(
    { suppliers: supplierRows.map(supplierFromPostgres) },
    rows.map(productFromPostgres),
    { persistMutations: false, livePriceMaster: true, batchPriceMaster: true, priceMasterTimeoutMs: 8000 },
  );
  const candidates = pickNoSupplierAutomationCandidates(built, {
    includeNoLinks: false,
    skipLinkedGrace: true,
    now: new Date().toISOString(),
  });

  console.log(JSON.stringify({
    offerId,
    products: built.map((product) => ({
      id: product.id,
      marketplace: product.marketplace,
      stock: product.marketplaceState?.stock,
      selectedSupplier: product.selectedSupplier?.partnerName || null,
      missingInPriceMaster: (product.links || []).map((link) => link.missingInPriceMaster),
    })),
    zeroStockCandidates: candidates.toZeroStock.map((product) => product.id),
  }, null, 2));

  if (!candidates.toZeroStock.length) {
    console.log("No zero-stock candidates.");
    return;
  }

  const automation = await runNoSupplierMarketplaceAutomation(
    { products: candidates.toZeroStock },
    {
      productIds: candidates.toZeroStock.map((product) => product.id),
      includeNoLinks: false,
      source: "fix_offer_zero_stock",
      skipLinkedGrace: true,
    },
  );
  console.log(JSON.stringify({
    applied: true,
    zeroStockSent: automation.zeroStockSent,
    archived: automation.archived,
    errors: automation.errors || [],
    statuses: automation.productStatuses || [],
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
