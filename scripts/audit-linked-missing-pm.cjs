#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const {
  buildFreshWarehouseProductsFromKnownProducts,
  pickNoSupplierAutomationCandidates,
  runNoSupplierMarketplaceAutomation,
  productFromPostgres,
  supplierFromPostgres,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

function parseArgs(argv) {
  const args = new Set(argv);
  const valueOf = (name, fallback) => {
    const prefix = `${name}=`;
    const match = argv.find((item) => item.startsWith(prefix));
    return match ? match.slice(prefix.length) : fallback;
  };
  return {
    apply: args.has("--apply"),
    limit: Math.max(1, Math.min(5000, Number(valueOf("--limit", 500)) || 500)),
    offset: Math.max(0, Number(valueOf("--offset", 0)) || 0),
    json: args.has("--json"),
  };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const prisma = getPrisma();
  if (!prisma) {
    console.error("Postgres is not configured.");
    process.exit(1);
  }

  const [rows, supplierRows] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where: { links: { some: {} } },
      include: { links: true },
      orderBy: { updatedAt: "desc" },
      skip: options.offset,
      take: options.limit,
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
  ]);
  const products = rows.map(productFromPostgres);
  const built = await buildFreshWarehouseProductsFromKnownProducts(
    { suppliers: supplierRows.map(supplierFromPostgres) },
    products,
    { persistMutations: false, livePriceMaster: false, batchPriceMaster: false },
  );
  const missingSupplier = built.filter((product) => (product.links || []).length > 0 && !product.selectedSupplier && !product.stockOnlyFallbackActive);
  const candidates = pickNoSupplierAutomationCandidates(missingSupplier, {
    includeNoLinks: false,
    now: new Date().toISOString(),
  });

  const report = {
    scanned: rows.length,
    linked: built.filter((product) => (product.links || []).length > 0).length,
    missingSupplier: missingSupplier.length,
    zeroStockCandidates: candidates.toZeroStock.length,
    sample: missingSupplier.slice(0, 20).map((product) => ({
      id: product.id,
      offerId: product.offerId || "",
      marketplace: product.marketplace || "",
      target: product.target || "",
      links: (product.links || []).length,
      stock: product.marketplaceState?.stock ?? product.stock ?? null,
      stockZeroAt: product.noSupplierAutomation?.stockZeroAt || null,
      partial: Boolean(product.partial),
      priceSource: product.priceSource || null,
    })),
  };

  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(`Scanned: ${report.scanned}`);
    console.log(`Linked: ${report.linked}`);
    console.log(`Missing PM supplier: ${report.missingSupplier}`);
    console.log(`Zero-stock candidates now: ${report.zeroStockCandidates}`);
    if (report.sample.length) {
      console.log("Sample:");
      for (const item of report.sample) {
        console.log(`- ${item.offerId || item.id} · ${item.marketplace}/${item.target} · links=${item.links} · stock=${item.stock} · zeroAt=${item.stockZeroAt || "-"}`);
      }
    }
  }

  if (options.apply && candidates.toZeroStock.length) {
    const automation = await runNoSupplierMarketplaceAutomation(
      { products: candidates.toZeroStock },
      { productIds: candidates.toZeroStock.map((product) => product.id), includeNoLinks: false, source: "audit_linked_missing_pm" },
    );
    const payload = {
      applied: true,
      zeroStockSent: automation.zeroStockSent,
      archived: automation.archived,
      errors: automation.errors?.length || 0,
    };
    console.log(options.json ? JSON.stringify(payload, null, 2) : `Applied zero stock: ${payload.zeroStockSent}, errors: ${payload.errors}`);
  }
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
