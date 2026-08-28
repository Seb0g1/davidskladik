#!/usr/bin/env node
"use strict";

require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");

const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");

function readJson(filename, fallback) {
  try {
    return JSON.parse(fs.readFileSync(path.join(dataDir, filename), "utf8"));
  } catch {
    return fallback;
  }
}

function redactDatabaseUrl(value = "") {
  return String(value).replace(/:\/\/([^:@/]+):([^@/]+)@/, "://$1:***@");
}

async function inspectPostgres() {
  if (!process.env.DATABASE_URL) return null;
  const { PrismaClient } = require("@prisma/client");
  const prisma = new PrismaClient();
  try {
    const [
      warehouseProducts,
      ozonProducts,
      productLinks,
      linkedProducts,
      managedSuppliers,
      appSettings,
      retryItems,
      priceHistory,
      snapshotItems,
      latestProduct,
    ] = await Promise.all([
      prisma.warehouseProduct.count(),
      prisma.warehouseProduct.count({ where: { marketplace: "ozon" } }),
      prisma.productLink.count(),
      prisma.warehouseProduct.count({ where: { links: { some: {} } } }),
      prisma.managedSupplier.count(),
      prisma.appSetting.count(),
      prisma.priceRetryQueueItem.count(),
      prisma.priceHistory.count(),
      prisma.priceMasterSnapshotItem.count(),
      prisma.warehouseProduct.findFirst({
        orderBy: { updatedAt: "desc" },
        select: { id: true, offerId: true, name: true, updatedAt: true, _count: { select: { links: true } } },
      }),
    ]);
    return {
      databaseUrl: redactDatabaseUrl(process.env.DATABASE_URL),
      warehouseProducts,
      ozonProducts,
      productLinks,
      linkedProducts,
      managedSuppliers,
      appSettings,
      retryItems,
      priceHistory,
      snapshotItems,
      latestProduct,
    };
  } finally {
    await prisma.$disconnect();
  }
}

async function main() {
  const warehouse = readJson("personal-warehouse.json", { products: [], suppliers: [] });
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const jsonSummary = {
    products: products.length,
    ozonProducts: products.filter((product) => String(product.marketplace || "").toLowerCase() === "ozon").length,
    productLinks: products.reduce((total, product) => total + (Array.isArray(product.links) ? product.links.length : 0), 0),
    linkedProducts: products.filter((product) => Array.isArray(product.links) && product.links.length).length,
    managedSuppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
    updatedAt: warehouse.updatedAt || null,
  };

  const postgres = await inspectPostgres();
  console.log(JSON.stringify({
    env: {
      dbMode: process.env.DB_MODE || null,
      jsonFallbackEnabled: process.env.JSON_FALLBACK_ENABLED || null,
      bullmqEnabled: process.env.BULLMQ_ENABLED || null,
    },
    json: jsonSummary,
    postgres,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
