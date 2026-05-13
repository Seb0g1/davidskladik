#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

function argEnabled(name) {
  return process.argv.includes(name);
}

const dryRun = argEnabled("--dry-run");

function cleanText(value) {
  return String(value ?? "").trim();
}

function productGroupKeys(product = {}) {
  const marketplace = cleanText(product.marketplace).toLowerCase();
  const productId = cleanText(product.productId).toLowerCase();
  const offerId = cleanText(product.offerId).toLowerCase();
  const keys = [];
  if (marketplace && productId) keys.push(`${marketplace}:product:${productId}`);
  if (marketplace && offerId) keys.push(`${marketplace}:offer:${offerId}`);
  return keys;
}

function linkIdentity(link = {}) {
  return [
    cleanText(link.supplierArticle).toLowerCase(),
    cleanText(link.partnerId),
    cleanText(link.supplierName).toLowerCase(),
    cleanText(link.keyword).toLowerCase(),
    cleanText(link.priceCurrency || "USD").toUpperCase(),
  ].join("|");
}

function chooseKeeper(products = []) {
  return [...products].sort((a, b) => {
    const linkDiff = (b.links?.length || 0) - (a.links?.length || 0);
    if (linkDiff) return linkDiff;
    const bUpdated = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
    const aUpdated = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
    return bUpdated - aUpdated;
  })[0];
}

function chooseFreshest(products = []) {
  return [...products].sort((a, b) => {
    const bUpdated = b.updatedAt ? new Date(b.updatedAt).getTime() : 0;
    const aUpdated = a.updatedAt ? new Date(a.updatedAt).getTime() : 0;
    return bUpdated - aUpdated;
  })[0];
}

async function mergeDuplicateGroup(prisma, products) {
  if (products.length < 2) return { merged: 0, movedLinks: 0, deletedLinks: 0 };
  const keeper = chooseKeeper(products);
  const freshest = chooseFreshest(products);
  const duplicates = products.filter((product) => product.id !== keeper.id);
  const existingKeeperLinks = new Set((keeper.links || []).map(linkIdentity));
  let movedLinks = 0;
  let deletedLinks = 0;

  await prisma.$transaction(async (tx) => {
    if (freshest && freshest.id !== keeper.id) {
      await tx.warehouseProduct.update({
        where: { id: keeper.id },
        data: {
          marketplace: freshest.marketplace,
          target: freshest.target,
          offerId: freshest.offerId,
          productId: freshest.productId,
          name: freshest.name,
          brand: freshest.brand,
          images: freshest.images,
          marketplaceState: freshest.marketplaceState,
          currentPrice: freshest.currentPrice,
          targetPrice: keeper.targetPrice ?? freshest.targetPrice,
          targetStock: keeper.targetStock ?? freshest.targetStock,
          status: freshest.status,
          archived: freshest.archived,
          raw: {
            ...(freshest.raw && typeof freshest.raw === "object" ? freshest.raw : {}),
            links: (keeper.links || []).map((link) => ({
              id: link.id,
              article: link.supplierArticle,
              supplierName: link.supplierName,
              partnerId: link.partnerId,
              keyword: link.keyword,
              priceCurrency: link.priceCurrency,
              createdAt: link.createdAt,
            })),
          },
        },
      });
    }

    for (const duplicate of duplicates) {
      for (const link of duplicate.links || []) {
        const identity = linkIdentity(link);
        if (existingKeeperLinks.has(identity)) {
          await tx.productLink.delete({ where: { id: link.id } });
          deletedLinks += 1;
          continue;
        }
        await tx.productLink.update({
          where: { id: link.id },
          data: { productId: keeper.id },
        });
        existingKeeperLinks.add(identity);
        movedLinks += 1;
      }
      await tx.warehouseProduct.delete({ where: { id: duplicate.id } });
    }
  }, { timeout: 60_000 });

  return { merged: duplicates.length, movedLinks, deletedLinks };
}

async function run() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set. Repair requires PostgreSQL mode.");
  }
  const prisma = new PrismaClient();
  try {
    const products = await prisma.warehouseProduct.findMany({
      include: { links: true },
      orderBy: { updatedAt: "desc" },
    });
    const groupsByKey = new Map();
    for (const product of products) {
      for (const key of productGroupKeys(product)) {
        if (!groupsByKey.has(key)) groupsByKey.set(key, new Set());
        groupsByKey.get(key).add(product.id);
      }
    }
    const productById = new Map(products.map((product) => [product.id, product]));
    const duplicateGroups = [];
    const seenSignatures = new Set();
    for (const ids of groupsByKey.values()) {
      if (ids.size < 2) continue;
      const sortedIds = Array.from(ids).sort();
      const signature = sortedIds.join("|");
      if (seenSignatures.has(signature)) continue;
      seenSignatures.add(signature);
      duplicateGroups.push(sortedIds.map((id) => productById.get(id)).filter(Boolean));
    }

    if (dryRun) {
      console.log(JSON.stringify({
        ok: true,
        dryRun: true,
        duplicateGroups: duplicateGroups.length,
        examples: duplicateGroups.slice(0, 10).map((group) => group.map((product) => ({
          id: product.id,
          target: product.target,
          offerId: product.offerId,
          productId: product.productId,
          links: product.links.length,
          updatedAt: product.updatedAt,
        }))),
      }, null, 2));
      return;
    }

    const summary = { duplicateGroups: duplicateGroups.length, mergedProducts: 0, movedLinks: 0, deletedLinks: 0 };
    for (const group of duplicateGroups) {
      const result = await mergeDuplicateGroup(prisma, group);
      summary.mergedProducts += result.merged;
      summary.movedLinks += result.movedLinks;
      summary.deletedLinks += result.deletedLinks;
    }
    console.log(JSON.stringify({ ok: true, ...summary }, null, 2));
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
