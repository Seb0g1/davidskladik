#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const {
  productFromPostgres,
  mergeProducts,
  warehouseProductCanonicalId,
  upsertWarehouseProductPostgres,
  replaceProductLinksInPostgres,
  warehouseLinkIdentityKey,
} = require("../server.js");

function argValue(name, fallback = "") {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : fallback;
}

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const limit = Math.max(1, Number(argValue("--limit", "5000")) || 5000);

function linkIdentity(link = {}) {
  return warehouseLinkIdentityKey(link);
}

function scoreWarehouseProductRow(row = {}) {
  const product = productFromPostgres(row);
  const canonicalId = warehouseProductCanonicalId(product);
  let score = 0;
  if (product.id === canonicalId) score += 1_000_000;
  score += (row.links?.length || 0) * 1_000;
  score += row.updatedAt ? new Date(row.updatedAt).getTime() : 0;
  return score;
}

function mergeDuplicateRows(rows = []) {
  let merged = null;
  for (const row of rows) {
    const product = productFromPostgres(row);
    merged = mergeProducts(merged ? [merged] : [], [product])[0] || product;
  }
  const canonicalId = warehouseProductCanonicalId(merged);
  merged.id = canonicalId;
  return merged;
}

function dedupeLinksForProduct(rows = [], canonicalId) {
  const byIdentity = new Map();
  for (const row of rows) {
    for (const link of row.links || []) {
      const normalized = {
        id: link.id,
        article: link.supplierArticle,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        keyword: link.keyword,
        priceCurrency: link.priceCurrency,
        createdAt: link.createdAt,
        updatedAt: link.updatedAt,
        raw: link.raw,
      };
      const identity = linkIdentity(normalized);
      const current = byIdentity.get(identity);
      if (!current || (link.updatedAt && (!current.updatedAt || new Date(link.updatedAt) > new Date(current.updatedAt)))) {
        byIdentity.set(identity, normalized);
      }
    }
  }
  return Array.from(byIdentity.values()).map((link) => ({
    ...link,
    productId: canonicalId,
  }));
}

async function findDuplicateGroups(prisma) {
  return prisma.$queryRaw`
    SELECT
      marketplace::text AS marketplace,
      COALESCE(target, '') AS target,
      LOWER(offer_id) AS offer_key,
      COUNT(*)::int AS cnt,
      array_agg(id ORDER BY updated_at DESC) AS ids
    FROM warehouse_products
    GROUP BY marketplace, COALESCE(target, ''), LOWER(offer_id)
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
    LIMIT ${limit}
  `;
}

async function reassignProductReferences(tx, fromIds, toId) {
  const ids = fromIds.filter((id) => id && id !== toId);
  if (!ids.length) return;
  await tx.priceHistory.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.priceRetryQueueItem.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.salesAutomationSkuState.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.ozonUnarchiveQueueItem.updateMany({ where: { productId: { in: ids } }, data: { productId: toId } });
  await tx.brandIndexItem.deleteMany({ where: { productId: { in: ids } } });
}

async function mergeDuplicateGroup(prisma, rows) {
  if (rows.length < 2) return { merged: 0, deleted: 0, canonicalId: "", offerId: "" };
  const canonicalId = warehouseProductCanonicalId(productFromPostgres(rows.sort((a, b) => scoreWarehouseProductRow(b) - scoreWarehouseProductRow(a))[0]));
  const merged = mergeDuplicateRows(rows);
  merged.links = dedupeLinksForProduct(rows, canonicalId).map((link) => ({
    id: link.id,
    article: link.article,
    supplierName: link.supplierName,
    partnerId: link.partnerId,
    keyword: link.keyword,
    priceCurrency: link.priceCurrency,
    createdAt: link.createdAt,
    updatedAt: link.updatedAt,
    ...(link.raw && typeof link.raw === "object" ? link.raw : {}),
  }));
  const deleteIds = rows.map((row) => row.id).filter((id) => id !== canonicalId);
  const offerId = merged.offerId || rows[0]?.offerId || "";

  if (dryRun || !apply) {
    return {
      merged: rows.length - 1,
      deleted: deleteIds.length,
      canonicalId,
      offerId,
      dryRun: true,
    };
  }

  await prisma.$transaction(async (tx) => {
    const allIds = rows.map((row) => row.id);
    await upsertWarehouseProductPostgres(tx, merged);
    await reassignProductReferences(tx, allIds, canonicalId);
    if (deleteIds.length) {
      await tx.warehouseProduct.deleteMany({ where: { id: { in: deleteIds } } });
    }
    await replaceProductLinksInPostgres(tx, merged);
  }, { timeout: 120_000 });

  return {
    merged: rows.length - 1,
    deleted: deleteIds.length,
    canonicalId,
    offerId,
  };
}

async function run() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set.");
  }
  if (!dryRun && !apply) {
    throw new Error("Pass --dry-run to preview or --apply to merge duplicates.");
  }

  const prisma = new PrismaClient();
  try {
    const groups = await findDuplicateGroups(prisma);
    const summary = {
      ok: true,
      dryRun: dryRun || !apply,
      duplicateGroups: groups.length,
      mergedProducts: 0,
      deletedProducts: 0,
      examples: [],
    };

    for (const group of groups) {
      const ids = Array.isArray(group.ids) ? group.ids : [];
      const rows = await prisma.warehouseProduct.findMany({
        where: { id: { in: ids } },
        include: { links: true },
      });
      if (rows.length < 2) continue;
      const result = await mergeDuplicateGroup(prisma, rows);
      summary.mergedProducts += result.merged;
      summary.deletedProducts += result.deleted;
      if (summary.examples.length < 12) {
        summary.examples.push({
          marketplace: group.marketplace,
          target: group.target,
          offerId: result.offerId,
          count: group.cnt,
          canonicalId: result.canonicalId,
          deleted: result.deleted,
        });
      }
    }

    console.log(JSON.stringify(summary, null, 2));
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
