#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const {
  buildWarehouseGroupDetailFromPostgres,
  resolveWarehouseProductTargetName,
  productFromPostgres,
  warehouseProductPageGroupKey,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

function parseArgs(argv) {
  const valueOf = (name, fallback) => {
    const prefix = `${name}=`;
    const match = argv.find((item) => item.startsWith(prefix));
    return match ? match.slice(prefix.length) : fallback;
  };
  return {
    limit: Math.max(1, Math.min(2000, Number(valueOf("--limit", 200)) || 200)),
    json: new Set(argv).has("--json"),
  };
}

function labelForProduct(product) {
  const account = resolveWarehouseProductTargetName(product);
  const base = String(product.marketplace || "").toLowerCase().includes("yandex") ? "Yandex" : "Ozon";
  return account && account.toLowerCase() !== String(product.marketplace || "").toLowerCase()
    ? `${base} · ${account}`
    : `${base} · ${product.offerId || product.target || product.id}`;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const prisma = getPrisma();
  if (!prisma) {
    console.error("Postgres is not configured.");
    process.exit(1);
  }

  const rows = await prisma.warehouseProduct.findMany({
    where: { links: { some: {} } },
    include: { links: true },
    orderBy: { updatedAt: "desc" },
    take: options.limit,
  });
  const byGroup = new Map();
  for (const row of rows) {
    const product = productFromPostgres(row);
    const groupKey = warehouseProductPageGroupKey(product);
    if (!groupKey) continue;
    if (!byGroup.has(groupKey)) byGroup.set(groupKey, []);
    byGroup.get(groupKey).push(product);
  }

  const duplicateGroups = [];
  for (const [groupKey, products] of byGroup.entries()) {
    if (products.length < 2) continue;
    const labels = products.map(labelForProduct);
    const unique = new Set(labels);
    const genericTargets = products.filter((product) => {
      const name = resolveWarehouseProductTargetName(product);
      return !name || ["ozon", "yandex", "yandex market"].includes(String(name).toLowerCase());
    });
    if (unique.size < products.length || genericTargets.length > 1) {
      duplicateGroups.push({
        groupKey,
        offerId: products[0]?.offerId || "",
        rows: products.length,
        uniqueLabels: unique.size,
        labels: Array.from(unique).sort(),
        genericTargetRows: genericTargets.length,
        sample: products.slice(0, 5).map((product) => ({
          id: product.id,
          marketplace: product.marketplace,
          target: product.target,
          targetName: resolveWarehouseProductTargetName(product),
          label: labelForProduct(product),
        })),
      });
    }
  }

  let checkedDetail = 0;
  let detailCollisions = 0;
  for (const item of duplicateGroups.slice(0, 20)) {
    const detail = await buildWarehouseGroupDetailFromPostgres(item.groupKey).catch(() => null);
    if (!detail?.products?.length) continue;
    checkedDetail += 1;
    const detailLabels = detail.products.map((product) => ({
      id: product.id,
      target: product.target,
      targetName: resolveWarehouseProductTargetName(product),
      label: labelForProduct(product),
    }));
    const uniqueDetail = new Set(detailLabels.map((entry) => entry.label));
    if (uniqueDetail.size < detailLabels.length) detailCollisions += 1;
    item.detail = detailLabels;
  }

  const report = {
    scannedProducts: rows.length,
    grouped: byGroup.size,
    duplicateLabelGroups: duplicateGroups.length,
    detailChecked: checkedDetail,
    detailCollisions,
    samples: duplicateGroups.slice(0, 15),
  };

  if (options.json) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  console.log(`Scanned products: ${report.scannedProducts}`);
  console.log(`Groups: ${report.grouped}`);
  console.log(`Groups with duplicate/generic marketplace labels: ${report.duplicateLabelGroups}`);
  console.log(`Group-detail collisions checked: ${report.detailCollisions}/${report.detailChecked}`);
  for (const item of report.samples.slice(0, 8)) {
    console.log(`- ${item.offerId || item.groupKey}: ${item.rows} rows, labels=${item.labels.join(", ")}`);
  }
}

main().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
