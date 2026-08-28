#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  const total = await prisma.priceMasterSnapshotItem.count();
  const byPartner60 = await prisma.priceMasterSnapshotItem.count({ where: { partnerId: "60" } });
  const latest = await prisma.priceMasterSnapshotItem.findFirst({ orderBy: { updatedAt: "desc" } });
  const latestP60 = await prisma.priceMasterSnapshotItem.findFirst({ where: { partnerId: "60" }, orderBy: { updatedAt: "desc" } });

  console.log("Total pm_snapshot_items:", total);
  console.log("By partner 60 (Инна):", byPartner60);
  console.log("Latest item updatedAt:", latest?.updatedAt);
  console.log("Latest partner60 item:", JSON.stringify(latestP60, null, 2));

  // Check if row 2290296 exists in PM snapshot
  const row2290296 = await prisma.$queryRawUnsafe(
    "SELECT row_id, article, partner_id, native_name, price::float as price, active, doc_date::text FROM pm_snapshot_items WHERE row_id = $1",
    "2290296",
  );
  console.log("\nRow 2290296 in pm_snapshot_items:", JSON.stringify(row2290296, null, 2));

  const row2237266 = await prisma.$queryRawUnsafe(
    "SELECT row_id, article, partner_id, native_name, price::float as price, active, doc_date::text FROM pm_snapshot_items WHERE row_id = $1",
    "2237266",
  );
  console.log("Row 2237266 in pm_snapshot_items:", JSON.stringify(row2237266, null, 2));
}

main().catch((e) => console.error("FAILED:", e.message)).finally(() => prisma.$disconnect());
