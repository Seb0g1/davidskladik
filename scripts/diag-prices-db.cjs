#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  const today = new Date(); today.setHours(0, 0, 0, 0);

  // --- Yandex price_history today ---
  const yandexToday = await prisma.priceHistory.count({ where: { marketplace: "yandex", createdAt: { gte: today } } });
  const yandexRecent = await prisma.priceHistory.findMany({
    where: { marketplace: "yandex" },
    orderBy: { createdAt: "desc" }, take: 5,
    select: { offerId: true, newPrice: true, createdAt: true, status: true },
  });
  console.log("=== YANDEX price_history ===");
  console.log("Entries today:", yandexToday);
  for (const r of yandexRecent) {
    console.log(" ", r.offerId, "=", r.newPrice, "RUB", new Date(r.createdAt).toISOString(), "status=" + r.status);
  }

  // --- Ozon-1 price_history today ---
  const ozon1Today = await prisma.priceHistory.count({ where: { marketplace: "ozon", createdAt: { gte: today } } });
  const ozon1Recent = await prisma.priceHistory.findMany({
    where: { marketplace: "ozon" },
    orderBy: { createdAt: "desc" }, take: 5,
    select: { offerId: true, newPrice: true, createdAt: true, status: true, target: true },
  });
  console.log("\n=== OZON price_history (всё) ===");
  console.log("Entries today:", ozon1Today);
  for (const r of ozon1Recent) {
    console.log(" ", r.offerId, "=", r.newPrice, "RUB", new Date(r.createdAt).toISOString(), "target=" + r.target, "status=" + r.status);
  }

  // --- Ozon-2 products ---
  const ozon2Count = await prisma.warehouseProduct.count({ where: { marketplace: "ozon", archived: false, target: { contains: "3d10ec43" } } });
  const ozon2Recent2 = await prisma.warehouseProduct.findMany({
    where: { marketplace: "ozon", archived: false, target: { contains: "3d10ec43" } },
    orderBy: { updatedAt: "desc" }, take: 5,
    select: { offerId: true, updatedAt: true, target: true },
  });
  console.log("\n=== OZON-2 (3d10ec43) products ===");
  console.log("Total active:", ozon2Count);
  for (const r of ozon2Recent2) {
    console.log(" ", r.offerId, "target=" + r.target, "updated=" + new Date(r.updatedAt).toISOString());
  }

  // --- priceRetryQueueItem counts via ORM ---
  const retryGroups = await prisma.priceRetryQueueItem.groupBy({
    by: ["marketplace", "target", "status"],
    _count: { id: true },
    orderBy: [{ marketplace: "asc" }, { status: "asc" }],
  });
  console.log("\n=== priceRetryQueueItem (retry queue) ===");
  for (const r of retryGroups) console.log(" ", r.marketplace, r.target || "-", "/", r.status, ":", r._count.id);

  // Sample pending items via ORM
  try {
    const pendingSample = await prisma.priceRetryQueueItem.findMany({
      where: { status: "pending" },
      orderBy: { updatedAt: "desc" },
      take: 10,
      select: { marketplace: true, target: true, offerId: true, price: true, lastError: true, attempts: true, updatedAt: true },
    });
    console.log("\nPending retry items (" + pendingSample.length + "):");
    for (const r of pendingSample) {
      console.log(" ", r.marketplace, r.target || "-", r.offerId, "price=" + r.price, "attempts=" + r.attempts, "err=" + String(r.lastError || "").slice(0, 80));
    }
    // Count by target for ozon-2
    const ozon2Pending = await prisma.priceRetryQueueItem.count({ where: { target: { contains: "3d10ec43" } } });
    console.log("\nOzon-2 retry items (all statuses):", ozon2Pending);
  } catch (e2) {
    console.log("retry queue error:", e2.message.slice(0, 200));
  }
}

main().then(() => prisma.$disconnect()).catch(e => { console.error(e.message); process.exit(1); });
