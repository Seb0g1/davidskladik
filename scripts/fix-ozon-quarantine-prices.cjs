#!/usr/bin/env node
"use strict";

const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const { getPrisma } = require("../lib/postgres.js");
const {
  sendWarehousePrices,
  processPriceRetryQueue,
  readPriceRetryQueue,
  writePriceRetryQueue,
  needsOzonOldPriceEscalation,
  resolveOzonOldPrice,
  priceRetryQueueKey,
} = require("../server.js");

async function loadFailedStates() {
  const prisma = getPrisma();
  if (!prisma?.salesAutomationSkuState) return [];
  return prisma.salesAutomationSkuState.findMany({
    where: {
      marketplace: "ozon",
      reason: { in: ["api_error", "ozon_price_not_applied", "in_retry", "verification_pending", "pm_live_timeout"] },
    },
    orderBy: { updatedAt: "desc" },
    take: 5000,
  });
}

async function prepareRetryQueueForQuarantine(rows = []) {
  const queue = await readPriceRetryQueue().catch(() => ({ items: [] }));
  const existing = new Map((queue.items || []).map((item) => [priceRetryQueueKey(item), item]));
  const now = new Date().toISOString();
  let added = 0;
  for (const row of rows) {
    const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
    const lastError = String(row.lastError || raw.detail || raw.error || "");
    if (!needsOzonOldPriceEscalation({ message: lastError }) && !lastError.toLowerCase().includes("скидк")) continue;
    const targetPrice = Number(row.targetPrice || raw.lastRequestedPrice || raw.requestedPrice || 0);
    const cabinetPrice = Number(row.currentPrice || raw.cabinetPriceAtSend || raw.currentPrice || 0);
    if (!row.productId || !targetPrice) continue;
    const item = {
      id: row.productId,
      productId: row.productId,
      marketplace: "ozon",
      target: row.target,
      offerId: row.offerId,
      price: targetPrice,
      oldPrice: Math.max(cabinetPrice, resolveOzonOldPrice(targetPrice, { oldPrice: cabinetPrice })),
      forceOldPrice: true,
      retryReason: "ozon_price_quarantine_release",
      status: "pending",
      queuedAt: now,
      nextRetryAt: now,
      attempts: 0,
      error: lastError,
    };
    existing.set(priceRetryQueueKey(item), item);
    added += 1;
  }
  const items = Array.from(existing.values()).slice(0, 5000);
  await writePriceRetryQueue({ items });
  return { added, total: items.length };
}

async function main() {
  const rows = await loadFailedStates();
  const quarantine = rows.filter((row) => needsOzonOldPriceEscalation({ message: row.lastError || "" })
    || String(row.lastError || "").toLowerCase().includes("скидк"));
  const allIds = Array.from(new Set(rows.map((row) => String(row.productId || "").trim()).filter(Boolean)));
  const quarantineIds = Array.from(new Set(quarantine.map((row) => String(row.productId || "").trim()).filter(Boolean)));

  console.log(JSON.stringify({
    phase: "audit",
    failedOzon: rows.length,
    quarantine: quarantine.length,
    quarantineSamples: quarantine.slice(0, 8).map((row) => ({
      offerId: row.offerId,
      productId: row.productId,
      targetPrice: row.targetPrice,
      currentPrice: row.currentPrice,
      lastError: String(row.lastError || "").slice(0, 120),
    })),
  }, null, 2));

  const queuePrep = await prepareRetryQueueForQuarantine(quarantine);
  console.log(JSON.stringify({ phase: "retry_queue_prepared", ...queuePrep }, null, 2));

  const retryPass = await processPriceRetryQueue({ respectNextRetryAt: false, limit: 1000, trigger: "quarantine_release" });
  console.log(JSON.stringify({ phase: "retry_queue_processed", ...retryPass }, null, 2));

  if (quarantineIds.length) {
    const quarantinePush = await sendWarehousePrices({
      productIds: quarantineIds,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      verify: true,
      marketplace: "ozon",
      reason: "ozon_quarantine_release",
      sourceEvent: "ozon_quarantine_release",
    });
    console.log(JSON.stringify({
      phase: "quarantine_push",
      selected: quarantinePush.selected,
      sent: quarantinePush.sent,
      failed: quarantinePush.failed,
      queued: quarantinePush.queued,
      failedSamples: (quarantinePush.failedItems || []).slice(0, 10),
    }, null, 2));
  }

  const maxRepush = Math.max(10, Math.min(80, Number(process.env.QUARANTINE_FIX_MAX_REPUSH || 40) || 40));
  const repushIds = allIds.filter((id) => !quarantineIds.includes(id)).slice(0, maxRepush);
  if (repushIds.length) {
    const result = await sendWarehousePrices({
      productIds: repushIds,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: false,
      livePriceMaster: false,
      verify: true,
      marketplace: "ozon",
      reason: "failed_price_repush_lite",
      sourceEvent: "failed_price_repush_lite",
    });
    console.log(JSON.stringify({
      phase: "failed_repush_lite",
      cappedAt: maxRepush,
      totalFailed: allIds.length,
      sent: result.sent,
      failed: result.failed,
    }, null, 2));
  }

  const after = await loadFailedStates();
  const afterQuarantine = after.filter((row) => needsOzonOldPriceEscalation({ message: row.lastError || "" })
    || String(row.lastError || "").toLowerCase().includes("скидк"));
  console.log(JSON.stringify({
    phase: "after",
    failedOzon: after.length,
    quarantineRemaining: afterQuarantine.length,
    quarantineRemainingSamples: afterQuarantine.slice(0, 8).map((row) => ({
      offerId: row.offerId,
      targetPrice: row.targetPrice,
      lastError: String(row.lastError || "").slice(0, 120),
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error("FIX_PRICES_FAILED:", error.message);
  process.exit(1);
});
