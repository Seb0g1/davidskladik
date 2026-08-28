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
  isOzonPriceDiscountQuarantineError,
  needsOzonOldPriceEscalation,
  resolveOzonOldPrice,
  planOzonQuarantinePriceSteps,
  computeOzonQuarantineNextPrice,
  roundPrice,
  buildOzonPricePayload,
  getOzonAccountByTarget,
  sendOzonPricePayloadChunks,
  priceRetryQueueKey,
} = require("../server.js");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function isQuarantineRow(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const lastError = String(row.lastError || raw.detail || raw.error || "");
  return needsOzonOldPriceEscalation({ message: lastError }) || lastError.toLowerCase().includes("скидк");
}

async function prepareRetryQueueForQuarantine(rows = []) {
  const queue = await readPriceRetryQueue().catch(() => ({ items: [] }));
  const existing = new Map((queue.items || []).map((item) => [priceRetryQueueKey(item), item]));
  const now = new Date().toISOString();
  let added = 0;
  for (const row of rows) {
    if (!isQuarantineRow(row)) continue;
    const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
    const targetPrice = roundPrice(row.targetPrice || raw.lastRequestedPrice || raw.requestedPrice || 0);
    const cabinetPrice = roundPrice(row.currentPrice || raw.cabinetPriceAtSend || raw.currentPrice || 0);
    if (!row.productId || !targetPrice) continue;
    const stepPrice = computeOzonQuarantineNextPrice(cabinetPrice, targetPrice);
    const item = {
      id: row.productId,
      productId: row.productId,
      marketplace: "ozon",
      target: row.target,
      offerId: row.offerId,
      price: stepPrice,
      finalTargetPrice: targetPrice,
      cabinetPrice,
      oldPrice: resolveOzonOldPrice(stepPrice, {}),
      forceOldPrice: true,
      retryReason: "ozon_quarantine_step",
      status: "pending",
      queuedAt: now,
      nextRetryAt: now,
      attempts: 0,
      error: String(row.lastError || raw.detail || raw.error || ""),
    };
    existing.set(priceRetryQueueKey(item), item);
    added += 1;
  }
  const items = Array.from(existing.values()).slice(0, 5000);
  await writePriceRetryQueue({ items });
  return { added, total: items.length };
}

async function releaseDiscountQuarantineStaged(rows = []) {
  const account = getOzonAccountByTarget("ozon");
  const stepDelayMs = Math.max(1500, Number(process.env.OZON_QUARANTINE_STEP_DELAY_MS || 2500) || 2500);
  const outcomes = [];

  for (const row of rows) {
    const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
    const cabinet = roundPrice(row.currentPrice || raw.cabinetPriceAtSend || 0);
    const target = roundPrice(row.targetPrice || raw.lastRequestedPrice || 0);
    const steps = planOzonQuarantinePriceSteps(cabinet, target);
    const stepResults = [];
    let ok = true;

    for (const stepPrice of steps) {
      const payload = buildOzonPricePayload({
        offerId: row.offerId,
        price: stepPrice,
        forceOldPrice: true,
        oldPrice: resolveOzonOldPrice(stepPrice, {}),
      });
      const sent = await sendOzonPricePayloadChunks(account, [payload]);
      const fail = sent.failed[0];
      if (fail) {
        ok = false;
        stepResults.push({
          stepPrice,
          ok: false,
          error: fail.error?.message || "send_failed",
        });
        break;
      }
      stepResults.push({ stepPrice, ok: true });
      if (stepPrice !== steps[steps.length - 1]) await sleep(stepDelayMs);
    }

    outcomes.push({
      offerId: row.offerId,
      productId: row.productId,
      cabinet,
      target,
      steps,
      ok,
      stepResults,
    });
  }

  return outcomes;
}

async function main() {
  const rows = await loadFailedStates();
  const quarantine = rows.filter(isQuarantineRow);
  const discountQuarantine = quarantine.filter((row) => isOzonPriceDiscountQuarantineError({ message: row.lastError || "" }));
  const allIds = Array.from(new Set(rows.map((row) => String(row.productId || "").trim()).filter(Boolean)));
  const quarantineIds = Array.from(new Set(quarantine.map((row) => String(row.productId || "").trim()).filter(Boolean)));

  console.log(JSON.stringify({
    phase: "audit",
    failedOzon: rows.length,
    quarantine: quarantine.length,
    discountQuarantine: discountQuarantine.length,
    quarantineSamples: quarantine.slice(0, 8).map((row) => ({
      offerId: row.offerId,
      productId: row.productId,
      targetPrice: row.targetPrice,
      currentPrice: row.currentPrice,
      plannedSteps: planOzonQuarantinePriceSteps(row.currentPrice, row.targetPrice),
      lastError: String(row.lastError || "").slice(0, 120),
    })),
  }, null, 2));

  const queuePrep = await prepareRetryQueueForQuarantine(quarantine);
  console.log(JSON.stringify({ phase: "retry_queue_prepared", ...queuePrep }, null, 2));

  if (discountQuarantine.length) {
    console.log(JSON.stringify({
      phase: "staged_quarantine_release_start",
      count: discountQuarantine.length,
    }, null, 2));
    const staged = await releaseDiscountQuarantineStaged(discountQuarantine);
    const succeededIds = staged.filter((item) => item.ok).map((item) => item.productId).filter(Boolean);
    console.log(JSON.stringify({
      phase: "staged_quarantine_release",
      total: staged.length,
      ok: staged.filter((item) => item.ok).length,
      failed: staged.filter((item) => !item.ok).length,
      samples: staged.slice(0, 8),
    }, null, 2));

    if (succeededIds.length) {
      const refresh = await sendWarehousePrices({
        productIds: succeededIds,
        force: false,
        onlyChanged: false,
        refreshMarketplacePrices: true,
        livePriceMaster: false,
        verify: true,
        marketplace: "ozon",
        reason: "quarantine_state_refresh",
        sourceEvent: "quarantine_state_refresh",
      });
      console.log(JSON.stringify({
        phase: "quarantine_state_refresh",
        productIds: succeededIds.length,
        sent: refresh.sent,
        failed: refresh.failed,
        skipped: refresh.skipped,
      }, null, 2));
    }
  }

  const retryPass = await processPriceRetryQueue({ respectNextRetryAt: false, limit: 200, trigger: "quarantine_release" });
  console.log(JSON.stringify({ phase: "retry_queue_processed", ...retryPass }, null, 2));

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
  const afterQuarantine = after.filter(isQuarantineRow);
  console.log(JSON.stringify({
    phase: "after",
    failedOzon: after.length,
    quarantineRemaining: afterQuarantine.length,
    quarantineRemainingSamples: afterQuarantine.slice(0, 8).map((row) => ({
      offerId: row.offerId,
      targetPrice: row.targetPrice,
      currentPrice: row.currentPrice,
      lastError: String(row.lastError || "").slice(0, 120),
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error("FIX_PRICES_FAILED:", error.message);
  process.exit(1);
});
