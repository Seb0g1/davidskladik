#!/usr/bin/env node
"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");

const root = path.resolve(__dirname, "..");
const dataDir = path.join(root, "data");
const prisma = new PrismaClient();

function text(value) {
  return String(value ?? "").trim();
}

function date(value) {
  const parsed = value ? new Date(value) : null;
  return parsed && Number.isFinite(parsed.getTime()) ? parsed : null;
}

async function readJson(name, fallback) {
  try {
    const raw = await fs.readFile(path.join(dataDir, name), "utf8");
    return raw.trim() ? JSON.parse(raw) : fallback;
  } catch (error) {
    if (error.code === "ENOENT") return fallback;
    throw error;
  }
}

function ozonQueueKey(item = {}) {
  return [item.target, item.productId || item.product_id, item.offerId || item.offer_id, item.id || item.productUuid]
    .map(text)
    .filter(Boolean)
    .join(":");
}

async function migrateOzonQueue() {
  const payload = await readJson("ozon-unarchive-queue.json", { items: [] });
  let count = 0;
  for (const item of Array.isArray(payload.items) ? payload.items : []) {
    const queueKey = ozonQueueKey(item);
    const offerId = text(item.offerId || item.offer_id || item.id);
    if (!queueKey || !offerId) continue;
    await prisma.ozonUnarchiveQueueItem.upsert({
      where: { queueKey },
      create: {
        queueKey,
        productId: text(item.productId || item.product_id || item.id) || null,
        offerId,
        target: text(item.target) || null,
        status: ["pending", "processing", "failed", "delayed"].includes(text(item.status)) ? text(item.status) : "pending",
        queuedAt: date(item.queuedAt || item.queued_at) || new Date(),
        nextRetryAt: date(item.nextRetryAt || item.next_retry_at),
        lastAttemptAt: date(item.lastAttemptAt || item.last_attempt_at),
        attempts: Math.max(0, Number(item.attempts || 0) || 0),
        warning: text(item.warning) || null,
        error: text(item.error) || null,
        raw: item,
      },
      update: {
        nextRetryAt: date(item.nextRetryAt || item.next_retry_at),
        attempts: Math.max(0, Number(item.attempts || 0) || 0),
        warning: text(item.warning) || null,
        error: text(item.error) || null,
        raw: item,
      },
    });
    count += 1;
  }
  return count;
}

async function migrateSupplierCart() {
  const payload = await readJson("supplier-cart-state.json", {});
  let draftRows = 0;
  if (payload.draft && Array.isArray(payload.draft.rows)) {
    const draftId = text(payload.draft.id) || `json-${Date.now()}`;
    await prisma.supplierCartDraft.updateMany({ where: { active: true, id: { not: draftId } }, data: { active: false } });
    await prisma.supplierCartDraft.upsert({
      where: { id: draftId },
      create: {
        id: draftId,
        generatedAt: date(payload.draft.generatedAt) || new Date(),
        generatedBy: text(payload.draft.generatedBy) || null,
        marketplace: text(payload.draft.params?.marketplace || "all") || "all",
        from: date(payload.draft.params?.from),
        to: date(payload.draft.params?.to),
        summary: payload.draft.summary || {},
        params: payload.draft.params || {},
        active: true,
      },
      update: {
        generatedAt: date(payload.draft.generatedAt) || new Date(),
        summary: payload.draft.summary || {},
        params: payload.draft.params || {},
        active: true,
      },
    });
    await prisma.supplierCartDraftRow.deleteMany({ where: { draftId } });
    for (const row of payload.draft.rows) {
      const cartKey = text(row.key);
      if (!cartKey) continue;
      await prisma.supplierCartDraftRow.create({
        data: {
          draftId,
          cartKey,
          marketplace: text(row.marketplace) || null,
          accountName: text(row.accountName) || null,
          orderId: text(row.orderId) || null,
          postingNumber: text(row.postingNumber) || null,
          offerId: text(row.offerId) || null,
          productName: text(row.productName) || null,
          quantity: Math.max(1, Number(row.quantity || 1) || 1),
          supplierName: text(row.supplierName) || null,
          partnerId: text(row.partnerId) || null,
          offerRowId: text(row.offerRowId) || null,
          price: Number(row.price || 0) || null,
          priceCurrency: text(row.priceCurrency) || null,
          supplierScore: Number(row.supplierScore || 0) || null,
          ready: row.ready === true,
          alreadyCommitted: row.alreadyCommitted === true,
          skipReason: text(row.skipReason) || null,
          requestDocId: text(row.requestDocId) || null,
          requestRowId: text(row.requestRowId) || null,
          raw: row,
        },
      });
      draftRows += 1;
    }
  }
  let blocks = 0;
  for (const block of Object.values(payload.supplierBlocks || {})) {
    const blockKey = text(block.key || `${text(block.offerId).toLowerCase()}|${text(block.partnerId).toLowerCase()}`);
    if (!blockKey || !text(block.offerId) || !text(block.partnerId) || !date(block.expiresAt)) continue;
    await prisma.supplierBlock.upsert({
      where: { blockKey },
      create: {
        blockKey,
        offerId: text(block.offerId),
        partnerId: text(block.partnerId),
        supplierName: text(block.supplierName) || null,
        reason: text(block.reason) || null,
        sourceKey: text(block.sourceKey) || null,
        blockedBy: text(block.blockedBy) || null,
        blockedAt: date(block.blockedAt) || new Date(),
        expiresAt: date(block.expiresAt),
        active: block.active !== false,
        raw: block,
      },
      update: { expiresAt: date(block.expiresAt), active: block.active !== false, raw: block },
    });
    blocks += 1;
  }
  return { draftRows, blocks };
}

async function migratePicking() {
  const payload = await readJson("supplier-picking-list.json", { rows: {} });
  let rows = 0;
  for (const row of Object.values(payload.rows || {})) {
    const pickingKey = text(row.key);
    if (!pickingKey) continue;
    await prisma.supplierPickingRow.upsert({
      where: { pickingKey },
      create: {
        pickingKey,
        marketplace: text(row.marketplace) || null,
        accountName: text(row.accountName) || null,
        orderId: text(row.orderId) || null,
        postingNumber: text(row.postingNumber) || null,
        offerId: text(row.offerId) || null,
        productName: text(row.productName) || null,
        quantity: Math.max(1, Number(row.quantity || 1) || 1),
        supplierName: text(row.supplierName) || null,
        partnerId: text(row.partnerId) || null,
        offerRowId: text(row.offerRowId) || null,
        price: Number(row.price || 0) || null,
        priceCurrency: text(row.priceCurrency) || null,
        trustFactor: Math.max(0, Math.min(100, Number(row.trustFactor || 100) || 100)),
        orderCutoffTime: text(row.orderCutoffTime) || null,
        reseller: row.reseller === true,
        supplierScore: Number(row.supplierScore || 0) || null,
        requestDocId: text(row.requestDocId) || null,
        requestRowId: text(row.requestRowId) || null,
        status: text(row.status || "open"),
        createdBy: text(row.createdBy) || null,
        pickedBy: text(row.pickedBy) || null,
        pickedAt: date(row.pickedAt),
        missingBy: text(row.missingBy) || null,
        missingAt: date(row.missingAt),
        missingReason: text(row.missingReason) || null,
        nextRetryAt: date(row.nextRetryAt),
        replacementFor: text(row.replacementFor) || null,
        replacementKey: text(row.replacementKey) || null,
        raw: row,
      },
      update: {
        status: text(row.status || "open"),
        pickedBy: text(row.pickedBy) || null,
        pickedAt: date(row.pickedAt),
        missingBy: text(row.missingBy) || null,
        missingAt: date(row.missingAt),
        missingReason: text(row.missingReason) || null,
        replacementFor: text(row.replacementFor) || null,
        replacementKey: text(row.replacementKey) || null,
        raw: row,
      },
    });
    rows += 1;
  }
  return rows;
}

async function migrateFinance() {
  const payload = await readJson("finance-state.json", { orders: [], expenses: [] });
  let orders = 0;
  let expenses = 0;
  for (const row of Array.isArray(payload.orders) ? payload.orders : []) {
    const id = text(row.id);
    const orderId = text(row.orderId || row.order_id || row.postingNumber || row.posting_number);
    if (!id || !orderId) continue;
    const marketplaceText = text(row.marketplace).toLowerCase();
    const marketplace = marketplaceText === "ozon" || marketplaceText === "yandex" ? marketplaceText : null;
    const money = (value) => {
      if (value === undefined || value === null || value === "") return null;
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    };
    const profit = money(row.profitAmount ?? row.profit_amount)
      ?? Number((Number(row.payoutAmount ?? row.payout_amount ?? row.saleAmount ?? row.sale_amount ?? 0)
        - Number(row.purchaseCost ?? row.purchase_cost ?? 0)
        - Number(row.feesAmount ?? row.fees_amount ?? 0)
        - Number(row.taxAmount ?? row.tax_amount ?? 0)
        - Number(row.penaltiesAmount ?? row.penalties_amount ?? 0)
        - Number(row.refundsAmount ?? row.refunds_amount ?? 0)).toFixed(2));
    await prisma.financeOrder.upsert({
      where: { id },
      create: {
        id,
        marketplace,
        target: text(row.target) || null,
        orderId,
        postingNumber: text(row.postingNumber || row.posting_number) || null,
        offerId: text(row.offerId || row.offer_id) || null,
        productName: text(row.productName || row.product_name || row.name) || null,
        quantity: Math.max(1, Number(row.quantity || 1) || 1),
        saleAmount: money(row.saleAmount ?? row.sale_amount),
        payoutAmount: money(row.payoutAmount ?? row.payout_amount),
        purchaseCost: money(row.purchaseCost ?? row.purchase_cost),
        feesAmount: money(row.feesAmount ?? row.fees_amount),
        taxAmount: money(row.taxAmount ?? row.tax_amount),
        penaltiesAmount: money(row.penaltiesAmount ?? row.penalties_amount),
        refundsAmount: money(row.refundsAmount ?? row.refunds_amount),
        profitAmount: profit,
        supplierName: text(row.supplierName || row.supplier_name) || null,
        partnerId: text(row.partnerId || row.partner_id) || null,
        source: text(row.source || "manual") || "manual",
        status: text(row.status || "open") || "open",
        soldAt: date(row.soldAt || row.sold_at),
        receivedAt: date(row.receivedAt || row.received_at),
        raw: row,
      },
      update: { raw: row, status: text(row.status || "open") || "open", profitAmount: profit },
    });
    orders += 1;
  }
  for (const row of Array.isArray(payload.expenses) ? payload.expenses : []) {
    const id = text(row.id);
    const amount = Number(row.amount || 0);
    if (!id || !(amount > 0)) continue;
    await prisma.financeExpense.upsert({
      where: { id },
      create: {
        id,
        type: text(row.type || "manual_purchase") || "manual_purchase",
        supplierName: text(row.supplierName || row.supplier_name) || null,
        partnerId: text(row.partnerId || row.partner_id) || null,
        offerId: text(row.offerId || row.offer_id) || null,
        productName: text(row.productName || row.product_name || row.name) || null,
        quantity: Math.max(1, Number(row.quantity || 1) || 1),
        amount,
        currency: text(row.currency || "RUB").toUpperCase() || "RUB",
        note: text(row.note) || null,
        source: text(row.source || "manual") || "manual",
        status: text(row.status || "confirmed") || "confirmed",
        spentAt: date(row.spentAt || row.spent_at) || new Date(),
        raw: row,
      },
      update: { raw: row, amount, status: text(row.status || "confirmed") || "confirmed" },
    });
    expenses += 1;
  }
  return { orders, expenses };
}

async function main() {
  const result = {
    ozonQueue: await migrateOzonQueue(),
    supplierCart: await migrateSupplierCart(),
    pickingRows: await migratePicking(),
    finance: await migrateFinance(),
  };
  console.log(JSON.stringify({ ok: true, result }, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
