"use strict";

// Unit tests for supplierLedgerSummaryFromEntries (server/parts/02d-finance-supplier-ledger.js).
// The part file is evaluated standalone in a VM context with tiny stubs for its helpers.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-finance-supplier-ledger.js"), "utf8");
const ctx = vm.createContext({
  process: { env: {} },
  cleanText: (value) => String(value ?? "").trim(),
  normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
  normalizeFinanceMoney: (value, fallback = 0) => {
    const n = Number(value ?? fallback);
    return Number.isFinite(n) ? Number(n.toFixed(2)) : Number(fallback || 0);
  },
  resolvePickingRowCurrency: (row) => String(row.priceCurrency || "USD").toUpperCase(),
});
vm.runInContext(`${source}
this.api = { supplierLedgerSummaryFromEntries };`, ctx);
const summarize = ctx.api.supplierLedgerSummaryFromEntries;

let seq = 0;
const at = (day, time = "12:00") => `2026-09-${String(day).padStart(2, "0")}T${time}:00.000Z`;
const debt = (usd, { rate = 93, day = 10, key, partnerId = "109", pricePaidRub } = {}) => ({
  id: `d${++seq}`,
  entryType: "purchase_debt",
  partnerId,
  supplierName: "Виталий (приносит)",
  amount: -(pricePaidRub || Math.round(usd * rate)),
  currency: "RUB",
  pickingKey: key || `pk${seq}`,
  status: "active",
  occurredAt: at(day),
  raw: { picking: { price: usd, quantity: 1, priceCurrency: "USD", pricePaidRub }, usdRate: rate },
});
const payment = (amount, currency = "USD", { day = 11, rate, partnerId = "109" } = {}) => ({
  id: `p${++seq}`,
  entryType: "payment",
  partnerId,
  supplierName: "Виталий (приносит)",
  amount,
  currency,
  status: "active",
  occurredAt: at(day),
  raw: rate ? { usdRate: rate } : {},
});

test("USD supplier: a 10.55 $ payment raises the balance by exactly 10.55 $", () => {
  const entries = [debt(300), debt(145.83), payment(456.1)];
  const before = summarize(entries, { usdRate: 93 });
  const after = summarize([...entries, payment(10.55, "USD", { day: 21 })], { usdRate: 93 });
  assert.equal(before.balanceUsd, 10.27);
  assert.equal(after.balanceUsd, 20.82);
  assert.equal(after.paidTotalUsdEquiv, 466.65);
  assert.equal(after.debtTotalUsd, 445.83);
});

test("USD balance does not drift when today's rate changes", () => {
  const entries = [debt(100, { rate: 90 }), payment(4500, "RUB", { rate: 90 }), payment(60)];
  const a = summarize(entries, { usdRate: 90 });
  const b = summarize(entries, { usdRate: 110 });
  assert.equal(a.balanceUsd, 10);
  assert.equal(b.balanceUsd, 10);
});

test("debt with an actual RUB amount paid is valued by that amount", () => {
  const s = summarize([debt(100, { rate: 100, pricePaidRub: 9000 })], { usdRate: 100 });
  assert.equal(s.debtTotalUsd, 90);
  assert.equal(s.balanceRub, -9000);
});

test("legacy 'Свести' entry sets the balance to its RUB target", () => {
  const legacy = {
    id: "c1", entryType: "balance_correction", partnerId: "109", amount: -1234.5, currency: "RUB", status: "active",
    occurredAt: at(15), raw: { source: "balance_correction", currentBalance: 99, targetBalance: 930, delta: -1234.5 },
  };
  const s = summarize([debt(100), payment(50), legacy, payment(5, "USD", { day: 16 })], { usdRate: 93 });
  assert.equal(s.balanceUsd, 15);
  assert.equal(s.balanceRub, 1395);
});

test("new corrections are plain deltas in their own currency", () => {
  const corr = {
    id: "c2", entryType: "balance_correction", partnerId: "109", amount: 7.5, currency: "USD", status: "active",
    occurredAt: at(15), raw: { source: "balance_correction", mode: "delta", targetBalance: -42.5, usdRate: 93 },
  };
  const s = summarize([debt(100), payment(50), corr], { usdRate: 93 });
  assert.equal(s.balanceUsd, -42.5);
  assert.equal(s.correctionsTotalUsd, 7.5);
});

test("full picking return cancels its debt exactly; partial return uses its own amount", () => {
  const d = debt(33.33, { key: "row-1", rate: 91 });
  const fullReturn = { id: "r1", entryType: "supplier_return", partnerId: "109", amount: Math.abs(d.amount), currency: "RUB", pickingKey: "row-1", status: "active", occurredAt: at(12), raw: { usdRate: 95 } };
  assert.equal(summarize([d, fullReturn], { usdRate: 95 }).balanceUsd, 0);
  const partial = { ...fullReturn, id: "r2", amount: 950 };
  assert.equal(summarize([d, partial], { usdRate: 95 }).balanceUsd, -23.33);
});

test("voided entries are ignored", () => {
  const s = summarize([debt(10), { ...payment(10), status: "voided" }], { usdRate: 93 });
  assert.equal(s.balanceUsd, -10);
  assert.equal(s.entries, 1);
});

test("RUB supplier balance stays in rubles", () => {
  const rubDebt = {
    id: "rd", entryType: "purchase_debt", supplierName: "Инна", amount: -5000, currency: "RUB", status: "active",
    occurredAt: at(10), raw: { picking: { price: 5000, quantity: 1, priceCurrency: "RUB" } },
  };
  const s = summarize([rubDebt, { ...payment(3000, "RUB"), partnerId: "", supplierName: "Инна" }], { usdRate: 100 });
  assert.equal(s.balanceRub, -2000);
  assert.equal(s.balance, -2000);
  assert.equal(s.balanceUsd, -20);
});

test("perSupplier: a legacy checkpoint only resets its own supplier", () => {
  const legacy = {
    id: "c3", entryType: "balance_correction", partnerId: "109", amount: 1, currency: "RUB", status: "active",
    occurredAt: at(15), raw: { targetBalance: 0 },
  };
  const other = debt(20, { partnerId: "200", day: 9 });
  const s = summarize([debt(100), other, legacy], { usdRate: 93, perSupplier: true });
  assert.equal(s.balanceUsd, -20);
});
