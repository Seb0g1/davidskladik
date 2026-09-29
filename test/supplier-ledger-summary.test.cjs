"use strict";

// Unit tests for supplierLedgerSummaryFromEntries (server/parts/02d-finance-supplier-ledger.js).
// The part file is evaluated standalone in a VM context with tiny stubs for its helpers.
// Model: every supplier is kept in its own currency (USD, RUB for Инна), no exchange rates.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-finance-supplier-ledger.js"), "utf8");
const isInna = (name) => /инна/i.test(String(name || ""));
const ctx = vm.createContext({
  process: { env: {} },
  cleanText: (value) => String(value ?? "").trim(),
  normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
  normalizeFinanceMoney: (value, fallback = 0) => {
    const n = Number(value ?? fallback);
    return Number.isFinite(n) ? Number(n.toFixed(2)) : Number(fallback || 0);
  },
  isInnaSupplierName: isInna,
  resolvePickingRowCurrency: (row) => (isInna(row.supplierName) ? "RUB" : String(row.priceCurrency || "USD").toUpperCase()),
});
vm.runInContext(`${source}
this.api = { supplierLedgerSummaryFromEntries, supplierLedgerSummaryAcrossSuppliers, supplierLedgerCurrencyFromIndex };`, ctx);
const { supplierLedgerSummaryFromEntries: summarize, supplierLedgerSummaryAcrossSuppliers: across } = ctx.api;

let seq = 0;
const at = (day, time = "12:00") => `2026-09-${String(day).padStart(2, "0")}T${time}:00.000Z`;
// Debts are stored in RUB (price × some rate) — the balance must ignore that and use the $ price.
const debt = (usd, { day = 10, key, partnerId = "34", supplierName = "Santa", rate = 93, qty = 1, pickedQuantity, pricePaid, pricePaidRub, priceCurrency = "USD" } = {}) => ({
  id: `d${++seq}`,
  entryType: "purchase_debt",
  partnerId,
  supplierName,
  amount: -Math.round(usd * rate * qty),
  currency: "RUB",
  pickingKey: key || `pk${seq}`,
  status: "active",
  occurredAt: at(day),
  raw: { picking: { price: usd, quantity: qty, pickedQuantity, priceCurrency, pricePaid, pricePaidRub } },
});
const payment = (amount, currency = "USD", { day = 11, partnerId = "34", supplierName = "Santa", raw = {} } = {}) => ({
  id: `p${++seq}`, entryType: "payment", partnerId, supplierName, amount, currency, status: "active", occurredAt: at(day), raw,
});

test("Santa: 1285 $ picked, 1076.43 $ paid → −208.57 $, independent of any rate", () => {
  const debts = [77, 7, 5, 34, 63, 119, 119, 8, 3, 23, 3, 4, 120, 57, 32, 3, 5, 399, 10, 52, 67, 75].map((usd, i) => debt(usd, { rate: 80 + i }));
  const s = summarize([...debts, payment(124), payment(283), payment(669.43)], { currency: "USD" });
  assert.equal(s.currency, "USD");
  assert.equal(s.debtTotal, 1285);
  assert.equal(s.paidTotal, 1076.43);
  assert.equal(s.balance, -208.57);
  assert.equal(s.balanceUsd, -208.57);
  assert.equal(s.balanceRub, 0);
});

test("a payment covers debt fully or partly, exactly by the amount entered", () => {
  const entries = [debt(100), debt(50)];
  assert.equal(summarize([...entries, payment(150)], { currency: "USD" }).balance, 0);
  assert.equal(summarize([...entries, payment(40)], { currency: "USD" }).balance, -110);
  assert.equal(summarize([...entries, payment(200)], { currency: "USD" }).balance, 50);
});

test("Инна is kept in rubles: price in ₽, payments in ₽", () => {
  const inna = { partnerId: "60", supplierName: "Инна" };
  const s = summarize([debt(5000, { ...inna, rate: 1, priceCurrency: "RUB" }), payment(3000, "RUB", inna)], { currency: "RUB" });
  assert.equal(s.balance, -2000);
  assert.equal(s.balanceRub, -2000);
  assert.equal(s.balanceUsd, 0);
});

test("actual price entered at «Собрал» and partial quantity define the debt", () => {
  assert.equal(summarize([debt(100, { pricePaid: 90 })], { currency: "USD" }).balance, -90);
  assert.equal(summarize([debt(10, { qty: 3, pickedQuantity: 2 })], { currency: "USD" }).balance, -20);
  // A legacy RUB «price paid» on a USD supplier is ignored — the $ price stands.
  assert.equal(summarize([debt(90, { pricePaidRub: 7830 })], { currency: "USD" }).balance, -90);
});

test("full return of a picked row cancels that row's debt exactly", () => {
  const d = debt(33.33, { key: "row-1", rate: 91 });
  const ret = { id: "r1", entryType: "supplier_return", partnerId: "34", supplierName: "Santa", amount: Math.abs(d.amount), currency: "RUB", pickingKey: "row-1", status: "active", occurredAt: at(12), raw: {} };
  assert.equal(summarize([d, ret], { currency: "USD" }).balance, 0);
  const partial = { ...ret, id: "r2", amount: 10, currency: "USD" };
  assert.equal(summarize([d, partial], { currency: "USD" }).balance, -23.33);
});

test("legacy 'Свести' sets the balance to its target in the supplier currency", () => {
  const legacy = {
    id: "c1", entryType: "balance_correction", partnerId: "34", amount: 63632, currency: "USD", status: "active",
    occurredAt: at(12), raw: { source: "balance_correction", currentBalance: -63613, targetBalance: 19 },
  };
  const s = summarize([debt(500, { day: 5 }), legacy, debt(10, { day: 14 })], { currency: "USD" });
  assert.equal(s.balance, 9);
});

test("legacy 'Свести' written in rubles for a dollar supplier converts its target, not reads it as dollars", () => {
  const legacy = {
    id: "c3", entryType: "balance_correction", partnerId: "34", amount: 209971.51, currency: "RUB", status: "active",
    occurredAt: at(14), raw: { source: "balance_correction", currentBalance: 4617.49, targetBalance: 9500 },
  };
  const s = summarize([debt(500, { day: 5 }), legacy], { currency: "USD" });
  assert.equal(s.balance, 100);
  assert.equal(s.foreignEntries, 1);
});

test("new corrections are plain deltas", () => {
  const corr = { id: "c2", entryType: "balance_correction", partnerId: "34", amount: 14.57, currency: "USD", status: "active", occurredAt: at(24), raw: { mode: "delta", targetBalance: 0 } };
  assert.equal(summarize([debt(14.57), corr], { currency: "USD" }).balance, 0);
});

test("voided entries are ignored", () => {
  const s = summarize([debt(10), { ...payment(10), status: "voided" }], { currency: "USD" });
  assert.equal(s.balance, -10);
  assert.equal(s.entries, 1);
});

test("currency is inferred from picked rows when unknown", () => {
  assert.equal(summarize([debt(10)]).currency, "USD");
  assert.equal(summarize([debt(500, { supplierName: "Инна", partnerId: "60", priceCurrency: "RUB", rate: 1 })]).currency, "RUB");
});

test("across suppliers: USD and RUB are never mixed; owed counts only debtors", () => {
  const index = { byPartner: new Map([["34", "USD"], ["60", "RUB"], ["7", "USD"]]), byName: new Map() };
  const entries = [
    debt(100), payment(30),
    debt(5000, { supplierName: "Инна", partnerId: "60", priceCurrency: "RUB", rate: 1 }),
    payment(50, "USD", { partnerId: "7", supplierName: "DimaAmerika" }),
  ];
  const t = across(entries, index);
  assert.equal(t.balanceUsd, -20);
  assert.equal(t.balanceRub, -5000);
  assert.equal(t.owedUsd, 70);
  assert.equal(t.owedRub, 5000);
});
