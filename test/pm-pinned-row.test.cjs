"use strict";

// Unit tests for filterSelectedRowMatchesToBestPin (server/parts/02a-price-master-match-helpers.js):
// a pinned PriceMaster row must never be swapped for a different product that a supplier
// (Далик) re-used the same article for.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-price-master-match-helpers.js"), "utf8");
const ctx = vm.createContext({
  cleanText: (value) => String(value ?? "").trim(),
  normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
});
vm.runInContext(`${source}
this.api = { filterSelectedRowMatchesToBestPin, pmRowConfirmsPinnedName };`, ctx);
const { filterSelectedRowMatchesToBestPin: filter, pmRowConfirmsPinnedName } = ctx.api;

const link = {
  matchType: "selected_row",
  sourceRowId: "100",
  article: "LQOA100",
  resolvedPriceMasterRow: { name: "LATTAFA QUEEN OF ARABIA EDP 100 ML" },
};
const row = (rowId, name, extra = {}) => ({ rowId, name, article: "LQOA100", active: true, price: 20, ...extra });
const ids = (rows) => Array.from(rows, (r) => r.rowId);

test("pinned row present: kept even when it is the only match", () => {
  assert.deepEqual(ids(filter(link, [row("100", "LATTAFA QUEEN OF ARABIA EDP 100 ML")])), ["100"]);
});

test("pinned row gone, a different product under the same article: nothing is returned", () => {
  assert.deepEqual(ids(filter(link, [row("205", "ALHAMBRA KISMET EDP 100 ML")])), []);
});

test("pinned row gone, different product sharing only the brand: nothing is returned", () => {
  assert.deepEqual(ids(filter(link, [row("206", "LATTAFA KHAMRAH QAHWA EDP 100 ML")])), []);
});

test("pinned row re-uploaded with a new RowID and slightly different name: followed", () => {
  assert.deepEqual(ids(filter(link, [row("300", "LATTAFA QUEEN OF ARABIA 100ML EDP")])), ["300"]);
});

test("several article rows: only the one confirming the pinned name survives", () => {
  const out = filter(link, [
    row("205", "ALHAMBRA KISMET EDP 100 ML"),
    row("300", "LATTAFA QUEEN OF ARABIA EDP 100ML"),
    row("206", "LATTAFA KHAMRAH QAHWA EDP 100 ML"),
  ]);
  assert.deepEqual(ids(out), ["300"]);
});

test("old links without a pinned name keep all rows for order-name disambiguation", () => {
  const legacy = { matchType: "selected_row", sourceRowId: "100", article: "LQOA100" };
  assert.deepEqual(ids(filter(legacy, [row("205", "ALHAMBRA KISMET EDP 100 ML")])), ["205"]);
});

test("pmRowConfirmsPinnedName rejects brand-only overlap", () => {
  assert.equal(pmRowConfirmsPinnedName({ name: "LATTAFA ASAD EDP 100 ML" }, "LATTAFA QUEEN OF ARABIA EDP 100 ML"), false);
  assert.equal(pmRowConfirmsPinnedName({ name: "Lattafa Queen of Arabia 100 ml" }, "LATTAFA QUEEN OF ARABIA EDP 100 ML"), true);
});
