"use strict";

// «Нет активного поставщика — нет продажи» (server/parts/02a-live-supplier-guard.js): which cards have a live row.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-live-supplier-guard.js"), "utf8");
const ctx = vm.createContext({ process: { env: {} }, cleanText: (v) => String(v ?? "").trim() });
vm.runInContext(`${source}\nthis.g = { buildLiveSupplierIndex, productHasLiveSupplierRow, liveGuardManualSellable };`, ctx);
const { buildLiveSupplierIndex, productHasLiveSupplierRow, liveGuardManualSellable } = ctx.g;

const index = buildLiveSupplierIndex([
  { rowId: 2236632, article: "PF0103SL300TN", name: "COMPAGNIE DE PROVENCE Black Tea Soap 300 ml", partnerId: 60, partnerName: "Инна" },
  { rowId: 500, article: "A-1", name: "Dior Sauvage edt 100ml", partnerId: 7, partnerName: "Сорин" },
]);

test("live by row, by article + supplier (id or name), by exact name + supplier", () => {
  assert.equal(productHasLiveSupplierRow({ links: [{ sourceRowId: "500" }] }, index), true);
  assert.equal(productHasLiveSupplierRow({ links: [{ article: "a-1", partnerId: "7" }] }, index), true);
  assert.equal(productHasLiveSupplierRow({ links: [{ supplierArticle: "A-1", supplierName: "сорин" }] }, index), true);
  // the supplier renamed the article — the selected supplier carries the new one
  assert.equal(productHasLiveSupplierRow({ selectedSupplier: { article: "PF0103SL300TN", partnerId: "60" }, links: [{ article: "69423", partnerId: "60", sourceRowId: "2236685" }] }, index), true);
  assert.equal(productHasLiveSupplierRow({ links: [{ exactName: "Dior  Sauvage edt 100ml", supplierName: "Сорин" }] }, index), true);
});

test("no live row: the article is gone, or exists only at another supplier", () => {
  assert.equal(productHasLiveSupplierRow({ links: [{ article: "69423", partnerId: "60", sourceRowId: "2236685" }] }, index), false);
  assert.equal(productHasLiveSupplierRow({ links: [{ article: "A-1", partnerId: "60" }] }, index), false);
  assert.equal(productHasLiveSupplierRow({ selectedSupplier: { article: "X", supplierName: "Инна" }, links: [] }, index), false);
});

test("nothing to check by → null (goes to the full calculation)", () => {
  assert.equal(productHasLiveSupplierRow({ links: [{ keyword: "dior sauvage" }] }, index), null);
  assert.equal(productHasLiveSupplierRow({ links: [] }, index), null);
});

test("«продаётся вручную» holds 48 hours", () => {
  const now = Date.parse("2026-10-03T00:00:00Z");
  assert.equal(liveGuardManualSellable({ noSupplierAutomation: { manualSellableAt: "2026-10-02T12:00:00Z" } }, now), true);
  assert.equal(liveGuardManualSellable({ noSupplierAutomation: { manualSellableAt: "2026-09-29T12:00:00Z" } }, now), false);
  assert.equal(liveGuardManualSellable({}, now), false);
});
