"use strict";

// Unit tests for supplierRowVolumeMismatch / markVolumeMismatchedMatches
// (server/parts/02a-price-master-match-helpers.js), loaded standalone in a VM context.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-price-master-match-helpers.js"), "utf8");
const ctx = vm.createContext({
  process: { env: {} },
  cleanText: (value) => String(value ?? "").trim(),
  normalizeSearchText: (value) => String(value || "").toLowerCase(),
  normalizeSupplierName: (value) => String(value || "").trim().toLowerCase(),
});
vm.runInContext(`${source}
this.api = { supplierRowVolumeMismatch, markVolumeMismatchedMatches, priceMasterBottleVolumes };`, ctx);
const { supplierRowVolumeMismatch: mismatch, markVolumeMismatchedMatches: mark, priceMasterBottleVolumes: volumes } = ctx.api;

test("a 60 ml card never takes a 120 ml supplier row (Mancera Wild Cherry 57031)", () => {
  assert.equal(mismatch("MANCERA Wild Cherry Парфюмированная вода унисекс 60мл", "MANCERA Wild Cherry EDP 120 ml - парфюмерная вода"), "60->120");
  assert.equal(mismatch("MANCERA Wild Cherry Парфюмированная вода унисекс 60мл", "MANCERA Wild Cherry EDP 60 ml - парфюмерная вода"), null);
});

test("a smaller bottle is a mismatch too", () => {
  assert.equal(mismatch("XERJOFF V Amabile 100 ml - парфюмерная вода", "XERJOFF Xerjoff V Amabile EDP 50 ml - парфюмерная вода"), "100->50");
});

test("volumes written as a bare number before the concentration count", () => {
  assert.equal(mismatch("JIMMY CHOO Blossom Парфюмерная вода 60 мл", "JIMMY CHOO BLOSSOM woman 40 EDP"), "60->40");
  assert.equal(mismatch("VIKTOR & ROLF GOOD FORTUNE Парфюмерная вода 90 мл", "VIKTOR & ROLF GOOD FORTUNE woman 90 EDP"), null);
});

test("same volume in other spellings, samples, sets and names without a volume all pass", () => {
  assert.equal(mismatch("Juicy Couture Noir 100 мл", "JUICY COUTURE NOIR w edp 100.0ml"), null);
  assert.equal(mismatch("Creed Delphinus Парфюмированная вода 2мл", "CREED Delphinus edp 1.7ml"), null);
  assert.equal(mismatch("Coach Blue Набор для мужчин 215мл", "Coach Blue m edt100ml+edt15ml+sh\\gel100ml"), null);
  assert.equal(mismatch("Lanvin Eclat", "Lanvin Eclat 100ml edp"), null);
});

test("shade codes in hair dye names are not volumes", () => {
  assert.deepEqual(Array.from(volumes("5/6 LC Стойкая крем-краска светлый шатен 60 мл")), [60]);
  assert.equal(mismatch("10/73 COLOR TOUCH Крем-краска 60 мл", "Montale Wild Pears edp 20ml"), "60->20");
});

test("marking turns only the wrong-bottle rows unavailable", () => {
  const rows = [
    { name: "MANCERA Wild Cherry EDP 120 ml", available: true },
    { name: "MANCERA Wild Cherry EDP 60 ml", available: true },
  ];
  const marked = mark(rows, "MANCERA Wild Cherry 60мл");
  assert.equal(marked[0].available, false);
  assert.equal(marked[0].volumeMismatch, "60->120");
  assert.equal(marked[1].available, true);
  assert.equal(marked[1].volumeMismatch, undefined);
});
