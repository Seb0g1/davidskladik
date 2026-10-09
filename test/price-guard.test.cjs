"use strict";
// Price guard (server/parts/02a-price-history-storage.js): placeholder / rouble-as-dollar prices are held;
// rises go out; a drop over 30% only with a fresh supplier price list
const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const parts = path.join(__dirname, "..", "server", "parts");
const pick = (file, name) => {
  const src = fs.readFileSync(path.join(parts, file), "utf8");
  const start = src.indexOf(`function ${name}(`);
  let depth = 0;
  for (let i = src.indexOf(") {", start) + 2; i < src.length; i += 1) {
    if (src[i] === "{") depth += 1;
    if (src[i] === "}") { depth -= 1; if (!depth) return src.slice(start, i + 1); }
  }
  throw new Error(name);
};
const ctx = vm.createContext({});
vm.runInContext([
  "const PRICE_GUARD_SUSPECT_USD = 900; const PRICE_GUARD_MAX_DROP = 0.3; const PRICE_GUARD_FRESH_HOURS = 36;",
  "function cleanText(v) { return String(v ?? '').trim(); }",
  pick("02a-price-master-match-helpers.js", "priceMasterBottleVolumes"),
  pick("02a-price-history-storage.js", "priceGuardRowProblem"),
  pick("02a-price-history-storage.js", "priceGuardSupplierFreshness"),
  pick("02a-price-history-storage.js", "priceGuardVerdict"),
  "this.api = { priceGuardRowProblem, priceGuardVerdict };",
].join("\n"), ctx);
const { priceGuardRowProblem, priceGuardVerdict } = ctx.api;

test("placeholder and rouble-as-dollar supplier prices are held", () => {
  assert.ok(priceGuardRowProblem({ price: 1000, priceCurrency: "USD", name: "ANNA SUI SECRET WISH 50ml" }));
  assert.ok(priceGuardRowProblem({ price: 3900, priceCurrency: "USD", name: "DE LAVIE PARFUMS EXOTICS (U) Парфюмерная вода 80ML" }));
  assert.ok(priceGuardRowProblem({ price: 4205.34, priceCurrency: "USD", name: "Michael Kors Sexy Amber 50 мл" }));
});

test("normal and rouble prices pass", () => {
  assert.equal(priceGuardRowProblem({ price: 45, priceCurrency: "USD", name: "DE LAVIE EXOTICS 80 ML" }), "");
  assert.equal(priceGuardRowProblem({ price: 3900, priceCurrency: "RUB", name: "X 80ML" }), "");
  assert.equal(priceGuardRowProblem({ price: 650, priceCurrency: "USD", name: "Clive Christian No1 50ml" }), "");
  assert.equal(priceGuardRowProblem({ price: 1200, priceCurrency: "USD", name: "Refill 1000 ml" }), "");
});

test("a rise is sent, a broken supplier row is held, an approved price passes", () => {
  const rise = { nextPrice: 30000, currentPrice: 9000, selectedSupplier: { price: 120, priceCurrency: "USD", name: "Y 100ml" } };
  assert.equal(priceGuardVerdict(rise), null);
  const broken = { nextPrice: 230403, currentPrice: 9000, selectedSupplier: { price: 3900, priceCurrency: "USD", name: "X 80ML" } };
  assert.ok(priceGuardVerdict(broken));
  assert.equal(priceGuardVerdict(broken, 230403), null);
});

test("a big drop needs a fresh supplier price list", () => {
  const hoursAgo = (h) => new Date(Date.now() - h * 3600_000).toISOString();
  const drop = (supplier) => ({ nextPrice: 7000, currentPrice: 230403, selectedSupplier: { price: 26, priceCurrency: "USD", name: "Z 50 мл", ...supplier } });
  assert.equal(priceGuardVerdict(drop({ docDate: hoursAgo(3) })), null);
  assert.match(priceGuardVerdict(drop({ docDate: hoursAgo(80) })).reason, /не свежий/);
  assert.match(priceGuardVerdict(drop({})).reason, /без даты/);
  assert.match(priceGuardVerdict(drop({ docDate: hoursAgo(1), priceSource: "timeout" })).reason, /не прочитан/);
  // a small drop goes out whatever the list's date
  assert.equal(priceGuardVerdict({ nextPrice: 8000, currentPrice: 9000, selectedSupplier: { price: 30, priceCurrency: "USD", name: "Q 50ml" } }), null);
});
