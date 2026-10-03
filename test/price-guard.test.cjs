"use strict";
// Price guard (server/parts/02a-price-history-storage.js): placeholder / rouble-as-dollar prices and 3× jumps
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
  "const PRICE_GUARD_SUSPECT_USD = 900; const PRICE_GUARD_MAX_RISE = 3;",
  "function cleanText(v) { return String(v ?? '').trim(); }",
  pick("02a-price-master-match-helpers.js", "priceMasterBottleVolumes"),
  pick("02a-price-history-storage.js", "priceGuardRowProblem"),
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

test("a 3× rise is held, an approved price passes, drops pass", () => {
  const product = { nextPrice: 30000, currentPrice: 9000, selectedSupplier: { price: 120, priceCurrency: "USD", name: "Y 100ml" } };
  assert.ok(priceGuardVerdict(product));
  assert.equal(priceGuardVerdict(product, 30000), null);
  assert.equal(priceGuardVerdict({ nextPrice: 7000, currentPrice: 230403, selectedSupplier: { price: 26, priceCurrency: "USD", name: "Z 50 мл" } }), null);
});
