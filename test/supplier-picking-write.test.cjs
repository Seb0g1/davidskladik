"use strict";

// writeSupplierPickingState({ onlyKeys }) must persist only the changed picking rows
// (server/parts/02d-supplier-picking-state.js). Prisma and helpers are stubbed.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02d-supplier-picking-state.js"), "utf8");

function load() {
  const upserts = [];
  const prisma = {
    supplierPickingRow: { upsert: (args) => { upserts.push(args.where.pickingKey); return args; } },
    $transaction: async (ops) => ops,
  };
  const ctx = vm.createContext({
    console, Date, JSON, Math, Number, String, Boolean, Array, Object, Set, Map, Promise,
    cleanText: (v) => String(v ?? "").trim(),
    shouldUsePostgresStorage: () => true,
    jsonFallbackEnabled: () => false,
    getPrisma: () => prisma,
    chunkArray: (items, size) => { const out = []; for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size)); return out; },
    logger: { warn() {}, info() {} },
    toDateOrNull: () => null,
    normalizeFinanceMoney: () => null,
    normalizeSupplierOrderCutoff: () => null,
    normalizeSupplierTrustFactor: () => null,
    computeMarketplaceSaleAmountRub: () => null,
  });
  vm.runInContext(`${source}\nthis.write = writeSupplierPickingState;`, ctx);
  return { write: ctx.write, upserts };
}

const state = {
  rows: {
    a: { key: "a", status: "open", offerId: "A", quantity: 1 },
    b: { key: "b", status: "open", offerId: "B", quantity: 1 },
    c: { key: "c", status: "picked", offerId: "C", quantity: 1 },
  },
};

test("supplier picking write: onlyKeys upserts just the changed row", async () => {
  const { write, upserts } = load();
  await write(state, { onlyKeys: ["b"] });
  assert.deepEqual(upserts, ["b"]);
});

test("supplier picking write: without onlyKeys all rows are persisted (unchanged behaviour)", async () => {
  const { write, upserts } = load();
  await write(state);
  assert.deepEqual(upserts.sort(), ["a", "b", "c"]);
});
