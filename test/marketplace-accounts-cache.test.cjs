"use strict";

// getMarketplaceAccounts caches only for the current synchronous run
// (server/parts/02a-marketplace-accounts.js). File access is stubbed.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02a-marketplace-accounts.js"), "utf8");

function load() {
  let reads = 0;
  let fileAccounts = [{ id: "ozon-a", marketplace: "ozon", name: "A" }];
  const ctx = vm.createContext({
    console, Date, JSON, Math, Number, String, Boolean, Array, Object, Set, Map, Promise, setImmediate,
    process: { env: {} },
    crypto: { randomUUID: () => "00000000-0000-0000-0000-000000000000" },
    cleanText: (v) => String(v ?? "").trim(),
    parseBooleanSetting: (v, fallback) => (v === undefined ? fallback : v === true || v === "true"),
    marketplaceAccountsPath: "/tmp/none.json",
    fsSync: { readFileSync: () => { reads += 1; return JSON.stringify({ accounts: fileAccounts }); } },
  });
  vm.runInContext(`${source}\nthis.get = getMarketplaceAccounts;`, ctx);
  return { get: ctx.get, reads: () => reads, setFile: (accounts) => { fileAccounts = accounts; } };
}

const nextTick = () => new Promise((resolve) => setImmediate(resolve));

test("marketplace accounts: one file read per synchronous run", async () => {
  const api = load();
  for (let i = 0; i < 1000; i += 1) api.get();
  assert.equal(api.reads(), 1);
  await nextTick();
  api.get();
  assert.equal(api.reads(), 2);
});

test("marketplace accounts: file changes are visible on the next run", async () => {
  const api = load();
  assert.equal(api.get()[0].name, "A");
  api.setFile([{ id: "ozon-a", marketplace: "ozon", name: "B" }]);
  await nextTick();
  assert.equal(api.get()[0].name, "B");
});

test("marketplace accounts: callers cannot mutate the cached copy", () => {
  const api = load();
  api.get()[0].name = "mutated";
  assert.equal(api.get()[0].name, "A");
});
