"use strict";

// Stale-while-revalidate behaviour of buildFastWarehousePage (server/parts/02a-warehouse-fast-page.js)
// with the page cache helpers from 02a-warehouse-fast-group-page.js. Heavy dependencies are stubbed.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const parts = path.join(__dirname, "..", "server", "parts");
const read = (name) => fs.readFileSync(path.join(parts, name), "utf8");

// Only the cache helpers — the real page builder in the same file would shadow the stub.
function cacheHelpersSource() {
  const src = read("02a-warehouse-fast-group-page.js");
  return src.slice(src.indexOf("function warehouseFastPageCacheKey"));
}

function load({ ttlMs = 1000, staleMs = 60_000 } = {}) {
  const ctx = vm.createContext({ console, setTimeout, clearTimeout, Promise, Date, JSON, Map, Math, Number, String, Boolean, Array, Object });
  vm.runInContext(`
    const warehouseFastPageCache = new Map();
    const warehouseFastPageInflight = new Map();
    let warehouseFastPageCacheGeneration = 0;
    const warehouseFastPageCacheTtlMs = ${ttlMs};
    const warehouseFastPageStaleTtlMs = ${staleMs};
    const warehouseFastPageCacheMax = 80;
    const warehouseFastPageBuildTimeoutMs = 5000;
    const logger = { warn() {}, info() {} };
    const cleanText = (v) => String(v || "").trim();
    const cloneAuditValue = (v) => (v == null ? null : JSON.parse(JSON.stringify(v)));
    const shouldUsePostgresStorage = () => true;
    const acquireWarehousePageBuildSlot = async () => true;
    const releaseWarehousePageBuildSlot = () => {};
    const promiseTimeout = (ms, message) => new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms).unref?.());
    let buildCount = 0;
    let nextBuildDelay = 0;
    async function buildFastWarehouseGroupPageFromPostgres() {
      buildCount += 1;
      const n = buildCount;
      if (nextBuildDelay) await new Promise((r) => setTimeout(r, nextBuildDelay));
      return { build: n, items: [{ groupKey: "g" + n }], grouped: true };
    }
    async function buildFastWarehousePageFromPostgres() { return null; }
    function invalidateWarehouseViewCache() { warehouseFastPageCache.clear(); warehouseFastPageCacheGeneration += 1; }
    ${read("02a-warehouse-fast-page.js")}
    ${cacheHelpersSource()}
    this.api = {
      build: () => buildFastWarehousePage({ page: 1, pageSize: 100, filters: { groupPage: true } }),
      builds: () => buildCount,
      setDelay: (ms) => { nextBuildDelay = ms; },
      age: (ms) => { for (const entry of warehouseFastPageCache.values()) entry.at -= ms; },
      invalidate: invalidateWarehouseViewCache,
      inflight: () => warehouseFastPageInflight.size,
    };
  `, ctx);
  return ctx.api;
}

const tick = (ms = 20) => new Promise((r) => setTimeout(r, ms));

test("warehouse page cache: fresh hit does not rebuild", async () => {
  const api = load();
  assert.equal((await api.build()).build, 1);
  const again = await api.build();
  assert.equal(again.build, 1);
  assert.equal(again.revalidating, undefined);
  assert.equal(api.builds(), 1);
});

test("warehouse page cache: stale entry is served instantly and rebuilt once in background", async () => {
  const api = load();
  await api.build();
  api.age(5000); // past TTL, inside stale window
  api.setDelay(50);
  const stale = await api.build();
  assert.equal(stale.build, 1);
  assert.equal(stale.stale, true);
  assert.equal(stale.revalidating, true);
  const staleAgain = await api.build(); // rebuild still in flight → no second build
  assert.equal(staleAgain.build, 1);
  await tick(120);
  assert.equal(api.builds(), 2);
  const fresh = await api.build();
  assert.equal(fresh.build, 2);
  assert.equal(fresh.revalidating, undefined);
  assert.equal(api.inflight(), 0);
});

test("warehouse page cache: entries past the stale window are rebuilt synchronously", async () => {
  const api = load({ ttlMs: 1000, staleMs: 1000 });
  await api.build();
  api.age(5000);
  const page = await api.build();
  assert.equal(page.build, 2);
  assert.equal(page.revalidating, undefined);
});

test("warehouse page cache: build finishing after a mutation does not repopulate the cache", async () => {
  const api = load();
  api.setDelay(50);
  const pending = api.build();
  await tick(10);
  api.invalidate();
  assert.equal((await pending).build, 1); // caller still gets its result
  api.setDelay(0);
  assert.equal((await api.build()).build, 2); // but it was not cached
});

test("warehouse page: response has no duplicate groups array", async () => {
  const src = read("02a-warehouse-fast-group-page.js");
  assert.doesNotMatch(src, /^\s*groups,\s*$/m);
  assert.doesNotMatch(src, /groups: \[\],/);
});
