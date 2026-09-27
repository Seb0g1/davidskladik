"use strict";

// Unit tests for the Ozon auto-archive restore quota in unarchiveProductsOnMarketplaces
// (server/parts/02f-unarchive-products-core.js): Ozon allows 100 restores of auto-archived
// products per window and rejects a whole request that would cross it.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const part = (name) => fs.readFileSync(path.join(__dirname, "..", "server", "parts", name), "utf8");

function setup({ remaining = 100, localUsed = 0, closed = false } = {}) {
  const ozon = { remaining, calls: 0, accepted: [] };
  const usage = { recorded: 0, closed: false };
  let queue = { daily: {}, items: [] };
  const ctx = vm.createContext({
    cleanText: (value) => String(value ?? "").trim(),
    chunkArray: (list, size) => { const out = []; for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size)); return out; },
    logger: { warn: () => {}, info: () => {} },
    ozonUnarchiveDailyLimit: 100,
    getOzonAccountByTarget: () => ({ id: "ozon" }),
    readOzonUnarchiveQueue: async () => queue,
    resolveOzonUnarchiveProductIds: async (items) => items.map((item) => ({ item, productId: item.productId })),
    ozonUnarchiveDailyUsed: () => localUsed,
    ozonUnarchiveWindowClosed: () => closed,
    setOzonUnarchiveDailyUsed: (q) => q,
    recordOzonUnarchiveUsage: async (_target, count) => { usage.recorded += count; },
    closeOzonUnarchiveWindow: async () => { usage.closed = true; },
    queueOzonUnarchiveItems: (q, items, opts) => ({ ...q, items: [...q.items, ...items.map((item) => ({ ...item, ...opts }))] }),
    removeOzonUnarchiveQueueItems: (q) => q,
    writeOzonUnarchiveQueueDelta: async (q) => { queue = q; },
    ozonNumericProductId: (value) => String(value || ""),
    nextOzonUnarchiveRetryAt: () => "later",
    nextOzonUnarchiveScheduledRunAt: () => new Date("2026-09-28T00:00:00Z"),
    nextOzonUnarchiveVisibilityRetryAt: () => "soon",
    rescheduleOzonUnarchiveQueueAutoSoon: async () => {},
    ozonUnarchiveQueuedActions: (items) => items.map((item) => ({ id: item.id, ok: true, pending: true, queuedByDailyLimit: true })),
    ozonRequest: async (_path, body) => {
      ozon.calls += 1;
      const ids = body.product_id;
      const limited = ids.filter((id) => Number(id) < 900000);
      if (limited.length > ozon.remaining) {
        const error = new Error("Ozon API /v1/product/unarchive failed: 400 {\"code\":11,\"message\":\"restore limit exceeded\"}");
        throw error;
      }
      ozon.remaining -= limited.length;
      ozon.accepted.push(...ids);
      return { result: true };
    },
  });
  vm.runInContext(`${part("02f-unarchive-products-core.js")}
this.api = { unarchiveProductsOnMarketplaces };`, ctx);
  return { unarchive: ctx.api.unarchiveProductsOnMarketplaces, ozon, usage, queue: () => queue };
}

const autoArchived = (n, start = 1) => Array.from({ length: n }, (_, i) => ({
  id: `p${start + i}`, target: "ozon", marketplace: "ozon", offerId: `o${start + i}`, productId: String(start + i),
  marketplaceState: { isAutoArchived: true },
}));
const manual = (n) => Array.from({ length: n }, (_, i) => ({
  id: `m${i}`, target: "ozon", marketplace: "ozon", offerId: `m${i}`, productId: String(900000 + i),
  marketplaceState: { isAutoArchived: false },
}));

test("tail of the window: 6 slots left, batch of 10 → all 6 used, window closed", async () => {
  const t = setup({ remaining: 6, localUsed: 90 });
  const actions = await t.unarchive(autoArchived(10));
  assert.equal(t.ozon.accepted.length, 6);
  assert.equal(t.usage.recorded, 6);
  assert.equal(t.usage.closed, true);
  assert.equal(actions.filter((a) => a.ok && !a.pending).length, 6);
  assert.equal(actions.filter((a) => a.queuedByDailyLimit).length, 4);
  assert.ok(t.ozon.calls <= 12, `too many API calls: ${t.ozon.calls}`);
});

test("local counter ahead of Ozon: bypass fills the real remainder exactly", async () => {
  const t = setup({ remaining: 37, localUsed: 100 });
  // Reconciler-style bypass ignores the local gate; Ozon decides.
  await t.unarchive(autoArchived(100), { forceOzonDailyLimit: true });
  assert.equal(t.ozon.accepted.length, 37);
  assert.equal(t.usage.recorded, 37);
  assert.equal(t.usage.closed, true);
});

test("full fresh window: 150 queued → exactly 100 restored, 50 deferred", async () => {
  const t = setup({ remaining: 100, localUsed: 0 });
  const actions = await t.unarchive(autoArchived(150), { forceOzonDailyLimit: true });
  assert.equal(t.ozon.accepted.length, 100);
  assert.equal(actions.filter((a) => a.queuedByDailyLimit).length, 50);
});

test("closed window: no API call for auto-archived products, manual ones still go", async () => {
  const t = setup({ remaining: 0, localUsed: 100, closed: true });
  const actions = await t.unarchive([...autoArchived(5), ...manual(3)], { forceOzonDailyLimit: true });
  assert.deepEqual(t.ozon.accepted.map(Number).sort((a, b) => a - b), [900000, 900001, 900002]);
  assert.equal(t.ozon.calls, 1);
  assert.equal(actions.filter((a) => a.queuedByDailyLimit).length, 5);
});

test("queue gate: local counter 98 → only 2 sent, rest deferred without an API error", async () => {
  const t = setup({ remaining: 50, localUsed: 98 });
  await t.unarchive(autoArchived(10));
  assert.equal(t.ozon.accepted.length, 2);
  assert.equal(t.usage.closed, false);
});

test("quota window follows Ozon's 03:00 MSK reset, not Moscow midnight", () => {
  const ctx = vm.createContext({ cleanText: (value) => String(value ?? "").trim() });
  vm.runInContext(`${part("02a-ozon-unarchive-queue-helpers.js")}
this.api = { ozonUnarchiveDateKey };`, ctx);
  const key = ctx.api.ozonUnarchiveDateKey;
  // 02:30 MSK on the 28th is still Ozon's window of the 27th; 03:00 MSK starts the 28th.
  assert.equal(key(new Date("2026-09-27T23:30:00Z")), "2026-09-27");
  assert.equal(key(new Date("2026-09-28T00:00:00Z")), "2026-09-28");
  assert.equal(key(new Date("2026-09-27T21:10:00Z")), "2026-09-27");
});
