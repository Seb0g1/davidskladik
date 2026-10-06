"use strict";

// Unit tests for the PriceMaster change watcher queue (server/parts/02f-pm-change-watcher.js):
// the newest change is served first and progress survives chunk by chunk.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const vm = require("vm");

const source = fs.readFileSync(path.join(__dirname, "..", "server", "parts", "02f-pm-change-watcher.js"), "utf8");
const ctx = vm.createContext({ process: { env: {} }, path, dataDir: "/tmp" });
vm.runInContext(`${source}
this.api = { enqueuePmChangeBatch, takePmChangeChunk, completePmChangeChunk, pmChangePendingCount, buildPmPartnerRowMaps, diffPmPartnerRows };`, ctx);
const { enqueuePmChangeBatch: enqueue, takePmChangeChunk: take, completePmChangeChunk: complete, pmChangePendingCount: count } = ctx.api;

const batch = (name, ids) => ({ at: name, partners: [{ id: name, name }], ids });

test("a fresh price list jumps ahead of the backlog", () => {
  let pending = enqueue([], batch("backlog", ["a", "b", "c", "d"]));
  pending = enqueue(pending, batch("Инна", ["x", "c"]));
  assert.deepEqual(Array.from(take(pending, 10)), ["x", "c"]);
  // "c" is rebuilt with the fresh batch, so the backlog no longer holds it.
  assert.deepEqual(Array.from(pending[1].ids), ["a", "b", "d"]);
  assert.equal(count(pending), 5);
});

test("chunks shrink the head batch and report it once finished", () => {
  let pending = enqueue([], batch("backlog", ["a", "b", "c"]));
  let step = complete(pending, take(pending, 2));
  assert.equal(step.finished, null);
  assert.deepEqual(Array.from(step.pending[0].ids), ["c"]);
  step = complete(step.pending, take(step.pending, 2));
  assert.equal(step.finished.at, "backlog");
  assert.equal(step.pending.length, 0);
});

test("an empty change adds nothing", () => {
  const pending = enqueue([batch("old", ["a"])], batch("empty", []));
  assert.equal(pending.length, 1);
});

const { buildPmPartnerRowMaps: maps, diffPmPartnerRows: diff } = ctx.api;

test("a row that left the price list is a stock change, a new price is a price change", () => {
  const before = maps([
    { partnerId: 121, article: "82255", name: "Montblanc Explorer Platinum  2 ml", price: 6 },
    { partnerId: 121, article: "81374", name: "Arabesque Elusive Musk", price: 40 },
    { partnerId: 121, article: "777", name: "Other", price: 10 },
  ]).get("121");
  const after = maps([
    { partnerId: 121, article: "81374", name: "Arabesque Elusive Musk", price: 42 },
    { partnerId: 121, article: "777", name: "Other", price: 10 },
    { partnerId: 121, article: "", name: "New row", price: 5 },
  ]).get("121");
  const d = diff(before, after);
  assert.deepEqual([...d.stock.articles].sort(), ["82255"]);
  assert.ok(d.stock.names.has("montblanc explorer platinum 2 ml"));
  assert.ok(d.stock.names.has("new row"));
  assert.deepEqual([...d.price.articles], ["81374"]);
  assert.ok(!d.price.articles.has("777") && !d.stock.articles.has("777"));
});

test("one article on two rows: losing one of them is seen", () => {
  const before = maps([{ partnerId: 1, article: "A", name: "x 50", price: 1 }, { partnerId: 1, article: "A", name: "x 100", price: 2 }]).get("1");
  const same = maps([{ partnerId: 1, article: "A", name: "x 100", price: 2 }, { partnerId: 1, article: "A", name: "x 50", price: 1 }]).get("1");
  const less = maps([{ partnerId: 1, article: "A", name: "x 100", price: 2 }]).get("1");
  assert.equal(diff(before, same).price.articles.size + diff(before, same).stock.articles.size, 0);
  assert.ok(diff(before, less).price.articles.has("a"));
});
