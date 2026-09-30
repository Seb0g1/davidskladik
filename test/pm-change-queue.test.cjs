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
this.api = { enqueuePmChangeBatch, takePmChangeChunk, completePmChangeChunk, pmChangePendingCount };`, ctx);
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
