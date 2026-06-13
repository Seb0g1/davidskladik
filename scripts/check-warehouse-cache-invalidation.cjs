#!/usr/bin/env node
"use strict";

// Guards against the "save a link, page still shows it as not linked" bug class
// (PLAN-HARDENING.md 1.4): invalidating warehouseFastPageCache *before* a warehouse
// mutation commits (and before any follow-up activation/recovery work it queues) leaves
// a window where a concurrent catalog read repopulates the cache with pre-mutation data
// and serves it stale for up to warehouseFastPageCacheTtlMs.
//
// Route handlers must wrap their mutation + response in withWarehouseMutation (see
// 01-bootstrap-helpers.js), which invalidates the cache in a `finally` AFTER the
// wrapped work completes. Calling invalidateWarehouseViewCache() directly from a route
// file re-introduces the early-invalidation race.

const fs = require("fs");
const path = require("path");

const partsDir = path.join(__dirname, "..", "server", "parts");

const ROUTE_CALL_RE = /\bapp\.(get|post|put|patch|delete)\(/;
const RAW_INVALIDATE_RE = /\binvalidateWarehouseViewCache\s*\(/;

const violations = [];

for (const file of fs.readdirSync(partsDir)) {
  if (!file.endsWith(".js")) continue;
  const fullPath = path.join(partsDir, file);
  const content = fs.readFileSync(fullPath, "utf8");
  if (!ROUTE_CALL_RE.test(content)) continue;

  const lines = content.split("\n");
  for (let i = 0; i < lines.length; i++) {
    if (RAW_INVALIDATE_RE.test(lines[i])) {
      violations.push({ file, line: i + 1 });
    }
  }
}

if (violations.length) {
  console.error("Found raw invalidateWarehouseViewCache() calls in route files:\n");
  for (const v of violations) {
    console.error(`  ${v.file}:${v.line}`);
  }
  console.error(
    "\nPLAN-HARDENING.md 1.4 contract: warehouse mutation routes must invalidate the page\n" +
    "cache AFTER their write (and any follow-up activation/recovery work) commits, not\n" +
    "before. Wrap the mutation + response in withWarehouseMutation(async () => { ... })\n" +
    "instead of calling invalidateWarehouseViewCache() directly — see\n" +
    "01-bootstrap-helpers.js.",
  );
  process.exit(1);
}

console.log("OK: route files use withWarehouseMutation instead of raw invalidateWarehouseViewCache() calls.");
