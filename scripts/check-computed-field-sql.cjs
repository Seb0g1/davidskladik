#!/usr/bin/env node
"use strict";

// Guards against the "dead sweep" bug class: a raw SQL predicate filtering
// warehouse_products.raw on a field that buildFreshWarehouseProductsForWarehouse()
// computes at read time and never persists (e.g. `raw -> 'selectedSupplier'`).
// Such a predicate matches zero rows forever, silently disabling the query.
//
// See lib/computed-product-fields.js for the canonical list of these fields.

const fs = require("fs");
const path = require("path");
const { COMPUTED_WAREHOUSE_PRODUCT_FIELDS } = require("../lib/computed-product-fields");

const computedFieldSet = new Set(COMPUTED_WAREHOUSE_PRODUCT_FIELDS);
const partsDir = path.join(__dirname, "..", "server", "parts");

// Matches `$queryRaw`, `$queryRawUnsafe`, `$executeRaw`, `$executeRawUnsafe` calls
// whose SQL is passed as a single backtick template literal.
const sqlCallRegex = /\$(?:query|execute)Raw(?:Unsafe)?\s*\(\s*`([\s\S]*?)`/g;

// Matches `raw -> 'field'`, `raw ->> 'field'`, `raw #> '{field}'`, `raw #>> '{field}'`,
// with or without a table alias prefix (e.g. `p.raw`).
const rawAccessRegex = /\braw\s*(?:->>?|#>>?)\s*'\{?([A-Za-z_][A-Za-z0-9_]*)\}?'/g;

const violations = [];

for (const file of fs.readdirSync(partsDir)) {
  if (!file.endsWith(".js")) continue;
  const fullPath = path.join(partsDir, file);
  const content = fs.readFileSync(fullPath, "utf8");

  let sqlMatch;
  while ((sqlMatch = sqlCallRegex.exec(content))) {
    const sql = sqlMatch[1];
    if (!/warehouse_products/i.test(sql)) continue;
    const sqlStart = sqlMatch.index + sqlMatch[0].indexOf("`") + 1;

    let fieldMatch;
    while ((fieldMatch = rawAccessRegex.exec(sql))) {
      const field = fieldMatch[1];
      if (!computedFieldSet.has(field)) continue;
      const absoluteIndex = sqlStart + fieldMatch.index;
      const line = content.slice(0, absoluteIndex).split("\n").length;
      violations.push({ file, line, field, snippet: fieldMatch[0].trim() });
    }
  }
}

if (violations.length) {
  console.error("Found warehouse_products SQL predicates filtering on computed (non-persisted) fields:\n");
  for (const v of violations) {
    console.error(`  ${v.file}:${v.line}  ${v.snippet}  (computed field: "${v.field}")`);
  }
  console.error(
    "\nThese fields come only from buildFreshWarehouseProductsForWarehouse() and are never written\n" +
    "to warehouse_products.raw, so `raw->'...'` predicates referencing them silently match zero rows\n" +
    "(this is how the dead stock-sweep regression happened). Resolve the field via a fresh build\n" +
    "(buildFreshWarehouseProducts) instead of filtering for it in SQL.\n" +
    "See lib/computed-product-fields.js for the full list.",
  );
  process.exit(1);
}

console.log(`OK: no warehouse_products SQL predicates filter on computed fields (${COMPUTED_WAREHOUSE_PRODUCT_FIELDS.length} tracked).`);
