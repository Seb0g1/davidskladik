#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const {
  readSupplierPickingState,
  normalizeSupplierPickingRow,
  upsertSupplierLedgerDebtFromPickingRow,
} = require("../server.js");
const { closePrisma } = require("../lib/postgres.js");

async function main() {
  const state = await readSupplierPickingState();
  const rows = Object.values(state.rows || {})
    .map(normalizeSupplierPickingRow)
    .filter((row) => row.status === "picked");
  let createdOrUpdated = 0;
  let skipped = 0;
  const request = { session: { username: "backfill-supplier-ledger", role: "admin" } };
  for (const row of rows) {
    const entry = await upsertSupplierLedgerDebtFromPickingRow(row, null, request);
    if (entry) createdOrUpdated += 1;
    else skipped += 1;
  }
  console.log(JSON.stringify({
    ok: true,
    pickedRows: rows.length,
    createdOrUpdated,
    skipped,
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePrisma().catch(() => {});
  });
