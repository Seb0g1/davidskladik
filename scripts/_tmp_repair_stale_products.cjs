#!/usr/bin/env node
/**
 * Two-phase repair for stale PM sourceRowId links:
 *
 * Phase 1: POST /api/warehouse/links/fix-stale-row-ids (dryRun=false)
 *   → Finds selected_row links where the pinned sourceRowId points to a row
 *     that is missing, inactive (active=false), or has price=0/NULL, while
 *     an active replacement row exists for the same article+partnerId.
 *   → Updates sourceRowId in product_links to point to the active row.
 *
 * Phase 2: POST /api/warehouse/links/recover-stale-stocks
 *   → Calls buildFreshWarehouseProducts + runSupplierRecoveryAutomation for
 *     each fixed product — restores marketplace stock/unarchive as needed.
 *
 * Usage:
 *   node scripts/_tmp_repair_stale_products.cjs           # dry-run (report only)
 *   node scripts/_tmp_repair_stale_products.cjs --apply   # fix + recover
 */
"use strict";
require("dotenv").config({ path: require("node:path").join(__dirname, "..", ".env") });

const APPLY = process.argv.includes("--apply");
const BASE = "http://localhost:3000";

async function login() {
  const res = await fetch(`${BASE}/api/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: process.env.APP_USER, password: process.env.APP_PASSWORD }),
  });
  const body = await res.json();
  if (!body.ok) { console.error("Auth failed", body); process.exit(1); }
  const cookie = (res.headers.get("set-cookie") || "").split(";")[0];
  console.log("Login: OK\n");
  return cookie;
}

async function main() {
  const cookie = await login();

  // Phase 1 — dry-run first to report what would be fixed
  console.log("=== Phase 1: scanning for stale sourceRowIds ===");
  const dryRes = await fetch(`${BASE}/api/warehouse/links/fix-stale-row-ids`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ dryRun: true }),
    signal: AbortSignal.timeout(60000),
  });
  const dryData = await dryRes.json();
  if (!dryData.ok) { console.error("fix-stale-row-ids dry-run failed:", dryData); process.exit(1); }

  console.log(`Found ${dryData.found} stale link(s) across ${dryData.productIds?.length ?? 0} product(s)\n`);
  for (const link of (dryData.links || [])) {
    const oldStatus = link.oldRowId ? `row=${link.oldRowId} active=${link.oldActive} price=${link.oldPrice}` : "NOT IN SNAPSHOT";
    console.log(
      `[${link.marketplace}] ${link.offerId} | partner=${link.partnerId} | article=${link.article}\n` +
      `  old: ${oldStatus} "${link.oldName || ""}"\n` +
      `  new: row=${link.newRowId} price=${link.newPrice} "${link.newName || ""}"\n`,
    );
  }

  if (!APPLY || !dryData.found) {
    if (dryData.found) console.log(`Run with --apply to fix ${dryData.found} stale row ID(s).`);
    return;
  }

  // Phase 1 — apply
  console.log("=== Phase 1: fixing sourceRowIds ===");
  const fixRes = await fetch(`${BASE}/api/warehouse/links/fix-stale-row-ids`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ dryRun: false }),
    signal: AbortSignal.timeout(60000),
  });
  const fixData = await fixRes.json();
  if (!fixData.ok) { console.error("fix-stale-row-ids failed:", fixData); process.exit(1); }
  console.log(`Fixed ${fixData.fixed} link(s) for ${fixData.productIds?.length ?? 0} product(s)\n`);

  const productIds = fixData.productIds || [];
  if (!productIds.length) { console.log("No products to recover."); return; }

  // Phase 2 — recover stocks, batch by 50
  console.log("=== Phase 2: recovering stocks ===");
  const batches = [];
  for (let i = 0; i < productIds.length; i += 50) batches.push(productIds.slice(i, i + 50));

  let totalFresh = 0, totalRecovered = 0, totalStocks = 0;
  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];
    console.log(`Batch ${b + 1}/${batches.length}: ${batch.length} product(s)...`);
    const recRes = await fetch(`${BASE}/api/warehouse/links/recover-stale-stocks`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Cookie: cookie },
      body: JSON.stringify({ productIds: batch }),
      signal: AbortSignal.timeout(120000),
    });
    const recData = await recRes.json();
    if (!recData.ok) {
      console.error("  recover-stale-stocks failed:", JSON.stringify(recData));
    } else {
      const r = recData.recovery || {};
      totalFresh += recData.fresh || 0;
      totalRecovered += r.recovered || 0;
      totalStocks += r.restoredStocks || 0;
      console.log(`  fresh=${recData.fresh} recovered=${r.recovered} restoredStocks=${r.restoredStocks}`);
    }
  }

  console.log(`\nDone. fixed=${fixData.fixed} products=${productIds.length} fresh=${totalFresh} recovered=${totalRecovered} restoredStocks=${totalStocks}`);
}

main().catch((e) => { console.error("FAILED:", e.message); process.exit(1); });
