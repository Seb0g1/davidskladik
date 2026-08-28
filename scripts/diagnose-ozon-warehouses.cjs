#!/usr/bin/env node
// Diagnose Ozon warehouse IDs for all accounts in marketplace-accounts.json
// Usage: node scripts/diagnose-ozon-warehouses.cjs
"use strict";

const fs = require("fs");
const path = require("path");

const dataDir = path.join(__dirname, "..", "data");
const accountsPath = path.join(dataDir, "marketplace-accounts.json");

async function ozonPost(pathname, body, account) {
  const url = `https://api-seller.ozon.ru${pathname}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Client-Id": account.clientId,
      "Api-Key": account.apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  const text = await res.text();
  try {
    return { ok: res.ok, status: res.status, data: JSON.parse(text) };
  } catch (_) {
    return { ok: res.ok, status: res.status, data: text };
  }
}

async function discoverWarehouses(account) {
  const attempts = [
    { label: "/v1/warehouse/list", fn: () => ozonPost("/v1/warehouse/list", {}, account) },
    { label: "/v2/warehouse/list", fn: () => ozonPost("/v2/warehouse/list", {}, account) },
    {
      label: "/v1/delivery-method/list",
      fn: () => ozonPost("/v1/delivery-method/list", { filter: { status: "ACTIVE" }, limit: 50, offset: 0 }, account),
    },
  ];

  for (const { label, fn } of attempts) {
    try {
      const { ok, status, data } = await fn();
      if (!ok) {
        console.log(`  ${label} → ${status}: ${JSON.stringify(data?.message || data?.error || data).slice(0, 120)}`);
        continue;
      }
      const raw = data.result || data.warehouses || data.items || [];
      const list = Array.isArray(raw) ? raw : raw.warehouses || raw.items || [];
      const ids = list.map((w) => ({
        id: w.warehouse_id || w.id,
        name: w.warehouse_name || w.name || "",
      }));
      if (ids.length) {
        console.log(`  ${label} → OK`);
        return ids;
      }
      // delivery-method: parse differently
      if (Array.isArray(raw) && raw[0]?.warehouse_id) {
        const dmIds = raw.map((m) => ({ id: m.warehouse_id, name: m.warehouse_name || m.name || "" }));
        if (dmIds.length) {
          console.log(`  ${label} → OK (delivery-method)`);
          return dmIds;
        }
      }
      console.log(`  ${label} → OK but empty list`);
    } catch (e) {
      console.log(`  ${label} → ERROR: ${e.message}`);
    }
  }

  // Last resort: get warehouse_id from existing product stock
  try {
    const plist = await ozonPost("/v3/product/list", { filter: { visibility: "ALL" }, limit: 1, last_id: "" }, account);
    const offerId = plist.data?.result?.items?.[0]?.offer_id;
    if (offerId) {
      const stocks = await ozonPost("/v4/product/info/stocks", { filter: { offer_id: [offerId], visibility: "ALL" }, limit: 1, last_id: "" }, account);
      const warehouseIds = [];
      for (const item of stocks.data?.result?.items || []) {
        for (const s of item.stocks || []) {
          if (s.warehouse_id && s.warehouse_id !== 0) {
            warehouseIds.push({ id: String(s.warehouse_id), name: s.warehouse_name || "" });
          }
        }
      }
      if (warehouseIds.length) {
        console.log(`  /v4/product/info/stocks (offer_id=${offerId}) → OK`);
        return warehouseIds;
      }
    }
  } catch (e) {
    console.log(`  /v4/product/info/stocks → ERROR: ${e.message}`);
  }

  return [];
}

async function main() {
  let accounts = [];
  try {
    const raw = JSON.parse(fs.readFileSync(accountsPath, "utf8"));
    accounts = (Array.isArray(raw.accounts) ? raw.accounts : []).filter(
      (a) => a.marketplace === "ozon" || !a.marketplace,
    );
  } catch (e) {
    console.error("Cannot read marketplace-accounts.json:", e.message);
    process.exit(1);
  }

  if (!accounts.length) {
    console.log("No Ozon accounts found in marketplace-accounts.json");
    process.exit(0);
  }

  for (const account of accounts) {
    console.log(`\n=== Account: ${account.name || account.id} (clientId=${account.clientId}) ===`);
    if (!account.clientId || !account.apiKey) {
      console.log("  SKIPPED — missing clientId or apiKey");
      continue;
    }
    const warehouses = await discoverWarehouses(account);
    if (warehouses.length) {
      console.log(`  Found ${warehouses.length} warehouse(s):`);
      for (const w of warehouses) {
        console.log(`    ID=${w.id}  name="${w.name}"`);
        console.log(`    → env var: OZON_STOCK_WAREHOUSE_IDS_${(account.id || "ozon").replace(/[^a-z0-9]/gi, "_").toUpperCase()}=${w.id}`);
      }
    } else {
      console.log("  No warehouses found via any method.");
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
