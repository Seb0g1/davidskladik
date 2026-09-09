#!/usr/bin/env node
"use strict";
// Reads product_id (SKU) list from an Excel file, resolves offer_id via Ozon API,
// and sets the correct ТН ВЭД code based on the Excel category column.
require("dotenv").config();
const path = require("node:path");
const { execSync } = require("node:child_process");

// ── Ozon accounts ────────────────────────────────────────────────────────────
const accounts = [];
if (process.env.OZON_CLIENT_ID && process.env.OZON_API_KEY) {
  accounts.push({ id: "ozon", clientId: process.env.OZON_CLIENT_ID, apiKey: process.env.OZON_API_KEY });
}
try {
  const shops = JSON.parse(process.env.YANDEX_SHOPS_JSON || "[]");
  // second ozon account sometimes stored separately
} catch {}
// Check for second ozon account
if (process.env.OZON2_CLIENT_ID && process.env.OZON2_API_KEY) {
  accounts.push({ id: "ozon-3d10ec43", clientId: process.env.OZON2_CLIENT_ID, apiKey: process.env.OZON2_API_KEY });
}

if (!accounts.length) { console.error("No Ozon credentials found"); process.exit(1); }

// ── Category → ТН ВЭД mapping (by Excel display name) ───────────────────────
const TNVED_BY_CAT_NAME = {
  "Парфюмерия": "3303001000",
  "Косметика для ухода за волосами": "3305900009",
  "Декоративная косметика": "3304990000",
  "Косметика для ухода": "3304990000",
  "Ароматы для дома": "3307490000",
  "Средства для гигиены тела": "3401300000",
  "Средства для гигиены полости рта": "3306100000",
  "Моющие и чистящие средства": "3402909000",
  "Личная гигиена": "3307200000",
  "Маска косметическая": "3304990000",
  "Средства для депиляции": "3307900009",
  "Средства для бритья и груминг": "3307900009",
};
const TNVED_DEFAULT = "3304990000";

const TNVED_ATTR_ID = 22232;
const MARKING_ATTR_ID = 23536;

const DRY_RUN = process.argv.includes("--dry-run");

async function ozonRequest(path, body, account) {
  const res = await fetch(`https://api-seller.ozon.ru${path}`, {
    method: "POST",
    headers: {
      "Client-Id": account.clientId,
      "Api-Key": account.apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Ozon API ${path} → ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

function chunkArray(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) result.push(arr.slice(i, i + size));
  return result;
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function ozonRequestWithRetry(path, body, account, retries = 3) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await ozonRequest(path, body, account);
    } catch (err) {
      if (err.message.includes("429") && attempt < retries) {
        const delay = 2000 * (attempt + 1);
        console.log(`  rate limited, retry in ${delay}ms...`);
        await sleep(delay);
        continue;
      }
      throw err;
    }
  }
}

// Read Excel using python helper script
function readExcel(filePath) {
  const helperPath = path.join(__dirname, "_read-tnved-excel.py");
  const result = execSync(`python3 "${helperPath}" "${filePath}"`, { encoding: "utf8" });
  return JSON.parse(result);
}

// Ozon dict cache: "catId:typeId:code" → { value, dictionary_value_id }
const dictCache = new Map();

async function fetchDictEntry(account, descCatId, typeId, code) {
  const key = `${descCatId}:${typeId}:${code}`;
  if (dictCache.has(key)) return dictCache.get(key);

  try {
    await sleep(200);
    const data = await ozonRequestWithRetry("/v1/product/attribute/dictionary/values", {
      attribute_id: TNVED_ATTR_ID,
      description_category_id: descCatId,
      type_id: typeId,
      language: "DEFAULT",
      limit: 5000,
      last_value_id: 0,
    }, account);
    for (const e of (data.result || [])) {
      const text = (e.value || "").trim();
      const m = text.match(/^(\d{10})/);
      if (m) {
        const k = `${descCatId}:${typeId}:${m[1]}`;
        if (!dictCache.has(k)) dictCache.set(k, { value: text, dictionary_value_id: Number(e.id) });
      }
    }
  } catch (err) {
    console.warn(`  dictFetch failed for cat=${descCatId} type=${typeId} code=${code}: ${err.message}`);
  }
  const entry = dictCache.get(key) || null;
  if (!entry) dictCache.set(key, null); // cache miss
  return entry;
}

async function main() {
  const excelPath = process.argv.find((a) => a.endsWith(".xlsx"));
  if (!excelPath) { console.error("Usage: node fix-tnved-by-sku.cjs <file.xlsx> [--dry-run]"); process.exit(1); }

  console.log(`Reading Excel: ${excelPath}`);
  const rows = readExcel(excelPath);
  console.log(`Loaded ${rows.length} rows`);

  // Group by category
  const byCat = {};
  for (const { sku, cat } of rows) {
    byCat[cat] = byCat[cat] || [];
    byCat[cat].push(sku);
  }
  console.log("Categories:", Object.entries(byCat).map(([c, s]) => `${c}: ${s.length}`).join(", "));
  console.log();

  if (DRY_RUN) console.log("DRY RUN — no updates will be sent\n");

  let totalUpdated = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  for (const account of accounts) {
    console.log(`\n=== Account: ${account.id} ===`);

    // Collect all SKUs, get offer_id + category info via product_id lookup
    const allSkus = rows.map((r) => r.sku);
    console.log(`Resolving ${allSkus.length} SKUs → offer_ids...`);

    // Map: sku → { offerId, descCatId, typeId }
    const skuInfo = new Map();
    for (const chunk of chunkArray(allSkus, 50)) {
      try {
        await sleep(300);
        const data = await ozonRequestWithRetry("/v3/product/info/list", {
          product_id: chunk,
        }, account);
        for (const item of (data.items || [])) {
          if (item.id && item.offer_id) {
            skuInfo.set(item.id, {
              offerId: (item.offer_id || "").trim(),
              descCatId: Number(item.description_category_id || 0),
              typeId: Number(item.type_id || 0),
            });
          }
        }
      } catch (err) {
        console.warn(`  chunk resolution failed: ${err.message}`);
      }
    }
    console.log(`Resolved ${skuInfo.size} products in account ${account.id}`);
    if (!skuInfo.size) { console.log("  → no products found for this account, skipping"); continue; }

    // Build update list
    const updateItems = [];

    for (const [catName, skus] of Object.entries(byCat)) {
      const expectedCode = TNVED_BY_CAT_NAME[catName] || TNVED_DEFAULT;

      for (const sku of skus) {
        const info = skuInfo.get(sku);
        if (!info) continue; // not in this account
        const { offerId, descCatId, typeId } = info;
        if (!offerId || !descCatId) { totalSkipped++; continue; }

        const dictEntry = await fetchDictEntry(account, descCatId, typeId, expectedCode);
        await sleep(100);
        if (!dictEntry) {
          console.warn(`  No dict entry for sku=${sku} offerId=${offerId} cat=${catName} code=${expectedCode} descCatId=${descCatId} typeId=${typeId}`);
          totalSkipped++;
          continue;
        }

        updateItems.push({
          offer_id: offerId,
          attributes: [
            { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
            { id: MARKING_ATTR_ID, values: [{ value: "false" }] },
          ],
        });
      }
    }

    console.log(`Sending ${updateItems.length} updates (skipped ${totalSkipped})...`);

    if (!DRY_RUN) {
      for (const chunk of chunkArray(updateItems, 100)) {
        try {
          await sleep(500);
          const result = await ozonRequestWithRetry("/v1/product/attributes/update", { items: chunk }, account);
          if (result.errors?.length) {
            console.warn(`  Partial errors in chunk: ${JSON.stringify(result.errors.slice(0, 3))}`);
            totalFailed += result.errors.length;
            totalUpdated += chunk.length - result.errors.length;
          } else {
            totalUpdated += chunk.length;
          }
        } catch (err) {
          console.error(`  Update chunk failed: ${err.message}`);
          totalFailed += chunk.length;
        }
      }
    } else {
      console.log("  DRY RUN: would update", updateItems.length, "products");
      console.log("  Sample:", updateItems.slice(0, 3).map((i) => `${i.offer_id}`).join(", "));
      totalUpdated += updateItems.length;
    }
  }

  console.log(`\nDone. updated=${totalUpdated} skipped=${totalSkipped} failed=${totalFailed}`);
}

main().catch((err) => { console.error(err.message); process.exit(1); });
