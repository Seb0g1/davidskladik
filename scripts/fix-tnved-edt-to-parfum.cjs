#!/usr/bin/env node
"use strict";
// Заменить ТН ВЭД «Туалетная вода» (3303009000) на «Духи» (3303001000):
// находим все товары в категории «Парфюмерия» (descCatId=17028988) и выставляем 3303001000.
//
// Использование:
//   node scripts/fix-tnved-edt-to-parfum.cjs --shop mv [--dry]
//   node scripts/fix-tnved-edt-to-parfum.cjs --shop aura [--dry]  (запускать на сервере)

require("dotenv").config();
const https = require("node:https");
const fs = require("node:fs");

const ARGS = process.argv.slice(2);
const DRY_RUN = ARGS.includes("--dry");
const SHOP = (ARGS.find(a => a.startsWith("--shop="))?.split("=")[1]) || (ARGS[ARGS.indexOf("--shop") + 1]) || "mv";

const ACCOUNTS_FILE = "/var/www/davidsklad/davidskladik/data/marketplace-accounts.json";

let CLIENT_ID, API_KEY;

if (SHOP === "mv") {
  CLIENT_ID = process.env.OZON_CLIENT_ID;
  API_KEY = process.env.OZON_API_KEY;
  if (!CLIENT_ID || !API_KEY) { console.error("Нет OZON_CLIENT_ID/OZON_API_KEY в .env"); process.exit(1); }
} else if (SHOP === "aura") {
  if (fs.existsSync(ACCOUNTS_FILE)) {
    const d = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8"));
    const aura = (d.accounts || []).find(a => a.id === "ozon-3d10ec43");
    if (!aura?.apiKey) { console.error("AURA account не найден в accounts.json"); process.exit(1); }
    CLIENT_ID = String(aura.clientId || "2533393");
    API_KEY = aura.apiKey;
  } else {
    CLIENT_ID = process.env.AURA_OZON_CLIENT_ID || "2533393";
    API_KEY = process.env.AURA_OZON_API_KEY;
    if (!API_KEY) { console.error("AURA: нет accounts.json и AURA_OZON_API_KEY"); process.exit(1); }
  }
} else { console.error("--shop: mv | aura"); process.exit(1); }

// Ozon category IDs for perfume
const PARFUM_CAT_IDS = new Set([17028988]); // Парфюмерия

const TNVED_ATTR_ID = 22232;
const MARKING_ATTR_ID = 23536;
const NEW_CODE = "3303001000";

function ozonRequest(endpoint, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: "api-seller.ozon.ru",
      path: endpoint, method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Client-Id": CLIENT_ID, "Api-Key": API_KEY,
        "Content-Length": Buffer.byteLength(payload),
      },
    }, res => {
      let data = "";
      res.on("data", c => data += c);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 400) reject(new Error(`Ozon ${res.statusCode}: ${JSON.stringify(json).slice(0, 300)}`));
          else resolve(json);
        } catch { reject(new Error(`Parse: ${data.slice(0, 200)}`)); }
      });
    });
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function chunks(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) result.push(arr.slice(i, i + size));
  return result;
}

async function getAllOfferIds() {
  const offerIds = [];
  let lastId = "";
  let page = 0;
  while (true) {
    const data = await ozonRequest("/v3/product/list", {
      filter: { visibility: "ALL" },
      last_id: lastId,
      limit: 1000,
    });
    const items = data.result?.items || [];
    offerIds.push(...items.map(i => i.offer_id).filter(Boolean));
    lastId = data.result?.last_id || "";
    page++;
    process.stdout.write(`\r   Загружено: ${offerIds.length}`);
    if (!lastId || items.length < 1000 || page > 200) break;
    await sleep(300);
  }
  process.stdout.write("\n");
  return offerIds;
}

async function getProductCategories(offerIds) {
  // Returns Map: offerId -> descCatId
  const result = new Map();
  for (const chunk of chunks(offerIds, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk });
      for (const item of (data.items || [])) {
        if (item.offer_id) result.set(item.offer_id, Number(item.description_category_id || 0));
      }
      await sleep(200);
    } catch (err) {
      console.error(`  ⚠ info chunk: ${err.message}`);
    }
  }
  return result;
}

async function main() {
  console.log(`\n🔄 Замена ТН ВЭД → Духи (${NEW_CODE}) [${SHOP.toUpperCase()}]${DRY_RUN ? " DRY" : ""}`);
  console.log(`   Client-Id: ${CLIENT_ID}\n`);

  // 1. Fetch dict entry for 3303001000
  console.log("📚 Ищу «Духи» в словаре Ozon...");
  const dictData = await ozonRequest("/v1/description-category/attribute/values", {
    description_category_id: 17028988, type_id: 93403, attribute_id: TNVED_ATTR_ID,
    language: "DEFAULT", last_value_id: 0, limit: 100,
  });
  const parfumEntry = (dictData.result || []).find(e => String(e.value).startsWith(NEW_CODE));
  if (!parfumEntry) { console.error("❌ 3303001000 не найден в словаре"); process.exit(1); }
  console.log(`   Найден: ${parfumEntry.value} (id: ${parfumEntry.id})\n`);

  // 2. Get all offer_ids
  console.log("📦 Загружаю список товаров...");
  const allOfferIds = await getAllOfferIds();
  console.log(`   Всего: ${allOfferIds.length}`);

  // 3. Get category for each product
  console.log("🔍 Получаю категории товаров...");
  const catMap = await getProductCategories(allOfferIds);
  console.log(`   Получено: ${catMap.size}`);

  // 4. Filter perfume category
  const parfumOfferIds = allOfferIds.filter(id => PARFUM_CAT_IDS.has(catMap.get(id)));
  console.log(`\n🎯 Товаров в категории «Парфюмерия» (${[...PARFUM_CAT_IDS].join(",")}): ${parfumOfferIds.length}`);

  if (!parfumOfferIds.length) { console.log("✅ Нечего менять."); return; }
  if (DRY_RUN) { console.log(`\n🔍 DRY RUN — выход. Примеры: ${parfumOfferIds.slice(0, 5).join(", ")}`); return; }

  // 5. Update all to 3303001000
  const updateItems = parfumOfferIds.map(offerId => ({
    offer_id: offerId,
    attributes: [
      { id: TNVED_ATTR_ID, values: [{ value: parfumEntry.value, dictionary_value_id: Number(parfumEntry.id) }] },
      { id: MARKING_ATTR_ID, values: [{ value: "false" }] },
    ],
  }));

  console.log(`\n📤 Обновляю ${updateItems.length} товаров...`);
  let updated = 0, errors = 0;
  const batches = chunks(updateItems, 100);
  for (let i = 0; i < batches.length; i++) {
    try {
      await ozonRequest("/v1/product/attributes/update", { items: batches[i] });
      updated += batches[i].length;
      console.log(`  Батч ${i + 1}/${batches.length}: ✅ ${batches[i].length} (всего: ${updated})`);
    } catch (err) {
      errors += batches[i].length;
      console.error(`  Батч ${i + 1}/${batches.length}: ❌ ${err.message}`);
    }
    if (i < batches.length - 1) await sleep(1000);
  }
  console.log(`\n🏁 Готово! Обновлено: ${updated} | Ошибок: ${errors}`);
}

main().catch(err => { console.error("💥", err.message); process.exit(1); });
