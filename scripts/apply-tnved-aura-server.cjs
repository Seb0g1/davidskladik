#!/usr/bin/env node
"use strict";
// Серверный скрипт: расставить коды ТН ВЭД для AURA (Ozon).
// Читает учётные данные из /var/www/davidsklad/davidskladik/data/marketplace-accounts.json
// Читает товары из aura-skus.json (загружен локально)
// Запуск: node apply-tnved-aura-server.cjs [--dry]

const https = require("node:https");
const fs = require("node:fs");
const path = require("node:path");

const DRY_RUN = process.argv.includes("--dry");
const ACCOUNTS_FILE = "/var/www/davidsklad/davidskladik/data/marketplace-accounts.json";
const SKUS_FILE = path.join(__dirname, "aura-skus.json");

// Load credentials from server accounts file
const accountsData = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8"));
const auraAccount = (accountsData.accounts || []).find(a => a.id === "ozon-3d10ec43");
if (!auraAccount || !auraAccount.apiKey) {
  console.error("AURA account (ozon-3d10ec43) not found in", ACCOUNTS_FILE);
  process.exit(1);
}
const CLIENT_ID = String(auraAccount.clientId || "2533393");
const API_KEY = auraAccount.apiKey;

console.log(`\nAURA Client-Id: ${CLIENT_ID}`);
console.log(`AURA API key length: ${API_KEY.length}`);

// TNVED mapping (same as main script)
const CATEGORY_TO_TNVED = {
  "Парфюмерия":                           "3303001000",
  "Косметика для ухода за волосами":       "3305900009",
  "Декоративная косметика":               "3304990000",
  "Косметика для ухода":                  "3304990000",
  "Маска косметическая":                  "3304990000",
  "Средства для гигиены тела":            "3304990000",
  "Средства для депиляции":               "3304990000",
  "Кисти косметические":                  "3304990000",
  "Инструменты для макияжа, маникюра, бани и солярия": "3304990000",
  "Мочалки и спонжи":                     "3304990000",
  "Ароматы для дома":                     "3307490000",
  "Личная гигиена":                       "3307200000",
  "Товары личной гигиены":               "3307200000",
  "Средства для принятия ванны":          "3307200000",
  "Средства для бритья и груминг":        "3307100000",
  "Бритвенные принадлежности":            "3307100000",
  "Средства для гигиены полости рта":     "3306100000",
  "Ватно-бумажная продукция":             "3401300000",
  "Моющие и чистящие средства":           "3402909000",
  "Свечи и подсвечники":                  "3406000000",
  "Аксессуары":                           null,
  "Аксессуары для волос":                 null,
  "Автоаксессуары":                       null,
  "Полотенца и скатерти":                 null,
  "Мебель и оборудование для салонов красоты": null,
  "Спецодежда":                           null,
  "Декор и интерьер":                     null,
  "Зеркала":                              null,
  "Аппараты косметологические":           null,
  "Массаж":                               null,
  "Оборудование и материалы для тату-салона": null,
};
const DEFAULT_TNVED = "3303001000";

function ozonRequest(endpoint, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: "api-seller.ozon.ru",
      path: endpoint,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Client-Id": CLIENT_ID,
        "Api-Key": API_KEY,
        "Content-Length": Buffer.byteLength(payload),
      },
    }, res => {
      let data = "";
      res.on("data", c => data += c);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 400) reject(new Error(`Ozon API ${res.statusCode}: ${JSON.stringify(json).slice(0, 300)}`));
          else resolve(json);
        } catch { reject(new Error(`Parse error: ${data.slice(0, 200)}`)); }
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

async function getProductInfoBySkus(skus) {
  const results = new Map();
  for (const chunk of chunks(skus, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { sku: chunk });
      for (const item of (data.items || [])) {
        const offerId = String(item.offer_id || "").trim();
        if (!offerId) continue;
        for (const src of (item.sources || [])) {
          const sku = Number(src.sku);
          if (sku) results.set(sku, { offerId, descCatId: Number(item.description_category_id || 0), typeId: Number(item.type_id || 0) });
        }
      }
      await sleep(300);
    } catch (err) { console.error(`  ⚠ chunk error: ${err.message}`); }
  }
  return results;
}

async function fetchTnvedDictMap(uniqueCategories) {
  const TNVED_ATTR_ID = 22232;
  const map = new Map();
  const seen = new Set();
  for (const { descCatId, typeId } of uniqueCategories) {
    if (!descCatId) continue;
    const key = `${descCatId}:${typeId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    try {
      let lastValueId = 0;
      for (let page = 0; page < 10; page++) {
        const data = await ozonRequest("/v1/description-category/attribute/values", {
          description_category_id: descCatId, type_id: typeId || 0, attribute_id: TNVED_ATTR_ID,
          language: "DEFAULT", last_value_id: lastValueId, limit: 100,
        });
        const items = data.result || [];
        for (const entry of items) {
          const text = String(entry.value || "").trim();
          const m = text.match(/^(\d{10})/);
          if (m && !map.has(m[1])) map.set(m[1], { value: text, dictionary_value_id: Number(entry.id) || 0 });
        }
        if (items.length < 100) break;
        lastValueId = items[items.length - 1]?.id || 0;
        if (!lastValueId) break;
        await sleep(200);
      }
      await sleep(300);
    } catch {}
  }
  return map;
}

async function main() {
  console.log(`\n🚀 Применение ТН ВЭД — AURA${DRY_RUN ? " [DRY RUN]" : ""}\n`);

  const excelRows = JSON.parse(fs.readFileSync(SKUS_FILE, "utf8"));
  console.log(`📄 Товаров: ${excelRows.length}`);

  const catStats = new Map();
  for (const r of excelRows) catStats.set(r.categoryName, (catStats.get(r.categoryName) || 0) + 1);
  for (const [cat, cnt] of [...catStats.entries()].sort((a, b) => b[1] - a[1])) {
    const code = Object.prototype.hasOwnProperty.call(CATEGORY_TO_TNVED, cat)
      ? (CATEGORY_TO_TNVED[cat] || "⛔ пропустить") : `❓ ${DEFAULT_TNVED} (fallback)`;
    console.log(`  ${cnt.toString().padStart(5)} × ${cat} → ${code}`);
  }

  const skus = excelRows.map(r => r.sku);
  console.log(`\n🔍 Запрашиваю offer_id для ${skus.length} SKU...`);
  const productInfoMap = await getProductInfoBySkus(skus);
  console.log(`   Получено: ${productInfoMap.size} из ${skus.length}`);

  const toUpdate = [];
  const skipped = [];
  for (const row of excelRows) {
    const info = productInfoMap.get(row.sku);
    if (!info) { skipped.push({ reason: "not_found", row }); continue; }
    let tnvedCode = Object.prototype.hasOwnProperty.call(CATEGORY_TO_TNVED, row.categoryName)
      ? CATEGORY_TO_TNVED[row.categoryName] : DEFAULT_TNVED;
    if (!tnvedCode) { skipped.push({ reason: "no_code", row }); continue; }
    toUpdate.push({ sku: row.sku, offerId: info.offerId, descCatId: info.descCatId, typeId: info.typeId, tnvedCode, categoryName: row.categoryName });
  }

  console.log(`\n📋 К обновлению: ${toUpdate.length} | Пропущено: ${skipped.length}`);
  if (!toUpdate.length) { console.log("✅ Нечего обновлять."); return; }

  console.log("\n📚 Загружаю словарь ТН ВЭД...");
  const uniqueCats = [...new Map(toUpdate.map(p => [`${p.descCatId}:${p.typeId}`, p])).values()]
    .map(p => ({ descCatId: p.descCatId, typeId: p.typeId }));
  const tnvedDictMap = await fetchTnvedDictMap(uniqueCats);
  console.log(`   Записей: ${tnvedDictMap.size}`);

  const TNVED_ATTR_ID = 22232;
  const MARKING_ATTR_ID = 23536;
  const updateItems = [];
  const skippedDict = [];
  for (const product of toUpdate) {
    const dictEntry = tnvedDictMap.get(product.tnvedCode);
    if (!dictEntry) { skippedDict.push(product); continue; }
    updateItems.push({
      offer_id: product.offerId,
      attributes: [
        { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
        { id: MARKING_ATTR_ID, values: [{ value: "false" }] },
      ],
    });
  }

  if (skippedDict.length) {
    console.log(`⚠ Код не найден в словаре: ${skippedDict.length}`);
    skippedDict.forEach(p => console.log(`  SKU ${p.sku} | cat:${p.descCatId}:${p.typeId} | ${p.tnvedCode}`));
  }
  console.log(`\n✅ К отправке: ${updateItems.length}`);

  if (DRY_RUN) { console.log("🔍 DRY RUN — пропуск."); return; }

  console.log("\n📤 Отправляю в Ozon...");
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
  console.log(`\n🏁 Обновлено: ${updated} | Ошибок: ${errors} | Пропущено: ${skipped.length + skippedDict.length}`);
}

main().catch(err => { console.error("💥", err.message); process.exit(1); });
