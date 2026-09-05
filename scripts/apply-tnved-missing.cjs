#!/usr/bin/env node
"use strict";
// Найти все товары в магазине у которых НЕ задан ТН ВЭД (атрибут 22232)
// и проставить правильный код на основе категории Ozon.
//
// Использование:
//   node scripts/apply-tnved-missing.cjs --shop mv [--dry]
//   node scripts/apply-tnved-missing.cjs --shop aura [--dry]  (на сервере)

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
  if (!CLIENT_ID || !API_KEY) { console.error("Нет OZON_CLIENT_ID/OZON_API_KEY"); process.exit(1); }
} else if (SHOP === "aura") {
  if (fs.existsSync(ACCOUNTS_FILE)) {
    const d = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8"));
    const aura = (d.accounts || []).find(a => a.id === "ozon-3d10ec43");
    if (!aura?.apiKey) { console.error("AURA не найдена в accounts.json"); process.exit(1); }
    CLIENT_ID = String(aura.clientId || "2533393");
    API_KEY = aura.apiKey;
  } else {
    CLIENT_ID = process.env.AURA_OZON_CLIENT_ID || "2533393";
    API_KEY = process.env.AURA_OZON_API_KEY;
    if (!API_KEY) { console.error("AURA: нет API ключа"); process.exit(1); }
  }
} else { console.error("--shop: mv | aura"); process.exit(1); }

// Маппинг descCatId → TNVED код (берём из Ozon category IDs)
const CAT_TO_TNVED = {
  17028988: "3303001000", // Парфюмерия → Духи
  17028992: "3305900009", // Косметика для ухода за волосами → Прочие средства для волос
  17028991: "3304990000", // Декоративная косметика
  17028990: "3304990000", // Косметика для ухода
  17028993: "3307490000", // Ароматы для дома
  17028994: "3307200000", // Личная гигиена
  17027920: "3402909000", // Моющие и чистящие средства
  200001240: "3306100000", // Средства для гигиены полости рта
  200001242: "3401300000", // Ватно-бумажная продукция
};
// Fallback по первым цифрам descCatId для неизвестных подкатегорий парфюмерии
const DEFAULT_PARFUM_CAT = "3303001000";
const DEFAULT_COSMETIC_CAT = "3304990000";

// Категории которые пропускаем (не косметика)
const SKIP_CAT_IDS = new Set([
  // Авто, аксессуары, одежда, мебель и т.д. — добавим по мере обнаружения
]);

const TNVED_ATTR_ID = 22232;
const MARKING_ATTR_ID = 23536;

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

async function fetchOfferIdsByVisibility(visibility) {
  const offerIds = [];
  let lastId = "";
  let page = 0;
  while (true) {
    const data = await ozonRequest("/v3/product/list", {
      filter: { visibility }, last_id: lastId, limit: 1000,
    });
    const items = data.result?.items || [];
    offerIds.push(...items.map(i => i.offer_id).filter(Boolean));
    lastId = data.result?.last_id || "";
    page++;
    if (!lastId || items.length < 1000 || page > 200) break;
    await sleep(300);
  }
  return offerIds;
}

async function getAllOfferIds() {
  process.stdout.write("   [ALL] ");
  const active = await fetchOfferIdsByVisibility("ALL");
  process.stdout.write(`${active.length} активных\n`);
  await sleep(500);
  process.stdout.write("   [ARCHIVED] ");
  const archived = await fetchOfferIdsByVisibility("ARCHIVED");
  process.stdout.write(`${archived.length} архивных\n`);
  // Дедупликация на случай пересечения
  const seen = new Set(active);
  const combined = [...active];
  for (const id of archived) { if (!seen.has(id)) combined.push(id); }
  return combined;
}

async function getProductsWithoutTnved(offerIds) {
  const noTnved = [];
  const seen = new Set(); // offer_id уже проверенных
  let checked = 0;
  // Запрашиваем сначала с ALL, потом с ARCHIVED — у архивных Ozon иногда не возвращает данные через ALL
  for (const visibility of ["ALL", "ARCHIVED"]) {
    for (const chunk of chunks(offerIds, 100)) {
      try {
        const data = await ozonRequest("/v4/product/info/attributes", {
          filter: { offer_id: chunk, visibility },
          limit: 100, sort_by: "id", sort_dir: "asc",
        });
        for (const item of (data.result || [])) {
          if (seen.has(item.offer_id)) continue;
          seen.add(item.offer_id);
          const tnved = (item.attributes || []).find(a => a.id === TNVED_ATTR_ID);
          if (!tnved || !tnved.values?.[0]?.value) {
            noTnved.push(item.offer_id);
          }
        }
        checked += chunk.length;
        process.stdout.write(`\r   [${visibility}] Проверено: ${checked}/${offerIds.length * 2} | Без ТН ВЭД: ${noTnved.length}`);
        await sleep(200);
      } catch (err) {
        console.error(`\n  ⚠ attr chunk [${visibility}]: ${err.message}`);
      }
    }
  }
  process.stdout.write("\n");
  return noTnved;
}

async function getProductCategories(offerIds) {
  const result = new Map(); // offerId -> descCatId
  for (const chunk of chunks(offerIds, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk });
      for (const item of (data.items || [])) {
        if (item.offer_id) result.set(item.offer_id, Number(item.description_category_id || 0));
      }
      await sleep(200);
    } catch (err) { console.error(`  ⚠ info chunk: ${err.message}`); }
  }
  return result;
}

async function fetchTnvedDictMap(uniqueCatTypeIds) {
  const map = new Map(); // "3303001000" -> { value, dictionary_value_id }
  const seen = new Set();
  for (const { descCatId, typeId } of uniqueCatTypeIds) {
    const key = `${descCatId}:${typeId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    try {
      let lastId = 0;
      for (let p = 0; p < 10; p++) {
        const data = await ozonRequest("/v1/description-category/attribute/values", {
          description_category_id: descCatId, type_id: typeId || 0,
          attribute_id: TNVED_ATTR_ID, language: "DEFAULT",
          last_value_id: lastId, limit: 100,
        });
        const items = data.result || [];
        for (const e of items) {
          const text = String(e.value || "").trim();
          const m = text.match(/^(\d{10})/);
          if (m && !map.has(m[1])) map.set(m[1], { value: text, dictionary_value_id: Number(e.id) });
        }
        if (items.length < 100) break;
        lastId = items[items.length - 1]?.id || 0;
        if (!lastId) break;
        await sleep(150);
      }
      await sleep(200);
    } catch {}
  }
  return map;
}

async function getProductCatTypes(offerIds) {
  // Returns Map: offerId -> { descCatId, typeId }
  const result = new Map();
  for (const chunk of chunks(offerIds, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk });
      for (const item of (data.items || [])) {
        if (item.offer_id) {
          result.set(item.offer_id, {
            descCatId: Number(item.description_category_id || 0),
            typeId: Number(item.type_id || 0),
          });
        }
      }
      await sleep(200);
    } catch (err) { console.error(`  ⚠ info chunk: ${err.message}`); }
  }
  return result;
}

async function main() {
  console.log(`\n🔍 Поиск товаров без ТН ВЭД [${SHOP.toUpperCase()}]${DRY_RUN ? " DRY" : ""}`);
  console.log(`   Client-Id: ${CLIENT_ID}\n`);

  // 1. Get all offer_ids
  console.log("📦 Загружаю список товаров...");
  const allOfferIds = await getAllOfferIds();
  console.log(`   Всего: ${allOfferIds.length}`);

  // 2. Find those without TNVED
  console.log("\n🔍 Проверяю наличие ТН ВЭД...");
  const noTnvedIds = await getProductsWithoutTnved(allOfferIds);
  console.log(`   Без ТН ВЭД: ${noTnvedIds.length}`);

  if (!noTnvedIds.length) { console.log("\n✅ Все товары уже имеют ТН ВЭД!"); return; }

  // 3. Get category + typeId for products without TNVED
  console.log("\n📂 Получаю категории товаров без ТН ВЭД...");
  const catTypeMap = await getProductCatTypes(noTnvedIds);

  // 4. Fetch dict for unique categories
  console.log("📚 Загружаю словарь ТН ВЭД...");
  const uniqueCats = [...new Map([...catTypeMap.values()].map(c => [`${c.descCatId}:${c.typeId}`, c])).values()];
  const tnvedDict = await fetchTnvedDictMap(uniqueCats);
  console.log(`   Записей в словаре: ${tnvedDict.size}`);

  // 5. Assign TNVED codes
  const toUpdate = [];
  const skipped = [];
  const catStats = new Map();

  for (const offerId of noTnvedIds) {
    const cat = catTypeMap.get(offerId);
    if (!cat) { skipped.push({ offerId, reason: "no_cat" }); continue; }

    const { descCatId } = cat;

    // Determine TNVED code
    let tnvedCode = CAT_TO_TNVED[descCatId];
    if (!tnvedCode) {
      // Unknown category: skip non-cosmetics, use default for cosmetics
      // Most unknown cats in cosmetics shops are cosmetics sub-types
      tnvedCode = DEFAULT_COSMETIC_CAT;
    }

    const dictEntry = tnvedDict.get(tnvedCode);
    if (!dictEntry) { skipped.push({ offerId, reason: "no_dict", descCatId, tnvedCode }); continue; }

    catStats.set(descCatId, (catStats.get(descCatId) || 0) + 1);
    toUpdate.push({
      offer_id: offerId,
      attributes: [
        { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
        { id: MARKING_ATTR_ID, values: [{ value: "false" }] },
      ],
    });
  }

  console.log(`\n📋 К обновлению: ${toUpdate.length} | Пропущено: ${skipped.length}`);
  console.log("   По категориям (descCatId):");
  for (const [cat, cnt] of [...catStats.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15)) {
    console.log(`     ${cnt.toString().padStart(5)} × catId=${cat} → ${CAT_TO_TNVED[cat] || DEFAULT_COSMETIC_CAT}`);
  }

  if (skipped.filter(s => s.reason === "no_dict").length) {
    console.log("\n  ⚠ Без записи в словаре:");
    skipped.filter(s => s.reason === "no_dict").slice(0, 5).forEach(s =>
      console.log(`    offerId=${s.offerId} | catId=${s.descCatId} | code=${s.tnvedCode}`)
    );
  }

  if (!toUpdate.length) { console.log("\n✅ Нечего обновлять."); return; }
  if (DRY_RUN) { console.log("\n🔍 DRY RUN — выход."); return; }

  // 6. Send updates
  console.log(`\n📤 Отправляю ${toUpdate.length} товаров...`);
  let updated = 0, errors = 0;
  const batches = chunks(toUpdate, 100);
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
  console.log(`\n🏁 Готово! Обновлено: ${updated} | Ошибок: ${errors} | Пропущено: ${skipped.length}`);
}

main().catch(err => { console.error("💥", err.message); process.exit(1); });
