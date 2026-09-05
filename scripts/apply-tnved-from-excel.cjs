#!/usr/bin/env node
"use strict";
// Расставить коды ТН ВЭД на товарах Ozon по файлам Маркировки.
// Использование:
//   node scripts/apply-tnved-from-excel.cjs --shop mv   # Magic Vibes (из .env)
//   node scripts/apply-tnved-from-excel.cjs --shop aura # AURA (нужны AURA_OZON_CLIENT_ID + AURA_OZON_API_KEY)
//   node scripts/apply-tnved-from-excel.cjs --dry       # без реальной отправки
//
// Переменные окружения для AURA:
//   AURA_OZON_CLIENT_ID=2533393
//   AURA_OZON_API_KEY=<ключ из личного кабинета AURA>

require("dotenv").config();
const fs = require("node:fs");
const https = require("node:https");
const path = require("node:path");
const XLSX = require("xlsx");

// ── Config ────────────────────────────────────────────────────────────────────

const ARGS = process.argv.slice(2);
const DRY_RUN = ARGS.includes("--dry");
const SHOP = (ARGS.find(a => a.startsWith("--shop="))?.split("=")[1]) || (ARGS[ARGS.indexOf("--shop") + 1]) || "mv";
const CUSTOM_FILE = (ARGS.find(a => a.startsWith("--file="))?.split("=").slice(1).join("=")) || (ARGS[ARGS.indexOf("--file") + 1]);

const SHOPS = {
  mv: {
    label: "Magic Vibes",
    excelFile: "C:/Users/Seb0g1/Downloads/Маркировка Товаров Magic Vibes.xlsx",
    clientId: process.env.OZON_CLIENT_ID,
    apiKey: process.env.OZON_API_KEY,
  },
  aura: {
    label: "AURA",
    excelFile: "C:/Users/Seb0g1/Downloads/Маркировка Товаров AURA.xlsx",
    clientId: process.env.AURA_OZON_CLIENT_ID || "2533393",
    apiKey: process.env.AURA_OZON_API_KEY,
  },
};

const shop = SHOPS[SHOP];
if (!shop) { console.error("Неизвестный --shop. Используй: mv | aura"); process.exit(1); }
if (CUSTOM_FILE) shop.excelFile = CUSTOM_FILE;
if (!shop.clientId || !shop.apiKey) {
  console.error(`Нет учётных данных для ${shop.label}.`);
  if (SHOP === "aura") console.error("Добавь в .env: AURA_OZON_CLIENT_ID=2533393 и AURA_OZON_API_KEY=<ключ>");
  process.exit(1);
}

// Коды ТН ВЭД по категориям Ozon (display names)
const CATEGORY_TO_TNVED = {
  "Парфюмерия":                           "3303001000", // Духи (не Туалетная вода 3303009000)
  "Косметика для ухода за волосами":       "3305900009", // Ozon: «Прочие средства для волос» (нет 3305900000)
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
  "Ватно-бумажная продукция":             "3401300000", // Ozon: нет 3401200000, ближайший — 3401300000
  "Моющие и чистящие средства":           "3402909000", // Ozon: нет 3402200000, ближайший — 3402909000
  "Свечи и подсвечники":                  "3406000000",
  // Аксессуары/инструменты/одежда — без кода ТН ВЭД косметики, пропускаем
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
const DEFAULT_TNVED = "3303001000"; // fallback для неизвестных категорий (духи)

// ── Ozon API helper ───────────────────────────────────────────────────────────

function ozonRequest(endpoint, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const options = {
      hostname: "api-seller.ozon.ru",
      path: endpoint,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Client-Id": shop.clientId,
        "Api-Key": shop.apiKey,
        "Content-Length": Buffer.byteLength(payload),
      },
    };
    const req = https.request(options, res => {
      let data = "";
      res.on("data", chunk => data += chunk);
      res.on("end", () => {
        try {
          const json = JSON.parse(data);
          if (res.statusCode >= 400) reject(new Error(`Ozon API ${res.statusCode}: ${JSON.stringify(json).slice(0, 300)}`));
          else resolve(json);
        } catch { reject(new Error(`Ozon API parse error: ${data.slice(0, 200)}`)); }
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

// ── Read Excel ────────────────────────────────────────────────────────────────

function readExcel(filePath) {
  const wb = XLSX.readFile(filePath);
  const ws = wb.Sheets[wb.SheetNames[0]];
  const data = XLSX.utils.sheet_to_json(ws, { header: 1 });
  // Row 0: filter header, Row 1: column headers, Row 2: descriptions, data from Row 3
  const headers = data[1];
  const skuIdx = headers.findIndex(h => String(h).replace(/\s/g, "").toUpperCase() === "SKU");
  const catIdx = headers.findIndex(h => String(h).trim().startsWith("Категория"));
  if (skuIdx < 0) throw new Error("Не найдена колонка SKU в файле: " + filePath);
  const rows = data.slice(3)
    .filter(r => r[skuIdx])
    .map(r => ({
      sku: Number(r[skuIdx]),
      categoryName: String(r[catIdx] || "").trim(),
      name: String(r[0] || "").trim(),
    }))
    .filter(r => r.sku > 0);
  return rows;
}

// ── Get offer_ids from Ozon marketplace skus ──────────────────────────────────
// Excel "SKU" column = Ozon marketplace FBO/FBS sku (not product_id/offer_id).
// /v3/product/info/list with { sku: [...] } returns offer_id and category info.

async function getProductInfoBySkus(skus) {
  const results = new Map(); // sku -> { offerId, descCatId, typeId, archived }
  for (const chunk of chunks(skus, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { sku: chunk });
      for (const item of (data.items || data.result?.items || [])) {
        const offerId = String(item.offer_id || "").trim();
        if (!offerId) continue;
        // item.sources[].sku links back to the queried FBO/FBS sku
        for (const src of (item.sources || [])) {
          const sku = Number(src.sku);
          if (sku) {
            results.set(sku, {
              offerId,
              descCatId: Number(item.description_category_id || 0),
              typeId: Number(item.type_id || 0),
              archived: !!item.is_archived,
            });
          }
        }
      }
      await sleep(300);
    } catch (err) {
      console.error(`  ⚠ Ошибка получения info для chunk (${chunk.length} SKU): ${err.message}`);
    }
  }
  return results;
}

// ── Fetch TNVED dict map ──────────────────────────────────────────────────────
// Получает полные значения из словаря Ozon для атрибута 22232 по категориям.
// Ozon требует отправлять не "3303009000", а полный текст с dictionary_value_id.

async function fetchTnvedDictMap(uniqueCategories) {
  const TNVED_ATTR_ID = 22232;
  const map = new Map(); // "3303009000" -> { value: "3303009000 - ...", dictionary_value_id: N }
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
          description_category_id: descCatId,
          type_id: typeId || 0,
          attribute_id: TNVED_ATTR_ID,
          language: "DEFAULT",
          last_value_id: lastValueId,
          limit: 100,
        });
        const items = data.result || [];
        for (const entry of items) {
          const text = String(entry.value || "").trim();
          const m = text.match(/^(\d{10})/);
          if (m && !map.has(m[1])) {
            map.set(m[1], { value: text, dictionary_value_id: Number(entry.id) || 0 });
          }
        }
        if (items.length < 100) break;
        lastValueId = items[items.length - 1]?.id || 0;
        if (!lastValueId) break;
        await sleep(200);
      }
      await sleep(300);
    } catch {
      // Категория может не поддерживать этот атрибут
    }
  }
  return map;
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n🚀 Применение ТН ВЭД — ${shop.label}${DRY_RUN ? " [DRY RUN]" : ""}`);
  console.log(`   Client-Id: ${shop.clientId}`);
  console.log(`   Файл: ${shop.excelFile}\n`);

  // 1. Читаем Excel
  console.log("📄 Читаю Excel...");
  const excelRows = readExcel(shop.excelFile);
  console.log(`   Товаров в файле: ${excelRows.length}`);

  // Статистика по категориям
  const catStats = new Map();
  for (const r of excelRows) {
    catStats.set(r.categoryName, (catStats.get(r.categoryName) || 0) + 1);
  }
  console.log("\n   Категории:");
  for (const [cat, cnt] of [...catStats.entries()].sort((a, b) => b[1] - a[1])) {
    const code = Object.prototype.hasOwnProperty.call(CATEGORY_TO_TNVED, cat)
      ? (CATEGORY_TO_TNVED[cat] || "⛔ пропустить")
      : `❓ ${DEFAULT_TNVED} (fallback)`;
    console.log(`     ${cnt.toString().padStart(5)} × ${cat} → ${code}`);
  }

  // 2. Получаем offer_ids по FBO/FBS sku из Excel
  const skus = excelRows.map(r => r.sku);
  console.log(`\n🔍 Запрашиваю offer_id для ${skus.length} товаров из Ozon API...`);
  const productInfoMap = await getProductInfoBySkus(skus);
  console.log(`   Получено: ${productInfoMap.size} из ${skus.length}`);

  const missing = skus.filter(sku => !productInfoMap.has(sku));
  if (missing.length) {
    console.log(`   ⚠ Не найдено ${missing.length} товаров: ${missing.slice(0, 5).join(", ")}${missing.length > 5 ? "..." : ""}`);
  }

  // 3. Строим список товаров для обновления
  const toUpdate = [];
  const skipped = [];
  for (const row of excelRows) {
    const info = productInfoMap.get(row.sku);
    if (!info) { skipped.push({ reason: "not_found", row }); continue; }

    let tnvedCode;
    if (Object.prototype.hasOwnProperty.call(CATEGORY_TO_TNVED, row.categoryName)) {
      tnvedCode = CATEGORY_TO_TNVED[row.categoryName];
    } else {
      tnvedCode = DEFAULT_TNVED;
    }
    if (!tnvedCode) { skipped.push({ reason: "no_code", row }); continue; }

    toUpdate.push({
      sku: row.sku,
      offerId: info.offerId,
      descCatId: info.descCatId,
      typeId: info.typeId,
      archived: info.archived,
      tnvedCode,
      categoryName: row.categoryName,
      name: row.name,
    });
  }

  console.log(`\n📋 К обновлению: ${toUpdate.length} товаров`);
  console.log(`   Пропущено: ${skipped.length} (нет в API: ${skipped.filter(s => s.reason === "not_found").length}, нет кода: ${skipped.filter(s => s.reason === "no_code").length})`);

  if (!toUpdate.length) {
    console.log("\n✅ Нечего обновлять.");
    return;
  }

  // 4. Загружаем словарь ТН ВЭД из Ozon для правильного форматирования значений
  console.log("\n📚 Загружаю словарь ТН ВЭД из Ozon...");
  const uniqueCats = [...new Map(toUpdate.map(p => [`${p.descCatId}:${p.typeId}`, p])).values()]
    .map(p => ({ descCatId: p.descCatId, typeId: p.typeId }));
  const tnvedDictMap = await fetchTnvedDictMap(uniqueCats);
  console.log(`   Записей в словаре: ${tnvedDictMap.size}`);

  // Покажем какие коды нашли
  const neededCodes = [...new Set(toUpdate.map(p => p.tnvedCode))];
  for (const code of neededCodes) {
    const entry = tnvedDictMap.get(code);
    console.log(`   ${code}: ${entry ? entry.value.slice(0, 80) : "❌ НЕ НАЙДЕН В СЛОВАРЕ"}`);
  }

  // 5. Формируем запросы на обновление
  const TNVED_ATTR_ID = 22232;
  const MARKING_ATTR_ID = 23536;

  const updateItems = [];
  const skippedNoDictEntry = [];
  for (const product of toUpdate) {
    const numericCode = product.tnvedCode.match(/^(\d{10})/)?.[1] || product.tnvedCode;
    const dictEntry = tnvedDictMap.get(numericCode);
    if (!dictEntry) {
      skippedNoDictEntry.push(product);
      continue;
    }
    updateItems.push({
      offer_id: product.offerId,
      attributes: [
        { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
        { id: MARKING_ATTR_ID, values: [{ value: "false" }] },
      ],
    });
  }

  if (skippedNoDictEntry.length) {
    console.log(`\n⚠ Пропущено (код не найден в словаре Ozon): ${skippedNoDictEntry.length}`);
    for (const p of skippedNoDictEntry.slice(0, 5)) {
      console.log(`   SKU ${p.sku} | cat:${p.descCatId}:${p.typeId} | code:${p.tnvedCode} | ${p.name.slice(0, 50)}`);
    }
  }

  console.log(`\n✅ Готово к отправке: ${updateItems.length} товаров`);

  if (DRY_RUN) {
    console.log("\n🔍 DRY RUN — реальная отправка пропущена. Примеры:");
    updateItems.slice(0, 5).forEach(item => {
      console.log(`   offer_id: ${item.offer_id}`);
      console.log(`   ТН ВЭД: ${item.attributes[0].values[0].value.slice(0, 60)}`);
    });
    return;
  }

  // 6. Отправляем батчами по 100
  console.log("\n📤 Отправляю в Ozon...");
  let updated = 0;
  let errors = 0;
  const batches = chunks(updateItems, 100);
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    try {
      await ozonRequest("/v1/product/attributes/update", { items: batch });
      updated += batch.length;
      console.log(`   Батч ${i + 1}/${batches.length}: ✅ ${batch.length} обновлено (всего: ${updated})`);
    } catch (err) {
      errors += batch.length;
      console.error(`   Батч ${i + 1}/${batches.length}: ❌ ошибка — ${err.message}`);
    }
    if (i < batches.length - 1) await sleep(1000); // Ozon rate limit
  }

  console.log(`\n🏁 Готово! Обновлено: ${updated} | Ошибок: ${errors} | Пропущено: ${skipped.length + skippedNoDictEntry.length}`);
  if (errors > 0) console.log("   Повтори команду завтра после 03:00 МСК если упёрся в лимит Ozon.");
}

main().catch(err => {
  console.error("\n💥 Критическая ошибка:", err.message);
  process.exit(1);
});
