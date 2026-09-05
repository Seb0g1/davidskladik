#!/usr/bin/env node
"use strict";
// Переносит товары из «Управление маркировкой ТН ВЭД заполнен.xlsx»
// в шаблон Ozon (формат «Парфюмерия_05.09.2026.xlsx»).
// Заполняет только Артикул (col B) + ТН ВЭД (col K), остальные колонки пустые.
//
// Использование:
//   node scripts/build-tnved-ozon-template.cjs \
//     --src "C:\...\Управление маркировкой ТН ВЭД заполнен.xlsx" \
//     --template "C:\...\Парфюмерия_05.09.2026.xlsx" \
//     --out "C:\...\Шаблон ТН ВЭД обновление.xlsx" \
//     [--shop mv|aura] [--dry]

require("dotenv").config();
const https = require("node:https");
const fs = require("node:fs");
const path = require("node:path");
const xlsx = require("xlsx");

const ARGS = process.argv.slice(2);
const DRY_RUN = ARGS.includes("--dry");
const SHOP = (ARGS.find(a => a.startsWith("--shop="))?.split("=")[1])
  || (ARGS[ARGS.indexOf("--shop") + 1])
  || "mv";

function getArg(name) {
  const eqForm = ARGS.find(a => a.startsWith(`--${name}=`));
  if (eqForm) return eqForm.split("=").slice(1).join("=");
  const idx = ARGS.indexOf(`--${name}`);
  return idx >= 0 ? ARGS[idx + 1] : null;
}

const SRC_FILE = getArg("src");
const TEMPLATE_FILE = getArg("template");
const OUT_FILE = getArg("out") || path.join(
  path.dirname(SRC_FILE || process.cwd()),
  "Шаблон ТН ВЭД обновление.xlsx"
);

if (!SRC_FILE || !TEMPLATE_FILE) {
  console.error("Укажите --src и --template");
  process.exit(1);
}

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

function ozonRequestOnce(endpoint, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request({
      hostname: "api-seller.ozon.ru",
      path: endpoint, method: "POST",
      timeout: 30000,
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
    req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
    req.write(payload);
    req.end();
  });
}

async function ozonRequest(endpoint, body, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await ozonRequestOnce(endpoint, body);
    } catch (err) {
      if (i < retries - 1) await sleep(1000 * (i + 1));
      else throw err;
    }
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function chunks(arr, size) {
  const result = [];
  for (let i = 0; i < arr.length; i += size) result.push(arr.slice(i, i + size));
  return result;
}

async function getOfferIdsBySku(skuList) {
  const map = new Map();
  const numericSkus = skuList.map(id => Number(id)).filter(id => id > 0);
  console.log(`   Проход 1/2: батчи по 100 (маппинг по OZN-штрихкоду)...`);
  let done = 0;
  for (const chunk of chunks(numericSkus, 100)) {
    try {
      const data = await ozonRequest("/v3/product/info/list", { sku: chunk });
      for (const item of (data.items || [])) {
        if (!item.offer_id) continue;
        for (const barcode of (item.barcodes || [])) {
          const s = String(barcode);
          if (s.startsWith("OZN")) {
            const skuNum = Number(s.slice(3));
            if (!isNaN(skuNum)) map.set(skuNum, item.offer_id);
          }
        }
      }
      done += chunk.length;
      process.stdout.write(`\r   Обработано: ${done}/${numericSkus.length} | offer_ids: ${map.size}`);
      await sleep(200);
    } catch (err) {
      console.error(`\n   ⚠ chunk: ${err.message}`);
    }
  }
  process.stdout.write("\n");

  const missing = numericSkus.filter(s => !map.has(s));
  if (missing.length > 0) {
    console.log(`   Проход 2/2: ${missing.length} SKU без OZN-штрихкода...`);
    let done2 = 0;
    for (const sku of missing) {
      try {
        const data = await ozonRequest("/v3/product/info/list", { sku: [sku] });
        const item = data.items?.[0];
        if (item?.offer_id) map.set(sku, item.offer_id);
        done2++;
        if (done2 % 50 === 0) process.stdout.write(`\r   Обработано: ${done2}/${missing.length} | offer_ids: ${map.size}`);
        await sleep(150);
      } catch {}
    }
    process.stdout.write(`\r   Обработано: ${done2}/${missing.length} | offer_ids: ${map.size}\n`);
  }
  return map;
}

async function main() {
  console.log(`\n📋 Построение шаблона Ozon с ТН ВЭД [${SHOP.toUpperCase()}]`);
  console.log(`   Источник: ${SRC_FILE}`);
  console.log(`   Шаблон:   ${TEMPLATE_FILE}`);
  console.log(`   Выход:    ${OUT_FILE}\n`);

  // 1. Читаем файл маркировки
  console.log("📂 Читаю файл маркировки...");
  const srcWb = xlsx.readFile(SRC_FILE, { raw: true });
  const srcWs = srcWb.Sheets[srcWb.SheetNames[0]];
  const srcRows = xlsx.utils.sheet_to_json(srcWs, { header: 1, defval: "", raw: true });

  const srcData = [];
  for (let i = 3; i < srcRows.length; i++) {
    const row = srcRows[i];
    if (!row.some(v => v !== "")) continue;
    const sku = String(row[1] || "").trim();
    const name = String(row[0] || "").trim();
    const tnved = String(row[9] || "").trim();
    if (!sku || !tnved) continue;
    srcData.push({ sku, name, tnved });
  }
  console.log(`   Строк с ТН ВЭД: ${srcData.length}`);

  // 2. Получаем offer_ids
  const offerIdMap = await getOfferIdsBySku(srcData.map(d => d.sku));
  console.log(`   offer_ids найдено: ${offerIdMap.size}/${srcData.length}`);

  const rows = [];
  let missing = 0;
  for (const { sku, name, tnved } of srcData) {
    const offerId = offerIdMap.get(Number(sku));
    if (!offerId) { missing++; continue; }
    rows.push({ offerId, name, tnved });
  }
  console.log(`   Строк для шаблона: ${rows.length} (без offer_id: ${missing})`);

  if (!rows.length) { console.log("\n⚠ Нет строк для записи."); return; }
  if (DRY_RUN) { console.log("\n🔍 DRY RUN — выход."); return; }

  // 3. Читаем шаблон ЦЕЛИКОМ (сохраняем всё форматирование)
  console.log("\n📄 Читаю шаблон целиком...");
  const tmplWb = xlsx.readFile(TEMPLATE_FILE, { cellStyles: true });
  const tmplWs = tmplWb.Sheets[tmplWb.SheetNames[0]];

  // 4. Удаляем все существующие строки данных (row 4+, т.к. 0-based)
  // Заголовки шаблона: строки 0-3 (Excel строки 1-4)
  const tmplRange = xlsx.utils.decode_range(tmplWs["!ref"] || "A1:AV5");
  const lastDataCol = tmplRange.e.c;

  console.log(`   Очищаю ${Math.max(0, tmplRange.e.r - 3)} строк данных из шаблона...`);
  for (let r = 4; r <= tmplRange.e.r; r++) {
    for (let c = 0; c <= lastDataCol; c++) {
      delete tmplWs[xlsx.utils.encode_cell({ r, c })];
    }
  }

  // 5. Записываем новые строки данных начиная с row 4
  console.log(`   Записываю ${rows.length} строк...`);
  for (let i = 0; i < rows.length; i++) {
    const { offerId, name, tnved } = rows[i];
    const r = i + 4;

    // Col 0 = №
    tmplWs[xlsx.utils.encode_cell({ r, c: 0 })] = { t: "n", v: i + 1 };
    // Col 1 = Артикул* — ТЕКСТ (важно: t:"s", иначе Excel покажет как число)
    tmplWs[xlsx.utils.encode_cell({ r, c: 1 })] = { t: "s", v: String(offerId) };
    // Col 2 = Название товара (для справки)
    tmplWs[xlsx.utils.encode_cell({ r, c: 2 })] = { t: "s", v: name };
    // Col 10 = ТН ВЭД коды ЕАЭС*
    tmplWs[xlsx.utils.encode_cell({ r, c: 10 })] = { t: "s", v: tnved };
  }

  // 6. Обновляем диапазон листа
  tmplWs["!ref"] = xlsx.utils.encode_range({
    s: { r: 0, c: 0 },
    e: { r: rows.length + 3, c: lastDataCol },
  });

  // 7. Сохраняем
  console.log(`\n💾 Сохраняю в: ${OUT_FILE}`);
  xlsx.writeFile(tmplWb, OUT_FILE);
  console.log(`\n✅ Готово! ${rows.length} товаров записано в шаблон.`);
  if (missing > 0) console.log(`   ⚠ ${missing} товаров без offer_id`);
  console.log(`   Заполнены: Артикул (col B) + Название (col C) + ТН ВЭД (col K)`);
}

main().catch(err => { console.error("💥", err.message); process.exit(1); });
