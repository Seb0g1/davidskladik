#!/usr/bin/env node
"use strict";

/**
 * Обновляет правила наценки Яндекса (markupRules[marketplace=yandex])
 * на основе медианных коэффициентов из истории цен.
 * Ozon и остальные правила НЕ трогает.
 *
 * Dry-run (показывает diff): node ... update-yandex-markup-rules-remote.cjs
 * Применить:                 node ... update-yandex-markup-rules-remote.cjs --apply
 */

const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";
const applyMode = process.argv.includes("--apply");

// Новые Яндекс-правила из анализа истории цен (median по тирам, n≥3)
// Источник: scripts/analyze-yandex-markups-direct.cjs, выполнено 2026-08-24
const NEW_YANDEX_RULES = [
  { minUsd: 0,   coefficient: 5.5873 },  // n=14
  { minUsd: 5,   coefficient: 4.9632 },  // n=130
  { minUsd: 10,  coefficient: 4.9254 },  // n=244
  { minUsd: 15,  coefficient: 4.4854 },  // n=349
  { minUsd: 20,  coefficient: 4.2965 },  // n=310
  { minUsd: 25,  coefficient: 4.2962 },  // n=311
  { minUsd: 30,  coefficient: 3.9298 },  // n=312
  { minUsd: 35,  coefficient: 3.9297 },  // n=283
  { minUsd: 40,  coefficient: 3.75   },  // n=267
  { minUsd: 45,  coefficient: 3.2732 },  // n=283
  { minUsd: 50,  coefficient: 3.0773 },  // n=211
  { minUsd: 55,  coefficient: 3.5255 },  // n=235
  { minUsd: 60,  coefficient: 3.3683 },  // n=365
  { minUsd: 70,  coefficient: 3.3683 },  // n=342
  { minUsd: 80,  coefficient: 3.1173 },  // n=330
  { minUsd: 90,  coefficient: 2.6558 },  // n=254
  { minUsd: 100, coefficient: 3.0427 },  // n=376
  { minUsd: 120, coefficient: 3.0427 },  // n=429
  { minUsd: 150, coefficient: 3.1691 },  // n=394
  { minUsd: 200, coefficient: 2.6914 },  // n=198
  { minUsd: 300, coefficient: 2.5774 },  // n=71
  { minUsd: 500, coefficient: 2.548  },  // n=11
].map((r) => ({ ...r, marketplace: "yandex" }));

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1", port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* raw */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = Array.isArray(headers["set-cookie"]) ? headers["set-cookie"] : [headers["set-cookie"]].filter(Boolean);
  const s = list.find((item) => String(item).startsWith("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

async function main() {
  if (!appPassword) { console.error("APP_PASSWORD missing in .env"); process.exit(1); }

  const lr = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(lr.headers);
  if (!cookie) { console.error("Login failed:", lr.status); process.exit(1); }

  // Получаем текущие настройки
  const sr = await request("GET", "/api/settings", { cookie });
  const settings = sr.body?.settings || sr.body;
  if (!settings?.markupRules) {
    console.error("Не удалось получить настройки:", JSON.stringify(sr.body, null, 2));
    process.exit(1);
  }

  const currentRules = Array.isArray(settings.markupRules) ? settings.markupRules : [];
  const currentYandex = currentRules.filter((r) => r.marketplace === "yandex");
  const otherRules = currentRules.filter((r) => r.marketplace !== "yandex");

  console.log(`=== ТЕКУЩИЕ ЯНДЕКС-ПРАВИЛА (${currentYandex.length} шт.) ===`);
  for (const r of currentYandex) {
    console.log(`  minUsd=${r.minUsd}, coefficient=${r.coefficient}`);
  }

  console.log(`\n=== НОВЫЕ ЯНДЕКС-ПРАВИЛА (${NEW_YANDEX_RULES.length} шт.) ===`);
  for (const r of NEW_YANDEX_RULES) {
    const old = currentYandex.find((c) => c.minUsd === r.minUsd);
    const marker = !old ? " [NEW]" : Math.abs(old.coefficient - r.coefficient) < 0.001 ? " [без изменений]" : ` [было: ${old.coefficient}]`;
    console.log(`  minUsd=${r.minUsd}, coefficient=${r.coefficient}${marker}`);
  }

  console.log(`\nOzon-правила (${otherRules.filter((r) => r.marketplace === "ozon").length} шт.) — НЕ ТРОГАЕМ`);

  if (!applyMode) {
    console.log("\nЗапустите с --apply чтобы сохранить.");
    return;
  }

  const newRules = [...otherRules, ...NEW_YANDEX_RULES];
  newRules.sort((a, b) => {
    if (a.marketplace !== b.marketplace) return a.marketplace.localeCompare(b.marketplace);
    return a.minUsd - b.minUsd;
  });

  console.log(`\nОтправляю PUT /api/settings с ${newRules.length} правилами (${otherRules.length} не-яндекс + ${NEW_YANDEX_RULES.length} яндекс)...`);

  const putRes = await request("PUT", "/api/settings", {
    cookie,
    body: {
      ...settings,
      markupRules: newRules,
    },
  });

  if (!putRes.body?.ok) {
    console.error("Ошибка сохранения:", JSON.stringify(putRes.body, null, 2));
    process.exit(1);
  }

  const savedRules = putRes.body?.settings?.markupRules || [];
  const savedYandex = savedRules.filter((r) => r.marketplace === "yandex");
  console.log(`\nСохранено! Яндекс-правил в БД: ${savedYandex.length}`);
  console.log(`Переценка поставлена в очередь: ${putRes.body.priceRepriceQueued ? "да" : "нет"} (${putRes.body.queued || 0} SKU)`);
  console.log("Готово.");
}

main().catch((e) => { console.error(e.message || String(e)); process.exit(1); });
