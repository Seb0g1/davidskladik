#!/usr/bin/env node
"use strict";

/**
 * Анализирует исторические наценки Яндекса из price_history.
 * Запускает dry-run restore-yandex-markups и показывает статистику по тирам.
 * Запуск: node scripts/analyze-yandex-markups-remote.cjs
 */

const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

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

async function pollJob(cookie, statusUrl, maxWaitMs = 180_000) {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    await sleep(2000);
    const res = await request("GET", statusUrl, { cookie });
    const job = res.body?.job || res.body;
    if (!job?.id) throw new Error(`Job not found (${res.status}): ${JSON.stringify(res.body)}`);
    if (job.status === "completed") return job;
    if (job.status === "failed") throw new Error(`Job failed: ${job.error || JSON.stringify(job)}`);
    process.stdout.write(`  [${job.status}] ${job.summary || "..."}\r`);
  }
  throw new Error("Timed out");
}

// Current Ozon coefficients from production markup rules (same as Yandex currently)
// Used for comparison in the output table
const CURRENT_OZON_RULES = [
  { minUsd: 0,   coeff: 16.3758 },
  { minUsd: 50,  coeff: 3.0772 },
  { minUsd: 60,  coeff: 2.9408 },
  { minUsd: 70,  coeff: 2.8 },
  { minUsd: 80,  coeff: 2.7 },
  { minUsd: 90,  coeff: 2.66 },
  { minUsd: 100, coeff: 2.58 },
  { minUsd: 120, coeff: 2.58 },
  { minUsd: 150, coeff: 2.58 },
  { minUsd: 200, coeff: 2.5 },
  { minUsd: 300, coeff: 2.5 },
  { minUsd: 500, coeff: 2.5 },
];

function getOzonCoeff(usd) {
  const rule = [...CURRENT_OZON_RULES].reverse().find((r) => usd >= r.minUsd);
  return rule?.coeff ?? null;
}

async function main() {
  if (!appPassword) { console.error("APP_PASSWORD missing in .env"); process.exit(1); }

  const lr = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(lr.headers);
  if (!cookie) { console.error("Login failed:", lr.status); process.exit(1); }
  console.log("Logged in. Starting dry-run analysis...\n");

  const startRes = await request("POST", "/api/warehouse/yandex/restore-markups", {
    cookie,
    body: { dryRun: true },
  });
  if (!startRes.body?.ok) {
    console.error("Failed to start job:", JSON.stringify(startRes.body, null, 2));
    process.exit(1);
  }
  const job = await pollJob(cookie, startRes.body.statusUrl, 180_000);
  const result = job.result;

  console.log(`\n=== РЕЗУЛЬТАТ DRY-RUN ===`);
  console.log(`Всего Яндекс: ${result.total}, с PM-привязкой: ${result.withLinks}`);
  console.log(`Найдено с историей цен: ${result.updated}, пропущено: ${result.skipped}, курс USD: ${result.usdRate}`);

  if (!result.tierStats?.length) {
    console.log("\nНет данных по тирам. Убедитесь, что сервер обновлён.");
    return;
  }

  console.log(`\n=== КОЭФФИЦИЕНТЫ ПО ЦЕНОВЫМ ТИРАМ (исторические Яндекс vs текущий Озон) ===`);
  console.log(`${"Тир PM (USD)".padEnd(16)} ${"N".padStart(5)} ${"median".padStart(8)} ${"avg".padStart(8)} ${"Озон сейчас".padStart(12)} ${"Разница%".padStart(9)}`);
  console.log("─".repeat(62));

  const suggestedRules = [];

  for (const tier of result.tierStats) {
    const label = tier.maxUsd ? `$${tier.minUsd}–$${tier.maxUsd}` : `$${tier.minUsd}+`;
    const ozonCoeff = getOzonCoeff(tier.minUsd);
    const diff = ozonCoeff ? Math.round(((tier.median - ozonCoeff) / ozonCoeff) * 100) : null;
    const diffStr = diff !== null ? `${diff > 0 ? "+" : ""}${diff}%` : "—";
    console.log(
      `${label.padEnd(16)} ${String(tier.count).padStart(5)} ${String(tier.median).padStart(8)} ${String(tier.avg).padStart(8)} ${ozonCoeff !== null ? String(ozonCoeff).padStart(12) : "".padStart(12)} ${diffStr.padStart(9)}`,
    );
    suggestedRules.push({ minUsd: tier.minUsd, median: tier.median, avg: tier.avg, ozon: ozonCoeff, count: tier.count });
  }

  // Suggest consolidated rule boundaries (group tiers with < 5 products)
  console.log(`\n=== ПРЕДЛАГАЕМЫЕ ПРАВИЛА ДЛЯ ЯНДЕКСА (median из истории) ===`);
  console.log("Минимальные тиры с ≥5 товарами:\n");

  // Group consecutive tiers with few products using the median of the consolidating tier
  const meaningful = suggestedRules.filter((r) => r.count >= 5);
  for (const r of meaningful) {
    console.log(`  { minUsd: ${String(r.minUsd).padEnd(4)}, coefficient: ${r.median} }   // n=${r.count}, avg=${r.avg}, ozon=${r.ozon || "—"}`);
  }

  console.log(`\n=== СРАВНЕНИЕ: текущие Яндекс-правила (=Озон) vs рекомендуемые ===`);
  const allBoundaries = Array.from(new Set([
    ...CURRENT_OZON_RULES.map((r) => r.minUsd),
    ...suggestedRules.map((r) => r.minUsd),
  ])).sort((a, b) => a - b);

  for (const minUsd of allBoundaries) {
    const ozon = getOzonCoeff(minUsd);
    const histTier = suggestedRules.find((r) => r.minUsd === minUsd);
    const suggested = histTier ? histTier.median : "—";
    console.log(`  $${String(minUsd).padEnd(4)}: сейчас=${ozon}, предлагается=${suggested}${histTier ? ` (n=${histTier.count})` : " (нет данных)"}`);
  }

  if (result.sampleSkipped?.length) {
    const byReason = {};
    // Count total skipped by reason via sampleSkipped (only sample, not full)
    for (const s of result.sampleSkipped) {
      byReason[s.reason] = (byReason[s.reason] || 0) + 1;
    }
    console.log(`\nПримеры пропущенных (всего ${result.skipped}):`);
    for (const s of result.sampleSkipped) {
      console.log(`  ${s.offerId}: ${s.reason}${s.markup !== undefined ? ` (markup=${s.markup})` : ""}${s.lastPrice ? ` lastPrice=${s.lastPrice}` : ""}`);
    }
  }
}

main().catch((e) => { console.error(e.message || String(e)); process.exit(1); });
