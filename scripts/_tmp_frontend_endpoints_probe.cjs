#!/usr/bin/env node
"use strict";

// Read-only: прогон всех GET-эндпоинтов, которые использует фронтенд, на проде.
// Отчёт: статус, время, размер, warnings/error в теле. Ничего не меняет.

const fs = require("node:fs");
const path = require("node:path");

const BASE = "https://davidsklad.ru";

function readEnvCreds() {
  const text = fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8");
  const get = (key) => {
    const match = text.match(new RegExp(`^${key}=(.*)$`, "m"));
    return match ? match[1].trim().replace(/^"|"$/g, "") : "";
  };
  return { username: get("APP_USER"), password: get("APP_PASSWORD") };
}

const ENDPOINTS = [
  // Дашборд и системные
  ["dashboard", "/api/dashboard/summary"],
  ["live-status", "/api/live-status"],
  ["system-status", "/api/system/status"],
  ["session", "/api/session"],
  ["settings", "/api/settings"],
  ["marketplace-accounts", "/api/marketplace-accounts"],
  ["users", "/api/users"],
  ["users-stats", "/api/users/stats?period=week"],
  ["audit-log", "/api/audit-log?limit=20&q="],
  ["notifications", "/api/notifications"],
  ["daily-sync", "/api/daily-sync"],
  // Склад
  ["warehouse-page", "/api/warehouse/products/page?page=1&pageSize=40&grouped=true"],
  ["warehouse-brands", "/api/warehouse/brands"],
  ["warehouse-no-supplier", "/api/warehouse/no-supplier"],
  ["price-history", "/api/warehouse/prices/history?limit=30"],
  ["price-retry-queue", "/api/warehouse/prices/retry-queue"],
  ["ai-drafts", "/api/warehouse/ai-drafts?status=pending"],
  ["yandex-quality", "/api/warehouse/yandex-quality-candidates?cached=1&threshold=60&limit=20"],
  ["pricemaster-search", "/api/pricemaster/search?q=creed"],
  // Поставщики / закупки
  ["suppliers", "/api/suppliers"],
  ["supplier-picking-list", "/api/supplier-picking-list?limit=50"],
  ["picking-invoices", "/api/supplier-picking-list/invoices?period=week"],
  ["supplier-cart-draft", "/api/supplier-cart/draft"],
  ["supplier-cart-history", "/api/supplier-cart/history"],
  ["supplier-cart-pm-status", "/api/supplier-cart/pricemaster/status"],
  ["supplier-ledger", "/api/supplier-ledger/payments"],
  // Финансы / консигнация
  ["finance-summary", "/api/finance/summary?period=month"],
  ["finance-orders", "/api/finance/orders?period=week&limit=50"],
  ["finance-expenses", "/api/finance/expenses?period=month"],
  ["consignment-summary", "/api/consignment/summary"],
  ["consignment-items", "/api/consignment/items?q="],
  ["consignment-suppliers", "/api/consignment/suppliers"],
  ["consignment-payouts", "/api/consignment/payouts"],
  ["consignment-topups", "/api/consignment/topups"],
  // Операции / автоматизация
  ["operations", "/api/operations"],
  ["sales-automation-summary", "/api/sales-automation/summary"],
  ["problem-products", "/api/problem-products?limit=30"],
  ["unarchive-queue", "/api/ozon/unarchive-queue"],
  ["import-candidates", "/api/ozon-yandex-import/candidates?q=&limit=20"],
  ["import-refresh-status", "/api/ozon-yandex-import/refresh/status"],
  // Коммуникации
  ["reviews", "/api/reviews?marketplace=all&unanswered=true&limit=30"],
  ["questions", "/api/questions?marketplace=all&unanswered=true&limit=30"],
  ["chats", "/api/chats?marketplace=all"],
  ["review-templates", "/api/reviews/templates"],
  ["question-templates", "/api/questions/templates"],
  ["chat-templates", "/api/chats/templates"],
  // Avito / WB
  ["avito-listings", "/api/avito/listings"],
  ["avito-import-rules", "/api/avito/import/rules"],
  ["avito-feed-info", "/api/avito/feed-info"],
  ["avito-uploads", "/api/avito/uploads"],
  ["wb-import-rules", "/api/wb/import/rules"],
  ["wb-sync-status", "/api/wb/sync/status"],
  ["wb-media-backfill-status", "/api/wb/media-backfill/status"],
  ["wb-chain-result", "/api/wb/chain/result"],
];

let cookie = "";

async function api(urlPath) {
  const startedAt = Date.now();
  try {
    const response = await fetch(`${BASE}${urlPath}`, {
      headers: { ...(cookie ? { Cookie: cookie } : {}) },
      signal: AbortSignal.timeout(45000),
    });
    const text = await response.text();
    let data = null;
    try { data = JSON.parse(text); } catch { /* html/text */ }
    return { status: response.status, ms: Date.now() - startedAt, bytes: text.length, data };
  } catch (error) {
    return { status: 0, ms: Date.now() - startedAt, bytes: 0, error: error?.message || String(error) };
  }
}

async function main() {
  const creds = readEnvCreds();
  const login = await fetch(`${BASE}/api/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(creds),
  });
  const setCookie = login.headers.get("set-cookie");
  if (setCookie) cookie = setCookie.split(";")[0];
  if (login.status !== 200) throw new Error(`login failed: ${login.status}`);

  const problems = [];
  for (const [name, urlPath] of ENDPOINTS) {
    const r = await api(urlPath);
    const body = r.data || {};
    const warnings = Array.isArray(body.warnings) && body.warnings.length ? ` warnings=${JSON.stringify(body.warnings).slice(0, 120)}` : "";
    const bodyError = body.error ? ` bodyError=${String(body.error).slice(0, 100)}` : "";
    const flag = r.status !== 200 ? "!!" : (r.ms > 5000 ? "SLOW" : (warnings || bodyError ? "warn" : "ok"));
    console.log(`${flag.padEnd(4)} ${name.padEnd(26)} ${String(r.status).padEnd(4)} ${String(r.ms + "ms").padEnd(8)} ${String(r.bytes).padEnd(9)}${r.error ? " FETCH:" + r.error : ""}${bodyError}${warnings}`);
    if (r.status !== 200 || r.error || bodyError) problems.push(name);
  }
  console.log(`\nПроблемных: ${problems.length}${problems.length ? " → " + problems.join(", ") : ""}`);
}

main().catch((error) => { console.error(error.message); process.exit(1); });
