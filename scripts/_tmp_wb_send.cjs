#!/usr/bin/env node
"use strict";

// Цепочка WB на проде через публичный API davidsklad.ru с админ-сессией.
// Режимы: probe (read-only: ping, rules, preview) | apply (импорт карточек)
// | media (досылка фото) | prices (отправка цен) | stocks (синк остатков).

const fs = require("node:fs");
const path = require("node:path");

const BASE = process.env.WB_CHAIN_BASE || "https://davidsklad.ru";
const mode = process.argv[2] || "probe";

function readEnvCreds() {
  const text = fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8");
  const get = (key) => {
    const match = text.match(new RegExp(`^${key}=(.*)$`, "m"));
    return match ? match[1].trim().replace(/^"|"$/g, "") : "";
  };
  return { username: get("APP_USER"), password: get("APP_PASSWORD") };
}

let cookie = "";

async function api(method, urlPath, body) {
  const response = await fetch(`${BASE}${urlPath}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const setCookie = response.headers.get("set-cookie");
  if (setCookie) cookie = setCookie.split(";")[0];
  const text = await response.text();
  let data = null;
  try { data = JSON.parse(text); } catch { data = text.slice(0, 400); }
  return { status: response.status, data };
}

function show(title, result) {
  console.log(`\n=== ${title} (HTTP ${result.status}) ===`);
  console.log(typeof result.data === "string" ? result.data : JSON.stringify(result.data, null, 2).slice(0, 4000));
}

async function main() {
  const creds = readEnvCreds();
  if (!creds.username || !creds.password) throw new Error("APP_USER/APP_PASSWORD не найдены в .env");
  const login = await api("POST", "/api/login", creds);
  if (login.status !== 200) {
    show("login FAILED", login);
    process.exit(1);
  }
  console.log(`login ok: ${login.data.username} (${login.data.role})`);

  if (mode === "probe") {
    show("wb/ping", await api("GET", "/api/wb/ping"));
    show("wb/import/rules", await api("GET", "/api/wb/import/rules"));
    show("wb/import/preview", await api("POST", "/api/wb/import/preview", {}));
    return;
  }
  if (mode === "subjects") {
    show("wb/subjects", await api("GET", `/api/wb/subjects?query=${encodeURIComponent(process.argv[3] || "духи")}`));
    return;
  }
  if (mode === "set-subject") {
    const subjectId = Number(process.argv[3]);
    const subjectName = process.argv[4] || "";
    if (!subjectId) throw new Error("usage: set-subject <subjectId> [name]");
    show("PUT rules", await api("PUT", "/api/wb/import/rules", { subjectId, subjectName }));
    return;
  }
  if (mode === "apply") {
    show("wb/import/apply", await api("POST", "/api/wb/import/apply", { limit: 1000 }));
    return;
  }
  if (mode === "cards") {
    const result = await api("GET", "/api/wb/cards?limit=1000");
    console.log(`cards total: ${result.data?.total}`);
    for (const card of (result.data?.cards || []).slice(0, 40)) {
      console.log(`  nmID=${card.nmID} vendorCode=${card.vendorCode} photos=${Array.isArray(card.photos) ? card.photos.length : 0} ${String(card.title || "").slice(0, 50)}`);
    }
    return;
  }
  if (mode === "errors") {
    show("wb/cards/errors", await api("GET", "/api/wb/cards/errors"));
    return;
  }
  if (mode === "media") {
    show("wb/media/backfill", await api("POST", "/api/wb/media/backfill", { limit: 500 }));
    return;
  }
  if (mode === "prices") {
    show("wb/prices/send", await api("POST", "/api/wb/prices/send", { dryRun: process.argv[3] === "dry" }));
    return;
  }
  if (mode === "warehouses") {
    show("wb/warehouses", await api("GET", "/api/wb/warehouses"));
    return;
  }
  if (mode === "stocks") {
    const warehouseId = Number(process.argv[3] || 0) || undefined;
    show("wb/stocks/sync", await api("POST", "/api/wb/stocks/sync", { warehouseId, dryRun: process.argv[4] === "dry" }));
    return;
  }
  throw new Error(`unknown mode: ${mode}`);
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
