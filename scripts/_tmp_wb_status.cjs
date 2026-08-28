#!/usr/bin/env node
"use strict";

// Read-only: состояние WB на проде — результат последней цепочки, правила,
// количество карточек и фото. Ничего не меняет.

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

let cookie = "";

async function api(method, urlPath, body) {
  const response = await fetch(`${BASE}${urlPath}`, {
    method,
    headers: { "Content-Type": "application/json", ...(cookie ? { Cookie: cookie } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const setCookie = response.headers.get("set-cookie");
  if (setCookie) cookie = setCookie.split(";")[0];
  const text = await response.text();
  let data = null;
  try { data = JSON.parse(text); } catch { data = text.slice(0, 300); }
  return { status: response.status, data };
}

async function main() {
  const creds = readEnvCreds();
  const login = await api("POST", "/api/login", creds);
  if (login.status !== 200) throw new Error(`login failed: ${login.status} ${JSON.stringify(login.data)}`);
  console.log(`login ok: ${login.data.username}`);

  const rules = await api("GET", "/api/wb/import/rules");
  console.log("\n=== rules ===");
  console.log(JSON.stringify(rules.data, null, 2));

  const chain = await api("GET", "/api/wb/chain/result");
  console.log("\n=== chain result (steps summary) ===");
  const result = chain.data?.result;
  if (result) {
    console.log(`mode=${result.mode} startedAt=${result.startedAt} finishedAt=${result.finishedAt}`);
    if (result.error) console.log("ERROR:", String(result.error).slice(0, 1500));
    for (const step of result.steps || []) {
      console.log(`[${step.at}] ${step.step}: ${JSON.stringify({ ...step, step: undefined, at: undefined }).slice(0, 700)}`);
    }
  } else {
    console.log("нет результата");
  }
  console.log("\n=== log tail ===");
  console.log(String(chain.data?.logTail || "").split("\n").slice(-25).join("\n"));

  const mediaBf = await api("GET", "/api/wb/media-backfill/status");
  console.log("\n=== media backfill status ===");
  console.log(JSON.stringify(mediaBf.data, null, 2));

  const sysStatus = await api("GET", "/api/system/status");
  const sys = sysStatus.data;
  console.log("\n=== system bullmq ===");
  console.log(JSON.stringify(sys?.bullmq || sys?.redis || null, null, 2));
  console.log("\n=== failed operation jobs ===");
  console.log(JSON.stringify((sys?.operations?.failed || sys?.jobs?.failed || []).slice(0, 10), null, 2));
  console.log("\n=== active operation jobs ===");
  console.log(JSON.stringify((sys?.operations?.active || []).slice(0, 5), null, 2));

  const cards = await api("GET", "/api/wb/cards?limit=5000");
  const list = cards.data?.cards || [];
  const withPhotos = list.filter((c) => Array.isArray(c.photos) && c.photos.length).length;
  console.log(`\n=== cards: total=${cards.data?.total} withPhotos=${withPhotos} ===`);
  for (const card of list.slice(0, 10)) {
    const charcs = Array.isArray(card.characteristics) ? card.characteristics.map((c) => c.name).join(",") : "";
    console.log(`  nmID=${card.nmID} vc=${card.vendorCode} photos=${(card.photos || []).length} charcs=[${charcs.slice(0, 120)}]`);
  }
}

main().catch((error) => { console.error(error.message); process.exit(1); });
