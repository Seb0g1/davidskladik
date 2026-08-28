#!/usr/bin/env node
"use strict";

// Read-only: хвост cron-лога ночной цепочки WB через /api/wb/chain/result.

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
  return { status: response.status, data: await response.json().catch(() => null) };
}

async function main() {
  const login = await api("POST", "/api/login", readEnvCreds());
  if (login.status !== 200) throw new Error(`login failed: ${login.status}`);

  const chain = await api("GET", "/api/wb/chain/result");
  console.log("=== cron log tail (data/wb-chain-cron.log) ===");
  console.log(chain.data?.cronLogTail || "<пусто или файла нет>");

  for (const p of ["/api/wb/sync/status", "/api/wb/media-backfill/status"]) {
    const r = await api("GET", p);
    console.log(`\n=== ${p} (${r.status}) ===`);
    console.log(JSON.stringify(r.data, null, 2));
  }
}

main().catch((error) => { console.error(error.message); process.exit(1); });
