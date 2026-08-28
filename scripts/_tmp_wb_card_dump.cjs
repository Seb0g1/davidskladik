#!/usr/bin/env node
"use strict";

// Дамп нескольких карточек WB целиком — проверить, какие данные реально в них.

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
  const result = await api("GET", "/api/wb/cards?limit=200");
  const cards = result.data?.cards || [];
  console.log(`total fetched: ${cards.length}`);
  // Одна карточка с фото и одна без — целиком.
  const withPhoto = cards.find((c) => Array.isArray(c.photos) && c.photos.length);
  const withoutPhoto = cards.find((c) => !(Array.isArray(c.photos) && c.photos.length));
  for (const [label, card] of [["WITH PHOTO", withPhoto], ["WITHOUT PHOTO", withoutPhoto]]) {
    console.log(`\n=== ${label} ===`);
    console.log(JSON.stringify(card, null, 2).slice(0, 3500));
  }
  // Сводка по заполненности.
  let noDescription = 0, noBrand = 0, noCharcs = 0, noPhotos = 0;
  for (const c of cards) {
    if (!String(c.description || "").trim()) noDescription += 1;
    if (!String(c.brand || "").trim()) noBrand += 1;
    if (!(Array.isArray(c.characteristics) && c.characteristics.length)) noCharcs += 1;
    if (!(Array.isArray(c.photos) && c.photos.length)) noPhotos += 1;
  }
  console.log(`\nиз ${cards.length}: без описания ${noDescription}, без бренда ${noBrand}, без характеристик ${noCharcs}, без фото ${noPhotos}`);
}

main().catch((error) => { console.error(error.message); process.exit(1); });
