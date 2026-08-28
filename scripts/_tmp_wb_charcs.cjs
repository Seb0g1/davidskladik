#!/usr/bin/env node
"use strict";

// Характеристики предмета WB 5522 «Селективный парфюм»: id, имя, тип, required.

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
  const result = await api("GET", "/api/wb/subjects/5522/characteristics");
  const list = result.data?.characteristics || [];
  console.log(`characteristics: ${list.length}`);
  for (const charc of list) {
    console.log(`${charc.charcID}\t${charc.name}\ttype=${charc.charcType}\trequired=${charc.required}\tunit=${charc.unitName || "-"}\tmax=${charc.maxCount || 0}`);
  }
}

main().catch((error) => { console.error(error.message); process.exit(1); });
