#!/usr/bin/env node
"use strict";

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
  if (login.status !== 200) throw new Error(`login failed: ${login.status}`);
  console.log("login ok:", login.data.username);

  // Fetch Volume values for духи/тут воды
  const r = await api("GET", "/api/avito/categories/dukhi_i_tualetnaya_voda/fields/Volume/values");
  console.log("\n=== Volume values (status", r.status, ")===");
  if (r.data?.result) {
    for (const item of r.data.result) {
      console.log(JSON.stringify(item));
    }
  } else {
    console.log(JSON.stringify(r.data, null, 2).slice(0, 2000));
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
