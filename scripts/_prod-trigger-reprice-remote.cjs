#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      path: urlPath,
      method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* keep */ }
        resolve({ status: res.statusCode, body: parsed, headers: res.headers });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const setCookie = headers["set-cookie"] || [];
  const list = Array.isArray(setCookie) ? setCookie : [setCookie];
  const session = list.find((item) => String(item).startsWith("pm_session="));
  return session ? String(session).split(";")[0] : "";
}

async function main() {
  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(login.headers);
  if (!cookie) throw new Error(`login failed: ${login.status}`);
  const reprice = await request("POST", "/api/sales-automation/run", {
    cookie,
    body: {
      force: true,
      onlyChanged: false,
      marketplace: "all",
      verify: true,
      limit: 50000,
      reason: "hard_currency_rule",
    },
  });
  console.log(JSON.stringify({ phase: "reprice", status: reprice.status, body: reprice.body }, null, 2));
}

main().catch((e) => { console.error(e.message); process.exit(1); });
