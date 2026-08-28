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
      hostname: "127.0.0.1", port, path: urlPath, method,
      headers: { ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}), ...(cookie ? { Cookie: cookie } : {}) },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => { try { resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(data) }); } catch { resolve({ status: res.statusCode, headers: res.headers, body: data }); } });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function main() {
  const lr = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = (lr.headers["set-cookie"] || []).find((c) => String(c).startsWith("pm_session="))?.split(";")[0];
  if (!cookie) { console.error("Login failed:", lr.status); process.exit(1); }

  const sr = await request("GET", "/api/settings", { cookie });
  const s = sr.body.settings || sr.body;
  console.log("=== DEFAULT MARKUPS ===");
  console.log(JSON.stringify(s.defaultMarkups, null, 2));
  console.log("\n=== MARKUP RULES ===");
  console.log(JSON.stringify(s.markupRules, null, 2));
  console.log("\n=== AVAILABILITY RULES ===");
  console.log(JSON.stringify(s.availabilityRules, null, 2));
}

main().catch((e) => { console.error(e.message || String(e)); process.exit(1); });
