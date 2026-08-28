#!/usr/bin/env node
"use strict";
const path = require("node:path");
const http = require("node:http");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000);
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

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
      res.on("data", c => data += c);
      res.on("end", () => {
        try { resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, headers: res.headers, body: data }); }
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = Array.isArray(headers["set-cookie"]) ? headers["set-cookie"] : [headers["set-cookie"]].filter(Boolean);
  const s = list.find(item => String(item).startsWith("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

async function main() {
  const lr = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(lr.headers);
  if (!cookie) { console.error("Login failed:", lr.status); process.exit(1); }

  const sr = await request("GET", "/api/settings", { cookie });
  const settings = sr.body?.settings || sr.body || {};
  const cart = settings.supplierCart || {};

  const before = cart.includeOzonStatuses || [];
  console.log("Before:", JSON.stringify(before));

  const after = before.filter(s => s.toUpperCase() !== "AWAITING_DELIVER");
  console.log("After:", JSON.stringify(after));

  if (before.length === after.length) {
    console.log("AWAITING_DELIVER not found — nothing to change.");
    return;
  }

  const putRes = await request("PUT", "/api/settings", {
    cookie,
    body: { ...settings, supplierCart: { ...cart, includeOzonStatuses: after } },
  });

  if (!putRes.body?.ok) {
    console.error("Save failed:", JSON.stringify(putRes.body).slice(0, 300));
    process.exit(1);
  }
  const saved = putRes.body?.settings?.supplierCart?.includeOzonStatuses;
  console.log("Saved:", JSON.stringify(saved));
  console.log("Done.");
}

main().catch(e => { console.error(e.message); process.exit(1); });
