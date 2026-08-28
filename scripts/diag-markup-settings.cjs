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

  console.log("=== DEFAULT MARKUPS ===");
  console.log(JSON.stringify(settings.defaultMarkups || {}, null, 2));

  console.log("\n=== MARKUP RULES (yandex-scoped) ===");
  const rules = (settings.markupRules || []);
  const yandexRules = rules.filter(r => !r.marketplace || r.marketplace === "all" || r.marketplace === "yandex");
  console.log(`Total rules: ${rules.length}, Yandex-applicable: ${yandexRules.length}`);
  for (const r of yandexRules) {
    console.log(`  minUsd: ${r.minUsd} → coeff: ${r.coefficient} [marketplace: ${r.marketplace || "all"}]`);
  }

  console.log("\n=== AVAILABILITY RULES (yandex-scoped) ===");
  const availRules = (settings.availabilityRules || []);
  const yandexAvail = availRules.filter(r => !r.marketplace || r.marketplace === "all" || r.marketplace === "yandex");
  for (const r of yandexAvail) {
    console.log(`  minAvailable: ${r.minAvailableSuppliers} → delta: ${r.coefficientDelta} stock: ${r.targetStock} [marketplace: ${r.marketplace || "all"}]`);
  }

  // Fetch product 234123 from warehouse API
  console.log("\n=== PRODUCT 234123 FROM API ===");
  for (const q of ["234123"]) {
    const wp = await request("GET", `/api/warehouse/products/page?q=${q}&pageSize=5`, { cookie });
    const items = wp.body?.items || [];
    console.log(`Query "${q}": ${items.length} results`);
    for (const p of items.slice(0, 5)) {
      console.log(JSON.stringify({
        id: p.id,
        offerId: p.offerId,
        name: p.name?.slice(0, 60),
        marketplace: p.marketplace,
        markup: p.markup,
        markupSource: p.markupSource,
        markupCoefficient: p.markupCoefficient,
        priceFormula: p.priceFormula ? {
          markupCoefficient: p.priceFormula.markupCoefficient,
          baseMarkupCoefficient: p.priceFormula.baseMarkupCoefficient,
          usdRate: p.priceFormula.usdRate,
          selectedSupplierPrice: p.priceFormula.selectedSupplierPrice,
          selectedSupplierCurrency: p.priceFormula.selectedSupplierCurrency,
        } : null,
      }, null, 2));
    }
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
