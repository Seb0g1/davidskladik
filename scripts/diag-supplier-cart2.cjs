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

  // --- Supplier cart config ---
  const cart = settings.supplierCart || {};
  console.log("=== SUPPLIER CART CONFIG ===");
  console.log("includeOzonStatuses:", JSON.stringify(cart.includeOzonStatuses));
  console.log("includeYandexStatuses:", JSON.stringify(cart.includeYandexStatuses));
  console.log("includeYandexSubstatuses:", JSON.stringify(cart.includeYandexSubstatuses));
  console.log("lookbackHours:", cart.lookbackHours);
  console.log("mode:", cart.mode);

  // --- Managed suppliers ---
  const suppliers = Array.isArray(settings.managedSuppliers) ? settings.managedSuppliers : [];
  console.log("\n=== MANAGED SUPPLIERS ===");
  console.log("Total:", suppliers.length);

  const inna = suppliers.filter(s => /инна|inna/i.test(String(s.name || s.supplierName || s.partnerName || "")));
  if (inna.length) {
    console.log("\nИнна:");
    for (const s of inna) console.log(JSON.stringify(s, null, 2));
  } else {
    console.log("Инна НЕ найдена по имени. Все поставщики:");
    for (const s of suppliers) {
      const name = s.name || s.supplierName || s.partnerName || "(no name)";
      console.log(`  ${name} | stopped:${s.stopped} | stockOnly:${s.stockOnly} | cutoff:${s.orderCutoffTime || "-"} | trust:${s.trustFactor || "-"} | pricingMode:${s.pricingMode || "-"}`);
    }
  }

  // --- Active supplier blocks ---
  const blocksResp = await request("GET", "/api/supplier-cart/blocks", { cookie });
  const blocks = blocksResp.body?.blocks || blocksResp.body || [];
  console.log("\n=== ACTIVE SUPPLIER BLOCKS ===");
  if (Array.isArray(blocks)) {
    const innaBlocks = blocks.filter(b => /инна|inna/i.test(String(b.supplierName || "")));
    console.log("Total blocks:", blocks.length);
    if (innaBlocks.length) {
      console.log("Инна blocks:", JSON.stringify(innaBlocks, null, 2));
    } else {
      console.log("Инна not in blocks.");
      for (const b of blocks.slice(0, 10)) {
        console.log(`  ${b.supplierName} | ${b.offerId} | expires:${b.expiresAt} | reason:${b.reason}`);
      }
    }
  } else {
    console.log("blocks response:", JSON.stringify(blocksResp.body).slice(0, 300));
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
