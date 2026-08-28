#!/usr/bin/env node
"use strict";
const https = require("http");

// Call the preview API on prod to see the computed price
async function apiGet(path) {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: "81.17.154.153",
      port: 3000,
      path,
      method: "GET",
      headers: {
        "Authorization": "Basic " + Buffer.from("david:CGJ-Ge-90").toString("base64"),
        "Content-Type": "application/json",
      },
    };
    const req = https.request(opts, (res) => {
      let data = "";
      res.on("data", (c) => data += c);
      res.on("end", () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on("error", reject);
    req.setTimeout(15000, () => reject(new Error("timeout")));
    req.end();
  });
}

async function apiPost(path, body) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const opts = {
      hostname: "81.17.154.153",
      port: 3000,
      path,
      method: "POST",
      headers: {
        "Authorization": "Basic " + Buffer.from("david:CGJ-Ge-90").toString("base64"),
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(payload),
      },
    };
    const req = https.request(opts, (res) => {
      let data = "";
      res.on("data", (c) => data += c);
      res.on("end", () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on("error", reject);
    req.setTimeout(30000, () => reject(new Error("timeout")));
    req.write(payload);
    req.end();
  });
}

async function main() {
  // Get K18001 from warehouse API
  console.log("\n=== Fetching K18001 from warehouse API ===\n");
  const res = await apiGet("/api/warehouse/products?search=K18001&limit=5");
  console.log(`Status: ${res.status}`);
  if (res.status === 200 && res.body?.data) {
    const products = res.body.data.filter((p) => p.offerId === "K18001" || p.offer_id === "K18001");
    for (const p of products) {
      console.log(`[${p.marketplace}] id=${p.id}`);
      console.log(`  currentPrice=${p.currentPrice || p.current_price} targetPrice=${p.targetPrice || p.target_price}`);
      console.log(`  nextPrice=${p.nextPrice || p.next_price}`);
      if (p.selectedSupplier) {
        const s = p.selectedSupplier;
        console.log(`  supplier: ${s.partnerName || s.supplierName} price=${s.price} currency=${s.priceCurrency || s.currency} coefficient=${s.markupCoefficient} finalPrice=${s.effectiveFinalPrice || s.calculatedPrice}`);
      }
    }
  } else {
    console.log(JSON.stringify(res.body).slice(0, 500));
  }

  // Get price breakdown for K18001
  console.log("\n=== Fetching price breakdown ===\n");
  const res2 = await apiGet("/api/warehouse/prices/breakdown?search=K18001&limit=5");
  console.log(`Status: ${res2.status}`);
  if (res2.status === 200 && res2.body?.data) {
    for (const item of res2.body.data.filter((p) => p.offerId === "K18001" || p.offer_id === "K18001")) {
      console.log(`[${item.marketplace}] offerId=${item.offerId}`);
      console.log(`  currentPrice=${item.currentPrice} nextPrice=${item.nextPrice} usdRate=${item.usdRate} markup=${item.markupCoefficient}`);
      if (item.selectedSupplier) {
        const s = item.selectedSupplier;
        console.log(`  supplier: ${s.partnerName} price=${s.price} currency=${s.priceCurrency} markupCoef=${s.markupCoefficient} finalPrice=${s.effectiveFinalPrice}`);
      }
    }
  } else {
    console.log(JSON.stringify(res2.body).slice(0, 1000));
  }

  // Also check ЮК345754
  console.log("\n=== Fetching ЮК345754 from warehouse API ===\n");
  const res3 = await apiGet("/api/warehouse/products?search=%D0%AE%D0%9A345754&limit=5");
  console.log(`Status: ${res3.status}`);
  if (res3.status === 200 && res3.body?.data) {
    const products2 = res3.body.data.filter((p) => (p.offerId || p.offer_id) === "ЮК345754");
    for (const p of products2) {
      console.log(`[${p.marketplace}] currentPrice=${p.currentPrice || p.current_price} targetPrice=${p.targetPrice || p.target_price} nextPrice=${p.nextPrice || p.next_price}`);
      if (p.selectedSupplier) {
        const s = p.selectedSupplier;
        console.log(`  supplier: ${s.partnerName || s.supplierName} price=${s.price} currency=${s.priceCurrency || s.currency} coefficient=${s.markupCoefficient} finalPrice=${s.effectiveFinalPrice || s.calculatedPrice}`);
      }
    }
  }
}

main().catch((e) => { console.error(e.message); process.exit(1); });
