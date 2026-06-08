#!/usr/bin/env node
"use strict";

require("dotenv").config();

const http = require("node:http");

function request(method, urlPath, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port: 3000,
      path: urlPath,
      method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...headers,
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* keep text */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
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
  const login = await request("POST", "/api/login", {
    username: process.env.APP_USER || "david",
    password: process.env.APP_PASSWORD || "",
  });
  const cookie = sessionCookie(login.headers);
  if (!cookie) throw new Error(`Login failed: HTTP ${login.status}`);
  const hdr = { Cookie: cookie };

  const [daily, page, pageAll, pageGrouped, live, health] = await Promise.all([
    request("GET", "/api/daily-sync", null, hdr),
    request("GET", "/api/warehouse/products/page?page=1&pageSize=8&linked=linked", null, hdr),
    request("GET", "/api/warehouse/products/page?page=1&pageSize=8", null, hdr),
    request("GET", "/api/warehouse/products/page?page=1&pageSize=40&grouped=true", null, hdr),
    request("GET", "/api/live-status", null, hdr),
    request("GET", "/api/health", null, hdr),
  ]);

  const items = Array.isArray(page.body?.items) ? page.body.items : [];
  console.log(JSON.stringify({
    daily: { status: daily.body?.status, running: daily.body?.running, lastRunAt: daily.body?.lastRunAt },
    warehousePageLinked: {
      total: page.body?.total,
      items: items.length,
      partial: page.body?.partial,
      linkedProducts: page.body?.linkedProducts,
      sample: items.slice(0, 4).map((item) => ({
        id: item.id,
        offerId: item.offerId,
        marketplace: item.marketplace,
        links: (item.links || []).length,
        manualGroupId: item.manualGroupId || item.raw?.manualGroupId || "",
      })),
    },
    warehousePageAll: {
      total: pageAll.body?.total,
      items: Array.isArray(pageAll.body?.items) ? pageAll.body.items.length : 0,
      partial: pageAll.body?.partial,
      sample: (pageAll.body?.items || []).slice(0, 4).map((item) => ({
        offerId: item.offerId,
        marketplace: item.marketplace,
        links: (item.links || []).length,
      })),
    },
    warehousePageGrouped: (() => {
      const groups = Array.isArray(pageGrouped.body?.items) ? pageGrouped.body.items : [];
      const multi = groups.filter((group) => Array.isArray(group.products) && group.products.length > 1);
      return {
        grouped: pageGrouped.body?.grouped,
        groupTotal: pageGrouped.body?.groupTotal ?? pageGrouped.body?.total,
        rowTotal: pageGrouped.body?.rowTotal ?? pageAll.body?.total,
        total: pageGrouped.body?.total,
        items: groups.length,
        partial: pageGrouped.body?.partial,
        sourceError: pageGrouped.body?.sourceError || "",
        multiMarketplace: multi.length,
        sample: multi.slice(0, 5).map((group) => ({
          groupKey: group.groupKey,
          marketplaces: group.marketplaces,
          offerId: group.offerId,
          products: (group.products || []).map((product) => product.marketplace),
          links: (group.links || []).length,
          withoutSupplier: group.statusSummary?.withoutSupplier || 0,
          primarySupplier: group.primary?.selectedSupplier?.supplierName || group.primary?.selectedSupplier?.partnerName || null,
          primaryPrice: group.primary?.nextPrice || group.primary?.targetPrice || group.primary?.marketplacePrice || null,
        })),
      };
    })(),
    liveStatus: live.body,
    health: health.body,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
