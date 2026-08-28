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

function sumStatus(statuses = [], marketplace, priceStatus) {
  return statuses
    .filter((row) => row.marketplace === marketplace && row.priceStatus === priceStatus)
    .reduce((total, row) => total + Number(row.count || 0), 0);
}

async function main() {
  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(login.headers);
  if (!cookie || login.status !== 200) throw new Error(`login failed: ${login.status}`);

  const [live, summary, linkedItems, unchangedItems] = await Promise.all([
    request("GET", "/api/live-status", { cookie }),
    request("GET", "/api/sales-automation/summary", { cookie }),
    request("GET", "/api/sales-automation/items?reason=linked_full_reprice&limit=1", { cookie }),
    request("GET", "/api/sales-automation/items?reason=unchanged_verified&limit=1", { cookie }),
  ]);

  const reasons = summary.body?.reasons || {};
  const statuses = summary.body?.statuses || [];
  const queue = live.body?.queue || live.body?.bullmq || {};
  const counts = queue.counts || {};

  console.log(JSON.stringify({
    at: new Date().toISOString(),
    bullmq: counts,
    consumerReady: queue.consumerReady ?? null,
    linkedRepriceRun: {
      queued: reasons.queued ?? 0,
      ok: reasons.ok ?? 0,
      unchanged_verified: reasons.unchanged_verified ?? 0,
      stock_only_excluded: reasons.stock_only_excluded_from_price_push ?? 0,
      api_error: reasons.api_error ?? 0,
      verification_pending: reasons.verification_pending ?? 0,
      in_retry: reasons.in_retry ?? 0,
      linkedItemsInDb: linkedItems.body?.total ?? null,
      unchangedItemsInDb: unchangedItems.body?.total ?? null,
    },
    ozon: {
      pending: sumStatus(statuses, "ozon", "pending"),
      success: sumStatus(statuses, "ozon", "success"),
      failed: sumStatus(statuses, "ozon", "failed"),
    },
    yandex: {
      pending: sumStatus(statuses, "yandex", "pending"),
      success: sumStatus(statuses, "yandex", "success"),
      failed: sumStatus(statuses, "yandex", "failed"),
    },
    retryTotal: summary.body?.retryTotal ?? null,
    ozonUnarchiveQueued: summary.body?.ozonUnarchiveQueued ?? null,
  }, null, 2));
}

main().catch((error) => {
  console.error("PRICE_STATUS_FAILED:", error.message);
  process.exit(1);
});
