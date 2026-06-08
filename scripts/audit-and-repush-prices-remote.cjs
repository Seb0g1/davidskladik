#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

const FAILED_REASONS = [
  "ozon_price_not_applied",
  "api_error",
  "in_retry",
  "pm_live_timeout",
  "verification_pending",
];

function parseArgs(argv) {
  const args = new Set(argv);
  return {
    apply: args.has("--apply"),
    dryRun: args.has("--dry-run") || !args.has("--apply"),
    limit: Math.max(100, Math.min(50000, Number((argv.find((item) => item.startsWith("--limit=")) || "").split("=")[1] || 5000) || 5000)),
    batchSize: Math.max(50, Math.min(2000, Number((argv.find((item) => item.startsWith("--batch=")) || "").split("=")[1] || 500) || 500)),
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1",
      port,
      path: urlPath,
      method,
      headers: {
        ...(payload
          ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) }
          : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try {
          parsed = JSON.parse(data);
        } catch {
          parsed = data;
        }
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

async function login() {
  if (!appPassword) throw new Error("APP_PASSWORD missing in server .env");
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(res.headers);
  if (!cookie || res.status !== 200) throw new Error(`Login failed: HTTP ${res.status}`);
  return cookie;
}

async function getJson(cookie, urlPath) {
  const res = await request("GET", urlPath, { cookie });
  if (res.status >= 400) throw new Error(`${urlPath} failed: HTTP ${res.status}`);
  return res.body;
}

async function postJson(cookie, urlPath, body) {
  const res = await request("POST", urlPath, { cookie, body });
  if (res.status >= 400) throw new Error(`${urlPath} failed: HTTP ${res.status} ${JSON.stringify(res.body)}`);
  return res.body;
}

async function collectFailedProductIds(cookie, limit, reasonsMap = {}) {
  const productIds = new Set();
  const samples = [];
  const activeReasons = FAILED_REASONS.filter((reason) => Number(reasonsMap[reason] || 0) > 0);
  const targets = activeReasons.length ? activeReasons : ["ozon_price_not_applied", "api_error"];

  for (const reason of targets) {
    const data = await getJson(cookie, `/api/sales-automation/items?reason=${encodeURIComponent(reason)}&limit=${limit}`);
    for (const item of data.items || []) {
      const id = String(item.productId || item.id || "").trim();
      if (!id) continue;
      productIds.add(id);
      if (samples.length < 20) {
        samples.push({
          productId: id,
          offerId: item.offerId,
          marketplace: item.marketplace,
          reason: item.reason || reason,
          priceApplyStatus: item.priceApplyStatus,
          targetPrice: item.targetPrice,
          lastVerifiedPrice: item.lastVerifiedPrice,
          lastError: item.lastError,
        });
      }
    }
  }

  const retryQueue = await getJson(cookie, "/api/warehouse/prices/retry-queue");
  const retryItems = Array.isArray(retryQueue.items) ? retryQueue.items : [];
  for (const item of retryItems) {
    const id = String(item.productId || item.id || "").trim();
    if (id) productIds.add(id);
  }

  return { productIds: Array.from(productIds), samples, retryQueueTotal: retryItems.length };
}

async function waitForPricePush(cookie, maxMinutes = 90) {
  const deadline = Date.now() + maxMinutes * 60 * 1000;
  while (Date.now() < deadline) {
    const live = await getJson(cookie, "/api/live-status");
    const push = live?.automation?.pricePush || {};
    console.log(`[price-push] running=${Boolean(push.running)} pending=${push.pendingScope || 0} scheduled=${Boolean(push.scheduled)}`);
    if (!push.running && !push.scheduled && Number(push.pendingScope || 0) === 0) return live;
    await sleep(10000);
  }
  throw new Error("price push wait timeout");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const cookie = await login();
  console.log(`Mode: ${options.apply ? "APPLY" : "DRY-RUN"}`);

  const summary = await getJson(cookie, "/api/sales-automation/summary");
  const failed = await collectFailedProductIds(cookie, Math.min(1000, options.limit), summary.reasons || {});

  let previewChanged = null;
  if (options.apply && !failed.productIds.length) {
    previewChanged = await getJson(cookie, `/api/warehouse/prices/preview?onlyChanged=true&limit=500&livePriceMaster=false`);
  }

  console.log("\n=== Audit ===");
  console.log(JSON.stringify({
    salesAutomation: {
      total: summary.total,
      retryTotal: summary.retryTotal,
      reasons: summary.reasons || {},
      statuses: summary.statuses || [],
    },
    retryQueue: failed.retryQueueTotal,
    failedProductIds: failed.productIds.length,
    previewChangedSelected: previewChanged?.selected ?? null,
    previewChangedToSend: Array.isArray(previewChanged?.items) ? previewChanged.items.length : null,
    failedSamples: failed.samples,
  }, null, 2));

  if (!options.apply) {
    console.log("\nDry-run complete. Re-run with --apply to resend failed prices.");
    return;
  }

  let retryResult = { processed: 0, retried: 0, failed: 0, remaining: 0 };
  if (failed.retryQueueTotal > 0) {
    console.log("\n=== Retry queue ===");
    retryResult = await postJson(cookie, "/api/warehouse/prices/retry", { confirmed: true });
    console.log(JSON.stringify(retryResult, null, 2));
  }

  const ids = failed.productIds;
  if (!ids.length) {
    console.log("\nNo failed product IDs in sales automation state; checking changed preview batch...");
    const changedIds = (previewChanged?.items || []).map((item) => String(item.id || item.productId || "").trim()).filter(Boolean);
    if (!changedIds.length) {
      console.log("Nothing to resend.");
      return;
    }
    ids.push(...changedIds);
  }

  const uniqueIds = Array.from(new Set(ids));
  console.log(`\n=== Repush ${uniqueIds.length} products in batches of ${options.batchSize} ===`);
  let queuedTotal = 0;
  for (let offset = 0; offset < uniqueIds.length; offset += options.batchSize) {
    const batch = uniqueIds.slice(offset, offset + options.batchSize);
    const result = await postJson(cookie, "/api/sales-automation/run", {
      productIds: batch,
      marketplace: "all",
      force: true,
      onlyChanged: false,
      verify: true,
      reason: "failed_price_repush",
    });
    queuedTotal += Number(result.queued || 0);
    console.log(`Batch ${Math.floor(offset / options.batchSize) + 1}: queued=${result.queued || 0} intent=${result.priceIntentId || "-"}`);
    await sleep(2000);
  }

  console.log("\n=== Waiting for price push queue ===");
  await waitForPricePush(cookie);

  const afterSummary = await getJson(cookie, "/api/sales-automation/summary");
  const afterFailed = await collectFailedProductIds(cookie, Math.min(500, options.limit), afterSummary.reasons || {});
  console.log("\n=== After repush ===");
  console.log(JSON.stringify({
    queuedTotal,
    retryResult,
    reasons: afterSummary.reasons || {},
    retryQueue: afterFailed.retryQueueTotal,
    remainingFailedProductIds: afterFailed.productIds.length,
    remainingSamples: afterFailed.samples.slice(0, 10),
  }, null, 2));
}

main().catch((error) => {
  console.error("PRICE_REPUSH_FAILED:", error.message);
  process.exit(1);
});
