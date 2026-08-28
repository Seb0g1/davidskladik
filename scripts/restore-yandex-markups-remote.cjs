#!/usr/bin/env node
"use strict";

const http = require("node:http");
const path = require("node:path");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";
const dryRun = !process.argv.includes("--apply");

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

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
      res.on("data", (chunk) => { data += chunk; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* raw */ }
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
  if (!appPassword) throw new Error("APP_PASSWORD missing in .env");
  const res = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(res.headers);
  if (!cookie) throw new Error(`Login failed: ${res.status}`);
  return cookie;
}

async function pollJob(cookie, statusUrl, maxWaitMs = 120_000) {
  const deadline = Date.now() + maxWaitMs;
  while (Date.now() < deadline) {
    await sleep(2000);
    const res = await request("GET", statusUrl, { cookie });
    const job = res.body?.job || res.body;
    if (!job?.id) throw new Error(`Job not found (status ${res.status}): ${JSON.stringify(res.body)}`);
    if (job.status === "completed") return job;
    if (job.status === "failed") throw new Error(`Job failed: ${job.error || JSON.stringify(job)}`);
    process.stdout.write(`  [${job.status}] ${job.summary || "..."}\r`);
  }
  throw new Error("Timed out waiting for job");
}

async function run(cookie, applyMode) {
  const res = await request("POST", "/api/warehouse/yandex/restore-markups", {
    cookie,
    body: { dryRun: !applyMode },
  });
  if (!res.body?.ok) {
    console.error("Failed to start job:", JSON.stringify(res.body, null, 2));
    process.exit(1);
  }
  const { jobId, statusUrl } = res.body;
  console.log(`  Job started: ${jobId}`);
  const maxWait = applyMode ? 300_000 : 120_000;
  const job = await pollJob(cookie, statusUrl, maxWait);
  return job.result;
}

async function main() {
  console.log(`restore-yandex-markups: ${dryRun ? "DRY RUN (pass --apply to apply)" : "APPLYING"}`);
  const cookie = await login();
  console.log("Logged in.");

  console.log("\n=== DRY RUN (preview) ===");
  const preview = await run(cookie, false);
  console.log(`\nWould update: ${preview.updated}, skip: ${preview.skipped}, USD rate: ${preview.usdRate}`);
  if (preview.sampleUpdates?.length) {
    console.log("\nSample updates:");
    for (const u of preview.sampleUpdates) {
      console.log(`  ${u.offerId}: markup=${u.markup} (lastPrice=${u.lastPrice}₽, pmRub≈${u.pmRub}₽)`);
    }
  }
  if (preview.sampleSkipped?.length) {
    console.log("\nSample skipped:");
    for (const s of preview.sampleSkipped) {
      console.log(`  ${s.offerId}: ${s.reason}${s.markup !== undefined ? ` markup=${s.markup}` : ""}${s.lastPrice ? ` lastPrice=${s.lastPrice}` : ""}`);
    }
  }

  if (dryRun) {
    console.log("\nRun with --apply to commit changes.");
    return;
  }

  console.log("\n=== APPLYING ===");
  const result = await run(cookie, true);
  console.log(`\nUpdated: ${result.updated}, skipped: ${result.skipped}`);
  console.log(result.summary || "Done.");
}

main().catch((error) => { console.error(error); process.exit(1); });
