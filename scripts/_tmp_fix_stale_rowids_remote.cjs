#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const APPLY = process.argv.includes("--apply");

async function main() {
  const base = "http://localhost:3000";

  const loginRes = await fetch(`${base}/api/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: process.env.APP_USER, password: process.env.APP_PASSWORD }),
  });
  const setCookie = loginRes.headers.get("set-cookie") || "";
  const cookie = setCookie.split(";")[0];
  const loginBody = await loginRes.json();
  if (!loginBody.ok) { console.error("Auth failed", loginBody); process.exit(1); }
  console.log("Login: OK");

  const res = await fetch(`${base}/api/warehouse/links/fix-stale-row-ids`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ dryRun: !APPLY }),
    signal: AbortSignal.timeout(30000),
  });
  const data = await res.json();
  console.log(JSON.stringify(data, null, 2));

  if (!APPLY && data.found > 0) {
    console.log(`\nDRY RUN: found ${data.found} stale link(s). Run with --apply to fix.`);
  } else if (APPLY) {
    console.log(`\nFixed ${data.fixed}/${data.found} link(s).`);
  }
}

main().catch(e => { console.error("FAILED:", e.message); process.exit(1); });
