#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_site_check_run.js";

const runScript = [
  'const http = require("node:http");',
  'const path = require("node:path");',
  'require("dotenv").config({ path: path.resolve(__dirname, "../.env") });',
  'const { PrismaClient } = require("@prisma/client");',
  'const p = new PrismaClient();',
  'const port = Number(process.env.PORT || 3000) || 3000;',
  'const username = process.env.APP_USER || "admin";',
  'const appPassword = process.env.APP_PASSWORD || "";',
  'function request(method, urlPath, { cookie = "", body = null } = {}) {',
  '  return new Promise((resolve, reject) => {',
  '    const payload = body ? JSON.stringify(body) : "";',
  '    const req = http.request({ hostname: "127.0.0.1", port, path: urlPath, method, headers: {',
  '      ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),',
  '      ...(cookie ? { Cookie: cookie } : {}),',
  '    } }, (res) => {',
  '      let data = "";',
  '      res.on("data", (chunk) => { data += chunk; });',
  '      res.on("end", () => { let parsed = data; try { parsed = JSON.parse(data); } catch {} resolve({ status: res.statusCode, headers: res.headers, body: parsed }); });',
  '    });',
  '    req.on("error", reject);',
  '    if (payload) req.write(payload);',
  '    req.end();',
  '  });',
  '}',
  'function sessionCookie(headers = {}) {',
  '  const setCookie = headers["set-cookie"] || [];',
  '  const list = Array.isArray(setCookie) ? setCookie : [setCookie];',
  '  const session = list.find((item) => String(item).startsWith("pm_session="));',
  '  return session ? String(session).split(";")[0] : "";',
  '}',
  'async function main() {',
  '  console.log("=== 1. new ozon rows imported today ===");',
  '  const fresh = await p.$queryRawUnsafe("SELECT offer_id AS \\"offerId\\", name, created_at FROM warehouse_products WHERE marketplace = \'ozon\' AND created_at > now() - interval \'1 day\' ORDER BY created_at DESC LIMIT 30");',
  '  console.log("count:", fresh.length);',
  '  for (const row of fresh) console.log(" -", row.offerId, "|", String(row.name).slice(0, 70));',
  '  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });',
  '  const cookie = sessionCookie(login.headers);',
  '  if (!cookie || login.status !== 200) throw new Error("login failed: " + login.status);',
  '  console.log("\\n=== 2. /health ===");',
  '  const health = await request("GET", "/health", {});',
  '  console.log("status:", health.status, "| ok:", health.body?.ok, "| heapMb:", health.body?.memory?.heapUsedMb);',
  '  console.log("\\n=== 3. catalog page 1 loads ===");',
  '  const t0 = Date.now();',
  '  const page = await request("GET", "/api/warehouse/products/page?page=1&pageSize=8", { cookie });',
  '  console.log("status:", page.status, "| items:", (page.body?.items || []).length, "| totalAll:", page.body?.totalAll, "| elapsedMs:", Date.now() - t0);',
  '  console.log("\\n=== 4. search finds a freshly imported product ===");',
  '  if (fresh.length) {',
  '    const probe = fresh[0];',
  '    const q = encodeURIComponent(probe.offerId);',
  '    const t1 = Date.now();',
  '    const found = await request("GET", "/api/warehouse/products/page?page=1&pageSize=8&q=" + q, { cookie });',
  '    const items = found.body?.items || [];',
  '    const hit = items.find((item) => String(item.offerId || "").toLowerCase() === String(probe.offerId).toLowerCase());',
  '    console.log("query:", probe.offerId, "| status:", found.status, "| items:", items.length, "| foundExact:", Boolean(hit), "| name:", hit ? String(hit.name).slice(0, 60) : "-", "| elapsedMs:", Date.now() - t1);',
  '  } else {',
  '    console.log("(no fresh rows to probe)");',
  '  }',
  '  console.log("\\n=== 5. deletion markers hidden from search ===");',
  '  const trash = await request("GET", "/api/warehouse/products/page?page=1&pageSize=8&q=" + encodeURIComponent("удалить"), { cookie });',
  '  console.log("q=удалить -> items:", (trash.body?.items || []).length);',
  '  await p.$disconnect();',
  '}',
  'main().catch((e) => { console.error("CHECK_FAILED:", e.message); process.exit(1); });',
].join("\n");

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1 ; pm2 ls | grep -E 'api|worker'", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", (d) => process.stdout.write(d));
        stream.stderr.on("data", (d) => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(runScript);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
