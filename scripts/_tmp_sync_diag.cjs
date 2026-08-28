#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_sync_diag_run.js";

const diagScript = [
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
  '  console.log("=== warehouse_products: ozon rows created per day (last 21 days) ===");',
  '  const perDay = await p.$queryRawUnsafe("SELECT DATE(created_at) AS day, COUNT(*)::int AS n FROM warehouse_products WHERE marketplace = \'ozon\' AND created_at > now() - interval \'21 days\' GROUP BY DATE(created_at) ORDER BY day");',
  '  for (const row of perDay) console.log(" ", row.day.toISOString().slice(0,10), ":", row.n);',
  '  const last = await p.$queryRawUnsafe("SELECT MAX(created_at) AS last_created, MAX(updated_at) AS last_updated FROM warehouse_products WHERE marketplace = \'ozon\'");',
  '  console.log("last ozon row created:", last[0]?.last_created, "| last updated:", last[0]?.last_updated);',
  '  const counts = await p.$queryRawUnsafe("SELECT marketplace, archived, COUNT(*)::int AS n FROM warehouse_products GROUP BY marketplace, archived ORDER BY marketplace, archived");',
  '  console.log("=== row counts ==="); for (const row of counts) console.log(" ", row.marketplace, "archived=" + row.archived, ":", row.n);',
  '  console.log("\\n=== sweep heartbeats ===");',
  '  const sweeps = await p.$queryRawUnsafe("SELECT name, status, last_run_at FROM sweep_heartbeats ORDER BY name").catch(() => []);',
  '  for (const s of sweeps) { const ageM = s.last_run_at ? Math.round((Date.now() - new Date(s.last_run_at).getTime())/60000) : -1; console.log(" ", s.name, "| status:", s.status, "| minutes ago:", ageM); }',
  '  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });',
  '  const cookie = sessionCookie(login.headers);',
  '  if (cookie && login.status === 200) {',
  '    const [live, daily, syncStatus] = await Promise.all([',
  '      request("GET", "/api/live-status", { cookie }),',
  '      request("GET", "/api/daily-sync", { cookie }),',
  '      request("GET", "/api/warehouse/sync/status", { cookie }),',
  '    ]);',
  '    console.log("\\n=== daily sync ===");',
  '    console.log(JSON.stringify({ status: daily.body?.status, running: daily.body?.running, lastRunAt: daily.body?.lastRunAt, lastResult: daily.body?.lastResult, error: daily.body?.error }, null, 2));',
  '    console.log("\\n=== warehouse sync status ===");',
  '    console.log(JSON.stringify({ status: syncStatus.body?.status, running: syncStatus.body?.running, startedAt: syncStatus.body?.startedAt, finishedAt: syncStatus.body?.finishedAt, error: syncStatus.body?.error, result: syncStatus.body?.result }, null, 2));',
  '    console.log("\\n=== live-status (sync-related) ===");',
  '    const lb = live.body || {};',
  '    console.log(JSON.stringify({ autoSync: lb.autoSync, marketplaceMaintenance: lb.marketplaceMaintenance, dailySync: lb.dailySync, linkedReconciler: lb.linkedReconciler }, null, 2));',
  '  } else {',
  '    console.log("login failed:", login.status);',
  '  }',
  '  await p.$disconnect();',
  '}',
  'main().catch((e) => { console.error("DIAG_FAILED:", e.message); process.exit(1); });',
].join("\n");

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", (d) => process.stdout.write(d));
        stream.stderr.on("data", (d) => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(diagScript);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
