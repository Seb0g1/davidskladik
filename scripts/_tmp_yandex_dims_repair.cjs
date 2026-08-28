#!/usr/bin/env node
"use strict";
// Диагностика/ремонт карточек Яндекса без габаритов: дергает на проде
// POST /api/ozon-yandex-import/repair-yandex-content (dry-run по умолчанию,
// --confirmed = реальная отправка карточек в Яндекс).
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const confirmed = process.argv.includes("--confirmed");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_yandex_dims_repair.js";

const runScript = [
  'const http = require("node:http");',
  'const path = require("node:path");',
  'require("dotenv").config({ path: path.resolve(__dirname, "../.env") });',
  'const port = Number(process.env.PORT || 3000) || 3000;',
  'const username = process.env.APP_USER || "admin";',
  'const appPassword = process.env.APP_PASSWORD || "";',
  'const confirmed = ' + JSON.stringify(confirmed) + ';',
  'function request(method, urlPath, { cookie = "", body = null, timeoutMs = 15 * 60_000 } = {}) {',
  '  return new Promise((resolve, reject) => {',
  '    const payload = body ? JSON.stringify(body) : "";',
  '    const req = http.request({ hostname: "127.0.0.1", port, path: urlPath, method, timeout: timeoutMs, headers: {',
  '      ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),',
  '      ...(cookie ? { Cookie: cookie } : {}),',
  '    } }, (res) => {',
  '      let data = "";',
  '      res.on("data", (chunk) => { data += chunk; });',
  '      res.on("end", () => { let parsed = data; try { parsed = JSON.parse(data); } catch {} resolve({ status: res.statusCode, headers: res.headers, body: parsed }); });',
  '    });',
  '    req.on("timeout", () => req.destroy(new Error("request timeout")));',
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
  '  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });',
  '  const cookie = sessionCookie(login.headers);',
  '  if (!cookie || login.status !== 200) throw new Error("login failed: " + login.status);',
  '  const body = { dryRun: !confirmed, limit: 50000 };',
  '  console.log("repair-yandex-content " + (confirmed ? "(CONFIRMED: отправка в Яндекс)" : "(dry run)") + "...");',
  '  const res = await request("POST", "/api/ozon-yandex-import/repair-yandex-content", { cookie, body });',
  '  console.log("HTTP " + res.status);',
  '  console.log(JSON.stringify(res.body, null, 2));',
  '  if (res.status >= 400) process.exit(1);',
  '}',
  'main().catch((e) => { console.error("REPAIR_FAILED:", e.message); process.exit(1); });',
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
    ws.end(runScript);
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password, readyTimeout: 60000 });
