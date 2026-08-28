#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const confirmed = process.argv.includes("--confirmed");
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_yandex_cleanup_run.js";

const runScript = [
  'const http = require("node:http");',
  'const path = require("node:path");',
  'require("dotenv").config({ path: path.resolve(__dirname, "../.env") });',
  'const port = Number(process.env.PORT || 3000) || 3000;',
  'const username = process.env.APP_USER || "admin";',
  'const appPassword = process.env.APP_PASSWORD || "";',
  'const confirmed = ' + JSON.stringify(confirmed) + ';',
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
  '  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });',
  '  const cookie = sessionCookie(login.headers);',
  '  if (!cookie || login.status !== 200) throw new Error("login failed: " + login.status);',
  '  const body = confirmed ? { dryRun: false, confirmed: true } : { dryRun: true };',
  '  console.log("Running yandex-cleanup delete-filtered-local " + (confirmed ? "(CONFIRMED)" : "(dry run)") + "...");',
  '  const res = await request("POST", "/api/yandex-cleanup/delete-filtered-local", { cookie, body });',
  '  console.log("HTTP " + res.status);',
  '  const out = res.body || {};',
  '  console.log(JSON.stringify({ summary: out.summary, deleted: out.deleted, failed: out.failed, localDeleted: out.localDeleted, localArchived: out.localArchived }, null, 2));',
  '  const rows = Array.isArray(out.rows) ? out.rows : [];',
  '  if (rows.length) {',
  '    console.log("Sample rows (first 40):");',
  '    for (const row of rows.slice(0, 40)) {',
  '      console.log(" -", row.offerId, "|", row.name, "| keyword:", row.hasBlockedKeyword, "| smallVol:", row.smallVolume);',
  '    }',
  '  }',
  '  if (res.status >= 400) process.exit(1);',
  '}',
  'main().catch((e) => { console.error("CLEANUP_FAILED:", e.message); process.exit(1); });',
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
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
