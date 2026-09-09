#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { Client } = require("ssh2");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const privateKey = fs.existsSync(defaultKeyPath) ? fs.readFileSync(defaultKeyPath) : null;
const sshPassword = process.env.DEPLOY_PASSWORD;
if (!privateKey && !sshPassword) { console.error("No SSH key or DEPLOY_PASSWORD"); process.exit(1); }

const APP_USER = process.env.APP_USER || "admin";
const APP_PASSWORD = process.env.APP_PASSWORD || "";
if (!APP_PASSWORD) { console.error("APP_PASSWORD missing"); process.exit(1); }

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      let out = ""; let errOut = "";
      stream.on("data", (d) => { out += d; });
      stream.stderr.on("data", (d) => { errOut += d; });
      stream.on("close", () => resolve({ out, errOut }));
    });
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) cfg.privateKey = privateKey;
    else cfg.password = sshPassword;
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });

  try {
    // Login
    const { out: loginOut } = await exec(conn,
      `curl -s -c /tmp/gencart_cookie.txt -X POST http://127.0.0.1:3000/api/login \
       -H 'Content-Type: application/json' -H 'Origin: http://127.0.0.1:3000' \
       -d '${JSON.stringify({ username: APP_USER, password: APP_PASSWORD })}'`
    );
    let login;
    try { login = JSON.parse(loginOut); } catch { login = {}; }
    if (!login.ok) { console.error("Login failed:", loginOut); process.exit(1); }
    console.log("Logged in OK");

    // Trigger generation
    const { out: genOut } = await exec(conn,
      `curl -s -b /tmp/gencart_cookie.txt -X POST http://127.0.0.1:3000/api/supplier-cart/generate \
       -H 'Content-Type: application/json' -H 'Origin: http://127.0.0.1:3000' \
       -d '{}'`
    );
    let data;
    try { data = JSON.parse(genOut); } catch { data = {}; }
    if (!data.ok) { console.error("Generate failed:", genOut.slice(0, 500)); process.exit(1); }

    console.log("Генерация запущена. Текущий черновик (старый):");
    console.log(`  total: ${data.total} | ready: ${data.ready} | committed: ${data.alreadyCommitted} | skipped: ${data.skipped}`);
    console.log(`  generatedAt: ${data.generatedAt || "—"}`);
    if (data.warnings?.length) console.log("  warnings:", JSON.stringify(data.warnings));

    // Wait 45 sec then check new draft
    console.log("\nЖду 45 сек пока генерация завершится...");
    await new Promise((r) => setTimeout(r, 45000));

    const { out: draftOut } = await exec(conn,
      `curl -s -b /tmp/gencart_cookie.txt http://127.0.0.1:3000/api/supplier-cart/draft \
       -H 'Origin: http://127.0.0.1:3000'`
    );
    let draft;
    try { draft = JSON.parse(draftOut); } catch { draft = {}; }
    console.log("\nНовый черновик:");
    console.log(`  total: ${draft.total} | ready: ${draft.ready} | committed: ${draft.alreadyCommitted} | skipped: ${draft.skipped}`);
    console.log(`  generatedAt: ${draft.generatedAt || "—"}`);
    if (draft.warnings?.length) console.log("  warnings:", JSON.stringify(draft.warnings));
    if (draft.rows?.length) {
      const readyRows = (draft.rows || []).filter(r => r.ready && !r.alreadyCommitted);
      console.log(`\n  Готовые к заказу (${readyRows.length}):`);
      for (const r of readyRows.slice(0, 10)) {
        console.log(`    ${r.productName || r.offerId} → ${r.supplierName || "?"} | ${r.skipReason || "ok"}`);
      }
    }
  } finally {
    await exec(conn, "rm -f /tmp/gencart_cookie.txt").catch(() => {});
    conn.end();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
