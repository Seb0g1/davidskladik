#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

const checkScript = `#!/usr/bin/env node
"use strict";
const http = require("http");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const port = Number(process.env.PORT || 3000);
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";

function req(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((res, rej) => {
    const payload = body ? JSON.stringify(body) : "";
    const r = http.request({ hostname: "127.0.0.1", port, path: urlPath, method,
      headers: { ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}), ...(cookie ? { Cookie: cookie } : {}) }
    }, (resp) => { let d=""; resp.on("data",c=>d+=c); resp.on("end",()=>{ let p=d; try{p=JSON.parse(d);}catch{}; res({status:resp.statusCode,body:p,headers:resp.headers}); }); });
    r.on("error",rej); if(payload)r.write(payload); r.end();
  });
}
function cookie(h) {
  const c=[].concat(h["set-cookie"]||[]).find(s=>s.startsWith("pm_session=")); return c?c.split(";")[0]:"";
}

async function main() {
  const login = await req("POST", "/api/login", { body: { username, password: appPassword } });
  const ck = cookie(login.headers);
  if (!ck) { console.log("LOGIN_FAILED"); return; }

  // Все Ozon с ошибками
  const errQ = await req("GET", "/api/sales-automation/items?marketplace=ozon&reason=api_error&limit=5000", { cookie: ck });
  const errItems = errQ.body?.items || [];

  // Все Ozon в очереди
  const queuedQ = await req("GET", "/api/sales-automation/items?marketplace=ozon&status=queued&limit=5000", { cookie: ck });
  const queuedItems = queuedQ.body?.items || [];

  console.log("\\n=== OZON api_error (" + errItems.length + " SKU) ===");
  // Группируем по типу ошибки
  const errTypes = {};
  for (const i of errItems) {
    const e = String(i.lastError||"unknown").slice(0,80);
    errTypes[e] = (errTypes[e]||0)+1;
  }
  for (const [e,n] of Object.entries(errTypes).sort((a,b)=>b[1]-a[1]).slice(0,10)) {
    console.log("  [" + n + "x] " + e);
  }
  console.log("\\nПримеры offer_id с ошибками:");
  for (const i of errItems.slice(0,10)) {
    console.log("  " + String(i.offerId).padEnd(25) + " err: " + String(i.lastError||"").slice(0,60));
  }

  console.log("\\n=== OZON queued (" + queuedItems.length + " SKU) — очередь пустая ===");
  console.log("  (Это позиции помеченные как queued, но BullMQ job уже завершён без обновления статуса)");
  console.log("\\nПримеры застрявших queued:");
  for (const i of queuedItems.slice(0,5)) {
    const upd = i.updatedAt ? new Date(i.updatedAt).toLocaleTimeString("ru-RU") : "?";
    console.log("  " + String(i.offerId).padEnd(25) + " updated: " + upd + " reason: " + i.reason);
  }

  // Проверяем в каком priceIntentId они застряли
  const intentCounts = {};
  for (const i of queuedItems) {
    const k = String((i.raw||{}).priceIntentId || i.priceIntentId || "?").slice(0,8);
    intentCounts[k] = (intentCounts[k]||0)+1;
  }
  console.log("\\nПо priceIntentId (группировка застрявших):");
  for (const [k,n] of Object.entries(intentCounts).sort((a,b)=>b[1]-a[1]).slice(0,5)) {
    console.log("  intent " + k + ": " + n + " SKU");
  }

  // Проверяем что с теми кто ok — когда отправлено
  const okQ = await req("GET", "/api/sales-automation/items?marketplace=ozon&reason=ok&limit=500", { cookie: ck });
  const okItems = okQ.body?.items || [];
  if (okItems.length) {
    const times = okItems.map(i => i.updatedAt||i.lastCalculatedAt).filter(Boolean).sort();
    const first = times[0] ? new Date(times[0]).toLocaleTimeString("ru-RU") : "?";
    const last = times[times.length-1] ? new Date(times[times.length-1]).toLocaleTimeString("ru-RU") : "?";
    console.log("\\n=== OZON ok (" + okItems.length + " SKU) ===");
    console.log("  Первый отправлен: " + first);
    console.log("  Последний отправлен: " + last);
  }
}
main().catch(e => { console.error(e.message||e); process.exit(1); });
`;

function exec(conn, cmd) {
  return new Promise((res, rej) => {
    conn.exec(cmd, (err, s) => {
      if (err) return rej(err);
      let o = "";
      s.on("data", d => { o+=d; process.stdout.write(d); });
      s.stderr.on("data", d => process.stderr.write(d));
      s.on("close", code => code ? rej(new Error("exit "+code)) : res(o));
    });
  });
}
function sftpPutString(conn, content, p) {
  return new Promise((res, rej) => {
    conn.sftp((err, sftp) => {
      if (err) return rej(err);
      const w = sftp.createWriteStream(p);
      w.on("close", res); w.on("error", rej); w.end(content, "utf8");
    });
  });
}
async function connect() {
  const conn = new Client();
  await new Promise((res, rej) => conn.on("ready", res).on("error", rej).connect({
    host: "81.17.154.153", username: "root", password, readyTimeout: 30000,
  }));
  return conn;
}
async function main() {
  const conn = await connect();
  const remote = "/var/www/davidsklad/davidskladik/scripts/_chk_ozon_err.cjs";
  try {
    await sftpPutString(conn, checkScript, remote);
    await exec(conn, "cd /var/www/davidsklad/davidskladik && node " + remote);
    await exec(conn, "rm -f " + remote);
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message||e); process.exit(1); });
