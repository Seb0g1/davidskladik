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
    }, (resp) => {
      let d = ""; resp.on("data", c => d += c); resp.on("end", () => {
        let p = d; try { p = JSON.parse(d); } catch {}
        res({ status: resp.statusCode, headers: resp.headers, body: p });
      });
    });
    r.on("error", rej); if (payload) r.write(payload); r.end();
  });
}
function cookie(h) {
  const c = [].concat(h["set-cookie"]||[]).find(s=>s.startsWith("pm_session="));
  return c ? c.split(";")[0] : "";
}

async function main() {
  const login = await req("POST", "/api/login", { body: { username, password: appPassword } });
  const ck = cookie(login.headers);
  if (!ck) { console.log("LOGIN_FAILED", login.status); return; }

  const [health, summary, items] = await Promise.all([
    req("GET", "/health", { cookie: ck }),
    req("GET", "/api/sales-automation/summary", { cookie: ck }),
    req("GET", "/api/sales-automation/items?marketplace=all&limit=500", { cookie: ck }),
  ]);

  const q = health.body?.queue?.counts || {};
  console.log("\\n=== QUEUE ===");
  console.log("  waiting:", q.waiting || 0);
  console.log("  active: ", q.active || 0);
  console.log("  delayed:", q.delayed || 0);
  console.log("  failed: ", q.failed || 0);

  const s = summary.body || {};
  console.log("\\n=== SALES AUTOMATION SUMMARY ===");
  console.log("  total SKU:", s.total || 0);
  console.log("  retry:    ", s.retryTotal || 0);
  console.log("  updatedAt:", s.updatedAt || "—");

  const all = (items.body?.items || []);
  const ozon   = all.filter(i => String(i.marketplace) === "ozon");
  const yandex = all.filter(i => String(i.marketplace) === "yandex");

  const ok    = ["ok","unchanged","unchanged_verified","verified","api_accepted"];
  const queue = ["queued","verification_pending"];
  const bad   = ["api_error","ozon_price_not_applied","pm_live_timeout","in_retry"];

  function stat(rows, label) {
    const okN    = rows.filter(r => ok.includes(String(r.reason))).length;
    const queueN = rows.filter(r => queue.includes(String(r.reason))).length;
    const badN   = rows.filter(r => bad.includes(String(r.reason))).length;
    const otherN = rows.length - okN - queueN - badN;
    console.log("\\n  " + label + " (" + rows.length + " SKU):");
    console.log("    ok/sent:  " + okN);
    console.log("    в очереди:" + queueN);
    console.log("    ошибки:   " + badN);
    console.log("    прочее:   " + otherN);
  }

  console.log("\\n=== СТАТУС ЦЕН ===");
  stat(ozon, "Ozon");
  stat(yandex, "Yandex");

  // Breakdown по reason
  const reasons = {};
  for (const i of all) { const r = String(i.reason||"?"); reasons[r] = (reasons[r]||0)+1; }
  console.log("\\n=== РАЗБИВКА ПО ПРИЧИНАМ ===");
  for (const [r,n] of Object.entries(reasons).sort((a,b)=>b[1]-a[1])) {
    console.log("  " + String(r).padEnd(30) + n);
  }

  // Последние ошибки
  const errors = all.filter(i => i.lastError && String(i.lastError).trim());
  if (errors.length) {
    console.log("\\n=== ПРИМЕРЫ ОШИБОК (до 5) ===");
    for (const e of errors.slice(0,5)) {
      console.log("  [" + e.marketplace + "] " + e.offerId + ": " + String(e.lastError).slice(0,120));
    }
  }
}
main().catch(e => { console.error(e.message||e); process.exit(1); });
`;

function exec(conn, cmd) {
  return new Promise((res, rej) => {
    conn.exec(cmd, (err, s) => {
      if (err) return rej(err);
      let o = "";
      s.on("data", d => { o += d; process.stdout.write(d); });
      s.stderr.on("data", d => process.stderr.write(d));
      s.on("close", code => code ? rej(new Error("exit " + code)) : res(o));
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
  const remote = "/var/www/davidsklad/davidskladik/scripts/_chk_prices.cjs";
  try {
    await sftpPutString(conn, checkScript, remote);
    await exec(conn, "cd /var/www/davidsklad/davidskladik && node " + remote);
    await exec(conn, "rm -f " + remote);
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message || e); process.exit(1); });
