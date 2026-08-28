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
        res({ status: resp.statusCode, body: p, headers: resp.headers });
      });
    });
    r.on("error", rej); if (payload) r.write(payload); r.end();
  });
}
function cookie(h) {
  const c = [].concat(h["set-cookie"]||[]).find(s => s.startsWith("pm_session="));
  return c ? c.split(";")[0] : "";
}

async function main() {
  const login = await req("POST", "/api/login", { body: { username, password: appPassword } });
  const ck = cookie(login.headers);
  if (!ck) { console.log("LOGIN_FAILED", login.status); return; }

  // Запрашиваем Ozon и Yandex раздельно с лимитом 5000
  const [health, ozonQ, yandexQ, retryQ] = await Promise.all([
    req("GET", "/health", { cookie: ck }),
    req("GET", "/api/sales-automation/items?marketplace=ozon&limit=5000", { cookie: ck }),
    req("GET", "/api/sales-automation/items?marketplace=yandex&limit=5000", { cookie: ck }),
    req("GET", "/api/sales-automation/items?marketplace=all&reason=in_retry&limit=5000", { cookie: ck }),
  ]);

  const q = health.body?.queue?.counts || {};
  console.log("\\n=== BULLMQ QUEUE ===");
  console.log("  waiting:", q.waiting||0, " active:", q.active||0,
              " delayed:", q.delayed||0, " failed:", q.failed||0);

  const okR    = ["ok","unchanged","unchanged_verified","verified","api_accepted"];
  const queueR = ["queued","verification_pending"];
  const badR   = ["api_error","ozon_price_not_applied","ozon_price_delayed","pm_live_timeout","in_retry"];
  const noPrice= ["no_price","no_supplier","no_pricemaster_link","stock_only_manual_price_missing","not_ready"];

  function analyze(rows, label) {
    const ok    = rows.filter(r => okR.includes(String(r.reason))).length;
    const inQ   = rows.filter(r => queueR.includes(String(r.reason))).length;
    const bad   = rows.filter(r => badR.includes(String(r.reason))).length;
    const skip  = rows.filter(r => noPrice.includes(String(r.reason))).length;
    const other = rows.length - ok - inQ - bad - skip;
    console.log("\\n" + label + " (" + rows.length + " SKU total):");
    console.log("  ✅ ok/sent:       " + ok);
    console.log("  ⏳ в очереди:     " + inQ);
    console.log("  ❌ ошибки:        " + bad);
    console.log("  ⏭  нет цены/PM:  " + skip);
    console.log("  ?  прочее:       " + other);

    // breakdown errors
    const errMap = {};
    for (const r of rows.filter(x => badR.includes(String(x.reason)))) {
      const k = String(r.reason); errMap[k] = (errMap[k]||0)+1;
    }
    if (Object.keys(errMap).length) {
      for (const [k,n] of Object.entries(errMap).sort((a,b)=>b[1]-a[1])) {
        console.log("    " + k + ": " + n);
      }
    }
    // sample errors
    const errSamples = rows.filter(r => r.lastError && String(r.lastError).trim()).slice(0,3);
    for (const e of errSamples) {
      console.log("    [err] " + e.offerId + ": " + String(e.lastError||"").slice(0,100));
    }
    return { ok, inQ, bad, skip, total: rows.length };
  }

  const ozonRows   = ozonQ.body?.items || [];
  const yandexRows = yandexQ.body?.items || [];
  const retryRows  = retryQ.body?.items || [];

  const ozonStat   = analyze(ozonRows,   "=== OZON ===");
  const yandexStat = analyze(yandexRows, "=== YANDEX ===");

  console.log("\\n=== RETRY (in_retry) ===");
  console.log("  Всего в retry:", retryRows.length);
  if (retryRows.length) {
    // группируем по marketplace
    const byMp = {};
    for (const r of retryRows) { const m = String(r.marketplace); byMp[m]=(byMp[m]||0)+1; }
    for (const [m,n] of Object.entries(byMp)) console.log("  " + m + ": " + n);
    // sample
    for (const r of retryRows.slice(0,3)) {
      console.log("  [retry] " + r.marketplace + " " + r.offerId
        + " lastErr: " + String(r.lastError||"").slice(0,80));
    }
  }

  // Итог
  const totalOk  = ozonStat.ok + yandexStat.ok;
  const totalQ   = ozonStat.inQ + yandexStat.inQ;
  const totalBad = ozonStat.bad + yandexStat.bad;
  console.log("\\n=== ИТОГ ===");
  console.log("  Ozon SKU: " + ozonStat.total + "  |  Yandex SKU: " + yandexStat.total);
  console.log("  ✅ Успешно отправлено:  " + totalOk);
  console.log("  ⏳ Ещё в очереди:      " + totalQ);
  console.log("  ❌ С ошибками:         " + totalBad);
  const pct = ozonStat.total + yandexStat.total > 0
    ? Math.round(100*totalOk/(ozonStat.total+yandexStat.total)) : 0;
  console.log("  Готовность:            " + pct + "%");
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
  const remote = "/var/www/davidsklad/davidskladik/scripts/_chk_prices_deep.cjs";
  try {
    await sftpPutString(conn, checkScript, remote);
    await exec(conn, "cd /var/www/davidsklad/davidskladik && node " + remote);
    await exec(conn, "rm -f " + remote);
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message || e); process.exit(1); });
