#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

const script = `#!/usr/bin/env node
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
  if (!ck) { console.log("LOGIN_FAILED"); return; }

  const r = await req("POST", "/api/sales-automation/run", { cookie: ck,
    body: { marketplace: "all", force: true, onlyChanged: false, limit: 50000, reason: "manual_force_reprice" },
  });
  console.log("status:", r.status);
  console.log("queued:", r.body?.queued, "batches:", r.body?.queuedBatches);
  console.log("priceIntentId:", r.body?.priceIntentId);
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
  const remote = "/var/www/davidsklad/davidskladik/scripts/_trigger_reprice.cjs";
  try {
    await sftpPutString(conn, script, remote);
    await exec(conn, "cd /var/www/davidsklad/davidskladik && node " + remote);
    await exec(conn, "rm -f " + remote);
  } finally { conn.end(); }
}
main().catch(e => { console.error(e.message || e); process.exit(1); });
