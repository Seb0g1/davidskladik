#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const root = path.resolve(__dirname, "..");
const remoteRoot = "/var/www/davidsklad/davidskladik";

const triggerScript = `#!/usr/bin/env node
"use strict";
const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const port = Number(process.env.PORT || 3000) || 3000;
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";
function request(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: "127.0.0.1", port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}
function sessionCookie(headers = {}) {
  const setCookie = headers["set-cookie"] || [];
  const list = Array.isArray(setCookie) ? setCookie : [setCookie];
  const session = list.find((item) => String(item).startsWith("pm_session="));
  return session ? String(session).split(";")[0] : "";
}
async function main() {
  const login = await request("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(login.headers);
  if (!cookie || login.status !== 200) throw new Error("login failed " + login.status);
  const reprice = await request("POST", "/api/sales-automation/run", {
    cookie,
    body: { force: true, marketplace: "all", limit: 50000, reason: "hard_currency_rule" },
  });
  if (reprice.status >= 400) throw new Error("reprice failed " + reprice.status + " " + JSON.stringify(reprice.body));
  console.log("REPRICE_TRIGGER:", JSON.stringify(reprice.body));
}
main().catch((e) => { console.error(e.message); process.exit(1); });
`;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => { process.stderr.write(d); });
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}: ${command}`)) : resolve(out)));
    });
  });
}

function sftpPut(conn, localPath, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      fs.createReadStream(localPath)
        .pipe(sftp.createWriteStream(remotePath))
        .on("close", resolve)
        .on("error", reject);
    });
  });
}

function sftpPutString(conn, content, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      const ws = sftp.createWriteStream(remotePath);
      ws.on("close", resolve);
      ws.on("error", reject);
      ws.end(content, "utf8");
    });
  });
}

async function connect() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 60000,
      keepaliveInterval: 10000,
      keepaliveCountMax: 24,
    });
  });
  return conn;
}

async function main() {
  let deployed = false;
  let workerOk = false;
  let queuedCount = null;
  let pending = null;
  let success = null;

  const conn = await connect();
  try {
    console.log("=== upload server.js + test/smoke.test.cjs ===");
    await exec(conn, `mkdir -p ${remoteRoot}/test`);
    await sftpPut(conn, path.join(root, "server.js"), `${remoteRoot}/server.js`);
    await sftpPut(conn, path.join(root, "test", "smoke.test.cjs"), `${remoteRoot}/test/smoke.test.cjs`);
    deployed = true;

    console.log("\n=== pm2 reload api + worker ===");
    await exec(conn, `cd ${remoteRoot} && pm2 reload ecosystem.config.cjs --only davidsklad-api,davidsklad-worker --update-env`);
    await exec(conn, "sleep 12");

    console.log("\n=== worker BACKGROUND_JOBS_ENABLED check ===");
    const envOut = await exec(conn, `cd ${remoteRoot} && pm2 jlist`);
    let jlist;
    try { jlist = JSON.parse(envOut); } catch { jlist = []; }
    const worker = jlist.find((p) => p.name === "davidsklad-worker");
    const bg = worker?.pm2_env?.env?.BACKGROUND_JOBS_ENABLED ?? worker?.pm2_env?.BACKGROUND_JOBS_ENABLED;
    workerOk = String(bg) === "true";
    console.log("WORKER_BACKGROUND_JOBS_ENABLED=" + bg + " ok=" + workerOk);

    console.log("\n=== trigger reprice (one-shot) ===");
    await sftpPutString(conn, triggerScript, `${remoteRoot}/scripts/_one-shot-hard-currency-reprice.cjs`);
    await exec(conn, `cd ${remoteRoot} && node scripts/_one-shot-hard-currency-reprice.cjs`);

    console.log("\n=== monitor snapshot ===");
    await sftpPut(conn, path.join(root, "scripts", "_prod-monitor-progress.cjs"), `${remoteRoot}/scripts/_prod-monitor-progress.cjs`);
    const monOut = await exec(conn, `cd ${remoteRoot} && node scripts/_prod-monitor-progress.cjs`);
    try {
      const snap = JSON.parse(monOut.trim().split("\n").pop() || "{}");
      queuedCount = snap?.salesAutomation?.reasons?.queued ?? null;
      pending = snap?.salesAutomation?.ozonPrice?.pending ?? null;
      success = snap?.salesAutomation?.ozonPrice?.success ?? null;
    } catch { /* parse from full output if needed */ }
  } catch (e) {
    console.error("DEPLOY_STEP_FAILED:", e.message);
    deployed = deployed && false;
  } finally {
    conn.end();
  }

  console.log("\n=== RESULT ===");
  console.log(JSON.stringify({
    deployed: deployed ? "yes" : "no",
    queuedCount,
    workerOk: workerOk ? "yes" : "no",
    pending,
    success,
  }, null, 2));

  if (!deployed || !workerOk) process.exit(1);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
