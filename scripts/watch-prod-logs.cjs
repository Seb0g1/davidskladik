#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const durationMs = Math.max(60_000, Number(process.env.WATCH_DURATION_MS || 30 * 60_000));
const intervalMs = Math.max(30_000, Number(process.env.WATCH_INTERVAL_MS || 120_000));
const startedAt = Date.now();
const deadline = startedAt + durationMs;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d.toString(); });
      stream.stderr.on("data", (d) => { out += d.toString(); });
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}: ${out}`)) : resolve(out)));
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
    });
  });
  return conn;
}

async function fetchIssues(sinceIso) {
  const conn = await connect();
  const sincePrefix = sinceIso.slice(0, 16);
  try {
    const cmd = [
      "echo '=== PM2 ==='",
      "pm2 jlist | node -e \"let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>{const j=JSON.parse(d);const p=j.find(x=>x.name==='davidsklad');console.log(JSON.stringify({status:p?.pm2_env?.status,restarts:p?.pm2_env?.restart_time,uptimeSec:p?.pm2_env?.pm_uptime?Math.floor((Date.now()-p.pm2_env.pm_uptime)/1000):null,memoryMb:p?.monit?.memory?Math.round(p.monit.memory/1024/1024):null},null,2));});\"",
      "echo '=== NEW ERRORS/WARNS ==='",
      `tail -n 1500 /root/.pm2/logs/davidsklad-error-0.log /root/.pm2/logs/davidsklad-out-0.log 2>/dev/null | grep -E '"level":"(error|warn)"|request error|Unhandled|ECONN|timeout|failed' | grep '${sincePrefix}' | tail -n 80`,
      "echo '=== HEALTH ==='",
      "curl -sS -m 15 http://127.0.0.1:3000/health | node -e \"let d='';process.stdin.on('data',c=>d+=c);process.stdin.on('end',()=>{try{const j=JSON.parse(d);console.log(JSON.stringify({ok:j.ok,version:j.version,slow:j.components?.runtime?.slowRequests?.totalRecent,stateWarnings:j.components?.runtime?.stateWarnings},null,2));}catch(e){console.log(d.slice(0,500));}});\"",
    ].join(" && ");
    return await exec(conn, cmd);
  } finally {
    conn.end();
  }
}

function logBlock(title, body) {
  const ts = new Date().toISOString();
  console.log(`\n[${ts}] ${title}`);
  console.log(body.trim() || "(no new issues)");
}

async function main() {
  console.log(JSON.stringify({
    watchStartedAt: new Date(startedAt).toISOString(),
    watchEndsAt: new Date(deadline).toISOString(),
    intervalMs,
  }, null, 2));

  let sinceIso = new Date(startedAt - 5000).toISOString().slice(0, 19);
  while (Date.now() < deadline) {
    try {
      const out = await fetchIssues(sinceIso);
      logBlock("prod log check", out);
      const errorHits = (out.match(/"level":"error"/g) || []).length;
      const warnHits = (out.match(/"level":"warn"/g) || []).length;
      if (errorHits > 0) {
        console.log(`ALERT errors=${errorHits} warns=${warnHits}`);
      }
      sinceIso = new Date().toISOString().slice(0, 19);
    } catch (error) {
      logBlock("watch failed", error.message || String(error));
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    await new Promise((resolve) => setTimeout(resolve, Math.min(intervalMs, remaining)));
  }
  console.log(`\n[${new Date().toISOString()}] watch complete (${Math.round((Date.now() - startedAt) / 1000)}s)`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
