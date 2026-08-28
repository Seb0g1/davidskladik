#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const conn = new Client();
conn.on("ready", () => {
  conn.exec([
    `cd ${remoteRoot}`,
    "pm2 list",
    "pm2 logs davidsklad-api --lines 15 --nostream --out || true",
    "pm2 logs davidsklad-api --lines 15 --nostream --err || true",
    "sleep 8",
    "curl -s -o /dev/null -w 'api:%{http_code} %{time_total}s\\n' --max-time 15 http://127.0.0.1:3000/api/live-status || true",
    "curl -s -o /dev/null -w 'worker:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3001/health || true",
    "node scripts/prod-post-deploy-check.cjs",
  ].join(" && "), (err, stream) => {
    if (err) { console.error(err); process.exit(1); }
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
  });
}).on("error", (e) => { console.error(e.message); process.exit(1); })
  .connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 60000 });
