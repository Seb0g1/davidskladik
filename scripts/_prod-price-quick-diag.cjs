#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);
const cmd = [
  "pm2 list",
  "free -h | head -2",
  "curl -s -o /dev/null -w 'live:%{http_code} %{time_total}s\\n' --max-time 10 http://127.0.0.1:3000/api/live-status || true",
  "curl -s -o /dev/null -w 'login:%{http_code} %{time_total}s\\n' --max-time 10 -X POST http://127.0.0.1:3000/api/login -H 'Content-Type: application/json' -d '{}' || true",
  "ps aux --sort=-%cpu | head -8",
].join(" && echo '---' && ");
const conn = new Client();
conn.on("ready", () => {
  conn.exec(cmd, (err, stream) => {
    if (err) throw err;
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
