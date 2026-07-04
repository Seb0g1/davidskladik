#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== pm2.log tail (last 15) ==="',
  'tail -15 /root/.pm2/pm2.log',
  'echo "=== memory-limit restarts in pm2.log ==="',
  'grep -c "memory limit" /root/.pm2/pm2.log || true',
  'grep "memory limit" /root/.pm2/pm2.log | tail -5 || true',
  'echo "=== current status ==="',
  'pm2 ls',
  'echo "=== api RSS growth: sample 3x over 20s ==="',
  'for i in 1 2 3; do ps -o rss=,etimes= -p $(pm2 pid davidsklad-api) 2>/dev/null; sleep 8; done',
  'echo "=== worker out log yesterday: daily sync mentions ==="',
  'grep -hE "daily sync|full marketplace import" "/root/.pm2/logs/davidsklad-worker-out-2__2026-07-04_00-00-00.log" 2>/dev/null | tail -10 || true',
  'echo "=== api out log yesterday: daily sync mentions ==="',
  'grep -hE "daily sync|full marketplace import" "/root/.pm2/logs/davidsklad-api-out-1__2026-07-04_00-00-00.log" 2>/dev/null | tail -10 || true',
].join(" ; ");
const conn = new Client();
conn.on("ready", () => {
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err); conn.end(); return; }
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", () => conn.end());
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
