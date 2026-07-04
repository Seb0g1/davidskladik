#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'cd /var/www/davidsklad/davidskladik',
  'echo "=== pm2 status ==="',
  'pm2 ls | grep -E "api|worker"',
  'echo "=== RSS samples over 3 min (every 20s) ==="',
  'for i in $(seq 1 9); do PID=$(pm2 pid davidsklad-api | tail -1); RSS=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d " "); ET=$(ps -o etimes= -p "$PID" 2>/dev/null | tr -d " "); H=$(curl -s -m 3 http://127.0.0.1:3000/health | grep -o \'"heapUsedMb":[0-9]*\' | cut -d: -f2 | head -1); echo "t=$((i*20))s uptime_s=$ET rss_mb=$((RSS/1024)) heap_mb=$H"; sleep 20; done',
  'echo "=== memory-limit kills in last 100 pm2.log lines ==="',
  'tail -100 /root/.pm2/pm2.log | grep -c "max-memory-restart" || true',
  'echo "=== post-deploy check ==="',
  'node scripts/prod-post-deploy-check.cjs 2>&1 | tail -20',
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
