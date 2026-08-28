#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
// Wait for the next api restart, then sample /health (heap) + RSS every 2s for 26s,
// and afterwards dump nginx access log lines from that window.
const cmd = [
  'OLDPID=$(pm2 pid davidsklad-api | tail -1)',
  'echo "waiting for restart of pid $OLDPID..."',
  'for i in $(seq 1 40); do NEW=$(pm2 pid davidsklad-api | tail -1); if [ "$NEW" != "$OLDPID" ]; then break; fi; sleep 2; done',
  'PID=$(pm2 pid davidsklad-api | tail -1)',
  'START=$(date +%H:%M:%S)',
  'echo "new pid $PID, sampling from $START"',
  'for i in $(seq 1 13); do RSS=$(ps -o rss= -p "$PID" | tr -d " "); H=$(curl -s -m 2 http://127.0.0.1:3000/health); HU=$(echo "$H" | grep -o \'"heapUsedMb":[0-9]*\' | cut -d: -f2); AR=$(echo "$H" | grep -o \'"activeHttpRequests":[0-9]*\' | cut -d: -f2); echo "sec=$((i*2)) rss_mb=$((RSS/1024)) heap_mb=$HU active=$AR"; sleep 2; done',
  'echo "=== nginx access log during window ==="',
  'tail -100 /var/log/nginx/access.log 2>/dev/null | grep -vE "GET /health|/api/live-status" | tail -40 || ls /var/log/nginx/',
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
