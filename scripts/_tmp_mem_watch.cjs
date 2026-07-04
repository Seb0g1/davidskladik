#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
// Sample heap (from /health) and RSS (ps) every 4s for ~80s to see what grows before the pm2 kill.
const cmd = [
  'for i in $(seq 1 20); do',
  '  PID=$(pm2 pid davidsklad-api 2>/dev/null | tail -1);',
  '  RSS=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d " ");',
  '  ET=$(ps -o etimes= -p "$PID" 2>/dev/null | tr -d " ");',
  '  H=$(curl -s -m 3 http://127.0.0.1:3000/health | grep -o \'"heapUsedMb":[0-9]*\' | cut -d: -f2);',
  '  A=$(curl -s -m 3 http://127.0.0.1:3000/health | grep -o \'"activeHttpRequests":[0-9]*\' | cut -d: -f2);',
  '  echo "t=$i pid=$PID uptime_s=$ET rss_mb=$((RSS/1024)) heap_mb=$H active_req=$A";',
  '  sleep 4;',
  'done',
].join(" ");
const conn = new Client();
conn.on("ready", () => {
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err); conn.end(); return; }
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", () => conn.end());
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
