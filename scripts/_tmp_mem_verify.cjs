#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'PID=$(pm2 pid davidsklad-api | tail -1)',
  'echo "api pid: $PID"',
  'cat "/proc/$PID/environ" | tr "\\0" "\\n" | grep MALLOC || echo "MALLOC_ARENA_MAX NOT SET!"',
  'echo "=== arena-shaped mappings (~64MB) now ==="',
  'pmap -x "$PID" 2>/dev/null | awk "\\$2 >= 60000 && \\$2 <= 70000 {count++} END {print count+0}"',
  'echo "=== RSS samples over 2 min (every 15s) ==="',
  'for i in $(seq 1 8); do RSS=$(ps -o rss= -p "$PID" | tr -d " "); ET=$(ps -o etimes= -p "$PID" | tr -d " "); if [ -z "$RSS" ]; then PID=$(pm2 pid davidsklad-api | tail -1); echo "t=$i PROCESS RESTARTED, new pid=$PID"; else echo "t=$i uptime_s=$ET rss_mb=$((RSS/1024))"; fi; sleep 15; done',
  'echo "=== restarts counter ==="',
  'pm2 ls | grep -E "api|worker"',
  'echo "=== new memory-limit kills since deploy ==="',
  'tail -50 /root/.pm2/pm2.log | grep -c "max-memory-restart" || true',
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
