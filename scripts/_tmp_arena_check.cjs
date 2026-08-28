#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'PID=$(pm2 pid davidsklad-api | tail -1)',
  'echo "api pid: $PID"',
  'echo "=== RSS by mapping size (top anon regions) ==="',
  'pmap -x "$PID" 2>/dev/null | sort -k3 -n -r | head -15',
  'echo "=== count of ~64MB arena-shaped mappings ==="',
  'pmap -x "$PID" 2>/dev/null | awk "\\$2 >= 60000 && \\$2 <= 70000 {count++} END {print count+0}"',
  'echo "=== nproc / MALLOC_ARENA_MAX ==="',
  'nproc',
  'cat "/proc/$PID/environ" 2>/dev/null | tr "\\0" "\\n" | grep -E "MALLOC|NODE_OPTIONS" || echo "(no MALLOC vars)"',
  'echo "=== total anon rss (MB) ==="',
  'pmap -x "$PID" 2>/dev/null | tail -1',
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
