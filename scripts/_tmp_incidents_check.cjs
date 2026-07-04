#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'cd /var/www/davidsklad/davidskladik',
  'echo "=== watchdog env ==="',
  'grep -iE "HEALTH_WATCHDOG" .env || echo "(none in .env)"',
  'echo "=== incidents per day ==="',
  'cut -c1-17 data/health-watchdog-incidents.jsonl 2>/dev/null | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}" | sort | uniq -c | tail -15 || true',
  'echo "=== last 12 incidents ==="',
  'tail -12 data/health-watchdog-incidents.jsonl 2>/dev/null || true',
  'echo "=== watchdog state ==="',
  'cat data/health-watchdog-state.json 2>/dev/null || true',
  'echo "=== current /health of api (time it) ==="',
  'time curl -s -m 12 http://127.0.0.1:3000/health | head -c 600',
  'echo ""',
  'echo "=== current /health of worker ==="',
  'curl -s -m 12 http://127.0.0.1:3001/health | head -c 400',
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
