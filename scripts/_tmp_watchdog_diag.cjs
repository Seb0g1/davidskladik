#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== watchdog log (last 40) ==="',
  'tail -40 /root/.pm2/logs/davidsklad-health-watchdog-out-3.log 2>/dev/null || ls /root/.pm2/logs/ | grep -i watchdog',
  'echo "=== watchdog error log (last 10) ==="',
  'tail -10 /root/.pm2/logs/davidsklad-health-watchdog-error-3.log 2>/dev/null || true',
  'echo "=== worker starts per day (scheduler enabled msg) ==="',
  'grep -h "ozon yandex auto import scheduler enabled" /root/.pm2/logs/davidsklad-worker-out-2.log 2>/dev/null | sed -E "s/.*\\"time\\":\\"([0-9-]+)T.*/\\1/" | sort | uniq -c || true',
  'echo "=== api starts per day ==="',
  'grep -hc "" /dev/null; grep -h "server listening\\|api ready\\|Server started\\|listening on" /root/.pm2/logs/davidsklad-api-out-1.log 2>/dev/null | tail -5 || true',
  'echo "=== free memory ==="',
  'free -m',
  'echo "=== api log last heap/memory pressure ==="',
  'grep -hE "memory pressure|heap|restart" /root/.pm2/logs/davidsklad-api-out-1.log 2>/dev/null | tail -15 || true',
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
