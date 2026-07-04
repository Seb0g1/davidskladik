#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'cd /var/www/davidsklad/davidskladik',
  'echo "=== env DAILY/IMPORT/SYNC ==="',
  'grep -iE "DAILY|FULL_IMPORT|AUTO_SYNC|OZON_SYNC" .env || true',
  'echo "=== worker log: daily sync / marketplace import ==="',
  'grep -hE "daily sync|full marketplace import|marketplace sync|ozon.*import" /root/.pm2/logs/davidsklad-worker-out-2.log 2>/dev/null | tail -50 || true',
  'echo "=== worker log: skipping / deferred ==="',
  'grep -hE "skipping|deferred" /root/.pm2/logs/davidsklad-worker-out-2.log 2>/dev/null | tail -20 || true',
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
