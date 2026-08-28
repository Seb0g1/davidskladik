#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== pm2 log files ==="',
  'ls -la /root/.pm2/logs/ | grep -iE "watchdog|worker|api"',
  'echo "=== watchdog out (any file) ==="',
  'tail -30 /root/.pm2/logs/davidsklad-health-watchdog-out*.log 2>/dev/null || true',
  'echo "=== pm2 describe worker ==="',
  'pm2 describe davidsklad-worker | grep -E "restarts|uptime|created|memory|exit|status" || true',
  'echo "=== pm2 main log (restart events, last 30) ==="',
  'grep -hE "davidsklad-(api|worker)" /root/.pm2/pm2.log 2>/dev/null | grep -iE "restart|exited|signal|kill" | tail -30 || true',
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
