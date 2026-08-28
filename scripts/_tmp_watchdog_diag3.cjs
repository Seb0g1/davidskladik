#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== SIGINT events per day (pm2.log) ==="',
  'grep -h "exited with code \\[0\\] via signal \\[SIGINT\\]" /root/.pm2/pm2.log | sed -E "s/^([0-9T-]+):[0-9]{2}:[0-9]{2}.*App \\[([a-z-]+)/\\1 \\2/" | cut -c1-30 | sort | uniq -c | tail -30',
  'echo "=== watchdog rotated log Jul 3 (tail) ==="',
  'tail -20 "/root/.pm2/logs/davidsklad-health-watchdog-out-3__2026-07-03_00-00-00.log" 2>/dev/null || true',
  'echo "=== watchdog process info ==="',
  'pm2 describe davidsklad-health-watchdog | grep -E "script|args|created|uptime|restarts" || true',
  'echo "=== crontab ==="',
  'crontab -l 2>/dev/null | tail -20 || true',
  'echo "=== watchdog script on server (find) ==="',
  'ls -la /var/www/davidsklad/davidskladik/scripts/ | grep -iE "watchdog|monitor" || true',
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
