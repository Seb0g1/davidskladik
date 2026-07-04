#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== pm2 status ==="',
  'pm2 jlist 2>/dev/null | node -e "const l=JSON.parse(require(String.fromCharCode(39)+\'fs\'+String.fromCharCode(39)).readFileSync(0));for(const p of l)console.log(p.name,\'| restarts:\',p.pm2_env.restart_time,\'| uptime min:\',Math.round((Date.now()-p.pm2_env.pm_uptime)/60000),\'| status:\',p.pm2_env.status,\'| mem MB:\',Math.round(p.monit.memory/1048576))" 2>/dev/null || pm2 ls',
  'echo "=== worker error log (last 30) ==="',
  'tail -30 /root/.pm2/logs/davidsklad-worker-error-2.log 2>/dev/null || true',
  'echo "=== worker out: last lines before a restart (search exit/signal/memory) ==="',
  'grep -hE "SIGTERM|SIGINT|graceful|shutdown|heap|memory|OOM|out of memory|FATAL|watchdog" /root/.pm2/logs/davidsklad-worker-out-2.log 2>/dev/null | tail -25 || true',
  'echo "=== dmesg OOM killer ==="',
  'dmesg -T 2>/dev/null | grep -iE "killed process|out of memory" | tail -10 || true',
  'echo "=== worker: daily sync / auto sync complete mentions ==="',
  'grep -hE "daily sync tick|auto sync complete|auto sync failed|auto sync skipped" /root/.pm2/logs/davidsklad-worker-out-2.log 2>/dev/null | tail -15 || true',
  'echo "=== health watchdog / restart reasons in api log ==="',
  'grep -hE "watchdog|pm2 restart|health probe" /root/.pm2/logs/davidsklad-api-out-1.log 2>/dev/null | tail -15 || true',
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
