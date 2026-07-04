#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== confirm new limit applied ==="',
  'pm2 describe davidsklad-api | grep -E "max memory|restarts|uptime"',
  'echo "=== watch 5 min (every 30s): uptime should only grow ==="',
  'for i in $(seq 1 10); do PID=$(pm2 pid davidsklad-api | tail -1); RSS=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d " "); ET=$(ps -o etimes= -p "$PID" 2>/dev/null | tr -d " "); echo "t=$((i*30))s uptime_s=$ET rss_mb=$((RSS/1024))"; sleep 30; done',
  'echo "=== kills in pm2.log during watch ==="',
  'tail -30 /root/.pm2/pm2.log | grep -cE "max-memory-restart|SIGINT" || true',
  'pm2 ls | grep -E "api|worker"',
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
