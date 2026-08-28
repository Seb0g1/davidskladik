#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const cmd = [
  'echo "=== api uptime/restarts now ==="',
  'pm2 ls | grep -E "api|worker"',
  'echo "=== api out log: last 60 lines ==="',
  'tail -60 /root/.pm2/logs/davidsklad-api-out-1.log',
  'echo "=== api error log: last 20 ==="',
  'tail -20 /root/.pm2/logs/davidsklad-api-error-1.log',
  'echo "=== recent memory-limit kills ==="',
  'grep -c "max-memory-restart" /root/.pm2/pm2.log || true',
  'grep "max-memory-restart" /root/.pm2/pm2.log | tail -3 || true',
  'echo "=== api /health memory right now ==="',
  'curl -s -m 10 http://127.0.0.1:3000/health | node -e "let d=\'\';process.stdin.on(\'data\',c=>d+=c).on(\'end\',()=>{try{const j=JSON.parse(d);console.log(JSON.stringify({memory:j.memory,active:j.activeHttpRequests,slow:j.recentSlowRequests},null,2))}catch(e){console.log(d.slice(0,300))}})"',
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
