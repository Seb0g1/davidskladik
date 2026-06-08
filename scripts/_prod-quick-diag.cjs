#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const cmd = [
  "pm2 list",
  "free -h | head -2",
  "ps aux | grep -E 'node scripts/(delete|repair|audit)' | grep -v grep || echo 'no heavy scripts'",
  "echo ===ERR===",
  "pm2 logs davidsklad --lines 40 --nostream --err || true",
  "echo ===OUT===",
  "pm2 logs davidsklad --lines 25 --nostream --out || true",
  "echo ===HTTP===",
  "curl -s -o /dev/null -w 'health:%{http_code}\\n' http://127.0.0.1:3000/api/health",
  "curl -s -o /dev/null -w 'app:%{http_code}\\n' http://127.0.0.1:3000/app/",
  "curl -s -o /dev/null -w 'js:%{http_code}\\n' http://127.0.0.1:3000/app-modern/assets/index-CXA1cW0t.js",
  "curl -s -o /dev/null -w 'oldjs:%{http_code}\\n' http://127.0.0.1:3000/app-modern/assets/index-CoMd9wpd.js",
  "echo ===INDEX===",
  "head -12 /var/www/davidsklad/davidskladik/public/app-modern/index.html",
  "ls -la /var/www/davidsklad/davidskladik/public/app-modern/assets/ | tail -6",
].join(" && ");

const conn = new Client();
conn.on("ready", () => {
  conn.exec(cmd, (err, stream) => {
    if (err) throw err;
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
  });
}).on("error", (e) => { console.error(e.message); process.exit(1); })
  .connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 60000 });
