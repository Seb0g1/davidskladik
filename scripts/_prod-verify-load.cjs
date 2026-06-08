#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);
const cmd = [
  "ps aux | grep -E 'repair-yandex|delete-yandex' | grep -v grep || echo 'no maintenance scripts'",
  "curl -s -o /dev/null -w 'page:%{http_code} %{time_total}s\\n' --max-time 20 'http://127.0.0.1:3000/api/warehouse/products/page?page=1&pageSize=40&grouped=true'",
  "grep -E 'slow request|warehouse_fast_page_build_timeout' /root/.pm2/logs/davidsklad-error-0.log | tail -6",
].join(" && echo '---' && ");
const conn = new Client();
conn.on("ready", () => {
  conn.exec(cmd, (err, stream) => {
    if (err) throw err;
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 30000 });
