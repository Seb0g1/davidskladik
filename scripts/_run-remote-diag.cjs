#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
const conn = new Client();
conn.on("ready", () => {
  conn.exec([
    "cd /var/www/davidsklad/davidskladik",
    "pm2 logs davidsklad-api --lines 80 --nostream || true",
    "echo '--- API ERR ---'",
    "pm2 logs davidsklad-api --lines 80 --nostream --err || true",
    "echo '--- WORKER OUT ---'",
    "pm2 logs davidsklad-worker --lines 40 --nostream || true",
    "echo '--- WORKER ERR ---'",
    "pm2 logs davidsklad-worker --lines 40 --nostream --err || true",
    "echo '--- TEST API START ---'",
    "timeout 8 env SERVER_ROLE=api NODE_ENV=production node api-entry.js 2>&1 | tail -30 || true",
  ].join(" && "), (err, stream) => {
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", () => { conn.end(); process.exit(0); });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 60000 });
