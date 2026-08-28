#!/usr/bin/env node
"use strict";

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) process.exit(1);

const conn = new Client();
conn.on("ready", () => {
  conn.exec([
    "pm2 list",
    "ls -lt /root/.pm2/logs/davidsklad-worker* 2>/dev/null | head -6",
    "echo '--- latest worker out ---'",
    "tail -n 40 $(ls -t /root/.pm2/logs/davidsklad-worker-out*.log 2>/dev/null | head -1)",
    "echo '--- latest worker err ---'",
    "tail -n 8 $(ls -t /root/.pm2/logs/davidsklad-worker-error*.log 2>/dev/null | head -1)",
  ].join(" && "), (err, stream) => {
    if (err) throw err;
    stream.on("data", (d) => process.stdout.write(d));
    stream.stderr.on("data", (d) => process.stderr.write(d));
    stream.on("close", (code) => { conn.end(); process.exit(code || 0); });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 60000 });
