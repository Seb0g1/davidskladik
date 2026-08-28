#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const conn = new Client();
conn.on("ready", () => {
  // Get last 100 worker lines, then last 50 api lines
  const cmd = `
    echo "=== WORKER LAST 60 LINES ===" && pm2 logs davidsklad-worker --lines 60 --nostream --no-color 2>&1 | tail -60
    echo ""
    echo "=== RATE LIMIT / WB PRICES in API ===" && pm2 logs davidsklad-api --lines 100 --nostream --no-color 2>&1 | grep -i "429\\|rate\\|price\\|wb\\|limit" | tail -20
  `;
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err.message); conn.end(); return; }
    let out = "";
    stream.on("data", (d) => out += d);
    stream.stderr.on("data", (d) => out += d);
    stream.on("close", () => { conn.end(); console.log(out); });
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password });
conn.on("error", (e) => { console.error("SSH:", e.message); process.exit(1); });
