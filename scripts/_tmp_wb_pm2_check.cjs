#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const conn = new Client();
conn.on("ready", () => {
  const cmd = `
    echo "=== SERVER TIME ===" && date -u
    echo ""
    echo "=== PM2 STATUS ===" && pm2 list --no-color 2>&1 | head -20
    echo ""
    echo "=== WB SYNC LAST 40 LINES ===" && pm2 logs davidsklad-worker --lines 40 --nostream --no-color 2>&1 | grep -i "wb" | tail -30
    echo ""
    echo "=== WB STATUS FILE ===" && cat /var/www/davidsklad/davidskladik/data/wb-sync-status.json 2>/dev/null
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
