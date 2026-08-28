#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const conn = new Client();
conn.on("ready", () => {
  const cmd = `pm2 logs davidsklad-api --lines 200 --nostream --no-color 2>&1 | grep -i "tnved\\|тнвэд\\|ozon.*apply\\|apply.*ozon\\|category.*attr\\|attr.*found\\|tnved.*apply\\|apply.*tnved" | tail -30`;
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err.message); conn.end(); return; }
    let out = "";
    stream.on("data", d => out += d);
    stream.stderr.on("data", d => out += d);
    stream.on("close", () => {
      conn.end();
      console.log("=== API TNVED LOGS ===\n" + (out || "(nothing)"));
    });
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password });
conn.on("error", e => { console.error("SSH:", e.message); process.exit(1); });
