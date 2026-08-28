#!/usr/bin/env node
"use strict";
const fs = require("fs");
const path = require("path");
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const localFile = path.join(__dirname, "_tmp_ozon_tnved_diag2.cjs");
const remoteFile = "/var/www/davidsklad/davidskladik/scripts/_tmp_ozon_tnved_diag2.cjs";

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error("SFTP err:", err.message); conn.end(); return; }
    sftp.fastPut(localFile, remoteFile, {}, (putErr) => {
      if (putErr) { console.error("Upload err:", putErr.message); conn.end(); return; }
      console.log("Uploaded. Running...");
      conn.exec(`cd /var/www/davidsklad/davidskladik && node scripts/_tmp_ozon_tnved_diag2.cjs 2>&1`, (execErr, stream) => {
        if (execErr) { console.error(execErr.message); conn.end(); return; }
        let out = "";
        stream.on("data", d => out += d);
        stream.stderr.on("data", d => out += d);
        stream.on("close", () => { conn.end(); console.log(out); });
      });
    });
  });
}).connect({ host: "81.17.154.153", port: 22, username: "root", password });
conn.on("error", e => { console.error("SSH:", e.message); process.exit(1); });
