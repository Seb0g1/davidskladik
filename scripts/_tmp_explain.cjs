#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_explain_run.js";

const lines = [
  'const https = require("https");',
  'const http = require("http");',
  // Hit the health endpoint from localhost
  'const req = http.get("http://localhost:3000/api/health", (res) => {',
  '  let body = "";',
  '  res.on("data", d => body += d);',
  '  res.on("end", () => {',
  '    try {',
  '      const data = JSON.parse(body);',
  '      console.log("recentSlowRequests:", JSON.stringify(data.recentSlowRequests || []));',
  '      console.log("heapPressureRatio:", data.heapPressureRatio);',
  '    } catch(e) { console.log("parse error:", e.message, body.slice(0,200)); }',
  '  });',
  '});',
  'req.on("error", e => console.log("req error:", e.message));',
  'req.setTimeout(5000, () => { req.destroy(); console.log("timeout"); });',
];

const diagScript = lines.join("\n");

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error(err); conn.end(); return; }
    const ws = sftp.createWriteStream(remoteScript);
    ws.on("close", () => {
      conn.exec("cd " + remoteRoot + " && node " + remoteScript + " 2>&1", (err2, stream) => {
        if (err2) { console.error(err2); conn.end(); return; }
        stream.on("data", d => process.stdout.write(d));
        stream.stderr.on("data", d => process.stderr.write(d));
        stream.on("close", () => { conn.exec("rm -f " + remoteScript, () => conn.end()); });
      });
    });
    ws.end(diagScript);
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
