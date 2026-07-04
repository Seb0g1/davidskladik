#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const conn = new Client();
conn.on("ready", () => {
  const cmd = [
    `echo "=== dashboard-summary.js grep ==="`,
    `grep "last_calculated" ${remoteRoot}/server/parts/02d-dashboard-summary.js && echo FOUND || echo NOT_FOUND`,
    `echo "=== head of dashboard-summary.js ==="`,
    `head -3 ${remoteRoot}/server/parts/02d-dashboard-summary.js`,
    `echo "=== priceQueue in dashboard-summary.js ==="`,
    `grep "priceQueue" ${remoteRoot}/server/parts/02d-dashboard-summary.js | head -5`,
  ].join(" && ");
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err); conn.end(); return; }
    stream.on("data", d => process.stdout.write(d));
    stream.stderr.on("data", d => process.stderr.write(d));
    stream.on("close", () => conn.end());
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
