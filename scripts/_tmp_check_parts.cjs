#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const conn = new Client();
conn.on("ready", () => {
  const cmd = [
    `echo "=== server/parts exists ==="`,
    `ls ${remoteRoot}/server/parts/ 2>/dev/null | wc -l`,
    `echo "=== dashboard summary priceQueue logic ==="`,
    `grep -n "last_calculated_at\|priceStatus.*pending\|salesAutomation.*count" ${remoteRoot}/server/parts/02d-dashboard-summary.js 2>/dev/null | head -5 || echo "File not found"`,
    `echo "=== PM fresh products outer timeout ==="`,
    `grep -n "preNorm\|specialCount\|articleUniqueCount" ${remoteRoot}/server/parts/02a-price-master-fresh-products.js 2>/dev/null | head -5 || echo "Not found"`,
    `echo "=== summary inflight ==="`,
    `grep -n "warehousePostgresSummaryInflight" ${remoteRoot}/server/parts/02a-warehouse-postgres-summary.js 2>/dev/null | head -5 || echo "Not found"`,
  ].join(" && ");
  conn.exec(cmd, (err, stream) => {
    if (err) { console.error(err); conn.end(); return; }
    stream.on("data", d => process.stdout.write(d));
    stream.stderr.on("data", d => process.stderr.write(d));
    stream.on("close", () => conn.end());
  });
}).connect({ host: "davidsklad.ru", port: 22, username: "root", password });
