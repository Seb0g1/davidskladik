#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_check_102400_run.js";

const diagScript = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
async function main() {
  const rows = await p.$queryRawUnsafe(\`
    SELECT
      id,
      raw->>'offerId' AS offer_id,
      marketplace,
      target_stock,
      raw->'noSupplierAutomation' AS nsa,
      raw->'lastStockSend' AS last_stock_send,
      raw->'marketplaceState' AS mp_state,
      updated_at
    FROM warehouse_products
    WHERE raw->>'offerId' = '102400'
  \`);
  for (const r of rows) {
    console.log("offer_id:", r.offer_id, "| marketplace:", r.marketplace, "| target_stock:", r.target_stock);
    console.log("  lastStockSend:", JSON.stringify(r.last_stock_send));
    console.log("  noSupplierAutomation:", JSON.stringify(r.nsa));
    console.log("  marketplaceState:", JSON.stringify(r.mp_state));
    console.log("  updated_at:", r.updated_at);
  }
  await p.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

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
