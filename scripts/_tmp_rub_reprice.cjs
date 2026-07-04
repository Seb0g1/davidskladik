#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_rub_reprice_run.js";

const diagScript = [
  'require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });',
  'const { PrismaClient } = require("@prisma/client");',
  'const p = new PrismaClient();',
  'async function main() {',
  '  // Clear current_price for all Inna products so price sweep picks them up',
  '  // The next BullMQ reprice job will recompute from PM with fixed formula',
  '  const result = await p.$executeRawUnsafe(`',
  '    UPDATE warehouse_products wp',
  '    SET current_price = NULL, updated_at = now()',
  '    FROM product_links pl',
  '    WHERE pl.product_id = wp.id',
  '      AND pl.price_currency = \'RUB\'',
  '      AND (lower(pl.supplier_name) LIKE \'%инна%\' OR lower(pl.supplier_name) LIKE \'%inna%\')',
  '      AND wp.archived = false',
  '      AND wp.target_price IS NOT NULL AND wp.target_price > 0',
  '  `).catch(e => { console.error("update failed:", e.message); return 0; });',
  '  console.log("Updated rows (current_price = NULL):", result);',
  '',
  '  // Verify',
  '  const check = await p.$queryRawUnsafe(`',
  '    SELECT COUNT(DISTINCT wp.id)::int AS n',
  '    FROM warehouse_products wp',
  '    JOIN product_links pl ON pl.product_id = wp.id AND pl.price_currency = \'RUB\'',
  '      AND (lower(pl.supplier_name) LIKE \'%инна%\' OR lower(pl.supplier_name) LIKE \'%inna%\')',
  '    WHERE wp.archived = false',
  '      AND wp.target_price IS NOT NULL AND wp.target_price > 0',
  '      AND wp.current_price IS NULL',
  '  `).catch(()=>[{n:-1}]);',
  '  console.log("Products now with current_price=NULL:", check[0]?.n);',
  '  console.log("Price sweep will pick these up within 120 seconds.");',
  '',
  '  await p.$disconnect();',
  '}',
  'main().catch(e=>{ console.error("diag error:", e.message); process.exit(1); });',
].join("\n");

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
