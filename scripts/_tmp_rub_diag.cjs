#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_rub_diag_run.js";

const diagScript = [
  'require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });',
  'const { PrismaClient } = require("@prisma/client");',
  'const p = new PrismaClient();',
  'async function main() {',
  '  // Find Inna supplier in managed_suppliers table',
  '  const innaSup = await p.$queryRawUnsafe(`',
  '    SELECT id, name, price_currency AS "priceCurrency", pricing_mode AS "pricingMode", stopped',
  '    FROM managed_suppliers',
  '    WHERE lower(name) LIKE \'%инна%\' OR lower(name) LIKE \'%inna%\'',
  '  `).catch(()=>[]);',
  '  console.log("=== Managed suppliers matching Inna ===");',
  '  innaSup.forEach(r => console.log(" ", JSON.stringify(r)));',
  '',
  '  // Check app_settings for fixedUsdRate',
  '  const settings = await p.$queryRawUnsafe(`SELECT key, value FROM app_settings ORDER BY key`).catch(()=>[]);',
  '  console.log("\\n=== All app_settings ===");',
  '  settings.forEach(r => console.log(" ", r.key, "=", r.value));',
  '',
  '  // Sample products linked to Inna supplier via product_links',
  '  if (innaSup.length) {',
  '    const sid = innaSup[0].id;',
  '    const products = await p.$queryRawUnsafe(`',
  '      SELECT wp.id, wp.offer_id, wp.target_price, wp.current_price,',
  '             wp.raw->>\'supplierPriceRub\' AS supplier_price_rub,',
  '             wp.raw->>\'selectedSupplierCurrency\' AS currency',
  '      FROM warehouse_products wp',
  '      JOIN product_links pl ON pl.product_id = wp.id',
  '      WHERE pl.supplier_id = \'${sid}\' AND wp.archived = false',
  '      LIMIT 5',
  '    `).catch(()=>[]);',
  '    console.log("\\n=== Sample Inna products via product_links (supplier_id=" + sid + ") ===");',
  '    products.forEach(r => console.log(" ", JSON.stringify(r)));',
  '    console.log("  (showing", products.length, "of possibly more)");',
  '  }',
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
