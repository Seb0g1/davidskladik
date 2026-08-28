#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_supplier_stop_diag_run.js";

const diagScript = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();

async function main() {
  // Найти продукты по артикулу
  const products = await p.$queryRawUnsafe(\`
    SELECT
      id,
      raw->>'offerId' AS offer_id,
      raw->>'selectedSupplier' AS selected_supplier,
      raw->'links' AS links,
      raw->'ozonStock' AS ozon_stock,
      raw->'yandexStock' AS yandex_stock,
      raw->>'wbStock' AS wb_stock,
      raw->'lastStockSend' AS last_stock_send,
      raw->'lastArchiveSend' AS last_archive_send,
      archived,
      updated_at
    FROM warehouse_products
    WHERE
      raw->>'offerId' IN ('102400', 'BRCU30')
      OR raw->>'article' IN ('102400', 'BRCU30')
      OR raw->>'id' IN ('102400', 'BRCU30')
    LIMIT 10
  \`);

  console.log("\\n=== Продукты 102400 и BRCU30 ===");
  for (const pr of products) {
    console.log("\\n--- offer_id:", pr.offer_id, "| id:", String(pr.id), "| archived:", pr.archived);
    console.log("  selectedSupplier:", pr.selected_supplier || "(null)");
    const links = pr.links || [];
    if (Array.isArray(links)) {
      links.forEach((l, i) => {
        console.log(\`  link[\${i}]: supplierName="\${l.supplierName}" active=\${l.active} snooze=\${JSON.stringify(l.snooze||null)}\`);
      });
    }
    console.log("  ozonStock:", JSON.stringify(pr.ozon_stock));
    console.log("  yandexStock:", JSON.stringify(pr.yandex_stock));
    console.log("  wbStock:", pr.wb_stock);
    console.log("  lastStockSend:", JSON.stringify(pr.last_stock_send));
    console.log("  lastArchiveSend:", JSON.stringify(pr.last_archive_send));
    console.log("  updated_at:", pr.updated_at);
  }

  // Проверить managed-поставщиков на остановку
  const supplierSettings = await p.$queryRawUnsafe(\`
    SELECT value FROM app_settings WHERE key = 'suppliers'
  \`);
  if (supplierSettings.length) {
    const suppliers = JSON.parse(supplierSettings[0].value || "[]");
    const stopped = suppliers.filter(s => s.stopped);
    console.log("\\n=== Остановленные поставщики ===");
    stopped.forEach(s => console.log(\` name="\${s.name}" stopped=\${s.stopped} inactiveUntil=\${s.inactiveUntil||''}\`));
  }

  await p.$disconnect();
}
main().catch(e => { console.error("diag error:", e.message); process.exit(1); });
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
