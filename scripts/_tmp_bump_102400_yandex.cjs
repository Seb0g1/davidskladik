#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }
const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteScript = remoteRoot + "/scripts/_tmp_bump_102400_run.js";

const diagScript = `
require("dotenv").config({ path: require("node:path").resolve(__dirname, "../.env") });
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
async function main() {
  // Сначала проверяем статус
  const rows = await p.$queryRawUnsafe(\`
    SELECT id, marketplace,
      raw->'lastStockSend' AS last_stock_send,
      raw->'noSupplierAutomation' AS nsa,
      target_stock,
      updated_at
    FROM warehouse_products
    WHERE raw->>'offerId' = '102400'
  \`);

  console.log("=== Состояние 102400 ===");
  let needsBump = false;
  for (const r of rows) {
    const nsa = r.nsa || {};
    const lss = r.last_stock_send || {};
    const alreadyZeroed = nsa.stockZeroAt || (lss.type === "zero_stock" && lss.status === "success" && String(r.marketplace).startsWith("yandex"));
    console.log(\`[\${r.marketplace}] id=\${r.id} target_stock=\${r.target_stock} stockZeroAt=\${nsa.stockZeroAt||null} lastSend=\${lss.type||null}/\${lss.status||null} updated=\${r.updated_at}\`);
    if (String(r.marketplace) === 'yandex' && !alreadyZeroed) needsBump = true;
  }

  if (!needsBump) {
    console.log("\\nЯндекс уже обнулён или stockZeroAt проставлен — ничего не делаем.");
    await p.$disconnect();
    return;
  }

  // Бампаем updated_at чтобы sweep взял первым
  const result = await p.$executeRawUnsafe(\`
    UPDATE warehouse_products
    SET updated_at = now()
    WHERE raw->>'offerId' = '102400'
      AND marketplace = 'yandex'
      AND (raw -> 'noSupplierAutomation' ->> 'stockZeroAt') IS NULL
  \`);
  console.log(\`\\nОбновлено строк: \${result}. Sweep подберёт товар в следующем тике (~3 мин).\`);
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
