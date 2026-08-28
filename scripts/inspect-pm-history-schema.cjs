#!/usr/bin/env node
"use strict";
// Запускается через SSH на продакшн-сервере.
// Ищет таблицы истории продаж в PriceMasterDB и показывает их структуру.
const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");

require("dotenv").config();
const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";

const remoteScript = `
require("dotenv").config();
const mysql = require("mysql2/promise");

async function main() {
  const conn = await mysql.createConnection({
    host: process.env.PM_DB_HOST,
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME,
    connectTimeout: 10000,
  });

  // Все таблицы с примерным числом строк
  const [tables] = await conn.query(
    "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"
  );
  console.log("=== ALL TABLES ===");
  tables.forEach(t => console.log("  " + t.TABLE_NAME + "  (~" + t.TABLE_ROWS + " rows)"));

  // Ищем таблицы с историей продаж
  const salesCandidates = tables
    .map(t => t.TABLE_NAME)
    .filter(n => /histor|sale|sell|realiz|продаж|движен|check|чек|прода|invoice|receipt|doc/i.test(n));

  console.log("\\n=== HISTORY/SALES CANDIDATES ===");
  for (const tbl of salesCandidates) {
    const [cols] = await conn.query(
      \`SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION\`,
      [tbl]
    );
    const [cnt] = await conn.query(\`SELECT COUNT(*) AS cnt FROM \\\`\${tbl}\\\`\`);
    console.log("\\n# " + tbl + "  (rows: " + cnt[0].cnt + ")");
    cols.forEach(c => console.log("  " + c.COLUMN_NAME + "  " + c.DATA_TYPE + (c.CHARACTER_MAXIMUM_LENGTH ? "("+c.CHARACTER_MAXIMUM_LENGTH+")" : "")));

    const [sample] = await conn.query(\`SELECT * FROM \\\`\${tbl}\\\` LIMIT 2\`);
    if (sample.length) console.log("  Sample:", JSON.stringify(sample[0]).slice(0, 300));
  }

  await conn.end();
}
main().catch(e => { console.error("ERROR:", e.message); process.exit(1); });
`;

const conn = new Client();
conn.on("ready", () => {
  conn.exec(
    `cd ${remoteRoot} && node -e ${JSON.stringify(remoteScript)}`,
    (err, stream) => {
      if (err) { console.error(err); conn.end(); return; }
      stream.on("data", d => process.stdout.write(d));
      stream.stderr.on("data", d => process.stderr.write(d));
      stream.on("close", () => conn.end());
    }
  );
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
