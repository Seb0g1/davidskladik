#!/usr/bin/env node
"use strict";

/**
 * Удаляет дублирующиеся строки из документа-заказа #665 в PriceMaster.
 * Запуск: DEPLOY_PASSWORD=... node scripts/fix-doc-665-duplicates.cjs
 */

const path = require("node:path");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) {
  console.error("DEPLOY_PASSWORD is required");
  process.exit(1);
}

const remoteRoot = "/var/www/davidsklad/davidskladik";

// Этот скрипт запускается на сервере — имеет доступ к .env и mysql2
const fixScript = `#!/usr/bin/env node
"use strict";
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const mysql = require("mysql2/promise");

async function main() {
  const conn = await mysql.createConnection({
    host: process.env.PM_DB_HOST || "localhost",
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME,
  });
  try {
    const DOC_ID = Number(process.env.FIX_DOC_ID || 665);
    console.log("\\n=== ДОКУМЕНТ #" + DOC_ID + " ===");

    // Показываем всё что есть
    const [allRows] = await conn.query(
      "SELECT RowID, OfferRowID, RequestQuant FROM RequestRows WHERE DocID=? ORDER BY OfferRowID, RowID",
      [DOC_ID],
    );
    console.log("Всего строк:", allRows.length);

    // Ищем дубли по OfferRowID
    const [dupes] = await conn.query(\`
      SELECT OfferRowID,
             COUNT(*) AS cnt,
             GROUP_CONCAT(RowID ORDER BY RowID SEPARATOR ',') AS row_ids,
             SUM(RequestQuant) AS total_quant
      FROM RequestRows
      WHERE DocID=?
      GROUP BY OfferRowID
      HAVING cnt > 1
    \`, [DOC_ID]);
    console.log("Дублирующихся OfferRowID:", dupes.length);
    for (const d of dupes) {
      console.log("  OfferRowID", d.OfferRowID, "| строки:", d.row_ids, "| кол-во:", d.total_quant);
    }
    if (!dupes.length) {
      console.log("Дублей нет, ничего не удаляем.");
      return;
    }

    // Удаляем дубли — оставляем строку с наименьшим RowID для каждого OfferRowID
    const [del] = await conn.query(\`
      DELETE r FROM RequestRows r
      INNER JOIN (
        SELECT MIN(RowID) AS keep_id, OfferRowID
        FROM RequestRows
        WHERE DocID=?
        GROUP BY OfferRowID
      ) AS keepers ON r.OfferRowID = keepers.OfferRowID AND r.RowID <> keepers.keep_id
      WHERE r.DocID=?
    \`, [DOC_ID, DOC_ID]);
    console.log("\\nУдалено строк:", del.affectedRows);

    const [remaining] = await conn.query(
      "SELECT COUNT(*) AS cnt FROM RequestRows WHERE DocID=?",
      [DOC_ID],
    );
    console.log("Осталось строк:", remaining[0].cnt);
    console.log("\\nДокумент #" + DOC_ID + " исправлен.");
  } finally {
    await conn.end();
  }
}
main().catch((e) => { console.error(e.message || e); process.exit(1); });
`;

function exec(conn, command) {
  return new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      let out = "";
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => { process.stderr.write(d); });
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}: ${command}`)) : resolve(out)));
    });
  });
}

function sftpPutString(conn, content, remotePath) {
  return new Promise((resolve, reject) => {
    conn.sftp((err, sftp) => {
      if (err) return reject(err);
      const ws = sftp.createWriteStream(remotePath);
      ws.on("close", resolve);
      ws.on("error", reject);
      ws.end(content, "utf8");
    });
  });
}

async function connect() {
  const conn = new Client();
  await new Promise((resolve, reject) => {
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153",
      username: "root",
      password,
      readyTimeout: 30000,
    });
  });
  return conn;
}

async function main() {
  console.log("=== Подключение к серверу ===");
  const conn = await connect();
  try {
    const remoteFix = `${remoteRoot}/scripts/_one-shot-fix-doc-dupes.cjs`;
    await sftpPutString(conn, fixScript, remoteFix);
    console.log("=== Запуск деdup-скрипта на сервере ===");
    await exec(conn, `cd ${remoteRoot} && node ${remoteFix}`);
    console.log("\n=== Очистка ===");
    await exec(conn, `rm -f ${remoteFix}`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
