#!/usr/bin/env node
"use strict";

/**
 * Находит и удаляет дублирующиеся строки во всех документах PriceMaster.
 * Запуск: DEPLOY_PASSWORD=... node scripts/fix-all-doc-duplicates.cjs
 * Опционально: FIX_DAYS_BACK=30 (default 90) чтобы ограничить диапазон.
 */

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

const remoteRoot = "/var/www/davidsklad/davidskladik";

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
    multipleStatements: true,
  });
  try {
    const daysBack = Number(process.env.FIX_DAYS_BACK || 90);
    console.log("\\n=== Ищем дубли за последние", daysBack, "дней ===\\n");

    // Все DocID с дублями за указанный период
    const [affected] = await conn.query(\`
      SELECT r.DocID,
             d.DocDate,
             d.PartnerID,
             COUNT(*) AS total_rows,
             SUM(CASE WHEN dup.cnt > 1 THEN 1 ELSE 0 END) AS dup_rows,
             COUNT(DISTINCT CASE WHEN dup.cnt > 1 THEN r.OfferRowID END) AS dup_offers
      FROM RequestRows r
      JOIN RequestDocs d ON d.DocID = r.DocID
      JOIN (
        SELECT DocID, OfferRowID, COUNT(*) AS cnt
        FROM RequestRows
        GROUP BY DocID, OfferRowID
        HAVING cnt > 1
      ) dup ON dup.DocID = r.DocID AND dup.OfferRowID = r.OfferRowID
      WHERE d.DocDate >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY r.DocID, d.DocDate, d.PartnerID
      ORDER BY r.DocID DESC
    \`, [daysBack]);

    if (!affected.length) {
      console.log("Дублей не найдено. Все документы чистые.");
      return;
    }

    console.log("Документов с дублями:", affected.length);
    let totalDupRows = 0;
    for (const doc of affected) {
      console.log("  Doc #" + doc.DocID
        + " (" + (doc.DocDate ? new Date(doc.DocDate).toLocaleDateString("ru-RU") : "?") + ")"
        + " | всего строк: " + doc.total_rows
        + " | дублирующихся позиций: " + doc.dup_offers
        + " | строк к удалению: " + (doc.dup_rows - doc.dup_offers));
      totalDupRows += (Number(doc.dup_rows) - Number(doc.dup_offers));
    }
    console.log("\\nВсего строк к удалению:", totalDupRows);
    console.log("\\nУдаляем дубли (оставляем строку с MIN(RowID) на каждый DocID+OfferRowID)...");

    // Удаляем все дубли за один запрос
    const [del] = await conn.query(\`
      DELETE r FROM RequestRows r
      INNER JOIN (
        SELECT MIN(RowID) AS keep_id, DocID, OfferRowID
        FROM RequestRows
        WHERE DocID IN (SELECT DocID FROM RequestDocs WHERE DocDate >= DATE_SUB(NOW(), INTERVAL ? DAY))
        GROUP BY DocID, OfferRowID
      ) AS keepers
        ON r.DocID = keepers.DocID
       AND r.OfferRowID = keepers.OfferRowID
       AND r.RowID <> keepers.keep_id
      INNER JOIN RequestDocs d ON d.DocID = r.DocID
      WHERE d.DocDate >= DATE_SUB(NOW(), INTERVAL ? DAY)
    \`, [daysBack, daysBack]);

    console.log("Удалено строк:", del.affectedRows);

    // Итоговая проверка
    const [check] = await conn.query(\`
      SELECT COUNT(*) AS remaining_dupes
      FROM (
        SELECT DocID, OfferRowID, COUNT(*) AS cnt
        FROM RequestRows
        WHERE DocID IN (SELECT DocID FROM RequestDocs WHERE DocDate >= DATE_SUB(NOW(), INTERVAL ? DAY))
        GROUP BY DocID, OfferRowID
        HAVING cnt > 1
      ) t
    \`, [daysBack]);
    const remaining = Number(check[0].remaining_dupes || 0);
    if (remaining === 0) {
      console.log("\\nПроверка: дублей больше нет. Все документы исправлены.");
    } else {
      console.log("\\nВНИМАНИЕ: осталось дублирующихся пар:", remaining, "— проверьте вручную.");
    }
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
      stream.on("close", (code) => (code ? reject(new Error(`exit ${code}`)) : resolve(out)));
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
    const remote = `${remoteRoot}/scripts/_one-shot-fix-all-dupes.cjs`;
    await sftpPutString(conn, fixScript, remote);
    console.log("=== Запуск на сервере ===\n");
    await exec(conn, `cd ${remoteRoot} && node ${remote}`);
    await exec(conn, `rm -f ${remote}`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
