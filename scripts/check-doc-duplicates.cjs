#!/usr/bin/env node
"use strict";

/**
 * DRY-RUN: показывает все документы с дублями в RequestRows. Ничего не удаляет.
 * Запуск: DEPLOY_PASSWORD=... node scripts/check-doc-duplicates.cjs
 */

const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD is required"); process.exit(1); }

const remoteRoot = "/var/www/davidsklad/davidskladik";

const checkScript = `#!/usr/bin/env node
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
    const [affected] = await conn.query(\`
      SELECT
        r.DocID,
        DATE(d.DocDate) AS doc_date,
        d.PartnerID,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT CASE WHEN dup.cnt > 1 THEN r.OfferRowID END) AS dup_offers,
        SUM(CASE WHEN dup.cnt > 1 THEN 1 ELSE 0 END)
          - COUNT(DISTINCT CASE WHEN dup.cnt > 1 THEN r.OfferRowID END) AS rows_to_delete
      FROM RequestRows r
      JOIN RequestDocs d ON d.DocID = r.DocID
      LEFT JOIN (
        SELECT DocID, OfferRowID, COUNT(*) AS cnt
        FROM RequestRows
        GROUP BY DocID, OfferRowID
        HAVING cnt > 1
      ) dup ON dup.DocID = r.DocID AND dup.OfferRowID = r.OfferRowID
      WHERE d.DocDate >= DATE_SUB(NOW(), INTERVAL 90 DAY)
      GROUP BY r.DocID, d.DocDate, d.PartnerID
      HAVING dup_offers > 0
      ORDER BY r.DocID DESC
    \`);

    if (!affected.length) {
      console.log("Дублей не найдено. Все документы чистые.");
      return;
    }

    let totalDelete = 0;
    let totalDocs = affected.length;
    console.log("\\nДокументы с дублями:");
    for (const doc of affected) {
      const dt = doc.doc_date ? new Date(doc.doc_date).toLocaleDateString("ru-RU") : "?";
      totalDelete += Number(doc.rows_to_delete || 0);
      console.log("  Doc #" + doc.DocID
        + " \\t" + dt
        + " \\tвсего строк: " + doc.total_rows
        + " \\tdup-позиций: " + doc.dup_offers
        + " \\tудалить: " + doc.rows_to_delete);
    }
    console.log("\\nИТОГО: документов=" + totalDocs + "  строк к удалению=" + totalDelete);
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
  console.log("Подключение...");
  const conn = await connect();
  try {
    const remote = `${remoteRoot}/scripts/_one-shot-check-dupes.cjs`;
    await sftpPutString(conn, checkScript, remote);
    await exec(conn, `cd ${remoteRoot} && node ${remote}`);
    await exec(conn, `rm -f ${remote}`);
  } finally {
    conn.end();
  }
}

main().catch((e) => { console.error(e.message || e); process.exit(1); });
