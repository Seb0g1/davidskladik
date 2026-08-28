#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const searchName = process.argv[2] || "Clive Christian Matsukita";
const likeParam = JSON.stringify("%" + searchName + "%");

const remoteScript = `
"use strict";
const mysql = require("mysql2/promise");
async function main() {
  const conn = await mysql.createConnection({
    host: process.env.PM_DB_HOST, port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER, password: process.env.PM_DB_PASSWORD,
    database: process.env.PM_DB_NAME, connectTimeout: 10000,
  });

  // 1. Колонки Products
  const [prodCols] = await conn.query(
    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='Products' ORDER BY ORDINAL_POSITION"
  );
  const pcNames = prodCols.map(function(c){ return c.COLUMN_NAME; });
  console.log("=== Products columns: " + pcNames.join(", "));

  // 2. Образец Products
  const [prodSamp] = await conn.query("SELECT * FROM Products LIMIT 3");
  prodSamp.forEach(function(r){ console.log("  sample: " + JSON.stringify(r).slice(0,500)); });

  // 3. Найти товар в Products по всем текстовым колонкам
  const textCols = pcNames.filter(function(c){ return /name|title|descr|article|caption/i.test(c); });
  console.log("\\nText columns to search: " + textCols.join(", "));
  const likePart = ${likeParam};
  let allProducts = [];
  for (const col of textCols) {
    try {
      const [rows] = await conn.query("SELECT * FROM Products WHERE " + col + " LIKE ? LIMIT 20", [likePart]);
      if (rows.length) {
        console.log("\\n  Found " + rows.length + " in Products by [" + col + "]:");
        rows.forEach(function(r){ console.log("    " + JSON.stringify(r).slice(0,500)); });
        allProducts = allProducts.concat(rows);
      }
    } catch(e) { console.log("  error on " + col + ": " + e.message); }
  }

  // 4. Если нашли — ищем продажи в SaleRows
  const productIds = [...new Set(allProducts.map(function(r){
    return r.ProductID || r.ID || r.Id || r.product_id;
  }).filter(Boolean))];
  console.log("\\nProductIDs from Products: " + JSON.stringify(productIds));

  if (productIds.length) {
    const ph = productIds.map(function(){ return "?"; }).join(",");
    const [saleRows] = await conn.query(
      "SELECT sr.*, sd.DocDate, sd.ContrAgentID, sd.ExRate, sd.Comment as DocComment" +
      " FROM SaleRows sr JOIN SaleDocs sd ON sr.DocID = sd.DocID" +
      " WHERE sr.ProductID IN (" + ph + ")" +
      " ORDER BY sd.DocDate DESC LIMIT 50",
      productIds
    );
    if (saleRows.length) {
      console.log("\\n=== SaleRows (with SaleDocs) for product (" + saleRows.length + " rows) ===");
      saleRows.forEach(function(r){ console.log("  " + JSON.stringify(r).slice(0,600)); });
    } else {
      console.log("\\n  No SaleRows found for these ProductIDs");
    }
  }

  // 5. Дополнительно — ищем в Products через OfferRows.NativeID (OfferDocs → Products link)
  console.log("\\n=== Checking OfferDocs / OfferRows link to Products ===");
  const [offerDocsCols] = await conn.query(
    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='OfferDocs' ORDER BY ORDINAL_POSITION"
  );
  console.log("OfferDocs columns: " + offerDocsCols.map(function(c){ return c.COLUMN_NAME; }).join(", "));
  const [odSamp] = await conn.query("SELECT * FROM OfferDocs LIMIT 2");
  odSamp.forEach(function(r){ console.log("  sample: " + JSON.stringify(r).slice(0,400)); });

  // 6. Все продажи Matsukita через Products — полный текстовый поиск в SaleRows через Products
  // Ищем все ProductID из Products с LIKE в любом текстовом поле + сразу SaleRows
  console.log("\\n=== Searching ALL Products + SaleRows for Matsukita ===");
  const allMatsCond = textCols.map(function(){ return "p." + textCols[0] + " LIKE ?"; });
  // Строим JOIN Products + SaleRows с LIKE
  if (textCols.length) {
    const conds = textCols.map(function(c){ return "p." + c + " LIKE ?"; }).join(" OR ");
    const params = textCols.map(function(){ return likePart; });
    try {
      const [joined] = await conn.query(
        "SELECT sr.*, sd.DocDate, sd.ContrAgentID, p." + textCols[0] + " as ProductName" +
        " FROM SaleRows sr" +
        " JOIN Products p ON sr.ProductID = p." + pcNames[0] +
        " JOIN SaleDocs sd ON sr.DocID = sd.DocID" +
        " WHERE " + conds +
        " ORDER BY sd.DocDate DESC LIMIT 50",
        params
      );
      if (joined.length) {
        console.log("Found " + joined.length + " sales via JOIN:");
        joined.forEach(function(r){ console.log("  " + JSON.stringify(r).slice(0,600)); });
      } else {
        console.log("  No results from JOIN Products+SaleRows");
      }
    } catch(e) { console.log("  JOIN error: " + e.message); }
  }

  await conn.end();
}
main().catch(function(e){ console.error("ERROR:", e.message); process.exit(1); });
`;

const tmpFile = path.join(os.tmpdir(), "pm_query_" + Date.now() + ".cjs");
fs.writeFileSync(tmpFile, remoteScript, "utf8");

const remoteRoot = "/var/www/davidsklad/davidskladik";
const remoteTmp = remoteRoot + "/_pm_query_tmp.cjs";

const conn = new Client();
conn.on("ready", () => {
  conn.sftp((err, sftp) => {
    if (err) { console.error("SFTP error:", err.message); conn.end(); return; }
    sftp.fastPut(tmpFile, remoteTmp, (err2) => {
      fs.unlinkSync(tmpFile);
      if (err2) { console.error("Upload error:", err2.message); conn.end(); return; }
      conn.exec(
        "cd " + remoteRoot + " && node --env-file .env _pm_query_tmp.cjs",
        (err3, stream) => {
          if (err3) { console.error(err3); conn.end(); return; }
          stream.on("data", d => process.stdout.write(d));
          stream.stderr.on("data", d => process.stderr.write(d));
          stream.on("close", () => {
            conn.exec("rm -f " + remoteTmp, () => conn.end());
          });
        }
      );
    });
  });
}).connect({ host: "81.17.154.153", username: "root", password, readyTimeout: 20000 });
