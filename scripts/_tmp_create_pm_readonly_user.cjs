"use strict";
const mysql = require("mysql2/promise");

const CFG = {
  host: "81.17.154.153",
  port: 65123,
  user: "pm_user",
  password: "K4NYWB,J}YZOrD<",
  database: "PriceMasterDB",
  connectTimeout: 15000,
};

const NEW_USER = "PriceMaster_Other";
const NEW_PASS = "9UyMLHXu.1tJ:Qz";
const NEW_HOST = "81.17.154.153";
const DB = "PriceMasterDB";

async function run() {
  const conn = await mysql.createConnection(CFG);
  console.log("Connected OK");

  const steps = [
    [`DROP USER IF EXISTS '${NEW_USER}'@'${NEW_HOST}'`, "drop old user (if any)"],
    [`CREATE USER '${NEW_USER}'@'${NEW_HOST}' IDENTIFIED BY '${NEW_PASS}'`, "create user"],
    [`GRANT SELECT (RowID,DocID,NativeID,NativeName,NativePrice,Active,Ignored) ON \`${DB}\`.\`OfferRows\` TO '${NEW_USER}'@'${NEW_HOST}'`, "grant OfferRows"],
    [`GRANT SELECT (DocID,DocDate,PartnerID) ON \`${DB}\`.\`OfferDocs\` TO '${NEW_USER}'@'${NEW_HOST}'`, "grant OfferDocs"],
    [`GRANT SELECT (PartnerID,PartnerName) ON \`${DB}\`.\`Partners\` TO '${NEW_USER}'@'${NEW_HOST}'`, "grant Partners"],
    [`FLUSH PRIVILEGES`, "flush"],
  ];

  for (const [sql, label] of steps) {
    try {
      await conn.query(sql);
      console.log("✓", label);
    } catch (e) {
      console.error("✗", label, "→", e.message);
      await conn.end();
      process.exit(1);
    }
  }

  // VIEW
  try {
    await conn.query(`
      CREATE OR REPLACE VIEW \`${DB}\`.\`v_pm_other_catalog\` AS
      SELECT
        p.PartnerName AS supplier_name,
        r.NativeName  AS product_name,
        r.NativeID    AS article_number,
        r.NativePrice AS price,
        d.DocDate     AS last_updated
      FROM \`${DB}\`.\`OfferRows\` r
      JOIN \`${DB}\`.\`OfferDocs\` d ON d.DocID    = r.DocID
      JOIN \`${DB}\`.\`Partners\`  p ON p.PartnerID = d.PartnerID
      WHERE r.Ignored = 0 AND r.Active = 1 AND r.NativePrice > 0
    `);
    console.log("✓ view created");
    await conn.query(`GRANT SELECT ON \`${DB}\`.\`v_pm_other_catalog\` TO '${NEW_USER}'@'${NEW_HOST}'`);
    await conn.query("FLUSH PRIVILEGES");
    console.log("✓ view grant OK");
  } catch (e) {
    console.error("✗ view →", e.message, "(not critical — column grants still work)");
  }

  // Verify
  const [rows] = await conn.query(`SHOW GRANTS FOR '${NEW_USER}'@'${NEW_HOST}'`);
  console.log("\n=== Granted permissions ===");
  rows.forEach(r => console.log(Object.values(r)[0]));
  await conn.end();
}

run().catch(e => { console.error("Fatal:", e.message); process.exit(1); });
