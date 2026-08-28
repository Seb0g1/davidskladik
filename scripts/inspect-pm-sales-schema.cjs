"use strict";
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

  console.log("=== All tables ===");
  const [tables] = await conn.query(
    "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
  );
  tables.forEach((t) => console.log(`  ${t.TABLE_NAME}  (rows~${t.TABLE_ROWS})`));

  const salesCandidates = tables
    .map((t) => t.TABLE_NAME)
    .filter((n) => /sale|realiz|check|чек|продаж|invoice|order|doc/i.test(n));

  console.log("\n=== Sales-related tables ===");
  for (const tbl of salesCandidates) {
    const [cols] = await conn.query(
      `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
      [tbl],
    );
    console.log(`\n# ${tbl}`);
    cols.forEach((c) => console.log(`  ${c.COLUMN_NAME}  ${c.DATA_TYPE}`));

    const [sample] = await conn.query(`SELECT * FROM \`${tbl}\` LIMIT 2`);
    if (sample.length) console.log("  Sample:", JSON.stringify(sample[0]));
  }

  await conn.end();
}

main().catch((e) => { console.error("ERROR:", e.message); process.exit(1); });
