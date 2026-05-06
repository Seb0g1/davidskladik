const mysql = require("mysql2/promise");

async function main() {
  const database = process.env.PM_DB_NAME;
  const connection = await mysql.createConnection({
    host: process.env.PM_DB_HOST,
    port: Number(process.env.PM_DB_PORT || 3306),
    user: process.env.PM_DB_USER,
    password: process.env.PM_DB_PASSWORD,
    database,
  });

  const [tables] = await connection.query(
    `SELECT TABLE_NAME, TABLE_ROWS
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = ?
     ORDER BY TABLE_NAME`,
    [database],
  );

  console.log(JSON.stringify(tables, null, 2));

  const interestingTables = [
    "Products",
    "OfferRows",
    "OfferDocs",
    "Brands",
    "Packs",
    "ProductTypes",
    "RegistryProductUnits",
  ];

  for (const tableName of interestingTables) {
    const [columns] = await connection.query(
      `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
       FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY ORDINAL_POSITION`,
      [database, tableName],
    );

    console.log(`\n# ${tableName}`);
    console.log(JSON.stringify(columns, null, 2));
  }

  const [productSamples] = await connection.query(
    `SELECT ProductID, ProductName, SalePrice, Stor, ExtID, Vol, ProductTypeID, PackID
     FROM Products
     ORDER BY ProductID
     LIMIT 5`,
  );

  const [offerSamples] = await connection.query(
    `SELECT r.RowID, r.NativeID, r.BarCode, r.NativeName, r.ProductID, r.NativePrice,
            r.Active, r.IsNew, r.Ignored, d.DocDate, d.PartnerID
     FROM OfferRows r
     JOIN OfferDocs d ON d.DocID = r.DocID
     ORDER BY d.DocDate DESC, r.RowID DESC
     LIMIT 5`,
  );

  console.log("\n# Product samples");
  console.log(JSON.stringify(productSamples, null, 2));
  console.log("\n# Latest offer samples");
  console.log(JSON.stringify(offerSamples, null, 2));

  await connection.end();
}

main().catch((error) => {
  console.error(error.code || error.message);
  process.exit(1);
});
