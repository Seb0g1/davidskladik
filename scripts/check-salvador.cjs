'use strict';
process.chdir(require('path').join(__dirname, '..'));
require('dotenv').config({ path: '.env' });
const { PrismaClient } = require('@prisma/client');
const mysql = require('mysql2/promise');

const p = new PrismaClient();

async function main() {
  // Find Salvador Dali products in warehouse
  const prods = await p.warehouseProduct.findMany({
    where: {
      archived: false,
      OR: [
        { name: { contains: 'Salvador', mode: 'insensitive' } },
        { offerId: { contains: 'salvador', mode: 'insensitive' } },
        { name: { contains: 'Dali', mode: 'insensitive' } },
      ],
    },
    include: { links: true },
  });

  console.log('Found:', prods.length, 'products matching Salvador/Dali');

  for (const prod of prods) {
    const sorinLinks = prod.links.filter((l) => /сорин|sorin/i.test(l.supplierName || ''));
    const otherLinks = prod.links.filter((l) => !/сорин|sorin/i.test(l.supplierName || ''));

    console.log('\n─────────────────────');
    console.log('offerId:', prod.offerId, '| marketplace:', prod.marketplace, '| target:', prod.target);
    console.log('name:', (prod.name || '').substring(0, 80));
    console.log('stock:', prod.stock, '| price:', prod.price);
    console.log('Sorin links:', sorinLinks.length);

    for (const l of sorinLinks) {
      console.log('  [SORIN] article:', l.supplierArticle, '| priceUsd:', l.priceUsd, '| supplier:', l.supplierName);
    }
    if (otherLinks.length) {
      console.log('Other links:', otherLinks.map((l) => (l.supplierName || '?') + ':' + l.supplierArticle).join(', '));
    }
  }

  if (!prods.length) {
    console.log('Not found in PostgreSQL. Trying broader search...');
    const raw = await p.$queryRawUnsafe(
      `SELECT p.id, p.offer_id, p.name, p.marketplace, l.supplier_name, l.supplier_article, l.price_usd
       FROM warehouse_products p
       JOIN product_links l ON l.product_id = p.id
       WHERE p.archived = false
         AND (p.name ILIKE '%salvador%' OR p.name ILIKE '%dali%' OR p.offer_id ILIKE '%salvador%')
       LIMIT 20`
    );
    console.log('Raw query results:', JSON.stringify(raw, null, 2));
  }

  // Also check in PM MySQL if available
  try {
    const pmConn = await mysql.createConnection({
      host: process.env.PM_DB_HOST,
      port: Number(process.env.PM_DB_PORT || 3306),
      user: process.env.PM_DB_USER,
      password: process.env.PM_DB_PASSWORD,
      database: process.env.PM_DB_NAME,
    });
    const [rows] = await pmConn.query(
      `SELECT TRIM(r.NativeID) AS article, TRIM(r.Name) AS name,
              p.PartnerName AS supplier, d.DocDate,
              COALESCE(r.Active, 1) AS active_row
       FROM OfferRows r
       JOIN OfferDocs d ON d.DocID = r.DocID
       JOIN Partners p ON p.PartnerID = d.PartnerID
       WHERE (r.Name LIKE '%Salvador%' OR r.Name LIKE '%Dali%' OR r.NativeID LIKE '%salvador%')
         AND r.Ignored = 0
       ORDER BY d.DocDate DESC
       LIMIT 20`
    );
    console.log('\n── PM MySQL rows ──');
    for (const row of rows) {
      console.log(`  ${row.article} | ${(row.name||'').substring(0,60)} | supplier=${row.supplier} | active=${row.active_row}`);
    }
    await pmConn.end();
  } catch (err) {
    console.log('PM MySQL not available:', err.message);
  }

  await p.$disconnect();
}

main().catch((e) => { console.error('FATAL:', e.message); process.exit(1); });
