'use strict';
// Fast dump: Yandex products from DB only (no API calls)
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const path = require('path');
const fs = require('fs').promises;
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();
const DATA_DIR = path.join(__dirname, '..', 'data');
const ASSIGNMENTS_PATH = path.join(DATA_DIR, 'ozon-tnved-assignments.json');
const SETTINGS_PATH = path.join(DATA_DIR, 'app-settings.json');
const OUTPUT_PATH = '/tmp/yandex_db_export.json';

function cleanText(v) { return v ? String(v).trim() : ''; }

async function main() {
  console.log('=== Yandex DB-only export ===');

  let assignments = [];
  try {
    const raw = JSON.parse(await fs.readFile(ASSIGNMENTS_PATH, 'utf8'));
    assignments = Array.isArray(raw) ? raw : (raw.assignments || []);
  } catch {}
  let settings = {};
  try { settings = JSON.parse(await fs.readFile(SETTINGS_PATH, 'utf8')); } catch {}
  const fallback = cleanText(settings?.tnved?.code || '3303001000');
  const assignMap = new Map(
    assignments.filter(a => a.tnvedCode)
      .map(a => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)])
  );
  console.log(`Assignments: ${assignMap.size}, fallback: ${fallback}`);

  console.log('Loading Yandex products...');
  const yandexRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'yandex', archived: false },
    select: { offerId: true, name: true, brand: true, images: true, currentPrice: true, raw: true },
    orderBy: { id: 'asc' },
    take: 50000,
  });
  const offerIdSet = [...new Set(yandexRows.map(r => cleanText(r.offerId)).filter(Boolean))];
  console.log('Unique offerIds:', offerIdSet.length);

  const dbMap = new Map();
  for (const row of yandexRows) {
    const oid = cleanText(row.offerId);
    if (oid && !dbMap.has(oid)) dbMap.set(oid, row);
  }

  console.log('Loading Ozon categories...');
  const ozonRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'ozon', offerId: { in: offerIdSet } },
    select: { offerId: true, raw: true },
  });
  const ozonCatMap = new Map();
  for (const row of ozonRows) {
    const oid = cleanText(row.offerId);
    if (!oid || ozonCatMap.has(oid)) continue;
    const rd = (row.raw && typeof row.raw === 'object') ? row.raw : {};
    const oz = (rd.ozon && typeof rd.ozon === 'object') ? rd.ozon : {};
    const descCatId = Number(oz.categoryId || oz.descCatId || 0);
    const typeId = Number(oz.typeId || 0);
    if (descCatId) ozonCatMap.set(oid, { descCatId, typeId });
  }
  console.log('Ozon categories found:', ozonCatMap.size);

  const products = [];
  for (const oid of offerIdSet) {
    const db = dbMap.get(oid);
    const rd = (db && db.raw && typeof db.raw === 'object') ? db.raw : {};
    const yd = (rd.yandex && typeof rd.yandex === 'object') ? rd.yandex : {};
    const imgs = (db && db.images && typeof db.images === 'object') ? db.images : {};

    // Compute TN VED
    const cat = ozonCatMap.get(oid);
    let tnved = null;
    if (cat) tnved = assignMap.get(`${cat.descCatId}:${cat.typeId}`) || assignMap.get(`${cat.descCatId}:0`) || null;
    if (!tnved) tnved = fallback;

    const name = (db && db.name) || '';
    const imageUrl = (yd.pictures && yd.pictures[0]) || imgs.imageUrl || '';
    const pictures = yd.pictures || (imgs.imageUrl ? [imgs.imageUrl] : []);
    const price = (db && db.currentPrice) || '';
    const brand = (db && db.brand) || '';
    const description = rd.description || yd.description || '';

    products.push({ offerId: oid, name, imageUrl, pictures, description, brand, price, tnvedCode: tnved });
  }

  console.log('Total products:', products.length);
  await fs.writeFile(OUTPUT_PATH, JSON.stringify({ products, exportedAt: new Date().toISOString() }, null, 2), 'utf8');
  console.log('Written to:', OUTPUT_PATH);
  await prisma['$disconnect']();
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
