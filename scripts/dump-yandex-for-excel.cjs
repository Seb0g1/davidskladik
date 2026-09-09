'use strict';
// Dumps all Yandex products with their data for Excel template filling
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const path = require('path');
const fs = require('fs').promises;
const https = require('https');
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();
const DATA_DIR = path.join(__dirname, '..', 'data');
const ASSIGNMENTS_PATH = path.join(DATA_DIR, 'ozon-tnved-assignments.json');
const SETTINGS_PATH = path.join(DATA_DIR, 'app-settings.json');
const ACCOUNTS_PATH = path.join(DATA_DIR, 'marketplace-accounts.json');
const OUTPUT_PATH = '/tmp/yandex_products_export.json';

const TNVED_DESCRIPTIONS = {
  '3303001000': 'Духи',
  '3303009000': 'Туалетная вода и прочие',
  '3304100000': 'Средства для макияжа губ',
  '3304200000': 'Средства для макияжа глаз',
  '3304300000': 'Средства для ногтей',
  '3304910000': 'Пудра косметическая',
  '3304990000': 'Прочие косметические средства',
  '3305100000': 'Шампуни',
  '3305200000': 'Средства для завивки или распрямления волос',
  '3305300000': 'Лаки для волос',
  '3305900009': 'Прочие средства для волос',
  '3306100000': 'Зубные пасты',
  '3306900000': 'Прочие средства для гигиены полости рта',
  '3307100000': 'Средства для бритья',
  '3307200000': 'Дезодоранты и антиперспиранты',
  '3307300000': 'Ароматизированные соли для ванн',
  '3307410000': 'Благовония',
  '3307490000': 'Прочие средства для парфюмирования',
  '3307900000': 'Прочие туалетные средства',
  '3401110000': 'Мыло туалетное',
  '3401200000': 'Мыло в других формах',
  '3402909000': 'Моющие и чистящие средства',
  '3406000000': 'Свечи и аналогичные изделия',
};

function cleanText(v) { return v ? String(v).trim() : ''; }
function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

function yandexRequest(apiKey, urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: 'api.partner.market.yandex.ru',
      path: urlPath,
      method: 'POST',
      headers: { 'Api-Key': apiKey, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
    }, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(raw);
          if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${JSON.stringify(parsed).slice(0, 300)}`));
          else resolve(parsed);
        } catch (e) { reject(new Error(`parse: ${raw.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('timeout')); });
    req.write(data);
    req.end();
  });
}

async function main() {
  console.log('=== Yandex Product Dump for Excel ===');

  // Load category assignments and settings
  let assignments = [];
  try {
    const raw = JSON.parse(await fs.readFile(ASSIGNMENTS_PATH, 'utf8'));
    assignments = Array.isArray(raw) ? raw : (Array.isArray(raw.assignments) ? raw.assignments : []);
  } catch { console.log('No assignments file'); }
  let settings = {};
  try { settings = JSON.parse(await fs.readFile(SETTINGS_PATH, 'utf8')); } catch {}
  const fallback = cleanText(settings?.tnved?.code || '3303001000');
  console.log('Default TN VED code:', fallback);

  const assignMap = new Map(
    assignments.filter(a => cleanText(a.tnvedCode))
      .map(a => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)])
  );
  console.log('Category assignments:', assignMap.size);

  // Load account
  let allAccounts = [];
  try {
    const accData = JSON.parse(await fs.readFile(ACCOUNTS_PATH, 'utf8'));
    allAccounts = Array.isArray(accData.accounts) ? accData.accounts : [];
  } catch {}
  const shop = allAccounts.find(s => s.marketplace === 'yandex' && s.apiKey && s.businessId && !s.hidden);
  if (!shop) { console.error('No Yandex shop found!'); process.exit(1); }
  console.log(`Using shop businessId=${shop.businessId}`);

  // Load all Yandex product offerIds from DB
  console.log('Loading Yandex products from DB...');
  const yandexRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'yandex', archived: false },
    select: { offerId: true, name: true, brand: true, images: true, currentPrice: true, raw: true },
    orderBy: { id: 'asc' },
    take: 50000,
  });
  const offerIdSet = [...new Set(yandexRows.map(r => cleanText(r.offerId)).filter(Boolean))];
  console.log('Yandex products:', offerIdSet.length);

  // Build DB map
  const dbMap = new Map();
  for (const row of yandexRows) {
    const oid = cleanText(row.offerId);
    if (oid && !dbMap.has(oid)) dbMap.set(oid, row);
  }

  // Load Ozon category data for TN VED assignment
  console.log('Loading Ozon category links...');
  const ozonRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'ozon', offerId: { in: offerIdSet } },
    select: { offerId: true, raw: true },
  });
  const ozonCatByOfferId = new Map();
  for (const row of ozonRows) {
    const oid = cleanText(row.offerId);
    if (!oid || ozonCatByOfferId.has(oid)) continue;
    const rawData = (row.raw && typeof row.raw === 'object') ? row.raw : {};
    const oz = (rawData.ozon && typeof rawData.ozon === 'object') ? rawData.ozon : {};
    const descCatId = Number(oz.categoryId || oz.descCatId || 0);
    const typeId = Number(oz.typeId || 0);
    if (descCatId) ozonCatByOfferId.set(oid, { descCatId, typeId });
  }
  console.log('Ozon category links found:', ozonCatByOfferId.size);

  // Compute TN VED code for each product
  const tnvedByOfferId = new Map();
  for (const oid of offerIdSet) {
    const cat = ozonCatByOfferId.get(oid);
    let code = null;
    if (cat) {
      code = assignMap.get(`${cat.descCatId}:${cat.typeId}`) || assignMap.get(`${cat.descCatId}:0`) || null;
    }
    if (!code) code = fallback;
    tnvedByOfferId.set(oid, code);
  }

  // Fetch offer-mappings from Yandex API to get complete product data
  console.log('Fetching offer-mappings from Yandex API (in batches of 100)...');
  const yandexOffers = new Map();
  const batches = chunkArray(offerIdSet, 100);
  for (let i = 0; i < batches.length; i++) {
    try {
      const resp = await yandexRequest(shop.apiKey, `/v2/businesses/${shop.businessId}/offer-mappings`, {
        offerIds: batches[i]
      });
      const oms = (resp.result && resp.result.offerMappings) || [];
      for (const om of oms) {
        const oid = cleanText(om.offer && om.offer.offerId);
        if (oid) yandexOffers.set(oid, { offer: om.offer, mapping: om.mapping });
      }
    } catch (e) {
      console.error(`  [batch ${i + 1}/${batches.length}] error: ${e.message}`);
    }
    if ((i + 1) % 20 === 0 || i === batches.length - 1) {
      console.log(`  Fetched ${yandexOffers.size} offers (batch ${i + 1}/${batches.length})`);
    }
    await sleep(200);
  }

  // Build final product list
  console.log('Building export data...');
  const products = [];
  for (const oid of offerIdSet) {
    const db = dbMap.get(oid);
    const ya = yandexOffers.get(oid);
    const tnved = tnvedByOfferId.get(oid) || fallback;

    const offer = ya && ya.offer ? ya.offer : null;
    const dbRaw = (db && db.raw && typeof db.raw === 'object') ? db.raw : {};
    const dbYandex = (dbRaw.yandex && typeof dbRaw.yandex === 'object') ? dbRaw.yandex : {};
    const dbImages = (db && db.images && typeof db.images === 'object') ? db.images : {};

    // Extract data from Yandex API response (primary) or DB (fallback)
    const name = (offer && offer.name) || (db && db.name) || '';
    const pictures = (offer && offer.pictures && offer.pictures.length > 0)
      ? offer.pictures
      : (dbYandex.pictures || (dbImages.imageUrl ? [dbImages.imageUrl] : []));
    const description = (offer && offer.description) || '';
    const vendor = (offer && offer.vendor) || (db && db.brand) || '';
    const barcodes = (offer && offer.barcodes && offer.barcodes.length > 0) ? offer.barcodes : [];
    const category = (offer && offer.category) || '';
    const marketSku = (ya && ya.mapping && ya.mapping.marketSku) ? String(ya.mapping.marketSku) : '';
    const price = (offer && offer.basicPrice && offer.basicPrice.value)
      ? offer.basicPrice.value
      : (db && db.currentPrice) || '';
    const weightDimensions = (offer && offer.weightDimensions) || null;
    const vendorCode = (offer && offer.vendorCode) || '';
    const params = (offer && offer.params) || [];

    products.push({
      offerId: oid,
      name,
      imageUrl: pictures[0] || '',
      pictures,
      description,
      category,
      vendor,
      vendorCode,
      barcodes,
      price: price || '',
      weight: weightDimensions ? weightDimensions.weight : '',
      length: weightDimensions ? weightDimensions.length : '',
      width: weightDimensions ? weightDimensions.width : '',
      height: weightDimensions ? weightDimensions.height : '',
      marketSku,
      tnvedCode: tnved,
      params,
    });
  }

  console.log(`Total products to export: ${products.length}`);
  await fs.writeFile(OUTPUT_PATH, JSON.stringify({ products, exportedAt: new Date().toISOString() }, null, 2), 'utf8');
  console.log(`Exported to: ${OUTPUT_PATH}`);
  console.log('Done!');
  await prisma['$disconnect']();
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
