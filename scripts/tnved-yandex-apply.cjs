'use strict';
// Standalone script: apply Yandex TNVED codes directly (bypasses PM2 API server)
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const path = require('path');
const fs = require('fs').promises;
const https = require('https');
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();
// data/ is at project root (assemble.js compiles with __dirname = projectRoot)
const DATA_DIR = path.join(__dirname, '..', 'data');
const ASSIGNMENTS_PATH = path.join(DATA_DIR, 'ozon-tnved-assignments.json');
const SETTINGS_PATH = path.join(DATA_DIR, 'app-settings.json');
const ACCOUNTS_PATH = path.join(DATA_DIR, 'marketplace-accounts.json');

function cleanText(v) { return v ? String(v).trim() : ''; }
function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function yandexRequest(shop, urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: 'api.partner.market.yandex.ru',
      path: urlPath,
      method: 'POST',
      headers: {
        'Api-Key': shop.apiKey,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
      },
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
  console.log('=== Yandex TNVED Apply ===');
  let assignments = [];
  try {
    const raw = JSON.parse(await fs.readFile(ASSIGNMENTS_PATH, 'utf8'));
    assignments = Array.isArray(raw) ? raw : (Array.isArray(raw.assignments) ? raw.assignments : []);
  } catch { console.log('No assignments file, using fallback only'); }
  let settings = {};
  try { settings = JSON.parse(await fs.readFile(SETTINGS_PATH, 'utf8')); } catch { console.log('No settings file'); }

  const fallback = cleanText(settings?.tnved?.code || '');
  console.log('Default code:', fallback);

  const assignMap = new Map(
    assignments
      .filter(a => cleanText(a.tnvedCode))
      .map(a => [`${Number(a.descCatId)}:${Number(a.typeId || 0)}`, cleanText(a.tnvedCode)])
  );
  console.log('Category assignments:', assignMap.size);

  if (!assignMap.size && !fallback) {
    console.error('ERROR: No assignments and no default code. Exiting.');
    process.exit(1);
  }

  console.log('Loading Yandex products from DB...');
  const yandexRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'yandex', archived: false },
    select: { offerId: true },
    orderBy: { id: 'asc' },
    take: 50000,
  });
  console.log('Yandex products:', yandexRows.length);

  const offerIdSet = [...new Set(yandexRows.map(r => cleanText(r.offerId)).filter(Boolean))];

  console.log('Loading Ozon category links...');
  const ozonRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'ozon', offerId: { in: offerIdSet } },
    select: { offerId: true, raw: true },
  });
  console.log('Ozon rows:', ozonRows.length);

  const ozonCatByOfferId = new Map();
  for (const row of ozonRows) {
    const offerId = cleanText(row.offerId);
    if (!offerId || ozonCatByOfferId.has(offerId)) continue;
    const raw = (row.raw && typeof row.raw === 'object') ? row.raw : {};
    const oz = (raw.ozon && typeof raw.ozon === 'object') ? raw.ozon : {};
    const descCatId = Number(oz.categoryId || oz.descCatId || 0);
    const typeId = Number(oz.typeId || 0);
    if (descCatId) ozonCatByOfferId.set(offerId, { descCatId, typeId });
  }

  const offers = [];
  let withCategory = 0, withFallback = 0, skipped = 0;
  for (const row of yandexRows) {
    const offerId = cleanText(row.offerId);
    if (!offerId) { skipped++; continue; }
    const cat = ozonCatByOfferId.get(offerId);
    let code = null;
    if (cat) {
      code = assignMap.get(`${cat.descCatId}:${cat.typeId}`) || assignMap.get(`${cat.descCatId}:0`) || null;
      if (code) withCategory++;
    }
    if (!code && fallback) { code = fallback; withFallback++; }
    if (!code) { skipped++; continue; }
    offers.push({ offerId, customsTariffCode: code });
  }

  console.log(`Offers: ${offers.length} total | byCategory=${withCategory} | byFallback=${withFallback} | skipped=${skipped}`);

  // Load shops from marketplace-accounts.json (env YANDEX_SHOPS_JSON not used on production)
  let allAccounts = [];
  try {
    const accData = JSON.parse(await fs.readFile(ACCOUNTS_PATH, 'utf8'));
    allAccounts = Array.isArray(accData.accounts) ? accData.accounts : [];
  } catch { console.log('No accounts file'); }
  const seenBusiness = new Set();
  const shops = allAccounts.filter(s => {
    if (s.marketplace !== 'yandex') return false;
    if (!s.apiKey || !s.businessId) return false;
    if (s.hidden) return false;
    const key = String(s.businessId);
    if (seenBusiness.has(key)) return false;
    seenBusiness.add(key);
    return true;
  });
  console.log('Unique Yandex businesses:', shops.length, shops.map(s => `${s.id || '?'}(biz=${s.businessId})`).join(', '));

  const sleep = ms => new Promise(r => setTimeout(r, ms));

  let totalOk = 0, totalFailed = 0;
  for (const shop of shops) {
    console.log(`\n--- Shop businessId=${shop.businessId} ---`);
    const chunks = chunkArray(offers, 100);
    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      let retries = 3;
      while (retries > 0) {
        try {
          await yandexRequest(shop, `/v2/businesses/${shop.businessId}/offer-mappings/update`, {
            offerMappings: chunk.map(offer => ({ offer })),
          });
          totalOk += chunk.length;
          if ((i + 1) % 20 === 0 || i === chunks.length - 1) {
            console.log(`  [${i + 1}/${chunks.length}] ok (total: ${totalOk})`);
          }
          break;
        } catch (err) {
          if (err.message && err.message.includes('420')) {
            retries--;
            console.log(`  [${i + 1}/${chunks.length}] rate limit, waiting 65s... (${retries} retries left)`);
            await sleep(65000);
          } else {
            console.error(`  [${i + 1}/${chunks.length}] FAILED: ${err.message}`);
            totalFailed += chunk.length;
            break;
          }
          if (retries === 0) {
            console.error(`  [${i + 1}/${chunks.length}] GAVE UP after rate limit retries`);
            totalFailed += chunk.length;
          }
        }
      }
      // Throttle: 100 offers per chunk, limit is 10000/min → 700ms gap between chunks
      await sleep(700);
    }
  }

  console.log(`\n=== RESULT: OK=${totalOk} FAILED=${totalFailed} ===`);
  await prisma.$disconnect();
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
