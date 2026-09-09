'use strict';
// v2: Sets "ТН ВЭД коды ЕАЭС" param (catalog format) + customsTariffCode for all Yandex offers
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

// Official EAEU TN VED descriptions (matching Yandex's catalog format)
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

function tnvedParamValue(code) {
  const desc = TNVED_DESCRIPTIONS[code] || 'Прочие';
  return `${code} - ${desc}`;
}

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

// Fetch offer-mappings for specific offerIds (in batches of 200)
async function fetchOfferMappingsByIds(apiKey, businessId, offerIds) {
  const all = new Map();
  const batches = chunkArray(offerIds, 100);
  for (let i = 0; i < batches.length; i++) {
    let resp;
    try {
      resp = await yandexRequest(apiKey, `/v2/businesses/${businessId}/offer-mappings`, { offerIds: batches[i] });
    } catch (e) {
      console.error(`  [batch ${i + 1}/${batches.length}] fetch error: ${e.message}`);
      await sleep(3000);
      // Try once more
      try {
        resp = await yandexRequest(apiKey, `/v2/businesses/${businessId}/offer-mappings`, { offerIds: batches[i] });
      } catch (e2) {
        console.error(`  [batch ${i + 1}/${batches.length}] retry failed: ${e2.message}`);
        continue;
      }
    }
    const oms = (resp.result && resp.result.offerMappings) || [];
    for (const om of oms) {
      all.set(cleanText(om.offer.offerId), om.offer);
    }
    if ((i + 1) % 10 === 0 || i === batches.length - 1) {
      console.log(`  Fetched ${all.size} offers (batch ${i + 1}/${batches.length})...`);
    }
    await sleep(200);
  }
  console.log(`  Total fetched: ${all.size} offer-mappings`);
  return all;
}

async function main() {
  console.log('=== Yandex TNVED Apply v2 (params + customsTariffCode) ===');

  // Load category assignments
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
  if (!assignMap.size && !fallback) { console.error('ERROR: No assignments and no default code.'); process.exit(1); }

  // Load Yandex products from DB
  console.log('Loading Yandex products from DB...');
  const yandexRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'yandex', archived: false },
    select: { offerId: true },
    orderBy: { id: 'asc' },
    take: 50000,
  });
  const offerIdSet = [...new Set(yandexRows.map(r => cleanText(r.offerId)).filter(Boolean))];
  console.log('Yandex products in DB:', offerIdSet.length);

  // Load Ozon category data for cross-mapping
  console.log('Loading Ozon category links...');
  const ozonRows = await prisma.warehouseProduct.findMany({
    where: { marketplace: 'ozon', offerId: { in: offerIdSet } },
    select: { offerId: true, raw: true },
  });
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

  // Build code map for each offerId
  const codeByOfferId = new Map();
  let withCategory = 0, withFallback = 0, skipped = 0;
  for (const offerId of offerIdSet) {
    const cat = ozonCatByOfferId.get(offerId);
    let code = null;
    if (cat) {
      code = assignMap.get(`${cat.descCatId}:${cat.typeId}`) || assignMap.get(`${cat.descCatId}:0`) || null;
      if (code) withCategory++;
    }
    if (!code && fallback) { code = fallback; withFallback++; }
    if (!code) { skipped++; continue; }
    codeByOfferId.set(offerId, code);
  }
  console.log(`Code assignments: byCategory=${withCategory} byFallback=${withFallback} skipped=${skipped}`);

  // Load accounts
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
  console.log('Unique Yandex businesses:', shops.length, shops.map(s => `${s.id}(biz=${s.businessId})`).join(', '));

  let totalOk = 0, totalFailed = 0;

  for (const shop of shops) {
    console.log(`\n--- Shop businessId=${shop.businessId} ---`);

    // Step 1: Fetch current offer data by specific offerIds (business pagination is broken)
    console.log('Fetching current offer-mappings from Yandex by offerIds...');
    const allOfferIds = [...codeByOfferId.keys()];
    const currentOffers = await fetchOfferMappingsByIds(shop.apiKey, shop.businessId, allOfferIds);

    // Step 2: Build update list
    const updates = [];
    for (const [offerId, code] of codeByOfferId) {
      const currentOffer = currentOffers.get(offerId);
      if (!currentOffer) continue; // Not in Yandex yet

      // Build new params: keep all existing params except ТН ВЭД entries, add new one
      const cleanedParams = (currentOffer.params || []).filter(p =>
        !p.name || (!p.name.includes('ТН ВЭД') && p.name !== 'Код ТН ВЭД ЕАЭС')
      );
      const newParams = [
        ...cleanedParams,
        { name: 'ТН ВЭД коды ЕАЭС', value: tnvedParamValue(code) }
      ];

      updates.push({
        offerId,
        code,
        updatedOffer: {
          ...currentOffer,
          customsTariffCode: code,
          params: newParams,
        }
      });
    }
    console.log(`Offers to update: ${updates.length}`);

    // Step 3: Send updates in chunks
    const chunks = chunkArray(updates, 100);
    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      let retries = 3;
      while (retries > 0) {
        try {
          await yandexRequest(shop.apiKey, `/v2/businesses/${shop.businessId}/offer-mappings/update`, {
            offerMappings: chunk.map(u => ({ offer: u.updatedOffer })),
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
            console.error(`  [${i + 1}/${chunks.length}] GAVE UP`);
            totalFailed += chunk.length;
          }
        }
      }
      await sleep(700); // throttle: 100 offers/chunk × ~86 chunks ≈ 8600 points, safe under 10000/min
    }
  }

  console.log(`\n=== RESULT: OK=${totalOk} FAILED=${totalFailed} ===`);
  await prisma.$disconnect();
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
