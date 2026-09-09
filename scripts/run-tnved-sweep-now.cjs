'use strict';
// Standalone TNVED sweep: fixes wrong codes and missing codes for all Ozon accounts
// Same logic as 02f-tnved-sweep.js runTnvedSweep()
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const path = require('path');
const fs = require('fs').promises;
const https = require('https');

const DATA_DIR = path.join(__dirname, '..', 'data');
const ACCOUNTS_PATH = path.join(DATA_DIR, 'marketplace-accounts.json');

const TNVED_ATTR_ID = 22232;
const TNVED_SWEEP_MARKING_ATTR_ID = 23536;

const TNVED_BY_CAT_ID = {
  17028988: "3303001000", // Парфюмерия → Духи
  17028992: "3305900009", // Косметика для ухода за волосами
  17028991: "3304990000", // Декоративная косметика
  17028990: "3304990000", // Косметика для ухода
  17028993: "3307490000", // Ароматы для дома
  17028994: "3307200000", // Личная гигиена
  17028712: "3304990000", // Парфюмерия (sub)
  200001240: "3306100000", // Средства для гигиены полости рта
  200001242: "3401300000", // Ватно-бумажная продукция
  17027920: "3402909000", // Моющие и чистящие средства
};
const TNVED_DEFAULT = "3303001000"; // Default: духи (user preference)

function cleanText(v) { return v ? String(v).trim() : ''; }
function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

function ozonRequest(account, urlPath, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: 'api-seller.ozon.ru',
      path: urlPath,
      method: 'POST',
      headers: {
        'Client-Id': account.clientId,
        'Api-Key': account.apiKey,
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

async function fetchDictEntry(account, descCatId, typeId, code) {
  try {
    let lastValueId = 0;
    for (let page = 0; page < 20; page++) {
      const data = await ozonRequest(account, '/v1/description-category/attribute/values', {
        description_category_id: Number(descCatId),
        type_id: Number(typeId || 0),
        attribute_id: TNVED_ATTR_ID,
        language: 'DEFAULT',
        last_value_id: lastValueId,
        limit: 100,
      });
      const entries = (data.result || []);
      for (const e of entries) {
        const text = cleanText(e.value || '');
        const m = text.match(/^(\d{10})/);
        if (m && m[1] === code) {
          return { value: text, dictionary_value_id: Number(e.id) };
        }
      }
      if (entries.length < 100) break;
      lastValueId = entries[entries.length - 1]?.id || 0;
    }
    return null;
  } catch (err) {
    console.error(`  fetchDictEntry error cat=${descCatId} code=${code}: ${err.message}`);
    return null;
  }
}

async function main() {
  console.log('=== Ozon TNVED Sweep (standalone) ===');

  // Load accounts: env-based primary + marketplace-accounts.json extras
  const accounts = [];
  if (process.env.OZON_CLIENT_ID && process.env.OZON_API_KEY) {
    accounts.push({ id: 'ozon', clientId: process.env.OZON_CLIENT_ID, apiKey: process.env.OZON_API_KEY });
  }
  try {
    const accData = JSON.parse(await fs.readFile(ACCOUNTS_PATH, 'utf8'));
    const localOzon = (accData.accounts || []).filter(a => a.marketplace === 'ozon' && a.clientId && a.apiKey && !a.hidden);
    accounts.push(...localOzon);
  } catch {}
  console.log('Ozon accounts:', accounts.length, accounts.map(a => a.id).join(', '));

  let totalUpdated = 0;
  let totalSkipped = 0;

  for (const account of accounts) {
    console.log(`\n--- Account ${account.id} ---`);

    // 1. Get all offer_ids
    const offerIds = [];
    let lastId = '';
    while (true) {
      const data = await ozonRequest(account, '/v3/product/list', {
        filter: { visibility: 'ALL' }, last_id: lastId, limit: 1000,
      });
      const items = (data.result && data.result.items) || [];
      offerIds.push(...items.map(i => cleanText(i.offer_id)).filter(Boolean));
      lastId = cleanText((data.result && data.result.last_id) || '');
      if (!lastId || items.length < 1000) break;
    }
    console.log(`Total active offer_ids: ${offerIds.length}`);
    if (!offerIds.length) continue;

    // 2. Find products without TNVED or with wrong code
    const noTnvedIds = [];
    const wrongCodeMap = new Map();
    const hashtagMap = new Map();

    for (const chunk of chunkArray(offerIds, 100)) {
      try {
        const data = await ozonRequest(account, '/v4/product/info/attributes', {
          filter: { offer_id: chunk, visibility: 'ALL' },
          limit: 100, sort_by: 'id', sort_dir: 'asc',
        });
        for (const item of (data.result || [])) {
          const tnved = (item.attributes || []).find(a => a.id === TNVED_ATTR_ID);
          const hashtag = (item.attributes || []).find(a => a.id === 23171);
          const currentCode = cleanText(tnved?.values?.[0]?.value || '').match(/^(\d{7,10})/)?.[1] || '';
          const currentHashtag = cleanText(hashtag?.values?.[0]?.value || '');
          const offerId = cleanText(item.offer_id);
          if (currentHashtag) hashtagMap.set(offerId, currentHashtag);
          if (!currentCode) noTnvedIds.push(offerId);
          else wrongCodeMap.set(offerId, currentCode);
        }
      } catch (err) {
        console.error(`  attrs chunk error: ${err.message}`);
      }
      await sleep(100);
    }

    console.log(`Without TNVED: ${noTnvedIds.length}, With code (to check): ${wrongCodeMap.size}`);

    // 3. Get category for all products needing update
    const needsCatCheck = [...noTnvedIds, ...wrongCodeMap.keys()];
    const catMap = new Map();

    for (const chunk of chunkArray(needsCatCheck, 100)) {
      try {
        const data = await ozonRequest(account, '/v3/product/info/list', { offer_id: chunk });
        for (const item of (data.items || [])) {
          const offerId = cleanText(item.offer_id || '');
          if (offerId) catMap.set(offerId, {
            descCatId: Number(item.description_category_id || 0),
            typeId: Number(item.type_id || 0),
          });
        }
      } catch (err) {
        console.error(`  category chunk error: ${err.message}`);
      }
      await sleep(100);
    }

    // 4. Build updates
    const updateItems = [];
    const dictCache = new Map();

    for (const offerId of [...noTnvedIds, ...wrongCodeMap.keys()]) {
      const cat = catMap.get(offerId);
      if (!cat) { totalSkipped++; continue; }
      const { descCatId, typeId } = cat;
      const expectedCode = TNVED_BY_CAT_ID[descCatId] || TNVED_DEFAULT;
      const currentCode = wrongCodeMap.get(offerId) || '';
      if (currentCode && currentCode === expectedCode) continue; // already correct

      const cacheKey = `${descCatId}:${typeId}:${expectedCode}`;
      let dictEntry = dictCache.get(cacheKey);
      if (!dictEntry) {
        dictEntry = await fetchDictEntry(account, descCatId, typeId, expectedCode);
        if (dictEntry) dictCache.set(cacheKey, dictEntry);
        await sleep(50);
      }

      if (!dictEntry) { totalSkipped++; console.log(`  no dict entry: offerId=${offerId} cat=${descCatId} code=${expectedCode}`); continue; }

      const attrs = [
        { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
        { id: TNVED_SWEEP_MARKING_ATTR_ID, values: [{ value: 'false' }] },
      ];
      if (hashtagMap.has(offerId)) {
        attrs.push({ id: 23171, values: [{ value: '#косметика #уход' }] });
      }
      updateItems.push({ offer_id: offerId, attributes: attrs });
    }

    console.log(`Updates to send: ${updateItems.length}`);

    // 5. Send updates
    let accountUpdated = 0;
    for (const chunk of chunkArray(updateItems, 100)) {
      try {
        const result = await ozonRequest(account, '/v1/product/attributes/update', { items: chunk });
        const errors = result.errors || [];
        accountUpdated += chunk.length - errors.length;
        if (errors.length) console.error(`  chunk errors:`, JSON.stringify(errors.slice(0, 3)));
      } catch (err) {
        console.error(`  update chunk error: ${err.message}`);
      }
      await sleep(500);
    }

    totalUpdated += accountUpdated;
    console.log(`  Account ${account.id}: updated=${accountUpdated}`);
  }

  console.log(`\n=== RESULT: updated=${totalUpdated} skipped=${totalSkipped} ===`);
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
