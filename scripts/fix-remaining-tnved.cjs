'use strict';
process.chdir(require('path').join(__dirname, '..'));
require('dotenv').config({ path: '.env' });

const fs = require('fs').promises;
const https = require('https');

const TNVED_ATTR_ID = 22232;
const TNVED_SWEEP_MARKING_ATTR_ID = 23536;

const TNVED_BY_CAT_TYPE = {
  '17028988:0': '3303001000',
  '17028992:0': '3305900009',
  '17028991:0': '3304990000',
  '17028990:0': '3304990000',
  '17028993:0': '3307490000',
  '17028994:0': '3307200000',
  '17028712:0': '3304990000',
  '200001240:0': '3306100000',
  '200001242:0': '3401300000',
  '17027920:0': '3402909000',
  '17028983:97769': '0511993900',
  '17028983:97704': '0511993900',
  '17028983:0':     '0511993900',
};
const TNVED_BY_CAT_ID = {
  17028988: '3303001000',
  17028992: '3305900009',
  17028991: '3304990000',
  17028990: '3304990000',
  17028993: '3307490000',
  17028994: '3307200000',
  17028712: '3304990000',
  200001240: '3306100000',
  200001242: '3401300000',
  17027920: '3402909000',
};
const TNVED_DEFAULT = '3303001000';

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
      hostname: 'api-seller.ozon.ru', path: urlPath, method: 'POST',
      headers: {
        'Client-Id': account.clientId, 'Api-Key': account.apiKey,
        'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data),
      },
    }, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(raw);
          if (res.statusCode >= 400) reject(new Error(`HTTP ${res.statusCode}: ${JSON.stringify(parsed).slice(0, 200)}`));
          else resolve(parsed);
        } catch (e) { reject(new Error(`parse: ${raw.slice(0, 100)}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(30000, () => { req.destroy(new Error('timeout')); });
    req.write(data); req.end();
  });
}

async function fetchDictEntry(account, descCatId, typeId, code) {
  try {
    let lastValueId = 0;
    for (let page = 0; page < 20; page++) {
      const data = await ozonRequest(account, '/v1/description-category/attribute/values', {
        description_category_id: Number(descCatId), type_id: Number(typeId || 0),
        attribute_id: TNVED_ATTR_ID, language: 'DEFAULT', last_value_id: lastValueId, limit: 100,
      });
      const entries = data.result || [];
      for (const e of entries) {
        const m = cleanText(e.value || '').match(/^(\d{10})/);
        if (m && m[1] === code) return { value: cleanText(e.value), dictionary_value_id: Number(e.id) };
      }
      if (entries.length < 100) break;
      lastValueId = entries[entries.length - 1]?.id || 0;
    }
    return null;
  } catch (err) {
    console.error(`  fetchDictEntry error cat=${descCatId} type=${typeId} code=${code}: ${err.message}`);
    return null;
  }
}

async function listDictCodes(account, descCatId, typeId) {
  const codes = [];
  let lastValueId = 0;
  for (let page = 0; page < 5; page++) {
    try {
      const data = await ozonRequest(account, '/v1/description-category/attribute/values', {
        description_category_id: Number(descCatId), type_id: Number(typeId || 0),
        attribute_id: TNVED_ATTR_ID, language: 'DEFAULT', last_value_id: lastValueId, limit: 100,
      });
      const entries = data.result || [];
      for (const e of entries) {
        const m = cleanText(e.value || '').match(/^(\d+)/);
        if (m) codes.push(m[1]);
      }
      if (entries.length < 100) break;
      lastValueId = entries[entries.length - 1]?.id || 0;
    } catch { break; }
  }
  return codes;
}

async function main() {
  const accounts = [];
  if (process.env.OZON_CLIENT_ID && process.env.OZON_API_KEY)
    accounts.push({ id: 'ozon', clientId: process.env.OZON_CLIENT_ID, apiKey: process.env.OZON_API_KEY });
  try {
    const accData = JSON.parse(await fs.readFile('data/marketplace-accounts.json', 'utf8'));
    accounts.push(...(accData.accounts || []).filter(a => a.marketplace === 'ozon' && a.clientId && a.apiKey && !a.hidden));
  } catch {}
  console.log('Accounts:', accounts.map(a => a.id).join(', '));

  let totalFixed = 0, totalSkipped = 0;

  for (const account of accounts) {
    console.log(`\n=== ${account.id} ===`);

    const offerIds = [];
    let lastId = '';
    while (true) {
      const data = await ozonRequest(account, '/v3/product/list', { filter: { visibility: 'ALL' }, last_id: lastId, limit: 1000 });
      const items = (data.result && data.result.items) || [];
      offerIds.push(...items.map(i => cleanText(i.offer_id)).filter(Boolean));
      lastId = cleanText((data.result && data.result.last_id) || '');
      if (!lastId || items.length < 1000) break;
    }
    console.log(`Active: ${offerIds.length}`);

    const noTnvedIds = [];
    for (const chunk of chunkArray(offerIds, 100)) {
      try {
        const data = await ozonRequest(account, '/v4/product/info/attributes', {
          filter: { offer_id: chunk, visibility: 'ALL' }, limit: 100, sort_by: 'id', sort_dir: 'asc',
        });
        for (const item of (data.result || [])) {
          const tnved = (item.attributes || []).find(a => a.id === TNVED_ATTR_ID);
          const code = cleanText(tnved?.values?.[0]?.value || '').match(/^(\d{7,10})/)?.[1] || '';
          if (!code) noTnvedIds.push(cleanText(item.offer_id || String(item.id || '')));
        }
      } catch (err) { console.error(`  attrs chunk err: ${err.message}`); }
      await sleep(80);
    }
    console.log(`No TNVED: ${noTnvedIds.length}`);
    if (!noTnvedIds.length) continue;

    const catMap = new Map();
    for (const chunk of chunkArray(noTnvedIds, 100)) {
      try {
        const data = await ozonRequest(account, '/v3/product/info/list', { offer_id: chunk });
        for (const item of (data.items || [])) {
          const offerId = cleanText(item.offer_id || '');
          if (offerId) catMap.set(offerId, { descCatId: Number(item.description_category_id || 0), typeId: Number(item.type_id || 0) });
        }
      } catch (err) { console.error(`  catMap chunk err: ${err.message}`); }
      await sleep(100);
    }

    const catBreak = {};
    for (const [, c] of catMap) {
      const k = `cat=${c.descCatId} type=${c.typeId}`;
      catBreak[k] = (catBreak[k] || 0) + 1;
    }
    console.log('Category breakdown:', catBreak);

    // Show valid codes for each unique category+type
    const seenCats = new Set();
    for (const [, c] of catMap) {
      const k = `${c.descCatId}:${c.typeId}`;
      if (!seenCats.has(k)) {
        seenCats.add(k);
        const codes = await listDictCodes(account, c.descCatId, c.typeId);
        console.log(`  cat=${c.descCatId} type=${c.typeId} valid codes:`, codes.slice(0, 10).join(', ') || '(none)');
        await sleep(200);
      }
    }

    const dictCache = new Map();
    const updateItems = [];
    for (const offerId of noTnvedIds) {
      const cat = catMap.get(offerId);
      if (!cat) { totalSkipped++; continue; }
      const { descCatId, typeId } = cat;
      const typeKey = `${descCatId}:${typeId}`;
      const expectedCode = TNVED_BY_CAT_TYPE[typeKey] || TNVED_BY_CAT_ID[descCatId] || TNVED_DEFAULT;

      const cacheKey = `${descCatId}:${typeId}:${expectedCode}`;
      let dictEntry = dictCache.get(cacheKey);
      if (dictEntry === undefined) {
        dictEntry = await fetchDictEntry(account, descCatId, typeId, expectedCode);
        dictCache.set(cacheKey, dictEntry || null);
        await sleep(80);
      }

      if (!dictEntry) {
        console.log(`  SKIP: ${offerId} cat=${descCatId} type=${typeId} code=${expectedCode} not in dict`);
        totalSkipped++;
        continue;
      }
      updateItems.push({ offer_id: offerId, attributes: [
        { id: TNVED_ATTR_ID, values: [{ value: dictEntry.value, dictionary_value_id: dictEntry.dictionary_value_id }] },
        { id: TNVED_SWEEP_MARKING_ATTR_ID, values: [{ value: 'false' }] },
      ]});
    }

    console.log(`Updates to send: ${updateItems.length}`);
    let accountFixed = 0;
    for (const chunk of chunkArray(updateItems, 100)) {
      try {
        const result = await ozonRequest(account, '/v1/product/attributes/update', { items: chunk });
        const errors = result.errors || [];
        accountFixed += chunk.length - errors.length;
        if (errors.length) console.error(`  errors:`, JSON.stringify(errors.slice(0, 3)));
      } catch (err) { console.error(`  update chunk err: ${err.message}`); }
      await sleep(500);
    }
    totalFixed += accountFixed;
    console.log(`  Account ${account.id}: fixed=${accountFixed}`);
  }

  console.log(`\n=== TOTAL: fixed=${totalFixed} skipped=${totalSkipped} ===`);
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
