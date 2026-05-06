/*
 * Removes warehouse products whose `target` is not in the allow-list.
 * Allow-list = ozon-targets ∪ current YANDEX_SHOPS_JSON ids.
 * Writes atomically via .tmp + rename. Run only with the server stopped.
 */
const fs = require('fs');
const path = require('path');

const dataPath = path.join(__dirname, '..', 'data', 'personal-warehouse.json');
const envPath = path.join(__dirname, '..', '.env');

function parseEnvShops() {
  if (!fs.existsSync(envPath)) return [];
  const raw = fs.readFileSync(envPath, 'utf8');
  const m = raw.match(/^YANDEX_SHOPS_JSON=(.+)$/m);
  if (!m) return [];
  try {
    const arr = JSON.parse(m[1]);
    return Array.isArray(arr) ? arr.map(s => String(s.id)) : [];
  } catch {
    return [];
  }
}

const yandexAllow = new Set(parseEnvShops());
console.log('Allowed yandex targets:', [...yandexAllow]);

const json = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
const before = json.products.length;
const counts = {};
for (const p of json.products) {
  const k = `${p.marketplace}/${p.target || '?'}`;
  counts[k] = (counts[k] || 0) + 1;
}
console.log('Before:', counts, 'total', before);

json.products = json.products.filter(p => {
  if (p.marketplace === 'ozon') return true;
  if (p.marketplace === 'yandex') return yandexAllow.has(String(p.target));
  return true;
});
json.updatedAt = new Date().toISOString();

const after = json.products.length;
console.log('After:', after, 'removed:', before - after);

const tmp = dataPath + '.tmp.prune';
fs.writeFileSync(tmp, JSON.stringify(json));
fs.renameSync(tmp, dataPath);
console.log('Saved.');
