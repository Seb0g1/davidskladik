'use strict';
const dotenv = require('dotenv');
dotenv.config();

const OZON_CLIENT_ID = process.env.OZON_CLIENT_ID;
const OZON_API_KEY = process.env.OZON_API_KEY;

async function ozon(path, body) {
  const r = await fetch('https://api-seller.ozon.ru' + path, {
    method: 'POST',
    headers: {
      'Client-Id': OZON_CLIENT_ID,
      'Api-Key': OZON_API_KEY,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  const text = await r.text();
  try { return JSON.parse(text); } catch { throw new Error('Bad JSON: ' + text.slice(0, 200)); }
}

async function run() {
  // Проверяем дерево категорий — конкретно ищем 17028988
  console.log('\n--- /v1/description-category/tree ---');
  const treeData = await ozon('/v1/description-category/tree', { language: 'DEFAULT' });
  const roots = treeData.result || [];
  console.log('Root categories count:', roots.length);

  // Ищем категорию 17028988 в дереве
  const TARGET_CAT_ID = 17028988;
  const TARGET_TYPE_ID = 93403;
  let found = null;

  function walk(nodes, depth = 0) {
    for (const n of nodes) {
      const id = n.description_category_id;
      if (Number(id) === TARGET_CAT_ID) {
        found = n;
        console.log('\nFound target category:', JSON.stringify(n).slice(0, 500));
      }
      // Print first root to see structure
      if (depth === 0) {
        const keys = Object.keys(n);
        console.log(`Root: ${n.category_name || n.name} [${id}] keys: ${keys.join(', ')}`);
        if (n.types) console.log('  types:', JSON.stringify(n.types).slice(0, 200));
        if (n.children) console.log('  children count:', n.children.length);
      }
      if (Array.isArray(n.children)) walk(n.children, depth + 1);
    }
  }
  walk(roots);

  if (!found) {
    console.log(`\nCategory ${TARGET_CAT_ID} NOT FOUND in tree`);
    // Попробуем найти категорию напрямую
    console.log('\n--- /v1/description-category/attribute (чтение типов) ---');
    const attrData = await ozon('/v1/description-category/attribute', {
      description_category_id: TARGET_CAT_ID,
      type_id: 0,
      language: 'DEFAULT',
    });
    console.log('Result length:', (attrData.result || []).length);
  }
}

run().catch(e => { console.error(e.message || e); process.exit(1); });
