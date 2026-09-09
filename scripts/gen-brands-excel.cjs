'use strict';
const dotenv = require('dotenv');
dotenv.config();
const ExcelJS = require('exceljs');
const XLSX = require('xlsx');

const BASE = 'https://davidsklad.ru';
const YANDEX_XLSX = process.argv[2] || 'C:/Users/Seb0g1/Downloads/файл с товарами_171782339_08-09-2026 (3).xlsx';
const REF_XLSX = 'C:/Users/Seb0g1/Downloads/MagicVibes_TNVED_BRAND.xlsx';

const GARBAGE_EXACT = new Set([
  'без бренда','нет бренда','no brand','no name','noname','нет','none',
  'не указан','не указано','б н','б/н','б/у','бренд','brand','unknown',
  'неизвестно','другое','прочее',
]);
const GARBAGE_PREFIXES = ['без ','для ','нет ','не ','из ','на '];
const GARBAGE_WORDS = new Set([
  'лосьон','бальзам','шампунь','кондиционер','сыворотка','крем','гель',
  'масло','тоник','спрей','скраб','пилинг','маска','мыло','пена',
  'дезодорант','тушь','помада','тени','пудра','средство','сливки',
  'набор','подарочный','комплект','набора','мусс','флюид',
  // итальянские слова-продукты
  'spazzola','specchio','pennello','lozione','crema','siero','shampoo',
  'brush','mirror','comb','tool','щетка','щётка',
]);

function normName(s) {
  return s.toLowerCase().replace(/[^а-яёa-z0-9 ]/gi, ' ').replace(/\s+/g, ' ').trim();
}

function isGarbage(s) {
  if (!s || s.length < 2) return true;
  const lower = s.toLowerCase().trim();
  if (GARBAGE_EXACT.has(lower)) return true;
  for (const p of GARBAGE_PREFIXES) if (lower.startsWith(p)) return true;
  const words = normName(s).split(' ').filter(Boolean);
  if (words.some(w => GARBAGE_WORDS.has(w))) return true;
  return false;
}

function stripCodePrefix(s) {
  return (s || '').replace(/^\d{7,10}\s*[-–]\s*/, '').trim();
}

// ── Чтение Яндекс-файла и нормализация брендов ──────────────────────────────
async function readYandexBrands() {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(YANDEX_XLSX);
  const sheet = wb.getWorksheet('Список товаров');
  if (!sheet) throw new Error('Лист "Список товаров" не найден в ' + YANDEX_XLSX);

  // Сначала собираем все бренды с подсчётом и кодами ТН ВЭД
  const rawBrands = new Map(); // rawBrand → { count, tnveds: Map<code,count> }
  sheet.eachRow({ includeEmpty: false }, (row, rowNum) => {
    if (rowNum < 4) return;
    const brand = String(row.getCell(9).value || '').trim();
    const tnved = String(row.getCell(34).value || '').trim();
    if (!brand || isGarbage(brand)) return;
    if (!rawBrands.has(brand)) rawBrands.set(brand, { count: 0, tnveds: new Map() });
    const d = rawBrands.get(brand);
    d.count++;
    if (tnved) d.tnveds.set(tnved, (d.tnveds.get(tnved) || 0) + 1);
  });

  // Нормализация по normName (case-insensitive dedup)
  const normGroups = new Map(); // norm → entries[]
  for (const [brand, data] of rawBrands) {
    const key = normName(brand);
    if (!normGroups.has(key)) normGroups.set(key, []);
    normGroups.get(key).push({ brand, ...data });
  }

  // Prefix-collapse: если norm A начинается с norm B + пробел, и B существует как отдельный бренд → A ≡ B
  const normsSortedByLen = [...normGroups.keys()].sort((a, b) => a.length - b.length);
  const normToCanon = new Map(); // norm → canonical norm
  for (const norm of normsSortedByLen) normToCanon.set(norm, norm);

  for (const norm of normsSortedByLen) {
    for (const shorter of normsSortedByLen) {
      if (shorter.length >= norm.length) break;
      if (norm.startsWith(shorter + ' ') && normGroups.has(shorter)) {
        normToCanon.set(norm, normToCanon.get(shorter) || shorter);
        break;
      }
    }
  }

  // Собираем canonical бренды
  const canonicals = new Map(); // canonNorm → { displayBrand, count, tnveds: Map }
  for (const [norm, canonNorm] of normToCanon) {
    const entries = normGroups.get(norm) || [];
    if (!canonicals.has(canonNorm)) {
      const canonEntries = (normGroups.get(canonNorm) || []).sort((a, b) => b.count - a.count);
      const best = canonEntries[0];
      canonicals.set(canonNorm, { displayBrand: best?.brand || canonNorm, count: 0, tnveds: new Map() });
    }
    const target = canonicals.get(canonNorm);
    for (const e of entries) {
      target.count += e.count;
      for (const [t, c] of e.tnveds) target.tnveds.set(t, (target.tnveds.get(t) || 0) + c);
    }
  }

  return canonicals; // Map: canonNorm → { displayBrand, count, tnveds }
}

async function run() {
  // ── 1. Login ────────────────────────────────────────────────────────────────
  const loginRes = await fetch(BASE + '/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: BASE },
    body: JSON.stringify({ username: process.env.APP_USER, password: process.env.APP_PASSWORD }),
  });
  if (!loginRes.ok) throw new Error('Login failed: ' + loginRes.status);
  const cookie = loginRes.headers.get('set-cookie');
  const h = { Cookie: cookie, Origin: BASE };

  // ── 2. Ozon данные: brandTnveds с topTypes ──────────────────────────────────
  console.log('Загружаю Ozon бренды...');
  const ozonData = await fetch(BASE + '/api/catalog/brands-tnved', { headers: h }).then(r => r.json());
  const ozonEntries = ozonData.brandTnveds || [];
  console.log('  Ozon бренды:', ozonEntries.length, ozonData.fromCache ? '(кэш)' : '(свежие)');

  // ── 3. Яндекс-файл ─────────────────────────────────────────────────────────
  console.log('Читаю Яндекс-файл:', YANDEX_XLSX);
  const yandexCanonicals = await readYandexBrands();
  console.log('  Яндекс canonical брендов:', yandexCanonicals.size);

  // ── 4. Референсный файл: код → описание ──────────────────────────────────
  const refCodeDesc = new Map();  // code → description string
  const refBrandCodes = new Map(); // normName → [{code, description}]
  try {
    const refWb = XLSX.readFile(REF_XLSX);
    const refWs = refWb.Sheets[refWb.SheetNames[0]];
    const refRows = XLSX.utils.sheet_to_json(refWs, { header: 1 });
    let lastBrand = '';
    for (const row of refRows.slice(1)) {
      const brand = String(row[0] || '').trim();
      const code = String(row[1] || '').trim();
      const desc = String(row[2] || '').trim();
      if (brand) lastBrand = brand;
      if (code) {
        if (desc) refCodeDesc.set(code, desc);
        const key = normName(lastBrand);
        if (!refBrandCodes.has(key)) refBrandCodes.set(key, []);
        if (!refBrandCodes.get(key).find(c => c.code === code))
          refBrandCodes.get(key).push({ code, description: desc });
      }
    }
    console.log('  Референс файл:', refBrandCodes.size, 'брендов,', refCodeDesc.size, 'кодов');
  } catch (e) {
    console.warn('  Референс файл:', e.message);
  }

  // ── 5. Нормализованный индекс Ozon брендов ─────────────────────────────────
  const ozonByNorm = new Map(); // normName → entry
  for (const e of ozonEntries) ozonByNorm.set(normName(e.brand), e);

  // ── 6. Объединяем бренды ───────────────────────────────────────────────────
  // brandMap: brand → { tnvedCodes: [{code,description}], topTypes: [], platform }
  const brandMap = new Map();

  // Ozon бренды
  for (const entry of ozonEntries) {
    if (!entry.brand || isGarbage(entry.brand)) continue;
    const key = normName(entry.brand);
    const inYandex = yandexCanonicals.has(key);

    let codes = (entry.tnvedCodes || []).map(tc => ({
      code: tc.code,
      description: stripCodePrefix(tc.fullValue) || refCodeDesc.get(tc.code) || '',
    }));
    if (!codes.length && refBrandCodes.has(key)) codes = refBrandCodes.get(key);

    brandMap.set(entry.brand, {
      tnvedCodes: codes,
      topTypes: entry.topTypes || [],
      tnvedCodeTypes: entry.tnvedCodeTypes || [],
      platform: inYandex ? 'Ozon + Яндекс' : 'Ozon',
    });
  }

  // Нормы Ozon-брендов (для prefix-match), сортируем по убыванию длины
  const ozonNormsSorted = [...ozonByNorm.keys()].sort((a, b) => b.length - a.length);
  // Нормы референсного файла
  const refNormsSorted = [...refBrandCodes.keys()].sort((a, b) => b.length - a.length);

  const MIN_YANDEX_ONLY_SKU = 5; // минимальный порог SKU для Яндекс-only бренда

  // Яндекс-only бренды (не в Ozon)
  for (const [canonNorm, yData] of yandexCanonicals) {
    if (ozonByNorm.has(canonNorm)) continue; // точное совпадение с Ozon → уже добавлен выше

    // Если это prefix-вариант Ozon-бренда → пропускаем (это product line, не новый бренд)
    const hasOzonParent = ozonNormsSorted.some(oNorm => canonNorm.startsWith(oNorm + ' '));
    if (hasOzonParent) continue;

    // Если мало SKU — скорее всего мусор или разовая запись
    if (yData.count < MIN_YANDEX_ONLY_SKU) continue;

    // Дополнительные проверки: не должно начинаться с кирилл. строчной (фрагмент)
    if (/^[а-яё]/i.test(yData.displayBrand) && /^[а-яё]/.test(yData.displayBrand[0])) continue;

    // TNVED: из Яндекс-файла, фолбэк референс
    let codes = [...yData.tnveds.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([code]) => ({ code, description: refCodeDesc.get(code) || '' }));

    if (!codes.length && refBrandCodes.has(canonNorm))
      codes = refBrandCodes.get(canonNorm);

    brandMap.set(yData.displayBrand, {
      tnvedCodes: codes,
      topTypes: [],
      platform: 'Яндекс',
    });
  }

  // ── 7. Генерируем строки ────────────────────────────────────────────────────
  const allBrands = [...brandMap.entries()].sort((a, b) => a[0].localeCompare(b[0], 'ru'));
  const rows = [];

  // Описания субкодов 3304 для читаемости
  const COSMETICS_CODE_DESC = {
    '3304100000': 'Губные помады и прочие средства для губ',
    '3304200000': 'Средства для макияжа глаз',
    '3304300000': 'Лаки для ногтей',
    '3304910000': 'Пудры (компактные и рассыпчатые)',
    '3304990000': 'Прочие средства по уходу за кожей лица',
  };

  for (const [brand, data] of allBrands) {
    const { platform, tnvedCodes, topTypes, tnvedCodeTypes } = data;

    // Проверяем: есть ли косметические коды 3304 с разбивкой по типам
    const cosmeticsRows = (tnvedCodeTypes || []).filter(e => e.code.startsWith('3304') && e.types.length);

    if (cosmeticsRows.length > 0) {
      // Для косметики — одна строка на субкод 3304
      // Сначала добавляем парфюмерные коды если есть (3303*)
      const perfumeCode = tnvedCodes.find(c => c.code.startsWith('3303'));
      let firstRow = true;

      if (perfumeCode) {
        const perfTypes = (tnvedCodeTypes || []).find(e => e.code === perfumeCode.code);
        rows.push({
          brand,
          code: perfumeCode.code,
          description: perfumeCode.description || refCodeDesc.get(perfumeCode.code) || '',
          types: (perfTypes?.types || topTypes || []).filter(t => !cosmeticsRows.some(c => c.types.includes(t))).join(', ') || (topTypes || []).join(', '),
          platform: firstRow ? platform : '',
          noCode: false,
        });
        firstRow = false;
      }

      // Строки по каждому 3304-субкоду
      const sortedCosm = cosmeticsRows.sort((a, b) => a.code.localeCompare(b.code));
      for (const { code, types } of sortedCosm) {
        const codeEntry = tnvedCodes.find(c => c.code === code);
        const description = codeEntry?.description || COSMETICS_CODE_DESC[code] || refCodeDesc.get(code) || '';
        rows.push({
          brand: firstRow ? brand : '',
          code,
          description,
          types: types.join(', '),
          platform: firstRow ? platform : '',
          noCode: false,
        });
        firstRow = false;
      }

      // Остальные коды (не 3303, не 3304) — отдельными строками
      for (const tc of tnvedCodes.filter(c => !c.code.startsWith('3303') && !c.code.startsWith('3304'))) {
        const extraTypes = (tnvedCodeTypes || []).find(e => e.code === tc.code);
        rows.push({
          brand: '',
          code: tc.code,
          description: tc.description || refCodeDesc.get(tc.code) || '',
          types: (extraTypes?.types || []).join(', '),
          platform: '',
          noCode: false,
        });
      }
    } else {
      // Парфюмерия и прочие — стандартная логика (одна строка или по кодам)
      const typesStr = (topTypes || []).join(', ');
      const codes = tnvedCodes;

      if (!codes.length) {
        rows.push({ brand, code: '', description: '', types: typesStr, platform, noCode: true });
      } else {
        for (let i = 0; i < codes.length; i++) {
          rows.push({
            brand: i === 0 ? brand : '',
            code: codes[i].code,
            description: codes[i].description,
            types: i === 0 ? typesStr : '',
            platform: i === 0 ? platform : '',
            noCode: false,
          });
        }
      }
    }
  }

  // ── 8. Пишем Excel ──────────────────────────────────────────────────────────
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Magic Vibes Склад';
  wb.created = new Date();

  const s = wb.addWorksheet('Бренды ТН ВЭД', { views: [{ state: 'frozen', ySplit: 1 }] });
  s.columns = [
    { header: 'Бренд',              key: 'brand',       width: 36 },
    { header: 'Код ТН ВЭД',         key: 'code',        width: 16 },
    { header: 'Описание категории',  key: 'description', width: 50 },
    { header: 'Тип товара',          key: 'types',       width: 55 },
    { header: 'Площадка',            key: 'platform',    width: 18 },
  ];

  const hr = s.getRow(1);
  hr.font = { bold: true };
  hr.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFD9E1F2' } };
  hr.height = 18;

  for (const row of rows) {
    const r = s.addRow(row);
    const isYandexOnly = row.platform === 'Яндекс';
    const isBoth = row.platform === 'Ozon + Яндекс';
    r.eachCell({ includeEmpty: true }, (cell, col) => {
      if (isYandexOnly)
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } };
      else if (isBoth)
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE8F5E9' } };
      if (col === 2) {
        cell.font = { name: 'Courier New', size: 10, color: row.noCode ? { argb: 'FFAAAAAA' } : undefined };
      }
      cell.border = { bottom: { style: 'thin', color: { argb: 'FFE0E0E0' } } };
    });
  }

  s.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: 5 } };

  const dateStr = new Date().toISOString().slice(0, 16).replace('T', '_').replace(':', '-');
  const outPath = process.argv[3] || `C:/Users/Seb0g1/Downloads/tnved-brands-${dateStr}.xlsx`;
  await wb.xlsx.writeFile(outPath);

  // ── 9. Статистика ───────────────────────────────────────────────────────────
  const uniq = (f) => new Set(rows.filter(r => r.brand && f(r)).map(r => r.brand)).size;
  console.log('\n─── Итог ───');
  console.log('Строк всего:           ', rows.length);
  console.log('Брендов всего:         ', brandMap.size);
  console.log('  Только Ozon:         ', uniq(r => r.platform === 'Ozon'));
  console.log('  Ozon + Яндекс:       ', uniq(r => r.platform === 'Ozon + Яндекс'));
  console.log('  Только Яндекс:       ', uniq(r => r.platform === 'Яндекс'));
  console.log('  Без кода ТН ВЭД:     ', uniq(r => !r.code));
  console.log('  С типом товара:      ', uniq(r => !!r.types));
  console.log('\nСохранено:', outPath);
}

run().catch(e => { console.error(e.message || e); process.exit(1); });
