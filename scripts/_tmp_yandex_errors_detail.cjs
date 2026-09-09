'use strict';

const ExcelJS = require('exceljs');
const path = require('path');

async function main() {
  const filePath = path.resolve('C:\\Users\\Seb0g1\\Downloads\\файл с товарами_171782339_09-09-2026.xlsx');
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);

  const sheet = workbook.getWorksheet('Список товаров');
  if (!sheet) {
    console.error('Sheet "Список товаров" not found!');
    console.log('Available sheets:', workbook.worksheets.map(s => s.name));
    process.exit(1);
  }

  const getCellText = (row, colIdx) => {
    const cell = row.getCell(colIdx);
    const v = cell.value;
    if (v === null || v === undefined) return '';
    if (typeof v === 'object' && v.richText) {
      return v.richText.map(r => r.text).join('');
    }
    return String(v).trim();
  };

  // Counters
  let totalRows = 0;
  let withCritical = 0;
  let withNonCritical = 0;
  let withAnyError = 0;
  let withNeedMoreInfo = 0;
  let withSotrудником = 0;

  // Lists
  const sizeGridRows = [];          // #3 Размерная сетка / Пол / Размер в сетке / Тип препарата / Детский размер / Тип
  const notMatchCategory = [];      // #4 не соответствует выбранной категории
  const noImage = [];               // #5 Нет изображения
  const noDimensions = [];          // #6 Нет габаритов / Нет веса
  const noPriceRows = [];           // #7 Цена не указана
  const priceDrop = [];             // #8 Цена сильно снизилась

  const SIZE_KEYWORDS = ['Размерная сетка', 'Пол', 'Размер в сетке', 'Тип препарата', 'Детский размер', 'Тип'];

  // Data starts at row 3, header is row 2
  sheet.eachRow((row, rowNumber) => {
    if (rowNumber < 3) return;

    const col1 = getCellText(row, 1);   // Критичные ошибки
    const col2 = getCellText(row, 2);   // Некритичные ошибки
    const col4 = getCellText(row, 4);   // Ваш SKU
    const col5 = getCellText(row, 5);   // Название
    const col6 = getCellText(row, 6);   // Изображения
    const col14 = getCellText(row, 14); // Категория
    const col16 = getCellText(row, 16); // Бренд
    const col34 = getCellText(row, 34); // ТН ВЭД

    // Skip completely empty rows
    if (!col1 && !col2 && !col4 && !col5) return;

    totalRows++;

    if (col1) withCritical++;
    if (col2) withNonCritical++;
    if (col1 || col2) withAnyError++;
    if (col2.includes('Нужна дополнительная информация')) withNeedMoreInfo++;
    if (col1.includes('Скрыт сотрудником')) withSotrудником++;

    // #3 Size grid / Пол / Тип keywords in critical errors
    const matchedSizeKW = SIZE_KEYWORDS.filter(kw => col1.includes(kw));
    if (matchedSizeKW.length > 0) {
      sizeGridRows.push({ offerId: col4, name: col5, category: col14, error: col1, matched: matchedSizeKW });
    }

    // #4 не соответствует выбранной категории (could be in either column)
    if (col1.includes('не соответствует выбранной категории') || col2.includes('не соответствует выбранной категории')) {
      notMatchCategory.push({ offerId: col4, name: col5, category: col14 });
    }

    // #5 Нет изображения
    if (col1.includes('Нет изображения') || col2.includes('Нет изображения')) {
      noImage.push({ offerId: col4, name: col5 });
    }

    // #6 Нет габаритов / Нет веса
    if (col1.includes('Нет габаритов') || col1.includes('Нет веса') ||
        col2.includes('Нет габаритов') || col2.includes('Нет веса')) {
      noDimensions.push({ offerId: col4, name: col5 });
    }

    // #7 Цена не указана
    if (col1.includes('Цена не указана') || col2.includes('Цена не указана')) {
      noPriceRows.push({ offerId: col4, name: col5, brand: col16 });
    }

    // #8 Цена сильно снизилась
    if (col1.includes('Цена сильно снизилась') || col2.includes('Цена сильно снизилась')) {
      priceDrop.push({ offerId: col4 });
    }
  });

  console.log('='.repeat(70));
  console.log('ЯНДЕКС МАРКЕТ — Анализ ошибок товаров');
  console.log('='.repeat(70));
  console.log(`\nВсего товаров (строк с данными): ${totalRows}`);
  console.log(`\n--- СЧЁТЧИКИ ---`);
  console.log(`a. С критичными ошибками (col 1):           ${withCritical}`);
  console.log(`b. С некритичными ошибками (col 2):         ${withNonCritical}`);
  console.log(`c. С ЛЮБОЙ ошибкой (col 1 OR col 2):        ${withAnyError}`);
  console.log(`d. "Нужна дополнительная информация":        ${withNeedMoreInfo}`);
  console.log(`   "Скрыт сотрудником" (критичные):         ${withSotrудником}`);

  console.log(`\n--- #3 РАЗМЕРНАЯ СЕТКА / ПОЛ / ТИП (критичные) — ${sizeGridRows.length} товаров ---`);
  if (sizeGridRows.length === 0) {
    console.log('  (нет)');
  } else {
    sizeGridRows.forEach((r, i) => {
      console.log(`\n  [${i + 1}] offerId: ${r.offerId}`);
      console.log(`       Название:  ${r.name}`);
      console.log(`       Категория: ${r.category}`);
      console.log(`       Ключи:     ${r.matched.join(', ')}`);
      console.log(`       Ошибка:    ${r.error.slice(0, 300)}${r.error.length > 300 ? '...' : ''}`);
    });
  }

  console.log(`\n--- #4 "НЕ СООТВЕТСТВУЕТ ВЫБРАННОЙ КАТЕГОРИИ" — ${notMatchCategory.length} товаров ---`);
  if (notMatchCategory.length === 0) {
    console.log('  (нет)');
  } else {
    notMatchCategory.forEach((r, i) => {
      console.log(`  [${i + 1}] offerId: ${r.offerId} | категория: ${r.category} | ${r.name}`);
    });
  }

  console.log(`\n--- #5 "НЕТ ИЗОБРАЖЕНИЯ" — ${noImage.length} товаров ---`);
  if (noImage.length === 0) {
    console.log('  (нет)');
  } else {
    noImage.slice(0, 50).forEach((r, i) => {
      console.log(`  [${i + 1}] offerId: ${r.offerId} | ${r.name}`);
    });
    if (noImage.length > 50) console.log(`  ... и ещё ${noImage.length - 50}`);
  }

  console.log(`\n--- #6 "НЕТ ГАБАРИТОВ" / "НЕТ ВЕСА" — ${noDimensions.length} товаров ---`);
  if (noDimensions.length === 0) {
    console.log('  (нет)');
  } else {
    noDimensions.slice(0, 50).forEach((r, i) => {
      console.log(`  [${i + 1}] offerId: ${r.offerId} | ${r.name}`);
    });
    if (noDimensions.length > 50) console.log(`  ... и ещё ${noDimensions.length - 50}`);
  }

  console.log(`\n--- #7 "ЦЕНА НЕ УКАЗАНА" — ${noPriceRows.length} товаров ---`);
  if (noPriceRows.length === 0) {
    console.log('  (нет)');
  } else {
    noPriceRows.forEach((r, i) => {
      console.log(`  [${i + 1}] offerId: ${r.offerId} | бренд: ${r.brand} | ${r.name}`);
    });
  }

  console.log(`\n--- #8 "ЦЕНА СИЛЬНО СНИЗИЛАСЬ" — ${priceDrop.length} товаров ---`);
  if (priceDrop.length === 0) {
    console.log('  (нет)');
  } else {
    priceDrop.forEach((r, i) => {
      console.log(`  [${i + 1}] offerId: ${r.offerId}`);
    });
  }

  console.log('\n' + '='.repeat(70));
  console.log('Готово.');
}

main().catch(err => {
  console.error('Ошибка:', err);
  process.exit(1);
});
