'use strict';

const ExcelJS = require('exceljs');

const FILE_PATH = 'C:\\Users\\Seb0g1\\Downloads\\файл с товарами_171782339_09-09-2026.xlsx';

// Safe cell text extraction — handles merged cells (MergeValue) and nulls
function safeText(cell) {
  if (!cell) return '';
  try {
    const v = cell.value;
    if (v === null || v === undefined) return '';
    // MergeValue or object with result/formula
    if (typeof v === 'object') {
      if (v.result !== undefined) return String(v.result);
      if (v.text !== undefined) return String(v.text);
      if (v.richText) return v.richText.map(r => r.text || '').join('');
      // SharedStringValue, MergeValue etc — try toString carefully
      try { return String(v); } catch { return ''; }
    }
    return String(v);
  } catch {
    return '';
  }
}

function getRowCells(row, maxCols) {
  const cells = [];
  for (let c = 1; c <= maxCols; c++) {
    cells.push(safeText(row.getCell(c)));
  }
  return cells;
}

async function main() {
  const workbook = new ExcelJS.Workbook();
  console.log('Loading file:', FILE_PATH);
  await workbook.xlsx.readFile(FILE_PATH);

  console.log(`\nSheets in workbook (${workbook.worksheets.length}):`);
  workbook.worksheets.forEach((ws, i) => {
    console.log(`  [${i}] "${ws.name}" — ${ws.rowCount} rows, ${ws.columnCount} cols`);
  });

  // Only process the main goods sheet
  const targetSheetNames = ['список товаров', 'товары', 'products', 'catalog'];
  let targetSheet = workbook.worksheets.find(ws =>
    targetSheetNames.some(n => ws.name.toLowerCase().includes(n))
  ) || workbook.worksheets[0];

  // Process all sheets but focus on the one with actual goods
  for (const ws of workbook.worksheets) {
    // Skip tiny/instruction sheets
    if (ws.rowCount < 5) continue;

    console.log(`\n${'='.repeat(70)}`);
    console.log(`SHEET: "${ws.name}" (${ws.rowCount} rows, ${ws.columnCount} cols)`);
    console.log('='.repeat(70));

    const maxCols = ws.columnCount || 60;

    // Find header row — scan first 10 rows
    let headerRowNum = 1;
    let headerValues = [];

    for (let r = 1; r <= Math.min(10, ws.rowCount); r++) {
      const row = ws.getRow(r);
      const cells = getRowCells(row, maxCols);
      const joined = cells.join('|').toLowerCase();
      if (
        joined.includes('ошибк') ||
        joined.includes('статус') ||
        joined.includes('offer_id') ||
        joined.includes('артикул') ||
        joined.includes('название') ||
        joined.includes('наименован') ||
        joined.includes('sku')
      ) {
        headerRowNum = r;
        headerValues = cells;
        break;
      }
    }

    if (headerValues.length === 0) {
      const row = ws.getRow(1);
      headerValues = getRowCells(row, maxCols);
      headerRowNum = 1;
    }

    // Trim trailing empty headers
    while (headerValues.length > 0 && !headerValues[headerValues.length - 1].trim()) {
      headerValues.pop();
    }

    console.log(`\nHeader row: ${headerRowNum}`);
    console.log('Columns:');
    headerValues.forEach((h, i) => {
      if (h.trim()) console.log(`  [${i + 1}] ${h}`);
    });

    const lowerHeaders = headerValues.map(h => h.toLowerCase());

    // Find key column indices (1-based for ExcelJS, but we store as 0-based array index)
    const errorColIdx = lowerHeaders.findIndex(h =>
      h === 'ошибки' || h.includes('ошибк') || h === 'errors' || h === 'error'
    );
    const statusColIdx = lowerHeaders.findIndex(h =>
      h === 'статус' || h === 'status' || h.startsWith('статус')
    );
    const offerColIdx = lowerHeaders.findIndex(h =>
      h === 'offer_id' || h === 'артикул' || h === 'sku' ||
      h.includes('offer_id') || h.includes('артикул продавца')
    );
    const nameColIdx = lowerHeaders.findIndex(h =>
      h === 'название' || h === 'наименование' || h === 'name' ||
      h.includes('назван') || h.includes('наименован')
    );

    console.log(`\nKey column indices (0-based):`);
    console.log(`  offer/sku: ${offerColIdx >= 0 ? offerColIdx : 'NOT FOUND'} (${headerValues[offerColIdx] || 'N/A'})`);
    console.log(`  name:      ${nameColIdx >= 0 ? nameColIdx : 'NOT FOUND'} (${headerValues[nameColIdx] || 'N/A'})`);
    console.log(`  status:    ${statusColIdx >= 0 ? statusColIdx : 'NOT FOUND'} (${headerValues[statusColIdx] || 'N/A'})`);
    console.log(`  errors:    ${errorColIdx >= 0 ? errorColIdx : 'NOT FOUND'} (${headerValues[errorColIdx] || 'N/A'})`);

    const totalDataRows = ws.rowCount - headerRowNum;
    console.log(`\nTotal data rows (approx): ${totalDataRows}`);

    const errorCounts = new Map();
    const rowsWithErrors = [];
    let rowsWithAnyError = 0;
    let nonEmptyRows = 0;

    for (let r = headerRowNum + 1; r <= ws.rowCount; r++) {
      const row = ws.getRow(r);
      const cells = getRowCells(row, Math.max(maxCols, headerValues.length));

      // Skip completely empty rows
      if (cells.every(c => !c.trim())) continue;
      nonEmptyRows++;

      const offerId = offerColIdx >= 0 ? (cells[offerColIdx] || '').trim() : '';
      const name = nameColIdx >= 0 ? (cells[nameColIdx] || '').trim() : '';
      const statusText = statusColIdx >= 0 ? (cells[statusColIdx] || '').trim() : '';
      const errorText = errorColIdx >= 0 ? (cells[errorColIdx] || '').trim() : '';

      let errorsForRow = [];

      if (errorText) {
        const parts = errorText.split(/[\n;]+/).map(s => s.trim()).filter(Boolean);
        errorsForRow.push(...parts);
      }

      // Check status column independently
      if (statusText && statusColIdx !== errorColIdx) {
        const stl = statusText.toLowerCase();
        if (
          stl.includes('ошибк') || stl.includes('error') || stl.includes('отклон') ||
          stl.includes('не прошёл') || stl.includes('не прошел') || stl.includes('failed')
        ) {
          if (!errorText) errorsForRow.push(`[статус] ${statusText}`);
        }
      }

      // Fallback: if no error columns found, scan all cells
      if (errorColIdx < 0 && statusColIdx < 0) {
        cells.forEach((c, ci) => {
          const cl = c.toLowerCase();
          if (c && (cl.includes('ошибк') || cl.includes('error') || cl.includes('отклон'))) {
            errorsForRow.push(`[col ${ci}] ${c}`);
          }
        });
      }

      if (errorsForRow.length > 0) {
        rowsWithAnyError++;
        for (const e of errorsForRow) {
          errorCounts.set(e, (errorCounts.get(e) || 0) + 1);
        }
        if (rowsWithErrors.length < 15) {
          rowsWithErrors.push({ offerId, name, errors: errorsForRow.join(' | ') });
        }
      }
    }

    console.log(`\nNon-empty data rows: ${nonEmptyRows}`);
    console.log(`Rows with errors: ${rowsWithAnyError}`);
    console.log(`Unique error types: ${errorCounts.size}`);

    const sorted = [...errorCounts.entries()].sort((a, b) => b[1] - a[1]);

    console.log(`\nTop ${Math.min(30, sorted.length)} most common errors:`);
    sorted.slice(0, 30).forEach(([err, cnt], i) => {
      console.log(`  ${String(i + 1).padStart(2, ' ')}. [${String(cnt).padStart(4, ' ')}x] ${err}`);
    });

    if (rowsWithErrors.length > 0) {
      console.log(`\nSample rows with errors:`);
      rowsWithErrors.forEach((r, i) => {
        const nameShort = r.name.length > 50 ? r.name.slice(0, 50) + '...' : r.name;
        console.log(`  ${i + 1}. offer="${r.offerId}" | "${nameShort}"`);
        console.log(`     errors: ${r.errors}`);
      });
    }

    // If no error/status column found, show unique values per column to help identify them
    if (errorColIdx < 0 && statusColIdx < 0 && ws.rowCount > 2) {
      console.log(`\n[!] No error/status column detected. Sampling unique values per column:`);
      const colSamples = headerValues.map(() => new Set());
      for (let r = headerRowNum + 1; r <= Math.min(ws.rowCount, headerRowNum + 100); r++) {
        const row = ws.getRow(r);
        const cells = getRowCells(row, headerValues.length);
        cells.forEach((c, i) => { if (c.trim()) colSamples[i].add(c.trim()); });
      }
      headerValues.forEach((h, i) => {
        if (!h.trim()) return;
        const sample = [...colSamples[i]].slice(0, 4);
        if (sample.length > 0) console.log(`  [${i}] ${h}: ${sample.join(' | ')}`);
      });
    }
  }

  console.log('\nDone.');
}

main().catch(err => {
  console.error('Error:', err.message);
  console.error(err.stack);
  process.exit(1);
});
