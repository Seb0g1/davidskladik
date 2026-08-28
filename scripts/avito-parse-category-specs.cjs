#!/usr/bin/env node
"use strict";
// Этап 0 плана PLAN-AVITO-FEED.md: парсит сохранённые страницы шаблонов
// Автозагрузки Avito (avito/*.html) и печатает справочник категорий.
// Имена файлов сдвинуты относительно содержимого — категория берётся из <h1>.

const fs = require("node:fs");
const path = require("node:path");

const avitoDir = path.join(__dirname, "..", "avito");

function decodeEntities(text) {
  return text
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#x27;|&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function toLines(html) {
  return decodeEntities(
    html
      .replace(/<style[\s\S]*?<\/style>/gi, "\n")
      .replace(/<script[\s\S]*?<\/script>/gi, "\n")
      .replace(/<[^>]+>/g, "\n"),
  )
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

// Разбивает документ на блоки полей по id="field-Xxx".
function extractFieldBlocks(html) {
  const blocks = [];
  const re = /id="field-([A-Za-z]+)"/g;
  const positions = [];
  let m;
  while ((m = re.exec(html)) !== null) positions.push({ name: m[1], index: m.index });
  for (let i = 0; i < positions.length; i += 1) {
    const start = positions[i].index;
    const end = i + 1 < positions.length ? positions[i + 1].index : Math.min(html.length, start + 200000);
    blocks.push({ name: positions[i].name, html: html.slice(start, end) });
  }
  return blocks;
}

function parseFieldBlock(block) {
  const cells = toLines(block.html);
  const required = cells.includes("Обязательно");
  // «Одно из значений» — дальше идёт список допустимых значений до следующего
  // служебного маркера (Пример/Формат/Обязательно и т.п.).
  const values = [];
  const startIdx = cells.findIndex((c) => /^Одно из значений/i.test(c));
  if (startIdx >= 0) {
    for (const cell of cells.slice(startIdx + 1)) {
      if (/^(Пример|Формат|Обязательно|Только для|Параметр|Как |Можно|Подробнее|Скопировать|№|\d+ из)/i.test(cell)) break;
      if (cell.length > 80) break;
      values.push(cell);
    }
  }
  return { name: block.name, required, values };
}

function parsePage(filePath) {
  const html = fs.readFileSync(filePath, "utf8");
  const h1s = [...html.matchAll(/<h1[^>]*>([^<]+)<\/h1>/g)].map((m) => decodeEntities(m[1]).trim());
  const category = h1s.find((t) => t && t !== "Автозагрузка") || null;
  const fields = extractFieldBlocks(html).map(parseFieldBlock);
  return { file: path.basename(filePath), category, fields };
}

const interesting = new Set(["Category", "GoodsType", "GoodsSubType", "SubType", "PerfumeryType", "Condition", "AdType", "Volume", "Gender", "Brand", "FragranceGroup", "PerfumeryNotes", "Material", "Color", "UnitQuantity"]);

const pages = [];
for (const file of fs.readdirSync(avitoDir)) {
  if (!file.endsWith(".html")) continue;
  const page = parsePage(path.join(avitoDir, file));
  if (!page.fields.length) continue;
  pages.push(page);
}

for (const page of pages) {
  console.log(`\n=== ${page.file} -> «${page.category}»`);
  for (const field of page.fields) {
    if (!interesting.has(field.name)) continue;
    const req = field.required ? "ОБЯЗ" : "опц ";
    console.log(`  [${req}] ${field.name}${field.values.length ? " = " + JSON.stringify(field.values) : ""}`);
  }
}
