"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02d-routes-main.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

/** 1-based inclusive line ranges -> output filenames */
const chunks = [
  { name: "02d-routes-system-warehouse.js", start: 1, end: 222 },
  { name: "02d-unarchive-helpers.js", start: 223, end: 473 },
  { name: "02d-routes-unarchive.js", start: 474, end: 698 },
  { name: "02d-routes-warehouse-catalog.js", start: 699, end: 938 },
  { name: "02d-routes-suppliers.js", start: 939, end: 1507 },
  { name: "02d-routes-warehouse-products.js", start: 1508, end: 2181 },
  { name: "02d-routes-warehouse-ai.js", start: 2182, end: 3591 },
  { name: "02d-routes-warehouse-links.js", start: 3592, end: 4329 },
  { name: "02d-prices-engine.js", start: 4330, end: 6279 },
  { name: "02d-routes-prices-finance.js", start: 6280, end: 7522 },
  { name: "02d-ozon-yandex-import.js", start: 7523, end: 7709 },
  { name: "02d-operations-core.js", start: 7710, end: lines.length },
];

function sliceLines(start, end) {
  return lines.slice(start - 1, end).join("\n");
}

let cursor = 1;
for (const chunk of chunks) {
  if (chunk.start !== cursor) {
    throw new Error(`gap or overlap before ${chunk.name}: expected start ${cursor}, got ${chunk.start}`);
  }
  if (chunk.end < chunk.start) {
    throw new Error(`invalid range for ${chunk.name}`);
  }
  const content = sliceLines(chunk.start, chunk.end);
  if (!content.trim()) {
    throw new Error(`empty chunk ${chunk.name}`);
  }
  fs.writeFileSync(path.join(partsDir, chunk.name), `${content}\n`);
  cursor = chunk.end + 1;
}

if (cursor - 1 !== lines.length) {
  throw new Error(`incomplete coverage: ended at line ${cursor - 1}, file has ${lines.length}`);
}

fs.unlinkSync(inputPath);

let source = fs.readFileSync(sourcePath, "utf8");
if (!source.includes('"02d-routes-main.js"')) {
  throw new Error("02d-routes-main.js not found in source.js");
}
source = source.replace(
  '  "02d-routes-main.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

const summary = chunks.map((c) => ({
  file: c.name,
  lines: c.end - c.start + 1,
}));
console.log("split-02d complete", { chunks: summary.length, totalLines: lines.length, summary });
