"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02a-helpers.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

/** 1-based inclusive line ranges -> output filenames */
const chunks = [
  { name: "02a-utils-marketplace-images.js", start: 1, end: 372 },
  { name: "02a-marketplace-accounts.js", start: 373, end: 718 },
  { name: "02a-warehouse-brand-search.js", start: 719, end: 1403 },
  { name: "02a-normalizers-links.js", start: 1404, end: 2155 },
  { name: "02a-supplier-pricing.js", start: 2156, end: 2683 },
  { name: "02a-ozon-api.js", start: 2684, end: 3094 },
  { name: "02a-yandex-ozon-catalog.js", start: 3095, end: 4081 },
  { name: "02a-warehouse-canonical.js", start: 4082, end: 4204 },
  { name: "02a-yandex-export-sync.js", start: 4205, end: 5980 },
  { name: "02a-app-settings.js", start: 5981, end: 6274 },
  { name: "02a-price-master-catalog.js", start: 6275, end: 6979 },
  { name: "02a-openai-images.js", start: 6980, end: 8488 },
  { name: "02a-price-master-snapshot.js", start: 8489, end: 9362 },
  { name: "02a-unarchive-automation.js", start: 9363, end: 10528 },
  { name: "02a-background-jobs-guards.js", start: 10529, end: 11886 },
  { name: "02a-price-master-links.js", start: 11887, end: 13522 },
  { name: "02a-warehouse-page.js", start: 13523, end: 14728 },
  { name: "02a-warehouse-automation.js", start: 14729, end: 15375 },
  { name: "02a-snapshot-compare.js", start: 15376, end: lines.length },
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
if (!source.includes('"02a-helpers.js"')) {
  throw new Error("02a-helpers.js not found in source.js");
}
source = source.replace(
  '  "02a-helpers.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

const summary = chunks.map((c) => ({
  file: c.name,
  lines: c.end - c.start + 1,
}));
console.log("split-02a complete", { chunks: summary.length, totalLines: lines.length, summary });
