"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02a-yandex-export-sync.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

const chunks = [
  { name: "02a-yandex-product-resolution.js", start: 1, end: 310 },
  { name: "02a-yandex-warehouse-pairing.js", start: 311, end: 579 },
  { name: "02a-yandex-postgres-sync-links.js", start: 580, end: 977 },
  { name: "02a-yandex-postgres-backfill.js", start: 978, end: 1347 },
  { name: "02a-yandex-catalog-cache-prices.js", start: 1348, end: lines.length },
];

function sliceLines(start, end) {
  return lines.slice(start - 1, end).join("\n");
}

let cursor = 1;
for (const chunk of chunks) {
  if (chunk.start < cursor) throw new Error(`overlap before ${chunk.name}`);
  if (chunk.start > cursor) {
    const gap = lines.slice(cursor - 1, chunk.start - 1);
    if (gap.some((line) => String(line).trim())) {
      throw new Error(`non-empty gap before ${chunk.name}: lines ${cursor}-${chunk.start - 1}`);
    }
  }
  const content = sliceLines(chunk.start, chunk.end);
  if (!content.trim()) throw new Error(`empty chunk ${chunk.name}`);
  fs.writeFileSync(path.join(partsDir, chunk.name), `${content}\n`);
  cursor = chunk.end + 1;
}

if (cursor - 1 !== lines.length) {
  throw new Error(`incomplete coverage: ended at ${cursor - 1}, file has ${lines.length}`);
}

fs.unlinkSync(inputPath);

let source = fs.readFileSync(sourcePath, "utf8");
if (!source.includes('"02a-yandex-export-sync.js"')) throw new Error("02a-yandex-export-sync.js not found in source.js");
source = source.replace(
  '  "02a-yandex-export-sync.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

console.log("split-02a-yandex-export-sync complete", { chunks: chunks.length, totalLines: lines.length });
