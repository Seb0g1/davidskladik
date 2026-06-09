"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02a-yandex-catalog-cache-prices.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

const chunks = [
  { name: "02a-yandex-offer-mappings.js", start: 1, end: 99 },
  { name: "02a-yandex-existing-offer-cache.js", start: 100, end: 214 },
  { name: "02a-yandex-price-preview-rules.js", start: 215, end: 338 },
  { name: "02a-ozon-product-payload-build.js", start: 339, end: lines.length },
];

function sliceLines(start, end) {
  return lines.slice(start - 1, end).join("\n");
}

let cursor = 1;
for (const chunk of chunks) {
  if (chunk.start < cursor) {
    throw new Error(`overlap before ${chunk.name}: expected start >= ${cursor}, got ${chunk.start}`);
  }
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
  throw new Error(`incomplete coverage: ended at line ${cursor - 1}, file has ${lines.length}`);
}

fs.unlinkSync(inputPath);

let source = fs.readFileSync(sourcePath, "utf8");
if (!source.includes('"02a-yandex-catalog-cache-prices.js"')) {
  throw new Error("02a-yandex-catalog-cache-prices.js not found in source.js");
}
source = source.replace(
  '  "02a-yandex-catalog-cache-prices.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

console.log("split-02a-yandex-catalog-cache-prices complete", { chunks: chunks.length, totalLines: lines.length });
