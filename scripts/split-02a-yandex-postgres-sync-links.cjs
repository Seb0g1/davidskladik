"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02a-yandex-postgres-sync-links.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

const chunks = [
  { name: "02a-yandex-link-pair-sync-core.js", start: 1, end: 84 },
  { name: "02a-yandex-linked-group-repair.js", start: 85, end: 233 },
  { name: "02a-yandex-materialize-exported.js", start: 234, end: 321 },
  { name: "02a-yandex-migrate-canonical-ids.js", start: 323, end: lines.length },
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
if (!source.includes('"02a-yandex-postgres-sync-links.js"')) {
  throw new Error("02a-yandex-postgres-sync-links.js not found in source.js");
}
source = source.replace(
  '  "02a-yandex-postgres-sync-links.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

console.log("split-02a-yandex-postgres-sync-links complete", { chunks: chunks.length, totalLines: lines.length });
