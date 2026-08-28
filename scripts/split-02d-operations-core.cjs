"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02d-operations-core.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

/** 1-based inclusive line ranges -> output filenames */
const chunks = [
  { name: "02d-operation-jobs-storage.js", start: 1, end: 75 },
  { name: "02d-supplier-cart-state.js", start: 76, end: 329 },
  { name: "02d-supplier-picking-state.js", start: 330, end: 584 },
  { name: "02d-supplier-cart-preview.js", start: 585, end: 1095 },
  { name: "02d-supplier-cart-insert.js", start: 1096, end: 1397 },
  { name: "02d-price-master-rollback.js", start: 1398, end: 1700 },
  { name: "02d-operation-runners-cart.js", start: 1701, end: 1747 },
  { name: "02d-operation-runners-core.js", start: 1748, end: 1936 },
  { name: "02d-operation-runners-archived.js", start: 1937, end: 2152 },
  { name: "02d-operation-runners-ai-links.js", start: 2153, end: 2421 },
  { name: "02d-run-operation-payload.js", start: 2422, end: 2568 },
  { name: "02d-start-operation-job.js", start: 2569, end: lines.length },
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
if (!source.includes('"02d-operations-core.js"')) {
  throw new Error("02d-operations-core.js not found in source.js");
}
source = source.replace(
  '  "02d-operations-core.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

const summary = chunks.map((c) => ({
  file: c.name,
  lines: c.end - c.start + 1,
}));
console.log("split-02d-operations-core complete", { chunks: summary.length, totalLines: lines.length, summary });
