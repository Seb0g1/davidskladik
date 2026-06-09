"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02d-prices-engine.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

/** 1-based inclusive line ranges -> output filenames */
const chunks = [
  { name: "02d-prices-stats-automation.js", start: 1, end: 186 },
  { name: "02d-prices-linked-reprice.js", start: 187, end: 269 },
  { name: "02d-prices-authoritative-queue.js", start: 270, end: 371 },
  { name: "02d-prices-link-activation.js", start: 372, end: 657 },
  { name: "02d-prices-timeout-retry.js", start: 658, end: 677 },
  { name: "02d-prices-send-warehouse.js", start: 678, end: 1226 },
  { name: "02d-prices-process-marketplace-job.js", start: 1227, end: 1304 },
  { name: "02d-prices-delta-background.js", start: 1305, end: 1448 },
  { name: "02d-prices-queue-inline.js", start: 1450, end: 1603 },
  { name: "02d-prices-immediate-push.js", start: 1604, end: 1812 },
  { name: "02d-prices-retry-queue.js", start: 1814, end: lines.length },
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
if (!source.includes('"02d-prices-engine.js"')) {
  throw new Error("02d-prices-engine.js not found in source.js");
}
source = source.replace(
  '  "02d-prices-engine.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

const summary = chunks.map((c) => ({
  file: c.name,
  lines: c.end - c.start + 1,
}));
console.log("split-02d-prices-engine complete", { chunks: summary.length, totalLines: lines.length, summary });
