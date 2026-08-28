"use strict";

const fs = require("fs");
const path = require("path");

const root = path.join(__dirname, "..");
const partsDir = path.join(root, "server", "parts");
const sourcePath = path.join(root, "server", "source.js");

const inputPath = path.join(partsDir, "02f-background-jobs.js");
const lines = fs.readFileSync(inputPath, "utf8").split(/\r?\n/);

/** 1-based inclusive line ranges -> output filenames */
const chunks = [
  { name: "02f-marketplace-sync-core.js", start: 1, end: 343 },
  { name: "02f-unarchive-queue-core.js", start: 344, end: 885 },
  { name: "02f-marketplace-automation-pickers.js", start: 886, end: 1095 },
  { name: "02f-marketplace-automation-runners.js", start: 1096, end: 1451 },
  { name: "02f-daily-maintenance-schedulers.js", start: 1452, end: 1786 },
  { name: "02f-manual-warehouse-sync.js", start: 1787, end: 1952 },
  { name: "02f-background-schedulers.js", start: 1954, end: 2108 },
  { name: "02f-sync-routes-middleware.js", start: 2110, end: lines.length },
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
if (!source.includes('"02f-background-jobs.js"')) {
  throw new Error("02f-background-jobs.js not found in source.js");
}
source = source.replace(
  '  "02f-background-jobs.js",\n',
  `${chunks.map((c) => `  "${c.name}",`).join("\n")}\n`,
);
fs.writeFileSync(sourcePath, source);

const summary = chunks.map((c) => ({
  file: c.name,
  lines: c.end - c.start + 1,
}));
console.log("split-02f-background-jobs complete", { chunks: summary.length, totalLines: lines.length, summary });
