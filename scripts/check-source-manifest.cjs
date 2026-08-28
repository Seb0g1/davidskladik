#!/usr/bin/env node
"use strict";

// Guards against the "function exists but isn't wired in" bug class: a part file
// added to server/parts/ but forgotten in server/source.js's partFiles manifest
// (so it's silently never loaded), or a stale manifest entry pointing at a file
// that no longer exists.

const fs = require("fs");
const path = require("path");
const { partFiles, partsDir } = require("../server/source");

const onDisk = new Set(
  fs.readdirSync(partsDir).filter((name) => name.endsWith(".js")),
);
const inManifest = new Set(partFiles);

const missingFromManifest = [...onDisk].filter((name) => !inManifest.has(name)).sort();
const missingFromDisk = partFiles.filter((name) => !onDisk.has(name)).sort();

const seen = new Set();
const duplicates = [];
for (const name of partFiles) {
  if (seen.has(name)) duplicates.push(name);
  seen.add(name);
}

let ok = true;

if (missingFromManifest.length) {
  ok = false;
  console.error("Files in server/parts/ that are not registered in server/source.js partFiles:");
  for (const name of missingFromManifest) console.error(`  ${name}`);
}

if (missingFromDisk.length) {
  ok = false;
  console.error("Entries in server/source.js partFiles with no matching file in server/parts/:");
  for (const name of missingFromDisk) console.error(`  ${name}`);
}

if (duplicates.length) {
  ok = false;
  console.error("Duplicate entries in server/source.js partFiles:");
  for (const name of duplicates) console.error(`  ${name}`);
}

if (!ok) {
  console.error(
    "\nA part file that exists but isn't in the manifest is never loaded into the assembled\n" +
    "server — its functions silently don't exist at runtime. Add/remove the entry in\n" +
    "server/source.js partFiles to match server/parts/.",
  );
  process.exit(1);
}

console.log(`OK: server/source.js partFiles matches server/parts/ (${partFiles.length} files).`);
