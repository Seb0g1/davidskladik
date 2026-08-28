#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

require("dotenv").config();

const { deleteYandexSmallVolumeOffers } = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) : 50000;
const lockPath = path.join(__dirname, "..", "data", "delete-yandex-small-volume.lock");

function acquireLock() {
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  if (fs.existsSync(lockPath)) {
    const ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
    if (ageMs < 2 * 60 * 60 * 1000) {
      throw new Error(`delete-yandex-small-volume already running (lock ${Math.round(ageMs / 1000)}s old)`);
    }
    fs.unlinkSync(lockPath);
  }
  fs.writeFileSync(lockPath, `${process.pid}\n${new Date().toISOString()}\n`);
}

function releaseLock() {
  try { fs.unlinkSync(lockPath); } catch { /* ignore */ }
}

async function main() {
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  acquireLock();
  try {
  const result = await deleteYandexSmallVolumeOffers({
    dryRun: dryRun || !apply,
    limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : 50000,
  });
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok && apply) process.exitCode = 1;
  } finally {
    releaseLock();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
