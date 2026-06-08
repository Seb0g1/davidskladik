#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { repairWeakYandexCardsFromOzonPostgres } = require("../server.js");

const lockPath = path.join(__dirname, "..", "data", "repair-yandex-media-from-ozon.lock");

function acquireLock() {
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  if (fs.existsSync(lockPath)) {
    const ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
    if (ageMs < 2 * 60 * 60 * 1000) {
      throw new Error(`repair-yandex-media-from-ozon already running (lock ${Math.round(ageMs / 1000)}s old)`);
    }
    fs.unlinkSync(lockPath);
  }
  fs.writeFileSync(lockPath, `${process.pid}\n${new Date().toISOString()}\n`);
}

function releaseLock() {
  try { fs.unlinkSync(lockPath); } catch { /* ignore */ }
}

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const push = process.argv.includes("--push");
const forcePush = process.argv.includes("--force-push");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const offerArg = process.argv.find((arg) => arg.startsWith("--offer="));
const offerIds = offerArg
  ? offerArg.split("=")[1].split(",").map((value) => value.trim()).filter(Boolean)
  : process.argv.slice(2).filter((arg) => !arg.startsWith("--") && arg);
const limit = limitArg ? Number(limitArg.split("=")[1]) : 0;

async function main() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  acquireLock();
  const prisma = new PrismaClient();
  try {
    const result = await repairWeakYandexCardsFromOzonPostgres(prisma, {
      dryRun: dryRun || !apply,
      pushToYandex: apply && push,
      forcePush: forcePush || offerIds.length > 0,
      offerIds,
      limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : 0,
      batchSize: 50,
    });
    console.log(JSON.stringify(result, null, 2));
    if (apply && result.pushFailed > 0) process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
    releaseLock();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
