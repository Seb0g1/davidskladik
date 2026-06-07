#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { repairWeakYandexCardsFromOzonPostgres } = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const push = process.argv.includes("--push");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) : 0;

async function main() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const prisma = new PrismaClient();
  try {
    const result = await repairWeakYandexCardsFromOzonPostgres(prisma, {
      dryRun: dryRun || !apply,
      pushToYandex: apply && push,
      limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : 0,
      batchSize: 50,
    });
    console.log(JSON.stringify(result, null, 2));
    if (apply && result.pushFailed > 0) process.exitCode = 1;
  } finally {
    await prisma.$disconnect();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
