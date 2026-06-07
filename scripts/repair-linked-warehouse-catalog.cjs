#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { repairLinkedWarehouseCatalogPostgres } = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) : 0;

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const prisma = new PrismaClient();
  try {
    const result = await repairLinkedWarehouseCatalogPostgres(prisma, {
      dryRun: dryRun || !apply,
      batchSize: 500,
      limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : 0,
      onProgress: (progress) => {
        if (progress?.summary) console.log(progress.summary);
      },
    });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
