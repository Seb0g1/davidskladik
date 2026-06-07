#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const {
  normalizeYandexWarehouseTargetsPostgres,
  migrateWarehouseProductCanonicalIdsPostgres,
} = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const prisma = new PrismaClient();
  try {
    const result = await normalizeYandexWarehouseTargetsPostgres(prisma, { dryRun: dryRun || !apply });
    console.log(JSON.stringify(result, null, 2));
    if (apply && result.updated > 0) {
      const migrated = await migrateWarehouseProductCanonicalIdsPostgres(prisma, { dryRun: false, limit: 50000 });
      console.log(JSON.stringify({ canonicalMigration: migrated }, null, 2));
    }
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
