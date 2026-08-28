#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { migrateWarehouseProductCanonicalIdsPostgres } = require("../server.js");

function argValue(name, fallback = "") {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : fallback;
}

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const limit = Number(argValue("--limit", "50000")) || 50000;

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) {
    throw new Error("Pass --dry-run to preview or --apply to migrate ids.");
  }
  const prisma = new PrismaClient();
  try {
    const result = await migrateWarehouseProductCanonicalIdsPostgres(prisma, {
      dryRun: dryRun || !apply,
      limit,
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
