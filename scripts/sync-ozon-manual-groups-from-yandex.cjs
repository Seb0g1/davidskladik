#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { syncOzonManualGroupsFromYandexPostgres } = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const prisma = new PrismaClient();
  try {
    console.log(JSON.stringify(await syncOzonManualGroupsFromYandexPostgres(prisma, { dryRun: dryRun || !apply }), null, 2));
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
