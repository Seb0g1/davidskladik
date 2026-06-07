#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { deleteYandexSmallVolumeOffers } = require("../server.js");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Number(limitArg.split("=")[1]) : 50000;

async function main() {
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const result = await deleteYandexSmallVolumeOffers({
    dryRun: dryRun || !apply,
    limit: Number.isFinite(limit) && limit > 0 ? Math.round(limit) : 50000,
  });
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok && apply) process.exitCode = 1;
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
