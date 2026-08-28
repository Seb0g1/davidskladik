#!/usr/bin/env node
"use strict";

require("dotenv").config();

const fs = require("node:fs/promises");
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");

const rootDir = path.join(__dirname, "..");
const defaultOutputPath = path.join(rootDir, "data", `postgres-export-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

const outputPath = path.resolve(rootDir, argValue("--out", defaultOutputPath));

const tables = [
  { key: "appUsers", model: "appUser", orderBy: { username: "asc" } },
  { key: "appSettings", model: "appSetting", orderBy: { key: "asc" } },
  { key: "warehouseProducts", model: "warehouseProduct", orderBy: { updatedAt: "desc" } },
  { key: "productLinks", model: "productLink", orderBy: { updatedAt: "desc" } },
  { key: "managedSuppliers", model: "managedSupplier", orderBy: { name: "asc" } },
  { key: "priceRetryQueueItems", model: "priceRetryQueueItem", orderBy: { createdAt: "desc" } },
  { key: "priceHistory", model: "priceHistory", orderBy: { createdAt: "desc" } },
  { key: "auditLogs", model: "auditLog", orderBy: { createdAt: "desc" } },
  { key: "syncRuns", model: "syncRun", orderBy: { startedAt: "desc" } },
  { key: "priceMasterSnapshotItems", model: "priceMasterSnapshotItem", orderBy: { updatedAt: "desc" } },
];

async function run() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set. Export requires PostgreSQL mode.");
  }

  const prisma = new PrismaClient();
  try {
    const data = {};
    const counts = {};
    for (const table of tables) {
      const rows = await prisma[table.model].findMany({ orderBy: table.orderBy });
      data[table.key] = rows;
      counts[table.key] = rows.length;
    }

    const payload = {
      version: 1,
      exportedAt: new Date().toISOString(),
      source: "postgres",
      counts,
      data,
    };

    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, JSON.stringify(payload, null, 2), "utf8");
    console.log(JSON.stringify({ ok: true, output: outputPath, counts }, null, 2));
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
