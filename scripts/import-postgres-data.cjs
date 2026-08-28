#!/usr/bin/env node
"use strict";

require("dotenv").config();

const fs = require("node:fs/promises");
const path = require("node:path");
const { PrismaClient } = require("@prisma/client");

const rootDir = path.join(__dirname, "..");

function argEnabled(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

const inputPath = path.resolve(rootDir, argValue("--in", path.join(rootDir, "data", "postgres-export.json")));
const replace = argEnabled("--replace");
const dryRun = argEnabled("--dry-run");

const tableDefs = [
  {
    key: "appUsers",
    model: "appUser",
    dateFields: ["createdAt", "updatedAt"],
  },
  {
    key: "appSettings",
    model: "appSetting",
    dateFields: ["createdAt", "updatedAt"],
  },
  {
    key: "warehouseProducts",
    model: "warehouseProduct",
    dateFields: ["createdAt", "updatedAt"],
  },
  {
    key: "managedSuppliers",
    model: "managedSupplier",
    dateFields: ["createdAt", "updatedAt"],
  },
  {
    key: "productLinks",
    model: "productLink",
    dateFields: ["createdAt", "updatedAt"],
  },
  {
    key: "priceRetryQueueItems",
    model: "priceRetryQueueItem",
    dateFields: ["nextRetryAt", "lastAttemptAt", "createdAt", "updatedAt"],
  },
  {
    key: "priceHistory",
    model: "priceHistory",
    dateFields: ["createdAt"],
  },
  {
    key: "auditLogs",
    model: "auditLog",
    dateFields: ["createdAt"],
  },
  {
    key: "syncRuns",
    model: "syncRun",
    dateFields: ["startedAt", "finishedAt"],
  },
  {
    key: "priceMasterSnapshotItems",
    model: "priceMasterSnapshotItem",
    dateFields: ["docDate", "updatedAt"],
  },
];

const deleteOrder = [
  "productLink",
  "priceRetryQueueItem",
  "priceHistory",
  "auditLog",
  "syncRun",
  "priceMasterSnapshotItem",
  "warehouseProduct",
  "managedSupplier",
  "appSetting",
  "appUser",
];

function toDateOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function normalizeRow(row, dateFields) {
  const normalized = { ...row };
  for (const field of dateFields) {
    if (Object.prototype.hasOwnProperty.call(normalized, field)) {
      normalized[field] = toDateOrNull(normalized[field]);
    }
  }
  return normalized;
}

async function createManyInBatches(model, rows, batchSize = 1000) {
  let count = 0;
  for (let index = 0; index < rows.length; index += batchSize) {
    const batch = rows.slice(index, index + batchSize);
    if (!batch.length) continue;
    const result = await model.createMany({ data: batch, skipDuplicates: true });
    count += result.count || 0;
  }
  return count;
}

async function run() {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set. Import requires PostgreSQL mode.");
  }

  const payload = JSON.parse(await fs.readFile(inputPath, "utf8"));
  if (!payload || payload.version !== 1 || !payload.data) {
    throw new Error("Unsupported export file format.");
  }

  const planned = {};
  for (const table of tableDefs) {
    planned[table.key] = Array.isArray(payload.data[table.key]) ? payload.data[table.key].length : 0;
  }

  if (dryRun) {
    console.log(JSON.stringify({ ok: true, dryRun: true, replace, input: inputPath, planned }, null, 2));
    return;
  }

  const prisma = new PrismaClient();
  try {
    if (replace) {
      for (const model of deleteOrder) {
        await prisma[model].deleteMany({});
      }
    }

    const imported = {};
    for (const table of tableDefs) {
      const rows = (Array.isArray(payload.data[table.key]) ? payload.data[table.key] : [])
        .map((row) => normalizeRow(row, table.dateFields));
      imported[table.key] = await createManyInBatches(prisma[table.model], rows, 1000);
    }

    console.log(JSON.stringify({ ok: true, replace, input: inputPath, imported }, null, 2));
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
