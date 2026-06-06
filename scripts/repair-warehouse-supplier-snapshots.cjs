#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

function argEnabled(name) {
  return process.argv.includes(name);
}

const dryRun = argEnabled("--dry-run");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Math.max(1, Number(limitArg.split("=")[1]) || 0) : 0;

function cleanText(value) {
  return String(value ?? "").trim();
}

function stripStaleWarehouseSupplierSnapshot(raw = {}) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  const next = { ...raw };
  for (const key of [
    "selectedSupplier",
    "suppliers",
    "priceFormula",
    "ready",
    "changed",
    "nextPrice",
    "fallbackSuppliers",
    "selectedSupplierReason",
    "priceSource",
    "priceSelectionReason",
  ]) {
    delete next[key];
  }
  return next;
}

function productNeedsSupplierSnapshotRepair(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const links = Array.isArray(row.links) ? row.links : [];
  if (!links.length) return false;
  return Boolean(
    raw.selectedSupplier
    || (Array.isArray(raw.suppliers) && raw.suppliers.length)
    || raw.ready
    || raw.priceFormula
    || raw.nextPrice,
  );
}

async function main() {
  const prisma = new PrismaClient();
  try {
    const rows = await prisma.warehouseProduct.findMany({
      where: { links: { some: {} } },
      include: { links: true },
      ...(limit ? { take: limit } : {}),
      orderBy: { updatedAt: "desc" },
    });
    const candidates = rows.filter(productNeedsSupplierSnapshotRepair);
    console.log(JSON.stringify({
      ok: true,
      dryRun,
      scanned: rows.length,
      candidates: candidates.length,
    }, null, 2));

    let repaired = 0;
    for (const row of candidates) {
      const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
      const cleanedRaw = stripStaleWarehouseSupplierSnapshot(raw);
      if (dryRun) {
        repaired += 1;
        continue;
      }
      await prisma.warehouseProduct.update({
        where: { id: row.id },
        data: {
          raw: cleanedRaw,
          updatedAt: new Date(),
        },
      });
      repaired += 1;
    }

    console.log(JSON.stringify({ ok: true, dryRun, repaired }, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error(error?.message || String(error));
  process.exit(1);
});
