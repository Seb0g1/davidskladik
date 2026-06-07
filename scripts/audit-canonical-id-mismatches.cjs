#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");
const { productFromPostgres, warehouseProductCanonicalId } = require("../server.js");

async function run() {
  const prisma = new PrismaClient();
  try {
    const rows = await prisma.warehouseProduct.findMany({
      include: { links: true },
      orderBy: { updatedAt: "desc" },
    });
    let mismatches = 0;
    let canonicalExists = 0;
    let canonicalMissing = 0;
    const samples = [];
    for (const row of rows) {
      const product = productFromPostgres(row);
      const canonicalId = warehouseProductCanonicalId(product);
      if (!canonicalId || product.id === canonicalId) continue;
      mismatches += 1;
      const conflict = await prisma.warehouseProduct.findUnique({ where: { id: canonicalId }, select: { id: true, offerId: true, marketplace: true } });
      if (conflict) canonicalExists += 1;
      else canonicalMissing += 1;
      if (samples.length < 5) {
        samples.push({
          id: product.id,
          canonicalId,
          offerId: product.offerId,
          marketplace: product.marketplace,
          target: product.target,
          conflict: Boolean(conflict),
        });
      }
    }
    console.log(JSON.stringify({
      ok: true,
      scanned: rows.length,
      mismatches,
      canonicalExists,
      canonicalMissing,
      samples,
    }, null, 2));
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
