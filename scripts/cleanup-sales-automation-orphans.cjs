#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

const dryRun = process.argv.includes("--dry-run");
const apply = process.argv.includes("--apply");

async function run() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required");
  if (!dryRun && !apply) throw new Error("Pass --dry-run or --apply");
  const prisma = new PrismaClient();
  try {
    const orphans = await prisma.$queryRaw`
      SELECT s.id, s.product_id, s.marketplace, s.target, s.offer_id
      FROM sales_automation_sku_states s
      WHERE s.product_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM warehouse_products w WHERE w.id = s.product_id)
      LIMIT 50000
    `;
    console.log(JSON.stringify({ ok: true, dryRun: dryRun || !apply, orphans: orphans.length, sample: orphans.slice(0, 5) }, null, 2));
    if (!apply || !orphans.length) return;
    const ids = orphans.map((row) => row.id);
    for (const chunk of ids.reduce((acc, id, i) => {
      const idx = Math.floor(i / 500);
      if (!acc[idx]) acc[idx] = [];
      acc[idx].push(id);
      return acc;
    }, [])) {
      await prisma.salesAutomationSkuState.updateMany({
        where: { id: { in: chunk } },
        data: { productId: null },
      });
    }
    console.log(JSON.stringify({ ok: true, cleared: ids.length }, null, 2));
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
