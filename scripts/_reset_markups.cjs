"use strict";
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  // Найти товары с markup > 0 в raw JSON
  const affected = await prisma.$queryRawUnsafe(`
    SELECT id, raw->>'markup' AS markup, raw->>'offerId' AS offer_id
    FROM warehouse_products
    WHERE (raw->>'markup')::text NOT IN ('0', '', 'null')
      AND raw->>'markup' IS NOT NULL
      AND (raw->>'markup')::float > 0
    ORDER BY (raw->>'markup')::float DESC
    LIMIT 100
  `);

  console.log("Товары с личным коэффициентом:", affected.length);
  affected.forEach((r) => console.log("  offerId:", r.offer_id, "markup:", r.markup));

  if (!affected.length) {
    console.log("Нечего сбрасывать.");
    await prisma.$disconnect();
    return;
  }

  // Сброс всех markup → 0
  const result = await prisma.$executeRawUnsafe(`
    UPDATE warehouse_products
    SET raw = jsonb_set(raw, '{markup}', '0')
    WHERE (raw->>'markup')::text NOT IN ('0', '', 'null')
      AND raw->>'markup' IS NOT NULL
      AND (raw->>'markup')::float > 0
  `);

  console.log("Сброшено:", result, "товаров");

  // Проверка
  const remaining = await prisma.$queryRawUnsafe(`
    SELECT COUNT(*) AS cnt FROM warehouse_products
    WHERE (raw->>'markup')::float > 0
  `);
  console.log("Осталось с markup > 0:", remaining[0]?.cnt);

  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
