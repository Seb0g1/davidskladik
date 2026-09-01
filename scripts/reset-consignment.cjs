/**
 * Сброс всех данных раздела "Реализация".
 * Запускать на сервере: node scripts/reset-consignment.cjs
 * Требует DATABASE_URL в окружении.
 */
'use strict';

const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

async function main() {
  console.log('Начинаем сброс данных Реализации...');

  const [items, ops, invoiceItems, invoices] = await prisma.$transaction([
    prisma.$executeRaw`TRUNCATE TABLE consignment_invoice_items CASCADE`,
    prisma.$executeRaw`TRUNCATE TABLE consignment_invoices CASCADE`,
    prisma.$executeRaw`TRUNCATE TABLE consignment_operations CASCADE`,
    prisma.$executeRaw`TRUNCATE TABLE consignment_items CASCADE`,
  ]);

  console.log('Готово. Все данные Реализации удалены.');
  console.log('  consignment_items           — очищено');
  console.log('  consignment_operations      — очищено');
  console.log('  consignment_invoices        — очищено');
  console.log('  consignment_invoice_items   — очищено');
}

main()
  .catch((e) => { console.error('Ошибка:', e); process.exit(1); })
  .finally(() => prisma.$disconnect());
