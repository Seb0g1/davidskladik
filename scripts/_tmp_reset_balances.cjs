"use strict";
// Сброс балансов сборщиков + долгов поставщиков (supplier ledger)
// Запускать: node scripts/_tmp_reset_balances.cjs
require("dotenv").config();
const { PrismaClient } = require("@prisma/client");

async function main() {
  const prisma = new PrismaClient({ datasources: { db: { url: process.env.DATABASE_URL } } });
  try {
    // 1. Балансы сборщиков
    const [deletedBalances, deletedTotals] = await Promise.all([
      prisma.appSetting.deleteMany({ where: { key: { startsWith: "picker_balance:" } } }),
      prisma.appSetting.deleteMany({ where: { key: { startsWith: "daily_cart_total:" } } }),
    ]);
    console.log(`Балансы сборщиков удалены: ${deletedBalances.count} записей`);
    console.log(`Дневные итоги удалены: ${deletedTotals.count} записей`);

    // 2. Долги поставщиков (supplier ledger)
    const deletedLedger = await prisma.supplierLedgerEntry.deleteMany({});
    console.log(`Журнал поставщиков очищен: ${deletedLedger.count} записей`);

    console.log("\nГотово! Все балансы и долги обнулены.");
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error("Ошибка:", error.message);
  process.exit(1);
});
