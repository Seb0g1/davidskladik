#!/usr/bin/env node
"use strict";

process.env.DISABLE_BACKGROUND_JOBS = "true";

// Suppress server boot noise
const originalStdoutWrite = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, encoding, callback) => {
  const text = Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk);
  if (text.startsWith("◇ injected env") || text.includes("[BullMQ]") || text.includes("bull")) {
    if (typeof callback === "function") callback();
    return true;
  }
  return originalStdoutWrite(chunk, encoding, callback);
};

require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

async function main() {
  const prisma = getPrisma();

  console.log("Подключение к БД...");

  // 1. Ledger entries (dolgs, payments, corrections)
  const ledger = await prisma.supplierLedgerEntry.deleteMany({});
  console.log(`✓ Записи долгов/оплат удалены: ${ledger.count}`);

  // 2. Picking rows (история сборки)
  const picking = await prisma.supplierPickingRow.deleteMany({});
  console.log(`✓ Строки сборки удалены: ${picking.count}`);

  // 3. Cart drafts (+ rows via CASCADE)
  const cart = await prisma.supplierCartDraft.deleteMany({});
  console.log(`✓ Черновики корзин удалены: ${cart.count}`);

  // 4. Finance orders
  const orders = await prisma.financeOrder.deleteMany({});
  console.log(`✓ Финансовые заказы удалены: ${orders.count}`);

  // 5. Finance expenses (закупки)
  const expenses = await prisma.financeExpense.deleteMany({});
  console.log(`✓ Расходы/закупки удалены: ${expenses.count}`);

  console.log("\n✅ Все данные сброшены. Балансы поставщиков = 0, история пуста.");

  await prisma.$disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error("Ошибка:", err.message);
  process.exit(1);
});
