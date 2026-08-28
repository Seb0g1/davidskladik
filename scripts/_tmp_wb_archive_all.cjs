#!/usr/bin/env node
"use strict";

// Одноразовый скрипт: перевести ВСЕ карточки WB в архив (черновик).
// Запускать на сервере:
//   cd /var/www/davidsklad/davidskladik
//   node scripts/_tmp_wb_archive_all.cjs

const path = require("path");
process.chdir(path.join(__dirname, ".."));
const server = require(path.join(__dirname, "..", "server.js"));

async function main() {
  await new Promise((r) => setTimeout(r, 2000));

  const accounts = server.getWbAccounts();
  if (!accounts.length) {
    console.error("Нет активных WB аккаунтов");
    process.exit(1);
  }

  for (const account of accounts) {
    console.log(`\n=== WB аккаунт: ${account.name || account.id} ===`);

    console.log("Получаем список активных карточек...");
    const cards = await server.wbCardsList(account);
    console.log(`Найдено: ${cards.length} карточек`);

    if (!cards.length) {
      console.log("Нечего архивировать.");
      continue;
    }

    const nmIDs = cards.map((c) => c.nmID).filter(Boolean);
    console.log(`Архивируем ${nmIDs.length} карточек...`);
    console.log("Пример nmID:", nmIDs.slice(0, 5).join(", "));

    const result = await server.wbArchiveCards(account, nmIDs);
    console.log(`✓ Заархивировано: ${result.archived}`);
  }

  console.log("\n✓ Готово — все карточки WB переведены в черновик (архив)");
  process.exit(0);
}

main().catch((e) => {
  console.error("Ошибка:", e.message);
  process.exit(1);
});
