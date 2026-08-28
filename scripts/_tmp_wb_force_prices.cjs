#!/usr/bin/env node
"use strict";
// Отправляет цены WB через exported server functions (без HTTP).
// Запуск: cd /var/www/davidsklad/davidskladik && node scripts/_tmp_wb_force_prices.cjs
const path = require("path");
process.chdir(path.join(__dirname, ".."));

async function main() {
  console.log("Загружаем сервер...");
  const server = require(path.join(__dirname, "..", "server.js"));
  await new Promise((r) => setTimeout(r, 3000));

  const {
    getWbAccountByTarget, readWbImportRules, loadAvitoPricingContext,
    wbCardsList, loadWbLinkedOzonProducts, wbSetPrices,
    wbSupplierPurchaseRub, wbSupplierPriceRub, wbCardSellable,
    cleanText,
  } = server;

  const cleanFn = cleanText || ((s) => String(s || "").trim());

  const account = getWbAccountByTarget("wb");
  if (!account) { console.error("WB кабинет не настроен"); process.exit(1); }
  console.log("WB аккаунт:", account.name || account.id);

  const rules = await readWbImportRules();
  console.log("Правила: maxWbPriceRub =", rules.maxWbPriceRub, "| minSupplierPriceRub =", rules.minSupplierPriceRub);

  const pricing = await loadAvitoPricingContext();
  console.log("Контекст цен загружен");

  console.log("Получаем список карточек WB...");
  const cards = (await wbCardsList(account)).filter((c) => Number(c.nmID) > 0);
  console.log("Карточек:", cards.length);

  console.log("Матчим с базой...");
  const linked = await loadWbLinkedOzonProducts(cards.map((c) => c.vendorCode));
  console.log("Найдено в БД:", linked.size, "из", cards.length);

  const items = [];
  let skippedNotLinked = 0, skippedNoSupplier = 0, skippedAboveMax = 0, skippedBelowMin = 0;

  for (const card of cards) {
    const product = linked.get(cleanFn(card.vendorCode).toLowerCase());
    if (!product) { skippedNotLinked += 1; continue; }
    const purchaseRub = wbSupplierPurchaseRub(product.supplier, pricing);
    if (!(purchaseRub > 0)) { skippedNoSupplier += 1; continue; }
    if (rules.minSupplierPriceRub > 0 && purchaseRub < rules.minSupplierPriceRub) { skippedBelowMin += 1; continue; }
    const priceRub = wbSupplierPriceRub(product.supplier, pricing);
    if (rules.maxWbPriceRub > 0 && priceRub > rules.maxWbPriceRub) { skippedAboveMax += 1; continue; }
    if (priceRub > 0) items.push({ nmID: card.nmID, price: priceRub, discount: 0 });
  }

  console.log("\n=== Статистика ===");
  console.log("Готово к отправке:", items.length);
  console.log("Не найдено в БД:", skippedNotLinked);
  console.log("Нет поставщика:", skippedNoSupplier);
  console.log("Выше лимита:", skippedAboveMax);
  console.log("Ниже минимума:", skippedBelowMin);
  console.log("Примеры цен:", items.slice(0, 3).map((x) => x.nmID + " → " + x.price + " ₽").join(", "));

  if (!items.length) {
    console.log("Нечего отправлять.");
    process.exit(0);
  }

  console.log("\nОтправляем цены на WB...");
  const result = await wbSetPrices(account, items);
  console.log("Результат:", JSON.stringify(result, null, 2));
  console.log("\n✓ Готово. Отправлено:", result.sent, "позиций.");
  process.exit(0);
}

main().catch(e => {
  console.error("Ошибка:", e.message);
  console.error(e.stack);
  process.exit(1);
});
