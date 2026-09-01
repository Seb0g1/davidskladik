'use strict';

const { getPartnerSummary } = require('../api');
const { fmt, fmtDate, backKeyboard } = require('./utils');

async function handleBalance(bot, chatId, _partnerId, viewerRole) {
  const msg = await bot.sendMessage(chatId, '⏳ <i>Загружаю данные...</i>', { parse_mode: 'HTML' });
  try {
    const data = await getPartnerSummary('');
    const s = data.summary;
    const ourProfit = Math.round(Number(s.salesRevenue || 0) - Number(s.sponsorProfit || 0));

    let text;
    if (viewerRole === 'admin') {
      text = [
        `💰 <b>Баланс реализации</b>`,
        ``,
        `👤 <b>У партнёра:</b>`,
        `  Баланс (мы ему должны): <b>${fmt(s.balance)} ₽</b>`,
        `  Его накопленная прибыль: <b>${fmt(s.sponsorProfit)} ₽</b>`,
        `  Выплачено ему: ${fmt(s.sponsorPayouts || 0)} ₽`,
        ``,
        `🏪 <b>У нас:</b>`,
        `  Наша прибыль: <b>${fmt(ourProfit)} ₽</b>`,
        `  Общая выручка: ${fmt(s.salesRevenue)} ₽`,
        ``,
        `📦 <b>Склад:</b>`,
        `  Товаров: ${s.items} позиций  |  Остаток: ${s.stockQuantity} шт`,
        `  Сумма по закупке: ${fmt(s.capitalization)} ₽`,
        ``,
        `📈 Продано всего: ${s.soldQuantity} шт`,
        ``,
        `🕐 <i>${fmtDate(data.updatedAt)}</i>`,
      ].join('\n');
    } else {
      text = [
        `💰 <b>Ваш баланс</b>`,
        ``,
        `💵 Ваш баланс: <b>${fmt(s.balance)} ₽</b>`,
        `  (деньги, которые мы вам должны)`,
        ``,
        `📊 <b>Склад:</b>`,
        `  Позиций на реализации: <b>${s.items}</b>`,
        `  Остаток: <b>${s.stockQuantity} шт</b>`,
        `  Сумма по закупке: ${fmt(s.capitalization)} ₽`,
        ``,
        `📈 <b>Итоги продаж:</b>`,
        `  Продано: <b>${s.soldQuantity} шт</b>`,
        `  Выручка: ${fmt(s.salesRevenue)} ₽`,
        `  Ваша прибыль: <b>${fmt(s.sponsorProfit)} ₽</b>`,
        ``,
        `🕐 <i>${fmtDate(data.updatedAt)}</i>`,
      ].join('\n');
    }

    await bot.editMessageText(text, {
      chat_id: chatId, message_id: msg.message_id,
      parse_mode: 'HTML', reply_markup: backKeyboard(),
    });
  } catch (e) {
    await bot.editMessageText(
      `❌ Не удалось загрузить данные.\n<code>${e.message}</code>`,
      { chat_id: chatId, message_id: msg.message_id, parse_mode: 'HTML', reply_markup: backKeyboard() }
    );
  }
}

module.exports = { handleBalance };
