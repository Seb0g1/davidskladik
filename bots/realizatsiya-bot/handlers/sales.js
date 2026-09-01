'use strict';

const { getPartnerSummary } = require('../api');
const { fmt, fmtDate, backKeyboard } = require('./utils');

async function handleSales(bot, chatId, _partnerId, viewerRole) {
  const msg = await bot.sendMessage(chatId, '⏳ <i>Загружаю продажи...</i>', { parse_mode: 'HTML' });
  try {
    const data = await getPartnerSummary('');
    const s = data.summary;
    const ourProfit = Math.round(Number(s.salesRevenue || 0) - Number(s.sponsorProfit || 0));

    let text;
    if (viewerRole === 'admin') {
      text = [
        `📈 <b>Продажи реализации</b>`,
        ``,
        `📅 <b>Этот месяц:</b>`,
        `  Продано: <b>${data.salesThisMonth || 0} шт</b>`,
        `  Выручка: <b>${fmt(data.revenueThisMonth)} ₽</b>`,
        ``,
        `📅 <b>Последние 30 дней:</b>`,
        `  Продано: <b>${data.salesLast30 || 0} шт</b>`,
        `  Выручка: <b>${fmt(data.revenueLast30)} ₽</b>`,
        ``,
        `📊 <b>За всё время:</b>`,
        `  Продано: ${s.soldQuantity} шт  |  Возвратов: ${s.returnedQuantity || 0} шт`,
        `  Общая выручка: <b>${fmt(s.salesRevenue)} ₽</b>`,
        ``,
        `👥 <b>Распределение:</b>`,
        `  У партнёра: <b>${fmt(s.sponsorProfit)} ₽</b>`,
        `  У нас: <b>${fmt(ourProfit)} ₽</b>`,
        ``,
        `🕐 <i>${fmtDate(data.updatedAt)}</i>`,
      ].join('\n');
    } else {
      text = [
        `📈 <b>Ваши продажи</b>`,
        ``,
        `📅 <b>Этот месяц:</b>`,
        `  Продано: <b>${data.salesThisMonth || 0} шт</b>`,
        `  Выручка: <b>${fmt(data.revenueThisMonth)} ₽</b>`,
        ``,
        `📅 <b>Последние 30 дней:</b>`,
        `  Продано: <b>${data.salesLast30 || 0} шт</b>`,
        `  Выручка: <b>${fmt(data.revenueLast30)} ₽</b>`,
        ``,
        `📊 <b>За всё время:</b>`,
        `  Продано: ${s.soldQuantity} шт`,
        `  Возвратов: ${s.returnedQuantity || 0} шт`,
        `  Общая выручка: ${fmt(s.salesRevenue)} ₽`,
        `  Ваша доля: <b>${fmt(s.sponsorProfit)} ₽</b>`,
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

module.exports = { handleSales };
