'use strict';

const { getPartnerSummary } = require('../api');
const { fmt, fmtDate, backKeyboard } = require('./utils');

function toCsv(items, summary) {
  const esc = (v) => `"${String(v || '').replace(/"/g, '""')}"`;
  const lines = ['Наименование,Артикул,Остаток шт,Закупка ₽,Продажа ₽'];
  for (const it of items) {
    lines.push([esc(it.name), esc(it.article || ''), it.quantity, it.purchasePrice, it.salePrice].join(','));
  }
  lines.push('');
  lines.push(`Баланс партнёра,,${fmt(summary.balance)} ₽,,`);
  lines.push(`Прибыль партнёра,,${fmt(summary.sponsorProfit)} ₽,,`);
  return lines.join('\n');
}

async function handleReport(bot, chatId, _partnerId, viewerRole) {
  const msg = await bot.sendMessage(chatId, '⏳ <i>Формирую отчёт...</i>', { parse_mode: 'HTML' });
  try {
    const data = await getPartnerSummary('');
    const s = data.summary;
    const items = (data.items || []).filter((it) => !it.archived);
    const ourProfit = Math.round(Number(s.salesRevenue || 0) - Number(s.sponsorProfit || 0));

    const lines = [
      `📋 <b>Полный отчёт</b>`,
      ``,
      `📦 <b>Склад:</b>`,
      `  Позиций: ${s.items}  |  Остаток: ${s.stockQuantity} шт`,
      `  Сумма по закупке: ${fmt(s.capitalization)} ₽`,
      ``,
      `📈 <b>Движение:</b>`,
      `  Продано: ${s.soldQuantity} шт  |  Возвращено: ${s.returnedQuantity || 0} шт`,
      ``,
      `💰 <b>Финансы:</b>`,
    ];

    if (viewerRole === 'admin') {
      lines.push(
        `  Выручка: <b>${fmt(s.salesRevenue)} ₽</b>`,
        `  У партнёра: <b>${fmt(s.sponsorProfit)} ₽</b>`,
        `  У нас: <b>${fmt(ourProfit)} ₽</b>`,
        `  Выплачено партнёру: ${fmt(s.sponsorPayouts || 0)} ₽`,
      );
    } else {
      lines.push(
        `  Ваш баланс: <b>${fmt(s.balance)} ₽</b>`,
        `  Ваша прибыль: <b>${fmt(s.sponsorProfit)} ₽</b>`,
        `  Выплачено вам: ${fmt(s.sponsorPayouts || 0)} ₽`,
      );
    }

    lines.push(``, `🕐 <i>${fmtDate(data.updatedAt)}</i>`);

    await bot.editMessageText(lines.join('\n'), {
      chat_id: chatId, message_id: msg.message_id,
      parse_mode: 'HTML', reply_markup: backKeyboard(),
    });

    if (items.length > 0) {
      const csv = toCsv(items, s);
      const buf = Buffer.from('﻿' + csv, 'utf8');
      const dateStr = new Date().toISOString().slice(0, 10);
      await bot.sendDocument(chatId, buf,
        { caption: `📎 Список товаров на ${dateStr}` },
        { filename: `realizatsiya-${dateStr}.csv`, contentType: 'text/csv; charset=utf-8' }
      );
    }
  } catch (e) {
    await bot.editMessageText(
      `❌ Ошибка формирования отчёта.\n<code>${e.message}</code>`,
      { chat_id: chatId, message_id: msg.message_id, parse_mode: 'HTML', reply_markup: backKeyboard() }
    );
  }
}

module.exports = { handleReport };
