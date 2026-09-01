'use strict';

const { getPartnerSummary } = require('../api');
const { fmt, backKeyboard } = require('./utils');

const PAGE_SIZE = 8;

async function handleProducts(bot, chatId, _partnerId, page = 0, editMsgId = null) {
  const loadMsg = editMsgId
    ? null
    : await bot.sendMessage(chatId, '⏳ <i>Загружаю товары...</i>', { parse_mode: 'HTML' });
  const msgId = editMsgId || loadMsg.message_id;

  try {
    const data = await getPartnerSummary('');
    const items = (data.items || []).filter((it) => !it.archived);

    if (!items.length) {
      await bot.editMessageText('📦 Нет товаров на реализации.', {
        chat_id: chatId, message_id: msgId, parse_mode: 'HTML', reply_markup: backKeyboard(),
      });
      return;
    }

    const totalPages = Math.ceil(items.length / PAGE_SIZE);
    const pg = Math.max(0, Math.min(page, totalPages - 1));
    const slice = items.slice(pg * PAGE_SIZE, (pg + 1) * PAGE_SIZE);

    const lines = [
      `📦 <b>Товары на реализации</b>`,
      `Стр. ${pg + 1}/${totalPages}  ·  Всего: ${items.length} позиций`,
      ``,
    ];

    for (const item of slice) {
      const stock = item.quantity > 0 ? `${item.quantity} шт` : '❌ нет';
      lines.push(
        `<b>${item.name}</b>`,
        `  Арт: <code>${item.article || '—'}</code>  |  Склад: <b>${stock}</b>`,
        `  Закупка: ${fmt(item.purchasePrice)} ₽  →  Продажа: ${fmt(item.salePrice)} ₽`,
        ``
      );
    }

    const navRow = [];
    if (pg > 0) navRow.push({ text: '◀ Назад', callback_data: `products_${pg - 1}` });
    if (pg + 1 < totalPages) navRow.push({ text: 'Вперёд ▶', callback_data: `products_${pg + 1}` });

    const keyboard = { inline_keyboard: [] };
    if (navRow.length) keyboard.inline_keyboard.push(navRow);
    keyboard.inline_keyboard.push([{ text: '« Главное меню', callback_data: 'menu_back' }]);

    await bot.editMessageText(lines.join('\n'), {
      chat_id: chatId, message_id: msgId,
      parse_mode: 'HTML', reply_markup: keyboard,
    });
  } catch (e) {
    await bot.editMessageText(
      `❌ Не удалось загрузить товары.\n<code>${e.message}</code>`,
      { chat_id: chatId, message_id: msgId, parse_mode: 'HTML', reply_markup: backKeyboard() }
    );
  }
}

module.exports = { handleProducts };
