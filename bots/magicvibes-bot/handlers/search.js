'use strict';

const { getCatalog } = require('../api');
const { setUserState, clearUserState } = require('../db');
const { backToMain, paginationKeyboard } = require('../keyboards/main');
const { productDetailKeyboard } = require('../keyboards/catalog');

async function handleSearchStart(bot, chatId, telegramId) {
  setUserState(telegramId, 'await_search');
  await bot.sendMessage(
    chatId,
    '🔍 Введите название аромата или бренд:',
    { reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'main_menu' }]] } }
  );
}

async function handleSearchQuery(bot, chatId, telegramId, query, page = 0) {
  clearUserState(telegramId);
  try {
    const data = await getCatalog({ q: query, page, pageSize: 5 });
    const products = data.items || data.products || data || [];
    const total = data.total || products.length;
    const totalPages = Math.ceil(total / 5) || 1;

    if (products.length === 0) {
      await bot.sendMessage(
        chatId,
        `🔍 По запросу "<b>${query}</b>" ничего не найдено.\n\nПопробуйте другой запрос.`,
        { parse_mode: 'HTML', reply_markup: backToMain() }
      );
      return;
    }

    const lines = products.map((p, i) => {
      const price = p.priceRub ? `${Number(p.priceRub).toLocaleString('ru')} ₽` : '';
      return `${i + 1 + page * 5}. ${p.name || p.offerId}${price ? ` — ${price}` : ''}`;
    });

    const kb = {
      inline_keyboard: [
        ...products.map((p) => [{ text: p.name || p.offerId, callback_data: `product_${p.offerId}` }]),
        ...(totalPages > 1 ? [(() => {
          const row = [];
          if (page > 0) row.push({ text: '←', callback_data: `search_page_${page - 1}_${encodeURIComponent(query)}` });
          row.push({ text: `${page + 1}/${totalPages}`, callback_data: 'noop' });
          if (page < totalPages - 1) row.push({ text: '→', callback_data: `search_page_${page + 1}_${encodeURIComponent(query)}` });
          return row;
        })()] : []),
        [{ text: '🏠 Главное меню', callback_data: 'main_menu' }],
      ],
    };

    await bot.sendMessage(
      chatId,
      `🔍 По запросу "<b>${query}</b>" найдено: ${total}\n\n${lines.join('\n')}`,
      { parse_mode: 'HTML', reply_markup: kb }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Ошибка поиска. Попробуйте позже.', { reply_markup: backToMain() });
  }
}

module.exports = { handleSearchStart, handleSearchQuery };
