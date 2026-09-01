'use strict';

const { setUserState, clearUserState } = require('../db');
const { backToMain } = require('../keyboards/main');
const fetch = require('node-fetch');

const BASE = process.env.MAGICVIBES_API_BASE || 'https://magicvibes.ru';

async function handlePromoStart(bot, chatId, telegramId) {
  setUserState(telegramId, 'await_promo');
  await bot.sendMessage(
    chatId,
    '🎁 Введите промокод:',
    { reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'main_menu' }]] } }
  );
}

async function handlePromoCode(bot, chatId, telegramId, code) {
  clearUserState(telegramId);
  try {
    const res = await fetch(`${BASE}/api/shop/promo/check`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: code.trim().toUpperCase() }),
      timeout: 8000,
    });
    if (!res.ok) throw new Error(`status ${res.status}`);
    const data = await res.json();
    if (data.valid) {
      const disc = data.discountPercent ? `${data.discountPercent}%` : data.discountRub ? `${data.discountRub} ₽` : '';
      const until = data.expiresAt ? ` до ${new Date(data.expiresAt).toLocaleDateString('ru')}` : '';
      await bot.sendMessage(
        chatId,
        `🎉 Промокод <b>${code.toUpperCase()}</b> действует!\n${disc ? `Скидка: <b>${disc}</b>` : ''}${until ? `\nДействует${until}` : ''}`,
        { parse_mode: 'HTML', reply_markup: backToMain() }
      );
    } else {
      await bot.sendMessage(chatId, `❌ Промокод <b>${code.toUpperCase()}</b> недействителен или истёк.`, {
        parse_mode: 'HTML',
        reply_markup: backToMain(),
      });
    }
  } catch {
    await bot.sendMessage(chatId, '⚠️ Не удалось проверить промокод. Попробуйте позже.', { reply_markup: backToMain() });
  }
}

module.exports = { handlePromoStart, handlePromoCode };
