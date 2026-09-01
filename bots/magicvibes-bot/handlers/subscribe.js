'use strict';

const { getBrands } = require('../api');
const { getUser, setUser } = require('../db');
const { backToMain } = require('../keyboards/main');

async function handleSubscribeMenu(bot, chatId, telegramId) {
  const user = getUser(telegramId) || {};
  const subscribed = user.subscribedBrands || [];

  try {
    const brands = await getBrands();
    const topBrands = brands.slice(0, 8);

    const rows = topBrands.map((b) => {
      const isOn = subscribed.includes(b.name);
      return [{
        text: `${isOn ? '✅' : '○'} ${b.name}`,
        callback_data: `sub_toggle_${encodeURIComponent(b.name).slice(0, 30)}`,
      }];
    });

    rows.push([{ text: '🔔 Подписан на: ' + (subscribed.length || 'никого'), callback_data: 'noop' }]);
    rows.push([{ text: '🏠 Главное меню', callback_data: 'main_menu' }]);

    await bot.sendMessage(
      chatId,
      '🔔 <b>Подписки на новинки</b>\n\nВыберите бренды для уведомлений о новых поступлениях:',
      { parse_mode: 'HTML', reply_markup: { inline_keyboard: rows } }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Не удалось загрузить бренды.', { reply_markup: backToMain() });
  }
}

async function handleSubToggle(bot, chatId, messageId, telegramId, brandEncoded) {
  let brand;
  try { brand = decodeURIComponent(brandEncoded); } catch { brand = brandEncoded; }

  const user = getUser(telegramId) || {};
  const subscribed = new Set(user.subscribedBrands || []);

  if (subscribed.has(brand)) {
    subscribed.delete(brand);
    await bot.answerCallbackQuery(undefined, { text: `🔕 Отписались от ${brand}` }).catch(() => {});
  } else {
    subscribed.add(brand);
    await bot.answerCallbackQuery(undefined, { text: `🔔 Подписались на ${brand}` }).catch(() => {});
  }

  setUser(telegramId, { subscribedBrands: Array.from(subscribed) });
  await handleSubscribeMenu(bot, chatId, telegramId);
}

module.exports = { handleSubscribeMenu, handleSubToggle };
