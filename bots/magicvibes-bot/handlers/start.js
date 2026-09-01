'use strict';

const { getUser, setUser, setUserState } = require('../db');
const { mainMenu } = require('../keyboards/main');

const WELCOME_LINKED = (name) =>
  `💎 <b>Добро пожаловать, ${name}!</b>\n\nВы в боте магазина <b>Magic Vibes</b> — элитная парфюмерия.\n\nВаш аккаунт привязан ✅\nЧем могу помочь?`;

const WELCOME_NEW =
  `✨ <b>Добро пожаловать в Magic Vibes!</b>\n\n` +
  `Я помогу вам:\n` +
  `• Найти любимый аромат 🌸\n` +
  `• Узнать статус заказа 📦\n` +
  `• Получать уведомления о новинках 🔔\n\n` +
  `Привяжите аккаунт магазина для доступа к заказам и персональным предложениям.`;

async function handleStart(bot, msg) {
  const chatId = msg.chat.id;
  const telegramId = msg.from.id;

  const user = getUser(telegramId);
  const isLinked = Boolean(user?.token);

  const text = isLinked
    ? WELCOME_LINKED(user.customer?.firstName || user.email || 'покупатель')
    : WELCOME_NEW;

  await bot.sendMessage(chatId, text, {
    parse_mode: 'HTML',
    reply_markup: mainMenu(isLinked),
  });
}

async function handleLinkAccount(bot, chatId, telegramId) {
  setUserState(telegramId, 'await_email');
  await bot.sendMessage(
    chatId,
    '📧 Введите <b>email</b>, который вы использовали при регистрации на <b>magicvibes.ru</b>:\n\n' +
    '<i>Мы отправим вам код подтверждения.</i>',
    {
      parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'main_menu' }]] },
    }
  );
}

module.exports = { handleStart, handleLinkAccount };
