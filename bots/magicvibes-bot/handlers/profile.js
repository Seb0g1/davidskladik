'use strict';

const { getMe } = require('../api');
const { getUser, deleteUser } = require('../db');
const { backToMain, mainMenu } = require('../keyboards/main');

async function handleProfile(bot, chatId, telegramId) {
  const user = getUser(telegramId);

  if (!user?.token) {
    await bot.sendMessage(
      chatId,
      '👤 <b>Профиль</b>\n\nАккаунт не привязан.',
      {
        parse_mode: 'HTML',
        reply_markup: {
          inline_keyboard: [
            [{ text: '🔗 Привязать аккаунт', callback_data: 'link_account' }],
            [{ text: '🏠 Главное меню', callback_data: 'main_menu' }],
          ],
        },
      }
    );
    return;
  }

  try {
    const me = await getMe(user.token).catch(() => null);
    const name = me?.firstName ? `${me.firstName} ${me.lastName || ''}`.trim() : user.email;
    const email = me?.email || user.email;

    await bot.sendMessage(
      chatId,
      `👤 <b>Ваш профиль</b>\n\n👤 Имя: ${name}\n📧 Email: ${email}\n🔗 Аккаунт привязан ✅\n\nДата привязки: ${user.linkedAt ? new Date(user.linkedAt).toLocaleDateString('ru') : '—'}`,
      {
        parse_mode: 'HTML',
        reply_markup: {
          inline_keyboard: [
            [{ text: '📦 Мои заказы', callback_data: 'orders' }],
            [{ text: '🔓 Отвязать аккаунт', callback_data: 'unlink_account' }],
            [{ text: '🏠 Главное меню', callback_data: 'main_menu' }],
          ],
        },
      }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Не удалось загрузить профиль.', { reply_markup: backToMain() });
  }
}

async function handleUnlinkAccount(bot, chatId, telegramId) {
  deleteUser(telegramId);
  await bot.sendMessage(
    chatId,
    '🔓 Аккаунт отвязан. До свидания!\n\nВы можете привязать другой аккаунт в любое время.',
    { reply_markup: mainMenu(false) }
  );
}

module.exports = { handleProfile, handleUnlinkAccount };
