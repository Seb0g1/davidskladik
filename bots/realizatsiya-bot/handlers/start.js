'use strict';

const { loadUsers, saveUsers } = require('../db');

const WELCOME = `
👋 <b>Бот реализации Magic Vibes</b>

Здесь вы можете отслеживать свои товары на реализации, баланс и продажи.

📌 Для начала укажите ваш идентификатор партнёра командой:
<code>/link ВАШИ_ДАННЫЕ</code>

Или введите ваш партнёрский ID прямо сейчас:
`.trim();

async function handleStart(bot, msg) {
  const chatId = msg.chat.id;
  const users = loadUsers();
  const user = users[String(chatId)];

  if (user?.partnerId) {
    await bot.sendMessage(chatId,
      `✅ Вы уже привязаны как партнёр: <b>${user.partnerId}</b>\n\nДоступные команды:\n/balance — баланс\n/sales — продажи\n/products — товары\n/report — отчёт\n/help — справка`,
      { parse_mode: 'HTML' }
    );
  } else {
    await bot.sendMessage(chatId, WELCOME, { parse_mode: 'HTML' });
  }
}

async function handleLink(bot, msg, partnerId) {
  const chatId = msg.chat.id;
  if (!partnerId || partnerId.trim().length < 2) {
    return bot.sendMessage(chatId, '❌ Укажите корректный ID партнёра.\nПример: <code>/link Иванов</code>', { parse_mode: 'HTML' });
  }
  const users = loadUsers();
  users[String(chatId)] = {
    telegramId: chatId,
    partnerId: partnerId.trim(),
    linkedAt: new Date().toISOString(),
  };
  saveUsers(users);
  await bot.sendMessage(chatId,
    `✅ Партнёр привязан: <b>${partnerId.trim()}</b>\n\nТеперь доступны:\n/balance — баланс\n/sales — продажи\n/products — товары\n/report — полный отчёт`,
    { parse_mode: 'HTML' }
  );
}

module.exports = { handleStart, handleLink };
