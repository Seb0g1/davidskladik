'use strict';

const { getAllUsers, getLastBroadcastTime, setLastBroadcastTime } = require('../db');
const { backToMain } = require('../keyboards/main');

const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '').split(',').map((s) => s.trim()).filter(Boolean);
const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function isAdmin(telegramId) {
  return ADMIN_IDS.includes(String(telegramId));
}

async function handleBroadcastCommand(bot, chatId, telegramId, text) {
  if (!isAdmin(telegramId)) {
    await bot.sendMessage(chatId, '⛔ Только для администраторов.', { reply_markup: backToMain() });
    return;
  }

  const lastTime = getLastBroadcastTime();
  if (Date.now() - lastTime < ONE_DAY_MS) {
    const nextTime = new Date(lastTime + ONE_DAY_MS).toLocaleTimeString('ru');
    await bot.sendMessage(chatId, `⚠️ Рассылка уже отправлялась сегодня. Следующая доступна в ${nextTime}.`, { reply_markup: backToMain() });
    return;
  }

  if (!text || text.trim().length < 5) {
    await bot.sendMessage(chatId, '📢 Введите текст рассылки (минимум 5 символов).\n\nПример: <code>/broadcast Новое поступление Dior!</code>', {
      parse_mode: 'HTML',
      reply_markup: backToMain(),
    });
    return;
  }

  const users = getAllUsers().filter((u) => u.telegramId);
  let sent = 0;
  let failed = 0;

  await bot.sendMessage(chatId, `📢 Отправляю рассылку ${users.length} пользователям...`);

  for (const user of users) {
    try {
      await bot.sendMessage(user.telegramId, `📣 <b>Magic Vibes</b>\n\n${text}`, { parse_mode: 'HTML' });
      sent++;
      await new Promise((r) => setTimeout(r, 50));
    } catch {
      failed++;
    }
  }

  setLastBroadcastTime();
  await bot.sendMessage(
    chatId,
    `✅ Рассылка завершена.\nОтправлено: ${sent}\nОшибок: ${failed}`,
    { reply_markup: backToMain() }
  );
}

module.exports = { handleBroadcastCommand, isAdmin };
