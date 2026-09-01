const fetch = require('node-fetch');

// Pending restarts: callbackQueryId → { process, chatId, msgId }
const pendingRestarts = new Map();

async function restartHandler(bot, msg, processName) {
  const chatId = msg.chat.id;

  if (!processName) {
    bot.sendMessage(chatId, 'Укажи процесс: /restart api или /restart worker');
    return;
  }

  if (!['api', 'worker'].includes(processName)) {
    bot.sendMessage(chatId, '❌ Допустимые процессы: api, worker');
    return;
  }

  const confirmKey = `restart_${processName}_${Date.now()}`;
  pendingRestarts.set(confirmKey, { process: processName, chatId, requestedBy: msg.from.id });

  await bot.sendMessage(chatId, [
    `⚠️ Подтвердить рестарт процесса <b>davidsklad-${processName}</b>?`,
    '',
    'Это прервёт обработку текущих запросов на несколько секунд.',
  ].join('\n'), {
    parse_mode: 'HTML',
    reply_markup: {
      inline_keyboard: [[
        { text: '✅ Подтвердить', callback_data: `confirm_restart:${confirmKey}` },
        { text: '❌ Отмена', callback_data: `cancel_restart:${confirmKey}` },
      ]],
    },
  });
}

restartHandler.handleCallback = async function handleCallback(bot, query) {
  const { data, message, from } = query;
  const chatId = message.chat.id;
  const msgId = message.message_id;

  if (data.startsWith('cancel_restart:')) {
    const key = data.slice('cancel_restart:'.length);
    pendingRestarts.delete(key);
    await bot.editMessageText('❌ Рестарт отменён', { chat_id: chatId, message_id: msgId });
    await bot.answerCallbackQuery(query.id);
    return;
  }

  if (data.startsWith('confirm_restart:')) {
    const key = data.slice('confirm_restart:'.length);
    const pending = pendingRestarts.get(key);
    if (!pending) {
      await bot.answerCallbackQuery(query.id, { text: 'Запрос устарел' });
      return;
    }
    pendingRestarts.delete(key);
    await bot.answerCallbackQuery(query.id);
    await bot.editMessageText(`⏳ Выполняю рестарт <b>davidsklad-${pending.process}</b>...`, {
      chat_id: chatId,
      message_id: msgId,
      parse_mode: 'HTML',
    });

    const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
    const secret = process.env.DAVIDSKLAD_API_SECRET || '';

    try {
      const res = await fetch(`${apiBase}/api/ops/restart`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-secret': secret },
        body: JSON.stringify({ process: pending.process }),
        timeout: 15000,
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status} — эндпоинт /api/ops/restart не реализован`);
      }

      await bot.editMessageText(
        `✅ <b>davidsklad-${pending.process}</b> перезапущен\n\n` +
        `👤 Инициатор: ${from.first_name} (${from.id})`,
        { chat_id: chatId, message_id: msgId, parse_mode: 'HTML' }
      );
    } catch (e) {
      await bot.editMessageText(
        `❌ Ошибка рестарта <b>davidsklad-${pending.process}</b>:\n${e.message}\n\n` +
        `Выполни вручную: <code>ssh root@81.17.154.153 "pm2 restart davidsklad-${pending.process}"</code>`,
        { chat_id: chatId, message_id: msgId, parse_mode: 'HTML' }
      );
    }
  }
};

module.exports = restartHandler;
