const fetch = require('node-fetch');

module.exports = async function logsHandler(bot, msg, n = 20, errorsOnly = false) {
  const chatId = msg.chat.id;
  const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
  const secret = process.env.DAVIDSKLAD_API_SECRET || '';

  await bot.sendMessage(chatId, `⏳ Получаю ${errorsOnly ? 'ошибки' : 'логи'}...`);

  try {
    const endpoint = errorsOnly
      ? `${apiBase}/api/ops/logs?level=error&n=${n}`
      : `${apiBase}/api/ops/logs?n=${n}`;

    const res = await fetch(endpoint, {
      headers: { 'x-api-secret': secret },
      timeout: 15000,
    });

    if (!res.ok) {
      bot.sendMessage(chatId, `❌ Логи недоступны: HTTP ${res.status}\n\nEндпоинт /api/ops/logs не реализован на сервере.`);
      return;
    }

    const data = await res.json();
    const entries = Array.isArray(data) ? data : (data.logs || []);

    if (entries.length === 0) {
      bot.sendMessage(chatId, errorsOnly ? '✅ Ошибок не найдено' : '📭 Логи пусты');
      return;
    }

    const text = entries
      .slice(-n)
      .map(e => {
        if (typeof e === 'string') return e;
        const level = e.level || e.severity || '';
        const ts = e.time || e.timestamp || '';
        const msg2 = e.msg || e.message || JSON.stringify(e);
        return `[${ts}] ${level.toUpperCase()} ${msg2}`;
      })
      .join('\n');

    const label = errorsOnly ? `📋 Последние ${n} ошибок` : `📋 Последние ${n} строк логов`;
    const chunks = splitIntoChunks(`${label}:\n\n<pre>${escapeHtml(text.slice(-3500))}</pre>`, 4096);
    for (const chunk of chunks) {
      await bot.sendMessage(chatId, chunk, { parse_mode: 'HTML' });
    }
  } catch (e) {
    bot.sendMessage(chatId, `❌ Ошибка получения логов: ${e.message}`);
  }
};

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function splitIntoChunks(text, maxLen) {
  const chunks = [];
  for (let i = 0; i < text.length; i += maxLen) chunks.push(text.slice(i, i + maxLen));
  return chunks;
}
