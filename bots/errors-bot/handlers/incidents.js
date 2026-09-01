const fetch = require('node-fetch');

module.exports = async function incidentsHandler(bot, msg) {
  const chatId = msg.chat.id;
  const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
  const secret = process.env.DAVIDSKLAD_API_SECRET || '';

  await bot.sendMessage(chatId, '⏳ Загружаю историю инцидентов...');

  try {
    // Try API first
    const res = await fetch(`${apiBase}/api/ops/incidents?n=10`, {
      headers: { 'x-api-secret': secret },
      timeout: 10000,
    });

    let incidents = [];

    if (res.ok) {
      const data = await res.json();
      incidents = Array.isArray(data) ? data : (data.incidents || []);
    } else {
      bot.sendMessage(chatId, `❌ Эндпоинт /api/ops/incidents недоступен (HTTP ${res.status})`);
      return;
    }

    if (incidents.length === 0) {
      bot.sendMessage(chatId, '✅ История инцидентов пуста');
      return;
    }

    const lines = ['🚨 <b>История инцидентов (последние 10)</b>', ''];

    for (const inc of incidents.slice(-10).reverse()) {
      const ts = inc.time || inc.timestamp || inc.date || '';
      const type = inc.type || inc.event || 'unknown';
      const detail = inc.detail || inc.message || inc.msg || '';
      const resolved = inc.resolved ? '✅' : '🔴';
      lines.push(`${resolved} <b>${type}</b>`);
      if (ts) lines.push(`   📅 ${new Date(ts).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' })}`);
      if (detail) lines.push(`   ${detail.slice(0, 200)}`);
      lines.push('');
    }

    bot.sendMessage(chatId, lines.join('\n'), { parse_mode: 'HTML' });
  } catch (e) {
    bot.sendMessage(chatId, `❌ Ошибка: ${e.message}`);
  }
};
