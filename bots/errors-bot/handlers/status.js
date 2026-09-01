const fetch = require('node-fetch');

module.exports = async function statusHandler(bot, msg) {
  const chatId = msg.chat.id;
  const thinking = await bot.sendMessage(chatId, '⏳ Запрашиваю статус...');

  const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
  const secret = process.env.DAVIDSKLAD_API_SECRET || '';

  const lines = ['📊 <b>Статус davidsklad.ru</b>', ''];

  try {
    const res = await fetch(`${apiBase}/api/health`, {
      headers: { 'x-api-secret': secret },
      timeout: 8000,
    });
    const data = await res.json();
    const ok = res.ok && (data.status === 'ok' || data.ok);
    lines.push(`🌐 API сервер: ${ok ? '✅ онлайн' : '❌ ошибка'}`);
    if (data.uptime) lines.push(`⏱ Uptime: ${formatUptime(data.uptime)}`);
    if (data.memory) {
      const mb = Math.round(data.memory.heapUsed / 1024 / 1024);
      lines.push(`💾 Heap: ${mb} MB`);
    }
  } catch (e) {
    lines.push(`🌐 API сервер: ❌ недоступен (${e.message})`);
  }

  // Try diagnostics endpoint
  try {
    const res2 = await fetch(`${apiBase}/api/ops/diagnostics`, {
      headers: { 'x-api-secret': secret },
      timeout: 10000,
    });
    if (res2.ok) {
      const d = await res2.json();
      if (d.db !== undefined) lines.push(`🗄 PostgreSQL: ${d.db ? '✅' : '❌'}`);
      if (d.redis !== undefined) lines.push(`🔴 Redis: ${d.redis ? '✅' : '❌'}`);
      if (d.pm !== undefined) lines.push(`📦 PriceMaster: ${d.pm ? '✅' : '❌'}`);
    }
  } catch (_) {
    // diagnostics endpoint may not exist
  }

  lines.push('');
  lines.push(`🕐 ${new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' })} МСК`);

  await bot.editMessageText(lines.join('\n'), {
    chat_id: chatId,
    message_id: thinking.message_id,
    parse_mode: 'HTML',
  });
};

function formatUptime(seconds) {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}ч ${m}м`;
}
