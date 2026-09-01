require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

const TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!TOKEN) {
  console.error('TELEGRAM_BOT_TOKEN is required');
  process.exit(1);
}

const ADMIN_IDS = (process.env.ADMIN_TELEGRAM_IDS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)
  .map(Number);

const bot = new TelegramBot(TOKEN, { polling: true });

const statusHandler = require('./handlers/status');
const logsHandler = require('./handlers/logs');
const incidentsHandler = require('./handlers/incidents');
const restartHandler = require('./handlers/restart');

function isAdmin(userId) {
  if (ADMIN_IDS.length === 0) return true; // no whitelist = open (dev mode)
  return ADMIN_IDS.includes(userId);
}

function authMiddleware(msg, next) {
  if (!isAdmin(msg.from.id)) {
    bot.sendMessage(msg.chat.id, '⛔ Доступ запрещён.');
    return;
  }
  next();
}

bot.onText(/\/start/, (msg) => {
  const name = msg.from.first_name || 'Администратор';
  bot.sendMessage(msg.chat.id, [
    `👋 Привет, ${name}!`,
    '',
    '🤖 Бот мониторинга <b>davidsklad.ru</b>',
    '',
    'Доступные команды:',
    '/status — статус сервера и PM2 процессов',
    '/logs [N] — последние N строк логов (default 20)',
    '/errors [N] — последние ошибки',
    '/incidents — история инцидентов',
    '/queue — статус очередей',
    '/settings — текущие настройки',
    '/restart api|worker — рестарт процесса',
  ].join('\n'), { parse_mode: 'HTML' });
});

bot.onText(/\/status/, (msg) => {
  authMiddleware(msg, () => statusHandler(bot, msg));
});

bot.onText(/\/logs(?:\s+(\d+))?/, (msg, match) => {
  authMiddleware(msg, () => logsHandler(bot, msg, parseInt(match[1] || '20', 10)));
});

bot.onText(/\/errors(?:\s+(\d+))?/, (msg, match) => {
  authMiddleware(msg, () => logsHandler(bot, msg, parseInt(match[1] || '20', 10), true));
});

bot.onText(/\/incidents/, (msg) => {
  authMiddleware(msg, () => incidentsHandler(bot, msg));
});

bot.onText(/\/queue/, (msg) => {
  authMiddleware(msg, async () => {
    const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
    const secret = process.env.DAVIDSKLAD_API_SECRET || '';
    try {
      const fetch = require('node-fetch');
      const res = await fetch(`${apiBase}/api/health`, {
        headers: { 'x-api-secret': secret },
        timeout: 8000,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const lines = [
        '📊 <b>Статус очередей</b>',
        '',
        `🌐 API: ${data.status || 'ok'}`,
        data.queues ? Object.entries(data.queues).map(([k, v]) => `  ${k}: ${JSON.stringify(v)}`).join('\n') : '(нет данных по очередям)',
      ];
      bot.sendMessage(msg.chat.id, lines.join('\n'), { parse_mode: 'HTML' });
    } catch (e) {
      bot.sendMessage(msg.chat.id, `❌ Ошибка получения данных очередей: ${e.message}`);
    }
  });
});

bot.onText(/\/settings/, (msg) => {
  authMiddleware(msg, async () => {
    const apiBase = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
    const secret = process.env.DAVIDSKLAD_API_SECRET || '';
    try {
      const fetch = require('node-fetch');
      const res = await fetch(`${apiBase}/api/settings`, {
        headers: { 'x-api-secret': secret },
        timeout: 8000,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const s = data.settings || data;
      const lines = [
        '⚙️ <b>Настройки davidsklad.ru</b>',
        '',
        `💵 Курс USD: ${s.fixedUsdRate || s.usdRate || '—'} ₽`,
        `📈 Наценка Ozon: ${s.defaultMarkups?.ozon || '—'}`,
        `📈 Наценка Yandex: ${s.defaultMarkups?.yandex || '—'}`,
        `📈 Наценка WB: ${s.defaultMarkups?.wb || '—'}`,
      ];
      bot.sendMessage(msg.chat.id, lines.join('\n'), { parse_mode: 'HTML' });
    } catch (e) {
      bot.sendMessage(msg.chat.id, `❌ Ошибка получения настроек: ${e.message}`);
    }
  });
});

bot.onText(/\/restart(?:\s+(api|worker))?/, (msg, match) => {
  authMiddleware(msg, () => restartHandler(bot, msg, match[1]));
});

// Callback for restart confirmation
bot.on('callback_query', (query) => {
  if (!isAdmin(query.from.id)) {
    bot.answerCallbackQuery(query.id, { text: '⛔ Доступ запрещён' });
    return;
  }
  restartHandler.handleCallback(bot, query);
});

bot.on('polling_error', (err) => {
  const logFile = path.join(__dirname, 'logs', 'error.log');
  const msg = `${new Date().toISOString()} POLLING_ERROR: ${err.message}\n`;
  fs.appendFileSync(logFile, msg);
  console.error('Polling error:', err.message);
});

console.log(`✅ @MagicVibeAlert_bot started (polling)`);
if (ADMIN_IDS.length > 0) console.log(`Admin IDs: ${ADMIN_IDS.join(', ')}`);
else console.log('WARNING: ADMIN_TELEGRAM_IDS not set — all users have access');
