'use strict';

require('dotenv').config();

const fs = require('fs');
const path = require('path');
const TelegramBot = require('node-telegram-bot-api');

const { getUser, setUser } = require('./db');
const { handleBalance } = require('./handlers/balance');
const { handleSales } = require('./handlers/sales');
const { handleProducts } = require('./handlers/products');
const { handleReport } = require('./handlers/report');
const { mainMenuKeyboard, roleKeyboard, checkPassword } = require('./handlers/utils');

const TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!TOKEN) { console.error('TELEGRAM_BOT_TOKEN is required'); process.exit(1); }

// Single partner — configured in .env as PARTNER_ID (empty = show all)
const PARTNER_ID = process.env.PARTNER_ID || '';

const LOG_FILE = path.resolve('./logs/error.log');
fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });

function logError(err, ctx = '') {
  const line = `${new Date().toISOString()} [ERROR] ${ctx}: ${err?.message || err}\n`;
  fs.appendFileSync(LOG_FILE, line, 'utf8');
  console.error(line.trim());
}

const bot = new TelegramBot(TOKEN, { polling: true });
console.log('Realizatsiya bot started, partner:', PARTNER_ID || '(all)');

// ─── State machine ───────────────────────────────────────────────────────────
const state = {};
function setState(chatId, data) { state[String(chatId)] = { ...(state[String(chatId)] || {}), ...data }; }
function getState(chatId) { return state[String(chatId)] || {}; }
function clearState(chatId) { delete state[String(chatId)]; }

// ─── Welcome screen ──────────────────────────────────────────────────────────
async function sendWelcome(chatId, editMsgId = null) {
  const text =
    `🌸 <b>Бот реализации Magic Vibes</b>\n\n` +
    `Отслеживайте товары, продажи и баланс.\n\n` +
    `Для входа нажмите кнопку ниже и введите пароль:`;
  const keyboard = { inline_keyboard: [[{ text: '🔑 Войти', callback_data: 'auth_start' }]] };
  if (editMsgId) {
    await bot.editMessageText(text, { chat_id: chatId, message_id: editMsgId, parse_mode: 'HTML', reply_markup: keyboard });
  } else {
    await bot.sendMessage(chatId, text, { parse_mode: 'HTML', reply_markup: keyboard });
  }
}

// ─── Main menu ───────────────────────────────────────────────────────────────
async function sendMainMenu(chatId, editMsgId = null) {
  const user = getUser(chatId);
  const role = user?.role || 'partner';
  const greeting = role === 'admin'
    ? `👑 <b>Панель администратора</b>\n\nВыберите действие:`
    : `✅ <b>Добро пожаловать!</b>\n\nВыберите что хотите посмотреть:`;
  const opts = { parse_mode: 'HTML', reply_markup: mainMenuKeyboard(role) };
  if (editMsgId) {
    await bot.editMessageText(greeting, { chat_id: chatId, message_id: editMsgId, ...opts });
  } else {
    await bot.sendMessage(chatId, greeting, opts);
  }
}

// ─── /start ──────────────────────────────────────────────────────────────────
bot.onText(/^\/start/, async (msg) => {
  const chatId = msg.chat.id;
  clearState(chatId);
  try {
    const user = getUser(chatId);
    if (user?.authenticated) {
      await sendMainMenu(chatId);
    } else {
      await sendWelcome(chatId);
    }
  } catch (e) { logError(e, '/start'); }
});

// ─── Text messages (password input only) ─────────────────────────────────────
bot.on('message', async (msg) => {
  if (msg.text?.startsWith('/')) return;
  const chatId = msg.chat.id;
  const text = (msg.text || '').trim();
  const s = getState(chatId);

  try {
    if (s.step === 'AWAIT_PASSWORD') {
      await bot.deleteMessage(chatId, msg.message_id).catch(() => {});
      if (checkPassword(text)) {
        setState(chatId, { step: 'AWAIT_ROLE' });
        await bot.editMessageText(
          `✅ <b>Пароль принят!</b>\n\nКто вы?`,
          { chat_id: chatId, message_id: s.promptMsgId, parse_mode: 'HTML', reply_markup: roleKeyboard() }
        );
      } else {
        await bot.editMessageText(
          `❌ <b>Неверный пароль.</b>\n\nПопробуйте ещё раз или обратитесь к администратору.`,
          {
            chat_id: chatId, message_id: s.promptMsgId, parse_mode: 'HTML',
            reply_markup: { inline_keyboard: [[{ text: '🔑 Попробовать снова', callback_data: 'auth_start' }]] },
          }
        );
      }
    }
  } catch (e) { logError(e, 'message'); }
});

// ─── Callback queries ─────────────────────────────────────────────────────────
bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const msgId = query.message.message_id;
  const data = query.data || '';

  try {
    await bot.answerCallbackQuery(query.id);
    const user = getUser(chatId);

    // Auth: show password prompt
    if (data === 'auth_start') {
      const promptMsg = await bot.editMessageText(
        `🔑 <b>Введите пароль:</b>\n\n<i>Напишите пароль в этот чат</i>`,
        {
          chat_id: chatId, message_id: msgId, parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: '« Назад', callback_data: 'auth_back' }]] },
        }
      );
      setState(chatId, { step: 'AWAIT_PASSWORD', promptMsgId: promptMsg.message_id });
      return;
    }

    if (data === 'auth_back') {
      clearState(chatId);
      await sendWelcome(chatId, msgId);
      return;
    }

    // Role selection — immediately go to main menu, no name entry
    if (data === 'role_partner') {
      setUser(chatId, { authenticated: true, role: 'partner' });
      clearState(chatId);
      await sendMainMenu(chatId, msgId);
      return;
    }

    if (data === 'role_admin') {
      setUser(chatId, { authenticated: true, role: 'admin' });
      clearState(chatId);
      await sendMainMenu(chatId, msgId);
      return;
    }

    // Require auth for everything below
    if (!user?.authenticated) {
      await sendWelcome(chatId, msgId);
      return;
    }

    const role = user.role;

    if (data === 'menu_back' || data === 'menu_refresh') {
      await sendMainMenu(chatId, msgId);
      return;
    }

    if (data === 'menu_balance') {
      await handleBalance(bot, chatId, PARTNER_ID, role);
      return;
    }

    if (data === 'menu_sales') {
      await handleSales(bot, chatId, PARTNER_ID, role);
      return;
    }

    if (data === 'menu_products') {
      await handleProducts(bot, chatId, PARTNER_ID, 0);
      return;
    }

    if (data === 'menu_report') {
      await handleReport(bot, chatId, PARTNER_ID, role);
      return;
    }

    if (data.startsWith('products_')) {
      const page = Number(data.replace('products_', '')) || 0;
      await handleProducts(bot, chatId, PARTNER_ID, page, msgId);
      return;
    }

  } catch (e) {
    logError(e, `callback:${data}`);
    await bot.answerCallbackQuery(query.id, { text: 'Произошла ошибка, попробуйте ещё раз' }).catch(() => {});
  }
});

bot.on('polling_error', (e) => logError(e, 'polling'));
process.on('unhandledRejection', (r) => logError(r, 'unhandledRejection'));
process.on('uncaughtException', (e) => { logError(e, 'uncaughtException'); process.exit(1); });
