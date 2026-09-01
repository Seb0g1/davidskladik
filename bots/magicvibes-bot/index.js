'use strict';

require('dotenv').config();

const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

const { getUser, setUser, setUserState, clearUserState, getUserState } = require('./db');
const { sendCode, verifyCode } = require('./api');
const { handleStart, handleLinkAccount } = require('./handlers/start');
const { handleCatalogHome, handleBrandsPage, handleBrandProducts, handleProductDetail } = require('./handlers/catalog');
const { handleSearchStart, handleSearchQuery } = require('./handlers/search');
const { handleOrders } = require('./handlers/orders');
const { handleProfile, handleUnlinkAccount } = require('./handlers/profile');
const { handleSubscribeMenu, handleSubToggle } = require('./handlers/subscribe');
const { handleBroadcastCommand, isAdmin } = require('./handlers/broadcast');
const { handlePromoStart, handlePromoCode } = require('./handlers/promo');
const { mainMenu, backToMain } = require('./keyboards/main');
const { decodeShort } = require('./keyboards/catalog');

// ─── Bot init ─────────────────────────────────────────────────────────────────
const TOKEN = process.env.TELEGRAM_BOT_TOKEN;
if (!TOKEN) { console.error('TELEGRAM_BOT_TOKEN not set'); process.exit(1); }

const USE_WEBHOOK = process.env.MAGICVIBES_BOT_USE_WEBHOOK === 'true';

let bot;
if (USE_WEBHOOK) {
  const WEBHOOK_URL = process.env.WEBHOOK_URL;
  const WEBHOOK_PORT = parseInt(process.env.WEBHOOK_PORT || '8443', 10);
  const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
  bot = new TelegramBot(TOKEN, {
    webHook: { port: WEBHOOK_PORT, secretToken: WEBHOOK_SECRET || undefined },
  });
  bot.setWebHook(`${WEBHOOK_URL}/bot${TOKEN}`).then(() => {
    console.log(`[magicvibes-bot] webhook set: ${WEBHOOK_URL}`);
  });
} else {
  // polling — NOTE: conflicts with davidsklad.ru 02f-telegram-news.js if both use same token
  bot = new TelegramBot(TOKEN, { polling: true });
  console.log('[magicvibes-bot] polling started');
}

// ─── Logging ──────────────────────────────────────────────────────────────────
const logsDir = path.resolve('./logs');
try { fs.mkdirSync(logsDir, { recursive: true }); } catch {}
function logError(err) {
  const line = `${new Date().toISOString()} ERROR ${err?.message || String(err)}\n`;
  fs.appendFileSync(path.join(logsDir, 'error.log'), line, 'utf8');
}

// ─── Inline mode ──────────────────────────────────────────────────────────────
const { getCatalog } = require('./api');
bot.on('inline_query', async (query) => {
  const q = query.query.trim();
  if (!q) return;
  try {
    const data = await getCatalog({ q, pageSize: 10 });
    const products = data.items || data.products || data || [];
    const results = products.slice(0, 10).map((p) => ({
      type: 'article',
      id: String(p.offerId),
      title: p.name || p.offerId,
      description: p.priceRub ? `${Number(p.priceRub).toLocaleString('ru')} ₽` : '',
      input_message_content: {
        message_text: `💎 <b>${p.name || p.offerId}</b>\n${p.priceRub ? `Цена: ${Number(p.priceRub).toLocaleString('ru')} ₽` : ''}\nhttps://magicvibes.ru/product/${encodeURIComponent(p.offerId)}`,
        parse_mode: 'HTML',
      },
      thumb_url: p.mainImage || undefined,
    }));
    await bot.answerInlineQuery(query.id, results, { cache_time: 60 });
  } catch { /* silent */ }
});

// ─── /start ───────────────────────────────────────────────────────────────────
bot.onText(/\/start/, async (msg) => {
  try { await handleStart(bot, msg); } catch (e) { logError(e); }
});

// ─── /broadcast <text> ────────────────────────────────────────────────────────
bot.onText(/\/broadcast(.*)/, async (msg, match) => {
  try { await handleBroadcastCommand(bot, msg.chat.id, msg.from.id, (match[1] || '').trim()); }
  catch (e) { logError(e); }
});

// ─── /help ────────────────────────────────────────────────────────────────────
bot.onText(/\/help/, async (msg) => {
  await bot.sendMessage(
    msg.chat.id,
    '💎 <b>Magic Vibes Bot</b>\n\n' +
    '/start — главное меню\n' +
    '/help — эта справка\n\n' +
    'Бот магазина <b>magicvibes.ru</b> — элитная парфюмерия.\n' +
    'Привяжите аккаунт для доступа к заказам и персональным уведомлениям.',
    { parse_mode: 'HTML', reply_markup: backToMain() }
  );
});

// ─── Text messages (state machine) ───────────────────────────────────────────
bot.on('message', async (msg) => {
  if (!msg.text || msg.text.startsWith('/')) return;
  const chatId = msg.chat.id;
  const telegramId = msg.from.id;
  const { state } = getUserState(telegramId);

  try {
    if (state === 'await_search') {
      await handleSearchQuery(bot, chatId, telegramId, msg.text.trim());
      return;
    }
    if (state === 'await_email') {
      const email = msg.text.trim().toLowerCase();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        await bot.sendMessage(chatId, '❌ Неверный формат email. Попробуйте ещё раз:');
        return;
      }
      await bot.sendMessage(chatId, '⏳ Отправляем код подтверждения...');
      try {
        await sendCode(email);
        setUserState(telegramId, 'await_code', { email });
        await bot.sendMessage(chatId, `📧 Код отправлен на <b>${email}</b>.\n\nВведите 6-значный код:`, {
          parse_mode: 'HTML',
          reply_markup: { inline_keyboard: [[{ text: '❌ Отмена', callback_data: 'main_menu' }]] },
        });
      } catch {
        await bot.sendMessage(chatId, '⚠️ Не удалось отправить код. Проверьте email и попробуйте снова.', { reply_markup: backToMain() });
      }
      return;
    }
    if (state === 'await_code') {
      const { email } = getUserState(telegramId).meta;
      const code = msg.text.trim();
      try {
        const result = await verifyCode(email, code);
        const token = result.token || result.accessToken;
        if (!token) throw new Error('no token');
        setUser(telegramId, { email, token, telegramId, linkedAt: Date.now() });
        clearUserState(telegramId);
        await bot.sendMessage(
          chatId,
          `✅ <b>Аккаунт привязан!</b>\n\nEmail: ${email}\n\nТеперь вам доступны заказы и персональные предложения 💎`,
          { parse_mode: 'HTML', reply_markup: mainMenu(true) }
        );
      } catch {
        await bot.sendMessage(chatId, '❌ Неверный код. Попробуйте ещё раз или нажмите "Отмена".');
      }
      return;
    }
    if (state === 'await_promo') {
      await handlePromoCode(bot, chatId, telegramId, msg.text.trim());
      return;
    }
  } catch (e) { logError(e); }
});

// ─── Callback queries ─────────────────────────────────────────────────────────
bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const messageId = query.message.message_id;
  const telegramId = query.from.id;
  const data = query.data;

  await bot.answerCallbackQuery(query.id).catch(() => {});

  try {
    if (data === 'noop') return;

    if (data === 'main_menu') {
      clearUserState(telegramId);
      const user = getUser(telegramId);
      const isLinked = Boolean(user?.token);
      await bot.sendMessage(
        chatId,
        isLinked ? `💎 Главное меню` : `✨ Главное меню`,
        { reply_markup: mainMenu(isLinked) }
      );
      return;
    }

    if (data === 'help') {
      await bot.sendMessage(
        chatId,
        '💎 <b>Magic Vibes Bot</b>\n\nБот магазина magicvibes.ru — элитная парфюмерия.\nПривяжите аккаунт для доступа к заказам.',
        { parse_mode: 'HTML', reply_markup: backToMain() }
      );
      return;
    }

    if (data === 'catalog_home') { await handleCatalogHome(bot, chatId); return; }
    if (data === 'search_start') { await handleSearchStart(bot, chatId, telegramId); return; }
    if (data === 'orders') { await handleOrders(bot, chatId, telegramId); return; }
    if (data === 'profile') { await handleProfile(bot, chatId, telegramId); return; }
    if (data === 'link_account') { await handleLinkAccount(bot, chatId, telegramId); return; }
    if (data === 'unlink_account') { await handleUnlinkAccount(bot, chatId, telegramId); return; }
    if (data === 'subscribe_menu') { await handleSubscribeMenu(bot, chatId, telegramId); return; }
    if (data === 'promo') { await handlePromoStart(bot, chatId, telegramId); return; }

    // brands_page_N
    if (data.startsWith('brands_page_')) {
      const page = parseInt(data.replace('brands_page_', ''), 10) || 0;
      await handleBrandsPage(bot, chatId, page);
      return;
    }

    // brand_<encoded>
    if (data.startsWith('brand_')) {
      const encoded = data.slice('brand_'.length);
      await handleBrandProducts(bot, chatId, encoded, 0);
      return;
    }

    // catalog_brand_<encoded>_<page>
    if (data.startsWith('catalog_brand_')) {
      const parts = data.slice('catalog_brand_'.length).split('_');
      const page = parseInt(parts[parts.length - 1], 10) || 0;
      const encoded = parts.slice(0, -1).join('_');
      await handleBrandProducts(bot, chatId, encoded, page);
      return;
    }

    // product_<offerId>
    if (data.startsWith('product_')) {
      const offerId = data.slice('product_'.length);
      await handleProductDetail(bot, chatId, offerId);
      return;
    }

    // search_page_<page>_<query>
    if (data.startsWith('search_page_')) {
      const rest = data.slice('search_page_'.length);
      const firstUs = rest.indexOf('_');
      const page = parseInt(rest.slice(0, firstUs), 10) || 0;
      const query = decodeURIComponent(rest.slice(firstUs + 1));
      await handleSearchQuery(bot, chatId, telegramId, query, page);
      return;
    }

    // sub_toggle_<brand>
    if (data.startsWith('sub_toggle_')) {
      const brandEncoded = data.slice('sub_toggle_'.length);
      await handleSubToggle(bot, chatId, messageId, telegramId, brandEncoded);
      return;
    }
  } catch (e) {
    logError(e);
    await bot.sendMessage(chatId, '⚠️ Произошла ошибка. Попробуйте снова.', { reply_markup: backToMain() }).catch(() => {});
  }
});

// ─── Error handling ───────────────────────────────────────────────────────────
bot.on('polling_error', (err) => logError(err));
bot.on('error', (err) => logError(err));

console.log('[magicvibes-bot] ready ✨');
