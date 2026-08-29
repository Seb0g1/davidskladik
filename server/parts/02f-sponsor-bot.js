// Telegram-бот для спонсора и Давида.
// Запускается ТОЛЬКО на production worker (SERVER_ROLE=worker) — иначе будут дубли.
// Значения в БД хранятся в USD → конвертируем через getUsdRate().

const SPONSOR_BOT_TOKEN = process.env.SPONSOR_BOT_TOKEN || "";
const SPONSOR_BOT_PASSWORD = process.env.SPONSOR_BOT_PASSWORD || "";
// Timeout shorter than startup delay so the OLD instance finishes its in-flight poll
// before the NEW instance starts (prevents duplicate responses during rolling restart).
const SPONSOR_BOT_POLL_TIMEOUT = 15;
const SPONSOR_BOT_ENABLED = Boolean(SPONSOR_BOT_TOKEN);

// chatId → { role: 'partner'|'david'|null, step: 'idle'|'await_password' }
const sponsorBotUsers = new Map();
let sponsorBotOffset = 0;
let sponsorBotRunning = false;
const sponsorBotSeenIds = new Set(); // дедупликация update_id

// ─── API ─────────────────────────────────────────────────────────────────────

async function sbApi(method, body = {}) {
  const resp = await fetch(`https://api.telegram.org/bot${SPONSOR_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(35000),
  });
  const data = await resp.json();
  if (!data.ok) throw new Error(`Telegram ${method}: ${data.description}`);
  return data.result;
}

async function sbSend(chatId, text, extra = {}) {
  return sbApi("sendMessage", { chat_id: chatId, text, parse_mode: "HTML", ...extra });
}

async function sbEdit(chatId, msgId, text, extra = {}) {
  return sbApi("editMessageText", {
    chat_id: chatId, message_id: msgId, text, parse_mode: "HTML", ...extra,
  }).catch(() => null);
}

async function sbAnswer(id, text = "") {
  return sbApi("answerCallbackQuery", { callback_query_id: id, text }).catch(() => null);
}

// ─── Reply keyboards (постоянная клавиатура внизу экрана) ────────────────────

const KB_PARTNER_REPLY = {
  keyboard: [
    [{ text: "📊 Сегодня" }, { text: "📈 За неделю" }],
    [{ text: "💰 Баланс" }, { text: "🔄 Обновить" }],
  ],
  resize_keyboard: true,
  persistent: true,
  is_persistent: true,
};

const KB_DAVID_REPLY = {
  keyboard: [
    [{ text: "📋 Отчёт партнёра" }, { text: "📊 Статистика" }],
    [{ text: "🔔 Уведомления" }],
  ],
  resize_keyboard: true,
  persistent: true,
  is_persistent: true,
};

const KB_REMOVE = { remove_keyboard: true };

// Inline-кнопки для обновления внутри сообщения
const inlineRefreshPartner = { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "refresh" }]] };
const inlineRefreshDavid = { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "d_refresh" }]] };

// ─── Валюта ───────────────────────────────────────────────────────────────────

async function sbGetRate() {
  try {
    const d = await getUsdRate({ force: false });
    return Number(d?.rate || 0) || 85;
  } catch { return 85; }
}

const F = (n) => Math.round(Number(n) || 0).toLocaleString("ru-RU");

// ─── Данные ───────────────────────────────────────────────────────────────────

async function sbGetStatsOnce({ days = 1 } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;
  const since = new Date();
  since.setDate(since.getDate() - (days - 1));
  since.setHours(0, 0, 0, 0);
  const [periodOps, allOps, rate] = await Promise.all([
    prisma.consignmentOperation.findMany({
      where: { createdAt: { gte: since }, type: { in: ["sale", "return", "writeoff"] } },
      select: { sponsorDelta: true, myDelta: true, type: true, unitSale: true, quantity: true },
    }),
    prisma.consignmentOperation.findMany({
      select: { sponsorDelta: true, balanceDelta: true, myDelta: true, type: true },
    }),
    sbGetRate(),
  ]);
  const $ = (v) => (Number(v) || 0) * rate;
  const sales = periodOps.filter((o) => o.type === "sale");
  return {
    rate,
    sales: sales.length,
    returns: periodOps.filter((o) => o.type === "return").length,
    periodSponsor: $(periodOps.reduce((s, o) => s + (Number(o.sponsorDelta) || 0), 0)),
    periodMy: $(periodOps.reduce((s, o) => s + (Number(o.myDelta) || 0), 0)),
    periodRevenue: $(sales.reduce((s, o) => s + (Number(o.unitSale) || 0) * (Number(o.quantity) || 0), 0)),
    totalBalance: $(allOps.reduce((s, o) => s + (Number(o.balanceDelta) || 0), 0)),
    totalSponsor: $(allOps.reduce((s, o) => s + (Number(o.sponsorDelta) || 0), 0)),
    totalMy: $(allOps.reduce((s, o) => s + (Number(o.myDelta) || 0), 0)),
    totalSales: allOps.filter((o) => o.type === "sale").length,
  };
}

async function sbGetStats(opts = {}) {
  // Retry up to 3 times (Prisma may not be ready right after startup)
  for (let i = 0; i < 3; i++) {
    try {
      const result = await sbGetStatsOnce(opts);
      if (result !== null) return result;
    } catch (err) {
      logger.warn("sponsor_bot_stats_error", {
        attempt: i + 1,
        detail: String(err?.message || err),
        stack: String(err?.stack || "").slice(0, 300),
      });
    }
    if (i < 2) await new Promise((r) => setTimeout(r, 4000));
  }
  return null;
}

// ─── Сообщения ────────────────────────────────────────────────────────────────

function msgNoData() {
  return "⚠️ Данные временно недоступны — попробуйте через несколько секунд.";
}

function msgToday(d, days = 1) {
  if (!d) return msgNoData();
  const label = days === 1
    ? new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" })
    : `${days} дней`;
  const lines = [
    days === 1 ? `📊 <b>Отчёт за ${label}</b>` : `📈 <b>Статистика за ${label}</b>`,
    ``,
    d.sales > 0
      ? `✅ Продаж: <b>${d.sales} шт</b>${d.returns > 0 ? `  ·  🔄 возвратов: ${d.returns}` : ``}`
      : `📭 Продаж пока нет`,
    d.sales > 0 ? `💵 Выручка: ${F(d.periodRevenue)} ₽` : ``,
    ``,
    `💰 <b>Ваша доля: ${F(d.periodSponsor)} ₽</b>`,
    d.sales > 0 ? `🔧 Доля магазина: ${F(d.periodMy)} ₽` : ``,
    ``,
    `━━━━━━━━━━━━━━━━━━`,
    `💳 Баланс: <b>${F(d.totalBalance)} ₽</b>`,
    `📈 Профит накопленный: ${F(d.totalSponsor)} ₽`,
  ];
  return lines.filter(Boolean).join("\n");
}

function msgBalance(d) {
  if (!d) return msgNoData();
  return [
    `💰 <b>Ваш баланс</b>`,
    ``,
    `💳 Текущий баланс:      <b>${F(d.totalBalance)} ₽</b>`,
    `📈 Накопл. профит:      <b>${F(d.totalSponsor)} ₽</b>`,
    `📦 Всего продаж:        <b>${d.totalSales} шт</b>`,
    ``,
    `<i>Курс USD/RUB: ${F(d.rate)} ₽</i>`,
  ].join("\n");
}

function msgPartnerWelcome(d) {
  if (!d) return msgNoData();
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `👋 <b>Добро пожаловать, партнёр!</b>`,
    `<i>${date}</i>`,
    ``,
    `💳 Баланс: <b>${F(d.totalBalance)} ₽</b>`,
    `📊 Продаж сегодня: <b>${d.sales} шт</b>`,
    `💰 Ваша доля сегодня: <b>${F(d.periodSponsor)} ₽</b>`,
    ``,
    `Используйте кнопки меню ниже 👇`,
  ].join("\n");
}

function msgDavidWelcome(d) {
  if (!d) return msgNoData();
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `👤 <b>Панель управления</b>`,
    `<i>${date}</i>`,
    ``,
    `📦 Продаж сегодня: <b>${d.sales} шт</b>`,
    `💰 Доля партнёра сегодня: <b>${F(d.periodSponsor)} ₽</b>`,
    ``,
    `Используйте кнопки меню ниже 👇`,
  ].join("\n");
}

function msgDavidStats(d) {
  if (!d) return msgNoData();
  return [
    `📊 <b>Общая статистика</b>`,
    ``,
    `📦 Всего продаж:          <b>${d.totalSales} шт</b>`,
    `💰 Профит партнёра:       <b>${F(d.totalSponsor)} ₽</b>`,
    `🔧 Мой профит:            <b>${F(d.totalMy)} ₽</b>`,
    `💳 Баланс партнёра:       <b>${F(d.totalBalance)} ₽</b>`,
    ``,
    `📅 <b>За 7 дней:</b>`,
    `  Продаж: ${d.sales} шт`,
    `  Доля партнёра: ${F(d.periodSponsor)} ₽`,
    ``,
    `<i>Курс USD/RUB: ${F(d.rate)} ₽</i>`,
  ].join("\n");
}

function msgNotifications() {
  const davidCount = [...sponsorBotUsers.values()].filter((u) => u.role === "david").length;
  const partnerCount = [...sponsorBotUsers.values()].filter((u) => u.role === "partner").length;
  return [
    `🔔 <b>Уведомления</b>`,
    ``,
    `✅ Бот активен и получает обновления`,
    `🤝 Партнёров: ${partnerCount}`,
    `👤 Давидов: ${davidCount}`,
    ``,
    `📬 Вы получаете уведомления при:`,
    `  · Ежедневном отчёте партнёру`,
    `  · Ручной отправке отчёта`,
  ].join("\n");
}

// ─── Обработчики ─────────────────────────────────────────────────────────────

async function handleStart(chatId) {
  sponsorBotUsers.set(chatId, { role: null, step: "await_password" });
  await sbSend(chatId,
    `✨ <b>Magic Vibes — Партнёрский кабинет</b>\n\nВведите пароль для входа:`,
    { reply_markup: KB_REMOVE }
  );
}

async function handlePassword(chatId, text) {
  if (!SPONSOR_BOT_PASSWORD) {
    await sbSend(chatId, "⚠️ Пароль не настроен на сервере.");
    return;
  }
  if (text !== SPONSOR_BOT_PASSWORD) {
    sponsorBotUsers.set(chatId, { role: null, step: "idle" });
    await sbSend(chatId, "❌ <b>Неверный пароль.</b> Попробуйте ещё раз: /start");
    return;
  }
  sponsorBotUsers.set(chatId, { role: null, step: "idle" });
  // Inline кнопки выбора роли (один раз, потом постоянная клавиатура)
  await sbSend(chatId, "✅ <b>Вход выполнен!</b>\n\nВыберите вашу роль:", {
    reply_markup: {
      inline_keyboard: [[
        { text: "🤝 Я партнёр", callback_data: "role:partner" },
        { text: "👤 Я Давид", callback_data: "role:david" },
      ]],
    },
  });
}

async function handlePartnerText(chatId, text) {
  const d1 = async () => sbGetStats({ days: 1 });
  const d7 = async () => sbGetStats({ days: 7 });

  if (text === "📊 Сегодня" || text === "🔄 Обновить") {
    const d = await d1();
    await sbSend(chatId, msgToday(d, 1), { reply_markup: inlineRefreshPartner });
    return;
  }
  if (text === "📈 За неделю") {
    const d = await d7();
    await sbSend(chatId, msgToday(d, 7), { reply_markup: inlineRefreshPartner });
    return;
  }
  if (text === "💰 Баланс") {
    const d = await d1();
    await sbSend(chatId, msgBalance(d), { reply_markup: inlineRefreshPartner });
    return;
  }
  // Любой другой текст — показать приветствие с текущей статистикой
  const d = await d1();
  await sbSend(chatId, msgPartnerWelcome(d));
}

async function handleDavidText(chatId, text) {
  if (text === "📋 Отчёт партнёра") {
    const d = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgToday(d, 1), { reply_markup: inlineRefreshDavid });
    return;
  }
  if (text === "📊 Статистика") {
    const d = await sbGetStats({ days: 7 });
    await sbSend(chatId, msgDavidStats(d), { reply_markup: inlineRefreshDavid });
    return;
  }
  if (text === "🔔 Уведомления") {
    await sbSend(chatId, msgNotifications());
    return;
  }
  const d = await sbGetStats({ days: 1 });
  await sbSend(chatId, msgDavidWelcome(d));
}

async function handleCallback(chatId, callbackId, data, msgId) {
  await sbAnswer(callbackId);

  if (data === "role:partner") {
    sponsorBotUsers.set(chatId, { role: "partner", step: "idle" });
    await sbEdit(chatId, msgId, "✅ <b>Роль: Партнёр</b>");
    // Keyboard sent immediately — stats load after
    const loadMsg = await sbSend(chatId, "⏳ Загружаю данные...", { reply_markup: KB_PARTNER_REPLY });
    const d = await sbGetStats({ days: 1 });
    const text = msgPartnerWelcome(d);
    if (loadMsg?.message_id) {
      await sbEdit(chatId, loadMsg.message_id, text, d ? { reply_markup: inlineRefreshPartner } : {});
    } else {
      await sbSend(chatId, text, d ? { reply_markup: inlineRefreshPartner } : {});
    }
    return;
  }
  if (data === "role:david") {
    sponsorBotUsers.set(chatId, { role: "david", step: "idle" });
    await sbEdit(chatId, msgId, "✅ <b>Роль: Давид</b>");
    const loadMsg = await sbSend(chatId, "⏳ Загружаю данные...", { reply_markup: KB_DAVID_REPLY });
    const d = await sbGetStats({ days: 1 });
    const text = msgDavidWelcome(d);
    if (loadMsg?.message_id) {
      await sbEdit(chatId, loadMsg.message_id, text, d ? { reply_markup: inlineRefreshDavid } : {});
    } else {
      await sbSend(chatId, text, d ? { reply_markup: inlineRefreshDavid } : {});
    }
    return;
  }
  if (data === "refresh") {
    const d = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgToday(d, 1), { reply_markup: inlineRefreshPartner });
    return;
  }
  if (data === "d_refresh") {
    const d = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgToday(d, 1), { reply_markup: inlineRefreshDavid });
    return;
  }
}

// ─── Dispatcher ───────────────────────────────────────────────────────────────

async function sponsorBotHandleUpdate(update) {
  const uid = update.update_id;
  if (sponsorBotSeenIds.has(uid)) return;
  sponsorBotSeenIds.add(uid);
  if (sponsorBotSeenIds.size > 500) {
    sponsorBotSeenIds.delete(sponsorBotSeenIds.values().next().value);
  }

  try {
    if (update.callback_query) {
      const cb = update.callback_query;
      const chatId = cb.message?.chat?.id;
      if (chatId) await handleCallback(chatId, cb.id, cb.data || "", cb.message?.message_id);
      return;
    }
    const msg = update.message;
    if (!msg) return;
    const chatId = msg.chat?.id;
    if (!chatId) return;
    const text = String(msg.text || "").trim();

    if (text === "/start" || text.startsWith("/start ")) {
      await handleStart(chatId);
      return;
    }
    const user = sponsorBotUsers.get(chatId);
    if (user?.step === "await_password") {
      await handlePassword(chatId, text);
      return;
    }
    if (!user?.role) {
      await sbSend(chatId, "Введите /start для входа.");
      return;
    }
    if (user.role === "partner") await handlePartnerText(chatId, text);
    else if (user.role === "david") await handleDavidText(chatId, text);
  } catch (err) {
    logger.warn("sponsor_bot_update_error", { detail: String(err?.message || err) });
  }
}

// ─── Polling ──────────────────────────────────────────────────────────────────

async function sponsorBotDrainOldUpdates() {
  let total = 0;
  try {
    for (;;) {
      const updates = await sbApi("getUpdates", { offset: sponsorBotOffset, timeout: 0, limit: 100 });
      if (!Array.isArray(updates) || !updates.length) break;
      for (const u of updates) sponsorBotSeenIds.add(u.update_id);
      sponsorBotOffset = updates[updates.length - 1].update_id + 1;
      total += updates.length;
      if (updates.length < 100) break;
    }
    if (total > 0) logger.info("sponsor_bot_drained", { skipped: total, nextOffset: sponsorBotOffset });
  } catch (err) {
    logger.warn("sponsor_bot_drain_error", { detail: String(err?.message || err) });
  }
}

async function sponsorBotPollOnce() {
  try {
    const updates = await sbApi("getUpdates", {
      offset: sponsorBotOffset,
      timeout: SPONSOR_BOT_POLL_TIMEOUT,
      allowed_updates: ["message", "callback_query"],
    });
    if (!Array.isArray(updates) || !updates.length) return;
    for (const update of updates) {
      sponsorBotOffset = update.update_id + 1;
      await sponsorBotHandleUpdate(update);
    }
  } catch (err) {
    if (/aborted|timeout/i.test(String(err?.message))) return;
    logger.warn("sponsor_bot_poll_error", { detail: String(err?.message || err) });
    await new Promise((r) => setTimeout(r, 5000));
  }
}

async function sponsorBotLoop() {
  sponsorBotRunning = true;
  // 20s: (a) lets Prisma finish init, (b) ensures the OLD worker's 15s poll timeout
  // has expired before we start polling — preventing dual-instance duplicate messages
  // during PM2 rolling restarts (kill_timeout=15s).
  await new Promise((r) => setTimeout(r, 20000));
  if (!sponsorBotRunning) return; // SIGTERM arrived during startup delay
  await sponsorBotDrainOldUpdates();
  while (sponsorBotRunning) {
    await sponsorBotPollOnce();
    await new Promise((r) => setTimeout(r, 200));
  }
  logger.info("sponsor_bot_loop_stopped");
}

// ─── Уведомление Давиду ───────────────────────────────────────────────────────

async function notifyDavidAboutSponsorReport(reportUsd) {
  if (!SPONSOR_BOT_ENABLED) return;
  const davidChats = [...sponsorBotUsers.entries()]
    .filter(([, st]) => st.role === "david")
    .map(([chatId]) => chatId);
  if (!davidChats.length) return;

  const rate = await sbGetRate();
  const toRub = (v) => F((Number(v) || 0) * rate);
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const hasSales = (reportUsd.todaySales || 0) > 0;

  const text = [
    `📨 <b>Отчёт отправлен партнёру</b>`,
    `<i>${date}</i>`,
    ``,
    hasSales ? `✅ Продаж: <b>${reportUsd.todaySales} шт</b>` : `📭 Продаж сегодня нет`,
    `💰 Доля партнёра: <b>${toRub(reportUsd.todaySponsorProfit)} ₽</b>`,
    `💳 Баланс партнёра: <b>${toRub(reportUsd.totalBalance)} ₽</b>`,
  ].join("\n");

  for (const chatId of davidChats) {
    sbSend(chatId, text, { reply_markup: inlineRefreshDavid }).catch((err) =>
      logger.warn("sponsor_bot_david_notify_error", { chatId, detail: String(err?.message || err) })
    );
  }
}

// ─── Старт только на worker ───────────────────────────────────────────────────

const isSponsorBotHost = process.env.SERVER_ROLE === "worker";
if (SPONSOR_BOT_ENABLED && isSponsorBotHost) {
  // Stop polling immediately on SIGTERM so the old instance doesn't race
  // with the new one during PM2 rolling restarts.
  process.once("SIGTERM", () => {
    sponsorBotRunning = false;
  });

  sponsorBotLoop().catch((err) =>
    logger.warn("sponsor_bot_loop_crashed", { detail: String(err?.message || err) })
  );
  logger.info("sponsor_bot_started", { tokenPrefix: SPONSOR_BOT_TOKEN.slice(0, 12) + "…" });
}
