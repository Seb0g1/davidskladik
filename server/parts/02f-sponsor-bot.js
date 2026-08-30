// Telegram-бот для спонсора и Давида.
// Только на production worker (SERVER_ROLE=worker).
// Значения в БД и в боте — USD.

const SPONSOR_BOT_TOKEN = process.env.SPONSOR_BOT_TOKEN || "";
const SPONSOR_BOT_PASSWORD = process.env.SPONSOR_BOT_PASSWORD || "";
// 15s poll timeout < 20s startup delay — old instance finishes poll before new starts.
const SPONSOR_BOT_POLL_TIMEOUT = 15;
const SPONSOR_BOT_ENABLED = Boolean(SPONSOR_BOT_TOKEN);

const sponsorBotUsers = new Map(); // chatId → { role, step }
let sponsorBotOffset = 0;
let sponsorBotRunning = false;
const sponsorBotSeenIds = new Set();

// ─── API ─────────────────────────────────────────────────────────────────────

async function sbApi(method, body = {}) {
  if (!SPONSOR_BOT_TOKEN) throw new Error("SPONSOR_BOT_TOKEN not set");
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

// ─── Keyboards ───────────────────────────────────────────────────────────────

// Reply Keyboard — постоянные кнопки внизу экрана
const KB_PARTNER_REPLY = {
  keyboard: [
    [{ text: "📊 Сегодня" }, { text: "📈 За 7 дней" }],
    [{ text: "💰 Баланс" }, { text: "🔄 Обновить" }],
  ],
  resize_keyboard: true,
  is_persistent: true,
};

const KB_DAVID_REPLY = {
  keyboard: [
    [{ text: "📋 Отчёт партнёра" }, { text: "📊 Статистика" }],
    [{ text: "🔔 Уведомления" }],
  ],
  resize_keyboard: true,
  is_persistent: true,
};

const KB_REMOVE = { remove_keyboard: true };

// Inline keyboard выбора роли
const KB_ROLE = {
  inline_keyboard: [[
    { text: "🤝 Я партнёр", callback_data: "role:partner" },
    { text: "👤 Я Давид", callback_data: "role:david" },
  ]],
};

// Inline refresh внутри сообщения
const KB_REFRESH_P = { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "p:refresh" }]] };
const KB_REFRESH_D = { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "d:refresh" }]] };

// ─── Currency ─────────────────────────────────────────────────────────────────

async function sbGetRate() {
  try {
    const rateData = await getUsdRate({ force: false });
    return Number(rateData?.rate || 0) || 85;
  } catch {
    return 85;
  }
}

const F = (n) => (Number(n) || 0).toFixed(2);

// ─── Stats (рабочая логика из b65d6143) ──────────────────────────────────────

async function sbGetStats({ days = 1 } = {}) {
  const prisma = getPrisma();
  if (!prisma) {
    logger.warn("sponsor_bot_stats_no_prisma", { DATABASE_URL: Boolean(process.env.DATABASE_URL) });
    return null;
  }

  const since = new Date();
  since.setDate(since.getDate() - (days - 1));
  since.setHours(0, 0, 0, 0);

  let periodOps, allOps;
  try {
    [periodOps, allOps] = await Promise.all([
      prisma.consignmentOperation.findMany({
        where: { createdAt: { gte: since }, type: { in: ["sale", "return", "writeoff"] } },
        select: { sponsorDelta: true, myDelta: true, type: true, unitSale: true, quantity: true },
      }),
      prisma.consignmentOperation.findMany({
        select: { sponsorDelta: true, balanceDelta: true, myDelta: true, type: true },
      }),
    ]);
  } catch (err) {
    logger.warn("sponsor_bot_stats_error", {
      detail: String(err?.message || err),
      code: err?.code,
      stack: String(err?.stack || "").slice(0, 500),
    });
    return null;
  }

  const sales = periodOps.filter((op) => op.type === "sale");
  const returns = periodOps.filter((op) => op.type === "return");
  const raw = (v) => Number(v) || 0;

  return {
    sales: sales.length,
    returns: returns.length,
    periodSponsor: raw(periodOps.reduce((s, op) => s + raw(op.sponsorDelta), 0)),
    periodMy: raw(periodOps.reduce((s, op) => s + raw(op.myDelta), 0)),
    periodRevenue: raw(sales.reduce((s, op) => s + raw(op.unitSale) * raw(op.quantity), 0)),
    totalBalance: raw(allOps.reduce((s, op) => s + raw(op.balanceDelta), 0)),
    totalSponsor: raw(allOps.reduce((s, op) => s + raw(op.sponsorDelta), 0)),
    totalMy: raw(allOps.reduce((s, op) => s + raw(op.myDelta), 0)),
    totalSales: allOps.filter((op) => op.type === "sale").length,
  };
}

// ─── Messages ─────────────────────────────────────────────────────────────────

const NO_DATA = "⚠️ Данные временно недоступны. Попробуйте позже.";

function msgToday(d) {
  if (!d) return NO_DATA;
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  return [
    `📊 <b>Отчёт за ${date}</b>`,
    ``,
    d.sales > 0
      ? `✅ Продаж: <b>${d.sales} шт</b>${d.returns > 0 ? `  |  🔄 Возвратов: ${d.returns}` : ``}`
      : `📭 Продаж сегодня нет`,
    d.sales > 0 ? `💵 Выручка: $${F(d.periodRevenue)}` : ``,
    ``,
    `💰 <b>Ваша доля за день: $${F(d.periodSponsor)}</b>`,
    `━━━━━━━━━━━━━━━`,
    `📈 Накопленный профит: $${F(d.totalSponsor)}`,
    `💳 Баланс: <b>$${F(d.totalBalance)}</b>`,
  ].filter(Boolean).join("\n");
}

function msgWeek(d) {
  if (!d) return NO_DATA;
  return [
    `📈 <b>Статистика за 7 дней</b>`,
    ``,
    `🛍 Продаж: <b>${d.sales} шт</b>${d.returns > 0 ? `  (возвратов: ${d.returns})` : ``}`,
    d.sales > 0 ? `💵 Выручка: $${F(d.periodRevenue)}` : ``,
    ``,
    `💰 <b>Ваша доля: $${F(d.periodSponsor)}</b>`,
    `🔧 Доля магазина: $${F(d.periodMy)}`,
    `━━━━━━━━━━━━━━━`,
    `💳 Баланс сейчас: $${F(d.totalBalance)}`,
  ].filter(Boolean).join("\n");
}

function msgBalance(d) {
  if (!d) return NO_DATA;
  return [
    `💰 <b>Баланс</b>`,
    ``,
    `💳 Текущий баланс:    <b>$${F(d.totalBalance)}</b>`,
    `📈 Накопл. профит:    <b>$${F(d.totalSponsor)}</b>`,
    `📦 Всего продаж:      <b>${d.totalSales} шт</b>`,
  ].join("\n");
}

function msgWelcomePartner(d) {
  if (!d) return NO_DATA;
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `👋 <b>Добро пожаловать, партнёр!</b>`,
    `<i>${date}</i>`,
    ``,
    `💳 Баланс: <b>$${F(d.totalBalance)}</b>`,
    `📊 Продаж сегодня: <b>${d.sales} шт</b>`,
    `💰 Ваша доля сегодня: <b>$${F(d.periodSponsor)}</b>`,
    ``,
    `Используйте кнопки меню 👇`,
  ].join("\n");
}

function msgWelcomeDavid(d) {
  if (!d) return NO_DATA;
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `👤 <b>Панель управления</b>`,
    `<i>${date}</i>`,
    ``,
    `📦 Продаж сегодня: <b>${d.sales} шт</b>`,
    `💰 Доля партнёра сегодня: <b>$${F(d.periodSponsor)}</b>`,
    ``,
    `Используйте кнопки меню 👇`,
  ].join("\n");
}

function msgDavidStats(d) {
  if (!d) return NO_DATA;
  return [
    `📊 <b>Общая статистика</b>`,
    ``,
    `📦 Всего продаж: <b>${d.totalSales} шт</b>`,
    `💰 Профит партнёра (всего): <b>$${F(d.totalSponsor)}</b>`,
    `🔧 Мой профит (всего): <b>$${F(d.totalMy)}</b>`,
    `💳 Баланс партнёра: <b>$${F(d.totalBalance)}</b>`,
  ].join("\n");
}

function msgNotifications() {
  const davidCount = [...sponsorBotUsers.values()].filter((u) => u.role === "david").length;
  const partnerCount = [...sponsorBotUsers.values()].filter((u) => u.role === "partner").length;
  return [
    `🔔 <b>Уведомления</b>`,
    ``,
    `✅ Бот активен`,
    `🤝 Партнёров: ${partnerCount}`,
    `👤 Давидов: ${davidCount}`,
    ``,
    `📬 Уведомления при ежедневном отчёте партнёру`,
  ].join("\n");
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async function handleStart(chatId) {
  sponsorBotUsers.set(chatId, { role: null, step: "await_password" });
  await sbSend(chatId,
    "✨ <b>Magic Vibes — Партнёрский кабинет</b>\n\nВведите пароль для доступа:",
    { reply_markup: KB_REMOVE }
  );
}

async function handlePassword(chatId, text) {
  if (!SPONSOR_BOT_PASSWORD) {
    await sbSend(chatId, "⚠️ Пароль не настроен (SPONSOR_BOT_PASSWORD не задан).");
    return;
  }
  if (text !== SPONSOR_BOT_PASSWORD) {
    sponsorBotUsers.set(chatId, { role: null, step: "idle" });
    await sbSend(chatId, "❌ <b>Неверный пароль.</b>\n\nПопробуйте снова: /start");
    return;
  }
  sponsorBotUsers.set(chatId, { role: null, step: "idle" });
  await sbSend(chatId, "✅ <b>Доступ разрешён.</b> Выберите роль:", { reply_markup: KB_ROLE });
}

async function handleCallback(chatId, callbackId, data, msgId) {
  await sbAnswer(callbackId);

  if (data === "role:partner") {
    sponsorBotUsers.set(chatId, { role: "partner", step: "idle" });
    await sbEdit(chatId, msgId, "✅ <b>Роль: Партнёр</b>");
    // Сначала ставим клавиатуру, потом загружаем данные
    const loadMsg = await sbSend(chatId, "⏳ Загружаю данные...", { reply_markup: KB_PARTNER_REPLY });
    const d = await sbGetStats({ days: 1 });
    if (loadMsg?.message_id) {
      await sbEdit(chatId, loadMsg.message_id, msgWelcomePartner(d),
        d ? { reply_markup: KB_REFRESH_P } : {}
      );
    }
    return;
  }

  if (data === "role:david") {
    sponsorBotUsers.set(chatId, { role: "david", step: "idle" });
    await sbEdit(chatId, msgId, "✅ <b>Роль: Давид</b>");
    const loadMsg = await sbSend(chatId, "⏳ Загружаю данные...", { reply_markup: KB_DAVID_REPLY });
    const d = await sbGetStats({ days: 1 });
    if (loadMsg?.message_id) {
      await sbEdit(chatId, loadMsg.message_id, msgWelcomeDavid(d),
        d ? { reply_markup: KB_REFRESH_D } : {}
      );
    }
    return;
  }

  if (data === "p:refresh") {
    const d = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgToday(d), { reply_markup: KB_REFRESH_P });
    return;
  }

  if (data === "d:refresh") {
    const d = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgWelcomeDavid(d), { reply_markup: KB_REFRESH_D });
    return;
  }
}

async function handlePartnerText(chatId, text) {
  if (text === "📊 Сегодня" || text === "🔄 Обновить") {
    const d = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgToday(d), { reply_markup: KB_REFRESH_P });
    return;
  }
  if (text === "📈 За 7 дней") {
    const d = await sbGetStats({ days: 7 });
    await sbSend(chatId, msgWeek(d), { reply_markup: KB_REFRESH_P });
    return;
  }
  if (text === "💰 Баланс") {
    const d = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgBalance(d), { reply_markup: KB_REFRESH_P });
    return;
  }
  // Любой другой текст — сводка
  const d = await sbGetStats({ days: 1 });
  await sbSend(chatId, msgWelcomePartner(d));
}

async function handleDavidText(chatId, text) {
  if (text === "📋 Отчёт партнёра") {
    const d = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgToday(d), { reply_markup: KB_REFRESH_D });
    return;
  }
  if (text === "📊 Статистика") {
    const d = await sbGetStats({ days: 7 });
    await sbSend(chatId, msgDavidStats(d), { reply_markup: KB_REFRESH_D });
    return;
  }
  if (text === "🔔 Уведомления") {
    await sbSend(chatId, msgNotifications());
    return;
  }
  const d = await sbGetStats({ days: 1 });
  await sbSend(chatId, msgWelcomeDavid(d));
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
  // 20s: Prisma init + ensures old instance (poll timeout=15s) finishes before we start
  await new Promise((r) => setTimeout(r, 20000));
  if (!sponsorBotRunning) return;
  try {
    const p = getPrisma();
    if (p) {
      await p.$connect();
      logger.info("sponsor_bot_db_connected");
    } else {
      logger.warn("sponsor_bot_db_skip_connect", { DATABASE_URL: Boolean(process.env.DATABASE_URL) });
    }
  } catch (err) {
    logger.warn("sponsor_bot_db_connect_failed", { detail: String(err?.message || err) });
  }
  await sponsorBotDrainOldUpdates();
  while (sponsorBotRunning) {
    await sponsorBotPollOnce();
    await new Promise((r) => setTimeout(r, 200));
  }
  logger.info("sponsor_bot_loop_stopped");
}

// ─── David notification ───────────────────────────────────────────────────────

async function notifyDavidAboutSponsorReport(reportUsd) {
  if (!SPONSOR_BOT_ENABLED) return;
  const davidChats = [...sponsorBotUsers.entries()]
    .filter(([, st]) => st.role === "david")
    .map(([chatId]) => chatId);
  if (!davidChats.length) return;

  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const hasSales = (reportUsd.todaySales || 0) > 0;

  const text = [
    `📨 <b>Отчёт отправлен партнёру</b>`,
    `<i>${date}</i>`,
    ``,
    hasSales ? `✅ Продаж: <b>${reportUsd.todaySales} шт</b>` : `📭 Продаж сегодня нет`,
    `💰 Доля партнёра за день: <b>$${F(reportUsd.todaySponsorProfit)}</b>`,
    `💳 Баланс партнёра: <b>$${F(reportUsd.totalBalance)}</b>`,
  ].join("\n");

  for (const chatId of davidChats) {
    sbSend(chatId, text).catch((err) =>
      logger.warn("sponsor_bot_david_notify_error", { chatId, detail: String(err?.message || err) })
    );
  }
}

// ─── Старт только на worker ───────────────────────────────────────────────────

const isSponsorBotHost = process.env.SERVER_ROLE === "worker";
if (SPONSOR_BOT_ENABLED && isSponsorBotHost) {
  process.once("SIGTERM", () => { sponsorBotRunning = false; });
  sponsorBotLoop().catch((err) =>
    logger.warn("sponsor_bot_loop_crashed", { detail: String(err?.message || err) })
  );
  logger.info("sponsor_bot_started", { tokenPrefix: SPONSOR_BOT_TOKEN.slice(0, 12) + "…" });
}
