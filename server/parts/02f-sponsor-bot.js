// Telegram-бот для спонсора и Давида — красивый интерфейс с inline-кнопками.
// SPONSOR_BOT_TOKEN — токен бота, SPONSOR_BOT_PASSWORD — пароль входа.
// Значения в БД хранятся в USD — конвертируем в RUB через getUsdRate().

const SPONSOR_BOT_TOKEN = process.env.SPONSOR_BOT_TOKEN || "";
const SPONSOR_BOT_PASSWORD = process.env.SPONSOR_BOT_PASSWORD || "";
const SPONSOR_BOT_POLL_TIMEOUT = 25;
const SPONSOR_BOT_ENABLED = Boolean(SPONSOR_BOT_TOKEN);

// chatId → { role: 'partner'|'david'|null, step: 'idle'|'await_password' }
const sponsorBotUsers = new Map();
let sponsorBotOffset = 0;
let sponsorBotRunning = false;
// Дедупликация: храним последние 500 обработанных update_id
const sponsorBotSeenIds = new Set();

// ─── API helpers ─────────────────────────────────────────────────────────────

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

async function sbAnswer(callbackQueryId, text = "") {
  return sbApi("answerCallbackQuery", { callback_query_id: callbackQueryId, text }).catch(() => null);
}

// ─── Currency ─────────────────────────────────────────────────────────────────

async function sbGetRate() {
  try {
    const rateData = await getUsdRate({ force: false });
    return Number(rateData?.rate || 0) || 85;
  } catch {
    return 85;
  }
}

function sbFmt(usd, rate) {
  const rub = Math.round((Number(usd) || 0) * rate);
  return rub.toLocaleString("ru-RU") + " ₽";
}

// ─── Keyboards ───────────────────────────────────────────────────────────────

const KB_ROLE = {
  inline_keyboard: [[
    { text: "🤝 Я партнёр", callback_data: "role:partner" },
    { text: "👤 Я Давид", callback_data: "role:david" },
  ]],
};

const kbPartner = () => ({
  inline_keyboard: [
    [
      { text: "📊 Сегодня", callback_data: "p:today" },
      { text: "💰 Баланс", callback_data: "p:balance" },
    ],
    [
      { text: "📈 За 7 дней", callback_data: "p:week" },
      { text: "🔄 Обновить", callback_data: "p:refresh" },
    ],
  ],
});

const kbDavid = () => ({
  inline_keyboard: [
    [
      { text: "📋 Последний отчёт", callback_data: "d:last" },
      { text: "📊 Статистика", callback_data: "d:stats" },
    ],
    [
      { text: "🔔 Статус уведомлений", callback_data: "d:status" },
    ],
  ],
});

const KB_BACK_PARTNER = { inline_keyboard: [[{ text: "◀️ В меню", callback_data: "p:menu" }]] };
const KB_BACK_DAVID = { inline_keyboard: [[{ text: "◀️ В меню", callback_data: "d:menu" }]] };

// ─── Data ────────────────────────────────────────────────────────────────────

async function sbGetStats({ days = 1 } = {}) {
  // Не используем shouldUsePostgresStorage() — работаем напрямую через getPrisma()
  const prisma = getPrisma();
  if (!prisma) return null;

  const since = new Date();
  since.setDate(since.getDate() - (days - 1));
  since.setHours(0, 0, 0, 0);

  let periodOps, allOps, rate;
  try {
    [periodOps, allOps, rate] = await Promise.all([
      prisma.consignmentOperation.findMany({
        where: { createdAt: { gte: since }, type: { in: ["sale", "return", "writeoff"] } },
        select: { sponsorDelta: true, myDelta: true, type: true, unitSale: true, quantity: true },
      }),
      prisma.consignmentOperation.findMany({
        select: { sponsorDelta: true, balanceDelta: true, myDelta: true, type: true },
      }),
      sbGetRate(),
    ]);
  } catch (err) {
    logger.warn("sponsor_bot_stats_error", { detail: String(err?.message || err) });
    return null;
  }

  const sales = periodOps.filter((op) => op.type === "sale");
  const returns = periodOps.filter((op) => op.type === "return");

  const usd2rub = (v) => (Number(v) || 0) * rate;

  const periodSponsorProfit = usd2rub(periodOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0));
  const periodMyProfit = usd2rub(periodOps.reduce((s, op) => s + (Number(op.myDelta) || 0), 0));
  const periodRevenue = usd2rub(sales.reduce((s, op) => s + (Number(op.unitSale) || 0) * (Number(op.quantity) || 0), 0));

  const totalBalance = usd2rub(allOps.reduce((s, op) => s + (Number(op.balanceDelta) || 0), 0));
  const totalSponsorProfit = usd2rub(allOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0));
  const totalMyProfit = usd2rub(allOps.reduce((s, op) => s + (Number(op.myDelta) || 0), 0));
  const totalSales = allOps.filter((op) => op.type === "sale").length;

  return {
    rate,
    sales: sales.length,
    returns: returns.length,
    periodSponsorProfit,
    periodMyProfit,
    periodRevenue,
    totalBalance,
    totalSponsorProfit,
    totalMyProfit,
    totalSales,
  };
}

// ─── Messages ────────────────────────────────────────────────────────────────

const F = (n) => Math.round(n).toLocaleString("ru-RU");

function msgPartnerMenu(d) {
  if (!d) return "⚠️ Данные временно недоступны. Попробуйте позже.";
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `🏠 <b>Главное меню</b>`,
    `<i>${date}</i>`,
    ``,
    `💰 Баланс: <b>${F(d.totalBalance)} ₽</b>`,
    `📈 Ваш профит (всего): <b>${F(d.totalSponsorProfit)} ₽</b>`,
    ``,
    `Выберите действие:`,
  ].join("\n");
}

function msgPartnerToday(d) {
  if (!d) return "⚠️ Данные временно недоступны.";
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  return [
    `📊 <b>Отчёт за ${date}</b>`,
    ``,
    d.sales > 0
      ? `✅ Продаж: <b>${d.sales} шт</b>${d.returns > 0 ? `  |  🔄 Возвратов: ${d.returns}` : ``}`
      : `📭 Продаж сегодня нет`,
    d.sales > 0 ? `💵 Выручка: ${F(d.periodRevenue)} ₽` : ``,
    ``,
    `💰 <b>Ваша доля за день: ${F(d.periodSponsorProfit)} ₽</b>`,
    ``,
    `━━━━━━━━━━━━━━━`,
    `📈 Накопленный профит: ${F(d.totalSponsorProfit)} ₽`,
    `💳 Текущий баланс: <b>${F(d.totalBalance)} ₽</b>`,
  ].filter(Boolean).join("\n");
}

function msgPartnerBalance(d) {
  if (!d) return "⚠️ Данные временно недоступны.";
  return [
    `💰 <b>Детали баланса</b>`,
    ``,
    `💳 Текущий баланс:         <b>${F(d.totalBalance)} ₽</b>`,
    `📈 Ваш профит (всего):     <b>${F(d.totalSponsorProfit)} ₽</b>`,
    `📦 Всего продаж:           <b>${d.totalSales} шт</b>`,
    ``,
    `<i>Курс USD: ${F(d.rate)} ₽</i>`,
  ].join("\n");
}

function msgPartnerWeek(d) {
  if (!d) return "⚠️ Данные временно недоступны.";
  return [
    `📈 <b>Статистика за 7 дней</b>`,
    ``,
    `🛍 Продаж: <b>${d.sales} шт</b>${d.returns > 0 ? `  (возвратов: ${d.returns})` : ``}`,
    d.sales > 0 ? `💵 Выручка: ${F(d.periodRevenue)} ₽` : ``,
    ``,
    `💰 <b>Ваша доля: ${F(d.periodSponsorProfit)} ₽</b>`,
    `🔧 Доля магазина: ${F(d.periodMyProfit)} ₽`,
    ``,
    `━━━━━━━━━━━━━━━`,
    `💳 Баланс сейчас: ${F(d.totalBalance)} ₽`,
  ].filter(Boolean).join("\n");
}

function msgDavidMenu(d) {
  if (!d) return "⚠️ Данные временно недоступны.";
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return [
    `👤 <b>Панель управления</b>`,
    `<i>${date}</i>`,
    ``,
    `📦 Продаж сегодня: <b>${d.sales} шт</b>`,
    `💰 Доля партнёра сегодня: <b>${F(d.periodSponsorProfit)} ₽</b>`,
    ``,
    `Выберите действие:`,
  ].join("\n");
}

function msgDavidStats(d) {
  if (!d) return "⚠️ Данные временно недоступны.";
  return [
    `📊 <b>Общая статистика</b>`,
    ``,
    `📦 Всего продаж: <b>${d.totalSales} шт</b>`,
    `💰 Профит партнёра (всего): <b>${F(d.totalSponsorProfit)} ₽</b>`,
    `🔧 Мой профит (всего): <b>${F(d.totalMyProfit)} ₽</b>`,
    `💳 Баланс партнёра: <b>${F(d.totalBalance)} ₽</b>`,
    ``,
    `📅 <b>За 7 дней:</b>`,
    `  Продаж: ${d.sales} шт`,
    `  Доля партнёра: ${F(d.periodSponsorProfit)} ₽`,
    ``,
    `<i>Курс USD: ${F(d.rate)} ₽</i>`,
  ].join("\n");
}

function msgDavidStatus() {
  const davidCount = [...sponsorBotUsers.values()].filter((u) => u.role === "david").length;
  const partnerCount = [...sponsorBotUsers.values()].filter((u) => u.role === "partner").length;
  return [
    `🔔 <b>Статус уведомлений</b>`,
    ``,
    `✅ Бот активен`,
    `👤 Давидов в системе: ${davidCount}`,
    `🤝 Партнёров в системе: ${partnerCount}`,
    ``,
    `📬 Уведомления приходят при:`,
    `  • Ежедневном отчёте партнёру`,
    `  • Ручной отправке отчёта`,
  ].join("\n");
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async function handleStart(chatId) {
  sponsorBotUsers.set(chatId, { role: null, step: "await_password" });
  await sbSend(chatId, [
    `✨ <b>Magic Vibes — Партнёрский кабинет</b>`,
    ``,
    `Добро пожаловать! Введите пароль для доступа:`,
  ].join("\n"));
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
  await sbSend(chatId, "✅ <b>Доступ разрешён!</b>\n\nВыберите вашу роль:", { reply_markup: KB_ROLE });
}

async function handleCallback(chatId, callbackId, data, msgId) {
  const user = sponsorBotUsers.get(chatId) || { role: null, step: "idle" };
  await sbAnswer(callbackId);

  if (data === "role:partner") {
    sponsorBotUsers.set(chatId, { ...user, role: "partner" });
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgPartnerMenu(stats), { reply_markup: kbPartner() });
    return;
  }
  if (data === "role:david") {
    sponsorBotUsers.set(chatId, { ...user, role: "david" });
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgDavidMenu(stats), { reply_markup: kbDavid() });
    return;
  }

  if (data === "p:menu") {
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgPartnerMenu(stats), { reply_markup: kbPartner() });
    return;
  }
  if (data === "p:today" || data === "p:refresh") {
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgPartnerToday(stats), { reply_markup: KB_BACK_PARTNER });
    return;
  }
  if (data === "p:balance") {
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgPartnerBalance(stats), { reply_markup: KB_BACK_PARTNER });
    return;
  }
  if (data === "p:week") {
    const stats = await sbGetStats({ days: 7 });
    await sbEdit(chatId, msgId, msgPartnerWeek(stats), { reply_markup: KB_BACK_PARTNER });
    return;
  }

  if (data === "d:menu") {
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgDavidMenu(stats), { reply_markup: kbDavid() });
    return;
  }
  if (data === "d:last") {
    const stats = await sbGetStats({ days: 1 });
    await sbEdit(chatId, msgId, msgPartnerToday(stats), { reply_markup: KB_BACK_DAVID });
    return;
  }
  if (data === "d:status") {
    await sbEdit(chatId, msgId, msgDavidStatus(), { reply_markup: KB_BACK_DAVID });
    return;
  }
  if (data === "d:stats") {
    const stats = await sbGetStats({ days: 7 });
    await sbEdit(chatId, msgId, msgDavidStats(stats), { reply_markup: KB_BACK_DAVID });
    return;
  }
}

async function handleText(chatId) {
  const user = sponsorBotUsers.get(chatId);
  if (!user?.role) {
    await sbSend(chatId, "Введите /start для начала.");
    return;
  }
  if (user.role === "partner") {
    const stats = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgPartnerMenu(stats), { reply_markup: kbPartner() });
  } else if (user.role === "david") {
    const stats = await sbGetStats({ days: 1 });
    await sbSend(chatId, msgDavidMenu(stats), { reply_markup: kbDavid() });
  }
}

// ─── Update dispatcher ────────────────────────────────────────────────────────

async function sponsorBotHandleUpdate(update) {
  // Дедупликация по update_id — защита от повторной обработки при рестарте
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
      const msgId = cb.message?.message_id;
      if (chatId) await handleCallback(chatId, cb.id, cb.data || "", msgId);
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
    await handleText(chatId);
  } catch (err) {
    logger.warn("sponsor_bot_update_error", { detail: String(err?.message || err) });
  }
}

// ─── Polling loop ─────────────────────────────────────────────────────────────

async function sponsorBotDrainOldUpdates() {
  // Drain loop — сбрасываем ВСЕ накопленные апдейты батчами по 100
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
  if (!SPONSOR_BOT_ENABLED) return;
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
  // Даём серверу/Prisma время инициализироваться перед первым обращением к БД
  await new Promise((r) => setTimeout(r, 8000));
  await sponsorBotDrainOldUpdates(); // пропустить старые — избегаем дублей при рестарте
  while (sponsorBotRunning) {
    await sponsorBotPollOnce();
    await new Promise((r) => setTimeout(r, 200));
  }
}

// ─── David notification ───────────────────────────────────────────────────────

async function notifyDavidAboutSponsorReport(reportUsd) {
  if (!SPONSOR_BOT_ENABLED) return;
  const davidChats = [...sponsorBotUsers.entries()]
    .filter(([, st]) => st.role === "david")
    .map(([chatId]) => chatId);
  if (!davidChats.length) return;

  const rate = await sbGetRate();
  const toRub = (v) => Math.round((Number(v) || 0) * rate);
  const date = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const hasSales = (reportUsd.todaySales || 0) > 0;

  const text = [
    `📨 <b>Отчёт отправлен партнёру</b>`,
    `<i>${date}</i>`,
    ``,
    hasSales ? `✅ Продаж: <b>${reportUsd.todaySales} шт</b>` : `📭 Продаж сегодня нет`,
    `💰 Доля партнёра за день: <b>${toRub(reportUsd.todaySponsorProfit).toLocaleString("ru-RU")} ₽</b>`,
    `💳 Баланс партнёра: <b>${toRub(reportUsd.totalBalance).toLocaleString("ru-RU")} ₽</b>`,
  ].join("\n");

  for (const chatId of davidChats) {
    sbSend(chatId, text, { reply_markup: kbDavid() }).catch((err) =>
      logger.warn("sponsor_bot_david_notify_error", { chatId, detail: String(err?.message || err) })
    );
  }
}

// ─── Start ───────────────────────────────────────────────────────────────────

if (SPONSOR_BOT_ENABLED && (process.env.BACKGROUND_JOBS_ENABLED !== "false" || process.env.NODE_ENV !== "production")) {
  sponsorBotLoop().catch((err) =>
    logger.warn("sponsor_bot_loop_crashed", { detail: String(err?.message || err) })
  );
  logger.info("sponsor_bot_started", { tokenPrefix: SPONSOR_BOT_TOKEN.slice(0, 12) + "…" });
}
