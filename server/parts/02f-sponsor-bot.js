// Telegram-бот для спонсора и Давида — красивый интерфейс с inline-кнопками.
// SPONSOR_BOT_TOKEN — токен бота, SPONSOR_BOT_PASSWORD — пароль входа.

const SPONSOR_BOT_TOKEN = process.env.SPONSOR_BOT_TOKEN || "";
const SPONSOR_BOT_PASSWORD = process.env.SPONSOR_BOT_PASSWORD || "";
const SPONSOR_BOT_POLL_TIMEOUT = 25;
const SPONSOR_BOT_ENABLED = Boolean(SPONSOR_BOT_TOKEN);

// chatId → { role: 'partner'|'david'|null, step: 'idle'|'await_password', lastReportMsgId }
const sponsorBotUsers = new Map();
let sponsorBotOffset = 0;
let sponsorBotRunning = false;

// ─── API helpers ────────────────────────────────────────────────────────────

async function sbApi(method, body = {}) {
  if (!SPONSOR_BOT_TOKEN) throw new Error("SPONSOR_BOT_TOKEN not set");
  const resp = await fetch(`https://api.telegram.org/bot${SPONSOR_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30000),
  });
  const data = await resp.json();
  if (!data.ok) throw new Error(`Telegram ${method}: ${data.description}`);
  return data.result;
}

async function sbSend(chatId, text, extra = {}) {
  return sbApi("sendMessage", { chat_id: chatId, text, parse_mode: "HTML", ...extra });
}

async function sbEdit(chatId, msgId, text, extra = {}) {
  return sbApi("editMessageText", { chat_id: chatId, message_id: msgId, text, parse_mode: "HTML", ...extra }).catch(() => null);
}

async function sbAnswer(callbackQueryId, text = "") {
  return sbApi("answerCallbackQuery", { callback_query_id: callbackQueryId, text }).catch(() => null);
}

// ─── Keyboards ───────────────────────────────────────────────────────────────

const KB_ROLE = {
  inline_keyboard: [[
    { text: "🤝 Я партнёр", callback_data: "role:partner" },
    { text: "👤 Я Давид", callback_data: "role:david" },
  ]],
};

function kbPartner() {
  return {
    inline_keyboard: [
      [
        { text: "📊 Отчёт за сегодня", callback_data: "p:today" },
        { text: "💰 Общий баланс", callback_data: "p:balance" },
      ],
      [
        { text: "📈 История за неделю", callback_data: "p:week" },
        { text: "🔄 Обновить", callback_data: "p:refresh" },
      ],
    ],
  };
}

function kbDavid() {
  return {
    inline_keyboard: [
      [
        { text: "📋 Последний отчёт", callback_data: "d:last" },
        { text: "🔔 Статус уведомлений", callback_data: "d:status" },
      ],
      [
        { text: "📊 Статистика", callback_data: "d:stats" },
      ],
    ],
  };
}

const KB_BACK_PARTNER = { inline_keyboard: [[{ text: "◀️ Назад в меню", callback_data: "p:menu" }]] };
const KB_BACK_DAVID = { inline_keyboard: [[{ text: "◀️ Назад в меню", callback_data: "d:menu" }]] };

// ─── Data helpers ────────────────────────────────────────────────────────────

const fmt = (n) => Math.round(Number(n) || 0).toLocaleString("ru-RU");
const fmtF = (n) => (Number(n) || 0).toLocaleString("ru-RU", { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const bar = (pct) => {
  const filled = Math.round(pct / 10);
  return "🟢".repeat(Math.max(0, filled)) + "⬜".repeat(Math.max(0, 10 - filled));
};

async function sbGetStats({ days = 1 } = {}) {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return null;

  const since = new Date();
  since.setDate(since.getDate() - days);
  since.setHours(0, 0, 0, 0);

  const [periodOps, allOps] = await Promise.all([
    prisma.consignmentOperation.findMany({
      where: { createdAt: { gte: since }, type: { in: ["sale", "return", "writeoff"] } },
      select: { sponsorDelta: true, myDelta: true, type: true, unitSale: true, quantity: true },
    }),
    prisma.consignmentOperation.findMany({
      select: { sponsorDelta: true, balanceDelta: true, myDelta: true, type: true },
    }),
  ]);

  const sales = periodOps.filter((op) => op.type === "sale");
  const returns = periodOps.filter((op) => op.type === "return");

  const periodSponsorProfit = periodOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0);
  const periodMyProfit = periodOps.reduce((s, op) => s + (Number(op.myDelta) || 0), 0);
  const periodRevenue = sales.reduce((s, op) => s + (Number(op.unitSale) || 0) * (Number(op.quantity) || 0), 0);

  const totalBalance = allOps.reduce((s, op) => s + (Number(op.balanceDelta) || 0), 0);
  const totalSponsorProfit = allOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0);
  const totalMyProfit = allOps.reduce((s, op) => s + (Number(op.myDelta) || 0), 0);
  const totalSales = allOps.filter((op) => op.type === "sale").length;

  return {
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

// ─── Message builders ─────────────────────────────────────────────────────────

function msgWelcome() {
  return `✨ <b>Magic Vibes — Партнёрский кабинет</b>\n\n` +
    `Добро пожаловать! Этот бот показывает статистику реализации.\n\n` +
    `🔐 Для входа отправьте пароль:`;
}

function msgChooseRole() {
  return `✅ <b>Вход выполнен!</b>\n\n` +
    `Выберите вашу роль:`;
}

function msgPartnerMenu(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  const today = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return `🏠 <b>Главное меню</b>\n` +
    `<i>${today}</i>\n\n` +
    `💰 Баланс: <b>${fmt(data.totalBalance)} ₽</b>\n` +
    `📈 Ваша прибыль (всего): <b>${fmt(data.totalSponsorProfit)} ₽</b>\n\n` +
    `Выберите действие:`;
}

function msgPartnerToday(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  const today = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const hasSales = data.sales > 0;
  return [
    `📊 <b>Отчёт за ${today}</b>`,
    ``,
    hasSales
      ? `✅ Продаж: <b>${data.sales} шт</b>${data.returns > 0 ? `  |  🔄 Возвратов: ${data.returns}` : ``}`
      : `📭 Продаж сегодня нет`,
    hasSales ? `💵 Выручка: ${fmt(data.periodRevenue)} ₽` : ``,
    ``,
    `💰 <b>Ваша доля за день: ${fmt(data.periodSponsorProfit)} ₽</b>`,
    ``,
    `━━━━━━━━━━━━━━━`,
    `📈 Общий накопленный профит: ${fmt(data.totalSponsorProfit)} ₽`,
    `💳 Текущий баланс: <b>${fmt(data.totalBalance)} ₽</b>`,
  ].filter((l) => l !== undefined).join("\n");
}

function msgPartnerBalance(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  return [
    `💰 <b>Детали баланса</b>`,
    ``,
    `💳 Текущий баланс:        <b>${fmt(data.totalBalance)} ₽</b>`,
    `📈 Ваш профит (всего):    <b>${fmt(data.totalSponsorProfit)} ₽</b>`,
    `📦 Всего продаж:          <b>${data.totalSales} шт</b>`,
  ].join("\n");
}

function msgPartnerWeek(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  const hasSales = data.sales > 0;
  return [
    `📈 <b>Статистика за 7 дней</b>`,
    ``,
    `🛍 Продаж: <b>${data.sales} шт</b>${data.returns > 0 ? `  (возвратов: ${data.returns})` : ``}`,
    hasSales ? `💵 Выручка: ${fmt(data.periodRevenue)} ₽` : ``,
    ``,
    `💰 <b>Ваша доля за неделю: ${fmt(data.periodSponsorProfit)} ₽</b>`,
    `🔧 Доля магазина: ${fmt(data.periodMyProfit)} ₽`,
    ``,
    `━━━━━━━━━━━━━━━`,
    `💳 Баланс сейчас: ${fmt(data.totalBalance)} ₽`,
  ].filter(Boolean).join("\n");
}

function msgDavidMenu(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  const today = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
  return `👤 <b>Панель управления</b>\n` +
    `<i>${today}</i>\n\n` +
    `📦 Продаж сегодня: <b>${data.sales} шт</b>\n` +
    `💰 Доля партнёра сегодня: <b>${fmt(data.periodSponsorProfit)} ₽</b>\n\n` +
    `Выберите действие:`;
}

function msgDavidStats(data) {
  if (!data) return `⚠️ Данные временно недоступны.`;
  return [
    `📊 <b>Общая статистика</b>`,
    ``,
    `📦 Всего продаж: <b>${data.totalSales} шт</b>`,
    `💰 Профит партнёра (всего): <b>${fmt(data.totalSponsorProfit)} ₽</b>`,
    `🔧 Мой профит (всего): <b>${fmt(data.totalMyProfit)} ₽</b>`,
    `💳 Баланс партнёра: <b>${fmt(data.totalBalance)} ₽</b>`,
    ``,
    `📅 За 7 дней:`,
    `  Продаж: ${data.sales} шт`,
    `  Доля партнёра: ${fmt(data.periodSponsorProfit)} ₽`,
  ].join("\n");
}

function msgDavidStatus(chatId) {
  const davidCount = [...sponsorBotUsers.values()].filter((u) => u.role === "david").length;
  const partnerCount = [...sponsorBotUsers.values()].filter((u) => u.role === "partner").length;
  return [
    `🔔 <b>Статус уведомлений</b>`,
    ``,
    `✅ Бот активен и принимает обновления`,
    `👤 Давидов в системе: ${davidCount}`,
    `🤝 Партнёров в системе: ${partnerCount}`,
    ``,
    `📬 Уведомления приходят автоматически при:`,
    `  • Ежедневном отчёте партнёру`,
    `  • Ручной отправке отчёта`,
  ].join("\n");
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async function handleStart(chatId) {
  sponsorBotUsers.set(chatId, { role: null, step: "await_password", lastReportMsgId: null });
  await sbSend(chatId, msgWelcome());
}

async function handlePassword(chatId, text) {
  if (!SPONSOR_BOT_PASSWORD) {
    await sbSend(chatId, "⚠️ Пароль не настроен на сервере (SPONSOR_BOT_PASSWORD).");
    return;
  }
  if (text !== SPONSOR_BOT_PASSWORD) {
    sponsorBotUsers.set(chatId, { role: null, step: "idle", lastReportMsgId: null });
    await sbSend(chatId, "❌ <b>Неверный пароль.</b>\n\nПопробуйте снова через /start");
    return;
  }
  sponsorBotUsers.set(chatId, { role: null, step: "idle", lastReportMsgId: null });
  await sbSend(chatId, msgChooseRole(), { reply_markup: KB_ROLE });
}

async function handleCallback(chatId, callbackId, data, msgId) {
  const user = sponsorBotUsers.get(chatId) || { role: null, step: "idle" };
  await sbAnswer(callbackId);

  // Выбор роли
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

  // Партнёрское меню
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

  // Давидово меню
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
    await sbEdit(chatId, msgId, msgDavidStatus(chatId), { reply_markup: KB_BACK_DAVID });
    return;
  }
  if (data === "d:stats") {
    const stats = await sbGetStats({ days: 7 });
    await sbEdit(chatId, msgId, msgDavidStats(stats), { reply_markup: KB_BACK_DAVID });
    return;
  }
}

async function handleText(chatId, text) {
  const user = sponsorBotUsers.get(chatId);
  if (!user || !user.role) {
    await sbSend(chatId, "Введите /start для начала работы.");
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
  try {
    const callbackQuery = update.callback_query;

    if (callbackQuery) {
      const chatId = callbackQuery.message?.chat?.id;
      const msgId = callbackQuery.message?.message_id;
      if (!chatId) return;
      await handleCallback(chatId, callbackQuery.id, callbackQuery.data || "", msgId);
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

    await handleText(chatId, text);
  } catch (err) {
    logger.warn("sponsor_bot_update_error", { detail: String(err?.message || err) });
  }
}

// ─── Polling loop ─────────────────────────────────────────────────────────────

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
      sponsorBotHandleUpdate(update);
    }
  } catch (err) {
    if (String(err?.message).includes("aborted") || String(err?.message).includes("timeout")) return;
    logger.warn("sponsor_bot_poll_error", { detail: String(err?.message || err) });
    await new Promise((r) => setTimeout(r, 5000));
  }
}

async function sponsorBotLoop() {
  sponsorBotRunning = true;
  while (sponsorBotRunning) {
    await sponsorBotPollOnce();
    await new Promise((r) => setTimeout(r, 300));
  }
}

// ─── David notification (called from sendSponsorDailyReport) ─────────────────

async function notifyDavidAboutSponsorReport(report) {
  if (!SPONSOR_BOT_ENABLED) return;
  const davidChats = [...sponsorBotUsers.entries()]
    .filter(([, st]) => st.role === "david")
    .map(([chatId]) => chatId);
  if (!davidChats.length) return;

  const todayStr = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const hasSales = (report.todaySales || 0) > 0;
  const text = [
    `📨 <b>Отчёт отправлен партнёру</b>`,
    `<i>${todayStr}</i>`,
    ``,
    hasSales ? `✅ Продаж: <b>${report.todaySales} шт</b>` : `📭 Продаж сегодня нет`,
    `💰 Доля партнёра за день: <b>${Math.round(report.todaySponsorProfit).toLocaleString("ru-RU")} ₽</b>`,
    `💳 Баланс партнёра: <b>${Math.round(report.totalBalance).toLocaleString("ru-RU")} ₽</b>`,
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
