// Telegram-бот для спонсора и Давида.
// /start → пароль → роль → отчёты о продажах.
// Давид получает уведомления при отправке отчёта партнёру.

const SPONSOR_BOT_TOKEN = process.env.SPONSOR_BOT_TOKEN || "";
const SPONSOR_BOT_PASSWORD = process.env.SPONSOR_BOT_PASSWORD || "";
const SPONSOR_BOT_POLL_TIMEOUT = 25; // секунд long-poll
const SPONSOR_BOT_ENABLED = Boolean(SPONSOR_BOT_TOKEN);

// chatId → { role: 'partner'|'david', waitingPassword: bool }
const sponsorBotUsers = new Map();
let sponsorBotOffset = 0;
let sponsorBotRunning = false;
let sponsorBotAbort = null;

async function sponsorBotApiCall(method, body = {}) {
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

async function sponsorBotSend(chatId, text, extra = {}) {
  return sponsorBotApiCall("sendMessage", { chat_id: chatId, text, parse_mode: "HTML", ...extra });
}

async function sponsorBotGetDailyReport() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return null;

  const today = new Date();
  today.setHours(0, 0, 0, 0);

  const [todayOps, allOps] = await Promise.all([
    prisma.consignmentOperation.findMany({
      where: { createdAt: { gte: today }, type: { in: ["sale", "return", "writeoff"] } },
      select: { sponsorDelta: true, type: true },
    }),
    prisma.consignmentOperation.findMany({
      select: { sponsorDelta: true, balanceDelta: true },
    }),
  ]);

  const todaySales = todayOps.filter((op) => op.type === "sale").length;
  const todaySponsorProfit = todayOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0);
  const totalBalance = allOps.reduce((s, op) => s + (Number(op.balanceDelta) || 0), 0);
  const totalSponsorProfit = allOps.reduce((s, op) => s + (Number(op.sponsorDelta) || 0), 0);

  return { todaySales, todaySponsorProfit, totalBalance, totalSponsorProfit };
}

function formatPartnerReport(data) {
  if (!data) return "Данные временно недоступны.";
  const fmt = (n) => Math.round(n).toLocaleString("ru-RU");
  const todayStr = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const lines = [
    `📊 <b>Ваш отчёт за ${todayStr}</b>`,
    ``,
    data.todaySales > 0 ? `✅ Продаж сегодня: ${data.todaySales} шт` : `📭 Продаж сегодня нет`,
    `💰 Ваша доля за день: <b>${fmt(data.todaySponsorProfit)} ₽</b>`,
    ``,
    `📈 Накопленная прибыль: ${fmt(data.totalSponsorProfit)} ₽`,
    `💳 Текущий баланс: ${fmt(data.totalBalance)} ₽`,
  ];
  return lines.join("\n");
}

async function sponsorBotHandleUpdate(update) {
  const msg = update.message || update.callback_query?.message;
  const callbackQuery = update.callback_query;
  const chatId = msg?.chat?.id || callbackQuery?.message?.chat?.id;
  if (!chatId) return;

  const userState = sponsorBotUsers.get(chatId) || { role: null, waitingPassword: false };

  // Callback от кнопок
  if (callbackQuery) {
    await sponsorBotApiCall("answerCallbackQuery", { callback_query_id: callbackQuery.id });
    const data = callbackQuery.data;

    if (data === "role_partner") {
      sponsorBotUsers.set(chatId, { ...userState, role: "partner" });
      const report = await sponsorBotGetDailyReport();
      await sponsorBotSend(chatId, formatPartnerReport(report));
      return;
    }

    if (data === "role_david") {
      sponsorBotUsers.set(chatId, { ...userState, role: "david" });
      await sponsorBotSend(chatId, "✅ Вы будете получать уведомления о продажах.");
      return;
    }

    if (data === "refresh") {
      const st = sponsorBotUsers.get(chatId);
      if (!st?.role) return;
      if (st.role === "partner") {
        const report = await sponsorBotGetDailyReport();
        await sponsorBotSend(chatId, formatPartnerReport(report), {
          reply_markup: { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "refresh" }]] },
        });
      }
      return;
    }
    return;
  }

  const text = String(msg?.text || "").trim();

  // /start
  if (text === "/start" || text.startsWith("/start ")) {
    sponsorBotUsers.set(chatId, { role: null, waitingPassword: true });
    await sponsorBotSend(chatId, "🔐 Введите пароль для доступа:");
    return;
  }

  // Проверка пароля
  if (userState.waitingPassword) {
    if (text === SPONSOR_BOT_PASSWORD) {
      sponsorBotUsers.set(chatId, { role: null, waitingPassword: false });
      await sponsorBotSend(chatId, "✅ Доступ разрешён. Выберите роль:", {
        reply_markup: {
          inline_keyboard: [
            [
              { text: "🤝 Я партнёр", callback_data: "role_partner" },
              { text: "👤 Я Давид", callback_data: "role_david" },
            ],
          ],
        },
      });
    } else {
      await sponsorBotSend(chatId, "❌ Неверный пароль. Попробуйте снова через /start");
      sponsorBotUsers.set(chatId, { role: null, waitingPassword: false });
    }
    return;
  }

  // Уже авторизован — показать меню
  if (userState.role === "partner") {
    const report = await sponsorBotGetDailyReport();
    await sponsorBotSend(chatId, formatPartnerReport(report), {
      reply_markup: { inline_keyboard: [[{ text: "🔄 Обновить", callback_data: "refresh" }]] },
    });
  } else if (userState.role === "david") {
    await sponsorBotSend(chatId, "📬 Вы получаете уведомления о продажах. Когда партнёру отправится отчёт — вы узнаете.");
  } else {
    await sponsorBotSend(chatId, "Введите /start для начала.");
  }
}

async function sponsorBotPollOnce() {
  if (!SPONSOR_BOT_ENABLED) return;
  try {
    const updates = await sponsorBotApiCall("getUpdates", {
      offset: sponsorBotOffset,
      timeout: SPONSOR_BOT_POLL_TIMEOUT,
      allowed_updates: ["message", "callback_query"],
    });
    if (!Array.isArray(updates) || !updates.length) return;
    for (const update of updates) {
      sponsorBotOffset = update.update_id + 1;
      sponsorBotHandleUpdate(update).catch((err) =>
        logger.warn("sponsor_bot_update_error", { detail: String(err?.message || err) })
      );
    }
  } catch (err) {
    if (String(err?.message).includes("aborted")) return;
    logger.warn("sponsor_bot_poll_error", { detail: String(err?.message || err) });
  }
}

async function sponsorBotLoop() {
  sponsorBotRunning = true;
  while (sponsorBotRunning) {
    await sponsorBotPollOnce();
    // короткая пауза между poll-циклами
    await new Promise((r) => setTimeout(r, 500));
  }
}

// Отправить уведомление Давиду когда партнёру отправлен отчёт
async function notifyDavidAboutSponsorReport(report) {
  if (!SPONSOR_BOT_ENABLED) return;
  const davidChats = [...sponsorBotUsers.entries()]
    .filter(([, st]) => st.role === "david")
    .map(([chatId]) => chatId);
  if (!davidChats.length) return;

  const fmt = (n) => Math.round(n).toLocaleString("ru-RU");
  const todayStr = new Date().toLocaleDateString("ru-RU", { day: "numeric", month: "long" });
  const text = [
    `📨 <b>Отчёт отправлен партнёру (${todayStr})</b>`,
    ``,
    report.todaySales > 0 ? `Продаж сегодня: ${report.todaySales} шт` : `Продаж сегодня нет`,
    `Партнёр заработал за день: <b>${fmt(report.todaySponsorProfit)} ₽</b>`,
    `Баланс партнёра: ${fmt(report.totalBalance)} ₽`,
  ].join("\n");

  for (const chatId of davidChats) {
    sponsorBotSend(chatId, text).catch((err) =>
      logger.warn("sponsor_bot_david_notify_error", { chatId, detail: String(err?.message || err) })
    );
  }
}

// Запуск бота (только на worker или когда фоновые задачи включены)
if (SPONSOR_BOT_ENABLED && (process.env.BACKGROUND_JOBS_ENABLED !== "false" || process.env.NODE_ENV !== "production")) {
  sponsorBotLoop().catch((err) =>
    logger.warn("sponsor_bot_loop_crashed", { detail: String(err?.message || err) })
  );
  logger.info("sponsor_bot_started", { token: SPONSOR_BOT_TOKEN.slice(0, 12) + "..." });
}
