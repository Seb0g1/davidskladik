// Telegram-бот подбора аромата для magicvibes.ru.
// Пятишаговый квиз → top-3 товара из каталога с фото и ссылками.
// Только на production worker (SERVER_ROLE=worker) или в одном процессе.
// SHOP_BOT_TOKEN — отдельный токен, не связан с SPONSOR_BOT_TOKEN.

const SHOP_BOT_TOKEN = cleanText(process.env.SHOP_BOT_TOKEN || "");
const SHOP_BOT_ENABLED = Boolean(SHOP_BOT_TOKEN);
const _shopBotBaseUrl = process.env.SHOP_BASE_URL || "https://magicvibes.ru";

if (!SHOP_BOT_ENABLED) {
  logger.info("shop_bot_disabled", { reason: "SHOP_BOT_TOKEN not set" });
}

// ─── API helpers ─────────────────────────────────────────────────────────────

async function _sbotApi(method, body = {}) {
  const resp = await fetch(`https://api.telegram.org/bot${SHOP_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(30000),
  });
  const data = await resp.json();
  if (!data.ok) {
    logger.warn("shop-bot api error", { method, description: data.description });
    return null;
  }
  return data.result;
}

async function _sbotSend(chatId, text, extra = {}) {
  return _sbotApi("sendMessage", { chat_id: chatId, text, parse_mode: "HTML", ...extra });
}

async function _sbotEdit(chatId, msgId, text, extra = {}) {
  return _sbotApi("editMessageText", { chat_id: chatId, message_id: msgId, text, parse_mode: "HTML", ...extra }).catch(() => null);
}

async function _sbotAnswer(callbackId) {
  return _sbotApi("answerCallbackQuery", { callback_query_id: callbackId }).catch(() => null);
}

async function _sbotPhoto(chatId, photoUrl, caption, extra = {}) {
  // Try sendPhoto, fall back to sendMessage if photo fails
  const result = await _sbotApi("sendPhoto", { chat_id: chatId, photo: photoUrl, caption, parse_mode: "HTML", ...extra }).catch(() => null);
  if (!result) {
    return _sbotSend(chatId, caption, extra);
  }
  return result;
}

// ─── Quiz definition ──────────────────────────────────────────────────────────

const _SBOT_QUIZ = [
  {
    key: "occasion",
    q: "✨ <b>Для какого случая ищем аромат?</b>",
    opts: [
      { label: "💫 Для себя / каждый день", val: "everyday" },
      { label: "🎁 В подарок", val: "gift" },
      { label: "💼 На работу / офис", val: "office" },
      { label: "🌙 На вечер / праздник", val: "evening" },
    ],
  },
  {
    key: "mood",
    q: "🌸 <b>Какое настроение хочется создать?</b>",
    opts: [
      { label: "🌊 Свежее и лёгкое", val: "fresh" },
      { label: "🍯 Тёплое и уютное", val: "warm" },
      { label: "🌺 Яркое и цветочное", val: "floral" },
      { label: "🌑 Загадочное и глубокое", val: "oriental" },
    ],
  },
  {
    key: "notes",
    q: "🧪 <b>Какие ноты нравятся больше всего?</b>",
    opts: [
      { label: "🍋 Цитрусовые / морские", val: "citrus" },
      { label: "🌹 Цветочные", val: "floral" },
      { label: "🌲 Древесные / мускусные", val: "woody" },
      { label: "🌶️ Восточные / сладкие", val: "oriental" },
    ],
  },
  {
    key: "budget",
    q: "💰 <b>Какой бюджет?</b>",
    opts: [
      { label: "до 3 000 ₽", val: "b1", maxRub: 3000 },
      { label: "3 000 — 7 000 ₽", val: "b2", minRub: 3000, maxRub: 7000 },
      { label: "7 000 — 15 000 ₽", val: "b3", minRub: 7000, maxRub: 15000 },
      { label: "✨ Без ограничений", val: "b4" },
    ],
  },
];

function _sbotInline(opts, prefix) {
  return {
    inline_keyboard: [
      opts.slice(0, 2).map((o) => ({ text: o.label, callback_data: `${prefix}:${o.val}` })),
      opts.slice(2).map((o) => ({ text: o.label, callback_data: `${prefix}:${o.val}` })),
    ],
  };
}

// ─── Catalog search ───────────────────────────────────────────────────────────

const _SBOT_KEYWORD_MAP = {
  // occasion
  office: ["Fresh", "Aqua", "Sport", "Office", "Green", "Citrus"],
  evening: ["Intense", "Oud", "Noir", "Night", "Black", "Extreme", "Absolu"],
  // mood
  fresh: ["Fresh", "Aqua", "Sport", "Citrus", "Sea", "Ocean", "Water", "Light"],
  warm: ["Warm", "Amber", "Vanilla", "Musk", "Woody", "Soft", "Sweet"],
  floral: ["Rose", "Floral", "Fleur", "Blossom", "Flower", "Petal", "Garden"],
  oriental: ["Oud", "Oriental", "Amber", "Intense", "Noir", "Dark", "Opium", "Nuit"],
  // notes
  citrus: ["Citrus", "Bergamot", "Lemon", "Orange", "Fresh", "Aqua", "Grapefruit"],
  woody: ["Cedar", "Sandalwood", "Vetiver", "Wood", "Oud", "Patchouli", "Musk", "Amber"],
};

async function _sbotFindProducts(answers) {
  try {
    // Build keyword list from answers
    const kws = new Set();
    for (const key of ["occasion", "mood", "notes"]) {
      const val = answers[key];
      if (val && _SBOT_KEYWORD_MAP[val]) {
        for (const kw of _SBOT_KEYWORD_MAP[val]) kws.add(kw);
      }
    }
    // Pick the first 3 distinct keywords to search
    const searchTerms = [...kws].slice(0, 3);

    const budgetOpt = _SBOT_QUIZ[3].opts.find((o) => o.val === answers.budget);
    const minRub = budgetOpt?.minRub;
    const maxRub = budgetOpt?.maxRub;

    // Try each keyword until we find enough products
    const seen = new Set();
    const results = [];

    for (const q of searchTerms) {
      if (results.length >= 6) break;
      const { products } = await buildShopProductsFromDb({
        q, brand: "", category: "", inStock: true, sort: "name", page: 1, pageSize: 12,
      });
      for (const p of products) {
        if (seen.has(p.offerId)) continue;
        if (minRub && p.priceRub < minRub) continue;
        if (maxRub && p.priceRub > maxRub) continue;
        seen.add(p.offerId);
        results.push(p);
        if (results.length >= 6) break;
      }
    }

    // If not enough, fallback — fetch popular in-stock within budget
    if (results.length < 3) {
      const { products: fallback } = await buildShopProductsFromDb({
        q: "", brand: "", category: "", inStock: true, sort: "name", page: 1, pageSize: 50,
      });
      for (const p of fallback) {
        if (seen.has(p.offerId)) continue;
        if (minRub && p.priceRub < minRub) continue;
        if (maxRub && p.priceRub > maxRub) continue;
        seen.add(p.offerId);
        results.push(p);
        if (results.length >= 6) break;
      }
    }

    // Shuffle slightly for variety, take top 3
    const shuffled = results.sort(() => Math.random() - 0.5).slice(0, 3);
    return shuffled;
  } catch (err) {
    logger.warn("shop-bot catalog search error", { detail: err?.message });
    return [];
  }
}

// ─── State ────────────────────────────────────────────────────────────────────

const _sbotState = new Map(); // chatId → { step: 0-3, answers: {} }
let _sbotOffset = 0;
let _sbotRunning = false;

function _sbotGetState(chatId) {
  return _sbotState.get(chatId) || { step: -1, answers: {} };
}

// ─── Welcome ──────────────────────────────────────────────────────────────────

async function _sbotStart(chatId) {
  _sbotState.set(chatId, { step: -1, answers: {} });
  await _sbotSend(chatId,
    "👋 Привет! Я помогу подобрать идеальный аромат из коллекции <b>Magic Vibes</b>.\n\nОтвечу на 4 вопроса — и предложу 3 варианта специально для тебя. Готов? 🌸",
    {
      reply_markup: {
        inline_keyboard: [[{ text: "✨ Начать подбор", callback_data: "start_quiz" }]],
      },
    }
  );
}

// ─── Send question ────────────────────────────────────────────────────────────

async function _sbotSendStep(chatId, stepIdx) {
  const step = _SBOT_QUIZ[stepIdx];
  const progress = `[${stepIdx + 1}/4] `;
  await _sbotSend(chatId, progress + step.q, {
    reply_markup: _sbotInline(step.opts, `q${stepIdx}`),
  });
}

// ─── Send results ─────────────────────────────────────────────────────────────

async function _sbotSendResults(chatId, answers) {
  await _sbotSend(chatId, "⏳ Подбираю ароматы специально для тебя...");
  const products = await _sbotFindProducts(answers);

  if (!products.length) {
    await _sbotSend(chatId, "😔 К сожалению, не нашёл подходящих ароматов в наличии. Загляни в каталог:",
      { reply_markup: { inline_keyboard: [[{ text: "🛍 Смотреть каталог", url: _shopBotBaseUrl + "/catalog" }]] } }
    );
    _sbotState.delete(chatId);
    return;
  }

  await _sbotSend(chatId, `✨ <b>Моя подборка для тебя — ${products.length} аромата:</b>`);

  for (let i = 0; i < products.length; i++) {
    const p = products[i];
    const price = p.priceRub ? `${p.priceRub.toLocaleString("ru-RU")} ₽` : "";
    const caption = [
      `<b>${i + 1}. ${p.brand} — ${p.name}</b>`,
      price ? `💰 ${price}` : "",
      p.inStock ? "✅ Есть в наличии" : "",
    ].filter(Boolean).join("\n");

    const link = `${_shopBotBaseUrl}/product/${encodeURIComponent(p.offerId)}`;
    const photo = Array.isArray(p.images) && p.images[0] ? p.images[0] : null;

    await _sbotPhoto(chatId, photo, caption, {
      reply_markup: { inline_keyboard: [[{ text: "🛒 Купить", url: link }]] },
    });
  }

  await _sbotSend(chatId,
    "💛 Нравится? Спрашивай — помогу с выбором!\n\nНажми /start чтобы подобрать снова.",
    { reply_markup: { inline_keyboard: [[{ text: "🔄 Подобрать ещё раз", callback_data: "start_quiz" }]] } }
  );
  _sbotState.delete(chatId);
}

// ─── Handle update ────────────────────────────────────────────────────────────

async function _sbotHandleUpdate(upd) {
  try {
    if (upd.callback_query) {
      const cb = upd.callback_query;
      const chatId = cb.message?.chat?.id;
      if (!chatId) return;
      await _sbotAnswer(cb.id);

      if (cb.data === "start_quiz") {
        _sbotState.set(chatId, { step: 0, answers: {} });
        await _sbotSendStep(chatId, 0);
        return;
      }

      const match = cb.data?.match(/^q(\d+):(.+)$/);
      if (match) {
        const stepIdx = Number(match[1]);
        const val = match[2];
        const state = _sbotGetState(chatId);
        const step = _SBOT_QUIZ[stepIdx];
        if (!step) return;

        const opt = step.opts.find((o) => o.val === val);
        state.answers[step.key] = val;
        state.step = stepIdx + 1;
        _sbotState.set(chatId, state);

        // Confirm selection
        await _sbotEdit(chatId, cb.message.message_id,
          cb.message.text + `\n\n<i>✓ ${opt?.label || val}</i>`
        ).catch(() => null);

        if (stepIdx + 1 < _SBOT_QUIZ.length) {
          await _sbotSendStep(chatId, stepIdx + 1);
        } else {
          await _sbotSendResults(chatId, state.answers);
        }
      }
      return;
    }

    if (upd.message) {
      const msg = upd.message;
      const chatId = msg.chat?.id;
      const text = cleanText(msg.text || "");
      if (!chatId) return;
      if (text.startsWith("/start") || text === "") {
        await _sbotStart(chatId);
      } else {
        // Unknown message — nudge to start
        await _sbotSend(chatId, "Нажми /start чтобы начать подбор аромата 🌸");
      }
    }
  } catch (err) {
    logger.warn("shop-bot handle update error", { detail: err?.message });
  }
}

// ─── Polling loop ─────────────────────────────────────────────────────────────

async function _sbotPoll() {
  if (!SHOP_BOT_ENABLED || _sbotRunning) return;
  _sbotRunning = true;
  try {
    const updates = await _sbotApi("getUpdates", {
      offset: _sbotOffset,
      timeout: 15,
      allowed_updates: ["message", "callback_query"],
    });
    if (Array.isArray(updates) && updates.length) {
      _sbotOffset = updates[updates.length - 1].update_id + 1;
      for (const upd of updates) await _sbotHandleUpdate(upd);
    }
  } catch (err) {
    logger.warn("shop-bot poll error", { detail: err?.message });
  } finally {
    _sbotRunning = false;
  }
}

if (SHOP_BOT_ENABLED) {
  const _sbotInterval = setInterval(_sbotPoll, 2000);
  if (typeof onShutdown === "function") {
    onShutdown(() => clearInterval(_sbotInterval));
  }
  logger.info("shop-bot started", { token: SHOP_BOT_TOKEN.slice(0, 8) + "…" });
}
