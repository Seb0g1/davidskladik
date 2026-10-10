// Chat autopilot (owner's rules, 2026-10-10): buyers' chats on Avito, Ozon and Yandex Market.
// • «Это оригинал?» — a warm answer with emoji goes out at once;
// • a question about the perfume itself — country of manufacture, how it smells, notes, how it differs from or
//   resembles another perfume, persistence, concentration, for whom — is answered from the product's data (the Ozon
//   card's attributes + our Fragrantica catalog) and the catalog data of every other perfume the buyer names; a second
//   AI pass checks the answer against that data. A checked answer goes out by itself (setting «productAnswers» =
//   "auto"), anything the check doubts waits on the «Мессенджер Авито» page for a person;
// • delivery, price, bargaining, stock, address, returns, complaints — never answered by the autopilot;
// • Avito: once the deal is done (Avito's system message «Оставьте отзыв о покупателе…») the buyer gets a thank-you
//   with a request for a review, once per chat.
// Only buyers' chats about our goods: on Avito the chat's item must be one of our feed's ads (or a perfume title) —
// the same Avito account also has flat-booking chats; on Ozon only Buyer_Seller chats.
// A message is answered once (handled ids), only when it is the buyer's latest message with no seller reply after
// it, and only when it is fresh (CHAT_AUTOPILOT_MAX_AGE_HOURS, and never older than the autopilot's first run).
// Runs on the worker every CHAT_AUTOPILOT_INTERVAL_MINUTES (3). State: data/chat-autopilot.json — settings,
// handled ids, the last seen update of every Avito chat, pending drafts, log. The api process only edits the state.

const chatAutopilotPath = path.join(dataDir, "chat-autopilot.json");
const chatAutopilotIntervalMs = Math.max(1, Number(process.env.CHAT_AUTOPILOT_INTERVAL_MINUTES || 3) || 3) * 60_000;
const chatAutopilotEnabled = process.env.CHAT_AUTOPILOT !== "false";
const CHAT_AUTOPILOT_MAX_AGE_MS = Math.max(1, Number(process.env.CHAT_AUTOPILOT_MAX_AGE_HOURS || 24) || 24) * 3600_000;
const CHAT_AUTOPILOT_AI_PER_RUN = Math.max(1, Number(process.env.CHAT_AUTOPILOT_AI_PER_RUN || 8) || 8);
const CHAT_AUTOPILOT_DEFAULTS = { avito: true, ozon: true, yandex: true, originality: true, productAnswers: "auto", thankYou: true };
let chatAutopilotTimer = null;
let chatAutopilotRunning = false;

async function readChatAutopilotState() {
  let raw = {};
  try { raw = JSON.parse(await fs.readFile(chatAutopilotPath, "utf8")); } catch { raw = {}; }
  const settings = { ...CHAT_AUTOPILOT_DEFAULTS, ...(raw.settings || {}) };
  if (!["auto", "draft", "off"].includes(settings.productAnswers)) settings.productAnswers = CHAT_AUTOPILOT_DEFAULTS.productAnswers;
  return {
    settings,
    startedAt: raw.startedAt || null,
    handled: raw.handled && typeof raw.handled === "object" ? raw.handled : {},
    seen: raw.seen && typeof raw.seen === "object" ? raw.seen : {},
    pending: Array.isArray(raw.pending) ? raw.pending : [],
    log: Array.isArray(raw.log) ? raw.log : [],
    lastRun: raw.lastRun || null,
    runRequestedAt: raw.runRequestedAt || null,
  };
}

let chatAutopilotWriteChain = Promise.resolve();
/** Read the freshest state, apply fn, write it back — serialized inside the process. */
function updateChatAutopilotState(fn) {
  const run = chatAutopilotWriteChain.then(async () => {
    const state = await readChatAutopilotState();
    const result = await fn(state);
    const handledKeys = Object.keys(state.handled);
    if (handledKeys.length > 8000) {
      for (const key of handledKeys.sort((a, b) => String(state.handled[a].at).localeCompare(String(state.handled[b].at))).slice(0, handledKeys.length - 8000)) delete state.handled[key];
    }
    const seenKeys = Object.keys(state.seen);
    if (seenKeys.length > 3000) for (const key of seenKeys.slice(0, seenKeys.length - 3000)) delete state.seen[key];
    state.log = state.log.slice(-300);
    await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
    const tmp = `${chatAutopilotPath}.${process.pid}.tmp`;
    await fs.writeFile(tmp, JSON.stringify(state));
    await fs.rename(tmp, chatAutopilotPath);
    return result;
  });
  chatAutopilotWriteChain = run.catch(() => {});
  return run;
}

function chatAutopilotLog(state, entry) {
  state.log.push({ at: new Date().toISOString(), ...entry });
}

// ── what the message is about ──────────────────────────────────────────────────
// about the perfume itself: the autopilot may answer from the product's data
const CHAT_PRODUCT_TOPIC_RE = /(оригинал|подлинн|подделк|стран[аеуы]|производ|изготов|сделан|нот[аыуе]?\b|пахн|запах|звуч|аромат|похож|отлича|отличи|разниц|сравн|лучше|аналог|стойк|шлейф|сезон|концентрац|парфюмерн|туалетн|\bedp\b|\bedt\b|духи|экстракт|extrait|для\s+(него|не[её]|мужчин|женщин|девушк|парн)|мужск|женск|унисекс|состав|год\s+выпуск|парфюмер)/i;
// what a person must answer: logistics, money, stock, meetings, returns, complaints
const CHAT_HUMAN_TOPIC_RE = /(достав|отправ|когда\s+(будет|придет|придёт|приед|сможете)|адрес|самовывоз|забрать|встрет|подъех|приех|курьер|трек|пвз|пункт|наличи|есть\s+ли\s+(у\s+вас|в\s+наличии)|ещ[её]\s+(есть|продает|продаёт|актуальн)|актуальн|сколько\s+(стоит|будет)|цен[аыуе]|скидк|торг|дешевл|уступ|оплат|предоплат|перевод|карт[аоу]|сбп|возврат|вернуть|обмен|брак|разбит|не\s+пришл|не\s+работает|жалоб|отзыв|чек|документ|сертификат|декларац|телефон|номер|whatsapp|ватсап|телеграм|telegram)/i;

// a cancellation, «подумаю», an order already placed — a person answers (the dry run on 2026-10-10 answered
// «У меня отмена. Подумаю насчёт аромата» with the perfume's notes)
const CHAT_NOT_A_QUESTION_RE = /(отмен|передума|подума|заказал|оформил|купил|спасибо|благодар)/i;
const CHAT_QUESTION_RE = /(\?|^(а\s+)?(как|какой|какая|какое|какие|какого|чем|где|откуда|кто|похож|отлич|стойк|подходит)|подскаж|скажите|расскаж|интересует|хочу\s+узнать)/i;

/** Pure: the message asks something the autopilot may answer from the product's data. */
function isChatProductQuestion(text = "") {
  const t = cleanText(text).toLowerCase();
  if (!t || t.length < 3) return false;
  if (CHAT_HUMAN_TOPIC_RE.test(t.replace(/оригинал[а-я]*/g, " "))) return false;
  if (CHAT_NOT_A_QUESTION_RE.test(t) || !CHAT_QUESTION_RE.test(t)) return false;
  return CHAT_PRODUCT_TOPIC_RE.test(t);
}

/** Pure: Avito's message after a finished deal — the moment to thank the buyer. */
const AVITO_DEAL_DONE_RE = /(оставьте\s+отзыв\s+о\s+покупател|покупатель\s+(получил|забрал)\s+заказ|заказ\s+(получен|доставлен|выдан)\s+покупател)/i;
function isAvitoDealDoneMessage(text = "") {
  return AVITO_DEAL_DONE_RE.test(cleanText(text));
}

const CHAT_PERFUME_TITLE_RE = /(\d+\s*(мл|ml)\b|парфюм|туалетн|духи|одеколон|eau\s+de|\bedp\b|\bedt\b|perfume|parfum|tester|тестер)/i;

const AVITO_THANK_YOU_TEXTS = [
  "Спасибо за покупку! 💜\nОчень рады, что вы выбрали Magic Stick ✨ Пусть аромат радует вас каждый день и дарит хорошее настроение 🌸\nЕсли всё понравилось, оставьте, пожалуйста, отзыв ⭐️ Для нас это очень важно и помогает другим покупателям сделать выбор 🙏",
  "Благодарим за заказ! 🌸\nНадеемся, аромат оправдал ожидания и станет вашим любимым ✨\nБудем очень признательны за отзыв о покупке ⭐️ Это займёт минутку, а нам поможет становиться лучше 💜",
  "Спасибо, что выбрали нас! ✨\nПусть новый аромат будет с вами в самые приятные моменты 🌷\nЕсли не сложно, оставьте отзыв ⭐️ Нам очень важно ваше мнение 🙏💜",
];

function avitoThankYouText(chatId = "") {
  let hash = 0;
  for (const ch of String(chatId)) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
  return AVITO_THANK_YOU_TEXTS[hash % AVITO_THANK_YOU_TEXTS.length];
}

function chatStoreName(marketplace = "", target = "") {
  return marketplace === "avito" ? "Magic Stick" : feedbackStoreName(marketplace, target);
}

function chatMarketplaceLabel(marketplace = "") {
  return marketplace === "avito" ? "Авито" : feedbackMarketplaceLabel(marketplace);
}

// ── the product of a chat ──────────────────────────────────────────────────────
let avitoListingByTitleCache = { at: 0, map: new Map() };
async function avitoListingByTitle(title) {
  if (Date.now() - avitoListingByTitleCache.at > 10 * 60_000) {
    const state = await readAvitoListingsFile().catch(() => ({ items: [] }));
    const map = new Map();
    for (const item of state.items || []) {
      const key = normalizeAvitoMatchText(item.title);
      if (key && !map.has(key)) map.set(key, item);
    }
    avitoListingByTitleCache = { at: Date.now(), map };
  }
  return avitoListingByTitleCache.map.get(normalizeAvitoMatchText(title)) || null;
}

/** The warehouse product behind a chat: { offerId, productName } (offerId may be empty). */
async function resolveChatProduct(chat) {
  const prisma = getPrisma();
  if (chat.marketplace === "avito") {
    const listing = await avitoListingByTitle(chat.itemTitle);
    let offerId = cleanText(listing?.sourceOfferId);
    if (!offerId && listing?.sourceProductId && prisma) {
      const row = await prisma.warehouseProduct.findFirst({ where: { id: cleanText(listing.sourceProductId) }, select: { offerId: true } }).catch(() => null);
      offerId = cleanText(row?.offerId);
    }
    return { offerId, sku: "", productName: cleanText(chat.itemTitle) };
  }
  if (chat.marketplace === "ozon") {
    if (chat.sku) return { offerId: "", sku: chat.sku, productName: chat.productName || "" };
    const posting = (cleanText(chat.text).match(/(\d{7,10}-\d{3,5})(?:-\d+)?/) || [])[1];
    if (posting && prisma) {
      const order = await prisma.financeOrder.findFirst({ where: { postingNumber: { startsWith: posting } }, select: { offerId: true, productName: true } }).catch(() => null);
      if (order) return { offerId: cleanText(order.offerId), sku: "", productName: cleanText(order.productName) };
    }
    return { offerId: "", sku: "", productName: "" };
  }
  if (chat.marketplace === "yandex" && chat.orderId && prisma) {
    const order = await prisma.financeOrder.findFirst({ where: { marketplace: "yandex", orderId: chat.orderId }, select: { offerId: true, productName: true } }).catch(() => null);
    if (order) return { offerId: cleanText(order.offerId), sku: "", productName: cleanText(order.productName) };
  }
  return { offerId: "", sku: "", productName: "" };
}

/** Catalog facts of a perfume found by free text (brand + name): notes, accords, year, perfumer. */
async function catalogFactsByText(text) {
  const q = cleanText(text).replace(/(парфюмерн[а-я]*|туалетн[а-я]*|вода|духи|одеколон|тестер|женск[а-я]*|мужск[а-я]*|унисекс|для\s+\S+|\d+\s*(мл|ml)\b)/gi, " ").replace(/\s+/g, " ").trim();
  if (q.length < 3) return null;
  const found = await listFragranticaPerfumes({ q, limit: 1 }).catch(() => null);
  const perfume = found?.items?.[0];
  if (!perfume?.id) return null;
  const catalog = await shopCatalogNotes(perfume.id).catch(() => null);
  if (!catalog) return null;
  const facts = [];
  const f = catalog.facts || {};
  if (catalog.topNotes.length) facts.push(`Верхние ноты: ${catalog.topNotes.join(", ")}`);
  if (catalog.middleNotes.length) facts.push(`Ноты сердца: ${catalog.middleNotes.join(", ")}`);
  if (catalog.baseNotes.length) facts.push(`Базовые ноты: ${catalog.baseNotes.join(", ")}`);
  if (catalog.accords.length) facts.push(`Аккорды: ${catalog.accords.join(", ")}`);
  if (f.family) facts.push(`Семейство: ${f.family}`);
  if (f.year) facts.push(`Год выпуска аромата: ${f.year}`);
  if ((f.perfumers || []).length) facts.push(`Парфюмер: ${f.perfumers.join(", ")}`);
  if (f.gender) facts.push(`Для кого: ${f.gender}`);
  return facts.length ? { name: [f.brand, f.aroma].filter(Boolean).join(" ") || q, facts } : null;
}

/** Perfumes the buyer names by name («чем отличается от Light Blue Intense?») — AI picks them out of the message. */
async function namedPerfumesInMessage(text, productName, aiSettings) {
  try {
    const completion = await createTextAiChat([
      { role: "system", content: `Из сообщения покупателя выпиши ароматы, которые он упоминает для сравнения, кроме товара, о котором чат. Верни JSON {"perfumes": ["бренд название", ...]} — не больше 2, пустой массив, если других ароматов нет.` },
      { role: "user", content: `Товар чата: «${productName || "—"}».\nСообщение: «${text}»` },
    ], { json: true, temperature: 0, maxTokens: 120, aiSettings });
    const parsed = JSON.parse(cleanText(completion.choices?.[0]?.message?.content || "{}"));
    return (Array.isArray(parsed.perfumes) ? parsed.perfumes : []).map(cleanText).filter(Boolean).slice(0, 2);
  } catch {
    return [];
  }
}

/** Facts → answer → check for one chat message. */
async function draftChatAnswer(chat, aiSettings) {
  const product = await resolveChatProduct(chat);
  const question = { marketplace: chat.marketplace === "avito" ? "ozon" : chat.marketplace, target: chat.marketplace === "ozon" ? chat.target : "ozon",
    sku: product.sku, offerId: product.offerId, text: chat.text, productName: product.productName };
  let facts = [];
  if (product.offerId || product.sku) facts = (await collectQuestionFacts(question).catch(() => ({ facts: [] }))).facts;
  if (!facts.length && product.productName) facts = (await catalogFactsByText(product.productName))?.facts || [];
  const mentioned = await findMentionedProducts(question).catch(() => []);
  for (const name of await namedPerfumesInMessage(chat.text, product.productName, aiSettings)) {
    const found = await catalogFactsByText(name).catch(() => null);
    if (found) mentioned.push({ ref: name, name: found.name, facts: found.facts });
  }
  const withData = { ...chat, productName: product.productName, mentioned };
  const draft = await buildChatAnswerWithFacts(withData, facts, aiSettings);
  if (!draft) throw new Error("ИИ не вернул текст");
  const check = await verifyQuestionAnswer(withData, facts, draft, aiSettings);
  return { offerId: product.offerId, productName: product.productName, facts: [...facts, ...mentioned.flatMap((m) => [`— ${m.name}:`, ...m.facts.slice(0, 12)])], draft, check };
}

async function buildChatAnswerWithFacts(chat, facts, aiSettings) {
  const storeName = chatStoreName(chat.marketplace, chat.target);
  const messages = [
    { role: "system", content: `Ты — консультант магазина парфюмерии «${storeName}» и отвечаешь покупателю в чате на ${chatMarketplaceLabel(chat.marketplace)}.
Правила:
- Только по-русски, тепло и коротко: 1–4 предложения, как живой продавец в мессенджере, 1–2 уместных эмодзи. Без подписи и без приветствия длиннее одного слова.
- Опирайся на «Данные о товаре». Спрашивают страну производства — назови её прямо, если она есть в данных.
- Спрашивают, как пахнет или на что похож аромат, — опиши по нотам и аккордам из данных.
- Покупатель назвал другой аромат и о нём есть данные — сравни по существу: общие и разные ноты, характер, для кого.
- Спрашивают «оригинал?» — подтверди, что у нас только оригинальная продукция.
- Отвечай только фактами из данных или общеизвестными фактами. Не придумывай ноты, страну, концентрацию.
- Не пиши «в данных не указано» — покупатель не видит наших данных; если факта нет, скажи, от чего это зависит, без догадок.
- Не обещай скидок, сроков, наличия; не давай ссылок, телефонов, адресов и других контактов, не зови в другие мессенджеры.
Верни только текст ответа.` },
    { role: "user", content: `Товар: «${chat.productName || "—"}».\nДанные о товаре:\n${facts.length ? facts.join("\n") : "(нет данных)"}${(chat.mentioned || []).map((m) => `\n\nДанные об аромате «${m.name}»:\n${m.facts.join("\n") || "(нет данных)"}`).join("")}\n\nСообщение покупателя: «${chat.text}»` },
  ];
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const completion = await createTextAiChat(messages, { json: false, temperature: 0.5, maxTokens: 350, aiSettings });
    const text = cleanText(completion.choices?.[0]?.message?.content || "");
    if (text && !/^\s*[{\[]/.test(text) && !/https?:\/\/|www\.|\+7\s?\d|8\s?\(?9\d\d/i.test(text)) return text;
  }
  return "";
}

async function buildChatOriginalityAnswer(chat, aiSettings) {
  const storeName = chatStoreName(chat.marketplace, chat.target);
  const fallback = `Здравствуйте! 🌸 Да, это 100% оригинальная продукция ✨ Мы работаем только с проверенными поставщиками и не продаём копии. Пусть аромат дарит вам радость! 💫`;
  try {
    const completion = await createTextAiChat([
      { role: "system", content: `Ты — консультант магазина «${storeName}» в чате на ${chatMarketplaceLabel(chat.marketplace)}. Покупатель спрашивает, оригинальный ли товар.
Напиши короткий тёплый ответ (2–3 предложения), как живой продавец в мессенджере: да, это оригинальная продукция, мы работаем только с оригиналом. Добавь 2–3 уместных эмодзи (✨🌸💫🤍 и т.п.).
Без подписи. Не обещай документов, сертификатов и маркировки, не упоминай другие магазины, сайты, ссылки и контакты. Верни только текст.` },
      { role: "user", content: `${chat.productName ? `Товар: «${chat.productName}». ` : ""}Сообщение: «${chat.text}»` },
    ], { json: false, temperature: 0.9, maxTokens: 200, aiSettings });
    const text = cleanText(completion.choices?.[0]?.message?.content || "");
    return text && !/https?:\/\/|www\./i.test(text) ? text : fallback;
  } catch {
    return fallback;
  }
}

// ── marketplace I/O ────────────────────────────────────────────────────────────
async function sendChatAutopilotMessage({ marketplace, target, chatId, text }) {
  if (marketplace === "avito") {
    const account = getAvitoAccountByTarget(target) || getAvitoAccounts()[0];
    if (!account) throw new Error("Avito аккаунт не найден");
    return sendAvitoMessage(account, chatId, text);
  }
  if (marketplace === "ozon") {
    const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
    if (!account) throw new Error("Ozon аккаунт не найден");
    return ozonRequest("/v1/chat/send/message", { chat_id: chatId, text }, account);
  }
  if (marketplace === "yandex") {
    const shop = getYandexShopByTarget(target) || getYandexShops()[0];
    if (!shop?.businessId) throw new Error("Yandex кабинет не найден");
    return yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats/message?chatId=${encodeURIComponent(chatId)}`, { message: { text } });
  }
  throw new Error(`неизвестная площадка ${marketplace}`);
}

const chatTimeMs = (value) => {
  if (typeof value === "number") return value < 1e12 ? value * 1000 : value;
  return Date.parse(value || "") || 0;
};

/**
 * Buyer messages waiting for an answer: { key, marketplace, target, chatId, messageId, text, createdAt, … } and
 * Avito chats whose deal is done ({ thankYou: true }). Every source fails on its own.
 */
async function collectChatAutopilotWork(settings, state) {
  const work = [];
  const warnings = [];
  const seenUpdates = {};

  if (settings.avito) {
    for (const account of getAvitoAccounts()) {
      try {
        const userId = await getAvitoUserId(account);
        const data = await avitoRequest(`/messenger/v2/accounts/${userId}/chats`, { query: { limit: 100 }, account });
        for (const chat of Array.isArray(data?.chats) ? data.chats : []) {
          const chatId = cleanText(chat.id);
          const seenKey = `avito:${chatId}`;
          const updated = Number(chat.updated || 0);
          if (!chatId || state.seen[seenKey] === updated) continue;
          if (chatTimeMs(updated) < Date.now() - 3 * 24 * 3600_000) { seenUpdates[seenKey] = updated; continue; }
          if (cleanText(chat.context?.type) !== "item") { seenUpdates[seenKey] = updated; continue; }
          const itemTitle = cleanText(chat.context?.value?.title);
          const ours = Boolean(await avitoListingByTitle(itemTitle)) || CHAT_PERFUME_TITLE_RE.test(itemTitle);
          if (!ours) { seenUpdates[seenKey] = updated; continue; }
          const history = await avitoRequest(`/messenger/v3/accounts/${userId}/chats/${encodeURIComponent(chatId)}/messages/`, { query: { limit: 30 }, account });
          const messages = (Array.isArray(history?.messages) ? history.messages : []).slice().sort((a, b) => Number(a.created || 0) - Number(b.created || 0));
          const base = { marketplace: "avito", target: cleanText(account.id || "avito"), chatId, itemTitle, productName: itemTitle };
          const dealDone = messages.find((m) => cleanText(m.type) === "system" && isAvitoDealDoneMessage(m.content?.text) && chatTimeMs(Number(m.created || 0)) > Date.now() - 3 * 24 * 3600_000);
          if (dealDone) work.push({ ...base, key: `thank:avito:${chatId}`, thankYou: true, messageId: cleanText(dealDone.id), createdAt: new Date(chatTimeMs(Number(dealDone.created))).toISOString(), text: "" });
          // the latest message of a person (Avito's own system notes do not count)
          const last = messages.filter((m) => cleanText(m.type) !== "system").pop();
          if (last && cleanText(last.direction) === "in" && cleanText(last.type) === "text") {
            work.push({ ...base, key: `msg:avito:${cleanText(last.id)}`, messageId: cleanText(last.id), text: cleanText(last.content?.text), createdAt: new Date(chatTimeMs(Number(last.created || 0))).toISOString() });
          }
          seenUpdates[seenKey] = updated;
        }
      } catch (error) {
        warnings.push(`Avito ${account.id}: ${error?.message || "ошибка"}`);
      }
    }
  }

  if (settings.ozon) {
    for (const account of getOzonAccounts()) {
      try {
        const data = await ozonRequest("/v3/chat/list", { limit: 100, filter: { unread_only: true } }, account);
        for (const entry of data?.chats || data?.result?.chats || []) {
          const chat = entry.chat && typeof entry.chat === "object" ? entry.chat : entry;
          if (!/buyer/i.test(cleanText(chat.chat_type))) continue;
          const chatId = cleanText(chat.chat_id || entry.chat_id);
          const history = await ozonRequest("/v3/chat/history", { chat_id: chatId, limit: 20, direction: "Backward" }, account);
          const rows = (history?.messages || history?.result?.messages || []).slice().reverse();
          const last = rows.filter((m) => !["crm", "support", "system", "courier"].includes(cleanText(m.user?.type).toLowerCase())).pop();
          if (!last || cleanText(last.user?.type).toLowerCase() !== "customer") continue;
          const message = normalizeOzonChatMessage(last);
          if (!message.text) continue;
          const sku = cleanText(last.context?.sku || "");
          work.push({ marketplace: "ozon", target: account.id || "ozon", chatId, key: `msg:ozon:${message.id}`, messageId: message.id, text: message.text, createdAt: message.createdAt, sku });
        }
      } catch (error) {
        warnings.push(`Ozon ${account.id}: ${error?.message || "ошибка"}`);
      }
    }
  }

  if (settings.yandex) {
    const seenBusinesses = new Set();
    for (const shop of getYandexShops()) {
      if (!shop.businessId || seenBusinesses.has(String(shop.businessId))) continue;
      seenBusinesses.add(String(shop.businessId));
      try {
        const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats?limit=20`, { statuses: ["NEW", "WAITING_FOR_PARTNER"] });
        for (const chat of data?.result?.chats || []) {
          const chatId = cleanText(chat.chatId || chat.id);
          const orderId = cleanText(chat.context?.orderId || chat.orderId || "");
          const history = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats/history?chatId=${encodeURIComponent(chatId)}&limit=20`, {});
          const rows = (history?.result?.messages || []).slice().sort((a, b) => cleanText(a.createdAt).localeCompare(cleanText(b.createdAt)));
          const last = rows.filter((m) => ["CUSTOMER", "PARTNER"].includes(cleanText(m.sender).toUpperCase())).pop();
          if (!last || cleanText(last.sender).toUpperCase() !== "CUSTOMER") continue;
          const message = normalizeYandexChatMessage(last);
          if (!message.text) continue;
          work.push({ marketplace: "yandex", target: shop.id || "yandex", chatId, orderId, key: `msg:yandex:${message.id || `${chatId}:${message.createdAt}`}`, messageId: message.id, text: message.text, createdAt: message.createdAt });
        }
      } catch (error) {
        warnings.push(`Yandex ${shop.id}: ${error?.message || "ошибка"}`);
      }
    }
  }
  return { work, warnings, seenUpdates };
}

// ── one run ────────────────────────────────────────────────────────────────────
async function runChatAutopilot({ source = "schedule" } = {}) {
  if (chatAutopilotRunning) return { status: "already_running" };
  chatAutopilotRunning = true;
  const totals = { originality: 0, answered: 0, drafts: 0, thanked: 0, errors: 0 };
  try {
    const startedAt = await updateChatAutopilotState((state) => { state.startedAt = state.startedAt || new Date().toISOString(); return state.startedAt; });
    const state = await readChatAutopilotState();
    const { settings } = state;
    const aiSettings = await readEffectiveAiSettings();
    assertTextGenerationConfigured(aiSettings);
    const { work, warnings, seenUpdates } = await collectChatAutopilotWork(settings, state);
    // a message older than the autopilot itself (or than a day) is the people's business
    const freshFrom = Math.max(Date.parse(startedAt) - 30 * 60_000, Date.now() - CHAT_AUTOPILOT_MAX_AGE_MS);
    const pendingIds = new Set(state.pending.map((p) => p.id));
    let aiCalls = 0;
    // an Avito chat whose message was not dealt with (error, AI limit of the run) is read again next run
    const retryChat = (item) => { if (item.marketplace === "avito") delete seenUpdates[`avito:${item.chatId}`]; };
    for (const item of work) {
      if (state.handled[item.key] || pendingIds.has(item.key)) continue;
      const base = { marketplace: item.marketplace, target: item.target, chatId: item.chatId, product: item.productName || item.itemTitle || "" };
      try {
        if (item.thankYou) {
          if (!settings.thankYou) continue;
          const text = avitoThankYouText(item.chatId);
          await sendChatAutopilotMessage({ ...item, text });
          totals.thanked += 1;
          await updateChatAutopilotState((s) => {
            s.handled[item.key] = { at: new Date().toISOString(), action: "thank_you" };
            chatAutopilotLog(s, { ...base, kind: "thank_you", action: "sent", text });
          });
          continue;
        }
        if (chatTimeMs(item.createdAt) < freshFrom) continue;
        const pickup = item.marketplace !== "avito" && isPickupCheckQuestion(item.text);
        const originality = pickup || isOriginalityOnlyQuestion(item.text);
        if (originality) {
          if (!settings.originality) continue;
          const text = pickup
            ? pickupCheckAnswer({ marketplace: item.marketplace, target: item.target }).replace(/\nС уважением,[^\n]*$/, "")
            : await buildChatOriginalityAnswer(item, aiSettings);
          await sendChatAutopilotMessage({ ...item, text });
          totals.originality += 1;
          await updateChatAutopilotState((s) => {
            s.handled[item.key] = { at: new Date().toISOString(), action: pickup ? "auto_pickup" : "auto_originality" };
            chatAutopilotLog(s, { ...base, kind: "message", action: "sent", reason: pickup ? "проверка в пункте выдачи" : "оригинальность", question: item.text, text });
          });
          continue;
        }
        if (settings.productAnswers === "off" || !isChatProductQuestion(item.text)) continue;
        if (aiCalls >= CHAT_AUTOPILOT_AI_PER_RUN) { retryChat(item); continue; }
        aiCalls += 1;
        const { offerId, productName, facts, draft, check } = await draftChatAnswer(item, aiSettings);
        const passed = check.ok && check.answersQuestion;
        if (settings.productAnswers === "auto" && passed) {
          await sendChatAutopilotMessage({ ...item, text: draft });
          totals.answered += 1;
          await updateChatAutopilotState((s) => {
            s.handled[item.key] = { at: new Date().toISOString(), action: "auto_answer" };
            chatAutopilotLog(s, { ...base, product: productName || base.product, kind: "message", action: "sent", reason: "вопрос об аромате", question: item.text, text: draft });
          });
        } else {
          totals.drafts += 1;
          await updateChatAutopilotState((s) => {
            if (s.pending.some((p) => p.id === item.key)) return;
            s.pending.push({ id: item.key, ...item, productName: productName || item.productName || "", offerId, facts, draft, check, createdAt: new Date().toISOString() });
            chatAutopilotLog(s, { ...base, product: productName || base.product, kind: "message", action: "draft", question: item.text, check: passed ? "ok" : "issues" });
          });
        }
      } catch (error) {
        totals.errors += 1;
        retryChat(item);
        const message = error?.message || String(error);
        await updateChatAutopilotState((s) => {
          // Avito refuses messages to a closed chat / a blocked buyer: never retried
          if (/403|404|blocked|заблок|closed/i.test(message)) s.handled[item.key] = { at: new Date().toISOString(), action: "refused" };
          chatAutopilotLog(s, { ...base, kind: item.thankYou ? "thank_you" : "message", action: "error", question: item.text, error: message });
        });
      }
    }
    await updateChatAutopilotState((s) => {
      Object.assign(s.seen, seenUpdates);
      s.lastRun = { at: new Date().toISOString(), source, ...totals, warnings };
      s.runRequestedAt = null;
    });
    if (Object.values(totals).some(Boolean)) logger.info("chat_autopilot_run", { source, ...totals });
    return { status: "ok", ...totals, warnings };
  } catch (error) {
    await updateChatAutopilotState((s) => { s.lastRun = { at: new Date().toISOString(), source, error: error?.message || String(error), ...totals }; s.runRequestedAt = null; }).catch(() => {});
    logger.warn("chat autopilot run failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    chatAutopilotRunning = false;
  }
}

// The worker checks every 30 s: a run is due by the interval, or someone pressed «Проверить сейчас».
function scheduleChatAutopilot(delayMs = 30_000) {
  if (!chatAutopilotEnabled) return;
  if (chatAutopilotTimer) clearTimeout(chatAutopilotTimer);
  chatAutopilotTimer = setTimeout(async () => {
    try {
      const state = await readChatAutopilotState();
      const lastAt = Date.parse(state.lastRun?.at || 0) || 0;
      if (state.runRequestedAt || Date.now() - lastAt >= chatAutopilotIntervalMs) await runChatAutopilot({ source: state.runRequestedAt ? "manual" : "schedule" });
    } catch (error) {
      logger.warn("chat autopilot tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleChatAutopilot(30_000);
    }
  }, Math.max(10_000, Number(delayMs) || 30_000));
  chatAutopilotTimer.unref?.();
}

// ── admin API (under /api/chats → the «Чаты» page grant) ───────────────────────
app.get("/api/chats/autopilot", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readChatAutopilotState();
    const sent = state.log.filter((e) => e.action === "sent");
    const dayAgo = Date.now() - 24 * 3600_000;
    response.json({
      ok: true,
      enabled: chatAutopilotEnabled,
      intervalMinutes: Math.round(chatAutopilotIntervalMs / 60000),
      settings: state.settings,
      lastRun: state.lastRun,
      runRequestedAt: state.runRequestedAt,
      day: {
        originality: sent.filter((e) => Date.parse(e.at) > dayAgo && e.reason === "оригинальность").length,
        answered: sent.filter((e) => Date.parse(e.at) > dayAgo && e.reason === "вопрос об аромате").length,
        thanked: sent.filter((e) => Date.parse(e.at) > dayAgo && e.kind === "thank_you").length,
      },
      pending: state.pending.slice().sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt))),
      log: state.log.slice(-80).reverse(),
    });
  } catch (error) { next(error); }
});

app.post("/api/chats/autopilot/settings", requireAdmin, async (request, response, next) => {
  try {
    const patch = {};
    for (const key of ["avito", "ozon", "yandex", "originality", "thankYou"]) if (typeof request.body?.[key] === "boolean") patch[key] = request.body[key];
    if (["auto", "draft", "off"].includes(request.body?.productAnswers)) patch.productAnswers = request.body.productAnswers;
    const settings = await updateChatAutopilotState((state) => { state.settings = { ...state.settings, ...patch }; return state.settings; });
    await appendAudit(request, "chats.autopilot.settings", { newValue: settings });
    response.json({ ok: true, settings });
  } catch (error) { next(error); }
});

app.post("/api/chats/autopilot/run", requireAdmin, async (_request, response, next) => {
  try {
    await updateChatAutopilotState((state) => { state.runRequestedAt = new Date().toISOString(); });
    response.json({ ok: true, queued: true });
  } catch (error) { next(error); }
});

app.post("/api/chats/autopilot/pending/send", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.body?.id);
    const { pending } = await readChatAutopilotState();
    const item = pending.find((p) => p.id === id);
    if (!item) return response.status(404).json({ error: "Черновик не найден — возможно, покупателю уже ответили." });
    const text = cleanText(request.body?.text || item.draft);
    if (!text) return response.status(400).json({ error: "Пустой ответ." });
    await sendChatAutopilotMessage({ ...item, text });
    await updateChatAutopilotState((state) => {
      state.pending = state.pending.filter((p) => p.id !== id);
      state.handled[id] = { at: new Date().toISOString(), action: "draft_sent" };
      chatAutopilotLog(state, { kind: "message", action: "sent", reason: "проверено человеком", marketplace: item.marketplace, target: item.target, chatId: item.chatId, product: item.productName, question: item.text, text, by: requestUsername(request) });
    });
    await appendAudit(request, "chats.send", { entityType: "chat", entityId: `${item.marketplace}:${item.chatId}` });
    response.json({ ok: true });
  } catch (error) { next(error); }
});

app.post("/api/chats/autopilot/pending/redraft", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.body?.id);
    const { pending } = await readChatAutopilotState();
    const item = pending.find((p) => p.id === id);
    if (!item) return response.status(404).json({ error: "Черновик не найден." });
    const aiSettings = await readEffectiveAiSettings();
    assertTextGenerationConfigured(aiSettings);
    const fresh = await draftChatAnswer(item, aiSettings);
    const updated = await updateChatAutopilotState((state) => {
      const p = state.pending.find((x) => x.id === id);
      if (!p) return null;
      Object.assign(p, { facts: fresh.facts, draft: fresh.draft, check: fresh.check, redraftedAt: new Date().toISOString() });
      return p;
    });
    response.json({ ok: true, item: updated });
  } catch (error) {
    if (error.statusCode) return response.status(error.statusCode).json({ error: error.message });
    next(error);
  }
});

app.post("/api/chats/autopilot/pending/dismiss", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.body?.id);
    await updateChatAutopilotState((state) => {
      const item = state.pending.find((p) => p.id === id);
      state.pending = state.pending.filter((p) => p.id !== id);
      state.handled[id] = { at: new Date().toISOString(), action: "draft_dismissed" };
      if (item) chatAutopilotLog(state, { kind: "message", action: "dismissed", marketplace: item.marketplace, chatId: item.chatId, product: item.productName, question: item.text, by: requestUsername(request) });
    });
    response.json({ ok: true });
  } catch (error) { next(error); }
});
