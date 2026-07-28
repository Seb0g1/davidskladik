// Unified chats (messenger) for all marketplaces.
//
// Ozon: POST /v3/chat/list, /v3/chat/history, send /v1/chat/send/message, read /v2/chat/read.
// Yandex: POST /v2/businesses/{businessId}/chats, GET .../chats/history?chatId=,
//         POST .../chats/message?chatId= { message: { text } }.
// WB (buyer-chat-api): GET /api/v1/seller/chats (чаты + replySign для отправки),
//         GET /api/v1/seller/events?next= — единый поток сообщений ВСЕХ чатов
//         (истории по одному чату у WB нет — фильтруем поток по chatID),
//         POST /api/v1/seller/message — multipart { replySign, message }.
//         Токен кабинета должен включать категорию «Чат с покупателями».
// Reply templates: data/chat-templates.json (same shape as review templates).

const chatTemplatesPath = path.join(dataDir, "chat-templates.json");

async function readChatTemplates() {
  try {
    const parsed = JSON.parse(await fs.readFile(chatTemplatesPath, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function writeChatTemplates(templates) {
  await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
  await fs.writeFile(chatTemplatesPath, JSON.stringify(templates, null, 2), "utf8");
}

function ozonChatTitle(chatType, chatId) {
  const type = cleanText(chatType).toLowerCase();
  if (type.includes("support")) return "Поддержка Ozon";
  if (type.includes("notification") || type.includes("news") || type.includes("system")) return "Новости Ozon";
  if (type.includes("courier") || type.includes("driver")) return "Курьер Ozon";
  return `Покупатель · ${cleanText(chatId).slice(0, 8)}`;
}

function normalizeOzonChat(entry = {}, account = {}) {
  const chat = entry.chat && typeof entry.chat === "object" ? entry.chat : entry;
  const chatId = cleanText(chat.chat_id || entry.chat_id);
  return {
    id: `ozon:${chatId}`,
    marketplace: "ozon",
    target: account.id || "ozon",
    chatId,
    type: cleanText(chat.chat_type || ""),
    status: cleanText(chat.chat_status || ""),
    unreadCount: Number(entry.unread_count ?? chat.unread_count ?? 0) || 0,
    lastMessageAt: cleanText(chat.last_message_at || entry.last_message_at || chat.created_at || ""),
    title: ozonChatTitle(chat.chat_type, chatId),
  };
}

// --- WB chat helpers ---

async function wbChatsListRaw(account) {
  // attempts: 1 — «global limiter per seller» WB штрафует за повторы, а UI
  // чатов не должен висеть в ретраях (nginx рубит запрос на 60-й секунде).
  const data = await wbRequest(account, "chat", "GET", "/api/v1/seller/chats", undefined, { attempts: 1 });
  const raw = data?.result?.chats || data?.result || data?.chats || [];
  return Array.isArray(raw) ? raw : [];
}

// Поток событий WB общий на все чаты и отдаётся только курсором next —
// сканируем целиком и кэшируем на минуту, чтобы переключение чатов и
// автообновление истории (каждые 15 с) не жгли лимиты API.
const wbChatEventsCache = new Map(); // account.id -> { at, events }

async function wbChatEventsAll(account) {
  const cacheKey = cleanText(account.id || "wb");
  const cached = wbChatEventsCache.get(cacheKey);
  if (cached && Date.now() - cached.at < 60_000) return cached.events;
  const events = [];
  const startedAt = Date.now();
  let next = 0;
  // Бюджет: не больше 12 страниц и ~15 секунд — иначе запрос UI упирается в
  // 60-секундный таймаут nginx, а лишние вызовы греют лимитер WB.
  for (let pageIndex = 0; pageIndex < 12 && Date.now() - startedAt < 15_000; pageIndex += 1) {
    const data = await wbRequest(account, "chat", "GET", `/api/v1/seller/events${next ? `?next=${encodeURIComponent(next)}` : ""}`, undefined, { attempts: 1 });
    const result = data?.result || data || {};
    const batch = Array.isArray(result.events) ? result.events : [];
    events.push(...batch);
    const nextCursor = Number(result.next || 0) || 0;
    if (!batch.length || !nextCursor || nextCursor === next) break;
    next = nextCursor;
  }
  wbChatEventsCache.set(cacheKey, { at: Date.now(), events });
  return events;
}

function wbChatEventTimestamp(event = {}) {
  const raw = event.addTimestamp ?? event.addTime ?? event.createdAt ?? "";
  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    // Секунды или миллисекунды — приводим к ISO.
    return new Date(numeric > 1e12 ? numeric : numeric * 1000).toISOString();
  }
  return cleanText(raw);
}

function normalizeWbChat(chat = {}, account = {}, lastMessageAtByChat = new Map()) {
  const chatId = cleanText(chat.chatID || chat.chatId || chat.id);
  const clientName = cleanText(chat.clientName || "");
  return {
    id: `wb:${chatId}`,
    marketplace: "wb",
    target: account.id || "wb",
    chatId,
    replySign: cleanText(chat.replySign || ""),
    type: "buyer",
    status: "",
    unreadCount: 0,
    lastMessageAt: lastMessageAtByChat.get(chatId) || "",
    title: clientName ? `Покупатель · ${clientName}` : `Чат ${chatId.slice(0, 8)}`,
  };
}

function normalizeWbChatMessage(event = {}) {
  const sender = cleanText(event.sender || event.source || "").toLowerCase();
  const isSeller = /seller|supplier/.test(sender);
  const message = event.message && typeof event.message === "object" ? event.message : {};
  const rawText = cleanText(message.text || (typeof event.message === "string" ? event.message : "") || event.text || "");
  // Вложения покупателя: images/attachments со ссылками — как у Ozon/Яндекса.
  const attachments = [];
  for (const item of [...(Array.isArray(message.images) ? message.images : []), ...(Array.isArray(message.attachments) ? message.attachments : [])]) {
    const url = cleanText(typeof item === "string" ? item : item?.url || item?.link || "");
    if (!url) continue;
    attachments.push(chatMediaAttachmentFromUrl(url, item?.name) || { type: "file", url, name: cleanText(item?.name) || "Файл" });
  }
  const extracted = extractChatMediaFromText(rawText);
  return {
    id: cleanText(event.eventID || event.eventId || event.id),
    author: isSeller ? "Вы" : (cleanText(event.clientName) || "Покупатель"),
    isSeller,
    text: extracted.text,
    attachments: [...attachments, ...extracted.attachments],
    createdAt: wbChatEventTimestamp(event),
    isRead: true,
  };
}

function normalizeYandexChat(chat = {}, shop = {}) {
  const orderId = cleanText(chat.context?.orderId || chat.orderId || "");
  return {
    id: `yandex:${cleanText(chat.chatId || chat.id)}`,
    marketplace: "yandex",
    target: shop.id || "yandex",
    orderId,
    chatId: cleanText(chat.chatId || chat.id),
    type: cleanText(chat.type || ""),
    status: cleanText(chat.status || ""),
    unreadCount: Number(chat.unreadMessageCount || 0) || 0,
    lastMessageAt: cleanText(chat.updatedAt || chat.createdAt || ""),
    title: orderId ? `Заказ №${orderId}` : `Чат ${cleanText(chat.chatId || chat.id)}`,
  };
}

app.get("/api/chats", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const unreadOnly = String(request.query.unread ?? "false") === "true";
    const chats = [];
    const warnings = [];

    if (marketplace === "all" || marketplace === "ozon") {
      for (const account of getOzonAccounts()) {
        try {
          const rows = [];
          let cursor = "";
          for (let pageIndex = 0; pageIndex < 5; pageIndex += 1) {
            const data = await ozonRequest("/v3/chat/list", {
              limit: 100,
              ...(cursor ? { cursor } : {}),
              filter: unreadOnly ? { unread_only: true } : {},
            }, account);
            const batch = data?.chats || data?.result?.chats || [];
            rows.push(...batch);
            cursor = cleanText(data?.cursor || data?.result?.cursor || "");
            const hasNext = data?.has_next ?? data?.result?.has_next;
            if (!batch.length || !cursor || hasNext === false) break;
          }
          chats.push(...rows.map((entry) => normalizeOzonChat(entry, account)));
        } catch (error) {
          warnings.push(`Ozon ${account.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    if (marketplace === "all" || marketplace === "yandex") {
      const seenBusinesses = new Set();
      for (const shop of getYandexShops()) {
        if (!shop.businessId || seenBusinesses.has(String(shop.businessId))) continue;
        seenBusinesses.add(String(shop.businessId));
        try {
          const rows = [];
          let pageToken = "";
          for (let pageIndex = 0; pageIndex < 10; pageIndex += 1) {
            const tokenPart = pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : "";
            const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats?limit=20${tokenPart}`, {});
            const batch = data?.result?.chats || [];
            rows.push(...batch);
            pageToken = cleanText(data?.result?.paging?.nextPageToken || "");
            if (!batch.length || !pageToken) break;
          }
          chats.push(...rows
            .map((chat) => normalizeYandexChat(chat, shop))
            .filter((chat) => !unreadOnly || chat.unreadCount > 0));
        } catch (error) {
          warnings.push(`Yandex ${shop.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    if (marketplace === "all" || marketplace === "wb") {
      for (const account of getWbAccounts()) {
        try {
          const rows = await wbChatsListRaw(account);
          // Время последнего сообщения берём из потока событий (best-effort).
          const lastMessageAtByChat = new Map();
          try {
            for (const event of await wbChatEventsAll(account)) {
              const chatId = cleanText(event.chatID || event.chatId);
              if (!chatId) continue;
              const at = wbChatEventTimestamp(event);
              if (at && at > (lastMessageAtByChat.get(chatId) || "")) lastMessageAtByChat.set(chatId, at);
            }
          } catch {
            /* список чатов важнее сортировки по времени */
          }
          // unreadOnly у WB не поддержан (API не отдаёт непрочитанность) — не режем список.
          chats.push(...rows.map((chat) => normalizeWbChat(chat, account, lastMessageAtByChat)));
        } catch (error) {
          warnings.push(`WB ${account.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    // Enrich: yandex chats reference an order — show the ordered product right in the list.
    const prisma = getPrisma();
    if (prisma) {
      const orderIdOf = (chat) => (chat.title.match(/№(\d+)/) || [])[1];
      const orderIds = Array.from(new Set(chats
        .filter((chat) => chat.marketplace === "yandex")
        .map(orderIdOf)
        .filter(Boolean)));
      if (orderIds.length) {
        const orders = await prisma.financeOrder.findMany({
          where: { marketplace: "yandex", orderId: { in: orderIds } },
          select: { orderId: true, productName: true },
        }).catch(() => []);
        const productByOrder = new Map();
        for (const order of orders) {
          if (order.productName && !productByOrder.has(order.orderId)) productByOrder.set(order.orderId, order.productName);
        }
        for (const chat of chats) {
          if (chat.marketplace !== "yandex") continue;
          const productName = productByOrder.get(orderIdOf(chat) || "");
          if (productName) chat.subtitle = String(productName).slice(0, 70);
        }
      }
    }
    if (marketplace === "all" || marketplace === "avito") {
      for (const account of getAvitoAccounts()) {
        try {
          const rows = await getAvitoChats(account, { unreadOnly });
          chats.push(...rows);
        } catch (error) {
          warnings.push(`Avito ${account.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    chats.sort((a, b) => {
      if ((b.unreadCount > 0) !== (a.unreadCount > 0)) return b.unreadCount > 0 ? 1 : -1;
      return cleanText(b.lastMessageAt).localeCompare(cleanText(a.lastMessageAt));
    });
    response.json({ ok: true, rows: chats.slice(0, 400), warnings });
  } catch (error) {
    next(error);
  }
});

const CHAT_IMAGE_EXT_RE = /\.(jpe?g|png|webp|gif|heic|bmp)(?:$|[?#])/i;
const CHAT_VIDEO_EXT_RE = /\.(mp4|mov|webm|m4v|avi)(?:$|[?#])/i;

function chatMediaAttachmentFromUrl(url, name = "") {
  const clean = cleanText(url);
  if (!/^https?:\/\//i.test(clean)) return null;
  let pathname = clean;
  try { pathname = new URL(clean).pathname; } catch { /* оставляем весь url */ }
  if (CHAT_VIDEO_EXT_RE.test(pathname)) return { type: "video", url: clean, name: cleanText(name) };
  if (CHAT_IMAGE_EXT_RE.test(pathname)) return { type: "image", url: clean, name: cleanText(name) };
  return null;
}

// Ozon присылает фото/видео покупателя ссылками прямо в тексте сообщения
// (голыми или markdown-ссылками) — выносим их во вложения, чтобы фронт
// показывал медиа, а не URL.
function extractChatMediaFromText(rawText) {
  const attachments = [];
  let text = String(rawText || "");
  text = text.replace(/\[([^\]\n]*)\]\((https?:\/\/[^)\s]+)\)/g, (full, label, url) => {
    const attachment = chatMediaAttachmentFromUrl(url, label);
    if (!attachment) return full;
    attachments.push(attachment);
    return "";
  });
  text = text.replace(/https?:\/\/[^\s"'<>]+/g, (url) => {
    const attachment = chatMediaAttachmentFromUrl(url);
    if (!attachment) return url;
    attachments.push(attachment);
    return "";
  });
  return {
    text: text.replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim(),
    attachments,
  };
}

function ozonChatAuthorLabel(userType) {
  const type = cleanText(userType).toLowerCase();
  if (type === "seller") return "Вы";
  if (type === "customer") return "Покупатель";
  if (type === "courier") return "Курьер";
  if (["crm", "support", "system"].includes(type)) return "Ozon";
  return cleanText(userType) || "Покупатель";
}

function normalizeOzonChatMessage(message = {}) {
  let text = "";
  const attachments = [];
  if (Array.isArray(message.data)) {
    for (const item of message.data) {
      if (typeof item === "string") {
        text += (text ? "\n" : "") + item;
      } else if (item && typeof item === "object") {
        const t = cleanText(item.type || "").toLowerCase();
        if (!t || t === "text") {
          text += (text ? "\n" : "") + cleanText(item.text || item.content || "");
        } else if (t === "video") {
          attachments.push({ type: "video", url: cleanText(item.url || item.uri || ""), previewUrl: cleanText(item.preview_url || "") });
        } else if (t === "image") {
          attachments.push({ type: "image", url: cleanText(item.url || item.uri || "") });
        } else if (t === "file") {
          attachments.push({ type: "file", url: cleanText(item.url || item.uri || ""), name: cleanText(item.name || item.file_name || "") });
        }
      }
    }
  } else {
    text = cleanText(message.data || "");
  }
  const userType = cleanText(message.user?.type || message.user_type || "");
  const extracted = extractChatMediaFromText(text || cleanText(message.text || ""));
  return {
    id: cleanText(message.message_id || message.id),
    author: ozonChatAuthorLabel(userType),
    isSeller: userType.toLowerCase() === "seller",
    text: extracted.text,
    attachments: [...attachments, ...extracted.attachments],
    createdAt: cleanText(message.created_at || ""),
    isRead: message.is_read !== false,
  };
}

function normalizeYandexChatMessage(message = {}) {
  const sender = cleanText(message.sender || "").toUpperCase();
  const authorLabel = sender === "PARTNER" ? "Вы"
    : sender === "CUSTOMER" ? "Покупатель"
    : sender === "MARKET" ? "Яндекс Маркет"
    : sender === "SUPPORT" ? "Поддержка"
    : (cleanText(message.sender) || "Покупатель");
  // Файлы покупателя приходят в payload: [{ name, url, size }] — фото и видео
  // показываем инлайн, остальное как ссылку на файл.
  const attachments = (Array.isArray(message.payload) ? message.payload : [])
    .map((file) => {
      const url = cleanText(file?.url);
      if (!url) return null;
      return chatMediaAttachmentFromUrl(url, file?.name)
        || { type: "file", url, name: cleanText(file?.name) || "Файл" };
    })
    .filter(Boolean);
  const extracted = extractChatMediaFromText(cleanText(message.message || message.text || ""));
  return {
    id: cleanText(message.messageId || message.id),
    author: authorLabel,
    isSeller: sender === "PARTNER",
    text: extracted.text,
    attachments: [...attachments, ...extracted.attachments],
    createdAt: cleanText(message.createdAt || ""),
    isRead: true,
  };
}

app.get("/api/chats/history", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace).toLowerCase();
    const target = cleanText(request.query.target);
    const chatId = cleanText(request.query.chatId);
    if (!marketplace || !chatId) return response.status(400).json({ error: "Нужны marketplace и chatId." });

    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
      if (!account) return response.status(400).json({ error: "Ozon аккаунт не найден." });
      const data = await ozonRequest("/v3/chat/history", {
        chat_id: chatId,
        limit: 100,
        direction: "Backward",
      }, account);
      const rows = (data?.messages || data?.result?.messages || []).map(normalizeOzonChatMessage).reverse();
      // mark as read up to the latest message
      const lastId = rows.length ? rows[rows.length - 1].id : "";
      if (lastId) {
        void ozonRequest("/v2/chat/read", { chat_id: chatId, from_message_id: Number(lastId) || lastId }, account)
          .catch(() => {});
      }
      // Context: customers reference the posting number in messages — resolve it to the
      // ordered product so the operator sees what the chat is about.
      let context = null;
      const postingMatch = rows.map((row) => (row.text || "").match(/(\d{7,10}-\d{3,5})(?:-\d+)?/)).find(Boolean);
      if (postingMatch) {
        const prismaContext = getPrisma();
        const order = prismaContext
          ? await prismaContext.financeOrder.findFirst({
            where: { postingNumber: { startsWith: postingMatch[1] } },
            select: { postingNumber: true, productName: true, offerId: true },
          }).catch(() => null)
          : null;
        context = {
          postingNumber: order?.postingNumber || postingMatch[0],
          productName: order?.productName || "",
          offerId: order?.offerId || "",
        };
      }
      return response.json({ ok: true, rows, context });
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target) || getYandexShops()[0];
      if (!shop?.businessId) return response.status(400).json({ error: "Yandex кабинет не найден." });
      const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats/history?chatId=${encodeURIComponent(chatId)}&limit=100`, {});
      const rows = (data?.result?.messages || []).map(normalizeYandexChatMessage);
      rows.sort((a, b) => cleanText(a.createdAt).localeCompare(cleanText(b.createdAt)));
      // Buyer name: order details expose the buyer for DBS orders.
      let context = null;
      const orderId = cleanText(request.query.orderId);
      if (orderId && shop.campaignId) {
        try {
          const orderData = await yandexRequest(shop, "GET", `/v2/campaigns/${shop.campaignId}/orders/${encodeURIComponent(orderId)}`);
          const buyer = orderData?.order?.buyer || {};
          const buyerName = [buyer.lastName, buyer.firstName, buyer.middleName].map(cleanText).filter(Boolean).join(" ");
          const item = (orderData?.order?.items || [])[0] || {};
          context = {
            orderId,
            buyerName,
            productName: cleanText(item.offerName || ""),
          };
        } catch {
          context = { orderId, buyerName: "", productName: "" };
        }
      }
      return response.json({ ok: true, rows, context });
    }

    if (marketplace === "wb") {
      const account = getWbAccountByTarget(target) || getWbAccounts()[0];
      if (!account) return response.status(400).json({ error: "Кабинет WB не найден." });
      const events = await wbChatEventsAll(account);
      const rows = events
        .filter((event) => cleanText(event.chatID || event.chatId) === chatId)
        .map(normalizeWbChatMessage)
        .filter((message) => message.text || message.attachments.length);
      rows.sort((a, b) => cleanText(a.createdAt).localeCompare(cleanText(b.createdAt)));
      return response.json({ ok: true, rows, context: null });
    }

    if (marketplace === "avito") {
      const account = getAvitoAccountByTarget(target) || getAvitoAccounts()[0];
      if (!account) return response.status(400).json({ error: "Avito аккаунт не найден." });
      const rows = await getAvitoChatHistory(account, chatId);
      return response.json({ ok: true, rows, context: null });
    }

    response.status(400).json({ error: "marketplace должен быть ozon, yandex, wb или avito." });
  } catch (error) {
    next(error);
  }
});

app.post("/api/chats/send", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.body?.marketplace).toLowerCase();
    const target = cleanText(request.body?.target);
    const chatId = cleanText(request.body?.chatId);
    const text = cleanText(request.body?.text);
    const imageUrls = Array.isArray(request.body?.imageUrls)
      ? request.body.imageUrls.map(cleanText).filter(Boolean)
      : [];
    if (!marketplace || !chatId || (!text && !imageUrls.length)) {
      return response.status(400).json({ error: "Нужны marketplace, chatId и text или imageUrls." });
    }

    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
      if (!account) return response.status(400).json({ error: "Ozon аккаунт не найден." });
      if (text) {
        await ozonRequest("/v1/chat/send/message", { chat_id: chatId, text }, account);
      }
      for (const imageUrl of imageUrls) {
        try {
          await ozonRequest("/v1/chat/send/message", { chat_id: chatId, type: "Images", data: [{ image_link: imageUrl }] }, account);
        } catch {
          await ozonRequest("/v1/chat/send/message", { chat_id: chatId, text: imageUrl }, account);
        }
      }
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `ozon:${chatId}` });
      return response.json({ ok: true });
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target) || getYandexShops()[0];
      if (!shop?.businessId) return response.status(400).json({ error: "Yandex кабинет не найден." });
      const fullText = [text, ...imageUrls].filter(Boolean).join("\n");
      const result = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats/message?chatId=${encodeURIComponent(chatId)}`, {
        message: { text: fullText },
      });
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `yandex:${chatId}` });
      return response.json({ ok: true, result });
    }

    if (marketplace === "wb") {
      const account = getWbAccountByTarget(target) || getWbAccounts()[0];
      if (!account) return response.status(400).json({ error: "Кабинет WB не найден." });
      // Отправка требует replySign чата: берём из запроса, иначе ищем в списке.
      let replySign = cleanText(request.body?.replySign);
      if (!replySign) {
        const chat = (await wbChatsListRaw(account))
          .find((item) => cleanText(item.chatID || item.chatId || item.id) === chatId);
        replySign = cleanText(chat?.replySign || "");
      }
      if (!replySign) return response.status(400).json({ error: "Чат WB не найден (нет replySign)." });
      const fullText = [text, ...imageUrls].filter(Boolean).join("\n").slice(0, 1000);
      const form = new FormData();
      form.append("replySign", replySign);
      form.append("message", fullText);
      const result = await wbRequest(account, "chat", "POST", "/api/v1/seller/message", form);
      // Сообщение появится в потоке событий — сбрасываем кэш, чтобы история обновилась.
      wbChatEventsCache.delete(cleanText(account.id || "wb"));
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `wb:${chatId}` });
      return response.json({ ok: true, result });
    }

    if (marketplace === "avito") {
      const account = getAvitoAccountByTarget(target) || getAvitoAccounts()[0];
      if (!account) return response.status(400).json({ error: "Avito аккаунт не найден." });
      if (text) {
        await sendAvitoMessage(account, chatId, text);
      }
      for (const imageUrl of imageUrls) {
        try {
          const imgRes = await fetch(imageUrl, { signal: AbortSignal.timeout(15000) });
          if (!imgRes.ok) throw new Error(`fetch image failed: ${imgRes.status}`);
          const buffer = Buffer.from(await imgRes.arrayBuffer());
          const fileName = imageUrl.split("/").pop()?.split("?")[0] || "image.jpg";
          const imageId = await uploadAvitoMessengerImage(account, buffer, fileName);
          await sendAvitoImageMessage(account, chatId, imageId);
        } catch (imgErr) {
          logger.warn("avito image upload/send failed, sending as text", { chatId, imageUrl, detail: imgErr?.message });
          await sendAvitoMessage(account, chatId, imageUrl);
        }
      }
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `avito:${chatId}` });
      return response.json({ ok: true });
    }

    response.status(400).json({ error: "marketplace должен быть ozon, yandex, wb или avito." });
  } catch (error) {
    next(error);
  }
});

app.get("/api/chats/templates", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ ok: true, templates: await readChatTemplates() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/chats/templates", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readChatTemplates();
    const title = cleanText(request.body?.title);
    const text = cleanText(request.body?.text);
    if (!title || !text) return response.status(400).json({ error: "Нужны title и text." });
    const id = cleanText(request.body?.id) || crypto.randomUUID();
    const existingIndex = templates.findIndex((item) => item.id === id);
    const template = { id, title: title.slice(0, 80), text: text.slice(0, 1500), updatedAt: new Date().toISOString() };
    if (existingIndex >= 0) templates[existingIndex] = template;
    else templates.push(template);
    await writeChatTemplates(templates);
    response.json({ ok: true, template, templates });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/chats/templates/:id", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readChatTemplates();
    const remaining = templates.filter((item) => item.id !== cleanText(request.params.id));
    await writeChatTemplates(remaining);
    response.json({ ok: true, templates: remaining });
  } catch (error) {
    next(error);
  }
});
