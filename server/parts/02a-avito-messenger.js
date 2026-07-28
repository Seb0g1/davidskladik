// Avito Messenger (v2 list, v3 messages) + Stats API + Balance + Profile.
// userId — числовой ID продавца, получается из /core/v1/accounts/self.

const avitoUserIdCache = new Map(); // clientId → { userId, expiresAt }

async function getAvitoUserId(account) {
  const clientId = cleanText(account?.clientId);
  if (!clientId) throw new Error("Avito account не настроен");
  const cached = avitoUserIdCache.get(clientId);
  if (cached && cached.expiresAt > Date.now()) return cached.userId;
  const data = await avitoRequest("/core/v1/accounts/self", { account });
  const userId = Number(data?.id || 0);
  if (!userId) throw new Error("Avito API не вернул user_id");
  avitoUserIdCache.set(clientId, { userId, expiresAt: Date.now() + 3_600_000 });
  return userId;
}

// v2 API: /messenger/v2/accounts/{user_id}/chats
function normalizeAvitoChat(chat = {}, account = {}) {
  const chatId = cleanText(String(chat.id || ""));
  const ctxValue = (chat.context?.value) || {};
  const title = cleanText(ctxValue.title || `Чат ${chatId.slice(0, 12)}`);
  // Последнее сообщение из last_message
  const lastMsg = chat.last_message || {};
  const lastText = cleanText(lastMsg.content?.text || "");
  const updated = Number(chat.updated || 0);
  // Покупатель — первый пользователь в массиве (не наш userId)
  return {
    id: `avito:${chatId}`,
    marketplace: "avito",
    target: cleanText(account.id || "avito"),
    chatId,
    type: cleanText(chat.context?.type || ""),
    status: "",
    unreadCount: Number(chat.unread_counter || 0) || 0,
    lastMessageAt: updated ? new Date(updated * 1000).toISOString() : "",
    title,
    subtitle: lastText ? lastText.slice(0, 80) : undefined,
  };
}

// v3 API: /messenger/v3/accounts/{user_id}/chats/{chat_id}/messages/
// Структура: { id, author_id, created, content: { text }, type, direction: "in"|"out", isRead }
function normalizeAvitoChatMessage(message = {}) {
  const isSeller = cleanText(message.direction || "") === "out";
  const content = message.content || {};
  let text = cleanText(content.text || "");
  const attachments = [];

  // Avito API: message.type is top-level ("text"|"image"|"link"|...), not content.type.
  const msgType = cleanText(message.type || content.type || "").toLowerCase();
  if (msgType === "image") {
    // content.image may be a flat size map {"1280x960":url,...} or nested {sizes:{...}}
    const img = content.image || {};
    const sizes = img.sizes || img;
    const url = cleanText(sizes["1280x960"] || sizes["640x480"] || sizes["100x75"] || img.url || "");
    if (url) attachments.push({ type: "image", url });
  } else if (msgType === "link" && content.link?.url) {
    const linkText = cleanText(content.link.text || content.link.url);
    text = `[${linkText}](${cleanText(content.link.url)})`;
  }
  // Images we sent as text (Avito API requires hosted image IDs for type:"image").
  // Detect upload URLs in text-only seller messages so they show as thumbnails.
  if (isSeller && !attachments.length && text && /^https?:\/\/.+\/uploads\/(?:images|ai-images)\/[^?#\s]+$/i.test(text)) {
    attachments.push({ type: "image", url: text });
    text = "";
  }

  const created = Number(message.created || 0);
  return {
    id: cleanText(String(message.id || "")),
    author: isSeller ? "Вы" : "Покупатель",
    isSeller,
    text,
    attachments,
    createdAt: created ? new Date(created * 1000).toISOString() : "",
    isRead: message.isRead !== false,
  };
}

async function getAvitoChats(account, { unreadOnly = false } = {}) {
  const userId = await getAvitoUserId(account);
  const query = { limit: 100 };
  if (unreadOnly) query.unread_only = true;
  // v2 для списка чатов
  const data = await avitoRequest(`/messenger/v2/accounts/${userId}/chats`, { query, account });
  return (Array.isArray(data?.chats) ? data.chats : []).map((c) => normalizeAvitoChat(c, account));
}

async function getAvitoChatHistory(account, chatId) {
  const userId = await getAvitoUserId(account);
  // v3 для истории сообщений
  const data = await avitoRequest(`/messenger/v3/accounts/${userId}/chats/${encodeURIComponent(chatId)}/messages/`, {
    query: { limit: 100 },
    account,
  });
  const messages = (Array.isArray(data?.messages) ? data.messages : [])
    .map(normalizeAvitoChatMessage)
    .reverse();
  // Помечаем прочитанным — fire-and-forget
  if (messages.length) {
    void avitoRequest(`/messenger/v3/accounts/${userId}/chats/${encodeURIComponent(chatId)}/read`, {
      method: "POST",
      body: {},
      account,
    }).catch(() => {});
  }
  return messages;
}

async function sendAvitoMessage(account, chatId, text) {
  const userId = await getAvitoUserId(account);
  // Avito Messenger API: send uses v1, read uses v3.
  return avitoRequest(`/messenger/v1/accounts/${userId}/chats/${encodeURIComponent(chatId)}/messages`, {
    method: "POST",
    body: { message: { text }, type: "text" },
    account,
  });
}

// Upload an image buffer to Avito Messenger hosting.
// Returns the image_id (the key from the response map).
async function uploadAvitoMessengerImage(account, imageBuffer, fileName) {
  const userId = await getAvitoUserId(account);
  const token = await getAvitoAccessToken(account);
  const timeoutMs = Math.max(5000, Number(process.env.AVITO_REQUEST_TIMEOUT_MS || 30000) || 30000);
  const form = new FormData();
  form.append("uploadfile[]", new Blob([imageBuffer]), fileName || "image.jpg");
  const response = await fetch(`${avitoBaseUrl}/messenger/v1/accounts/${userId}/uploadImages`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: form,
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  const data = parseApiResponse(text);
  if (!response.ok) {
    const message = data.error?.message || data.message || `Avito uploadImages error ${response.status}`;
    const error = new Error(message);
    error.statusCode = response.status;
    error.avito = data;
    throw error;
  }
  const imageId = Object.keys(data)[0];
  if (!imageId) throw new Error("Avito uploadImages: empty response");
  return imageId;
}

// Send an image message in Avito chat using a previously uploaded image_id.
async function sendAvitoImageMessage(account, chatId, imageId) {
  const userId = await getAvitoUserId(account);
  return avitoRequest(`/messenger/v1/accounts/${userId}/chats/${encodeURIComponent(chatId)}/messages/image`, {
    method: "POST",
    body: { image_id: imageId },
    account,
  });
}

async function getAvitoBalance(account) {
  const userId = await getAvitoUserId(account);
  // Ответ: { real: 118.92, bonus: 342.08 } — без обёртки result
  const data = await avitoRequest(`/core/v1/accounts/${userId}/balance/`, { account });
  const real = Number(data?.real ?? data?.result?.real ?? 0) || 0;
  const bonus = Number(data?.bonus ?? data?.result?.bonus ?? 0) || 0;
  return { real, bonus, total: real + bonus };
}

async function getAvitoMe(account) {
  const data = await avitoRequest("/core/v1/accounts/self", { account });
  return {
    id: Number(data?.id || 0),
    name: cleanText(data?.name || ""),
    email: cleanText(data?.email || ""),
    phone: cleanText(data?.phone || ""),
    avatar: cleanText(data?.photo || ""),
  };
}

async function getAvitoItemStats(account, itemIds, { dateFrom = "", dateTo = "" } = {}) {
  if (!itemIds?.length) return [];
  const userId = await getAvitoUserId(account);
  const from = dateFrom || new Date(Date.now() - 30 * 86400_000).toISOString().slice(0, 10);
  const to = dateTo || new Date().toISOString().slice(0, 10);
  const data = await avitoRequest(`/stats/v1/accounts/${userId}/items`, {
    method: "POST",
    body: {
      item_ids: itemIds.slice(0, 200),
      date_from: from,
      date_to: to,
      fields: ["views", "uniqViews", "contacts", "favorites"],
    },
    account,
  });
  return Array.isArray(data?.result) ? data.result : [];
}
