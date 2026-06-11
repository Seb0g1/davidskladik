// Unified chats (messenger) for both marketplaces.
//
// Ozon: POST /v3/chat/list, /v3/chat/history, send /v1/chat/send/message, read /v2/chat/read.
// Yandex: POST /v2/businesses/{businessId}/chats, GET .../chats/history?chatId=,
//         POST .../chats/message?chatId= { message: { text } }.
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

function normalizeOzonChat(entry = {}, account = {}) {
  const chat = entry.chat && typeof entry.chat === "object" ? entry.chat : entry;
  return {
    id: `ozon:${cleanText(chat.chat_id || entry.chat_id)}`,
    marketplace: "ozon",
    target: account.id || "ozon",
    chatId: cleanText(chat.chat_id || entry.chat_id),
    type: cleanText(chat.chat_type || ""),
    status: cleanText(chat.chat_status || ""),
    unreadCount: Number(entry.unread_count ?? chat.unread_count ?? 0) || 0,
    lastMessageAt: cleanText(chat.last_message_at || entry.last_message_at || chat.created_at || ""),
    title: cleanText(chat.chat_type) === "Seller_Support" ? "Поддержка Ozon" : `Покупатель · ${cleanText(chat.chat_id || entry.chat_id).slice(0, 8)}`,
  };
}

function normalizeYandexChat(chat = {}, shop = {}) {
  const orderId = cleanText(chat.context?.orderId || chat.orderId || "");
  return {
    id: `yandex:${cleanText(chat.chatId || chat.id)}`,
    marketplace: "yandex",
    target: shop.id || "yandex",
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
          const data = await ozonRequest("/v3/chat/list", {
            limit: 500,
            filter: unreadOnly ? { unread_only: true } : {},
          }, account);
          const rows = data?.chats || data?.result?.chats || [];
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
    chats.sort((a, b) => {
      if ((b.unreadCount > 0) !== (a.unreadCount > 0)) return b.unreadCount > 0 ? 1 : -1;
      return cleanText(b.lastMessageAt).localeCompare(cleanText(a.lastMessageAt));
    });
    response.json({ ok: true, rows: chats.slice(0, 400), warnings });
  } catch (error) {
    next(error);
  }
});

function normalizeOzonChatMessage(message = {}) {
  const data = Array.isArray(message.data) ? message.data.join("\n") : cleanText(message.data || "");
  return {
    id: cleanText(message.message_id || message.id),
    author: cleanText(message.user?.type || message.user_type || ""),
    isSeller: ["seller", "Seller"].includes(cleanText(message.user?.type || message.user_type)),
    text: data || cleanText(message.text || ""),
    createdAt: cleanText(message.created_at || ""),
    isRead: message.is_read !== false,
  };
}

function normalizeYandexChatMessage(message = {}) {
  const sender = cleanText(message.sender || "");
  return {
    id: cleanText(message.messageId || message.id),
    author: sender,
    isSeller: sender === "PARTNER",
    text: cleanText(message.message || message.text || ""),
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
      return response.json({ ok: true, rows });
    }

    response.status(400).json({ error: "marketplace должен быть ozon или yandex." });
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
    if (!marketplace || !chatId || !text) return response.status(400).json({ error: "Нужны marketplace, chatId и text." });

    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
      if (!account) return response.status(400).json({ error: "Ozon аккаунт не найден." });
      const result = await ozonRequest("/v1/chat/send/message", { chat_id: chatId, text }, account);
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `ozon:${chatId}` });
      return response.json({ ok: true, result });
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target) || getYandexShops()[0];
      if (!shop?.businessId) return response.status(400).json({ error: "Yandex кабинет не найден." });
      const result = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats/message?chatId=${encodeURIComponent(chatId)}`, {
        message: { text },
      });
      await appendAudit(request, "chats.send", { entityType: "chat", entityId: `yandex:${chatId}` });
      return response.json({ ok: true, result });
    }

    response.status(400).json({ error: "marketplace должен быть ozon или yandex." });
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
