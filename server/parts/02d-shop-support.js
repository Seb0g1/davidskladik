// Support chat API — public endpoints for shop visitors, admin endpoints for staff
// Public:  POST /api/shop/support/chats, GET/POST /api/shop/support/chats/:token/messages
// Admin:   GET /api/support/chats, GET /api/support/chats/:id, POST /api/support/chats/:id/reply, PATCH /api/support/chats/:id/status
// Categories managed via app settings: settings.support.categories[]

const _supportCrypto = require("crypto");

function genSupportToken() {
  return _supportCrypto.randomBytes(32).toString("hex");
}

// ─── Rate-limit simple in-memory: max 5 chats per IP per hour ────────────────
const _supportIpRate = new Map();
function supportRateCheck(ip) {
  const now = Date.now();
  const entry = _supportIpRate.get(ip) || { count: 0, reset: now + 3600_000 };
  if (now > entry.reset) { entry.count = 0; entry.reset = now + 3600_000; }
  entry.count += 1;
  _supportIpRate.set(ip, entry);
  return entry.count <= 5;
}

// ─── Public: create chat ──────────────────────────────────────────────────────
app.post("/api/shop/support/chats", async (request, response, next) => {
  try {
    const ip = request.ip || request.socket?.remoteAddress || "unknown";
    if (!supportRateCheck(ip)) {
      return response.status(429).json({ error: "Слишком много обращений. Попробуйте позже." });
    }
    const name = cleanText(request.body?.name);
    const category = cleanText(request.body?.category);
    const firstMessage = cleanText(request.body?.message);
    if (!name) return response.status(400).json({ error: "Укажите ваше имя.", code: "name_required" });
    if (!category) return response.status(400).json({ error: "Выберите категорию вопроса.", code: "category_required" });
    if (!firstMessage) return response.status(400).json({ error: "Напишите ваш вопрос.", code: "message_required" });

    const prisma = getPrisma();
    const token = genSupportToken();
    const now = new Date();
    const chat = await prisma.supportChat.create({
      data: {
        sessionToken: token,
        visitorName: name.slice(0, 100),
        category: category.slice(0, 100),
        status: "open",
        unreadAdmin: true,
        unreadUser: false,
        lastMessageAt: now,
        messages: {
          create: { role: "user", body: firstMessage.slice(0, 4000) },
        },
      },
    });
    response.json({ ok: true, chatId: chat.id, token });
  } catch (error) {
    next(error);
  }
});

// ─── Public: poll messages (user uses token) ──────────────────────────────────
app.get("/api/shop/support/chats/:token/messages", async (request, response, next) => {
  try {
    const token = cleanText(request.params.token);
    if (!token) return response.status(400).json({ error: "Token required." });
    const prisma = getPrisma();
    const chat = await prisma.supportChat.findUnique({
      where: { sessionToken: token },
      include: { messages: { orderBy: { createdAt: "asc" } } },
    });
    if (!chat) return response.status(404).json({ error: "Чат не найден." });
    // Mark as read by user
    if (chat.unreadUser) {
      await prisma.supportChat.update({ where: { id: chat.id }, data: { unreadUser: false } });
    }
    response.json({
      ok: true,
      chatId: chat.id,
      status: chat.status,
      messages: chat.messages.map((m) => ({ id: m.id, role: m.role, body: m.body, createdAt: m.createdAt })),
    });
  } catch (error) {
    next(error);
  }
});

// ─── Public: user sends message ───────────────────────────────────────────────
app.post("/api/shop/support/chats/:token/messages", async (request, response, next) => {
  try {
    const token = cleanText(request.params.token);
    const body = cleanText(request.body?.message);
    if (!body) return response.status(400).json({ error: "Сообщение не может быть пустым." });
    if (body.length > 4000) return response.status(400).json({ error: "Сообщение слишком длинное." });
    const prisma = getPrisma();
    const chat = await prisma.supportChat.findUnique({ where: { sessionToken: token } });
    if (!chat) return response.status(404).json({ error: "Чат не найден." });
    if (chat.status === "closed") return response.status(409).json({ error: "Чат закрыт.", code: "chat_closed" });
    const now = new Date();
    const message = await prisma.supportMessage.create({
      data: { chatId: chat.id, role: "user", body: body.slice(0, 4000) },
    });
    await prisma.supportChat.update({
      where: { id: chat.id },
      data: { unreadAdmin: true, lastMessageAt: now, updatedAt: now },
    });
    response.json({ ok: true, message: { id: message.id, role: message.role, body: message.body, createdAt: message.createdAt } });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: list chats ────────────────────────────────────────────────────────
app.get("/api/support/chats", requireStaff, async (request, response, next) => {
  try {
    const status = cleanText(request.query.status || "open");
    const prisma = getPrisma();
    const where = status === "all" ? {} : { status };
    const chats = await prisma.supportChat.findMany({
      where,
      orderBy: { lastMessageAt: "desc" },
      take: 200,
      include: {
        messages: { orderBy: { createdAt: "desc" }, take: 1 },
      },
    });
    response.json({
      ok: true,
      chats: chats.map((c) => ({
        id: c.id,
        visitorName: c.visitorName,
        category: c.category,
        status: c.status,
        unreadAdmin: c.unreadAdmin,
        lastMessageAt: c.lastMessageAt,
        createdAt: c.createdAt,
        lastMessage: c.messages[0] ? { role: c.messages[0].role, body: c.messages[0].body } : null,
      })),
    });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: unread count (for badge) ─────────────────────────────────────────
app.get("/api/support/chats/unread-count", requireStaff, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const count = await prisma.supportChat.count({ where: { unreadAdmin: true, status: "open" } });
    response.json({ ok: true, count });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: get chat detail ───────────────────────────────────────────────────
app.get("/api/support/chats/:id", requireStaff, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const prisma = getPrisma();
    const chat = await prisma.supportChat.findUnique({
      where: { id },
      include: { messages: { orderBy: { createdAt: "asc" } } },
    });
    if (!chat) return response.status(404).json({ error: "Чат не найден." });
    // Mark as read by admin
    if (chat.unreadAdmin) {
      await prisma.supportChat.update({ where: { id }, data: { unreadAdmin: false } });
    }
    response.json({
      ok: true,
      chat: {
        id: chat.id,
        visitorName: chat.visitorName,
        category: chat.category,
        status: chat.status,
        createdAt: chat.createdAt,
        messages: chat.messages.map((m) => ({ id: m.id, role: m.role, body: m.body, createdAt: m.createdAt })),
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: reply ─────────────────────────────────────────────────────────────
app.post("/api/support/chats/:id/reply", requireStaff, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const body = cleanText(request.body?.message);
    if (!body) return response.status(400).json({ error: "Сообщение не может быть пустым." });
    const prisma = getPrisma();
    const chat = await prisma.supportChat.findUnique({ where: { id } });
    if (!chat) return response.status(404).json({ error: "Чат не найден." });
    if (chat.status === "closed") return response.status(409).json({ error: "Чат закрыт.", code: "chat_closed" });
    const now = new Date();
    const message = await prisma.supportMessage.create({
      data: { chatId: id, role: "admin", body: body.slice(0, 4000) },
    });
    await prisma.supportChat.update({
      where: { id },
      data: { unreadAdmin: false, unreadUser: true, lastMessageAt: now, updatedAt: now },
    });
    response.json({ ok: true, message: { id: message.id, role: message.role, body: message.body, createdAt: message.createdAt } });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: update status ─────────────────────────────────────────────────────
app.patch("/api/support/chats/:id/status", requireStaff, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const status = cleanText(request.body?.status);
    if (!["open", "closed"].includes(status)) {
      return response.status(400).json({ error: "status must be open or closed." });
    }
    const prisma = getPrisma();
    const chat = await prisma.supportChat.update({ where: { id }, data: { status } });
    response.json({ ok: true, id: chat.id, status: chat.status });
  } catch (error) {
    next(error);
  }
});

// ─── Admin: get support categories from settings ──────────────────────────────
app.get("/api/support/categories", requireStaff, async (request, response, next) => {
  try {
    const settings = await readAppSettings();
    response.json({ ok: true, categories: settings.support?.categories || defaultSupportCategories() });
  } catch (error) {
    next(error);
  }
});

app.put("/api/support/categories", requireAdmin, async (request, response, next) => {
  try {
    const raw = request.body?.categories;
    const categories = Array.isArray(raw)
      ? raw.map((c) => cleanText(c)).filter(Boolean).slice(0, 20)
      : defaultSupportCategories();
    const settings = await readAppSettings();
    await writeAppSettings({ ...settings, support: { ...(settings.support || {}), categories } });
    response.json({ ok: true, categories });
  } catch (error) {
    next(error);
  }
});

// ─── Public: get categories (for shop widget) ─────────────────────────────────
app.get("/api/shop/support/categories", async (request, response, next) => {
  try {
    const settings = await readAppSettings();
    response.json({ ok: true, categories: settings.support?.categories || defaultSupportCategories() });
  } catch (error) {
    next(error);
  }
});
