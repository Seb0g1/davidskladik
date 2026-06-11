// Marketplace event pollers (worker only): orders / chats / reviews / questions.
// Each source is best-effort: failures are logged and the source backs off, the rest
// keep working. Last-seen state survives restarts in data/notifications-state.json.

const notifyPollEnabled = process.env.NOTIFY_POLL_ENABLED !== "false";
const notifyPollIntervalMs = Math.max(45_000, Number(process.env.NOTIFY_POLL_SECONDS || 90) * 1000 || 90_000);
const notificationsStatePath = path.join(dataDir, "notifications-state.json");
let notifyPollTimer = null;
let notifyPollRunning = false;
const notifySourceFailures = new Map();

async function readNotificationsState() {
  try {
    return JSON.parse(await fs.readFile(notificationsStatePath, "utf8"));
  } catch {
    return {};
  }
}

async function writeNotificationsState(state) {
  await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
  await fs.writeFile(notificationsStatePath, JSON.stringify(state, null, 2)).catch(() => {});
}

function notifySourceShouldSkip(key) {
  const failures = notifySourceFailures.get(key) || 0;
  // After 5 consecutive failures poll the source only every ~15 minutes.
  return failures >= 5 && (failures - 5) % 10 !== 0;
}

function notifySourceResult(key, ok) {
  if (ok) notifySourceFailures.delete(key);
  else notifySourceFailures.set(key, (notifySourceFailures.get(key) || 0) + 1);
}

async function pollOzonOrderNotifications(state) {
  for (const account of getOzonAccounts()) {
    const key = `ozon-orders:${account.id}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const sinceIso = state[key]?.since || new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      const data = await ozonRequest("/v3/posting/fbs/list", {
        dir: "DESC",
        filter: { since: sinceIso, to: new Date().toISOString() },
        limit: 50,
        offset: 0,
        with: { analytics_data: false, financial_data: false },
      }, account);
      const postings = data?.result?.postings || [];
      let maxSeen = sinceIso;
      for (const posting of postings) {
        const createdAt = cleanText(posting.in_process_at || posting.created_at);
        if (createdAt && createdAt > maxSeen) maxSeen = createdAt;
        const products = (posting.products || []).map((item) => cleanText(item.offer_id)).filter(Boolean);
        await insertAppNotification({
          type: "order",
          marketplace: "ozon",
          externalId: cleanText(posting.posting_number),
          title: `Новый заказ Ozon ${cleanText(posting.posting_number)}`,
          body: products.slice(0, 5).join(", "),
          url: "/app/finance",
        });
      }
      state[key] = { since: maxSeen };
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll ozon orders failed", { account: account.id, detail: error?.message || String(error) });
    }
  }
}

async function pollOzonChatNotifications(state) {
  for (const account of getOzonAccounts()) {
    const key = `ozon-chats:${account.id}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await ozonRequest("/v3/chat/list", {
        filter: { unread_only: true },
        limit: 50,
      }, account);
      const chats = data?.chats || data?.result?.chats || [];
      for (const entry of chats) {
        const chat = entry.chat || entry;
        const chatId = cleanText(chat.chat_id || chat.chatId || entry.chat_id);
        const lastMessageId = cleanText(entry.last_message_id || chat.last_message_id || chat.last_message_unread_id || "");
        if (!chatId || !lastMessageId) continue;
        await insertAppNotification({
          type: "chat",
          marketplace: "ozon",
          externalId: `${chatId}:${lastMessageId}`,
          title: "Новое сообщение в чате Ozon",
          body: `Чат ${chatId} · непрочитанных: ${Number(entry.unread_count || chat.unread_count || 1)}`,
          url: "/app/warehouse",
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll ozon chats failed", { account: account.id, detail: error?.message || String(error) });
    }
  }
}

async function pollOzonReviewNotifications(state) {
  for (const account of getOzonAccounts()) {
    const key = `ozon-reviews:${account.id}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await ozonRequest("/v1/review/list", {
        limit: 50,
        sort_dir: "DESC",
        status: "UNPROCESSED",
      }, account);
      const reviews = data?.reviews || data?.result?.reviews || [];
      for (const review of reviews) {
        const id = cleanText(review.id || review.review_id);
        if (!id) continue;
        await insertAppNotification({
          type: "review",
          marketplace: "ozon",
          externalId: id,
          title: `Новый отзыв Ozon · ${Number(review.rating || 0)}★`,
          body: cleanText(review.text || "").slice(0, 200),
          url: "/app/warehouse",
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll ozon reviews failed", { account: account.id, detail: error?.message || String(error) });
    }
  }
}

async function pollOzonQuestionNotifications(state) {
  for (const account of getOzonAccounts()) {
    const key = `ozon-questions:${account.id}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await ozonRequest("/v1/question/list", {
        filter: { status: "NEW" },
      }, account);
      const questions = data?.questions || data?.result?.questions || [];
      for (const question of questions) {
        const id = cleanText(question.id || question.question_id);
        if (!id) continue;
        await insertAppNotification({
          type: "question",
          marketplace: "ozon",
          externalId: id,
          title: "Новый вопрос Ozon",
          body: cleanText(question.text || "").slice(0, 200),
          url: "/app/warehouse",
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll ozon questions failed", { account: account.id, detail: error?.message || String(error) });
    }
  }
}

async function pollYandexOrderNotifications(state) {
  for (const shop of getYandexShops()) {
    if (!shop.campaignId) continue;
    const key = `yandex-orders:${shop.id}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await yandexRequest(shop, "GET", `/v2/campaigns/${shop.campaignId}/orders?pageSize=50&page=1`);
      const orders = data?.orders || [];
      const lastSeenId = Number(state[key]?.lastOrderId || 0);
      let maxId = lastSeenId;
      for (const order of orders) {
        const id = Number(order.id || 0);
        if (!id) continue;
        if (id > maxId) maxId = id;
        if (lastSeenId && id <= lastSeenId) continue;
        const items = (order.items || []).map((item) => cleanText(item.offerName || item.offerId)).filter(Boolean);
        await insertAppNotification({
          type: "order",
          marketplace: "yandex",
          externalId: String(id),
          title: `Новый заказ Яндекс №${id}`,
          body: items.slice(0, 5).join(", "),
          url: "/app/finance",
        });
      }
      state[key] = { lastOrderId: maxId };
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll yandex orders failed", { shop: shop.id, detail: error?.message || String(error) });
    }
  }
}

async function pollYandexReviewNotifications(state) {
  const seenBusinesses = new Set();
  for (const shop of getYandexShops()) {
    if (!shop.businessId || seenBusinesses.has(String(shop.businessId))) continue;
    seenBusinesses.add(String(shop.businessId));
    const key = `yandex-reviews:${shop.businessId}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/goods-feedback`, {});
      const feedbacks = data?.result?.feedbacks || [];
      for (const feedback of feedbacks.slice(0, 50)) {
        const id = cleanText(feedback.feedbackId || feedback.id);
        if (!id) continue;
        if (feedback.needReaction === false) continue;
        await insertAppNotification({
          type: "review",
          marketplace: "yandex",
          externalId: id,
          title: `Новый отзыв Яндекс · ${Number(feedback.statistics?.rating || 0)}★`,
          body: cleanText(feedback.description?.advantages || feedback.description?.comment || "").slice(0, 200),
          url: "/app/warehouse",
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll yandex reviews failed", { shop: shop.id, detail: error?.message || String(error) });
    }
  }
}

async function pollYandexChatNotifications(state) {
  const seenBusinesses = new Set();
  for (const shop of getYandexShops()) {
    if (!shop.businessId || seenBusinesses.has(String(shop.businessId))) continue;
    seenBusinesses.add(String(shop.businessId));
    const key = `yandex-chats:${shop.businessId}`;
    if (notifySourceShouldSkip(key)) continue;
    try {
      const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/chats`, {});
      const chats = data?.result?.chats || [];
      for (const chat of chats.slice(0, 50)) {
        const chatId = cleanText(chat.chatId || chat.id);
        const updatedAt = cleanText(chat.updatedAt || "");
        if (!chatId) continue;
        const lastSeen = cleanText(state[key]?.byChat?.[chatId] || "");
        if (updatedAt && lastSeen && updatedAt <= lastSeen) continue;
        if (!state[key]) state[key] = { byChat: {} };
        if (!state[key].byChat) state[key].byChat = {};
        state[key].byChat[chatId] = updatedAt || new Date().toISOString();
        if (!lastSeen) continue; // first sight: remember silently, don't spam history
        await insertAppNotification({
          type: "chat",
          marketplace: "yandex",
          externalId: `${chatId}:${updatedAt}`,
          title: "Новое сообщение в чате Яндекс",
          body: `Чат ${chatId}`,
          url: "/app/warehouse",
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      notifySourceResult(key, false);
      logger.warn("notify poll yandex chats failed", { shop: shop.id, detail: error?.message || String(error) });
    }
  }
}

async function runNotificationPollCycle() {
  if (notifyPollRunning) return;
  notifyPollRunning = true;
  try {
    if (!(await ensureNotificationsTable().catch(() => false))) return;
    const state = await readNotificationsState();
    await pollOzonOrderNotifications(state);
    await pollYandexOrderNotifications(state);
    await pollOzonChatNotifications(state);
    await pollYandexChatNotifications(state);
    await pollOzonReviewNotifications(state);
    await pollYandexReviewNotifications(state);
    await pollOzonQuestionNotifications(state);
    await writeNotificationsState(state);
  } catch (error) {
    logger.warn("notification poll cycle failed", { detail: error?.message || String(error) });
  } finally {
    notifyPollRunning = false;
  }
}

function scheduleNotificationPolling(delayMs = notifyPollIntervalMs) {
  if (!notifyPollEnabled) return;
  if (notifyPollTimer) clearTimeout(notifyPollTimer);
  notifyPollTimer = setTimeout(async () => {
    try {
      await runNotificationPollCycle();
    } finally {
      scheduleNotificationPolling(notifyPollIntervalMs);
    }
  }, Math.max(10_000, Number(delayMs) || notifyPollIntervalMs));
  notifyPollTimer.unref?.();
}
