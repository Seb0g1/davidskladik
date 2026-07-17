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
  if (failures <= 0) return false;
  // Permanent errors (permission denied, feature not enabled) get very long backoff.
  // Every 200th tick ≈ 5 hours at 90-second poll interval.
  if (failures >= 100) return (failures - 100) % 200 !== 0;
  // After 5 consecutive transient failures: poll every ~15 minutes (1 in 10 ticks).
  return failures >= 5 && (failures - 5) % 10 !== 0;
}

function notifySourceResult(key, ok, { permanent = false } = {}) {
  if (ok) {
    notifySourceFailures.delete(key);
  } else {
    const prev = notifySourceFailures.get(key) || 0;
    // Permanent errors (e.g. PermissionDenied) jump straight to the high-backoff tier
    // so they don't spam the log every 90 seconds while still polling occasionally in
    // case the permission is later granted.
    notifySourceFailures.set(key, permanent ? Math.max(100, prev + 1) : prev + 1);
  }
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
        const items = (posting.products || []).map((item) => {
          const name = cleanText(item.name) || cleanText(item.offer_id);
          const qty = Math.max(1, Number(item.quantity || 1) || 1);
          return `${name} ×${qty}`;
        }).filter(Boolean);
        const totalQty = (posting.products || []).reduce((sum, item) => sum + (Number(item.quantity || 1) || 1), 0);
        await insertAppNotification({
          type: "order",
          marketplace: "ozon",
          externalId: cleanText(posting.posting_number),
          title: `Новый заказ Ozon № ${cleanText(posting.posting_number)} · ${totalQty} шт`,
          body: items.slice(0, 5).join(" · "),
          url: "/app/finance",
          eventAt: createdAt || null,
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
      const isPermissionDenied = /PermissionDenied|not available|permission denied/i.test(error?.message || "");
      notifySourceResult(key, false, { permanent: isPermissionDenied });
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
      const isPermissionDenied = /PermissionDenied|not available|permission denied/i.test(error?.message || "");
      notifySourceResult(key, false, { permanent: isPermissionDenied });
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
        const items = (order.items || []).map((item) => {
          const name = cleanText(item.offerName) || cleanText(item.offerId);
          const qty = Math.max(1, Number(item.count || 1) || 1);
          return `${name} ×${qty}`;
        }).filter(Boolean);
        const totalQty = (order.items || []).reduce((sum, item) => sum + (Number(item.count || 1) || 1), 0);
        await insertAppNotification({
          type: "order",
          marketplace: "yandex",
          externalId: String(id),
          title: `Новый заказ Яндекс № ${id} · ${totalQty} шт`,
          body: items.slice(0, 5).join(" · "),
          url: "/app/finance",
          eventAt: order.creationDate ? new Date(order.creationDate.split(" ")[0].split("-").reverse().join("-")).toISOString() : null,
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
          eventAt: cleanText(feedback.createdAt) || null,
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

// WB Token Bucket «global limiter per seller» ОБЩИЙ на все разделы API
// кабинета: опрос каждые 90 с продлевает штраф и валит даже синк цен и
// media-backfill (выяснено 2026-07-17: retry вырастал до ~48 мин). Поэтому
// WB-поллеры ходят не чаще раза в WB_NOTIFY_POLL_MINUTES, без ретраев
// (attempts: 1) и замолкают до конца штрафа по любому 429.
const wbNotifyMinIntervalMs = Math.max(5, Number(process.env.WB_NOTIFY_POLL_MINUTES || 15) || 15) * 60_000;
const wbNotifyLastRunAt = new Map();
let wbNotifyCooldownUntil = 0;

function wbNotifyShouldSkip(key) {
  if (Date.now() < wbNotifyCooldownUntil) return true;
  if (Date.now() - (wbNotifyLastRunAt.get(key) || 0) < wbNotifyMinIntervalMs) return true;
  return false;
}

function wbNotifyRegisterError(error) {
  if (Number(error?.statusCode) !== 429) return;
  const retrySec = Math.max(300, Number(error?.retryAfterSec || 0) || 0);
  wbNotifyCooldownUntil = Math.max(wbNotifyCooldownUntil, Date.now() + retrySec * 1000);
  logger.warn("wb notify pollers cooldown", { untilSec: retrySec });
}

async function pollWbOrderNotifications(state) {
  for (const account of getWbAccounts()) {
    const key = `wb-orders:${account.id}`;
    if (notifySourceShouldSkip(key) || wbNotifyShouldSkip(key)) continue;
    wbNotifyLastRunAt.set(key, Date.now());
    try {
      const data = await wbRequest(account, "marketplace", "GET", "/api/v3/orders/new", undefined, { attempts: 1 });
      const orders = Array.isArray(data?.orders) ? data.orders : [];
      for (const order of orders.slice(0, 50)) {
        const id = cleanText(order.id);
        if (!id) continue;
        const priceRub = Math.round(Number(order.convertedPrice || order.price || 0) / 100);
        await insertAppNotification({
          type: "order",
          marketplace: "wb",
          externalId: id,
          title: `Новый заказ WB № ${id}${priceRub ? ` · ${priceRub} ₽` : ""}`,
          body: cleanText(order.article || ""),
          url: "/app/finance",
          eventAt: cleanText(order.createdAt) || null,
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      wbNotifyRegisterError(error);
      notifySourceResult(key, false);
      logger.warn("notify poll wb orders failed", { account: account.id, status: error?.statusCode, detail: error?.message || String(error) });
    }
  }
}

async function pollWbReviewNotifications(state) {
  for (const account of getWbAccounts()) {
    const key = `wb-reviews:${account.id}`;
    if (notifySourceShouldSkip(key) || wbNotifyShouldSkip(key)) continue;
    wbNotifyLastRunAt.set(key, Date.now());
    try {
      const data = await wbRequest(account, "feedbacks", "GET", "/api/v1/feedbacks?isAnswered=false&take=50&skip=0&order=dateDesc", undefined, { attempts: 1 });
      for (const feedback of data?.data?.feedbacks || []) {
        const id = cleanText(feedback.id);
        if (!id) continue;
        await insertAppNotification({
          type: "review",
          marketplace: "wb",
          externalId: id,
          title: `Новый отзыв WB · ${Number(feedback.productValuation || 0)}★`,
          body: cleanText(feedback.text || feedback.pros || "").slice(0, 200),
          url: "/app/reviews",
          eventAt: cleanText(feedback.createdDate) || null,
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      wbNotifyRegisterError(error);
      // 403 — в токене нет категории «Вопросы и отзывы»: не спамим лог каждые 90 с.
      const isPermissionDenied = Number(error?.statusCode) === 403;
      notifySourceResult(key, false, { permanent: isPermissionDenied });
      logger.warn("notify poll wb reviews failed", { account: account.id, status: error?.statusCode, detail: error?.message || String(error) });
    }
  }
}

async function pollWbQuestionNotifications(state) {
  for (const account of getWbAccounts()) {
    const key = `wb-questions:${account.id}`;
    if (notifySourceShouldSkip(key) || wbNotifyShouldSkip(key)) continue;
    wbNotifyLastRunAt.set(key, Date.now());
    try {
      const data = await wbRequest(account, "feedbacks", "GET", "/api/v1/questions?isAnswered=false&take=50&skip=0&order=dateDesc", undefined, { attempts: 1 });
      for (const question of data?.data?.questions || []) {
        const id = cleanText(question.id);
        if (!id) continue;
        await insertAppNotification({
          type: "question",
          marketplace: "wb",
          externalId: id,
          title: "Новый вопрос WB",
          body: cleanText(question.text || "").slice(0, 200),
          url: "/app/questions",
          eventAt: cleanText(question.createdDate) || null,
        });
      }
      notifySourceResult(key, true);
    } catch (error) {
      wbNotifyRegisterError(error);
      const isPermissionDenied = Number(error?.statusCode) === 403;
      notifySourceResult(key, false, { permanent: isPermissionDenied });
      logger.warn("notify poll wb questions failed", { account: account.id, status: error?.statusCode, detail: error?.message || String(error) });
    }
  }
}

async function pollWbChatNotifications(state) {
  for (const account of getWbAccounts()) {
    const key = `wb-chats:${account.id}`;
    if (notifySourceShouldSkip(key) || wbNotifyShouldSkip(key)) continue;
    wbNotifyLastRunAt.set(key, Date.now());
    try {
      // Поток событий с курсором next: первый прогон молча запоминает хвост
      // (не спамим историей), дальше уведомляем о новых сообщениях покупателей.
      const storedNext = Number(state[key]?.next || 0) || 0;
      let next = storedNext;
      const clientEvents = [];
      for (let pageIndex = 0; pageIndex < 5; pageIndex += 1) {
        const suffix = next ? `?next=${encodeURIComponent(next)}` : "";
        const data = await wbRequest(account, "chat", "GET", `/api/v1/seller/events${suffix}`, undefined, { attempts: 1 });
        const result = data?.result || data || {};
        const batch = Array.isArray(result.events) ? result.events : [];
        for (const event of batch) {
          const sender = cleanText(event.sender || event.source || "").toLowerCase();
          if (!/seller|supplier/.test(sender)) clientEvents.push(event);
        }
        const nextCursor = Number(result.next || 0) || 0;
        if (!batch.length || !nextCursor || nextCursor === next) break;
        next = nextCursor;
      }
      if (storedNext) {
        for (const event of clientEvents.slice(0, 50)) {
          const chatId = cleanText(event.chatID || event.chatId);
          const eventId = cleanText(event.eventID || event.eventId || event.id);
          if (!chatId || !eventId) continue;
          const text = cleanText(event.message?.text || "");
          await insertAppNotification({
            type: "chat",
            marketplace: "wb",
            externalId: eventId,
            title: `Новое сообщение в чате WB${event.clientName ? ` · ${cleanText(event.clientName)}` : ""}`,
            body: text.slice(0, 200) || `Чат ${chatId}`,
            url: "/app/chats",
          });
        }
      }
      state[key] = { next };
      notifySourceResult(key, true);
    } catch (error) {
      wbNotifyRegisterError(error);
      const isPermissionDenied = Number(error?.statusCode) === 403;
      notifySourceResult(key, false, { permanent: isPermissionDenied });
      logger.warn("notify poll wb chats failed", { account: account.id, status: error?.statusCode, detail: error?.message || String(error) });
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
    await pollWbOrderNotifications(state);
    await pollOzonChatNotifications(state);
    await pollYandexChatNotifications(state);
    await pollWbChatNotifications(state);
    await pollOzonReviewNotifications(state);
    await pollYandexReviewNotifications(state);
    await pollWbReviewNotifications(state);
    await pollOzonQuestionNotifications(state);
    await pollWbQuestionNotifications(state);
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
