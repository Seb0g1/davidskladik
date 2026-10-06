// Feedback autopilot (owner's rules, 2026-10-06):
// • reviews rated 4–5 get an AI answer sent right away (signed by the cabinet's store: Magic Stick / AURA / Parfumerius);
// • a question only about originality gets a warm answer with emoji sent right away, no check;
// • any other question: the product's data is gathered (the Ozon card's attributes — country of manufacture,
//   type, volume… — and our perfume catalog), AI drafts the answer, a second AI pass checks it against that data,
//   and the draft waits on the Вопросы page for a person to send, edit or dismiss.
// Runs on the worker every FEEDBACK_AUTOPILOT_INTERVAL_MINUTES (20). State: data/feedback-autopilot.json —
// settings (each mode can be switched off), handled ids (an item is never answered twice), pending drafts, log.
// The api process only edits the state (settings, send/dismiss a draft, «run now»); every change re-reads the
// file first, so the worker and the api never overwrite each other's items.

const feedbackAutopilotPath = path.join(dataDir, "feedback-autopilot.json");
const feedbackAutopilotIntervalMs = Math.max(5, Number(process.env.FEEDBACK_AUTOPILOT_INTERVAL_MINUTES || 20) || 20) * 60_000;
const feedbackAutopilotEnabled = process.env.FEEDBACK_AUTOPILOT !== "false";
const FEEDBACK_AUTOPILOT_PER_RUN = Math.max(1, Number(process.env.FEEDBACK_AUTOPILOT_PER_RUN || 15) || 15);
const FEEDBACK_AUTOPILOT_DEFAULTS = { reviews: true, originality: true, questionDrafts: true };
let feedbackAutopilotTimer = null;
let feedbackAutopilotRunning = false;

async function readFeedbackAutopilotState() {
  let raw = {};
  try { raw = JSON.parse(await fs.readFile(feedbackAutopilotPath, "utf8")); } catch { raw = {}; }
  return {
    settings: { ...FEEDBACK_AUTOPILOT_DEFAULTS, ...(raw.settings || {}) },
    handled: raw.handled && typeof raw.handled === "object" ? raw.handled : {},
    pending: Array.isArray(raw.pending) ? raw.pending : [],
    log: Array.isArray(raw.log) ? raw.log : [],
    lastRun: raw.lastRun || null,
    runRequestedAt: raw.runRequestedAt || null,
  };
}

let feedbackAutopilotWriteChain = Promise.resolve();
/** Read the freshest state, apply fn, write it back — serialized inside the process. */
function updateFeedbackAutopilotState(fn) {
  const run = feedbackAutopilotWriteChain.then(async () => {
    const state = await readFeedbackAutopilotState();
    const result = await fn(state);
    const handledKeys = Object.keys(state.handled);
    if (handledKeys.length > 8000) {
      for (const key of handledKeys.sort((a, b) => String(state.handled[a].at).localeCompare(String(state.handled[b].at))).slice(0, handledKeys.length - 8000)) delete state.handled[key];
    }
    state.log = state.log.slice(-300);
    await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
    const tmp = `${feedbackAutopilotPath}.${process.pid}.tmp`;
    await fs.writeFile(tmp, JSON.stringify(state));
    await fs.rename(tmp, feedbackAutopilotPath);
    return result;
  });
  feedbackAutopilotWriteChain = run.catch(() => {});
  return run;
}

function feedbackAutopilotLog(state, entry) {
  state.log.push({ at: new Date().toISOString(), ...entry });
}

// ── what the question is about ─────────────────────────────────────────────────
const ORIGINALITY_RE = /(оригинал|подлинн|подделк|паленк|палён|фейк|копия|реплик|не\s+подделка|настоящ|original|fake)/i;
// anything beyond originality makes it a «real» question that needs data and a check
const OTHER_TOPIC_RE = /(стран|производ|изготов|срок|годност|дата|партия|бат[чк]|объ[её]м|мл\b|состав|нот[аыу]|аромат\s+как|стойк|шлейф|честн|маркир|коробк|упаков|тестер|миниат|пробник|отлив|доставк|наличи|скидк|цен[аы]|размер|подар|отличи|чем\s+отлича|похож|сезон|возраст|мужск|женск|унисекс|аллерг|чек|документ|сертификат|декларац)/i;

/** Pure: a question asking only whether the product is original. */
function isOriginalityOnlyQuestion(text = "") {
  const t = cleanText(text).toLowerCase();
  if (!t || !ORIGINALITY_RE.test(t)) return false;
  return !OTHER_TOPIC_RE.test(t.replace(ORIGINALITY_RE, " "));
}

// ── marketplace I/O ────────────────────────────────────────────────────────────
async function sendReviewReply({ marketplace, target, externalId, text }) {
  if (marketplace === "ozon") {
    const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
    if (!account) throw new Error("Ozon аккаунт не найден");
    return ozonRequest("/v1/review/comment/create", { review_id: externalId, text, mark_review_as_processed: true }, account);
  }
  if (marketplace === "yandex") {
    const shop = getYandexShopByTarget(target) || getYandexShops()[0];
    if (!shop?.businessId) throw new Error("Yandex кабинет не найден");
    return yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/goods-feedback/comments/update`, { feedbackId: Number(externalId) || externalId, comment: { text } });
  }
  if (marketplace === "wb") {
    const account = getWbAccountByTarget(target) || getWbAccounts()[0];
    if (!account) throw new Error("Кабинет WB не найден");
    return wbRequest(account, "feedbacks", "POST", "/api/v1/feedbacks/answer", { id: externalId, text: text.slice(0, 5000) });
  }
  throw new Error(`неизвестный маркетплейс ${marketplace}`);
}

async function sendQuestionAnswer({ marketplace, target, externalId, sku, text }) {
  if (marketplace === "wb") {
    const account = getWbAccountByTarget(target) || getWbAccounts()[0];
    if (!account) throw new Error("Кабинет WB не найден");
    return wbRequest(account, "feedbacks", "PATCH", "/api/v1/questions", { id: externalId, answer: { text: text.slice(0, 5000) }, state: "wbRu" });
  }
  const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
  if (!account) throw new Error("Ozon аккаунт не найден");
  return ozonRequest("/v1/question/answer/create", { question_id: externalId, ...(Number(sku) ? { sku: Number(sku) } : {}), text }, account);
}

async function feedbackAutopilotReviews() {
  const out = [];
  for (const account of getOzonAccounts()) {
    try {
      const data = await ozonRequest("/v1/review/list", { limit: 100, sort_dir: "DESC", status: "UNPROCESSED" }, account);
      out.push(...(data?.reviews || data?.result?.reviews || []).map((r) => normalizeOzonReview(r, account)));
    } catch (error) { logger.warn("feedback autopilot: ozon reviews", { account: account.id, detail: error?.message }); }
  }
  const seen = new Set();
  for (const shop of getYandexShops()) {
    if (!shop.businessId || seen.has(String(shop.businessId))) continue;
    seen.add(String(shop.businessId));
    try {
      const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/goods-feedback?limit=50`, { reactionStatus: "NEED_REACTION" });
      out.push(...(data?.result?.feedbacks || []).map((f) => normalizeYandexReview(f, shop)));
    } catch (error) { logger.warn("feedback autopilot: yandex reviews", { shop: shop.id, detail: error?.message }); }
  }
  for (const account of getWbAccounts()) {
    try {
      out.push(...(await listWbFeedbacks(account, { onlyUnanswered: true, limit: 100 })).map((f) => normalizeWbReview(f, account)));
    } catch (error) { logger.warn("feedback autopilot: wb reviews", { account: account.id, detail: error?.message }); }
  }
  return out.filter((r) => r.needsReply);
}

async function feedbackAutopilotQuestions() {
  const out = [];
  for (const account of getOzonAccounts()) {
    try {
      const data = await ozonRequest("/v1/question/list", { filter: { status: "NEW" } }, account);
      const rows = data?.questions || data?.result?.questions || [];
      const names = await resolveOzonSkuNames(rows.map((q) => q.sku), account);
      for (const q of rows) {
        if (Number(q.answers_count || 0) > 0) continue;
        out.push({ marketplace: "ozon", target: account.id || "ozon", externalId: cleanText(q.id), sku: cleanText(q.sku), offerId: "",
          productName: cleanText(q.product_name || "") || names.get(cleanText(q.sku)) || "", text: cleanText(q.text), createdAt: cleanText(q.published_at || "") });
      }
    } catch (error) { logger.warn("feedback autopilot: ozon questions", { account: account.id, detail: error?.message }); }
  }
  for (const account of getWbAccounts()) {
    try {
      const data = await wbRequest(account, "feedbacks", "GET", "/api/v1/questions?isAnswered=false&take=100&skip=0&order=dateDesc", undefined, { attempts: 1 });
      for (const q of data?.data?.questions || []) {
        const d = q.productDetails || {};
        out.push({ marketplace: "wb", target: account.id || "wb", externalId: cleanText(q.id), sku: cleanText(d.nmId || ""), offerId: cleanText(d.supplierArticle || ""),
          productName: cleanText(d.productName || ""), text: cleanText(q.text), createdAt: cleanText(q.createdDate || "") });
      }
    } catch (error) { logger.warn("feedback autopilot: wb questions", { account: account.id, detail: error?.message }); }
  }
  return out.filter((q) => q.externalId && q.text);
}

// ── product data for a question ────────────────────────────────────────────────
const FEEDBACK_SKIP_ATTR_RE = /(фото|изображ|видео|штрих|баркод|ссылк|rich|json|hashtag|хештег|ключев|seo|тнвэд|тн вэд|код\s+продав|артикул|партномер|таблица размер)/i;

/** Text facts about the product: Ozon card attributes by name (country, type, volume…) + our perfume catalog. */
async function collectQuestionFacts(question) {
  const facts = [];
  let offerId = cleanText(question.offerId);
  let account = question.marketplace === "ozon" ? (getOzonAccountByTarget(question.target) || getOzonAccounts()[0]) : getOzonAccounts()[0];
  try {
    let info = null;
    if (question.marketplace === "ozon" && Number(question.sku)) {
      const data = await ozonRequest("/v3/product/info/list", { sku: [Number(question.sku)] }, account);
      info = (data?.items || [])[0] || null;
      offerId = cleanText(info?.offer_id) || offerId;
    }
    if (offerId && account) {
      const attrs = await ozonRequest("/v4/product/info/attributes", { filter: { offer_id: [offerId], visibility: "ALL" }, limit: 1 }, account);
      const card = (attrs?.result || attrs?.items || [])[0];
      if (card) {
        const names = new Map((await ozonGetCategoryAttributes(account, card.description_category_id, card.type_id).catch(() => []))
          .map((a) => [Number(a.id), cleanText(a.name)]));
        for (const a of card.attributes || []) {
          const name = names.get(Number(a.id)) || "";
          const value = (a.values || []).map((v) => cleanText(v.value)).filter(Boolean).join(", ");
          if (!name || !value || FEEDBACK_SKIP_ATTR_RE.test(name)) continue;
          facts.push(`${name}: ${value.slice(0, Number(a.id) === 4191 ? 700 : 200)}`);
        }
      }
    }
  } catch (error) {
    logger.warn("feedback autopilot: product facts", { question: question.externalId, detail: error?.message });
  }
  try {
    const perfumeId = offerId ? await shopPerfumeIdForOffer(offerId) : 0;
    const catalog = perfumeId ? await shopCatalogNotes(perfumeId) : null;
    if (catalog) {
      const f = catalog.facts || {};
      if (catalog.topNotes.length) facts.push(`Верхние ноты: ${catalog.topNotes.join(", ")}`);
      if (catalog.middleNotes.length) facts.push(`Ноты сердца: ${catalog.middleNotes.join(", ")}`);
      if (catalog.baseNotes.length) facts.push(`Базовые ноты: ${catalog.baseNotes.join(", ")}`);
      if (catalog.accords.length) facts.push(`Аккорды: ${catalog.accords.join(", ")}`);
      if (f.year) facts.push(`Год выпуска аромата: ${f.year}`);
      if ((f.perfumers || []).length) facts.push(`Парфюмер: ${f.perfumers.join(", ")}`);
      if (f.gender) facts.push(`Для кого (каталог): ${f.gender}`);
    }
  } catch { /* catalog facts are optional */ }
  return { offerId, facts: facts.slice(0, 60) };
}

// ── AI ─────────────────────────────────────────────────────────────────────────
async function buildOriginalityAnswer(question, aiSettings) {
  const storeName = feedbackStoreName(question.marketplace, question.target);
  const fallback = `Здравствуйте! 🌸 Да, это 100% оригинальная продукция ✨ Мы работаем только с проверенными поставщиками и не продаём копии. Пусть аромат дарит вам радость! 💫\nС уважением, команда ${storeName}`;
  try {
    const completion = await createTextAiChat([
      { role: "system", content: `Ты — консультант магазина «${storeName}» на маркетплейсе ${feedbackMarketplaceLabel(question.marketplace)}. Покупатель спрашивает, оригинальный ли товар.
Напиши короткий тёплый ответ (2–3 предложения): да, это оригинальная продукция, мы работаем только с оригиналом. Добавь 2–3 уместных эмодзи (✨🌸💫🤍 и т.п.).
Не обещай документов, сертификатов и маркировки, не упоминай другие магазины, сайты и ссылки.
Последней строкой: «С уважением, команда ${storeName}». Верни только текст ответа.` },
      { role: "user", content: `${question.productName ? `Товар: «${question.productName}». ` : ""}Вопрос: «${question.text}»` },
    ], { json: false, temperature: 0.9, maxTokens: 220, aiSettings });
    const text = cleanText(completion.choices?.[0]?.message?.content || "");
    return text && !/https?:\/\/|www\./i.test(text) ? text : fallback;
  } catch {
    return fallback;
  }
}

async function buildQuestionAnswerWithFacts(question, facts, aiSettings) {
  const storeName = feedbackStoreName(question.marketplace, question.target);
  const completion = await createTextAiChat([
    { role: "system", content: `Ты — консультант магазина парфюмерии и косметики «${storeName}» на маркетплейсе ${feedbackMarketplaceLabel(question.marketplace)}. Ответь на вопрос покупателя о товаре.
Правила:
- Только по-русски, тепло и по делу, 1–4 предложения.
- Опирайся на «Данные о товаре». Если там есть ответ (например, страна-изготовитель) — дай его прямо.
- Если в данных ответа нет, а общеизвестного факта о товаре ты точно не знаешь — честно скажи и предложи уточнить в чате с продавцом. Ничего не выдумывай.
- Не упоминай другие магазины, сайты и ссылки, не обещай скидок.
- Последней строкой: «С уважением, команда ${storeName}». Верни только текст ответа.` },
    { role: "user", content: `Товар: «${question.productName || "—"}».\nДанные о товаре:\n${facts.length ? facts.join("\n") : "(нет данных)"}\n\nВопрос покупателя: «${question.text}»` },
  ], { json: false, temperature: 0.4, maxTokens: 350, aiSettings });
  return cleanText(completion.choices?.[0]?.message?.content || "");
}

/** Second pass: does the answer only state what the product data supports, and does it answer the question? */
async function verifyQuestionAnswer(question, facts, answer, aiSettings) {
  try {
    const completion = await createTextAiChat([
      { role: "system", content: `Ты проверяешь ответ магазина на вопрос покупателя. Верни JSON {"ok": boolean, "answersQuestion": boolean, "issues": [строки по-русски]}.
ok=false, если в ответе есть утверждение о товаре, которого нет в «Данных о товаре» и которое не является общеизвестным фактом, или если ответ противоречит данным. answersQuestion=false, если ответ не отвечает на вопрос.` },
      { role: "user", content: `Данные о товаре:\n${facts.length ? facts.join("\n") : "(нет данных)"}\n\nВопрос: «${question.text}»\n\nОтвет: «${answer}»` },
    ], { json: true, temperature: 0, maxTokens: 300, aiSettings });
    const parsed = JSON.parse(cleanText(completion.choices?.[0]?.message?.content || "{}"));
    return { ok: parsed.ok !== false, answersQuestion: parsed.answersQuestion !== false, issues: Array.isArray(parsed.issues) ? parsed.issues.map(cleanText).filter(Boolean).slice(0, 5) : [] };
  } catch (error) {
    return { ok: false, answersQuestion: false, issues: [`проверка не выполнена: ${error?.message || "ошибка ИИ"}`] };
  }
}

// ── one run ────────────────────────────────────────────────────────────────────
async function runFeedbackAutopilot({ source = "schedule" } = {}) {
  if (feedbackAutopilotRunning) return { status: "already_running" };
  feedbackAutopilotRunning = true;
  const totals = { reviewsAnswered: 0, originalityAnswered: 0, drafts: 0, errors: 0 };
  try {
    const { settings } = await readFeedbackAutopilotState();
    const aiSettings = await readEffectiveAiSettings();
    assertTextGenerationConfigured(aiSettings);

    if (settings.reviews) {
      const reviews = (await feedbackAutopilotReviews()).filter((r) => r.rating >= 4);
      const { handled } = await readFeedbackAutopilotState();
      for (const review of reviews.filter((r) => !handled[`review:${r.id}`]).slice(0, FEEDBACK_AUTOPILOT_PER_RUN)) {
        try {
          const text = await buildReviewReplyDraft({ ...review, reviewText: review.text }, aiSettings);
          if (!text) throw new Error("ИИ не вернул текст");
          await sendReviewReply({ ...review, text });
          totals.reviewsAnswered += 1;
          await updateFeedbackAutopilotState((state) => {
            state.handled[`review:${review.id}`] = { at: new Date().toISOString(), action: "auto_reply" };
            feedbackAutopilotLog(state, { kind: "review", action: "sent", marketplace: review.marketplace, target: review.target, rating: review.rating, product: review.productName, text });
          });
        } catch (error) {
          totals.errors += 1;
          await updateFeedbackAutopilotState((state) => feedbackAutopilotLog(state, { kind: "review", action: "error", marketplace: review.marketplace, product: review.productName, error: error?.message || String(error) }));
        }
      }
    }

    if (settings.originality || settings.questionDrafts) {
      const questions = await feedbackAutopilotQuestions();
      const openIds = new Set(questions.map((q) => `question:${q.marketplace}:${q.externalId}`));
      // a draft whose question got an answer elsewhere is no longer needed
      await updateFeedbackAutopilotState((state) => { state.pending = state.pending.filter((p) => openIds.has(p.id)); });
      const { handled, pending } = await readFeedbackAutopilotState();
      const pendingIds = new Set(pending.map((p) => p.id));
      let done = 0;
      for (const question of questions) {
        if (done >= FEEDBACK_AUTOPILOT_PER_RUN) break;
        const id = `question:${question.marketplace}:${question.externalId}`;
        if (handled[id] || pendingIds.has(id)) continue;
        const originality = isOriginalityOnlyQuestion(question.text);
        if (originality && !settings.originality) continue;
        if (!originality && !settings.questionDrafts) continue;
        done += 1;
        try {
          if (originality) {
            const text = await buildOriginalityAnswer(question, aiSettings);
            await sendQuestionAnswer({ ...question, text });
            totals.originalityAnswered += 1;
            await updateFeedbackAutopilotState((state) => {
              state.handled[id] = { at: new Date().toISOString(), action: "auto_originality" };
              feedbackAutopilotLog(state, { kind: "question", action: "sent", reason: "оригинальность", marketplace: question.marketplace, target: question.target, product: question.productName, question: question.text, text });
            });
          } else {
            const { offerId, facts } = await collectQuestionFacts(question);
            const draft = await buildQuestionAnswerWithFacts(question, facts, aiSettings);
            if (!draft) throw new Error("ИИ не вернул текст");
            const check = await verifyQuestionAnswer(question, facts, draft, aiSettings);
            totals.drafts += 1;
            await updateFeedbackAutopilotState((state) => {
              if (state.pending.some((p) => p.id === id)) return;
              state.pending.push({ id, ...question, offerId, facts, draft, check, createdAt: new Date().toISOString() });
              feedbackAutopilotLog(state, { kind: "question", action: "draft", marketplace: question.marketplace, product: question.productName, question: question.text, check: check.ok && check.answersQuestion ? "ok" : "issues" });
            });
          }
        } catch (error) {
          totals.errors += 1;
          await updateFeedbackAutopilotState((state) => feedbackAutopilotLog(state, { kind: "question", action: "error", marketplace: question.marketplace, product: question.productName, question: question.text, error: error?.message || String(error) }));
        }
      }
    }
    await updateFeedbackAutopilotState((state) => { state.lastRun = { at: new Date().toISOString(), source, ...totals }; state.runRequestedAt = null; });
    logger.info("feedback_autopilot_run", { source, ...totals });
    return { status: "ok", ...totals };
  } catch (error) {
    await updateFeedbackAutopilotState((state) => { state.lastRun = { at: new Date().toISOString(), source, error: error?.message || String(error), ...totals }; state.runRequestedAt = null; }).catch(() => {});
    logger.warn("feedback autopilot run failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    feedbackAutopilotRunning = false;
  }
}

// The worker checks every minute: a run is due by the interval, or someone pressed «Запустить сейчас» in the admin.
function scheduleFeedbackAutopilot(delayMs = 60_000) {
  if (!feedbackAutopilotEnabled) return;
  if (feedbackAutopilotTimer) clearTimeout(feedbackAutopilotTimer);
  feedbackAutopilotTimer = setTimeout(async () => {
    try {
      const state = await readFeedbackAutopilotState();
      const lastAt = Date.parse(state.lastRun?.at || 0) || 0;
      if (state.runRequestedAt || Date.now() - lastAt >= feedbackAutopilotIntervalMs) await runFeedbackAutopilot({ source: state.runRequestedAt ? "manual" : "schedule" });
    } catch (error) {
      logger.warn("feedback autopilot tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleFeedbackAutopilot(60_000);
    }
  }, Math.max(15_000, Number(delayMs) || 60_000));
  feedbackAutopilotTimer.unref?.();
}

// ── admin API ──────────────────────────────────────────────────────────────────
app.get("/api/feedback-autopilot", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readFeedbackAutopilotState();
    response.json({
      ok: true,
      enabled: feedbackAutopilotEnabled,
      intervalMinutes: Math.round(feedbackAutopilotIntervalMs / 60000),
      settings: state.settings,
      lastRun: state.lastRun,
      runRequestedAt: state.runRequestedAt,
      pending: state.pending.slice().sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt))),
      log: state.log.slice(-60).reverse(),
    });
  } catch (error) { next(error); }
});

app.post("/api/feedback-autopilot/settings", requireAdmin, async (request, response, next) => {
  try {
    const patch = {};
    for (const key of Object.keys(FEEDBACK_AUTOPILOT_DEFAULTS)) if (typeof request.body?.[key] === "boolean") patch[key] = request.body[key];
    const settings = await updateFeedbackAutopilotState((state) => { state.settings = { ...state.settings, ...patch }; return state.settings; });
    await appendAudit(request, "feedback.autopilot.settings", { newValue: settings });
    response.json({ ok: true, settings });
  } catch (error) { next(error); }
});

app.post("/api/feedback-autopilot/run", requireAdmin, async (request, response, next) => {
  try {
    await updateFeedbackAutopilotState((state) => { state.runRequestedAt = new Date().toISOString(); });
    response.json({ ok: true, queued: true });
  } catch (error) { next(error); }
});

app.post("/api/feedback-autopilot/pending/:id/send", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const { pending } = await readFeedbackAutopilotState();
    const item = pending.find((p) => p.id === id);
    if (!item) return response.status(404).json({ error: "Черновик не найден — возможно, на вопрос уже ответили." });
    const text = cleanText(request.body?.text || item.draft);
    if (!text) return response.status(400).json({ error: "Пустой ответ." });
    await sendQuestionAnswer({ ...item, text });
    await updateFeedbackAutopilotState((state) => {
      state.pending = state.pending.filter((p) => p.id !== id);
      state.handled[id] = { at: new Date().toISOString(), action: "draft_sent" };
      feedbackAutopilotLog(state, { kind: "question", action: "sent", reason: "проверено человеком", marketplace: item.marketplace, target: item.target, product: item.productName, question: item.text, text, by: requestUsername(request) });
    });
    await appendAudit(request, "questions.reply", { entityType: "question", entityId: `${item.marketplace}:${item.externalId}` });
    response.json({ ok: true });
  } catch (error) { next(error); }
});

app.post("/api/feedback-autopilot/pending/:id/dismiss", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    await updateFeedbackAutopilotState((state) => {
      const item = state.pending.find((p) => p.id === id);
      state.pending = state.pending.filter((p) => p.id !== id);
      state.handled[id] = { at: new Date().toISOString(), action: "draft_dismissed" };
      if (item) feedbackAutopilotLog(state, { kind: "question", action: "dismissed", marketplace: item.marketplace, product: item.productName, question: item.text, by: requestUsername(request) });
    });
    response.json({ ok: true });
  } catch (error) { next(error); }
});
