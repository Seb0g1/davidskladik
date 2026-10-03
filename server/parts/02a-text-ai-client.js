// Текстовый AI отдельно от картинок.
//
// Текст (описания, ответы на отзывы, ноты, правка карточек, AI-помощник) идёт в отдельный
// OpenAI-совместимый провайдер — сейчас наш прокси DeepSeek (ai.sebog1.ru). Картинки остаются на
// старом клиенте (getOpenAiClient / codex.sale) без изменений.
//
//   env:       AI_TEXT_BASE_URL, AI_TEXT_API_KEY, AI_TEXT_MODEL (деф. deepseek-chat),
//              AI_TEXT_TIMEOUT_MS (180000), AI_TEXT_CONCURRENCY (1), AI_TEXT_MIN_GAP_MS (2000)
//   настройки: ai.textBaseUrl / ai.textApiKey / ai.textProviderModel / ai.textFallback
//              (страница «Настройки» → AI; пустые поля → env)
//
// Прокси — это веб-чат: отвечает 2–60 с, ограничивает частоту, иногда теряет сессию (401). Поэтому:
// не больше AI_TEXT_CONCURRENCY запросов разом и пауза между ними, таймаут, 2 повтора с растущей
// паузой на 429/5xx/сеть, 401 — понятная ошибка без повторов. Не настроен — как раньше (старый
// провайдер). textFallback=true — при недоступности прокси уходим на старый провайдер.

// Ночное окно (по Москве, деф. 00:00–09:00, NIGHT_WORK_HOURS="0-9"): тяжёлая фоновая работа (конвейер,
// рендер, описания ИИ) идёт шире; днём — скромнее, сайт и магазин в приоритете.
function isNightWorkWindow(now = new Date()) {
  const [from, to] = String(process.env.NIGHT_WORK_HOURS || "0-9").split("-").map((v) => Number(v));
  const hour = (now.getUTCHours() + 3) % 24;
  return from <= to ? hour >= from && hour < to : hour >= from || hour < to;
}

/** Text AI parallel requests: AI_TEXT_CONCURRENCY pins it; else 4 at night, 2 by day. */
function textAiConcurrencyNow() {
  if (process.env.AI_TEXT_CONCURRENCY) return textAiDefaults.concurrency;
  return isNightWorkWindow() ? 4 : 2;
}

const textAiDefaults = {
  timeoutMs: Math.max(10_000, Number(process.env.AI_TEXT_TIMEOUT_MS || 180_000) || 180_000),
  concurrency: Math.max(1, Math.min(4, Number(process.env.AI_TEXT_CONCURRENCY || 1) || 1)),
  minGapMs: Math.max(0, Number(process.env.AI_TEXT_MIN_GAP_MS ?? 500) || 0),
  retries: 2,
};

function effectiveTextAiSettings(aiSettings = {}) {
  const baseUrl = normalizeOpenAiCompatibleBaseUrl(cleanText(aiSettings.textBaseUrl) || cleanText(process.env.AI_TEXT_BASE_URL));
  const apiKey = cleanText(aiSettings.textApiKey) || cleanText(process.env.AI_TEXT_API_KEY);
  const model = cleanText(aiSettings.textProviderModel) || cleanText(process.env.AI_TEXT_MODEL) || "deepseek-chat";
  return {
    configured: Boolean(baseUrl && apiKey),
    baseUrl,
    apiKey,
    model,
    fallback: aiSettings.textFallback === true,
  };
}

// Какой провайдер получит текстовый запрос: "text" (прокси) или "legacy" (общий ключ/адрес).
function resolveTextAiProvider(aiSettings = {}) {
  const text = effectiveTextAiSettings(aiSettings);
  if (text.configured) return { kind: "text", baseUrl: text.baseUrl, model: text.model, fallback: text.fallback };
  return { kind: "legacy", baseUrl: cleanText(aiSettings.baseUrl), model: cleanText(aiSettings.textModel) || openaiTextModel, fallback: false };
}

// ─── Очередь к прокси ────────────────────────────────────────────────────────

let textAiActive = 0;
let textAiLastStartedAt = 0;
const textAiWaiters = [];

async function withTextAiSlot(fn) {
  while (textAiActive >= textAiConcurrencyNow()) {
    await new Promise((resolve) => textAiWaiters.push(resolve));
  }
  textAiActive += 1;
  try {
    const gap = textAiDefaults.minGapMs - (Date.now() - textAiLastStartedAt);
    if (gap > 0) await sleep(gap);
    textAiLastStartedAt = Date.now();
    return await fn();
  } finally {
    textAiActive -= 1;
    const next = textAiWaiters.shift();
    if (next) next();
  }
}

function textAiErrorStatus(error = {}) {
  return Number(error?.status || error?.statusCode || error?.response?.status || 0);
}

function isRetryableTextAiError(error = {}) {
  const status = textAiErrorStatus(error);
  if (status === 429 || status >= 500) return true;
  if (status) return false;
  // network / timeout (OpenAI SDK: APIConnectionError, APIConnectionTimeoutError)
  return /timeout|timed out|ECONNRESET|ECONNREFUSED|ETIMEDOUT|socket|network|fetch failed|Connection error/i.test(cleanText(error?.message || error?.name));
}

function textAiAuthError(error = {}, baseUrl = "") {
  const host = (() => {
    try {
      return new URL(baseUrl).host;
    } catch {
      return baseUrl || "прокси";
    }
  })();
  const detail = cleanText(error?.error?.message || error?.message);
  const message = /proxy api key|invalid or missing/i.test(detail)
    ? `Неверный ключ текстового AI (AI_TEXT_API_KEY) для ${host}.`
    : `Слетела авторизация DeepSeek на ${host}, нужно обновить вход.`;
  const wrapped = new Error(message);
  wrapped.statusCode = 502;
  wrapped.code = "text_ai_auth";
  return wrapped;
}

function getTextAiClient(text) {
  return new OpenAI({ apiKey: text.apiKey, baseURL: text.baseUrl, timeout: textAiDefaults.timeoutMs, maxRetries: 0 });
}

async function runTextProviderChat(text, messages, { json, temperature, maxTokens }) {
  const client = getTextAiClient(text);
  const request = { model: text.model, messages };
  if (temperature !== undefined) request.temperature = temperature;
  if (json) request.response_format = { type: "json_object" };
  if (maxTokens) request.max_tokens = maxTokens;
  let lastError = null;
  for (let attempt = 0; attempt <= textAiDefaults.retries; attempt += 1) {
    try {
      return await withTextAiSlot(() => client.chat.completions.create(request));
    } catch (error) {
      lastError = error;
      if (textAiErrorStatus(error) === 401) throw textAiAuthError(error, text.baseUrl);
      if (!isRetryableTextAiError(error) || attempt === textAiDefaults.retries) break;
      const pause = 5000 * 3 ** attempt;
      logger.warn("text ai request retry", { status: textAiErrorStatus(error), attempt: attempt + 1, pauseMs: pause, detail: cleanText(error?.message).slice(0, 200) });
      await sleep(pause);
    }
  }
  const wrapped = new Error(`Текстовый AI не ответил: ${cleanText(lastError?.message) || "ошибка"}`);
  wrapped.statusCode = 502;
  wrapped.code = "text_ai_unavailable";
  wrapped.cause = lastError;
  throw wrapped;
}

async function runLegacyTextChat(aiSettings, messages, { json, temperature, maxTokens }) {
  assertTextGenerationConfigured(aiSettings);
  const client = getOpenAiClient(aiSettings);
  const request = { model: aiSettings.textModel || openaiTextModel, messages, temperature };
  if (json) request.response_format = { type: "json_object" };
  if (maxTokens) request.max_tokens = maxTokens;
  return createOpenAiChatCompletionWithFallback(client, request, {
    preferCompatible: shouldPreferCompatibleOpenAiChatRequest(aiSettings),
  });
}

// Единая точка для текстовых вызовов. Возвращает chat.completion (choices[0].message.content).
async function createTextAiChat(messages = [], { json = true, temperature = 0.2, maxTokens, aiSettings = null } = {}) {
  const settings = aiSettings || (await readEffectiveAiSettings());
  if (settings.enabled === false) {
    const error = new Error("AI-генерация выключена в настройках сайта.");
    error.statusCode = 400;
    error.code = "openai_disabled";
    throw error;
  }
  const text = effectiveTextAiSettings(settings);
  if (!text.configured) return runLegacyTextChat(settings, messages, { json, temperature, maxTokens });
  try {
    return await runTextProviderChat(text, messages, { json, temperature, maxTokens });
  } catch (error) {
    if (text.fallback && error.code !== "openai_disabled" && isOpenAiDirectConfigured(settings)) {
      logger.warn("text ai provider failed, falling back to the legacy provider", { detail: error?.message });
      return runLegacyTextChat(settings, messages, { json, temperature, maxTokens });
    }
    throw error;
  }
}

// JSON-ответ: снимает ```json-обёртку и мусор вокруг; пустой разбор — один повтор с напоминанием.
async function createTextAiJson(messages = [], options = {}) {
  const first = await createTextAiChat(messages, { ...options, json: true });
  let data = extractJsonObjectFromText(first?.choices?.[0]?.message?.content || "");
  if (data && Object.keys(data).length) return { data, completion: first };
  const retry = await createTextAiChat([
    ...messages,
    { role: "assistant", content: cleanText(first?.choices?.[0]?.message?.content).slice(0, 2000) || "(пусто)" },
    { role: "user", content: "Верни только валидный JSON-объект по заданной схеме, без пояснений и без ```." },
  ], { ...options, json: true });
  data = extractJsonObjectFromText(retry?.choices?.[0]?.message?.content || "");
  return { data: data || {}, completion: retry };
}

// ─── Текст для витрины ───────────────────────────────────────────────────────

// Описание с абзацами: пробелы внутри строк схлопываются, абзацы (пустая строка) сохраняются.
function normalizeParagraphText(value = "", maxLength = 6000) {
  const paragraphs = String(value || "")
    .replace(/\r\n?/g, "\n")
    .split(/\n\s*\n+/)
    .map((p) => p.replace(/[ \t\f\v]+/g, " ").replace(/\s*\n\s*/g, " ").trim())
    .filter(Boolean);
  let out = "";
  for (const paragraph of paragraphs) {
    const next = out ? `${out}\n\n${paragraph}` : paragraph;
    if (next.length > maxLength) break;
    out = next;
  }
  return out || cleanText(value).slice(0, maxLength);
}

// Ozon (аннотация) и Маркет понимают <br>/<p>, а голые переводы строк склеивают в один абзац.
function formatDescriptionForMarketplace(value = "", marketplace = "yandex") {
  const text = cleanText(value);
  if (!text || /<\s*(p|br|ul|ol|li|h\d|div)\b/i.test(text) || !/\n/.test(text)) return text;
  const paragraphs = normalizeParagraphText(text, 100_000).split("\n\n");
  const escape = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  if (marketplace === "ozon") return paragraphs.map(escape).join("<br/><br/>");
  return paragraphs.map((p) => `<p>${escape(p)}</p>`).join("");
}
