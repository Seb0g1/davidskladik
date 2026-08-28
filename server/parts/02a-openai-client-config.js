function readOpenAiRelayEnv() {
  return {
    url: cleanText(process.env.OPENAI_RELAY_URL),
    secret: cleanText(process.env.OPENAI_RELAY_SECRET),
  };
}

function isOpenAiRelayConfigured() {
  const relay = readOpenAiRelayEnv();
  return Boolean(relay.url && relay.secret);
}

function effectiveAiSettingsFromAppSettings(settings = {}) {
  const stored = normalizeAppSettings(settings).ai || {};
  return {
    ...stored,
    enabled: stored.enabled !== false,
    apiKey: configuredAiApiKey(stored),
    baseUrl: cleanText(stored.baseUrl) || openaiBaseUrl,
    textModel: cleanText(stored.textModel) || openaiTextModel,
    imageModel: effectiveOpenAiImageModel(stored.imageModel || openaiImageModel, stored),
    imageSize: cleanText(stored.imageSize) || openaiImageSize,
    imageQuality: cleanText(stored.imageQuality) || openaiImageQuality,
    imageFormat: cleanText(stored.imageFormat) || openaiImageFormat,
  };
}

function configuredAiEnvApiKey() {
  return cleanText(process.env.CODEX_LB_API_KEY)
    || cleanText(process.env.CODEX_SALE_API_KEY)
    || cleanText(process.env.OPENAI_API_KEY);
}

function configuredAiApiKey(aiSettings = {}) {
  return cleanText(aiSettings.apiKey) || configuredAiEnvApiKey();
}

async function readEffectiveAiSettings() {
  return effectiveAiSettingsFromAppSettings(await readAppSettings());
}

function forceCodexSaleAiImageSettings(aiSettings = {}) {
  return {
    ...aiSettings,
    providerId: "codexsale",
    baseUrl: "https://codex.sale/v1",
    imageModel: "gpt-image-2",
  };
}

function isOpenAiDirectConfigured(aiSettings = {}) {
  return Boolean(configuredAiApiKey(aiSettings));
}

function assertOpenAiRelayEnvPair() {
  const relay = readOpenAiRelayEnv();
  if (relay.url && !relay.secret) {
    const error = new Error("Задан OPENAI_RELAY_URL, но не задан OPENAI_RELAY_SECRET.");
    error.statusCode = 400;
    error.code = "openai_relay_secret_missing";
    throw error;
  }
  if (!relay.url && relay.secret) {
    const error = new Error("Задан OPENAI_RELAY_SECRET без OPENAI_RELAY_URL.");
    error.statusCode = 400;
    error.code = "openai_relay_url_missing";
    throw error;
  }
}

function assertImageGenerationConfigured(aiSettings = {}) {
  assertOpenAiRelayEnvPair();
  if (aiSettings.enabled === false) {
    const error = new Error("AI-генерация выключена в настройках сайта.");
    error.statusCode = 400;
    error.code = "openai_disabled";
    throw error;
  }
  if (isOpenAiRelayConfigured() || isOpenAiDirectConfigured(aiSettings)) return;
  const error = new Error(
    "Генерация недоступна: задайте AI API key в настройках сайта или OPENAI_API_KEY на сервере.",
  );
  error.statusCode = 400;
  error.code = "openai_not_configured";
  throw error;
}

function assertTextGenerationConfigured(aiSettings = {}) {
  if (aiSettings.enabled === false) {
    const error = new Error("AI-описание выключено в настройках сайта.");
    error.statusCode = 400;
    error.code = "openai_disabled";
    throw error;
  }
  if (isOpenAiDirectConfigured(aiSettings)) return;
  const error = new Error("AI-описание недоступно: задайте API key и Base endpoint в настройках сайта.");
  error.statusCode = 400;
  error.code = "openai_text_not_configured";
  throw error;
}

function getOpenAiClient(aiSettings = {}) {
  const apiKey = configuredAiApiKey(aiSettings);
  if (!apiKey) {
    const error = new Error("OPENAI_API_KEY не задан для прямого вызова OpenAI.");
    error.statusCode = 400;
    error.code = "openai_api_key_missing";
    throw error;
  }
  const options = { apiKey };
  const baseUrl = cleanText(aiSettings.baseUrl) || openaiBaseUrl;
  if (baseUrl) options.baseURL = baseUrl;
  return new OpenAI(options);
}

function extractJsonObjectFromText(text = "") {
  const raw = cleanText(text);
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (_error) {
    const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced?.[1]) {
      try {
        return JSON.parse(fenced[1]);
      } catch (_nestedError) {
        // fall through
      }
    }
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(raw.slice(start, end + 1));
      } catch (_nestedError) {
        // fall through
      }
    }
  }
  return {};
}

function isOpenAiRequestFormatError(error = {}) {
  const detail = cleanText(error?.message || error?.error?.message || error?.detail);
  const code = cleanText(error?.code || error?.error?.code).toLowerCase();
  const type = cleanText(error?.type || error?.error?.type).toLowerCase();
  const status = Number(error?.status || error?.statusCode || 0);
  return Boolean(
    /response_format|json_object|temperature|format|параметр|параметры|формат запроса|отклонил формат/i.test(detail)
      || code === "invalid_request_error"
      || type === "invalid_request_error"
      || status === 400
  );
}

function shouldPreferCompatibleOpenAiChatRequest(aiSettings = {}) {
  const providerId = cleanText(aiSettings.providerId).toLowerCase();
  const baseUrl = cleanText(aiSettings.baseUrl).toLowerCase();
  return providerId === "codexsale" || baseUrl.includes("codex.sale");
}

function isCodexSaleAiProvider(aiSettings = {}) {
  return shouldPreferCompatibleOpenAiChatRequest(aiSettings);
}

function openAiChatCompletionAttempts(request = {}, options = {}) {
  const cleanAttempt = (item = {}) => Object.fromEntries(Object.entries(item).filter(([, value]) => value !== undefined));
  const strict = cleanAttempt(request);
  const withoutJsonFormat = cleanAttempt({ ...request, response_format: undefined });
  const compatible = cleanAttempt({ ...request, response_format: undefined, temperature: undefined });
  const rawAttempts = options.preferCompatible ? [compatible, strict, withoutJsonFormat] : [strict, withoutJsonFormat, compatible];
  const seen = new Set();
  return rawAttempts.filter((attempt) => {
    const key = JSON.stringify(attempt);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function createOpenAiChatCompletionWithFallback(client, request = {}, options = {}) {
  const attempts = openAiChatCompletionAttempts(request, options);
  let lastError = null;
  for (const attempt of attempts) {
    try {
      return await client.chat.completions.create(attempt);
    } catch (error) {
      lastError = error;
      if (!isOpenAiRequestFormatError(error)) break;
    }
  }
  throw normalizeOpenAiImageError(lastError);
}

