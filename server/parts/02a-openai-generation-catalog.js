async function createOpenAiJsonChat(messages = []) {
  const aiSettings = await readEffectiveAiSettings();
  assertTextGenerationConfigured(aiSettings);
  const client = getOpenAiClient(aiSettings);
  const request = {
    model: aiSettings.textModel || openaiTextModel,
    messages,
    temperature: 0.2,
    response_format: { type: "json_object" },
  };
  return createOpenAiChatCompletionWithFallback(client, request, {
    preferCompatible: shouldPreferCompatibleOpenAiChatRequest(aiSettings),
  });
}

async function imageBase64FromOpenAiImageResult(result) {
  const first = result?.data?.[0];
  if (!first) return "";
  if (typeof first.b64_json === "string" && first.b64_json.length) return first.b64_json;
  const url = typeof first.url === "string" ? first.url.trim() : "";
  if (!url) return "";
  const dataMatch = /^data:[^;]+;base64,([\s\S]+)$/i.exec(url);
  if (dataMatch) return dataMatch[1].replace(/\s+/g, "");
  if (/^https?:\/\//i.test(url)) {
    const response = await fetch(url);
    if (!response.ok) return "";
    return Buffer.from(await response.arrayBuffer()).toString("base64");
  }
  return "";
}

function normalizeOpenAiCompatibleBaseUrl(value) {
  const raw = cleanText(value).replace(/\/+$/u, "");
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    const host = parsed.hostname.toLowerCase();
    if (host === "codex.sale" || host.endsWith(".codex.sale")) {
      return `${parsed.origin}/v1`;
    }
    parsed.pathname = parsed.pathname
      .replace(/\/v1\/(?:chat\/completions|responses|images\/generations|images\/edits|models)$/iu, "/v1")
      .replace(/\/(?:chat\/completions|responses|images\/generations|images\/edits|models)$/iu, "");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString().replace(/\/+$/u, "");
  } catch (_error) {
    return raw
      .replace(/\/v1\/(?:chat\/completions|responses|images\/generations|images\/edits|models)$/iu, "/v1")
      .replace(/\/(?:chat\/completions|responses|images\/generations|images\/edits|models)$/iu, "");
  }
}

function openAiCompatibleImageBaseUrl(aiSettings = {}) {
  return normalizeOpenAiCompatibleBaseUrl(cleanText(aiSettings.baseUrl) || openaiBaseUrl || "https://api.openai.com/v1");
}

async function parseOpenAiCompatibleImageResponse(response) {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = summarizeApiErrorPayload(payload, `OpenAI image HTTP ${response.status}`);
    const error = new Error(message);
    error.statusCode = response.status >= 400 && response.status < 600 ? response.status : 502;
    error.code = cleanText(payload?.error?.code || payload?.code) || "openai_image_request_failed";
    error.error = payload?.error;
    throw error;
  }
  const imageBase64 = await imageBase64FromOpenAiImageResult(payload);
  if (!imageBase64) {
    const error = new Error("OpenAI-compatible image endpoint РЅРµ РІРµСЂРЅСѓР» b64_json РёР»Рё URL РёР·РѕР±СЂР°Р¶РµРЅРёСЏ.");
    error.statusCode = 502;
    error.code = "openai_image_empty";
    throw error;
  }
  return imageBase64;
}

async function fetchOpenAiCompatibleImageEdit(aiSettings, { prompt, sourceBuffer, sourceMimeType, sourceFileName }) {
  const apiKey = configuredAiApiKey(aiSettings);
  const model = effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings);
  const endpoint = `${openAiCompatibleImageBaseUrl(aiSettings)}/images/edits`;
  logger.info("ai image edit request", {
    provider: cleanText(aiSettings.providerId) || "openai-compatible",
    endpoint,
    model,
    size: aiSettings.imageSize || openaiImageSize,
    sourceMimeType: cleanText(sourceMimeType) || "image/png",
    promptLength: cleanText(prompt).length,
  });
  const form = new FormData();
  form.append("model", model);
  form.append("image", new Blob([sourceBuffer], { type: cleanText(sourceMimeType) || "image/png" }), sourceFileName || fileNameFromImageMime(sourceMimeType));
  form.append("prompt", prompt);
  form.append("size", aiSettings.imageSize || openaiImageSize);
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}` },
    body: form,
  });
  const imageBase64 = await parseOpenAiCompatibleImageResponse(response);
  logger.info("ai image edit response", {
    provider: cleanText(aiSettings.providerId) || "openai-compatible",
    endpoint,
    model,
    ok: true,
    bytesBase64: imageBase64.length,
  });
  return imageBase64;
}

async function fetchOpenAiCompatibleImageGeneration(aiSettings, { prompt }) {
  const apiKey = configuredAiApiKey(aiSettings);
  const model = effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings);
  const endpoint = `${openAiCompatibleImageBaseUrl(aiSettings)}/images/generations`;
  logger.info("ai image generation request", {
    provider: cleanText(aiSettings.providerId) || "openai-compatible",
    endpoint,
    model,
    size: aiSettings.imageSize || openaiImageSize,
    promptLength: cleanText(prompt).length,
  });
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      prompt,
      size: aiSettings.imageSize || openaiImageSize,
      response_format: "b64_json",
    }),
  });
  const imageBase64 = await parseOpenAiCompatibleImageResponse(response);
  logger.info("ai image generation response", {
    provider: cleanText(aiSettings.providerId) || "openai-compatible",
    endpoint,
    model,
    ok: true,
    bytesBase64: imageBase64.length,
  });
  return imageBase64;
}

async function fetchCodexSaleImage(aiSettings, imageOptions = {}) {
  if (!imageOptions?.sourceBuffer) {
    return fetchOpenAiCompatibleImageGeneration(aiSettings, imageOptions);
  }
  try {
    return await fetchOpenAiCompatibleImageEdit(aiSettings, imageOptions);
  } catch (error) {
    if (isOpenAiBillingLimitError(error)) throw error;
    if (imageOptions.allowGenerationFallback === false) throw error;
    logger.warn("codex sale image edit failed, trying generation endpoint", {
      detail: error?.message || String(error),
    });
    return fetchOpenAiCompatibleImageGeneration(aiSettings, imageOptions);
  }
}

async function fetchDirectOpenAiImageGeneration(client, aiSettings, prompt) {
  const request = {
    model: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
    prompt,
    size: aiSettings.imageSize || openaiImageSize,
    response_format: "b64_json",
  };
  const quality = cleanText(aiSettings.imageQuality || openaiImageQuality);
  if (quality && quality !== "auto") request.quality = quality;
  const result = await client.images.generate(request);
  return imageBase64FromOpenAiImageResult(result);
}

async function resizeOzonAiImageOutputBuffer(buffer, format) {
  if (!ozonAiImageTargetPx) return buffer;
  try {
    let pipeline = sharp(buffer).rotate();
    pipeline = pipeline.resize(ozonAiImageTargetPx, ozonAiImageTargetPx, { fit: "cover", position: "centre" });
    const fmt = cleanText(format).toLowerCase();
    if (fmt === "jpeg" || fmt === "jpg") return await pipeline.jpeg({ quality: 92, mozjpeg: true }).toBuffer();
    if (fmt === "webp") return await pipeline.webp({ quality: 92 }).toBuffer();
    return await pipeline.png({ compressionLevel: 9 }).toBuffer();
  } catch (error) {
    logger.warn("ozon ai image resize to target px failed, keeping original buffer", { detail: error?.message || String(error) });
    return buffer;
  }
}

function ozonAiImageStoredSizeLabel(aiSettings = {}) {
  return ozonAiImageTargetPx ? `${ozonAiImageTargetPx}x${ozonAiImageTargetPx}` : (cleanText(aiSettings.imageSize) || openaiImageSize);
}

function isOpenAiBillingLimitError(error = {}) {
  const detail = cleanText(error?.message || error?.error?.message || error?.detail);
  const code = cleanText(error?.code || error?.error?.code).toLowerCase();
  return Boolean(
    code === "openai_billing_limit"
      || /billing hard limit|hard limit has been reached|quota|insufficient_quota|billing limit/i.test(detail)
  );
}


