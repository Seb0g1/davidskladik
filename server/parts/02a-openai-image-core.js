function normalizeOpenAiImageError(error) {
  const detail = cleanText(error?.message || error?.error?.message || error?.detail);
  if (/billing hard limit|hard limit has been reached|quota|insufficient_quota/i.test(detail)) {
    const billingError = new Error("Лимит AI-провайдера исчерпан на API-ключе. Пополните баланс или увеличьте hard limit в настройках Codex Sale/OpenAI, затем повторите генерацию.");
    billingError.statusCode = 402;
    billingError.code = "openai_billing_limit";
    return billingError;
  }
  if (error && Number.isFinite(Number(error.status)) && !Number.isFinite(Number(error.statusCode))) {
    error.statusCode = Number(error.status);
  }
  return error;
}

async function readAiLogoReference() {
  try {
    const buffer = await fs.readFile(aiImageLogoPath);
    return {
      sourceBuffer: buffer,
      sourceMimeType: imageMimeFromPath(aiImageLogoPath),
      sourceFileName: path.basename(aiImageLogoPath),
      payload: {
        base64: buffer.toString("base64"),
        mimeType: imageMimeFromPath(aiImageLogoPath),
        fileName: path.basename(aiImageLogoPath),
      },
    };
  } catch (_error) {
    return null;
  }
}

async function fetchOpenAiImageViaRelay({ prompt, sourceBuffer, sourceMimeType, referenceImages = [] }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), openaiRelayTimeoutMs);
  try {
    const body = {
      prompt,
      sourceImageBase64: sourceBuffer.toString("base64"),
      sourceMimeType: cleanText(sourceMimeType) || "image/png",
      model: openaiImageModel,
      size: openaiImageSize,
      quality: openaiImageQuality,
      output_format: openaiImageFormat,
    };
    if (referenceImages.length) body.referenceImages = referenceImages;
    if (openAiImageSupportsInputFidelity(openaiImageModel)) body.input_fidelity = "high";
    if (openaiImageConfig && typeof openaiImageConfig === "object") body.image_config = openaiImageConfig;
    const relay = readOpenAiRelayEnv();
    const response = await fetch(relay.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${relay.secret}`,
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = cleanText(payload.detail || payload.error) || `Relay HTTP ${response.status}`;
      const error = new Error(message);
      error.statusCode = Number.isFinite(Number(payload.statusCode)) ? Number(payload.statusCode) : (response.status >= 400 && response.status < 600 ? response.status : 502);
      error.code = cleanText(payload.code) || "openai_relay_error";
      throw normalizeOpenAiImageError(error);
    }
    const imageBase64 = payload.b64_json || payload.imageBase64;
    if (!imageBase64) {
      const error = new Error("Relay не вернул изображение (пустой b64_json).");
      error.statusCode = 502;
      error.code = "openai_relay_empty";
      throw error;
    }
    return imageBase64;
  } catch (error) {
    if (error?.name === "AbortError") {
      const timeoutError = new Error(`Таймаут relay OpenAI (${Math.round(openaiRelayTimeoutMs / 1000)} с). Увеличьте OPENAI_RELAY_TIMEOUT_MS или проверьте сеть.`);
      timeoutError.statusCode = 504;
      timeoutError.code = "openai_relay_timeout";
      throw timeoutError;
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function generateOzonAiImageDraftFromPromptOnly(product, { prompt, sourceImageUrl = "", batchId, variantIndex = 1, variantTotal = 1, presetId = "", presetLabel = "", skipPackshotRules = false }, request) {
  const aiSettings = await readEffectiveAiSettings();
  assertImageGenerationConfigured(aiSettings);
  const sourceHint = cleanText(sourceImageUrl)
    ? `\n\nReference product image URL for context: ${cleanText(sourceImageUrl)}. Generate a new marketplace-ready image; do not copy watermarks or UI elements from the source.`
    : "";
  const generatedPrompt = buildOzonAiImagePrompt(product, `${cleanText(prompt)}${sourceHint}`, { variantIndex, variantTotal, skipPackshotRules });
  let imageBase64;
  try {
    if (isCodexSaleAiProvider(aiSettings)) {
      imageBase64 = await fetchCodexSaleImage(aiSettings, { prompt: generatedPrompt });
    } else {
      const client = getOpenAiClient(aiSettings);
      imageBase64 = await fetchDirectOpenAiImageGeneration(client, aiSettings, generatedPrompt);
    }
  } catch (error) {
    throw normalizeOpenAiImageError(error);
  }
  if (!imageBase64) {
    const error = new Error("OpenAI РЅРµ РІРµСЂРЅСѓР» РёР·РѕР±СЂР°Р¶РµРЅРёРµ. РџРѕРїСЂРѕР±СѓР№С‚Рµ РїРѕРІС‚РѕСЂРёС‚СЊ РіРµРЅРµСЂР°С†РёСЋ.");
    error.statusCode = 502;
    error.code = "openai_image_empty";
    throw error;
  }

  let outBuffer = Buffer.from(imageBase64, "base64");
  outBuffer = await resizeOzonAiImageOutputBuffer(outBuffer, aiSettings.imageFormat || openaiImageFormat);

  await fs.mkdir(aiImageDir, { recursive: true });
  const extension = aiImageExtension(aiSettings.imageFormat || openaiImageFormat);
  const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}${extension}`;
  const filePath = path.join(aiImageDir, fileName);
  await fs.writeFile(filePath, outBuffer);

  const relativeUrl = `/uploads/ai-images/${fileName}`;
  return normalizeAiImageDraft({
    status: "pending",
    prompt: generatedPrompt,
    productName: product.name || product.ozon?.name,
    sourceImageUrl: cleanText(sourceImageUrl),
    resultUrl: `${uploadBaseUrl(request)}${relativeUrl}`,
    batchId,
    variantIndex,
    variantTotal,
    presetId: cleanText(presetId),
    presetLabel: cleanText(presetLabel),
    model: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
    size: ozonAiImageStoredSizeLabel(aiSettings),
    quality: aiSettings.imageQuality || openaiImageQuality,
    format: aiSettings.imageFormat || openaiImageFormat,
  });
}

async function generateOzonAiImageDraft(product, options = {}, request) {
  const { prompt, sourceImageUrl, batchId, variantIndex = 1, variantTotal = 1, requireSourceImage = false, allowGenerationFallback = true, forceCodexSale = false, skipPackshotRules = false } = options;
  const hasExplicitSource = Object.prototype.hasOwnProperty.call(options, "sourceImageUrl");
  const sourceUrl = hasExplicitSource
    ? cleanText(sourceImageUrl)
    : (cleanText(sourceImageUrl) || firstImageUrl(product.ozon?.primaryImage || product.ozon?.images || product.imageUrl));
  if (!sourceUrl) {
    if (requireSourceImage) {
      const error = new Error("Для генерации через Codex нужно исходное фото товара. Добавьте фото в карточку или загрузите его перед генерацией.");
      error.statusCode = 400;
      error.code = "source_image_required";
      throw error;
    }
    return generateOzonAiImageDraftFromPromptOnly(product, { prompt, batchId, variantIndex, variantTotal, presetId: options.presetId, presetLabel: options.presetLabel, skipPackshotRules }, request);
  }

  let aiSettings = await readEffectiveAiSettings();
  if (forceCodexSale) aiSettings = forceCodexSaleAiImageSettings(aiSettings);
  assertImageGenerationConfigured(aiSettings);

  const sourcePath = localPublicFilePathFromUrl(sourceUrl, request);
  let sourceBuffer;
  let sourceFileName;
  let sourceMimeType;
  if (sourcePath) {
    sourceBuffer = await fs.readFile(sourcePath);
    sourceMimeType = imageMimeFromPath(sourcePath);
    sourceFileName = path.basename(sourcePath);
  } else if (/^https?:\/\//i.test(sourceUrl)) {
    const sourceResponse = await fetch(sourceUrl);
    if (!sourceResponse.ok) {
      const error = new Error(`Не удалось скачать исходное фото для AI-генерации: HTTP ${sourceResponse.status}.`);
      error.statusCode = 400;
      error.code = "source_image_fetch_failed";
      throw error;
    }
    sourceMimeType = cleanText(sourceResponse.headers.get("content-type")).split(";")[0];
    if (!supportedOpenAiSourceMime(sourceMimeType)) {
      const error = new Error("Исходное фото должно быть PNG, JPG или WEBP.");
      error.statusCode = 400;
      error.code = "unsupported_source_image";
      throw error;
    }
    sourceBuffer = Buffer.from(await sourceResponse.arrayBuffer());
    sourceFileName = fileNameFromImageMime(sourceMimeType);
  } else {
    const error = new Error("Укажите исходное фото как URL или загрузите его через импорт изображений.");
    error.statusCode = 400;
    error.code = "source_image_url_required";
    throw error;
  }

  const generatedPrompt = buildOzonAiImagePrompt(product, prompt, { variantIndex, variantTotal, skipPackshotRules });
  const logoReference = await readAiLogoReference();
  const referenceImages = logoReference?.payload ? [logoReference.payload] : [];
  let imageBase64;
  try {
    if (!forceCodexSale && isOpenAiRelayConfigured()) {
      imageBase64 = await fetchOpenAiImageViaRelay({ prompt: generatedPrompt, sourceBuffer, sourceMimeType, referenceImages });
    } else if (isCodexSaleAiProvider(aiSettings)) {
      imageBase64 = await fetchCodexSaleImage(aiSettings, {
        prompt: generatedPrompt,
        sourceBuffer,
        sourceMimeType,
        sourceFileName,
        allowGenerationFallback,
      });
    } else {
      const client = getOpenAiClient(aiSettings);
      const image = [await toFile(sourceBuffer, sourceFileName, { type: sourceMimeType })];
      if (logoReference) {
        image.push(await toFile(logoReference.sourceBuffer, logoReference.sourceFileName, { type: logoReference.sourceMimeType }));
      }
      const editRequest = {
        model: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
        image: image.length === 1 ? image[0] : image,
        prompt: generatedPrompt,
        size: aiSettings.imageSize || openaiImageSize,
        quality: aiSettings.imageQuality || openaiImageQuality,
        output_format: aiSettings.imageFormat || openaiImageFormat,
      };
      if (openAiImageSupportsInputFidelity(effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings))) editRequest.input_fidelity = "high";
      if (openaiImageConfig && typeof openaiImageConfig === "object") {
        editRequest.image_config = JSON.stringify(openaiImageConfig);
      }
      const result = await client.images.edit(editRequest);
      imageBase64 = await imageBase64FromOpenAiImageResult(result);
    }
  } catch (error) {
    throw normalizeOpenAiImageError(error);
  }
  if (!imageBase64) {
    const error = new Error("OpenAI не вернул изображение. Попробуйте повторить генерацию.");
    error.statusCode = 502;
    error.code = "openai_image_empty";
    throw error;
  }

  let outBuffer = Buffer.from(imageBase64, "base64");
  outBuffer = await resizeOzonAiImageOutputBuffer(outBuffer, aiSettings.imageFormat || openaiImageFormat);

  await fs.mkdir(aiImageDir, { recursive: true });
  const extension = aiImageExtension(aiSettings.imageFormat || openaiImageFormat);
  const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}${extension}`;
  const filePath = path.join(aiImageDir, fileName);
  await fs.writeFile(filePath, outBuffer);

  const relativeUrl = `/uploads/ai-images/${fileName}`;
  return normalizeAiImageDraft({
    status: "pending",
    prompt: generatedPrompt,
    productName: product.name || product.ozon?.name,
    sourceImageUrl: sourceUrl,
    resultUrl: `${uploadBaseUrl(request)}${relativeUrl}`,
    batchId,
    variantIndex,
    variantTotal,
    slotOrder: options.slotOrder,
    presetId: cleanText(options.presetId),
    presetLabel: cleanText(options.presetLabel),
    model: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
    size: ozonAiImageStoredSizeLabel(aiSettings),
    quality: aiSettings.imageQuality || openaiImageQuality,
    format: aiSettings.imageFormat || openaiImageFormat,
  });
}

