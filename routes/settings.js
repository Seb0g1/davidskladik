function registerSettingsRoutes(app, deps) {
  const {
    requireAdmin,
    cleanText,
    readAppSettings,
    writeAppSettings,
    publicAppSettings,
    defaultAppSettings,
    maskedSecretValue,
    normalizeAiSettings,
    effectiveAiSettingsFromAppSettings,
    assertTextGenerationConfigured,
    getOpenAiClient,
    createOpenAiChatCompletionWithFallback,
    shouldPreferCompatibleOpenAiChatRequest,
    openaiTextModel,
    priceAffectingSettingsChanged,
    queueImmediateAutoPricePush,
    appendAudit,
    logger,
    normalizeOpenAiImageError,
  } = deps;

app.get("/api/settings", requireAdmin, async (_request, response, next) => {
  try {
    const settings = await readAppSettings();
    response.json({
      settings: publicAppSettings(settings),
    });
  } catch (error) {
    next(error);
  }
});

async function saveSettingsHandler(request, response, next) {
  try {
    const previous = await readAppSettings();
    const rawSettings = { ...(request.body || {}) };
    if (!rawSettings.ai) {
      rawSettings.ai = previous.ai || {};
    } else {
      const incomingKey = cleanText(rawSettings.ai.apiKey || rawSettings.ai.api_key);
      const clearKey = rawSettings.ai.clearApiKey === true || rawSettings.ai.apiKeySet === false;
      if (clearKey) {
        rawSettings.ai = { ...rawSettings.ai, apiKey: "" };
      } else if (!incomingKey || incomingKey === maskedSecretValue) {
        rawSettings.ai = { ...rawSettings.ai, apiKey: previous.ai?.apiKey || "" };
      }
    }
    const settings = await writeAppSettings(rawSettings);
    const shouldReprice = priceAffectingSettingsChanged(previous, settings);
    appendAudit(request, "settings.update", {
      fixedUsdRate: settings.fixedUsdRate,
      defaultMarkups: settings.defaultMarkups,
      markupRules: settings.markupRules.length,
      availabilityRules: settings.availabilityRules.length,
      priceAffecting: shouldReprice,
      ai: {
        enabled: settings.ai?.enabled !== false,
        providerId: settings.ai?.providerId,
        baseUrl: settings.ai?.baseUrl,
        textModel: settings.ai?.textModel,
        imageModel: settings.ai?.imageModel,
        apiKeySet: Boolean(settings.ai?.apiKey),
      },
    }).catch((auditError) => {
      logger.warn("settings audit append failed", { detail: auditError?.message || String(auditError) });
    });
    let priceRepriceQueued = false;
    let priceRepriceQueueError = "";
    if (shouldReprice) {
      try {
        queueImmediateAutoPricePush([], "settings_price_update", { force: true });
        priceRepriceQueued = true;
      } catch (queueError) {
        priceRepriceQueueError = queueError?.message || String(queueError);
        logger.warn("settings auto price queue failed", { detail: queueError?.message || String(queueError) });
      }
    }
    response.json({
      ok: true,
      settings: publicAppSettings(settings),
      priceAffectingChanged: shouldReprice,
      priceRepriceQueued,
      priceRepriceReason: shouldReprice ? "settings_price_update" : "no_price_affecting_changes",
      priceRepriceQueueError,
    });
  } catch (error) {
    next(error);
  }
}

app.put("/api/settings", requireAdmin, saveSettingsHandler);
app.post("/api/settings", requireAdmin, saveSettingsHandler);

app.post("/api/settings/ai/test", requireAdmin, async (request, response, next) => {
  try {
    const previous = await readAppSettings();
    const rawAi = request.body?.ai || previous.ai || {};
    const incomingKey = cleanText(rawAi.apiKey || rawAi.api_key);
    const clearKey = rawAi.clearApiKey === true || rawAi.apiKeySet === false;
    const ai = normalizeAiSettings({
      ...rawAi,
      apiKey: clearKey ? "" : ((!incomingKey || incomingKey === maskedSecretValue) ? previous.ai?.apiKey || "" : incomingKey),
    }, previous.ai || defaultAppSettings().ai);
    const effective = effectiveAiSettingsFromAppSettings({ ...previous, ai });
    assertTextGenerationConfigured(effective);
    const client = getOpenAiClient(effective);
    const startedAt = Date.now();
    const result = await createOpenAiChatCompletionWithFallback(client, {
      model: effective.textModel || openaiTextModel,
      messages: [
        { role: "system", content: "Return JSON only." },
        { role: "user", content: "{\"ok\":true}" },
      ],
      temperature: 0,
      response_format: { type: "json_object" },
    }, {
      preferCompatible: shouldPreferCompatibleOpenAiChatRequest(effective),
    });
    response.json({
      ok: true,
      providerId: effective.providerId,
      baseUrl: effective.baseUrl,
      model: cleanText(result?.model) || effective.textModel,
      latencyMs: Date.now() - startedAt,
    });
  } catch (error) {
    next(normalizeOpenAiImageError(error));
  }
});
}

module.exports = {
  registerSettingsRoutes,
};
