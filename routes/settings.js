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
    queueAuthoritativePriceReprice,
    queueMarketplaceJob,
    QUEUE_PRIORITY,
    runAvitoFeedRefresh,
    appendAudit,
    logger,
    normalizeOpenAiImageError,
    uploadImages,
    sharp,
    fs,
    path,
    crypto,
    brandingImageDir,
    uploadBaseUrl,
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
    let priceRepriceQueue = null;
    if (shouldReprice) {
      try {
        if (typeof queueAuthoritativePriceReprice === "function") {
          priceRepriceQueue = await queueAuthoritativePriceReprice({
            marketplace: "all",
            reason: "settings_price_update",
            sourceEvent: "settings.update",
            force: true,
            onlyChanged: false,
            refreshMarketplacePrices: true,
            livePriceMaster: true,
            verify: true,
          });
        } else {
          queueImmediateAutoPricePush([], "settings_price_update", { force: true });
        }
        priceRepriceQueued = true;
        if (typeof queueMarketplaceJob === "function") {
          queueMarketplaceJob("wb-marketplace-sync", { source: "settings_price_update" }, { priority: QUEUE_PRIORITY.PRICE_BACKGROUND })
            .catch((error) => logger.warn("wb sync queue after settings failed", { detail: error?.message || String(error) }));
        }
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
      priceIntentId: priceRepriceQueue?.priceIntentId || null,
      queued: priceRepriceQueue?.queued || 0,
      queuedBatches: priceRepriceQueue?.queuedBatches || 0,
      priceRepriceQueueError,
    });
  } catch (error) {
    next(error);
  }
}

app.put("/api/settings", requireAdmin, saveSettingsHandler);
app.post("/api/settings", requireAdmin, saveSettingsHandler);

function pricingAdjustMarketplace(value) {
  const text = cleanText(value || "all").toLowerCase();
  return ["ozon", "yandex", "avito", "wb"].includes(text) ? text : "all";
}

const PRICING_ADJUST_MARKETPLACES = ["ozon", "yandex", "avito", "wb"];

function pricingAdjustCoefficient(value, multiplier) {
  const current = Number(value || 0);
  if (!Number.isFinite(current) || current <= 0) return null;
  return Math.max(0.0001, Number((current * multiplier).toFixed(4)));
}

function pricingAdjustRule(rule, marketplace, multiplier) {
  const normalized = {
    marketplace: pricingAdjustMarketplace(rule?.marketplace),
    minUsd: Number(rule?.minUsd || 0) || 0,
    coefficient: Number(rule?.coefficient || 0) || 0,
  };
  if (marketplace === "all") {
    return [{ ...normalized, coefficient: pricingAdjustCoefficient(normalized.coefficient, multiplier) || normalized.coefficient }];
  }
  if (normalized.marketplace === marketplace) {
    return [{ ...normalized, coefficient: pricingAdjustCoefficient(normalized.coefficient, multiplier) || normalized.coefficient }];
  }
  if (normalized.marketplace !== "all") return [normalized];
  // Общее правило («all») при точечной корректировке распадается на правила
  // по маркетплейсам: скорректированное + нетронутые копии для остальных.
  const others = PRICING_ADJUST_MARKETPLACES.filter((key) => key !== marketplace);
  return [
    { ...normalized, marketplace, coefficient: pricingAdjustCoefficient(normalized.coefficient, multiplier) || normalized.coefficient },
    ...others.map((key) => ({ ...normalized, marketplace: key })),
  ];
}

app.post("/api/settings/pricing/adjust-percent", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = pricingAdjustMarketplace(request.body?.marketplace);
    const direction = cleanText(request.body?.direction).toLowerCase() === "increase" ? "increase" : "decrease";
    const percent = Number(request.body?.percent);
    if (!Number.isFinite(percent) || percent <= 0 || percent > 90) {
      return response.status(400).json({ error: "Укажите процент от 0.01 до 90.", code: "pricing_adjust_percent_invalid" });
    }
    const multiplier = direction === "increase" ? 1 + percent / 100 : 1 - percent / 100;
    const previous = await readAppSettings();
    const oldValue = {
      defaultMarkups: previous.defaultMarkups || {},
      markupRules: Array.isArray(previous.markupRules) ? previous.markupRules : [],
    };
    const nextDefaultMarkups = { ...(previous.defaultMarkups || {}) };
    for (const key of PRICING_ADJUST_MARKETPLACES) {
      if (marketplace !== "all" && marketplace !== key) continue;
      const adjusted = pricingAdjustCoefficient(nextDefaultMarkups[key], multiplier);
      if (adjusted) nextDefaultMarkups[key] = adjusted;
    }
    const nextRules = (Array.isArray(previous.markupRules) ? previous.markupRules : [])
      .flatMap((rule) => pricingAdjustRule(rule, marketplace, multiplier));
    const settings = await writeAppSettings({
      ...previous,
      defaultMarkups: nextDefaultMarkups,
      markupRules: nextRules,
    });
    const newValue = {
      defaultMarkups: settings.defaultMarkups || {},
      markupRules: settings.markupRules || [],
    };
    appendAudit(request, "settings.pricing_adjust_percent", {
      marketplace,
      direction,
      percent,
      multiplier: Number(multiplier.toFixed(6)),
      oldValue,
      newValue,
    }).catch((auditError) => {
      logger.warn("settings pricing adjust audit append failed", { detail: auditError?.message || String(auditError) });
    });
    let priceRepriceQueued = false;
    let priceRepriceQueueError = "";
    let priceRepriceQueue = null;
    try {
      if (marketplace === "avito") {
        // Цены Avito считаются на лету при каждой сборке фида — пересчёт цен
        // склада не нужен, достаточно обновить сохранённые объявления.
        if (typeof runAvitoFeedRefresh === "function") {
          await runAvitoFeedRefresh({ source: "settings_price_adjust" }).catch(() => null);
        }
        priceRepriceQueued = true;
      } else if (typeof queueAuthoritativePriceReprice === "function") {
        priceRepriceQueue = await queueAuthoritativePriceReprice({
          marketplace,
          reason: "settings_price_adjust_percent",
          sourceEvent: "settings.pricing_adjust_percent",
          force: true,
          onlyChanged: false,
          refreshMarketplacePrices: true,
          livePriceMaster: true,
          verify: true,
        });
        if ((marketplace === "all" || marketplace === "wb") && typeof queueMarketplaceJob === "function") {
          queueMarketplaceJob("wb-marketplace-sync", { source: "settings_price_adjust" }, { priority: QUEUE_PRIORITY.PRICE_BACKGROUND })
            .catch((error) => logger.warn("wb sync queue after price adjust failed", { detail: error?.message || String(error) }));
        }
      } else {
        queueImmediateAutoPricePush([], "settings_price_adjust_percent", { force: true });
      }
      priceRepriceQueued = true;
    } catch (queueError) {
      priceRepriceQueueError = queueError?.message || String(queueError);
      logger.warn("settings pricing adjust auto price queue failed", { detail: queueError?.message || String(queueError) });
    }
    response.json({
      ok: true,
      settings: publicAppSettings(settings),
      changed: true,
      oldValue,
      newValue,
      priceRepriceQueued,
      priceRepriceReason: "settings_price_adjust_percent",
      priceIntentId: priceRepriceQueue?.priceIntentId || null,
      queued: priceRepriceQueue?.queued || 0,
      queuedBatches: priceRepriceQueue?.queuedBatches || 0,
      priceRepriceQueueError,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/settings/branding/logo", requireAdmin, uploadImages.single("logo"), async (request, response, next) => {
  try {
    const file = request.file;
    const marketplace = cleanText(request.body?.marketplace || request.query?.marketplace).toLowerCase();
    const shopName = cleanText(request.body?.shopName || request.body?.shop_name || request.query?.shopName);
    if (marketplace !== "ozon" && marketplace !== "yandex") {
      return response.status(400).json({ error: "marketplace must be ozon or yandex", code: "branding_marketplace_invalid" });
    }
    if (!file) {
      return response.status(400).json({ error: "Upload PNG logo file.", code: "branding_logo_required" });
    }
    if (String(file.mimetype || "").toLowerCase() !== "image/png") {
      return response.status(400).json({ error: "Logo must be PNG.", code: "branding_logo_png_required" });
    }
    const meta = await sharp(file.buffer).metadata();
    if (Number(meta.width) !== 258 || Number(meta.height) !== 258) {
      return response.status(400).json({
        error: "Logo must be exactly 258x258 PNG.",
        code: "logo_invalid_size",
        width: meta.width || 0,
        height: meta.height || 0,
      });
    }
    await fs.mkdir(brandingImageDir, { recursive: true });
    const fileName = `${marketplace}-${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}.png`;
    const filePath = path.join(brandingImageDir, fileName);
    await sharp(file.buffer).png({ compressionLevel: 9 }).toFile(filePath);
    const logoUrl = `/uploads/branding/${fileName}`;
    const previous = await readAppSettings();
    const previousBranding = previous.branding || {};
    const previousMarketplaces = previousBranding.marketplaces || {};
    const branding = {
      ...previousBranding,
      marketplaces: {
        ...previousMarketplaces,
        [marketplace]: {
          ...(previousMarketplaces[marketplace] || {}),
          shopName: shopName || previousMarketplaces[marketplace]?.shopName || (marketplace === "ozon" ? "Magic Stick" : "parfumerius"),
          logoUrl,
        },
      },
    };
    const settings = await writeAppSettings({ ...previous, branding });
    appendAudit(request, "settings.branding.logo_upload", {
      marketplace,
      shopName: branding.marketplaces[marketplace].shopName,
      logoUrl,
      width: meta.width,
      height: meta.height,
    }).catch((auditError) => {
      logger.warn("settings branding audit append failed", { detail: auditError?.message || String(auditError) });
    });
    response.json({
      ok: true,
      marketplace,
      shopName: branding.marketplaces[marketplace].shopName,
      logoUrl,
      url: `${uploadBaseUrl(request)}${logoUrl}`,
      width: meta.width,
      height: meta.height,
      settings: publicAppSettings(settings),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/settings/branding/extra-card", requireAdmin, uploadImages.single("image"), async (request, response, next) => {
  try {
    const file = request.file;
    const marketplace = cleanText(request.body?.marketplace || request.query?.marketplace).toLowerCase();
    const label = cleanText(request.body?.label || request.query?.label);
    if (marketplace !== "ozon" && marketplace !== "yandex") {
      return response.status(400).json({ error: "marketplace must be ozon or yandex", code: "branding_marketplace_invalid" });
    }
    if (!file) {
      return response.status(400).json({ error: "Upload image file.", code: "branding_extra_card_required" });
    }
    const meta = await sharp(file.buffer).metadata();
    if (!meta.width || !meta.height || meta.width < 600 || meta.height < 600) {
      return response.status(400).json({
        error: "Extra card image must be at least 600x600.",
        code: "extra_card_invalid_size",
        width: meta.width || 0,
        height: meta.height || 0,
      });
    }
    await fs.mkdir(brandingImageDir, { recursive: true });
    const fileName = `${marketplace}-extra-${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}.png`;
    const filePath = path.join(brandingImageDir, fileName);
    const output = await sharp(file.buffer)
      .rotate()
      .resize(1600, 1600, { fit: "inside", withoutEnlargement: true })
      .png({ compressionLevel: 9 })
      .toBuffer();
    await fs.writeFile(filePath, output);
    const outputMeta = await sharp(output).metadata();
    const imageUrl = `/uploads/branding/${fileName}`;
    const previous = await readAppSettings();
    const previousBranding = previous.branding || {};
    const previousMarketplaces = previousBranding.marketplaces || {};
    const currentMarketplace = previousMarketplaces[marketplace] || {};
    const extraCard = {
      id: crypto.randomUUID(),
      label: label || `Фото ${Math.min(7, 6 + (Array.isArray(currentMarketplace.extraCards) ? currentMarketplace.extraCards.length : 0))}`,
      url: imageUrl,
      width: outputMeta.width || meta.width,
      height: outputMeta.height || meta.height,
      createdAt: new Date().toISOString(),
    };
    const branding = {
      ...previousBranding,
      marketplaces: {
        ...previousMarketplaces,
        [marketplace]: {
          ...currentMarketplace,
          shopName: currentMarketplace.shopName || (marketplace === "ozon" ? "Magic Stick" : "parfumerius"),
          extraCards: [...(Array.isArray(currentMarketplace.extraCards) ? currentMarketplace.extraCards : []), extraCard].slice(-2),
        },
      },
    };
    const settings = await writeAppSettings({ ...previous, branding });
    appendAudit(request, "settings.branding.extra_card_upload", {
      marketplace,
      label: extraCard.label,
      imageUrl,
      width: extraCard.width,
      height: extraCard.height,
    }).catch((auditError) => {
      logger.warn("settings branding extra card audit append failed", { detail: auditError?.message || String(auditError) });
    });
    response.json({
      ok: true,
      marketplace,
      card: extraCard,
      url: `${uploadBaseUrl(request)}${imageUrl}`,
      settings: publicAppSettings(settings),
    });
  } catch (error) {
    next(error);
  }
});

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
