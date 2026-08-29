function defaultSupportCategories() {
  return ["Заказ и оплата", "Доставка", "Возврат", "Качество товара", "Другое"];
}

function parseJsonField(value, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "object") return value;
  return JSON.parse(String(value));
}

function defaultAppSettings() {
  return {
    fixedUsdRate: Number(process.env.DEFAULT_USD_RATE || 95),
    defaultMarkups: {
      ozon: Number(process.env.DEFAULT_OZON_MARKUP || 1.7),
      yandex: Number(process.env.DEFAULT_YANDEX_MARKUP || 1.6),
      avito: Number(process.env.DEFAULT_AVITO_MARKUP || 1.6),
      wb: Number(process.env.DEFAULT_WB_MARKUP || 1.6),
    },
    automation: {
      autoSyncEnabled: autoSyncMinutes > 0,
      autoSyncMinutes: Math.max(5, Number(autoSyncMinutes || 30) || 30),
    },
    ai: {
      enabled: true,
      providerId: cleanText(process.env.OPENAI_PROVIDER_ID || "codexsale"),
      baseUrl: openaiBaseUrl || "https://codex.sale/v1",
      apiKey: "",
      textModel: openaiTextModel,
      imageModel: openaiImageModel,
      imageSize: openaiImageSize,
      imageQuality: openaiImageQuality,
      imageFormat: openaiImageFormat,
    },
    markupRules: [],
    availabilityRules: [
      { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
      { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
    ],
    branding: {
      marketplaces: {
        ozon: { shopName: "Magic Stick", logoUrl: "" },
        yandex: { shopName: "parfumerius", logoUrl: "" },
      },
    },
    supplierCart: {
      enabled: true,
      autoEnabled: true,
      scheduleTimes: ["09:30", "12:00", "15:00"],
      timezone: "Europe/Moscow",
      mode: "draft",
      marketplaces: ["ozon", "yandex", "wb"],
      lookbackHours: 168,
      includeOzonStatuses: ["awaiting_packaging"],
      includeYandexStatuses: ["PROCESSING"],
      includeYandexSubstatuses: ["STARTED", "PACKAGING"],
    },
  };
}

function normalizeMarkupRule(input = {}) {
  const minUsd = Number(input.minUsd ?? input.min_usd ?? 0);
  const coefficient = Number(input.coefficient ?? input.markup ?? 0);
  const rawMarketplace = cleanText(input.marketplace || input.target || "all").toLowerCase();
  const marketplace = ["ozon", "yandex", "avito", "wb"].includes(rawMarketplace) ? rawMarketplace : "all";
  if (!Number.isFinite(minUsd) || !Number.isFinite(coefficient) || coefficient <= 0) return null;
  return {
    minUsd: Math.max(0, Number(minUsd.toFixed(4))),
    coefficient: Number(coefficient.toFixed(4)),
    marketplace,
  };
}

function normalizeAvailabilityRule(input = {}) {
  const minAvailableSuppliers = Number(input.minAvailableSuppliers ?? input.min_available_suppliers ?? input.minSuppliers ?? 0);
  const coefficientDelta = Number(input.coefficientDelta ?? input.coefficient_delta ?? input.markupDelta ?? 0);
  const targetStock = Number(input.targetStock ?? input.target_stock ?? input.stock ?? 0);
  const rawMarketplace = cleanText(input.marketplace || input.target || "all").toLowerCase();
  const marketplace = rawMarketplace === "ozon" || rawMarketplace === "yandex" ? rawMarketplace : "all";
  if (!Number.isFinite(minAvailableSuppliers) || minAvailableSuppliers < 0) return null;
  if (!Number.isFinite(coefficientDelta)) return null;
  if (!Number.isFinite(targetStock) || targetStock < 0) return null;
  return {
    marketplace,
    minAvailableSuppliers: Math.max(0, Math.round(minAvailableSuppliers)),
    coefficientDelta: Number(coefficientDelta.toFixed(4)),
    targetStock: Math.max(0, Math.round(targetStock)),
  };
}

function normalizeMarketplaceBranding(input = {}, fallback = {}) {
  const raw = input && typeof input === "object" ? input : {};
  const rawExtraCards = Array.isArray(raw.extraCards || raw.extra_cards) ? (raw.extraCards || raw.extra_cards) : [];
  return {
    shopName: cleanText(raw.shopName || raw.shop_name || fallback.shopName),
    logoUrl: cleanText(raw.logoUrl || raw.logo_url || fallback.logoUrl),
    extraCards: rawExtraCards
      .map((item) => ({
        id: cleanText(item?.id) || crypto.randomUUID(),
        label: cleanText(item?.label),
        url: cleanText(item?.url || item?.imageUrl || item?.image_url),
        width: Number(item?.width || 0) || undefined,
        height: Number(item?.height || 0) || undefined,
        createdAt: item?.createdAt || item?.created_at || new Date().toISOString(),
      }))
      .filter((item) => item.url)
      .slice(-2),
  };
}

function normalizeBrandingSettings(input = {}, fallback = defaultAppSettings().branding) {
  const raw = input && typeof input === "object" ? input : {};
  const marketplaces = raw.marketplaces && typeof raw.marketplaces === "object" ? raw.marketplaces : {};
  const fallbackMarketplaces = fallback?.marketplaces || {};
  return {
    marketplaces: {
      ozon: normalizeMarketplaceBranding(marketplaces.ozon || raw.ozon, fallbackMarketplaces.ozon || { shopName: "Magic Stick", logoUrl: "" }),
      yandex: normalizeMarketplaceBranding(marketplaces.yandex || raw.yandex, fallbackMarketplaces.yandex || { shopName: "parfumerius", logoUrl: "" }),
    },
  };
}

function normalizeSupplierCartSettings(input = {}, fallback = defaultAppSettings().supplierCart) {
  const raw = input && typeof input === "object" ? input : {};
  const marketplaces = Array.isArray(raw.marketplaces)
    ? raw.marketplaces.map((item) => cleanText(item).toLowerCase()).filter((item) => item === "ozon" || item === "yandex" || item === "wb")
    : fallback.marketplaces;
  const lookbackHours = Number(raw.lookbackHours ?? raw.lookback_hours ?? fallback.lookbackHours);
  const normalizeStatuses = (value, fallbackValue) => {
    const source = Array.isArray(value) ? value : splitList(value);
    const statuses = source.map((item) => cleanText(item).toUpperCase()).filter(Boolean);
    return statuses.length ? Array.from(new Set(statuses)) : fallbackValue;
  };
  return {
    enabled: parseBooleanSetting(raw.enabled, fallback.enabled !== false),
    autoEnabled: parseBooleanSetting(raw.autoEnabled ?? raw.auto_enabled, fallback.autoEnabled !== false),
    scheduleTimes: (Array.isArray(raw.scheduleTimes || raw.schedule_times) ? (raw.scheduleTimes || raw.schedule_times) : fallback.scheduleTimes || ["09:30", "12:00", "15:00"])
      .map((item) => normalizeSupplierOrderCutoff(item))
      .filter(Boolean)
      .slice(0, 10),
    timezone: cleanText(raw.timezone || fallback.timezone || "Europe/Moscow") || "Europe/Moscow",
    mode: "draft",
    marketplaces: marketplaces.length ? Array.from(new Set(marketplaces)) : ["ozon", "yandex", "wb"],
    lookbackHours: Number.isFinite(lookbackHours) && lookbackHours > 0 ? Math.min(720, Math.round(lookbackHours)) : fallback.lookbackHours,
    includeOzonStatuses: normalizeStatuses(raw.includeOzonStatuses || raw.include_ozon_statuses, fallback.includeOzonStatuses),
    includeYandexStatuses: normalizeStatuses(raw.includeYandexStatuses || raw.include_yandex_statuses, fallback.includeYandexStatuses),
    includeYandexSubstatuses: normalizeStatuses(raw.includeYandexSubstatuses || raw.include_yandex_substatuses, fallback.includeYandexSubstatuses),
  };
}

function normalizeAiSettings(input = {}, fallback = defaultAppSettings().ai) {
  const raw = input && typeof input === "object" ? input : {};
  const hasApiKey = Object.prototype.hasOwnProperty.call(raw, "apiKey") || Object.prototype.hasOwnProperty.call(raw, "api_key");
  const imageModel = cleanText(raw.imageModel || raw.image_model || fallback.imageModel || openaiImageModel);
  const imageFormat = cleanText(raw.imageFormat || raw.image_format || fallback.imageFormat || openaiImageFormat).toLowerCase();
  const imageSize = cleanText(raw.imageSize || raw.image_size || fallback.imageSize || openaiImageSize);
  const imageQuality = cleanText(raw.imageQuality || raw.image_quality || fallback.imageQuality || openaiImageQuality);
  const textModel = cleanText(raw.textModel || raw.text_model || fallback.textModel || openaiTextModel);
  const baseUrl = normalizeOpenAiCompatibleBaseUrl(cleanText(raw.baseUrl || raw.base_url || fallback.baseUrl || openaiBaseUrl));
  const apiKey = hasApiKey ? cleanText(raw.apiKey ?? raw.api_key ?? "") : cleanText(fallback.apiKey);
  return {
    enabled: parseBooleanSetting(raw.enabled, fallback.enabled !== false),
    providerId: cleanText(raw.providerId || raw.provider_id || fallback.providerId || "codexsale"),
    baseUrl,
    apiKey: apiKey === maskedSecretValue ? cleanText(fallback.apiKey) : apiKey,
    textModel,
    imageModel: effectiveOpenAiImageModel(imageModel || "gpt-image-2", { providerId: raw.providerId || raw.provider_id || fallback.providerId, baseUrl }),
    imageSize: imageSize || "1024x1024",
    imageQuality: imageQuality || "auto",
    imageFormat: ["png", "jpeg", "jpg", "webp"].includes(imageFormat) ? imageFormat : "png",
  };
}

function normalizeAppSettings(input = {}) {
  const fallback = defaultAppSettings();
  const fixedUsdRate = Number(input.fixedUsdRate ?? input.fixed_usd_rate ?? fallback.fixedUsdRate);
  const defaultMarkups = {
    ozon: Number(input.defaultMarkups?.ozon ?? input.default_ozon_markup ?? fallback.defaultMarkups.ozon),
    yandex: Number(input.defaultMarkups?.yandex ?? input.default_yandex_markup ?? fallback.defaultMarkups.yandex),
    avito: Number(input.defaultMarkups?.avito ?? input.default_avito_markup ?? fallback.defaultMarkups.avito),
    wb: Number(input.defaultMarkups?.wb ?? input.default_wb_markup ?? fallback.defaultMarkups.wb),
  };
  const rawAutomation = input.automation || {};
  const automationEnabled = parseBooleanSetting(
    rawAutomation.autoSyncEnabled ?? input.autoSyncEnabled ?? input.auto_sync_enabled,
    fallback.automation.autoSyncEnabled,
  );
  const automationMinutes = Number(rawAutomation.autoSyncMinutes ?? input.autoSyncMinutes ?? input.auto_sync_minutes ?? fallback.automation.autoSyncMinutes);
  const rules = Array.isArray(input.markupRules)
    ? input.markupRules.map(normalizeMarkupRule).filter(Boolean)
    : [];
  rules.sort((a, b) => a.minUsd - b.minUsd);
  const availabilityRules = Array.isArray(input.availabilityRules)
    ? input.availabilityRules.map(normalizeAvailabilityRule).filter(Boolean)
    : fallback.availabilityRules.map(normalizeAvailabilityRule).filter(Boolean);
  availabilityRules.sort((a, b) =>
    Number(b.minAvailableSuppliers || 0) - Number(a.minAvailableSuppliers || 0)
    || String(a.marketplace || "all").localeCompare(String(b.marketplace || "all")),
  );
  return {
    fixedUsdRate: Number.isFinite(fixedUsdRate) && fixedUsdRate > 0 ? fixedUsdRate : fallback.fixedUsdRate,
    defaultMarkups: {
      ozon: Number.isFinite(defaultMarkups.ozon) && defaultMarkups.ozon > 0 ? defaultMarkups.ozon : fallback.defaultMarkups.ozon,
      yandex: Number.isFinite(defaultMarkups.yandex) && defaultMarkups.yandex > 0 ? defaultMarkups.yandex : fallback.defaultMarkups.yandex,
      avito: Number.isFinite(defaultMarkups.avito) && defaultMarkups.avito > 0 ? defaultMarkups.avito : fallback.defaultMarkups.avito,
      wb: Number.isFinite(defaultMarkups.wb) && defaultMarkups.wb > 0 ? defaultMarkups.wb : fallback.defaultMarkups.wb,
    },
    automation: {
      autoSyncEnabled: automationEnabled,
      autoSyncMinutes: Number.isFinite(automationMinutes) && automationMinutes >= 5 ? Math.round(automationMinutes) : fallback.automation.autoSyncMinutes,
    },
    ai: normalizeAiSettings(input.ai || {}, fallback.ai),
    markupRules: rules,
    availabilityRules,
    branding: normalizeBrandingSettings(input.branding || {}, fallback.branding),
    supplierCart: normalizeSupplierCartSettings(input.supplierCart || input.supplier_cart || {}, fallback.supplierCart),
    tnved: {
      code: cleanText((input.tnved?.code ?? input.tnvedCode) || "").slice(0, 20),
      autoEnabled: parseBooleanSetting(input.tnved?.autoEnabled, true),
    },
    support: {
      categories: Array.isArray(input.support?.categories)
        ? input.support.categories.map((c) => cleanText(c)).filter(Boolean).slice(0, 20)
        : defaultSupportCategories(),
    },
    sponsorTelegramChatId: cleanText(input.sponsorTelegramChatId ?? input.sponsor_telegram_chat_id ?? "") || "",
    sponsorDailyReportEnabled: parseBooleanSetting(input.sponsorDailyReportEnabled ?? input.sponsor_daily_report_enabled, false),
    sponsorDailyReportHour: Math.min(23, Math.max(0, Math.round(Number(input.sponsorDailyReportHour ?? input.sponsor_daily_report_hour ?? 20) || 20))),
  };
}

function maskSecret(value = "") {
  const secret = cleanText(value);
  if (!secret) return "";
  return secret.length <= 8 ? "••••" : `••••${secret.slice(-4)}`;
}

function publicAppSettings(settings = {}) {
  const normalized = normalizeAppSettings(settings);
  const effectiveAiApiKey = configuredAiApiKey(normalized.ai);
  return {
    ...normalized,
    ai: {
      ...normalized.ai,
      apiKey: normalized.ai.apiKey ? maskedSecretValue : "",
      apiKeyMasked: maskSecret(normalized.ai.apiKey),
      apiKeySet: Boolean(effectiveAiApiKey),
      source: normalized.ai.apiKey ? "settings" : (effectiveAiApiKey ? "env" : "empty"),
    },
  };
}

function priceAffectingSettingsChanged(previous = {}, next = {}) {
  const prev = normalizeAppSettings(previous || {});
  const current = normalizeAppSettings(next || {});
  const pick = (settings) => ({
    fixedUsdRate: Number(settings.fixedUsdRate || 0),
    defaultMarkups: settings.defaultMarkups || {},
    markupRules: settings.markupRules || [],
    availabilityRules: settings.availabilityRules || [],
  });
  return JSON.stringify(pick(prev)) !== JSON.stringify(pick(current));
}

async function readAppSettings() {
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const row = await prisma.appSetting.findUnique({ where: { key: "app" } });
      if (row?.value) return normalizeAppSettings(row.value);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read app settings postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const parsed = JSON.parse(await fs.readFile(appSettingsPath, "utf8"));
    return normalizeAppSettings(parsed);
  } catch (error) {
    if (error.code !== "ENOENT") logger.warn("read app settings failed", { detail: error.message });
    return defaultAppSettings();
  }
}

async function writeAppSettings(settings) {
  const normalized = normalizeAppSettings(settings);
  invalidateWarehouseViewCache();
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      await prisma.appSetting.upsert({
        where: { key: "app" },
        create: { key: "app", value: normalized },
        update: { value: normalized },
      });
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write app settings postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${appSettingsPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(normalized, null, 2), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, appSettingsPath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
  return normalized;
}

