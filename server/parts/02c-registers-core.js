registerMarketplaceRoutes(app, {
  defaultUsdRate: process.env.DEFAULT_USD_RATE,
  defaultOzonMarkup: process.env.DEFAULT_OZON_MARKUP,
  defaultYandexMarkup: process.env.DEFAULT_YANDEX_MARKUP,
  readAppSettings,
  defaultAppSettings,
  marketplaceTargets,
  getMarketplaceAccounts,
  sanitizeMarketplaceAccount,
  getHiddenMarketplaceAccounts,
  findMarketplaceAccount,
  testMarketplaceAccountConnection,
  logger,
  readMarketplaceAccounts,
  normalizeMarketplaceAccount,
  writeMarketplaceAccounts,
  appendAudit,
  getEnvOzonAccounts,
  getEnvYandexShops,
  getEnvAvitoAccounts,
  accountPayloadWithSecretFallback,
});

registerUsersRoutes(app, {
  requireAdmin,
  cleanText,
  requestUsername,
  configuredUsersForAdminAsync,
  publicAppUser,
  normalizeAppUser,
  normalizeAppRole,
  readStoredAppUsers,
  writeStoredAppUsers,
  readDeletedAppUsers,
  writeDeletedAppUsers,
  appendAudit,
  getPrisma,
  shouldUsePostgresStorage,
  jsonFallbackEnabled,
  readAuditFiltered,
  readWarehouse,
  logger,
});

registerSettingsRoutes(app, {
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
});

registerSystemMediaRoutes(app, {
  getUsdRate,
  uploadImages,
  fs,
  path,
  crypto,
  uploadImageDir,
  imageExtension,
  uploadBaseUrl,
  consumeUploadQuota,
  pruneUploadDirectory,
  logger,
});

async function readSalesAutomationSystemSummary() {
  if (!shouldUsePostgresStorage()) return { source: "disabled", total: 0 };
  try {
    const prisma = getPrisma();
    const [total, latest, reasonGroups, statusGroups] = await Promise.all([
      prisma.salesAutomationSkuState.count(),
      prisma.salesAutomationSkuState.findFirst({
        orderBy: { updatedAt: "desc" },
        select: {
          marketplace: true,
          target: true,
          offerId: true,
          reason: true,
          priceStatus: true,
          stockStatus: true,
          lastCalculatedAt: true,
          lastPriceSentAt: true,
          lastStockSentAt: true,
          lastError: true,
          updatedAt: true,
          raw: true,
        },
      }),
      prisma.salesAutomationSkuState.groupBy({ by: ["reason"], _count: { _all: true } }),
      prisma.salesAutomationSkuState.groupBy({ by: ["priceStatus"], _count: { _all: true } }),
    ]);
    const reasons = Object.fromEntries(reasonGroups.map((row) => [row.reason || "unknown", row._count._all]));
    const priceStatuses = Object.fromEntries(statusGroups.map((row) => [String(row.priceStatus || "unknown"), row._count._all]));
    const latestRaw = latest?.raw && typeof latest.raw === "object" && !Array.isArray(latest.raw) ? latest.raw : {};
    return {
      source: "postgres",
      total,
      reasons,
      priceStatuses,
      pmTimeout: Number(reasons.pm_live_timeout || 0),
      queued: Number(reasons.queued || 0),
      verificationPending: Number(reasons.verification_pending || 0),
      notApplied: Number(reasons.ozon_price_not_applied || 0),
      lastCalculatedAt: latest?.lastCalculatedAt?.toISOString?.() || null,
      lastPriceSentAt: latest?.lastPriceSentAt?.toISOString?.() || null,
      latest: latest ? {
        marketplace: latest.marketplace,
        target: latest.target,
        offerId: latest.offerId,
        reason: latest.reason,
        priceStatus: latest.priceStatus,
        stockStatus: latest.stockStatus,
        priceApplyStatus: latestRaw.priceApplyStatus || latestRaw.applyStatus || null,
        priceIntentId: latestRaw.priceIntentId || null,
        updatedAt: latest.updatedAt?.toISOString?.() || null,
        lastError: latest.lastError || "",
      } : null,
    };
  } catch (error) {
    return { source: "postgres", total: 0, error: error?.message || String(error) };
  }
}

