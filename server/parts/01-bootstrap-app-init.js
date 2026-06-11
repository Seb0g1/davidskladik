const fs = require("fs/promises");
const fsSync = require("fs");
const path = require("path");
const crypto = require("crypto");
const { execFileSync } = require("child_process");
const express = require("express");
const compression = require("compression");
const rateLimit = require("express-rate-limit");
const multer = require("multer");
const mysql = require("mysql2/promise");
const { Queue, Worker } = require("bullmq");
const OpenAI = require("openai");
const sharp = require("sharp");
const { toFile } = require("openai/uploads");
require("dotenv").config();

const logger = require("./lib/logger");
const { createStaticAppHandlers } = require("./lib/static-app");
const { registerAuthSessionRoutes } = require("./routes/auth-session");
const { registerMarketplaceRoutes } = require("./routes/marketplaces");
const { registerOperationsRoutes } = require("./routes/operations");
const { registerSettingsRoutes } = require("./routes/settings");
const { registerStaticAppRoutes } = require("./routes/static-app");
const { registerSystemMediaRoutes } = require("./routes/system-media");
const { registerUsersRoutes } = require("./routes/users");
const { registerLegacyCatalogRoutes } = require("./routes/legacy-catalog");
const {
  postgresModeEnabled,
  jsonFallbackEnabled,
  getPrisma,
  closePrisma,
} = require("./lib/postgres");

if (process.env.NODE_ENV === "production") {
  const secret = process.env.APP_SESSION_SECRET;
  if (!secret || secret === "dev-secret") {
    logger.error("В production задайте уникальный APP_SESSION_SECRET в .env");
    process.exit(1);
  }
}

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 40,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (_request, response) => {
    response.status(429).json({ error: "Слишком много попыток входа. Подождите несколько минут." });
  },
});

const app = express();
const trustProxyEnv = process.env.TRUST_PROXY_HOPS;
const trustProxyHops = Number(
  trustProxyEnv !== undefined && trustProxyEnv !== null && String(trustProxyEnv).trim() !== ""
    ? trustProxyEnv
    : (process.env.NODE_ENV === "production" ? 1 : 0),
);
if (Number.isFinite(trustProxyHops) && trustProxyHops > 0) {
  app.set("trust proxy", trustProxyHops);
}

const port = Number(process.env.PORT || 3000);
const dataDir = path.join(__dirname, "data");
const configDir = path.join(__dirname, "config");
const publicDir = path.join(__dirname, "public");
const modernAppDir = path.join(publicDir, "app-modern");
const uploadImageDir = path.join(publicDir, "uploads", "images");
const aiImageDir = path.join(publicDir, "uploads", "ai-images");
const brandingImageDir = path.join(publicDir, "uploads", "branding");
const aiImageLogoPath = path.join(publicDir, "logo.png");
const snapshotPath = path.join(dataDir, "snapshot.json");
const historyPath = path.join(dataDir, "history.jsonl");
const exchangeRatePath = path.join(dataDir, "exchange-rate.json");
const warehousePath = path.join(dataDir, "personal-warehouse.json");
const dailySyncPath = path.join(dataDir, "daily-sync.json");
const linkedReconcilerStatePath = path.join(dataDir, "linked-reconciler-state.json");
const marketplaceAccountsPath = path.join(dataDir, "marketplace-accounts.json");
const auditLogPath = path.join(dataDir, "audit-log.jsonl");
const appSettingsPath = path.join(dataDir, "app-settings.json");
const appUsersPath = path.join(dataDir, "app-users.json");
const appDeletedUsersPath = path.join(dataDir, "app-users-deleted.json");
const priceRetryQueuePath = path.join(dataDir, "price-retry-queue.json");
const ozonUnarchiveQueuePath = path.join(dataDir, "ozon-unarchive-queue.json");
const ozonUnarchiveDailyStatePath = path.join(dataDir, "ozon-unarchive-daily.json");
const yandexUnarchiveQueuePath = path.join(dataDir, "yandex-unarchive-queue.json");
const yandexExistingOffersCachePath = path.join(dataDir, "yandex-existing-offers.json");
const operationJobsPath = path.join(dataDir, "operation-jobs.json");
const aiImageJobsPath = path.join(dataDir, "ai-image-jobs.json");
const supplierCartStatePath = path.join(dataDir, "supplier-cart-state.json");
const supplierPickingListPath = path.join(dataDir, "supplier-picking-list.json");
const financeStatePath = path.join(dataDir, "finance-state.json");
const ozonProductRulesPath = path.join(configDir, "ozon-product-rules.json");
const ozonProductRulesExamplePath = path.join(configDir, "ozon-product-rules.example.json");
const buildVersion = cleanBuildVersion(process.env.APP_BUILD_VERSION || process.env.GIT_COMMIT || readGitCommit());
const {
  cacheControlForMutableAsset,
  serveIndexHtml,
  servePublicHtml,
  serveModernAppHtml,
} = createStaticAppHandlers({ fs, path, publicDir, modernAppDir, buildVersion });
const sessionCookieName = "pm_session";
const sessionTtlMs = 1000 * 60 * 60 * 12;
const autoSyncMinutes = Number(process.env.AUTO_SYNC_MINUTES || process.env.DEFAULT_AUTO_SYNC_MINUTES || 30);
const autoSyncInitialDelaySeconds = Math.max(30, Number(process.env.AUTO_SYNC_INITIAL_DELAY_SECONDS || 600) || 600);
const warehouseWarmOnStartup = process.env.WAREHOUSE_WARM_ON_STARTUP === "true";
const warehouseFullMemoryLoadEnabled = process.env.WAREHOUSE_FULL_MEMORY_LOAD_ENABLED === "true";
const backgroundJobsEnabled = process.env.BACKGROUND_JOBS_ENABLED === "true";
const warehouseInMemoryPageMax = Math.max(500, Math.min(50000, Number(process.env.WAREHOUSE_IN_MEMORY_PAGE_MAX || 1500) || 1500));
const warehousePageBuildMaxConcurrent = Math.max(1, Number(process.env.WAREHOUSE_PAGE_BUILD_MAX_CONCURRENT || 2) || 2);
const warehousePageBuildAcquireTimeoutMs = Math.max(1000, Number(process.env.WAREHOUSE_PAGE_BUILD_ACQUIRE_TIMEOUT_MS || 5000) || 5000);
const warehouseJsonMaxLoadBytes = Math.max(50_000_000, Number(process.env.WAREHOUSE_JSON_MAX_LOAD_BYTES || 300_000_000) || 300_000_000);
const autoZeroStockOnNoSupplier = process.env.AUTO_ZERO_STOCK_ON_NO_SUPPLIER !== "false";
const autoArchiveOnNoLinks = process.env.AUTO_ARCHIVE_ON_NO_LINKS === "true";
const keepUnlinkedProductsSellable = process.env.KEEP_UNLINKED_PRODUCTS_SELLABLE !== "false";
const autoRestoreOnSupplierReturn = process.env.AUTO_RESTORE_ON_SUPPLIER_RETURN !== "false";
const bullmqEnabled = process.env.BULLMQ_ENABLED === "true";
const redisUrl = cleanText(process.env.REDIS_URL);
const serverRole = cleanText(process.env.SERVER_ROLE || "monolith").toLowerCase();
const isApiServer = serverRole === "api";
const isWorkerServer = serverRole === "worker";
const isMonolithServer = !isApiServer && !isWorkerServer;
const bullmqWorkerConcurrency = Math.max(1, Math.min(4, Number(process.env.BULLMQ_WORKER_CONCURRENCY || 1) || 1));
const bullmqLockDurationMs = Math.max(120000, Number(process.env.BULLMQ_LOCK_DURATION_MS || 900000) || 900000);
const bullmqStalledIntervalMs = Math.max(30000, Number(process.env.BULLMQ_STALLED_INTERVAL_MS || 120000) || 120000);
const bullmqMaxStalledCount = Math.max(2, Number(process.env.BULLMQ_MAX_STALLED_COUNT || 3) || 3);
const immediateAutoPushMaxScope = Math.max(1, Math.min(1000, Number(process.env.AUTO_PRICE_IMMEDIATE_MAX_SCOPE || 500) || 500));
const linkedActivationImmediateMaxScope = Math.max(
  1,
  Math.min(1000, Number(process.env.LINKED_ACTIVATION_IMMEDIATE_MAX_SCOPE || immediateAutoPushMaxScope) || immediateAutoPushMaxScope),
);
const warehouseFastPageBuildTimeoutMs = Math.max(5000, Number(process.env.WAREHOUSE_FAST_PAGE_BUILD_TIMEOUT_MS || 20000) || 20000);
const warehousePagePmTimeoutMs = Math.max(2000, Number(process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 8000) || 8000);
const linkedNoSupplierZeroGraceMs = Math.max(0, Number(process.env.LINKED_NO_SUPPLIER_ZERO_GRACE_MS || 120000) || 120000);
const linkedNoSupplierZeroStockQueueCooldownMs = Math.max(
  60_000,
  Number(process.env.LINKED_NO_SUPPLIER_ZERO_STOCK_QUEUE_COOLDOWN_MS || 300000) || 300000,
);
const linkedNoSupplierZeroStockQueuedAt = new Map();
const marketplaceQueueAutoPricePushEnabled = process.env.MARKETPLACE_QUEUE_AUTO_PRICE_PUSH_ENABLED !== "false";
const immediateAutoPushDelayMs = Math.max(1200, Number(process.env.AUTO_PRICE_IMMEDIATE_DELAY_MS || 10000) || 10000);
const immediateAutoPushFollowupDelayMs = Math.max(1000, Number(process.env.AUTO_PRICE_IMMEDIATE_FOLLOWUP_DELAY_MS || immediateAutoPushDelayMs) || immediateAutoPushDelayMs);
const priceMasterDeltaPricePushEnabled = process.env.PRICEMASTER_DELTA_PRICE_PUSH_ENABLED !== "false";
const priceMasterDeltaMaxChanges = Math.max(1, Number(process.env.PRICEMASTER_DELTA_MAX_CHANGES || 5000) || 5000);
const priceMasterDeltaMaxProducts = Math.max(1, Number(process.env.PRICEMASTER_DELTA_MAX_PRODUCTS || 2000) || 2000);
const autoPriceReconcileMaxProducts = Math.max(1, Number(process.env.AUTO_PRICE_RECONCILE_MAX_PRODUCTS || 12000) || 12000);
const autoPriceReconcileBatchSize = Math.max(50, Math.min(1000, Number(process.env.AUTO_PRICE_RECONCILE_BATCH_SIZE || 500) || 500));
const authoritativeRepriceBatchSize = Math.max(25, Math.min(500, Number(process.env.AUTHORITATIVE_REPRICE_BATCH_SIZE || 200) || 200));
const autoPricePmTimeoutMs = Math.max(1500, Number(process.env.AUTO_PRICE_PM_TIMEOUT_MS || 10000) || 10000);
const autoPricePmTimeoutRetryMs = Math.max(10000, Number(process.env.AUTO_PRICE_PM_TIMEOUT_RETRY_MS || 120000) || 120000);
const autoPricePmTimeoutRetryAttempts = Math.max(0, Math.min(5, Number(process.env.AUTO_PRICE_PM_TIMEOUT_RETRY_ATTEMPTS || 2) || 2));
const dailySyncTime = process.env.DAILY_SYNC_TIME || "11:00";
const dailySyncEnabled = process.env.DAILY_SYNC_ENABLED !== "false";
const dailySyncSendPrices = process.env.DAILY_SYNC_SEND_PRICES !== "false";
const marketplaceMaintenanceHours = Math.max(
  1,
  Math.min(24, Number(process.env.MARKETPLACE_MAINTENANCE_HOURS || process.env.DEFAULT_MARKETPLACE_MAINTENANCE_HOURS || 6) || 6),
);
const marketplaceMaintenanceEnabled = process.env.MARKETPLACE_MAINTENANCE_ENABLED !== "false";
const marketplaceMaintenanceMinUptimeSec = Math.max(
  60,
  Number(process.env.MARKETPLACE_MAINTENANCE_MIN_UPTIME_SEC || 1800) || 1800,
);
const marketplaceMaintenanceDeferRetryMs = Math.max(
  60_000,
  Number(process.env.MARKETPLACE_MAINTENANCE_DEFER_RETRY_MS || 900000) || 900000,
);
const marketplaceMaintenancePairBackfillEnabled = process.env.MARKETPLACE_MAINTENANCE_PAIR_BACKFILL_ENABLED === "true";
const marketplaceMaintenancePmSyncEnabled = process.env.MARKETPLACE_MAINTENANCE_PM_SYNC_ENABLED === "true";
const marketplaceMaintenanceLinkedScanLimit = Math.max(
  500,
  Math.min(10000, Number(process.env.MARKETPLACE_MAINTENANCE_LINKED_SCAN_LIMIT || 3000) || 3000),
);
const marketplaceMaintenanceRecentSlowWindowMs = Math.max(
  30_000,
  Number(process.env.MARKETPLACE_MAINTENANCE_RECENT_SLOW_WINDOW_MS || 120000) || 120000,
);
// Rolling linked-product reconciler (worker-only). Off by default; the worker env enables it.
const linkedReconcilerEnabled = process.env.LINKED_RECONCILER_ENABLED === "true";
const linkedReconcilerBatchSize = Math.max(10, Math.min(1000, Number(process.env.LINKED_RECONCILER_BATCH_SIZE || 100) || 100));
const linkedReconcilerIntervalMinutes = Math.max(1, Number(process.env.LINKED_RECONCILER_INTERVAL_MINUTES || 10) || 10);
const linkedReconcilerMaxProductsPerTick = Math.max(
  linkedReconcilerBatchSize,
  Math.min(5000, Number(process.env.LINKED_RECONCILER_MAX_PRODUCTS_PER_TICK || 300) || 300),
);
const linkedReconcilerInitialDelaySeconds = Math.max(30, Number(process.env.LINKED_RECONCILER_INITIAL_DELAY_SECONDS || 420) || 420);
const linkedReconcilerDeferRetryMs = Math.max(60_000, Number(process.env.LINKED_RECONCILER_DEFER_RETRY_MS || 300000) || 300000);
const linkedReconcilerSendTargetStock = process.env.LINKED_RECONCILER_SEND_TARGET_STOCK !== "false";
// Daily full marketplace import controls (the heavy buildWarehouseView({sync:true}) path).
const dailyFullImportEnabled = process.env.DAILY_FULL_IMPORT_ENABLED !== "false";
const dailyFullImportDeferUnderLoad = process.env.DAILY_FULL_IMPORT_DEFER_UNDER_LOAD !== "false";
const pmDbPoolSize = Math.max(1, Number(process.env.PM_DB_POOL_SIZE || 8) || 8);
const pmDbConnectTimeoutMs = Math.max(1000, Number(process.env.PM_DB_CONNECT_TIMEOUT_MS || 10000) || 10000);
const warehouseViewCacheMs = Math.max(1000, Number(process.env.WAREHOUSE_VIEW_CACHE_MS || 120000) || 120000);
const ozonWarehouseListEnabled = process.env.OZON_WAREHOUSE_LIST_ENABLED === "true";
const ozonBaseUrl = "https://api-seller.ozon.ru";
const yandexBaseUrl = "https://api.partner.market.yandex.ru";
const yandexCleanupDeleteLimit = Math.max(1, Math.min(10000, Number(process.env.YANDEX_CLEANUP_DELETE_LIMIT || 10000) || 10000));
const yandexImportSendLimit = Math.max(1, Math.min(10000, Number(process.env.YANDEX_IMPORT_SEND_LIMIT || 5000) || 5000));
const yandexStockCampaignIds = new Set(
  String(process.env.YANDEX_STOCK_CAMPAIGN_IDS || "128820967")
    .split(/[\s,;]+/)
    .map((value) => cleanText(value))
    .filter(Boolean),
);
const configuredOzonUnarchiveDailyLimit = process.env.OZON_UNARCHIVE_DAILY_LIMIT === undefined
  ? 100
  : (Number(process.env.OZON_UNARCHIVE_DAILY_LIMIT) || 0);
const ozonUnarchiveDailyLimit = configuredOzonUnarchiveDailyLimit > 0
  ? Math.max(1, Math.min(10000, configuredOzonUnarchiveDailyLimit))
  : Infinity;
const ozonUnarchiveQueueBatchLimit = Math.max(1, Math.min(1000, Number(process.env.OZON_UNARCHIVE_QUEUE_BATCH_LIMIT || 100) || 100));
const ozonUnarchiveQueueAutoEnabled = process.env.OZON_UNARCHIVE_QUEUE_AUTO_ENABLED !== "false";
const ozonUnarchiveQueueAutoIntervalMinutes = Math.max(5, Number(process.env.OZON_UNARCHIVE_QUEUE_AUTO_INTERVAL_MINUTES || 30) || 30);
const ozonUnarchiveQueueAutoInitialDelaySeconds = Math.max(30, Number(process.env.OZON_UNARCHIVE_QUEUE_AUTO_INITIAL_DELAY_SECONDS || 180) || 180);
const yandexUnarchiveQueueAutoEnabled = process.env.YANDEX_UNARCHIVE_QUEUE_AUTO_ENABLED !== "false";
const yandexUnarchiveQueueBatchLimit = Math.max(1, Math.min(500, Number(process.env.YANDEX_UNARCHIVE_QUEUE_BATCH_LIMIT || 200) || 200));
const yandexUnarchiveQueueAutoIntervalMinutes = Math.max(1, Number(process.env.YANDEX_UNARCHIVE_QUEUE_AUTO_INTERVAL_MINUTES || 15) || 15);
const yandexUnarchiveQueueAutoInitialDelaySeconds = Math.max(30, Number(process.env.YANDEX_UNARCHIVE_QUEUE_AUTO_INITIAL_DELAY_SECONDS || 120) || 120);
const yandexUnarchiveMaxAttempts = Math.max(1, Number(process.env.YANDEX_UNARCHIVE_MAX_ATTEMPTS || 8) || 8);
const yandexUnarchiveRetryMinutes = [1, 5, 15, 60, 240, 1440];
const autoPricePushHeartbeatLogMs = Math.max(15000, Number(process.env.AUTO_PRICE_PUSH_HEARTBEAT_MS || 30000) || 30000);
const autoPricePushStallGuardMs = Math.max(600000, Number(process.env.AUTO_PRICE_PUSH_STALL_GUARD_MS || 600000) || 600000);
const ozonUnarchiveVerifyDelayMs = Math.max(0, Number(process.env.OZON_UNARCHIVE_VERIFY_DELAY_MS || 5000) || 5000);
const ozonUnarchiveVerifyAttempts = Math.max(1, Math.min(10, Number(process.env.OZON_UNARCHIVE_VERIFY_ATTEMPTS || 3) || 3));
const ozonUnarchiveVisibilityRetryMinutes = Math.max(5, Number(process.env.OZON_UNARCHIVE_VISIBILITY_RETRY_MINUTES || ozonUnarchiveQueueAutoIntervalMinutes) || ozonUnarchiveQueueAutoIntervalMinutes);
const ozonPriceVerifyDelayMs = Math.max(0, Number(process.env.OZON_PRICE_VERIFY_DELAY_MS || 30000) || 30000);
const ozonPriceVerifyAttempts = Math.max(1, Math.min(10, Number(process.env.OZON_PRICE_VERIFY_ATTEMPTS || 5) || 5));
// Deferred verification: pipeline does not block on Ozon applying prices (default on).
const ozonPriceVerifyDeferred = process.env.OZON_PRICE_VERIFY_DEFERRED !== "false";
const ozonPriceVerifyToleranceRub = Math.max(0, Number(process.env.OZON_PRICE_VERIFY_TOLERANCE_RUB || 0) || 0);
const stateWriteTransactionTimeoutMs = Math.max(10000, Number(process.env.STATE_WRITE_TRANSACTION_TIMEOUT_MS || 30000) || 30000);
const stateWriteTransactionMaxWaitMs = Math.max(5000, Number(process.env.STATE_WRITE_TRANSACTION_MAX_WAIT_MS || 10000) || 10000);
const priceRetryQueueWriteChunkSize = Math.max(10, Math.min(500, Number(process.env.PRICE_RETRY_QUEUE_WRITE_CHUNK_SIZE || 100) || 100));
const ozonUnarchiveQueueWriteChunkSize = Math.max(10, Math.min(500, Number(process.env.OZON_UNARCHIVE_QUEUE_WRITE_CHUNK_SIZE || 100) || 100));
const stateJsonBackupOnPostgresWrite = process.env.STATE_JSON_BACKUP_ON_POSTGRES_WRITE === "true";
const exchangeRateTtlMs = 6 * 60 * 60 * 1000;
const rawOpenaiImageModel = normalizeOpenAiImageModelName(process.env.OPENAI_IMAGE_MODEL || "gpt-image-2");
const openaiImageModel = (() => {
  const normalized = cleanText(rawOpenaiImageModel).toLowerCase();
  return normalized.startsWith("gpt-image") || normalized.startsWith("dall-e") ? rawOpenaiImageModel : "gpt-image-2";
})();
const openaiTextModel = cleanText(process.env.OPENAI_TEXT_MODEL || process.env.AI_TEXT_MODEL || "gpt-5.4-mini");
const openaiImageSize = cleanText(process.env.OPENAI_IMAGE_SIZE || "1024x1024");
const ozonAiImageTargetPx = (() => {
  const raw = process.env.OZON_AI_IMAGE_TARGET_PX;
  if (raw === undefined || raw === null || String(raw).trim() === "") return 1000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 64) return 0;
  return Math.min(4096, Math.floor(n));
})();
const openaiImageQuality = cleanText(process.env.OPENAI_IMAGE_QUALITY || "auto");
const openaiImageFormat = cleanText(process.env.OPENAI_IMAGE_FORMAT || "png");
const openaiBaseUrl = cleanText(process.env.OPENAI_BASE_URL);
const openaiRelayUrl = cleanText(process.env.OPENAI_RELAY_URL);
const openaiRelaySecret = cleanText(process.env.OPENAI_RELAY_SECRET);
const openaiRelayTimeoutMs = Math.max(30_000, Number(process.env.OPENAI_RELAY_TIMEOUT_MS || 180_000) || 180_000);
const aiImageGenerationAttempts = Math.max(1, Math.min(5, Number(process.env.AI_IMAGE_GENERATION_ATTEMPTS || 3) || 3));
const aiImageGenerationRetryDelayMs = Math.max(1000, Number(process.env.AI_IMAGE_GENERATION_RETRY_DELAY_MS || 5000) || 5000);
const aiImageGenerationSequenceDelayMs = Math.max(0, Number(process.env.AI_IMAGE_GENERATION_SEQUENCE_DELAY_MS || 1200) || 1200);
const maskedSecretValue = "__masked__";
let openaiImageConfig = null;
{
  const raw = cleanText(process.env.OPENAI_IMAGE_CONFIG);
  if (raw) {
    try {
      openaiImageConfig = JSON.parse(raw);
    } catch (_e) {
      logger.warn("OPENAI_IMAGE_CONFIG is not valid JSON, ignored", { detail: raw.slice(0, 120) });
    }
  }
