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
const marketplaceAccountsPath = path.join(dataDir, "marketplace-accounts.json");
const auditLogPath = path.join(dataDir, "audit-log.jsonl");
const appSettingsPath = path.join(dataDir, "app-settings.json");
const appUsersPath = path.join(dataDir, "app-users.json");
const appDeletedUsersPath = path.join(dataDir, "app-users-deleted.json");
const priceRetryQueuePath = path.join(dataDir, "price-retry-queue.json");
const ozonUnarchiveQueuePath = path.join(dataDir, "ozon-unarchive-queue.json");
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
const autoSyncInitialDelaySeconds = Math.max(30, Number(process.env.AUTO_SYNC_INITIAL_DELAY_SECONDS || 120) || 120);
const autoZeroStockOnNoSupplier = process.env.AUTO_ZERO_STOCK_ON_NO_SUPPLIER !== "false";
const autoArchiveOnNoLinks = process.env.AUTO_ARCHIVE_ON_NO_LINKS === "true";
const keepUnlinkedProductsSellable = process.env.KEEP_UNLINKED_PRODUCTS_SELLABLE !== "false";
const autoRestoreOnSupplierReturn = process.env.AUTO_RESTORE_ON_SUPPLIER_RETURN !== "false";
const bullmqEnabled = process.env.BULLMQ_ENABLED === "true";
const redisUrl = cleanText(process.env.REDIS_URL);
const bullmqWorkerConcurrency = Math.max(1, Math.min(4, Number(process.env.BULLMQ_WORKER_CONCURRENCY || 1) || 1));
const bullmqLockDurationMs = Math.max(60000, Number(process.env.BULLMQ_LOCK_DURATION_MS || 300000) || 300000);
const bullmqStalledIntervalMs = Math.max(30000, Number(process.env.BULLMQ_STALLED_INTERVAL_MS || 60000) || 60000);
const bullmqMaxStalledCount = Math.max(1, Number(process.env.BULLMQ_MAX_STALLED_COUNT || 1) || 1);
const marketplaceQueueAutoPricePushEnabled = process.env.MARKETPLACE_QUEUE_AUTO_PRICE_PUSH_ENABLED === "true";
const priceMasterDeltaPricePushEnabled = process.env.PRICEMASTER_DELTA_PRICE_PUSH_ENABLED !== "false";
const priceMasterDeltaMaxChanges = Math.max(1, Number(process.env.PRICEMASTER_DELTA_MAX_CHANGES || 5000) || 5000);
const priceMasterDeltaMaxProducts = Math.max(1, Number(process.env.PRICEMASTER_DELTA_MAX_PRODUCTS || 2000) || 2000);
const autoPriceReconcileMaxProducts = Math.max(1, Number(process.env.AUTO_PRICE_RECONCILE_MAX_PRODUCTS || 12000) || 12000);
const autoPriceReconcileBatchSize = Math.max(50, Math.min(1000, Number(process.env.AUTO_PRICE_RECONCILE_BATCH_SIZE || 500) || 500));
const dailySyncTime = process.env.DAILY_SYNC_TIME || "11:00";
const dailySyncEnabled = process.env.DAILY_SYNC_ENABLED !== "false";
const dailySyncSendPrices = process.env.DAILY_SYNC_SEND_PRICES !== "false";
const pmDbPoolSize = Math.max(1, Number(process.env.PM_DB_POOL_SIZE || 8) || 8);
const pmDbConnectTimeoutMs = Math.max(1000, Number(process.env.PM_DB_CONNECT_TIMEOUT_MS || 10000) || 10000);
const warehouseViewCacheMs = Math.max(1000, Number(process.env.WAREHOUSE_VIEW_CACHE_MS || 120000) || 120000);
const ozonWarehouseListEnabled = process.env.OZON_WAREHOUSE_LIST_ENABLED === "true";
const ozonBaseUrl = "https://api-seller.ozon.ru";
const yandexBaseUrl = "https://api.partner.market.yandex.ru";
const yandexCleanupDeleteLimit = Math.max(1, Math.min(10000, Number(process.env.YANDEX_CLEANUP_DELETE_LIMIT || 10000) || 10000));
const yandexImportSendLimit = Math.max(1, Math.min(10000, Number(process.env.YANDEX_IMPORT_SEND_LIMIT || 5000) || 5000));
const yandexStockCampaignIds = new Set(["128820967"]);
const ozonUnarchiveDailyLimit = Math.max(1, Math.min(10000, Number(process.env.OZON_UNARCHIVE_DAILY_LIMIT || 100) || 100));
const ozonUnarchiveQueueBatchLimit = Math.max(1, Math.min(1000, Number(process.env.OZON_UNARCHIVE_QUEUE_BATCH_LIMIT || ozonUnarchiveDailyLimit) || ozonUnarchiveDailyLimit));
const ozonUnarchiveQueueAutoEnabled = process.env.OZON_UNARCHIVE_QUEUE_AUTO_ENABLED !== "false";
const ozonUnarchiveQueueAutoIntervalMinutes = Math.max(5, Number(process.env.OZON_UNARCHIVE_QUEUE_AUTO_INTERVAL_MINUTES || 30) || 30);
const ozonUnarchiveQueueAutoInitialDelaySeconds = Math.max(30, Number(process.env.OZON_UNARCHIVE_QUEUE_AUTO_INITIAL_DELAY_SECONDS || 180) || 180);
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
}
function openAiImageSupportsInputFidelity(model = openaiImageModel) {
  const normalized = cleanText(model).toLowerCase();
  if (!normalized || normalized === "gpt-image-2") return false;
  return normalized.startsWith("gpt-image-1");
}
function normalizeOpenAiImageModelName(model) {
  const normalized = cleanText(model);
  if (normalized.toLowerCase() === "gpt-image-1.5-high-fidelity") return "gpt-image-1.5";
  return normalized;
}
function isOpenAiImageModelName(model) {
  const normalized = cleanText(model).toLowerCase();
  return normalized.startsWith("gpt-image") || normalized.startsWith("dall-e");
}
function effectiveOpenAiImageModel(model, aiSettings = {}) {
  if (isCodexSaleAiProvider(aiSettings)) return "gpt-image-2";
  const normalized = normalizeOpenAiImageModelName(model);
  return isOpenAiImageModelName(normalized) ? normalized : (openaiImageModel || "gpt-image-2");
}
const ozonAiImageDefaultPrompt = cleanText(process.env.OZON_AI_IMAGE_PROMPT)
  || 'Сгенерируй продающее изображение для карточки товара на Ozon. Используй название товара: "{productName}". Сохрани узнаваемость товара с исходного фото, улучшив фон, свет, композицию и визуальную привлекательность для маркетплейса. Не добавляй логотипы, водяные знаки, недостоверные характеристики или лишний текст.';

const aiImageStudioPresets = [
  {
    id: "white-packshot",
    label: "White background",
    prompt: "Centered ecommerce packshot on a clean white background. Single perfume bottle only, full bottle visible from cap to base, not cropped, centered with 10-15% empty margin on every side. Preserve the exact bottle shape, cap color and existing label from the source photo. Remove boxes and outer packaging. No added text outside the product label, no watermark.",
  },
  {
    id: "premium-shadow",
    label: "Premium shadow",
    prompt: "Premium perfume bottle photo with elegant soft shadow and subtle reflective surface. Full bottle visible, centered, not cut off by frame edges, product occupies about 75-85% of canvas height. Keep only the original bottle recognizable. No box, no packaging, no props, no text overlays.",
  },
  {
    id: "lifestyle",
    label: "Lifestyle",
    prompt: "Minimal marketplace lifestyle scene. Single perfume bottle only, centered, full bottle visible with safe margins, soft daylight, product in focus. Remove any box or packaging from the source. No hands, no faces, no typography or marketing text.",
  },
  {
    id: "bottle-only",
    label: "Bottle only",
    prompt: "Marketplace image showing only the perfume bottle, clean studio composition. Entire bottle must fit inside the square image, centered, no crop at top, bottom, left, or right. If the source photo includes a box, remove it. Do not create packaging. No extra text outside the original bottle label.",
  },
  {
    id: "close-up",
    label: "Close-up",
    prompt: "Clean hero image of the perfume bottle with crisp highlights, but keep the full bottle visible from cap to base. Do not crop the bottle. Keep proportions and label area consistent with the source photo. Remove packaging. No decorative text, no headline, no logo outside the original label.",
  },
];

let dailySyncTimer = null;
let dailySyncNextRunAt = null;
let dailySyncPromise = null;
let manualWarehouseSyncPromise = null;
let manualWarehouseSyncState = {
  status: "idle",
  trigger: null,
  startedAt: null,
  finishedAt: null,
  result: null,
  error: null,
  progress: {
    percent: 0,
    stage: "Ожидание",
    meta: "Синхронизация ещё не запускалась.",
    processed: 0,
    total: 0,
  },
};
let autoSyncTimer = null;
let autoSyncRunning = false;
let autoSyncNextRunAt = null;
let ozonUnarchiveQueueAutoTimer = null;
let ozonUnarchiveQueueAutoRunning = false;
let ozonUnarchiveQueueProcessQueued = false;
let ozonUnarchiveQueueAutoNextRunAt = null;
let ozonUnarchiveQueueAutoLastRunAt = null;
let ozonUnarchiveQueueAutoLastResult = null;
let warehouseWritePromise = Promise.resolve();
let warehouseMemoryCache = null;
let warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
let warehousePostgresHashCache = new Map();
let warehousePostgresUpdatedAtCache = new Map();
let warehousePostgresWriteRunning = false;
let warehousePostgresWriteQueuedPayload = null;

function setManualWarehouseSyncProgress(patch = {}) {
  manualWarehouseSyncState = {
    ...manualWarehouseSyncState,
    progress: {
      ...(manualWarehouseSyncState.progress || {}),
      ...patch,
      updatedAt: new Date().toISOString(),
    },
  };
}
let warehousePostgresLinkBackfillPromise = null;
let warehousePostgresLinkBackfillDone = false;
let warehousePostgresBrandBackfillPromise = null;
let warehousePostgresBrandBackfillDone = false;
let priceMasterSnapshotMemoryCache = null;
let priceMasterArticleIndexCache = null;
let priceMasterSnapshotIndexCache = null;
const priceMasterLinkLookupCache = new Map();
const priceMasterLinkLookupCacheTtlMs = Math.max(1000, Number(process.env.LINK_SAVE_PM_CACHE_MS || 120000));
const priceMasterLinkLookupCacheMax = Math.max(100, Number(process.env.LINK_SAVE_PM_CACHE_MAX || 2000));
const priceMasterSearchCache = new Map();
const priceMasterSearchCacheTtlMs = Math.max(1000, Number(process.env.PRICEMASTER_SEARCH_CACHE_MS || 30000));
const priceMasterSearchCacheMax = Math.max(100, Number(process.env.PRICEMASTER_SEARCH_CACHE_MAX || 1000));
let warehousePostgresSummaryCache = null;
const warehousePostgresSummaryCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_PAGE_SUMMARY_CACHE_MS || 15000));
let warehousePostgresSuppliersCache = null;
let warehouseBrandListCache = null;
const warehouseBrandListCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_BRAND_LIST_CACHE_MS || 120000));
let warehousePostgresDetailCache = new Map();
const warehousePostgresDetailCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_DETAIL_CACHE_MS || 15000));
const warehouseFastPageCache = new Map();
const warehouseFastPageCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_FAST_PAGE_CACHE_MS || 5000));
const warehouseFastPageCacheMax = Math.max(20, Number(process.env.WAREHOUSE_FAST_PAGE_CACHE_MAX || 200));
const warehouseViewCache = new Map();
const warehouseViewBuilds = new Map();
let lastWarehouseViewSnapshot = null;
let ozonRequestChain = Promise.resolve();
let ozonLastRequestAt = 0;
const ozonWarehouseCache = new Map();
let immediateAutoPushTimer = null;
let immediateAutoPushAll = false;
const immediateAutoPushIds = new Set();
let immediateAutoPushChain = Promise.resolve();
let immediateAutoPushForce = false;
const changedPriceAutoPushAt = new Map();
let changedPriceAutoPushLastBatchAt = 0;
const detectedPriceAutoPushDefaultCooldownMs = 15 * 60_000;
const detectedPriceAutoPushDefaultBatchCooldownMs = 30_000;
let priceRetryTimer = null;
let priceRetryRunning = false;
let supplierCartAutoTimer = null;
let supplierCartAutoRunning = false;
let supplierCartAutoNextRunAt = null;
let supplierCartAutoLastRunAt = null;
let supplierCartAutoLastResult = null;
let marketplaceQueue = null;
let marketplaceWorker = null;
const warehouseProductMutationLocks = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeFetchError(error) {
  const parts = [error?.message || String(error)];
  const cause = error?.cause;
  if (cause) {
    const causeParts = [
      cause.code,
      cause.errno,
      cause.syscall,
      cause.address,
      cause.port,
      cause.message,
    ].filter(Boolean);
    if (causeParts.length) parts.push(`cause=${causeParts.join(" ")}`);
  }
  return parts.join("; ");
}

function formatRuNumber(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return "0";
  return new Intl.NumberFormat("ru-RU").format(number);
}

async function withWarehouseProductMutationLock(productIds = [], worker) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : [productIds])
    .map((id) => cleanText(id))
    .filter(Boolean)))
    .sort();
  const lockIds = ids.length ? ids : ["__warehouse_products__"];
  const previousLocks = lockIds.map((id) => warehouseProductMutationLocks.get(id)).filter(Boolean);
  const run = (async () => {
    if (previousLocks.length) await Promise.allSettled(previousLocks);
    return worker();
  })();
  for (const id of lockIds) warehouseProductMutationLocks.set(id, run);
  try {
    return await run;
  } finally {
    for (const id of lockIds) {
      if (warehouseProductMutationLocks.get(id) === run) warehouseProductMutationLocks.delete(id);
    }
  }
}
function warehouseViewCacheKey({ sync = false, limit = Number.POSITIVE_INFINITY, usdRate, refreshPrices = false } = {}) {
  const limitKey = Number.isFinite(Number(limit)) ? Number(limit) : "all";
  const rateKey = Number.isFinite(Number(usdRate)) && Number(usdRate) > 0 ? Number(usdRate) : "default";
  return JSON.stringify({ sync: Boolean(sync), refreshPrices: Boolean(refreshPrices), limit: limitKey, usdRate: rateKey });
}

function invalidateWarehouseViewCache() {
  warehouseViewCache.clear();
  warehouseViewBuilds.clear();
  warehousePostgresSummaryCache = null;
  warehousePostgresSuppliersCache = null;
  warehouseBrandListCache = null;
  warehousePostgresDetailCache.clear();
  warehouseFastPageCache.clear();
}

function warehouseProductIndexFor(warehouse = {}) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  if (warehouseMemoryProductIndexCache.products === products) return warehouseMemoryProductIndexCache.byId;
  const byId = new Map();
  products.forEach((product, index) => {
    if (product?.id) byId.set(String(product.id), index);
  });
  warehouseMemoryProductIndexCache = { products, byId };
  return byId;
}

app.use(express.json({ limit: "1mb" }));
app.use(compression({ threshold: 1024 }));

const uploadImages = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
    files: 10,
  },
  fileFilter: (_request, file, callback) => {
    if (/^image\/(png|jpe?g|webp|gif)$/i.test(file.mimetype)) return callback(null, true);
    const error = new Error("Можно загружать только изображения PNG, JPG, WEBP или GIF.");
    error.statusCode = 400;
    return callback(error);
  },
});

const pool = mysql.createPool({
  host: process.env.PM_DB_HOST,
  port: Number(process.env.PM_DB_PORT || 3306),
  user: process.env.PM_DB_USER,
  password: process.env.PM_DB_PASSWORD,
  database: process.env.PM_DB_NAME,
  waitForConnections: true,
  connectionLimit: pmDbPoolSize,
  connectTimeout: pmDbConnectTimeoutMs,
  decimalNumbers: true,
  dateStrings: true,
});

function base64Url(input) {
  return Buffer.from(input).toString("base64url");
}

function sign(value) {
  return crypto
    .createHmac("sha256", process.env.APP_SESSION_SECRET || "dev-secret")
    .update(value)
    .digest("base64url");
}

function timingSafeEqual(a, b) {
  const first = Buffer.from(String(a));
  const second = Buffer.from(String(b));
  if (first.length !== second.length) return false;
  return crypto.timingSafeEqual(first, second);
}

function parseCookies(header = "") {
  return Object.fromEntries(
    header
      .split(";")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const index = part.indexOf("=");
        return [decodeURIComponent(part.slice(0, index)), decodeURIComponent(part.slice(index + 1))];
      }),
  );
}

function promiseTimeout(ms, message = "operation_timeout") {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), Math.max(1, Number(ms) || 1));
  });
}

function pageTrace(label, startedAt) {
  if (process.env.WAREHOUSE_PAGE_TRACE !== "true") return;
  logger.info("warehouse page trace", { label, elapsedMs: Date.now() - startedAt });
}

function shouldUsePostgresStorage() {
  return postgresModeEnabled();
}

async function runWithPostgresFallback(label, postgresAction, fallbackAction) {
  if (!shouldUsePostgresStorage()) return fallbackAction();
  try {
    const prisma = getPrisma();
    if (!prisma) return fallbackAction();
    return await postgresAction(prisma);
  } catch (error) {
    if (!jsonFallbackEnabled()) throw error;
    logger.warn(`${label} postgres failed, using JSON fallback`, { detail: error?.message || String(error) });
    return fallbackAction();
  }
}

function toDateOrNull(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function configuredUsers() {
  const deletedUsernames = readDeletedAppUsernamesSync();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...readStoredAppUsersSync());
  return dedupeAppUsers(users).filter((user) => !user.disabled && !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

async function configuredUsersAsync() {
  const deletedUsernames = await readDeletedAppUsernames();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...await readStoredAppUsers());
  return dedupeAppUsers(users).filter((user) => !user.disabled && !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

async function configuredUsersForAdminAsync() {
  const deletedUsernames = await readDeletedAppUsernames();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...await readStoredAppUsers({ includeDisabled: true }));
  return dedupeAppUsers(users).filter((user) => !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

function normalizeAppRole(value, fallback = "manager") {
  return cleanText(value).toLowerCase() === "admin" ? "admin" : fallback;
}

const passwordHashPrefix = "scrypt";

function isPasswordHash(value) {
  return cleanText(value).startsWith(`${passwordHashPrefix}$`);
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("base64url");
  const hash = crypto.scryptSync(String(password || ""), salt, 64).toString("base64url");
  return `${passwordHashPrefix}$${salt}$${hash}`;
}

function verifyStoredPassword(password, storedPassword) {
  const stored = cleanText(storedPassword);
  if (!isPasswordHash(stored)) return timingSafeEqual(password, stored);
  const [, salt, expectedHash] = stored.split("$");
  if (!salt || !expectedHash) return false;
  const actual = crypto.scryptSync(String(password || ""), salt, 64);
  const expected = Buffer.from(expectedHash, "base64url");
  if (actual.length !== expected.length) return false;
  return crypto.timingSafeEqual(actual, expected);
}

function passwordForStorage(password) {
  const value = cleanText(password);
  if (!value || isPasswordHash(value)) return value;
  return hashPassword(value);
}

function normalizeAppUser(input = {}, { source = "local", protectedUser = false, defaultRole = "manager" } = {}) {
  const username = cleanText(input.username || input.user || input.login);
  const role = normalizeAppRole(input.role, defaultRole);
  return {
    username,
    password: cleanText(input.password),
    role,
    source: input.source || source,
    protected: Boolean(input.protected ?? protectedUser),
    disabled: Boolean(input.disabled),
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || new Date().toISOString(),
  };
}

function readEnvJsonUsers() {
  const rawUsers = cleanText(process.env.APP_USERS_JSON || "");
  if (!rawUsers) return [];
  try {
    const parsed = JSON.parse(rawUsers);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => normalizeAppUser(item, { source: "env-json", protectedUser: true, defaultRole: "manager" }))
      .filter((item) => item.username && item.password);
  } catch (error) {
    logger.warn("APP_USERS_JSON parse failed", { detail: error?.message || String(error) });
    return [];
  }
}

function readStoredAppUsersSync() {
  try {
    const parsed = JSON.parse(fsSync.readFileSync(appUsersPath, "utf8"));
    const users = Array.isArray(parsed.users) ? parsed.users : [];
    return users
      .map((item) => normalizeAppUser(item, { source: "local", defaultRole: "manager" }))
      .filter((item) => item.username && item.password);
  } catch (_error) {
    return [];
  }
}

function normalizeDeletedAppUser(input = {}) {
  const username = cleanText(input.username || input.user || input.login);
  if (!username) return null;
  return {
    username,
    deletedAt: input.deletedAt || new Date().toISOString(),
    deletedBy: cleanText(input.deletedBy || ""),
    reason: cleanText(input.reason || "hard_delete"),
  };
}

function readDeletedAppUsersSync() {
  try {
    const parsed = JSON.parse(fsSync.readFileSync(appDeletedUsersPath, "utf8"));
    const users = Array.isArray(parsed.users) ? parsed.users : [];
    return users.map(normalizeDeletedAppUser).filter(Boolean);
  } catch (_error) {
    return [];
  }
}

function readDeletedAppUsernamesSync() {
  return new Set(readDeletedAppUsersSync().map((item) => cleanText(item.username).toLowerCase()).filter(Boolean));
}

async function readDeletedAppUsers() {
  return readDeletedAppUsersSync();
}

async function readDeletedAppUsernames() {
  return new Set((await readDeletedAppUsers()).map((item) => cleanText(item.username).toLowerCase()).filter(Boolean));
}

async function writeDeletedAppUsers(users = []) {
  const byUser = new Map();
  for (const user of users.map(normalizeDeletedAppUser).filter(Boolean)) {
    byUser.set(cleanText(user.username).toLowerCase(), user);
  }
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    users: Array.from(byUser.values()).sort((a, b) => a.username.localeCompare(b.username)),
  };
  const temporaryPath = `${appDeletedUsersPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(temporaryPath, appDeletedUsersPath);
  return payload.users;
}

function appUserFromPostgres(row = {}) {
  return normalizeAppUser({
    username: row.username,
    password: row.passwordHash,
    role: row.role,
    source: row.source || "postgres",
    protected: row.protected,
    disabled: row.active === false,
    createdAt: row.createdAt ? row.createdAt.toISOString() : null,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : null,
  }, { source: row.source || "postgres", defaultRole: "manager" });
}

async function readStoredAppUsersFromPostgres(prisma, { includeDisabled = false } = {}) {
  const rows = await prisma.appUser.findMany({
    where: {
      protected: false,
      ...(includeDisabled ? {} : { active: true }),
    },
    orderBy: [
      { role: "asc" },
      { username: "asc" },
    ],
  });
  return rows.map(appUserFromPostgres).filter((item) => item.username && item.password);
}

async function readStoredAppUsers({ includeDisabled = false } = {}) {
  return runWithPostgresFallback(
    "read app users",
    (prisma) => readStoredAppUsersFromPostgres(prisma, { includeDisabled }),
    async () => readStoredAppUsersSync(),
  );
}

function dedupeAppUsers(users = []) {
  const result = new Map();
  for (const user of users) {
    if (!user?.username) continue;
    const key = user.username.toLowerCase();
    if (result.has(key) && result.get(key).protected) continue;
    result.set(key, user);
  }
  return Array.from(result.values());
}

function publicAppUser(user = {}) {
  return {
    username: user.username,
    role: user.role || "manager",
    source: user.source || "local",
    protected: Boolean(user.protected),
    disabled: Boolean(user.disabled),
    createdAt: user.createdAt || null,
    updatedAt: user.updatedAt || null,
  };
}

async function writeStoredAppUsers(users = []) {
  const normalized = dedupeAppUsers(users.map((item) => normalizeAppUser(item, { source: "local", defaultRole: "manager" })))
    .filter((item) => item.username && item.password)
    .map((item) => ({ ...item, source: "local", protected: false }));
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const desiredUsernames = normalized.map((item) => item.username);
      await prisma.$transaction(async (tx) => {
        await tx.appUser.updateMany({
          where: {
            protected: false,
            ...(desiredUsernames.length ? { username: { notIn: desiredUsernames } } : {}),
          },
          data: { active: false },
        });
        for (const user of normalized) {
          await tx.appUser.upsert({
            where: { username: user.username },
            create: {
              username: user.username,
              passwordHash: passwordForStorage(user.password),
              role: user.role === "admin" ? "admin" : "manager",
              active: !user.disabled,
              source: "postgres",
              protected: false,
              createdAt: toDateOrNull(user.createdAt) || new Date(),
              updatedAt: toDateOrNull(user.updatedAt) || new Date(),
            },
            update: {
              passwordHash: passwordForStorage(user.password),
              role: user.role === "admin" ? "admin" : "manager",
              active: !user.disabled,
              source: "postgres",
              protected: false,
            },
          });
        }
      });
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write app users postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    users: normalized.map((user) => ({ ...user, password: passwordForStorage(user.password) })),
  };
  const temporaryPath = `${appUsersPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, appUsersPath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
  return normalized;
}

function createSessionToken(user) {
  const username = typeof user === "string" ? user : user.username;
  const role = typeof user === "string" ? process.env.APP_ROLE || "admin" : user.role || "manager";
  const payload = base64Url(
    JSON.stringify({
      username,
      role,
      expiresAt: Date.now() + sessionTtlMs,
    }),
  );
  return `${payload}.${sign(payload)}`;
}

function readSession(request) {
  const token = parseCookies(request.headers.cookie)[sessionCookieName];
  if (!token || !token.includes(".")) return null;

  const [payload, signature] = token.split(".");
  if (!timingSafeEqual(sign(payload), signature)) return null;

  try {
    const session = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (!session.expiresAt || session.expiresAt < Date.now()) return null;
    return session;
  } catch (_error) {
    return null;
  }
}

const uploadSessionStats = new Map();
const UPLOAD_QUOTA_WINDOW_MS = 24 * 60 * 60 * 1000;

function uploadSessionKey(request) {
  const session = readSession(request);
  if (session?.username) return `user:${session.username}`;
  const token = parseCookies(request.headers.cookie)[sessionCookieName];
  if (token) return `sess:${crypto.createHash("sha256").update(token).digest("hex").slice(0, 24)}`;
  const ip = request.ip || request.socket?.remoteAddress || "unknown";
  return `ip:${ip}`;
}

function consumeUploadQuota(request, fileCount) {
  const max = Math.max(1, Number(process.env.UPLOAD_MAX_FILES_PER_SESSION || 200));
  const key = uploadSessionKey(request);
  const now = Date.now();
  let entry = uploadSessionStats.get(key);
  if (!entry || entry.resetAt < now) {
    entry = { count: 0, resetAt: now + UPLOAD_QUOTA_WINDOW_MS };
  }
  if (entry.count + fileCount > max) {
    const err = new Error(
      `Превышен лимит загрузок: не более ${max} файлов за 24 часа для этой сессии.`,
    );
    err.statusCode = 429;
    throw err;
  }
  entry.count += fileCount;
  uploadSessionStats.set(key, entry);
}

function isAdminSession(session) {
  return cleanText(session?.role).toLowerCase() === "admin";
}

function isAdminPagePath(pathname = "") {
  return ["/settings", "/settings.html", "/pricemaster", "/pricemaster.html"].includes(pathname);
}

async function pruneUploadDirectory() {
  const retentionDays = Number(process.env.UPLOAD_RETENTION_DAYS || 14);
  const maxMb = Number(process.env.UPLOAD_MAX_DISK_MB || 800);
  try {
    await fs.mkdir(uploadImageDir, { recursive: true });
  } catch (_e) {
    return;
  }
  let names;
  try {
    names = await fs.readdir(uploadImageDir);
  } catch (_e) {
    return;
  }
  const cutoff = Date.now() - Math.max(1, retentionDays) * 24 * 60 * 60 * 1000;
  const files = [];
  for (const name of names) {
    const fp = path.join(uploadImageDir, name);
    try {
      const st = await fs.stat(fp);
      if (st.isFile()) files.push({ fp, size: st.size, mtime: st.mtimeMs });
    } catch (_e) {
      /* skip */
    }
  }
  for (const f of files) {
    if (f.mtime < cutoff) await fs.unlink(f.fp).catch(() => {});
  }
  let alive = [];
  try {
    for (const name of await fs.readdir(uploadImageDir)) {
      const fp = path.join(uploadImageDir, name);
      try {
        const st = await fs.stat(fp);
        if (st.isFile()) alive.push({ fp, size: st.size, mtime: st.mtimeMs });
      } catch (_e) {
        /* skip */
      }
    }
  } catch (_e) {
    return;
  }
  let total = alive.reduce((sum, f) => sum + f.size, 0);
  const maxBytes = Math.max(10, maxMb) * 1024 * 1024;
  alive.sort((a, b) => a.mtime - b.mtime);
  while (total > maxBytes && alive.length) {
    const f = alive.shift();
    await fs.unlink(f.fp).catch(() => {});
    total -= f.size;
  }
}

function requireAuth(request, response, next) {
  const publicPaths = [
    "/login",
    "/login.html",
    "/styles.css",
    "/login.js",
    "/app.js",
    "/product.js",
    "/product-builder-ui.js",
    "/ozon-product.js",
    "/yandex-product.js",
    "/ozon-yandex-import.html",
    "/ozon-yandex-import.js",
    "/operations.html",
    "/operations.js",
    "/ai-drafts.html",
    "/ai-drafts.js",
    "/health",
  ];
  if (publicPaths.includes(request.path)) return next();
  if (request.path.startsWith("/uploads/images/")) return next();
  if (request.path.startsWith("/uploads/ai-images/")) return next();
  if (request.path.startsWith("/uploads/branding/")) return next();
  if (request.path === "/api/login" || request.path === "/api/session") return next();

  const session = readSession(request);
  if (session) {
    request.session = session;
    if (isAdminPagePath(request.path) && !isAdminSession(session)) {
      return response.redirect("/");
    }
    return next();
  }

  if (request.path.startsWith("/api/")) {
    return response.status(401).json({ error: "Требуется вход" });
  }

  return response.redirect("/login.html");
}

function requireAdmin(request, response, next) {
  if (isAdminSession(request.session)) return next();
  return response.status(403).json({ error: "Доступ только для администратора.", code: "admin_required" });
}

function requireStaff(request, response, next) {
  const role = cleanText(request.session?.role).toLowerCase();
  if (role === "admin" || role === "manager") return next();
  return response.status(403).json({ error: "Нужен доступ сотрудника.", code: "staff_required" });
}

function healthTimeout(promise, timeoutMs = 2500) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("health check timeout")), timeoutMs)),
  ]);
}

function cleanBuildVersion(value) {
  return String(value || "").trim().replace(/[^\w.-]/g, "").slice(0, 80) || "dev";
}

function readGitCommit() {
  try {
    return execFileSync("git", ["rev-parse", "--short", "HEAD"], {
      cwd: __dirname,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
  } catch (_error) {
    return "";
  }
}

async function collectHealthDetails({ deep = false } = {}) {
  const requiredPostgresTables = [
    "sales_automation_sku_states",
    "ozon_unarchive_queue",
    "supplier_cart_drafts",
    "supplier_cart_draft_rows",
    "supplier_picking_rows",
    "supplier_blocks",
    "brand_index_items",
    "finance_orders",
    "finance_expenses",
  ];
  const components = {
    storage: {
      mode: shouldUsePostgresStorage() ? "postgres" : "json",
      postgresMode: postgresModeEnabled(),
      jsonFallback: jsonFallbackEnabled(),
    },
    postgresTables: {
      required: requiredPostgresTables,
      missing: [],
      ok: shouldUsePostgresStorage() ? null : true,
    },
    postgres: {
      configured: Boolean(cleanText(process.env.DATABASE_URL)),
      enabled: shouldUsePostgresStorage(),
      ok: shouldUsePostgresStorage() ? null : true,
    },
    pricemaster: {
      configured: Boolean(cleanText(process.env.PM_DB_HOST) && cleanText(process.env.PM_DB_NAME)),
      ok: null,
    },
    redis: {
      configured: Boolean(redisUrl),
      enabled: bullmqEnabled,
      queueMode: bullmqEnabled && redisUrl && marketplaceQueue ? "bullmq" : "inline",
      ok: bullmqEnabled ? null : true,
    },
    ozon: {
      configured: getOzonAccounts().length > 0,
      accounts: getOzonAccounts().length,
      warehouseListEnabled: ozonWarehouseListEnabled,
    },
    yandex: {
      configured: getYandexShops().length > 0,
      shops: getYandexShops().length,
    },
  };

  if (components.postgres.enabled && deep) {
    try {
      await healthTimeout(getPrisma().$queryRaw`SELECT 1 AS ok`);
      components.postgres.ok = true;
      const rows = await healthTimeout(getPrisma().$queryRaw`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
      `);
      const present = new Set((Array.isArray(rows) ? rows : []).map((row) => cleanText(row.table_name)));
      components.postgresTables.present = Array.from(present);
      components.postgresTables.missing = requiredPostgresTables.filter((name) => !present.has(name));
      components.postgresTables.ok = components.postgresTables.missing.length === 0;
    } catch (error) {
      components.postgres.ok = false;
      components.postgres.error = error?.message || String(error);
      components.postgresTables.ok = false;
      components.postgresTables.error = error?.message || String(error);
    }
  }

  if (components.pricemaster.configured && deep) {
    try {
      const [rows] = await healthTimeout(pool.query("SELECT VERSION() AS version, NOW() AS serverTime"));
      components.pricemaster.ok = true;
      components.pricemaster.version = rows?.[0]?.version || null;
      components.pricemaster.serverTime = rows?.[0]?.serverTime || null;
    } catch (error) {
      components.pricemaster.ok = false;
      components.pricemaster.error = error?.message || String(error);
    }
  } else if (!components.pricemaster.configured) {
    components.pricemaster.ok = false;
  }

  if (bullmqEnabled && redisUrl && deep) {
    try {
      if (!marketplaceQueue) throw new Error("BullMQ is enabled but queue is not initialized");
      components.redis.counts = await healthTimeout(marketplaceQueue.getJobCounts("waiting", "active", "delayed", "failed"));
      components.redis.ok = true;
    } catch (error) {
      components.redis.ok = false;
      components.redis.error = error?.message || String(error);
    }
  }

  const required = [
    components.postgres.enabled ? components.postgres : null,
    components.postgres.enabled ? components.postgresTables : null,
    components.pricemaster.configured ? components.pricemaster : null,
    components.redis.enabled ? components.redis : null,
  ].filter(Boolean);
  const ok = required.every((component) => component.ok !== false);
  return { ok, service: "magic-vibes-warehouse", version: buildVersion, time: new Date().toISOString(), components };
}

app.get("/health", async (request, response) => {
  const deep = ["1", "true", "yes"].includes(String(request.query.deep || "").toLowerCase());
  response.json(await collectHealthDetails({ deep }));
});

registerAuthSessionRoutes(app, {
  loginLimiter,
  configuredUsersAsync,
  timingSafeEqual,
  verifyStoredPassword,
  createSessionToken,
  sessionCookieName,
  sessionTtlMs,
  readSession,
  isAdminSession,
  isSecureSessionCookie: () => String(process.env.PUBLIC_BASE_URL || "").startsWith("https://"),
});

app.use(requireAuth);
registerStaticAppRoutes(app, {
  express,
  path,
  publicDir,
  cacheControlForMutableAsset,
  serveIndexHtml,
  servePublicHtml,
  serveModernAppHtml,
});

function cleanLimit(value, fallback = 100, max = 500) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(parsed, max);
}

function uploadBaseUrl(request) {
  return String(process.env.PUBLIC_BASE_URL || `${request.protocol}://${request.get("host")}`).replace(/\/$/, "");
}

function isLocalhostName(hostname = "") {
  const host = cleanText(hostname).toLowerCase();
  return host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "0.0.0.0";
}

function marketplaceImageError(code, message, details = {}) {
  const error = new Error(message);
  error.statusCode = 400;
  error.code = code;
  Object.assign(error, details);
  return error;
}

function marketplaceImageLooksLikePlaceholder(value = "") {
  const text = cleanText(value).toLowerCase();
  if (!text) return true;
  const filename = text.split("?")[0].split("#")[0].split("/").pop() || text;
  return /placeholder|no[-_ ]?image|missing[-_ ]?image|empty[-_ ]?image|broken[-_ ]?image|default[-_ ]?image|sync|reload|refresh/.test(filename);
}

async function validateLocalUploadImagePath(pathname = "") {
  const normalizedPath = cleanText(pathname);
  const roots = [
    { prefix: "/uploads/ai-images/", dir: aiImageDir },
    { prefix: "/uploads/images/", dir: uploadImageDir },
    { prefix: "/uploads/branding/", dir: brandingImageDir },
  ];
  const match = roots.find((item) => normalizedPath.startsWith(item.prefix));
  if (!match) return;
  const relativeName = decodeURIComponent(normalizedPath.slice(match.prefix.length));
  if (!relativeName || relativeName !== path.basename(relativeName)) {
    throw marketplaceImageError("marketplace_image_url_invalid", "Некорректный путь AI-фото для отправки в маркетплейс.", { imagePath: normalizedPath });
  }
  const filePath = path.join(match.dir, relativeName);
  let stat = null;
  try {
    stat = await fs.stat(filePath);
  } catch (_error) {
    throw marketplaceImageError("marketplace_image_file_missing", "Файл AI-фото не найден на сервере. Сгенерируйте фото заново.", { imagePath: normalizedPath });
  }
  if (!stat.isFile() || stat.size < 1024) {
    throw marketplaceImageError("marketplace_image_file_empty", "AI-фото пустое или повреждено. Сгенерируйте фото заново.", { imagePath: normalizedPath, size: stat.size || 0 });
  }
  try {
    const meta = await sharp(filePath).metadata();
    if (!meta.width || !meta.height || meta.width < 80 || meta.height < 80) throw new Error("invalid_dimensions");
  } catch (error) {
    throw marketplaceImageError("marketplace_image_decode_failed", "AI-С„РѕС‚Рѕ РЅРµ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РєР°Рє РІР°Р»РёРґРЅРѕРµ РёР·РѕР±СЂР°Р¶РµРЅРёРµ.", { imagePath: normalizedPath, detail: error?.message || String(error) });
  }
}

async function validatePublicMarketplaceImageUrl(imageUrl = "") {
  const url = cleanText(imageUrl);
  if (!url) return;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 7000);
  let response = null;
  try {
    response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        Accept: "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
      },
      signal: controller.signal,
    });
  } catch (error) {
    throw marketplaceImageError(
      "marketplace_image_public_fetch_failed",
      "Публичный URL AI-фото не открывается с сервера. Яндекс тоже не сможет забрать картинку.",
      { imageUrl: url, detail: error?.message || String(error) },
    );
  } finally {
    clearTimeout(timeout);
  }
  const contentType = cleanText(response.headers.get("content-type")).toLowerCase();
  if (!response.ok && response.status !== 206) {
    throw marketplaceImageError(
      "marketplace_image_public_http_error",
      `Публичный URL AI-фото вернул HTTP ${response.status}. Отправка в маркетплейс остановлена.`,
      { imageUrl: url, status: response.status, contentType },
    );
  }
  if (!contentType.startsWith("image/")) {
    throw marketplaceImageError(
      "marketplace_image_public_not_image",
      "Публичный URL AI-фото открывается не как изображение. Проверьте PUBLIC_BASE_URL и доступность /uploads/ai-images.",
      { imageUrl: url, status: response.status, contentType },
    );
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length < 256) {
    throw marketplaceImageError(
      "marketplace_image_public_empty",
      "Публичный URL AI-фото возвращает слишком маленький файл. Сгенерируйте фото заново.",
      { imageUrl: url, status: response.status, contentType, size: bytes.length },
    );
  }
  try {
    const meta = await sharp(bytes).metadata();
    if (!meta.width || !meta.height || meta.width < 80 || meta.height < 80) throw new Error("invalid_dimensions");
  } catch (error) {
    throw marketplaceImageError("marketplace_image_decode_failed", "РџСѓР±Р»РёС‡РЅС‹Р№ URL РІРµСЂРЅСѓР» С„Р°Р№Р», РєРѕС‚РѕСЂС‹Р№ РЅРµ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РєР°Рє РёР·РѕР±СЂР°Р¶РµРЅРёРµ.", { imageUrl: url, status: response.status, contentType, detail: error?.message || String(error) });
  }
}

async function normalizeMarketplaceImageUrlForSend(rawUrl = "", request = null) {
  const sourceUrl = cleanText(rawUrl);
  if (marketplaceImageLooksLikePlaceholder(sourceUrl)) {
    throw marketplaceImageError("marketplace_image_placeholder", "Р¤РѕС‚Рѕ РїРѕС…РѕР¶Рµ РЅР° placeholder/Р·Р°РіР»СѓС€РєСѓ. РћС‚РїСЂР°РІРєР° РѕСЃС‚Р°РЅРѕРІР»РµРЅР°.", { imageUrl: sourceUrl });
  }
  if (!sourceUrl) {
    throw marketplaceImageError("marketplace_image_url_missing", "В AI-черновике нет URL фото для отправки.");
  }
  const publicBase = cleanText(process.env.PUBLIC_BASE_URL);
  const fallbackBase = request ? uploadBaseUrl(request) : publicBase;
  let parsed = null;
  try {
    parsed = sourceUrl.startsWith("/") ? new URL(sourceUrl, publicBase || fallbackBase) : new URL(sourceUrl);
  } catch (_error) {
    throw marketplaceImageError("marketplace_image_url_invalid", "AI-фото имеет некорректный URL для отправки.", { imageUrl: sourceUrl });
  }
  if (!/^https?:$/i.test(parsed.protocol)) {
    throw marketplaceImageError("marketplace_image_url_invalid", "Маркетплейс принимает только http/https URL фото.", { imageUrl: sourceUrl });
  }
  if (isLocalhostName(parsed.hostname)) {
    if (!publicBase) {
      throw marketplaceImageError(
        "marketplace_image_public_base_missing",
        "AI-фото сейчас ссылается на localhost. Укажите PUBLIC_BASE_URL=https://davidsklad.ru и повторите отправку.",
        { imageUrl: sourceUrl },
      );
    }
    let publicParsed = null;
    try {
      publicParsed = new URL(publicBase);
    } catch (_error) {
      throw marketplaceImageError("marketplace_image_public_base_invalid", "PUBLIC_BASE_URL некорректный. Укажите публичный адрес сайта, например https://davidsklad.ru.", { publicBase });
    }
    parsed.protocol = publicParsed.protocol;
    parsed.host = publicParsed.host;
  }
  if (isLocalhostName(parsed.hostname)) {
    throw marketplaceImageError("marketplace_image_url_not_public", "AI-фото должно иметь публичный URL, не localhost.", { imageUrl: parsed.toString() });
  }
  await validateLocalUploadImagePath(parsed.pathname);
  await validatePublicMarketplaceImageUrl(parsed.toString());
  return parsed.toString();
}

function imageExtension(file) {
  const byMime = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
  };
  return byMime[String(file.mimetype || "").toLowerCase()] || path.extname(file.originalname || "").toLowerCase() || ".img";
}

function imageMimeFromPath(filePath) {
  const extension = path.extname(filePath || "").toLowerCase();
  const byExtension = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
  };
  return byExtension[extension] || "image/png";
}

function supportedOpenAiSourceMime(mimeType) {
  return /^image\/(png|jpe?g|webp)$/i.test(cleanText(mimeType));
}

function fileNameFromImageMime(mimeType, fallback = "source.png") {
  const mime = cleanText(mimeType).toLowerCase();
  if (mime.includes("jpeg") || mime.includes("jpg")) return "source.jpg";
  if (mime.includes("webp")) return "source.webp";
  if (mime.includes("png")) return "source.png";
  return fallback;
}

function aiImageExtension(format = openaiImageFormat) {
  const normalized = cleanText(format || "png").toLowerCase();
  if (normalized === "jpeg" || normalized === "jpg") return ".jpg";
  if (normalized === "webp") return ".webp";
  return ".png";
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function parseMoneyValue(value) {
  if (value == null || value === "") return null;
  const raw = typeof value === "string" ? value.replace(/\s/g, "").replace(",", ".") : value;
  const number = Number(raw);
  if (!Number.isFinite(number) || number <= 0) return null;
  return number;
}

function pickOzonCabinetListedPrice(details = {}) {
  if (!details || typeof details !== "object") return null;
  return (
    details.marketingSellerPrice ||
    details.currentPrice ||
    details.marketingPrice ||
    details.retailPrice ||
    null
  );
}

function roundPrice(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number) || number <= 0) return 0;
  return Math.round(number);
}

function shouldSkipWarehousePriceSend({
  currentPrice,
  nextPrice,
  minDiffRub = 0,
  minDiffPct = 0,
  force = false,
} = {}) {
  const current = roundPrice(currentPrice);
  const next = roundPrice(nextPrice);
  if (!next) return { skip: true, reason: "not_ready" };
  if (force) return { skip: false, reason: null };
  const diffRub = Math.abs(next - current);
  if (current > 0 && diffRub <= 0) return { skip: true, reason: "unchanged" };
  const minRub = Math.max(0, Number(minDiffRub || 0) || 0);
  if (current > 0 && minRub > 0 && diffRub < minRub) {
    return { skip: true, reason: "min_diff_rub", diffRub, minDiffRub: minRub };
  }
  const minPct = Math.max(0, Number(minDiffPct || 0) || 0);
  const diffPct = current > 0 ? (diffRub / current) * 100 : 100;
  if (current > 0 && minPct > 0 && diffPct < minPct) {
    return { skip: true, reason: "min_diff_pct", diffPct, minDiffPct: minPct };
  }
  return { skip: false, reason: null };
}

function priceHistoryDedupeWindowMs() {
  return Math.max(60_000, Number(process.env.PRICE_HISTORY_DEDUPE_WINDOW_MS || 15 * 60_000) || 15 * 60_000);
}

function normalizePriceHistoryComparable(entry = {}) {
  return {
    productId: cleanText(entry.productId || entry.id) || "",
    marketplace: cleanText(entry.marketplace || "ozon").toLowerCase() || "ozon",
    target: cleanText(entry.target || entry.marketplace) || "",
    offerId: cleanText(entry.offerId || entry.offer_id),
    oldPrice: entry.oldPrice === undefined || entry.oldPrice === null ? null : roundPrice(entry.oldPrice),
    newPrice: roundPrice(entry.newPrice ?? entry.price ?? 0),
    status: cleanText(entry.status || (entry.error ? "failed" : "success")).toLowerCase(),
    error: cleanText(entry.error || ""),
    at: toDateOrNull(entry.createdAt || entry.at) || null,
  };
}

function isDuplicatePriceHistoryEntry(previous = {}, next = {}, { now = new Date(), windowMs = priceHistoryDedupeWindowMs() } = {}) {
  const left = normalizePriceHistoryComparable(previous);
  const right = normalizePriceHistoryComparable(next);
  if (!right.offerId || !right.newPrice) return false;
  if (left.productId && right.productId && left.productId !== right.productId) return false;
  if (left.marketplace !== right.marketplace) return false;
  if (left.target !== right.target) return false;
  if (left.offerId !== right.offerId) return false;
  if (left.oldPrice !== right.oldPrice) return false;
  if (left.newPrice !== right.newPrice) return false;
  if (left.status !== right.status) return false;
  if (left.error !== right.error) return false;
  if (!left.at) return false;
  return Math.abs(now.getTime() - left.at.getTime()) <= windowMs;
}

function parseBooleanSetting(value, fallback = true) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const text = String(value).trim().toLowerCase();
  if (["false", "0", "off", "no", "нет"].includes(text)) return false;
  if (["true", "1", "on", "yes", "да"].includes(text)) return true;
  return fallback;
}

function buildOzonPricePayload(item = {}) {
  const price = roundPrice(item.price);
  const payload = {
    offer_id: String(item.offerId || item.offer_id || "").trim(),
    price: String(price),
    currency_code: "RUB",
  };
  const forceOldPrice = item.forceOldPrice === true || item.retryReason === "ozon_old_price_adjusted";
  if (forceOldPrice || parseBooleanSetting(process.env.OZON_PRICE_PUSH_SET_OLD_PRICE, true)) {
    const oldPrice = resolveOzonOldPrice(price, item);
    payload.old_price = String(oldPrice);
  } else if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_RESET_OLD_PRICE, false)) {
    payload.old_price = "0";
  }
  if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_DISABLE_AUTO_ACTIONS, true)) {
    payload.auto_action_enabled = "DISABLED";
    payload.price_strategy_enabled = "DISABLED";
  }
  if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_SET_MIN_PRICE, false)) {
    payload.min_price = String(price);
  }
  return payload;
}

function resolveOzonOldPrice(price, item = {}) {
  const currentPrice = roundPrice(price);
  if (!currentPrice) return 0;
  const markupPct = Math.max(0, Number(process.env.OZON_OLD_PRICE_MARKUP_PCT || 20) || 20);
  const markupOldPrice = roundPrice(currentPrice * (1 + markupPct / 100));
  const requestedOldPrice = roundPrice(item.oldPrice ?? item.old_price ?? item.oldPriceRub ?? 0);
  return Math.max(currentPrice + 1, markupOldPrice, requestedOldPrice);
}

function normalizeMarketplaceAccount(input = {}, current = {}) {
  const marketplace = cleanText(input.marketplace || current.marketplace).toLowerCase() === "yandex" ? "yandex" : "ozon";
  const fallbackName = marketplace === "ozon" ? "Ozon" : "Yandex Market";
  return {
    id: cleanText(input.id || current.id) || `${marketplace}-${crypto.randomUUID().slice(0, 8)}`,
    marketplace,
    name: cleanText(input.name ?? current.name) || fallbackName,
    clientId: cleanText(input.clientId ?? input.client_id ?? current.clientId),
    apiKey: cleanText(input.apiKey ?? input.api_key ?? current.apiKey),
    businessId: cleanText(input.businessId ?? input.business_id ?? current.businessId),
    campaignId: cleanText(input.campaignId ?? input.campaign_id ?? current.campaignId),
    hidden: Boolean(input.hidden ?? current.hidden),
    syncEnabled: parseBooleanSetting(input.syncEnabled ?? input.sync_enabled, current.syncEnabled !== false),
    createdAt: current.createdAt || input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function readMarketplaceAccountsSync() {
  try {
    const data = JSON.parse(fsSync.readFileSync(marketplaceAccountsPath, "utf8"));
    const accounts = Array.isArray(data.accounts) ? data.accounts : [];
    return accounts.map((account) => normalizeMarketplaceAccount(account));
  } catch (_error) {
    return [];
  }
}

async function readMarketplaceAccounts() {
  return readMarketplaceAccountsSync();
}

async function writeMarketplaceAccounts(accounts) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = accounts.map((account) => normalizeMarketplaceAccount(account));
  await fs.writeFile(
    marketplaceAccountsPath,
    JSON.stringify({ updatedAt: new Date().toISOString(), accounts: normalized }, null, 2),
  );
  return normalized;
}

function getEnvOzonAccounts() {
  if (!process.env.OZON_CLIENT_ID || !process.env.OZON_API_KEY) return [];
  return [
    {
      id: "ozon",
      marketplace: "ozon",
      name: process.env.OZON_NAME || "Ozon",
      clientId: process.env.OZON_CLIENT_ID,
      apiKey: process.env.OZON_API_KEY,
      source: "env",
      readOnly: true,
    },
  ];
}

function getEnvYandexShops() {
  try {
    const shops = JSON.parse(process.env.YANDEX_SHOPS_JSON || "[]");
    if (!Array.isArray(shops)) return [];
    return shops.map((shop, index) => ({
      id: cleanText(shop.id) || `yandex-env-${index + 1}`,
      marketplace: "yandex",
      name: cleanText(shop.name) || "Yandex Market",
      businessId: cleanText(shop.businessId || shop.business_id),
      campaignId: cleanText(shop.campaignId || shop.campaign_id),
      apiKey: cleanText(shop.apiKey || shop.api_key),
      source: "env",
      readOnly: true,
    }));
  } catch (_error) {
    return [];
  }
}

function getMarketplaceAccounts() {
  const envAccounts = [...getEnvOzonAccounts(), ...getEnvYandexShops()];
  const localAccounts = readMarketplaceAccountsSync().map((account) => ({ ...account, source: "local", readOnly: false }));
  const localById = new Map(localAccounts.map((account) => [account.id, account]));
  const hiddenIds = new Set(localAccounts.filter((account) => account.hidden).map((account) => account.id));
  const usedIds = new Set();
  const mergedEnvAccounts = envAccounts
    .filter((account) => !hiddenIds.has(account.id))
    .map((account) => {
      usedIds.add(account.id);
      const override = localById.get(account.id);
      return override
        ? { ...account, ...override, source: "local", readOnly: false, inheritedFromEnv: true }
        : account;
    });
  const standaloneLocalAccounts = localAccounts.filter((account) => !account.hidden && !usedIds.has(account.id));
  return [...mergedEnvAccounts, ...standaloneLocalAccounts];
}

function getHiddenMarketplaceAccounts() {
  const envById = new Map([...getEnvOzonAccounts(), ...getEnvYandexShops()].map((account) => [account.id, account]));
  return readMarketplaceAccountsSync()
    .filter((account) => account.hidden)
    .map((account) => ({ ...(envById.get(account.id) || account), ...account, source: "local", readOnly: false }));
}

function maskSecret(value) {
  const text = cleanText(value);
  if (!text) return "";
  if (text.length <= 6) return `${text[0] || ""}***`;
  return `${text.slice(0, 3)}...${text.slice(-3)}`;
}

function sanitizeMarketplaceAccount(account = {}) {
  return {
    id: account.id,
    marketplace: account.marketplace,
    name: account.name,
    clientId: account.clientId ? maskSecret(account.clientId) : "",
    apiKey: account.apiKey ? maskSecret(account.apiKey) : "",
    businessId: account.businessId || "",
    campaignId: account.campaignId || "",
    configured: account.marketplace === "ozon"
      ? Boolean(account.clientId && account.apiKey)
      : Boolean(account.apiKey && account.businessId),
    source: account.source || "local",
    readOnly: Boolean(account.readOnly),
    inheritedFromEnv: Boolean(account.inheritedFromEnv),
    syncEnabled: account.syncEnabled !== false,
    updatedAt: account.updatedAt || account.createdAt || null,
  };
}

function findMarketplaceAccount(id) {
  const accountId = cleanText(id);
  if (!accountId) return null;
  return getMarketplaceAccounts().find((account) => account.id === accountId) || null;
}

async function testMarketplaceAccountConnection(account = {}) {
  const marketplace = cleanText(account.marketplace).toLowerCase();
  if (marketplace === "ozon") {
    if (!account.clientId || !account.apiKey) {
      const error = new Error("Для проверки Ozon нужны Client-Id и Api-Key.");
      error.statusCode = 400;
      throw error;
    }
    const data = await ozonRequest(
      "/v3/product/list",
      { filter: { visibility: "ALL" }, limit: 1, last_id: "" },
      account,
    );
    const sampleCount = Array.isArray(data?.result?.items) ? data.result.items.length : 0;
    return {
      ok: true,
      marketplace: "ozon",
      sampleCount,
      message: "Ozon подключен. Ключи работают, список товаров доступен.",
      checkedAt: new Date().toISOString(),
    };
  }

  if (marketplace === "yandex") {
    if (!account.businessId || !account.apiKey) {
      const error = new Error("Для проверки Yandex Market нужны Business ID и Api-Key.");
      error.statusCode = 400;
      throw error;
    }
    const mappings = await getYandexOfferMappings(account, 1);
    return {
      ok: true,
      marketplace: "yandex",
      sampleCount: Array.isArray(mappings) ? mappings.length : 0,
      message: "Yandex Market подключен. Ключи работают, каталог доступен.",
      checkedAt: new Date().toISOString(),
    };
  }

  const error = new Error("Неизвестный маркетплейс.");
  error.statusCode = 400;
  throw error;
}

function accountPayloadWithSecretFallback(body = {}, current = {}) {
  const payload = { ...body };
  if (!cleanText(payload.clientId ?? payload.client_id) && current.clientId) payload.clientId = current.clientId;
  if (!cleanText(payload.apiKey ?? payload.api_key) && current.apiKey) payload.apiKey = current.apiKey;
  if (!cleanText(payload.businessId ?? payload.business_id) && current.businessId) payload.businessId = current.businessId;
  if (!cleanText(payload.campaignId ?? payload.campaign_id) && current.campaignId) payload.campaignId = current.campaignId;
  return payload;
}

async function appendAudit(request, action, details = {}) {
  const entry = {
    at: new Date().toISOString(),
    user: request.session?.username || "system",
    role: request.session?.role || "admin",
    action,
    productId: details.productId || details.productIds || null,
    oldValue: details.oldValue ?? details.before ?? null,
    newValue: details.newValue ?? details.after ?? null,
    details,
  };
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const user = entry.user && entry.user !== "system"
        ? await prisma.appUser.findUnique({ where: { username: entry.user } }).catch(() => null)
        : null;
      await prisma.auditLog.create({
        data: {
          username: entry.user,
          userId: user?.id || null,
          action: entry.action,
          entityType: cleanText(details.entityType || action.split(".")[0]) || null,
          entityId: cleanText(details.entityId || details.productId || details.id || "") || null,
          oldValue: cloneAuditValue(entry.oldValue),
          newValue: cloneAuditValue(entry.newValue),
          details: cloneAuditValue(details) || {},
          createdAt: toDateOrNull(entry.at) || new Date(),
        },
      });
      if (!jsonFallbackEnabled()) return;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("append audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  await fs.appendFile(auditLogPath, `${JSON.stringify(entry)}\n`, "utf8");
}

function auditRowToEntry(row = {}) {
  return {
    at: row.createdAt ? row.createdAt.toISOString() : null,
    user: row.username,
    action: row.action,
    productId: row.entityId || row.details?.productId || null,
    oldValue: row.oldValue,
    newValue: row.newValue,
    details: row.details || {},
  };
}

async function readAudit(limit = 200) {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().auditLog.findMany({
        take: limit,
        orderBy: { createdAt: "desc" },
      });
      return rows.map(auditRowToEntry);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const content = await fs.readFile(auditLogPath, "utf8");
    return content.trim().split("\n").filter(Boolean).slice(-limit).reverse().map((line) => JSON.parse(line));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

async function readAuditSince(since) {
  if (shouldUsePostgresStorage()) {
    try {
      const sinceDate = toDateOrNull(since) || new Date(0);
      const rows = await getPrisma().auditLog.findMany({
        where: { createdAt: { gte: sinceDate } },
        orderBy: { createdAt: "asc" },
      });
      return rows.map(auditRowToEntry);
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read audit since postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const sinceMs = new Date(since).getTime();
    const content = await fs.readFile(auditLogPath, "utf8");
    return content
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line))
      .filter((entry) => new Date(entry.at).getTime() >= sinceMs);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function isAccountSyncEnabled(account = {}) {
  return account.syncEnabled !== false;
}

function getOzonAccounts({ includeSyncDisabled = false } = {}) {
  return getMarketplaceAccounts()
    .filter((account) => account.marketplace === "ozon")
    .filter((account) => includeSyncDisabled || isAccountSyncEnabled(account));
}

function getYandexShops({ includeSyncDisabled = false } = {}) {
  return getMarketplaceAccounts()
    .filter((account) => account.marketplace === "yandex")
    .filter((account) => includeSyncDisabled || isAccountSyncEnabled(account));
}

function getOzonAccountByTarget(targetId) {
  const accounts = getOzonAccounts();
  const target = cleanText(targetId || "");
  if (target === "ozon") return accounts[0] || null;
  return accounts.find((account) => (
    cleanText(account.id) === target
    || cleanText(account.clientId) === target
  )) || null;
}

function getYandexShopByTarget(targetId) {
  const shops = getYandexShops();
  const target = cleanText(targetId || "");
  const normalizedTarget = target.toLowerCase();
  if (target === "yandex") return shops[0] || null;
  const exact = shops.find((shop) => (
    cleanText(shop.id).toLowerCase() === normalizedTarget
    || cleanText(shop.campaignId).toLowerCase() === normalizedTarget
    || cleanText(shop.businessId).toLowerCase() === normalizedTarget
    || cleanText(shop.name).toLowerCase() === normalizedTarget
    || parseYandexCampaignIds(shop.campaignId).some((campaignId) => cleanText(campaignId).toLowerCase() === normalizedTarget)
    || parseYandexCampaignIds(shop.campaignId).some((campaignId) => `${cleanText(shop.id).toLowerCase()}-${cleanText(campaignId).toLowerCase()}` === normalizedTarget)
  ));
  if (exact) return exact;
  if (shops.length === 1 && target && target !== "ozon") return shops[0] || null;
  return null;
}

function matchesOzonTarget(targetId, accountId) {
  const target = cleanText(targetId || "");
  return target === cleanText(accountId || "") || target === "ozon";
}

function matchesYandexTarget(targetId, shopId) {
  const target = cleanText(targetId || "");
  const shop = getYandexShopByTarget(target);
  if (shop) return cleanText(shop.id).toLowerCase() === cleanText(shopId || "").toLowerCase();
  return target.toLowerCase() === cleanText(shopId || "").toLowerCase() || target.toLowerCase() === "yandex";
}

function marketplaceTargets() {
  const ozonAccounts = getOzonAccounts();
  const yandexShops = getYandexShops();
  return [
    ...(ozonAccounts.length
      ? ozonAccounts.map((account) => ({
          id: account.id,
          marketplace: "ozon",
          name: account.name || "Ozon",
          configured: Boolean(account.clientId && account.apiKey),
          source: account.source,
          readOnly: Boolean(account.readOnly),
        }))
      : [{ id: "ozon", marketplace: "ozon", name: "Ozon", configured: false }]),
    ...(yandexShops.length
      ? yandexShops.map((shop) => ({
          id: shop.id,
          marketplace: "yandex",
          name: shop.name || "Yandex Market",
          businessId: shop.businessId,
          configured: Boolean(shop.apiKey && shop.businessId),
          source: shop.source,
          readOnly: Boolean(shop.readOnly),
        }))
      : []),
  ];
}

function targetById(targetId) {
  if (targetId === "ozon") {
    const [account] = getOzonAccounts();
    if (account) return { id: account.id, marketplace: "ozon", name: account.name || "Ozon" };
  }
  if (targetId === "yandex") {
    const [shop] = getYandexShops();
    if (shop) return { id: shop.id, marketplace: "yandex", name: shop.name || "Yandex Market", businessId: shop.businessId };
  }
  return marketplaceTargets().find((target) => target.id === targetId) || null;
}

function isWarehouseProductTargetEnabled(product = {}) {
  const marketplace = cleanText(product.marketplace || "").toLowerCase();
  if (!marketplace) return true;
  if (marketplace === "yandex") {
    const target = cleanText(product.target).toLowerCase();
    const accounts = getYandexShops({ includeSyncDisabled: true });
    if (!accounts.length) return true;
    if (target === "yandex" || target.startsWith("yandex")) return true;
    return accounts.some((account) => matchesYandexTarget(product.target, account.id));
  }
  const accounts = getOzonAccounts();
  if (!accounts.length) return marketplace !== "yandex";
  return accounts.some((account) => (
    matchesOzonTarget(product.target, account.id)
  ));
}

function calculateRubPrice(usdPrice, usdRate, markupCoefficient) {
  return roundPrice(Number(usdPrice || 0) * Number(usdRate || 0) * Number(markupCoefficient || 0));
}

function normalizePriceMasterPrice(rawPrice, usdRate, currency = "USD") {
  const originalPrice = Number(rawPrice || 0);
  const rate = Number(usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const mode = cleanText(currency || "USD").toUpperCase();
  const isRub = mode === "RUB" || mode === "RUR";
  const price = isRub && rate > 0 ? originalPrice / rate : originalPrice;
  return {
    price: Number(Number(price || 0).toFixed(4)),
    originalPrice,
    sourceCurrency: isRub ? "RUB" : "USD",
    convertedFromRub: Boolean(isRub),
  };
}

function cleanText(value) {
  return String(value || "").trim();
}

function extractBrandFromAttributes(attributes = []) {
  for (const attribute of attributes || []) {
    const id = Number(attribute.id || attribute.attribute_id || 0);
    const name = cleanText(attribute.name || attribute.attribute_name || attribute.attributeName).toLowerCase();
    if (id !== 85 && name !== "бренд" && name !== "brand") continue;
    const values = Array.isArray(attribute.values) ? attribute.values : [];
    const fromValues = values
      .map((item) => cleanText(item.value || item.name || item.text))
      .find(Boolean);
    return fromValues || cleanText(attribute.value);
  }
  return "";
}

function extractBrandFromNestedAttributes(value = {}, depth = 0) {
  if (!value || depth > 6) return "";
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = extractBrandFromNestedAttributes(item, depth + 1);
      if (found) return found;
    }
    return "";
  }
  if (typeof value !== "object") return "";
  const directName = cleanText(value.name || value.attribute_name || value.attributeName || value.title).toLowerCase();
  const directId = Number(value.id || value.attribute_id || value.attributeId || 0);
  if (directId === 85 || directName === "бренд" || directName === "brand" || directName.includes("brand")) {
    const values = Array.isArray(value.values) ? value.values : [];
    const fromValues = values
      .map((item) => cleanText(item?.value || item?.name || item?.text))
      .find(Boolean);
    const directValue = cleanText(value.value || value.text);
    if (fromValues || directValue) return fromValues || directValue;
  }
  for (const [key, child] of Object.entries(value)) {
    const normalizedKey = normalizeSearchText(key);
    if (normalizedKey.includes("brand") || normalizedKey.includes("vendor") || normalizedKey.includes("бренд")) {
      if (typeof child === "string" || typeof child === "number") {
        const text = cleanText(child);
        if (text) return text;
      }
    }
    const found = extractBrandFromNestedAttributes(child, depth + 1);
    if (found) return found;
  }
  return "";
}

function flattenAttributeText(attributes = []) {
  return (attributes || [])
    .flatMap((attribute) => [
      attribute?.name,
      attribute?.attribute_name,
      attribute?.attributeName,
      attribute?.value,
      ...(Array.isArray(attribute?.values) ? attribute.values.flatMap((item) => [item?.value, item?.name, item?.text]) : []),
    ])
    .map(cleanText)
    .filter(Boolean)
    .join(" ");
}

function collectWarehouseBrandCandidates(value, { depth = 0, key = "" } = {}) {
  if (depth > 8 || value === null || value === undefined) return [];
  const normalizedKey = normalizeSearchText(key);
  const keyLooksLikeBrand =
    normalizedKey.includes("brand")
    || normalizedKey.includes("vendor")
    || normalizedKey.includes("manufacturer")
    || normalizedKey.includes("trademark")
    || normalizedKey.includes("бренд")
    || normalizedKey.includes("производитель");
  if (typeof value === "string" || typeof value === "number") {
    const text = cleanText(value);
    return keyLooksLikeBrand && text ? [text] : [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => collectWarehouseBrandCandidates(item, { depth: depth + 1, key }));
  }
  if (typeof value !== "object") return [];
  const direct = [];
  if (keyLooksLikeBrand) {
    const fromNamedValue = cleanText(value.value || value.text || value.name || value.title);
    if (fromNamedValue) direct.push(fromNamedValue);
  }
  if (Array.isArray(value.values)) {
    const attributeName = normalizeSearchText(value.name || value.attribute_name || value.attributeName || value.id || value.attributeId || key);
    const attributeLooksLikeBrand = attributeName.includes("brand") || attributeName.includes("vendor") || attributeName.includes("бренд") || attributeName.includes("производитель");
    if (attributeLooksLikeBrand) {
      direct.push(...value.values.flatMap((item) => [item?.value, item?.name, item?.text]).map(cleanText).filter(Boolean));
    }
  }
  return [
    ...direct,
    ...Object.entries(value).flatMap(([childKey, childValue]) => collectWarehouseBrandCandidates(childValue, { depth: depth + 1, key: childKey })),
  ];
}

function resolveWarehouseBrandCandidates(product = {}) {
  return Array.from(new Map([
    product.brand,
    product.vendor,
    product.brandName,
    product.raw?.brand,
    product.raw?.vendor,
    product.raw?.brandName,
    product.ozon?.vendor,
    product.ozon?.brand,
    product.ozon?.brandName,
    product.yandex?.vendor,
    product.yandex?.brand,
    product.yandex?.brandName,
    extractBrandFromAttributes(product.ozon?.attributes),
    extractBrandFromAttributes(product.yandex?.attributes),
    extractBrandFromNestedAttributes(product.ozon),
    extractBrandFromNestedAttributes(product.yandex),
    extractBrandFromNestedAttributes(product.rawPayload),
    extractBrandFromNestedAttributes(product.raw),
  ].map(cleanText).filter(Boolean).map((item) => [item.toLowerCase(), item])).values());
}

function resolveWarehouseBrand(product = {}) {
  return resolveWarehouseBrandCandidates(product)[0] || "";
}

function normalizedBrandIndexKey(value = "") {
  return normalizeSearchText(value).replace(/\s+/g, " ").trim();
}

function warehouseBrandIndexRowsForProduct(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const candidates = [];
  const push = (value, source, confidence = 80) => {
    const displayBrand = cleanText(value);
    const normalizedBrand = normalizedBrandIndexKey(displayBrand);
    if (!displayBrand || !normalizedBrand || normalizedBrand.length < 2) return;
    candidates.push({
      normalizedBrand,
      displayBrand,
      productId: normalized.id,
      marketplace: normalized.marketplace === "yandex" ? "yandex" : "ozon",
      offerId: normalized.offerId || normalized.id,
      source,
      confidence,
    });
  };
  push(normalized.brand, "brand", 100);
  push(normalized.vendor, "vendor", 95);
  push(normalized.brandName, "brand_name", 95);
  push(normalized.raw?.brand, "raw", 90);
  push(normalized.raw?.vendor, "raw", 88);
  push(normalized.ozon?.brand || normalized.ozon?.vendor || normalized.ozon?.brandName, "ozon_attribute", 90);
  push(normalized.yandex?.brand || normalized.yandex?.vendor || normalized.yandex?.brandName, "yandex_attribute", 90);
  for (const brand of resolveWarehouseBrandCandidates(normalized)) push(brand, "raw", 80);
  const unique = new Map();
  for (const row of candidates) {
    const key = `${row.normalizedBrand}|${row.productId}|${row.source}`;
    if (!unique.has(key)) unique.set(key, row);
  }
  return Array.from(unique.values());
}

async function rebuildWarehouseBrandIndexPostgres(prisma, { limit = 100000 } = {}) {
  if (!prisma?.brandIndexItem) return { ok: false, skipped: true, reason: "brand_index_model_missing" };
  const rows = await prisma.warehouseProduct.findMany({
    where: enabledWarehouseTargetWhere(),
    select: { id: true, marketplace: true, target: true, offerId: true, productId: true, name: true, brand: true, raw: true },
    take: Math.max(100, Math.min(200000, Number(limit || 100000) || 100000)),
    orderBy: [{ updatedAt: "desc" }],
  });
  let indexed = 0;
  await prisma.brandIndexItem.deleteMany({});
  for (const batch of chunkArray(rows, 500)) {
    const data = batch.flatMap((row) => warehouseBrandIndexRowsForProduct(productFromPostgres({ ...row, links: [] })));
    if (!data.length) continue;
    const result = await prisma.brandIndexItem.createMany({ data, skipDuplicates: true });
    indexed += result.count || 0;
  }
  warehouseBrandListCache = null;
  return { ok: true, scanned: rows.length, indexed };
}

async function brandIndexProductIdsForFilterPostgres(prisma, brandFilter = "") {
  const normalizedBrand = normalizedBrandIndexKey(brandFilter);
  if (!normalizedBrand || !prisma?.brandIndexItem) return [];
  let rows = await prisma.brandIndexItem.findMany({
    where: { normalizedBrand: { contains: normalizedBrand, mode: "insensitive" } },
    select: { productId: true },
    take: 50000,
  });
  if (!rows.length) {
    await rebuildWarehouseBrandIndexPostgres(prisma, { limit: Number(process.env.WAREHOUSE_BRAND_INDEX_REBUILD_LIMIT || 100000) || 100000 });
    rows = await prisma.brandIndexItem.findMany({
      where: { normalizedBrand: { contains: normalizedBrand, mode: "insensitive" } },
      select: { productId: true },
      take: 50000,
    });
  }
  return Array.from(new Set(rows.map((row) => row.productId).filter(Boolean)));
}

function warehouseBrandSearchHaystack(product = {}) {
  return [
    ...resolveWarehouseBrandCandidates(product),
    product.name,
    product.ozon?.name,
    product.yandex?.name,
    flattenAttributeText(product.ozon?.attributes),
    flattenAttributeText(product.yandex?.attributes),
  ]
    .map((value) => normalizeSearchText(value))
    .filter(Boolean)
    .join(" ");
}

function warehouseBrandDeepHaystack(product = {}) {
  const parts = [];
  const visit = (value, key = "", depth = 0) => {
    if (depth > 6 || value === null || value === undefined) return;
    const normalizedKey = normalizeSearchText(key);
    const keyLooksLikeBrand =
      normalizedKey.includes("brand")
      || normalizedKey.includes("vendor")
      || normalizedKey.includes("manufacturer")
      || normalizedKey.includes("trademark")
      || normalizedKey.includes("бренд")
      || normalizedKey.includes("производитель");
    if (keyLooksLikeBrand && (typeof value === "string" || typeof value === "number")) {
      parts.push(value);
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item) => visit(item, key, depth + 1));
      return;
    }
    if (typeof value === "object") {
      Object.entries(value).forEach(([childKey, childValue]) => visit(childValue, childKey, depth + 1));
    }
  };
  visit(product);
  return parts.map((value) => normalizeSearchText(value)).filter(Boolean).join(" ");
}

function warehouseBrandMatches(product = {}, brandFilter = "") {
  const needle = normalizeSearchText(brandFilter);
  if (!needle) return true;
  if (warehouseBrandSearchHaystack(product).includes(needle)) return true;
  if (process.env.WAREHOUSE_ENABLE_DEEP_BRAND_SCAN !== "true") return false;
  return warehouseBrandDeepHaystack(product).includes(needle);
}

function firstImageUrl(value) {
  if (Array.isArray(value)) return cleanText(value[0]);
  const text = cleanText(value);
  if (!text) return "";
  return text.split(/\r?\n|,/).map(cleanText).find(Boolean) || "";
}

function localPublicFilePathFromUrl(value, request) {
  const raw = cleanText(value);
  if (!raw) return "";
  let pathname = raw;
  if (/^https?:\/\//i.test(raw)) {
    try {
      const parsed = new URL(raw);
      const base = new URL(uploadBaseUrl(request));
      if (parsed.origin !== base.origin) return "";
      pathname = parsed.pathname;
    } catch (_error) {
      return "";
    }
  }
  const decodedPathname = decodeURIComponent(pathname.split("?")[0] || "");
  if (!decodedPathname.startsWith("/uploads/")) return "";
  const normalized = path.normalize(decodedPathname.replace(/^\/+/, ""));
  const fullPath = path.join(publicDir, normalized);
  const relative = path.relative(publicDir, fullPath);
  if (relative.startsWith("..") || path.isAbsolute(relative)) return "";
  return fullPath;
}

function normalizeSearchText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function includesKeyword(value, keyword) {
  const search = normalizeSearchText(keyword);
  if (!search) return true;
  const source = normalizeSearchText(value);
  return search.split(" ").every((token) => source.includes(token));
}

function exactPriceMasterNameMatches(value, expected) {
  const left = normalizeSearchText(value);
  const right = normalizeSearchText(expected);
  return Boolean(left && right && left === right);
}

function priceMasterRowMatchesLink(row = {}, link = {}) {
  const supplierOk =
    !link.supplierName ||
    normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
  const partnerOk = !link.partnerId || String(row.partnerId || "") === String(link.partnerId);
  const keywordOk = includesKeyword(row.name, link.keyword);
  if (!supplierOk || !partnerOk || !keywordOk) return false;
  if (link.matchType === "selected_row") {
    if (link.sourceRowId && String(row.rowId || "") === String(link.sourceRowId)) return true;
    if (link.exactName) return exactPriceMasterNameMatches(row.name, link.exactName);
    return false;
  }
  if (link.matchType === "exact_name") {
    return exactPriceMasterNameMatches(row.name, link.exactName || link.article);
  }
  return true;
}

function hasObjectData(value) {
  if (!value || typeof value !== "object") return false;
  return Object.values(value).some((item) => {
    if (Array.isArray(item)) return item.length > 0;
    if (item && typeof item === "object") return hasObjectData(item);
    return item !== undefined && item !== null && item !== "";
  });
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value || {}).filter(([, item]) => {
      if (Array.isArray(item)) return item.length > 0;
      if (item && typeof item === "object") return hasObjectData(item);
      return item !== undefined && item !== null && item !== "";
    }),
  );
}

function hasDraftInput(input = {}) {
  return Boolean(input && typeof input === "object" && Object.values(input).some((value) => {
    if (Array.isArray(value)) return value.length > 0;
    if (value && typeof value === "object") return hasObjectData(value);
    return value !== undefined && value !== null && value !== "";
  }));
}

function normalizeOzonDraft(input = {}) {
  if (!hasDraftInput(input)) return {};
  const draft = compactObject({
    offerId: cleanText(input.offerId || input.offer_id),
    name: cleanText(input.name),
    vendor: cleanText(input.vendor || input.brand),
    description: cleanText(input.description),
    marketCategoryId: Number(input.marketCategoryId || input.market_category_id || 0) || undefined,
    categoryId: Number(input.categoryId || input.category_id || 0) || undefined,
    typeId: Number(input.typeId || input.type_id || input.descriptionTypeId || input.description_type_id || 0) || undefined,
    price: Number(input.price || 0) || undefined,
    minPrice: Number(input.minPrice || input.min_price || 0) || undefined,
    oldPrice: Number(input.oldPrice || input.old_price || 0) || undefined,
    marketingPrice: Number(input.marketingPrice || input.marketing_price || 0) || undefined,
    marketingSellerPrice: Number(input.marketingSellerPrice || input.marketing_seller_price || 0) || undefined,
    retailPrice: Number(input.retailPrice || input.retail_price || 0) || undefined,
    currencyCode: cleanText(input.currencyCode || input.currency_code || "RUB"),
    vat: input.vat !== undefined ? String(input.vat) : undefined,
    barcode: cleanText(input.barcode),
    barcodes: splitList(input.barcodes),
    depth: Number(input.depth || 0) || undefined,
    width: Number(input.width || 0) || undefined,
    height: Number(input.height || 0) || undefined,
    dimensionUnit: cleanText(input.dimensionUnit || input.dimension_unit || "mm"),
    weight: Number(input.weight || 0) || undefined,
    weightUnit: cleanText(input.weightUnit || input.weight_unit || "g"),
    primaryImage: cleanText(input.primaryImage || input.primary_image),
    images: splitList(input.images),
    images360: splitList(input.images360),
    colorImage: cleanText(input.colorImage || input.color_image),
    attributes: parseJsonField(input.attributesJson ?? input.attributes, []),
    complexAttributes: parseJsonField(input.complexAttributesJson ?? input.complex_attributes, []),
    extra: parseJsonField(input.extraJson ?? input.extra, {}),
  });

  return hasObjectData(draft) ? draft : {};
}

function normalizeYandexDraft(input = {}) {
  if (!hasDraftInput(input)) return {};
  const draft = compactObject({
    offerId: cleanText(input.offerId || input.offer_id),
    name: cleanText(input.name),
    description: cleanText(input.description),
    marketCategoryId: Number(input.marketCategoryId || input.market_category_id || input.categoryId || 0) || undefined,
    vendor: cleanText(input.vendor || input.brand),
    pictures: splitList(input.pictures || input.images),
    barcodes: splitList(input.barcodes || input.barcode),
    price: Number(input.price || 0) || undefined,
    extra: parseJsonField(input.yandexExtraJson ?? input.extra, {}),
  });

  return hasObjectData(draft) ? draft : {};
}

function normalizeProductExports(exports = {}) {
  if (!exports || typeof exports !== "object") return {};
  return Object.fromEntries(
    Object.entries(exports)
      .filter(([target]) => target)
      .map(([target, value]) => [
        target,
        compactObject({
          status: cleanText(value?.status),
          sentAt: value?.sentAt || null,
          error: cleanText(value?.error),
          targetName: cleanText(value?.targetName),
        }),
      ]),
  );
}

function normalizeAiImageDraft(input = {}) {
  if (!input || typeof input !== "object") return null;
  const status = cleanText(input.status || "pending").toLowerCase();
  const allowedStatus = new Set(["pending", "approved", "rejected"]);
  const draft = compactObject({
    id: cleanText(input.id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "pending",
    prompt: cleanText(input.prompt),
    productName: cleanText(input.productName || input.product_name),
    sourceImageUrl: cleanText(input.sourceImageUrl || input.source_image_url),
    resultUrl: cleanText(input.resultUrl || input.result_url || input.url),
    batchId: cleanText(input.batchId || input.batch_id),
    variantIndex: Number(input.variantIndex || input.variant_index || 0) || 0,
    variantTotal: Number(input.variantTotal || input.variant_total || 0) || 0,
    presetId: cleanText(input.presetId || input.preset_id),
    presetLabel: cleanText(input.presetLabel || input.preset_label),
    layout: cleanText(input.layout),
    model: cleanText(input.model),
    size: cleanText(input.size),
    quality: cleanText(input.quality),
    format: cleanText(input.format),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    reviewedAt: input.reviewedAt || input.reviewed_at || null,
    sentAt: input.sentAt || input.sent_at || null,
    sentMarketplace: cleanText(input.sentMarketplace || input.sent_marketplace),
    sendResult: input.sendResult && typeof input.sendResult === "object" ? input.sendResult : undefined,
  });
  return draft.resultUrl || draft.sourceImageUrl || draft.prompt ? draft : null;
}

function normalizeAiImageDrafts(input = []) {
  const drafts = Array.isArray(input) ? input : [];
  return drafts.map(normalizeAiImageDraft).filter(Boolean).slice(-50);
}

function aiRecommendationText(input) {
  if (input === undefined || input === null) return "";
  if (typeof input === "string" || typeof input === "number" || typeof input === "boolean") {
    const value = cleanText(input);
    return value === "[object Object]" ? "" : value;
  }
  if (Array.isArray(input)) return input.map(aiRecommendationText).filter(Boolean).join(": ");
  if (typeof input === "object") {
    return aiRecommendationText(
      input.message
        || input.text
        || input.name
        || input.title
        || input.description
        || input.comment
        || input.reason
        || input.recommendation
        || input.value
        || input.details
        || input.error,
    );
  }
  return "";
}

function normalizeAiContentDraft(input = {}) {
  if (!input || typeof input !== "object") return null;
  const status = cleanText(input.status || "pending").toLowerCase();
  const allowedStatus = new Set(["pending", "approved", "rejected"]);
  const draft = compactObject({
    id: cleanText(input.id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "pending",
    marketplace: cleanText(input.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex",
    source: cleanText(input.source || "manual"),
    name: cleanText(input.name),
    vendor: cleanText(input.vendor || input.brand),
    description: cleanText(input.description),
    bulletPoints: Array.isArray(input.bulletPoints || input.bullets)
      ? (input.bulletPoints || input.bullets).map((item) => cleanText(item)).filter(Boolean).slice(0, 12)
      : [],
    seoKeywords: Array.isArray(input.seoKeywords || input.keywords)
      ? (input.seoKeywords || input.keywords).map((item) => cleanText(item)).filter(Boolean).slice(0, 20)
      : [],
    qualityBefore: Number.isFinite(Number(input.qualityBefore)) ? Number(input.qualityBefore) : undefined,
    recommendations: Array.isArray(input.recommendations)
      ? input.recommendations.map(aiRecommendationText).filter(Boolean).slice(0, 20)
      : [],
    model: cleanText(input.model),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    reviewedAt: input.reviewedAt || input.reviewed_at || null,
    sentAt: input.sentAt || input.sent_at || null,
    sentMarketplace: cleanText(input.sentMarketplace || input.sent_marketplace),
    sendResult: input.sendResult && typeof input.sendResult === "object" ? input.sendResult : undefined,
  });
  return draft.name || draft.description || draft.vendor || draft.bulletPoints?.length ? draft : null;
}

function normalizeAiContentDrafts(input = []) {
  const drafts = Array.isArray(input) ? input : [];
  return drafts.map(normalizeAiContentDraft).filter(Boolean).slice(-50);
}

function normalizeMarketplaceState(input = {}) {
  if (!input || typeof input !== "object") {
    return { code: "unknown", label: "Статус не загружен" };
  }
  const warehouses = Array.isArray(input.warehouses)
    ? input.warehouses
        .map((warehouse) => ({
          warehouseId: cleanText(warehouse.warehouseId || warehouse.warehouse_id || warehouse.id),
          warehouseName: cleanText(warehouse.warehouseName || warehouse.warehouse_name || warehouse.name),
          present: Number.isFinite(Number(warehouse.present)) ? Number(warehouse.present) : 0,
          reserved: Number.isFinite(Number(warehouse.reserved)) ? Number(warehouse.reserved) : 0,
          stock: Number.isFinite(Number(warehouse.stock)) ? Number(warehouse.stock) : undefined,
        }))
        .filter((warehouse) => warehouse.warehouseId || warehouse.warehouseName)
    : [];
  return compactObject({
    code: cleanText(input.code || "unknown"),
    label: cleanText(input.label || "Статус не загружен"),
    visibility: cleanText(input.visibility),
    state: cleanText(input.state),
    stateName: cleanText(input.stateName || input.state_name),
    stateDescription: cleanText(input.stateDescription || input.state_description),
    stock: Number.isFinite(Number(input.stock)) ? Number(input.stock) : undefined,
    present: Number.isFinite(Number(input.present)) ? Number(input.present) : undefined,
    reserved: Number.isFinite(Number(input.reserved)) ? Number(input.reserved) : undefined,
    warehouses,
    archived: input.archived !== undefined ? Boolean(input.archived) : undefined,
    hasStocks: input.hasStocks !== undefined ? Boolean(input.hasStocks) : undefined,
    partial: input.partial || input.partialSync || input.isPartial ? true : undefined,
  });
}

function normalizeOzonPriceDetails(input = {}) {
  const price = input.price && typeof input.price === "object" ? input.price : input;
  const minPrice = parseMoneyValue(price.min_price ?? price.minPrice ?? input.min_price ?? input.minPrice);
  const currentPrice = parseMoneyValue(
    price.price ?? input.price?.price ?? (typeof input.price === "object" ? input.price?.price : undefined),
  );
  const oldPrice = parseMoneyValue(price.old_price ?? price.oldPrice ?? input.old_price ?? input.oldPrice);
  const marketingPrice = parseMoneyValue(
    price.marketing_price ?? price.marketingPrice ?? input.marketing_price ?? input.marketingPrice,
  );
  const marketingSellerPrice = parseMoneyValue(
    price.marketing_seller_price ?? price.marketingSellerPrice ?? input.marketing_seller_price ?? input.marketingSellerPrice,
  );
  const retailPrice = parseMoneyValue(price.retail_price ?? price.retailPrice ?? input.retail_price ?? input.retailPrice);
  return compactObject({
    currentPrice,
    minPrice,
    oldPrice,
    marketingPrice,
    marketingSellerPrice,
    retailPrice,
    currencyCode: cleanText(price.currency_code || price.currencyCode || input.currency_code || input.currencyCode),
  });
}

function normalizeLastPriceSend(input = {}) {
  if (!input || typeof input !== "object") return null;
  if (!Object.keys(input).length) return null;
  const status = cleanText(input.status || "").toLowerCase();
  const at = cleanText(input.at || input.sentAt || input.updatedAt);
  const requestedPrice = Number(input.requestedPrice ?? input.price ?? input.newPrice ?? 0);
  const cabinetPriceAtSend = Number(input.cabinetPriceAtSend ?? input.oldPrice ?? 0);
  const oldPriceForRetry = Number(input.oldPriceForRetry ?? 0);
  const detail = cleanText(input.detail || input.error);
  const nextRetryAt = cleanText(input.nextRetryAt || input.retryAt);
  if (!status && !at && !detail && !nextRetryAt && !Number.isFinite(requestedPrice) && !Number.isFinite(cabinetPriceAtSend)) return null;
  return {
    status: status || null,
    at: at || null,
    requestedPrice: Number.isFinite(requestedPrice) && requestedPrice > 0 ? roundPrice(requestedPrice) : null,
    cabinetPriceAtSend: Number.isFinite(cabinetPriceAtSend) && cabinetPriceAtSend > 0 ? roundPrice(cabinetPriceAtSend) : null,
    oldPriceForRetry: Number.isFinite(oldPriceForRetry) && oldPriceForRetry > 0 ? roundPrice(oldPriceForRetry) : null,
    detail: detail || "",
    nextRetryAt: nextRetryAt || null,
  };
}

function normalizeLastMarketplaceCommand(input = {}) {
  if (!input || typeof input !== "object") return null;
  if (!Object.keys(input).length) return null;
  const type = cleanText(input.type || input.action || input.command);
  const status = cleanText(input.status || (input.ok === true ? "success" : (input.ok === false ? "error" : ""))).toLowerCase();
  const at = cleanText(input.at || input.sentAt || input.updatedAt);
  const target = cleanText(input.target || input.shopId || input.account || "");
  const offerId = cleanText(input.offerId || input.offer_id || input.sku || "");
  const stock = Number(input.stock ?? input.count ?? input.targetStock ?? NaN);
  const error = cleanText(input.error || "");
  const warning = cleanText(input.warning || "");
  const detail = cleanText(input.detail || input.message || "");
  const nextRetryAt = cleanText(input.nextRetryAt || input.next_retry_at || "");
  const pending = Boolean(input.pending);
  const queuedByDailyLimit = Boolean(input.queuedByDailyLimit);
  if (!type && !status && !at && !target && !offerId && !error && !warning && !detail && !Number.isFinite(stock)) return null;
  return compactObject({
    type: type || null,
    status: status || null,
    at: at || null,
    target: target || null,
    offerId: offerId || null,
    stock: Number.isFinite(stock) ? Math.max(0, Math.round(stock)) : null,
    error: error || null,
    warning: warning || null,
    detail: detail || null,
    pending,
    queuedByDailyLimit,
    nextRetryAt: nextRetryAt || null,
  });
}

function marketplaceCommandFromAction(action = {}, product = {}, at = new Date().toISOString()) {
  return normalizeLastMarketplaceCommand({
    type: action.type,
    status: action.ok ? "success" : (action.skipped ? "skipped" : "error"),
    at,
    target: action.target || product.target,
    offerId: action.offerId || product.offerId,
    stock: action.stock,
    error: action.error,
    warning: action.warning,
    detail: action.reason || "",
    pending: action.pending,
    queuedByDailyLimit: action.queuedByDailyLimit,
    nextRetryAt: action.nextRetryAt,
  });
}

function normalizeWarehouseProduct(input = {}) {
  const target = cleanText(input.target || input.marketplace || "ozon");
  const inputMarketplace = cleanText(input.marketplace || input.marketplace_id || "").toLowerCase();
  const normalizedTarget = target.toLowerCase();
  const fallbackMarketplace = inputMarketplace === "yandex" || normalizedTarget === "yandex" || normalizedTarget.startsWith("yandex-") ? "yandex" : "ozon";
  const targetMeta = targetById(target) || {
    id: target,
    marketplace: fallbackMarketplace,
    name: fallbackMarketplace === "yandex" ? "Yandex Market" : target,
  };
  const ozonDraft = normalizeOzonDraft(input.ozon || input.ozonDraft || {});
  const yandexDraft = normalizeYandexDraft(input.yandex || input.yandexDraft || {});
  const imageUrl = firstImageUrl(input.imageUrl || input.image || input.primaryImage || ozonDraft.primaryImage || ozonDraft.images || yandexDraft.pictures);
  const name = cleanText(input.name || ozonDraft.name || yandexDraft.name || input.offerId || input.offer_id);
  const rawMarkup = Number(input.markup || 0);
  const keepYandexMarkup = Boolean(input.markupSource === "manual" || input.yandex?.extra?.manualMarkup === true);
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    target: targetMeta.id,
    marketplace: targetMeta.marketplace,
    targetName: targetMeta.name || (targetMeta.marketplace === "yandex" ? "Yandex Market" : "Ozon"),
    offerId: cleanText(input.offerId || input.offer_id),
    productId: cleanText(input.productId || input.product_id),
    sku: cleanText(input.sku || input.productSku || input.fboSku || input.fbsSku),
    productUrl: cleanText(input.productUrl || input.product_url || input.url),
    manualGroupId: cleanText(input.manualGroupId || input.manual_group_id),
    imageUrl,
    marketplacePrice: Number(input.marketplacePrice ?? input.currentPrice ?? input.current_price ?? 0) || null,
    marketplaceMinPrice: Number(input.marketplaceMinPrice ?? input.minPrice ?? input.min_price ?? input.ozonMinPrice ?? 0) || null,
    currentPrice: Number(input.currentPrice ?? input.marketplacePrice ?? input.current_price ?? 0) || null,
    targetPrice: Number(input.targetPrice ?? input.nextPrice ?? input.calculatedPrice ?? 0) || null,
    targetStock: Number.isFinite(Number(input.targetStock)) ? Number(input.targetStock) : null,
    stockOnlyManualPrices: normalizeStockOnlyManualPrices(input.stockOnlyManualPrices || input.stock_only_manual_prices || input.raw?.stockOnlyManualPrices),
    supplierCount: Number.isFinite(Number(input.supplierCount)) ? Number(input.supplierCount) : 0,
    availableSupplierCount: Number.isFinite(Number(input.availableSupplierCount)) ? Number(input.availableSupplierCount) : 0,
    name,
    keyword: cleanText(input.keyword),
    markup: targetMeta.marketplace === "yandex" && !keepYandexMarkup ? 0 : rawMarkup,
    autoPriceEnabled: input.autoPriceEnabled !== undefined ? Boolean(input.autoPriceEnabled) : true,
    autoPriceMin: Number.isFinite(Number(input.autoPriceMin)) && Number(input.autoPriceMin) > 0 ? roundPrice(Number(input.autoPriceMin)) : null,
    autoPriceMax: Number.isFinite(Number(input.autoPriceMax)) && Number(input.autoPriceMax) > 0 ? roundPrice(Number(input.autoPriceMax)) : null,
    source: cleanText(input.source || (input.productId || input.product_id ? "marketplace" : "manual")),
    ozon: ozonDraft,
    yandex: yandexDraft,
    lastOzonPriceSend: normalizeLastPriceSend(input.lastOzonPriceSend || input.last_ozon_price_send),
    lastYandexPriceSend: normalizeLastPriceSend(input.lastYandexPriceSend || input.last_yandex_price_send),
    lastStockSend: normalizeLastMarketplaceCommand(input.lastStockSend || input.last_stock_send),
    lastArchiveSend: normalizeLastMarketplaceCommand(input.lastArchiveSend || input.last_archive_send),
    marketplaceState: normalizeMarketplaceState(input.marketplaceState || input.marketplace_state || input.ozonState),
    exports: normalizeProductExports(input.exports),
    aiImages: normalizeAiImageDrafts(input.aiImages || input.ai_images || input.imageDrafts),
    aiContentDrafts: normalizeAiContentDrafts(input.aiContentDrafts || input.ai_content_drafts || input.contentDrafts),
    priceHistory: Array.isArray(input.priceHistory) ? input.priceHistory.slice(-100) : [],
    noSupplierAutomation: {
      stockZeroAt: input.noSupplierAutomation?.stockZeroAt || null,
      archivedAt: input.noSupplierAutomation?.archivedAt || null,
      recoveredAt: input.noSupplierAutomation?.recoveredAt || null,
      manualSellableAt: input.noSupplierAutomation?.manualSellableAt || null,
      lastError: input.noSupplierAutomation?.lastError || null,
    },
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || new Date().toISOString(),
    links: compactWarehouseLinks(input.links || []),
  };
}

function normalizeManualPriceValue(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return null;
  return roundPrice(number);
}

function normalizeStockOnlyManualPrices(input = {}) {
  const value = input && typeof input === "object" && !Array.isArray(input) ? input : {};
  return compactObject({
    default: normalizeManualPriceValue(value.default ?? value.base ?? value.group),
    ozon: normalizeManualPriceValue(value.ozon),
    yandex: normalizeManualPriceValue(value.yandex),
  });
}

function stockOnlyManualPriceForProduct(product = {}) {
  const prices = normalizeStockOnlyManualPrices(product.stockOnlyManualPrices || product.raw?.stockOnlyManualPrices);
  const marketplace = cleanText(product.marketplace).toLowerCase();
  const scoped = marketplace === "ozon" ? prices.ozon : marketplace === "yandex" ? prices.yandex : null;
  return normalizeManualPriceValue(scoped ?? prices.default);
}

function parsePriceMasterNoArticleRowId(value) {
  const text = cleanText(value);
  const prefix = "__no_article__:";
  if (!text.toLowerCase().startsWith(prefix)) return "";
  return cleanText(text.slice(prefix.length));
}

function normalizeWarehouseLink(input = {}) {
  const priceCurrency = cleanText(input.priceCurrency || input.price_currency || input.currency).toUpperCase();
  const matchTypeRaw = cleanText(input.matchType || input.match_type);
  let matchType = ["article", "selected_row", "exact_name"].includes(matchTypeRaw) ? matchTypeRaw : "article";
  const exactName = cleanText(input.exactName || input.exact_name || input.nativeName || input.name);
  let sourceRowId = cleanText(input.sourceRowId || input.source_row_id || input.rowId);
  let article = cleanText(input.article || input.offerId || input.nativeId);
  const syntheticNoArticleRowId = parsePriceMasterNoArticleRowId(article);
  if (syntheticNoArticleRowId) {
    article = "";
    sourceRowId = sourceRowId || syntheticNoArticleRowId;
    matchType = "selected_row";
  }
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article,
    matchType: article ? "article" : matchType,
    exactName,
    sourceRowId,
    keyword: cleanText(input.keyword),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    priceCurrency: priceCurrency === "RUB" || priceCurrency === "RUR" ? "RUB" : "USD",
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || input.createdAt || new Date().toISOString(),
    createdBy: cleanText(input.createdBy || input.created_by),
    updatedBy: cleanText(input.updatedBy || input.updated_by || input.createdBy || input.created_by),
  };
}

function warehouseLinkHasMatchTarget(input = {}) {
  const link = normalizeWarehouseLink(input);
  return Boolean(link.article || link.exactName || link.sourceRowId);
}

function warehouseLinkIdentityKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    link.partnerId,
    normalizeSupplierName(link.supplierName),
    link.keyword.toLowerCase(),
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkTargetKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    warehouseLinkSupplierSignature(link),
    link.keyword.toLowerCase(),
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkPrimaryTargetKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  const primary = link.article
    ? `article:${link.article.toLowerCase()}`
    : (link.sourceRowId ? `row:${link.sourceRowId}` : `name:${link.exactName.toLowerCase()}`);
  return [
    link.matchType,
    primary,
    link.priceCurrency,
  ].join("|");
}

function warehouseLinkSupplierKeys(input = {}) {
  const link = normalizeWarehouseLink(input);
  return [
    normalizeSupplierName(link.supplierName),
    link.partnerId ? `partner:${link.partnerId}` : "",
  ].filter(Boolean);
}

function warehouseLinkSupplierSignature(input = {}) {
  const keys = warehouseLinkSupplierKeys(input).sort();
  return keys.length ? keys.join("&") : "manual";
}

function warehouseLinksHaveCompatibleSupplierTarget(existingLink = {}, incomingLink = {}) {
  const left = warehouseLinkSupplierKeys(existingLink);
  const right = warehouseLinkSupplierKeys(incomingLink);
  if (!left.length && !right.length) return true;
  if (!left.length && right.length) return true;
  if (left.length && !right.length) return false;
  const rightSet = new Set(right);
  return left.some((key) => rightSet.has(key));
}

function warehouseLinksEqualForSave(a = {}, b = {}) {
  const left = normalizeWarehouseLink(a);
  const right = normalizeWarehouseLink(b);
  return warehouseLinkPrimaryTargetKey(a) === warehouseLinkPrimaryTargetKey(b)
    && left.keyword.toLowerCase() === right.keyword.toLowerCase()
    && warehouseLinksHaveCompatibleSupplierTarget(left, right);
}

function warehouseLinkMeaningfulSignature(input = {}) {
  const link = normalizeWarehouseLink(input);
  return JSON.stringify({
    primary: warehouseLinkPrimaryTargetKey(link),
    supplierTarget: warehouseLinkSupplierKeys(link).sort().join("|"),
    keyword: link.keyword.toLowerCase(),
    priceCurrency: link.priceCurrency,
    exactName: link.exactName.toLowerCase(),
    sourceRowId: link.sourceRowId,
  });
}

function warehouseProductLinkDetailsSignature(product = {}) {
  return compactWarehouseLinks(product.links || [])
    .map(warehouseLinkMeaningfulSignature)
    .sort()
    .join("||");
}

function mergeWarehouseLinkForSave(existing = {}, incoming = {}, { now = new Date().toISOString(), username = "system" } = {}) {
  const current = normalizeWarehouseLink(existing);
  const next = normalizeWarehouseLink(incoming);
  return normalizeWarehouseLink({
    ...current,
    ...next,
    id: current.id || next.id,
    article: current.article || next.article,
    exactName: current.exactName || next.exactName,
    sourceRowId: current.sourceRowId || next.sourceRowId,
    supplierName: next.supplierName || current.supplierName,
    partnerId: next.partnerId || current.partnerId,
    keyword: next.keyword || current.keyword,
    priceCurrency: next.priceCurrency || current.priceCurrency,
    createdAt: current.createdAt || next.createdAt || now,
    createdBy: current.createdBy || next.createdBy || username,
    updatedAt: now,
    updatedBy: username,
  });
}

function compactWarehouseLinks(links = []) {
  const result = [];
  for (const input of Array.isArray(links) ? links : []) {
    const link = normalizeWarehouseLink(input);
    if (!warehouseLinkHasMatchTarget(link)) continue;
    const index = result.findIndex((existing) => warehouseLinksEqualForSave(existing, link));
    if (index < 0) {
      result.push(link);
      continue;
    }
    const existing = result[index];
    result[index] = normalizeWarehouseLink({
      ...existing,
      ...link,
      id: existing.id || link.id,
      article: existing.article || link.article,
      exactName: existing.exactName || link.exactName,
      sourceRowId: existing.sourceRowId || link.sourceRowId,
      supplierName: existing.supplierName || link.supplierName,
      partnerId: existing.partnerId || link.partnerId,
      keyword: existing.keyword || link.keyword,
      priceCurrency: existing.priceCurrency || link.priceCurrency,
      createdAt: existing.createdAt || link.createdAt,
      createdBy: existing.createdBy || link.createdBy,
      updatedAt: link.updatedAt || existing.updatedAt,
      updatedBy: link.updatedBy || existing.updatedBy,
    });
  }
  return result;
}

function warehouseProductHasLinks(product = {}, links = []) {
  const existing = compactWarehouseLinks(product.links || []);
  return compactWarehouseLinks(links).every((link) => existing.some((item) => warehouseLinksEqualForSave(item, link)));
}

function warehouseProductLinksSignature(product = {}) {
  return compactWarehouseLinks(product.links || [])
    .map((link) => warehouseLinkTargetKey(link))
    .sort()
    .join("||");
}

function warehouseGroupLinkSignature(products = []) {
  const rows = (Array.isArray(products) ? products : [])
    .map((product) => ({
      productId: product.id || "",
      offerId: product.offerId || "",
      marketplace: normalizeWarehouseProduct(product).marketplace || "",
      linkCount: compactWarehouseLinks(product.links || []).length,
      signature: warehouseProductLinksSignature(product),
    }));
  const signatures = Array.from(new Set(rows.map((row) => row.signature)));
  return {
    ok: signatures.length <= 1,
    signature: signatures[0] || "",
    uniqueCount: signatures.length,
    products: rows,
  };
}

function buildCommonWarehouseGroupLinks(products = [], incomingLinks = [], { now = new Date().toISOString(), username = "system" } = {}) {
  const byKey = new Map();
  const addLink = (input = {}) => {
    const normalized = normalizeWarehouseLink(input);
    if (!warehouseLinkHasMatchTarget(normalized)) return;
    const key = warehouseLinkTargetKey(normalized);
    const next = normalizeWarehouseLink({
      ...normalized,
      createdAt: normalized.createdAt || now,
      updatedAt: normalized.updatedAt || now,
      createdBy: normalized.createdBy || username,
      updatedBy: normalized.updatedBy || username,
    });
    const existing = byKey.get(key);
    byKey.set(key, existing ? mergeWarehouseLinkForSave(existing, next, { now, username }) : next);
  };
  for (const product of Array.isArray(products) ? products : []) {
    for (const link of Array.isArray(product.links) ? product.links : []) addLink(link);
  }
  for (const link of Array.isArray(incomingLinks) ? incomingLinks : []) addLink(link);
  return compactWarehouseLinks(Array.from(byKey.values()));
}

function marketplacePriceBreakdown(products = []) {
  return (Array.isArray(products) ? products : []).map((product) => {
    const normalized = normalizeWarehouseProduct(product);
    const supplier = product.selectedSupplier || {};
    const formula = product.priceFormula || {};
    return {
      productId: product.id || "",
      offerId: product.offerId || "",
      marketplace: normalized.marketplace || "",
      target: product.target || "",
      supplierName: supplier.supplierName || supplier.partnerName || supplier.name || "",
      supplierArticle: supplier.article || supplier.supplierArticle || "",
      markupCoefficient: Number(product.markupCoefficient || supplier.markupCoefficient || formula.markupCoefficient || 0) || null,
      baseMarkupCoefficient: Number(supplier.baseMarkupCoefficient || formula.baseMarkupCoefficient || 0) || null,
      usdRate: Number(product.usdRate || formula.usdRate || 0) || null,
      selectedSupplierPrice: Number(supplier.price ?? formula.selectedSupplierPrice ?? 0) || null,
      selectedSupplierCurrency: supplier.priceCurrency || supplier.currency || formula.selectedSupplierCurrency || "",
      calculatedPrice: Number(supplier.calculatedPrice || formula.calculatedPrice || 0) || null,
      targetPrice: Number(product.targetPrice || product.nextPrice || formula.targetPrice || 0) || null,
      currentPrice: Number(product.currentPrice || formula.currentPrice || 0) || null,
      targetStock: Number(product.targetStock || 0) || null,
    };
  });
}

function normalizeSupplierArticle(input = {}) {
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article: cleanText(input.article),
    keyword: cleanText(input.keyword),
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    createdAt: input.createdAt || new Date().toISOString(),
  };
}

function normalizeSupplierPricingMode(input = {}) {
  const raw = input.raw && typeof input.raw === "object" && !Array.isArray(input.raw) ? input.raw : {};
  const value = cleanText(
    input.pricingMode
    || input.pricing_mode
    || input.priceMode
    || input.price_mode
    || raw.pricingMode
    || raw.pricing_mode
    || raw.priceMode
    || raw.price_mode
  ).toLowerCase().replace(/[-\s]+/g, "_");
  return ["stock_only", "stockonly", "inventory_only", "no_price", "stock_fallback"].includes(value)
    ? "stock_only"
    : "normal";
}

function normalizeSupplierTrustFactor(value, fallback = 100) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return Math.max(0, Math.min(100, Number(fallback || 100) || 100));
  return Math.max(0, Math.min(100, Math.round(parsed)));
}

function normalizeSupplierOrderCutoff(value = "") {
  const text = cleanText(value);
  const match = text.match(/^(\d{1,2})(?::?(\d{2}))?$/);
  if (!match) return "";
  const hour = Math.max(0, Math.min(23, Number(match[1]) || 0));
  const minute = Math.max(0, Math.min(59, Number(match[2] || 0) || 0));
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function normalizeManagedSupplier(input = {}) {
  const inactiveUntil = cleanText(input.inactiveUntil || input.inactive_until);
  const stopped = Boolean(input.stopped);
  const priceCurrency = cleanText(input.priceCurrency || input.price_currency || input.currency || "USD").toUpperCase();
  const pricingMode = normalizeSupplierPricingMode(input);
  const raw = input.raw && typeof input.raw === "object" && !Array.isArray(input.raw) ? input.raw : {};
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual"),
    name: cleanText(input.name),
    priceCurrency: priceCurrency === "RUB" || priceCurrency === "RUR" ? "RUB" : "USD",
    pricingMode,
    stockOnly: pricingMode === "stock_only",
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor ?? input.trust_factor ?? raw.trustFactor ?? raw.trust_factor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time || raw.orderCutoffTime || raw.order_cutoff_time),
    reseller: Boolean(input.reseller ?? raw.reseller),
    stopped,
    note: cleanText(input.note),
    stopReason: cleanText(input.stopReason || input.stop_reason),
    inactiveComment: cleanText(input.inactiveComment || input.inactive_comment),
    inactiveUntil: inactiveUntil || null,
    inactiveUntilUnknown: Boolean(input.inactiveUntilUnknown || input.inactive_until_unknown || (stopped && !inactiveUntil)),
    articles: Array.isArray(input.articles) ? input.articles.map(normalizeSupplierArticle) : [],
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function supplierImpactCount(warehouse = {}, supplier = {}) {
  return supplierImpactProductIds(warehouse, supplier).length;
}

function supplierImpactCountMap(warehouse = {}, suppliers = []) {
  const counts = new Map();
  const supplierKeys = new Map();
  for (const supplier of suppliers || []) {
    const keys = [
      normalizeSupplierName(supplier.name),
      cleanText(supplier.partnerId) ? `partner:${cleanText(supplier.partnerId)}` : "",
    ].filter(Boolean);
    for (const key of keys) {
      if (!supplierKeys.has(key)) supplierKeys.set(key, new Set());
      supplierKeys.get(key).add(supplier.id);
    }
    counts.set(supplier.id, 0);
  }
  if (!supplierKeys.size) return counts;
  const productHitsBySupplier = new Map();
  for (const product of warehouse.products || []) {
    const productSupplierIds = new Set();
    for (const link of product.links || []) {
      const keys = [
        normalizeSupplierName(link.supplierName),
        cleanText(link.partnerId) ? `partner:${cleanText(link.partnerId)}` : "",
      ].filter(Boolean);
      for (const key of keys) {
        const ids = supplierKeys.get(key);
        if (!ids) continue;
        ids.forEach((id) => productSupplierIds.add(id));
      }
    }
    productSupplierIds.forEach((id) => {
      if (!productHitsBySupplier.has(id)) productHitsBySupplier.set(id, new Set());
      productHitsBySupplier.get(id).add(product.id);
    });
  }
  for (const [id, productIds] of productHitsBySupplier.entries()) counts.set(id, productIds.size);
  return counts;
}

function supplierImpactProductIds(warehouse = {}, ...suppliers) {
  const matchers = suppliers
    .filter(Boolean)
    .map((supplier) => ({
      name: normalizeSupplierName(supplier.name),
      partnerId: cleanText(supplier.partnerId),
    }))
    .filter((supplier) => supplier.name || supplier.partnerId);
  if (!matchers.length) return [];
  const productIds = new Set();
  for (const product of warehouse.products || []) {
    for (const link of product.links || []) {
      const normalizedLinkSupplier = normalizeSupplierName(link.supplierName);
      const linkPartnerId = cleanText(link.partnerId);
      if (matchers.some((supplier) =>
        (supplier.name && normalizedLinkSupplier === supplier.name)
        || (supplier.partnerId && linkPartnerId === supplier.partnerId),
      )) {
        productIds.add(product.id);
        break;
      }
    }
  }
  return Array.from(productIds);
}

function priceMasterChangedRowMatchesWarehouseLink(row = {}, link = {}) {
  if (!row || !link) return false;
  const supplierOk =
    !link.supplierName
    || normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
  const partnerOk = !link.partnerId || String(row.partnerId || "") === String(link.partnerId);
  const keywordOk = includesKeyword(row.name, link.keyword);
  if (!supplierOk || !partnerOk || !keywordOk) return false;
  if (link.matchType === "selected_row") {
    if (link.sourceRowId && String(row.rowId || "") === String(link.sourceRowId)) return true;
    if (link.exactName) return exactPriceMasterNameMatches(row.name, link.exactName);
    return false;
  }
  if (link.matchType === "exact_name") {
    return exactPriceMasterNameMatches(row.name, link.exactName || link.article);
  }
  const article = cleanText(link.article).toLowerCase();
  return Boolean(article && cleanText(row.article).toLowerCase() === article);
}

function priceMasterChangeImpactProductIds(warehouse = {}, changes = [], options = {}) {
  const maxChanges = Math.max(1, Number(options.maxChanges || priceMasterDeltaMaxChanges) || priceMasterDeltaMaxChanges);
  const maxProducts = Math.max(1, Number(options.maxProducts || priceMasterDeltaMaxProducts) || priceMasterDeltaMaxProducts);
  const relevantTypes = new Set(["price_changed", "inactive", "returned", "missing", "new"]);
  const relevantChanges = (Array.isArray(changes) ? changes : []).filter((change) => relevantTypes.has(change?.type));
  if (!relevantChanges.length) return { productIds: [], scannedChanges: 0, skipped: false, reason: null };
  if (relevantChanges.length > maxChanges) {
    if (options.fullReconcileOnTooMany === true) {
      const reconcileLimit = Math.max(1, Number(options.fullReconcileMaxProducts || autoPriceReconcileMaxProducts) || autoPriceReconcileMaxProducts);
      const products = Array.isArray(warehouse.products) ? warehouse.products : [];
      const linked = products.filter((product) => product?.autoPriceEnabled !== false && Array.isArray(product.links) && product.links.length);
      const expanded = expandWarehouseProductsToGroups(products, linked);
      const ids = Array.from(new Set(expanded.map((product) => cleanText(product.id)).filter(Boolean)));
      const truncated = ids.length > reconcileLimit;
      return {
        productIds: ids.slice(0, reconcileLimit),
        scannedChanges: relevantChanges.length,
        skipped: truncated,
        reason: truncated ? "too_many_pricemaster_changes_full_reconcile_limited" : "too_many_pricemaster_changes_full_reconcile",
        fallbackFullReconcile: true,
        directProducts: linked.length,
        groupExpandedProducts: ids.length,
      };
    }
    return {
      productIds: [],
      scannedChanges: relevantChanges.length,
      skipped: true,
      reason: "too_many_pricemaster_changes",
    };
  }
  const rows = relevantChanges.flatMap((change) => [change.current, change.previous].filter(Boolean));
  if (!rows.length) return { productIds: [], scannedChanges: relevantChanges.length, skipped: false, reason: null };

  const productIds = new Set();
  const matchedProducts = [];
  for (const product of warehouse.products || []) {
    const links = Array.isArray(product.links) ? product.links : [];
    if (!links.length) continue;
    const matched = links.some((link) => rows.some((row) => priceMasterChangedRowMatchesWarehouseLink(row, link)));
    if (!matched) continue;
    productIds.add(product.id);
    matchedProducts.push(product);
    if (productIds.size >= maxProducts) {
      const expanded = expandWarehouseProductsToGroups(warehouse.products || [], matchedProducts);
      const expandedIds = Array.from(new Set([...Array.from(productIds), ...expanded.map((product) => product.id).filter(Boolean)]));
      return {
        productIds: expandedIds.slice(0, maxProducts),
        scannedChanges: relevantChanges.length,
        skipped: true,
        reason: "too_many_impacted_products",
        directProducts: productIds.size,
        groupExpandedProducts: expandedIds.length,
      };
    }
  }
  const expanded = expandWarehouseProductsToGroups(warehouse.products || [], matchedProducts);
  const expandedIds = Array.from(new Set([...Array.from(productIds), ...expanded.map((product) => product.id).filter(Boolean)]));
  return {
    productIds: expandedIds.slice(0, maxProducts),
    scannedChanges: relevantChanges.length,
    skipped: expandedIds.length > maxProducts,
    reason: expandedIds.length > maxProducts ? "too_many_impacted_products" : null,
    directProducts: productIds.size,
    groupExpandedProducts: expandedIds.length,
  };
}

function warehouseProductAutomationFingerprint(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const state = normalized.marketplaceState || {};
  const links = (Array.isArray(normalized.links) ? normalized.links : [])
    .map((link) => ({
      article: cleanText(link.article).toLowerCase(),
      matchType: cleanText(link.matchType || "article"),
      exactName: cleanText(link.exactName).toLowerCase(),
      sourceRowId: cleanText(link.sourceRowId),
      partnerId: cleanText(link.partnerId),
    }))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

  return JSON.stringify({
    id: normalized.id,
    target: normalized.target,
    marketplace: normalized.marketplace,
    offerId: cleanText(normalized.offerId).toLowerCase(),
    marketplacePrice: Number(normalized.marketplacePrice || 0) || 0,
    currentPrice: Number(normalized.currentPrice || 0) || 0,
    targetStock: normalized.targetStock === null || normalized.targetStock === undefined ? null : Number(normalized.targetStock),
    state: {
      code: cleanText(state.code),
      active: Boolean(state.active),
      archived: Boolean(state.archived),
      outOfStock: Boolean(state.outOfStock),
      partial: Boolean(state.partial),
    },
    noSupplierAutomation: {
      stockZeroAt: cleanText(normalized.noSupplierAutomation?.stockZeroAt || ""),
      archivedAt: cleanText(normalized.noSupplierAutomation?.archivedAt || ""),
      recoveredAt: cleanText(normalized.noSupplierAutomation?.recoveredAt || ""),
    },
    links,
  });
}

function changedWarehouseProductIdsByAutomationFingerprint(beforeProducts = [], afterProducts = []) {
  const before = new Map();
  for (const product of Array.isArray(beforeProducts) ? beforeProducts : []) {
    if (!product?.id) continue;
    before.set(String(product.id), warehouseProductAutomationFingerprint(product));
  }
  const changed = [];
  for (const product of Array.isArray(afterProducts) ? afterProducts : []) {
    if (!product?.id) continue;
    const id = String(product.id);
    const fingerprint = warehouseProductAutomationFingerprint(product);
    if (!before.has(id) || before.get(id) !== fingerprint) changed.push(product.id);
  }
  return changed;
}

function backgroundAutomationProductIds(priceMaster = {}, warehouse = {}, options = {}) {
  const productIds = new Set(
    (Array.isArray(warehouse.marketplaceSyncChangedProductIds) ? warehouse.marketplaceSyncChangedProductIds : [])
      .map((id) => cleanText(id))
      .filter(Boolean),
  );
  const priceMasterDelta = priceMasterChangeImpactProductIds(warehouse, priceMaster.changedRows || [], {
    maxChanges: options.maxChanges || priceMasterDeltaMaxChanges,
    maxProducts: options.maxProducts || priceMasterDeltaMaxProducts,
  });
  for (const id of priceMasterDelta.productIds || []) {
    const normalizedId = cleanText(id);
    if (normalizedId) productIds.add(normalizedId);
  }
  return {
    productIds: Array.from(productIds),
    marketplaceChanged: Array.isArray(warehouse.marketplaceSyncChangedProductIds) ? warehouse.marketplaceSyncChangedProductIds.length : 0,
    priceMasterDelta,
  };
}

function cloneAuditValue(value) {
  return value == null ? null : JSON.parse(JSON.stringify(value));
}

function requestUsername(request) {
  return cleanText(request?.session?.username || "system") || "system";
}

function productConflict(product, expectedUpdatedAt) {
  const expected = typeof expectedUpdatedAt === "object" && expectedUpdatedAt !== null
    ? cleanText(expectedUpdatedAt.expectedUpdatedAt || expectedUpdatedAt.updatedAt || "")
    : cleanText(expectedUpdatedAt || "");
  const expectedLinksSignature = typeof expectedUpdatedAt === "object" && expectedUpdatedAt !== null
    ? cleanText(expectedUpdatedAt.expectedLinksSignature || expectedUpdatedAt.linksSignature || "")
    : "";
  if (!expected) return null;
  if (cleanText(product?.updatedAt || "") === expected) return null;
  const currentLinksSignature = warehouseProductLinksSignature(product);
  if (expectedLinksSignature && expectedLinksSignature === currentLinksSignature) return null;
  return {
    id: product.id,
    offerId: product.offerId || product.ozon?.offerId || product.yandex?.offerId || "",
    expectedUpdatedAt: expected,
    expectedLinksSignature: expectedLinksSignature || undefined,
    currentLinksSignature: expectedLinksSignature ? currentLinksSignature : undefined,
    currentUpdatedAt: product.updatedAt || null,
    freshProduct: normalizeWarehouseProduct(product),
  };
}

function productLocksFromRequest(body = {}) {
  const locks = new Map();
  if (body.expectedUpdatedAt && body.productId) {
    locks.set(String(body.productId), {
      expectedUpdatedAt: cleanText(body.expectedUpdatedAt),
      expectedLinksSignature: cleanText(body.expectedLinksSignature || body.linksSignature || ""),
    });
  }
  if (body.expectedUpdatedAt && Array.isArray(body.productIds) && body.productIds.length === 1) {
    locks.set(String(body.productIds[0]), {
      expectedUpdatedAt: cleanText(body.expectedUpdatedAt),
      expectedLinksSignature: cleanText(body.expectedLinksSignature || body.linksSignature || ""),
    });
  }
  for (const item of Array.isArray(body.optimisticLocks) ? body.optimisticLocks : []) {
    const id = cleanText(item?.id);
    if (id) {
      locks.set(id, {
        expectedUpdatedAt: cleanText(item?.expectedUpdatedAt || ""),
        expectedLinksSignature: cleanText(item?.expectedLinksSignature || item?.linksSignature || ""),
      });
    }
  }
  return locks;
}

function collectProductConflicts(products = [], locks = new Map()) {
  return products
    .map((product) => productConflict(product, locks.get(String(product.id))))
    .filter(Boolean);
}

function collectProductConflictsExceptBackground(products = [], locks = new Map(), { mergeOnly = false } = {}) {
  const conflicts = collectProductConflicts(products, locks);
  return conflicts;
}

function canIgnoreStaleLinkSaveConflict(product = {}, links = [], lock = {}) {
  if (!lock?.expectedUpdatedAt) return false;
  const existingLinks = compactWarehouseLinks(product.links || []);
  if (!existingLinks.length) return true;
  return warehouseProductHasLinks(product, links);
}

function conflictResponse(response, conflicts) {
  return response.status(409).json({
    error: "Конфликт обновления: карточка уже изменена другим пользователем.",
    code: conflicts.length > 1 ? "warehouse_bulk_conflict" : "warehouse_product_conflict",
    conflicts,
  });
}

function normalizeSupplierName(value) {
  return String(value || "").trim().toLowerCase();
}

function isSupplierInactiveDateDue(isoDate, now = new Date()) {
  const value = cleanText(isoDate);
  if (!value) return false;
  const dateOnly = value.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateOnly)) return false;
  const today = now.toISOString().slice(0, 10);
  return dateOnly <= today;
}

function applySupplierAutoReactivate(warehouse, now = new Date()) {
  const suppliers = Array.isArray(warehouse?.suppliers) ? warehouse.suppliers : [];
  const reactivated = [];
  for (const supplier of suppliers) {
    if (!supplier?.stopped) continue;
    if (!supplier.inactiveUntil) continue;
    if (!isSupplierInactiveDateDue(supplier.inactiveUntil, now)) continue;
    supplier.stopped = false;
    supplier.stopReason = "";
    supplier.inactiveComment = "";
    supplier.inactiveUntil = null;
    supplier.inactiveUntilUnknown = false;
    supplier.updatedAt = new Date().toISOString();
    reactivated.push({ id: supplier.id, name: supplier.name });
  }
  return reactivated;
}

async function readCachedExchangeRate() {
  try {
    return JSON.parse(await fs.readFile(exchangeRatePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

async function writeExchangeRate(rate) {
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(exchangeRatePath, JSON.stringify(rate, null, 2), "utf8");
}

function parseApiResponse(text) {
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_error) {
    return { raw: text.slice(0, 1000) };
  }
}

function summarizeApiErrorPayload(data = {}, fallback = "API error") {
  const parts = [];
  const push = (value) => {
    const text = cleanText(value);
    if (text && !parts.includes(text)) parts.push(text);
  };
  const visit = (value, depth = 0) => {
    if (!value || parts.length >= 12 || depth > 5) return;
    if (typeof value !== "object") {
      push(value);
      return;
    }
    push(value.message);
    push(value.error);
    push(value.code);
    push(value.field);
    push(value.sku || value.offerId || value.offer_id);
    for (const key of ["errors", "details", "result", "results", "warnings", "params", "raw"]) {
      const nested = value[key];
      if (Array.isArray(nested)) {
        for (const item of nested) visit(item, depth + 1);
      } else if (nested && typeof nested === "object") {
        visit(nested, depth + 1);
      } else {
        push(nested);
      }
    }
  };
  visit(data);
  return parts.slice(0, 6).join("; ").slice(0, 1000) || fallback;
}

function apiPayloadHasErrors(data = {}) {
  if (!data || typeof data !== "object") return false;
  const status = cleanText(data.status || data.result?.status).toUpperCase();
  if (status === "ERROR" || status === "FAILED") return true;
  if (Array.isArray(data.errors) && data.errors.length) return true;
  if (Array.isArray(data.result?.errors) && data.result.errors.length) return true;
  if (Array.isArray(data.results) && data.results.some(apiPayloadHasErrors)) return true;
  return false;
}

async function getUsdRate({ force = false } = {}) {
  const cached = await readCachedExchangeRate();
  if (!force && cached?.rate && Date.now() - new Date(cached.fetchedAt).getTime() < exchangeRateTtlMs) {
    return { ...cached, cached: true };
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(process.env.USD_RATE_TIMEOUT_MS || 3000) || 3000));
    const response = await fetch("https://www.cbr-xml-daily.ru/daily_json.js", { signal: controller.signal })
      .finally(() => clearTimeout(timeout));
    if (!response.ok) throw new Error(`CBR rate request failed: ${response.status}`);
    const data = await response.json();
    const rate = Number(data.Valute?.USD?.Value);
    if (!Number.isFinite(rate) || rate <= 0) throw new Error("USD rate was not found in CBR response");

    const payload = {
      rate: Number(rate.toFixed(4)),
      source: "CBR",
      fetchedAt: new Date().toISOString(),
      validForHours: 6,
    };
    await writeExchangeRate(payload);
    return { ...payload, cached: false };
  } catch (error) {
    if (cached?.rate) return { ...cached, cached: true, warning: error.message };
    return {
      rate: Number(process.env.DEFAULT_USD_RATE || 95),
      source: "fallback",
      fetchedAt: new Date().toISOString(),
      validForHours: 6,
      cached: false,
      warning: error.message,
    };
  }
}

function isOzonRateLimitError(error) {
  const message = String(error?.message || "").toLowerCase();
  const status = Number(error?.statusCode || error?.status || 0);
  return status === 429
    || message.includes("rate limit")
    || message.includes("too many request")
    || message.includes("resourceexhausted")
    || message.includes("items limit")
    || message.includes("limit exceeded");
}

function ozonRetryDelayMs(attempt, response = null) {
  const retryAfter = Number(response?.headers?.get?.("retry-after") || 0);
  if (Number.isFinite(retryAfter) && retryAfter > 0) return Math.min(30_000, retryAfter * 1000);
  const base = Math.max(500, Number(process.env.OZON_RATE_LIMIT_RETRY_MS || 1200) || 1200);
  return Math.min(30_000, base * attempt * attempt);
}

function isOzonResourceExhaustedError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("resourceexhausted")
    || combined.includes("items limit")
    || combined.includes("limit exceeded")
    || combined.includes("acquire limit");
}

function isOzonPerItemPriceLimitError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("price-batch-set")
    && (
      combined.includes("per item")
      || combined.includes("items limit")
      || combined.includes("acquire limit per item")
      || combined.includes("10")
      || combined.includes("раз в час")
    );
}

function isOzonOldPriceLessError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("old price is less than price")
    || (combined.includes("old_price") && combined.includes("less") && combined.includes("price"))
    || (combined.includes("old price") && combined.includes("less") && combined.includes("price"));
}

function getOzonPriceBatchSize() {
  return Math.max(1, Math.min(100, Number(process.env.OZON_PRICE_BATCH_SIZE || 1) || 1));
}

function getOzonPriceBatchDelayMs() {
  return Math.max(0, Number(process.env.OZON_PRICE_BATCH_DELAY_MS || 1200) || 1200);
}

function getOzonPriceBatchMaxAttempts() {
  return Math.max(1, Number(process.env.OZON_PRICE_BATCH_MAX_ATTEMPTS || 6) || 6);
}

function getOzonPriceBatchBackoffMs() {
  return Math.max(500, Number(process.env.OZON_PRICE_BATCH_BACKOFF_MS || 2500) || 2500);
}

function enqueueOzonRequest(task) {
  const minIntervalMs = Math.max(0, Number(process.env.OZON_REQUEST_MIN_INTERVAL_MS || 450) || 450);
  const run = async () => {
    const waitMs = Math.max(0, ozonLastRequestAt + minIntervalMs - Date.now());
    if (waitMs > 0) await sleep(waitMs);
    ozonLastRequestAt = Date.now();
    return task();
  };
  const queued = ozonRequestChain.then(run, run);
  ozonRequestChain = queued.catch(() => {});
  return queued;
}

async function ozonRequest(pathname, body, account = null) {
  const selectedAccount = account || getOzonAccountByTarget("ozon");
  const clientId = selectedAccount?.clientId;
  const apiKey = selectedAccount?.apiKey;

  if (!clientId || !apiKey) {
    const error = new Error("Добавьте Client-Id и Api-Key Ozon в настройках кабинетов или в .env.");
    error.statusCode = 400;
    throw error;
  }

  return enqueueOzonRequest(async () => {
    const maxAttempts = Math.max(1, Number(process.env.OZON_REQUEST_MAX_ATTEMPTS || 4) || 4);
    let lastError = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const response = await fetch(`${ozonBaseUrl}${pathname}`, {
          method: "POST",
          headers: {
            "Client-Id": clientId,
            "Api-Key": apiKey,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(body || {}),
        });

        const text = await response.text();
        const data = parseApiResponse(text);

        if (!response.ok) {
          const message = data.message || data.error || `Ozon API error ${response.status}`;
          const error = new Error(message);
          error.statusCode = response.status;
          error.ozon = data;
          if (!isOzonRateLimitError(error) || attempt >= maxAttempts) throw error;
          lastError = error;
          await sleep(ozonRetryDelayMs(attempt, response));
          continue;
        }

        return data;
      } catch (error) {
        if (!isOzonRateLimitError(error) || attempt >= maxAttempts) throw error;
        lastError = error;
        await sleep(ozonRetryDelayMs(attempt));
      }
    }
    throw lastError || new Error("Ozon API request failed");
  });
}

async function sendOzonPriceBatch(account, prices) {
  const maxAttempts = getOzonPriceBatchMaxAttempts();
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await ozonRequest("/v1/product/import/prices", { prices }, account);
      const delayMs = getOzonPriceBatchDelayMs();
      if (delayMs > 0) await sleep(delayMs);
      return result;
    } catch (error) {
      lastError = error;
      if (!isOzonRateLimitError(error) && !isOzonResourceExhaustedError(error)) throw error;
      if (isOzonPerItemPriceLimitError(error)) throw error;
      if (attempt >= maxAttempts) throw error;
      const delayMs = Math.min(60_000, getOzonPriceBatchBackoffMs() * attempt * attempt);
      logger.warn("ozon price batch rate limited, retrying", {
        account: account?.id || account?.name || "ozon",
        items: prices.length,
        attempt,
        maxAttempts,
        delayMs,
        detail: error?.message || String(error),
      });
      await sleep(delayMs);
    }
  }
  throw lastError || new Error("Ozon price batch failed");
}

function ozonPriceResultErrorMessage(result = {}) {
  const errors = Array.isArray(result.errors) ? result.errors : [];
  if (!errors.length) return "";
  return errors
    .map((error) => cleanText(error.message || error.error || error.code || JSON.stringify(error)))
    .filter(Boolean)
    .join("; ");
}

function extractOzonPriceResponseFailures(response = {}, payloads = []) {
  const results = Array.isArray(response?.result) ? response.result : [];
  if (!results.length) return [];
  const payloadByOffer = new Map(payloads.map((payload) => [String(payload.offer_id || ""), payload]));
  const payloadByProduct = new Map(payloads.map((payload) => [String(payload.product_id || ""), payload]));
  const failed = [];
  for (const result of results) {
    const offerId = String(result.offer_id || "");
    const productId = String(result.product_id || "");
    const payload = payloadByOffer.get(offerId) || payloadByProduct.get(productId);
    if (!payload) continue;
    const detail = ozonPriceResultErrorMessage(result);
    if (result.updated === false || detail) {
      const error = new Error(detail || "Ozon price update was not applied");
      error.ozon = result;
      failed.push({ payload, error });
    }
  }
  return failed;
}

async function sendOzonPricePayloadChunks(account, prices) {
  const results = [];
  const failed = [];
  for (const chunk of chunkArray(prices, getOzonPriceBatchSize())) {
    try {
      const response = await sendOzonPriceBatch(account, chunk);
      const responseFailures = extractOzonPriceResponseFailures(response, chunk);
      if (responseFailures.length) failed.push(...responseFailures);
      results.push({ response, count: chunk.length - responseFailures.length });
    } catch (error) {
      if (!isOzonResourceExhaustedError(error) || chunk.length <= 1) {
        failed.push(...chunk.map((payload) => ({ payload, error })));
        continue;
      }
      logger.warn("ozon price batch limit exceeded, falling back to single-item sends", {
        account: account?.id || account?.name || "ozon",
        items: chunk.length,
        detail: error?.message || String(error),
      });
      for (const payload of chunk) {
        try {
          results.push({ response: await sendOzonPriceBatch(account, [payload]), count: 1 });
        } catch (singleError) {
          failed.push({ payload, error: singleError });
        }
      }
    }
  }
  return { results, failed };
}

function normalizeOzonWarehouse(input = {}) {
  const warehouseId = cleanText(input.warehouseId || input.warehouse_id || input.id);
  const warehouseName = cleanText(input.warehouseName || input.warehouse_name || input.name);
  return warehouseId || warehouseName ? { warehouseId, warehouseName } : null;
}

function normalizeOzonStockWarehouse(input = {}) {
  const normalized = normalizeOzonWarehouse(input);
  if (!normalized) return null;
  const present = Number(input.present || 0);
  const reserved = Number(input.reserved || 0);
  const stock = Number.isFinite(Number(input.stock))
    ? Number(input.stock)
    : Math.max(0, present - reserved);
  return {
    ...normalized,
    present: Number.isFinite(present) ? present : 0,
    reserved: Number.isFinite(reserved) ? reserved : 0,
    stock,
  };
}

function parseOzonStockWarehouseIds(account = {}) {
  const accountKey = cleanText(account.id || account.name || "ozon")
    .replace(/[^a-z0-9]/gi, "_")
    .toUpperCase();
  return splitList(
    process.env[`OZON_STOCK_WAREHOUSE_IDS_${accountKey}`]
      || process.env.OZON_STOCK_WAREHOUSE_IDS
      || process.env.OZON_STOCK_WAREHOUSE_ID
      || "",
  );
}

function parseOzonStockWarehouseNames(account = {}) {
  const accountKey = cleanText(account.id || account.name || "ozon")
    .replace(/[^a-z0-9]/gi, "_")
    .toUpperCase();
  return splitList(
    process.env[`OZON_STOCK_WAREHOUSE_NAMES_${accountKey}`]
      || process.env.OZON_STOCK_WAREHOUSE_NAMES
      || "",
  ).map((name) => normalizeSupplierName(name));
}

async function getOzonWarehouses(account = null, { refresh = false } = {}) {
  const selectedAccount = account || getOzonAccountByTarget("ozon");
  const cacheKey = cleanText(selectedAccount?.id || selectedAccount?.clientId || "ozon");
  const cached = ozonWarehouseCache.get(cacheKey);
  if (!refresh && cached && Date.now() - cached.at < 10 * 60 * 1000) return cached.items;
  const data = await ozonRequest("/v1/warehouse/list", {}, selectedAccount);
  const raw = data.result || data.warehouses || data.items || [];
  const items = (Array.isArray(raw) ? raw : raw.warehouses || raw.items || [])
    .map(normalizeOzonWarehouse)
    .filter(Boolean);
  ozonWarehouseCache.set(cacheKey, { at: Date.now(), items });
  return items;
}

async function resolveOzonStockWarehouses(account = null, product = null) {
  const configuredIds = parseOzonStockWarehouseIds(account);
  if (configuredIds.length) {
    return configuredIds.map((warehouseId) => ({ warehouseId, warehouseName: "" }));
  }

  const configuredNames = parseOzonStockWarehouseNames(account);
  const storedWarehouses = Array.isArray(product?.marketplaceState?.warehouses)
    ? product.marketplaceState.warehouses.map(normalizeOzonWarehouse).filter(Boolean)
    : [];
  if (storedWarehouses.length) {
    if (configuredNames.length) {
      const matchedStored = storedWarehouses.filter((warehouse) =>
        configuredNames.some((name) => normalizeSupplierName(warehouse.warehouseName).includes(name)),
      );
      if (matchedStored.length) return matchedStored;
    } else {
      return storedWarehouses;
    }
  }

  if (!ozonWarehouseListEnabled) return [];

  try {
    const warehouses = await getOzonWarehouses(account);
    if (configuredNames.length) {
      return warehouses.filter((warehouse) =>
        configuredNames.some((name) => normalizeSupplierName(warehouse.warehouseName).includes(name)),
      );
    }
    if (warehouses.length) return warehouses;
  } catch (error) {
    logger.warn("ozon warehouse list failed", {
      account: account?.id || account?.name || "ozon",
      detail: error?.message || String(error),
    });
  }
  return [];
}

async function buildOzonStockPayloadItems(items = [], account = null, stockResolver = () => 0, { allWarehouses = false } = {}) {
  const payloadItems = [];
  for (const item of items) {
    const offerId = cleanText(item.offerId || item.offer_id);
    if (!offerId) continue;
    const stock = Math.max(0, Math.round(Number(stockResolver(item) || 0)));
    const warehouses = await resolveOzonStockWarehouses(account, item);
    if (!warehouses.length) {
      payloadItems.push({ offer_id: offerId, stock });
      continue;
    }
    const targetWarehouses = allWarehouses ? warehouses : warehouses.slice(0, 1);
    for (const warehouse of targetWarehouses) {
      payloadItems.push({
        offer_id: offerId,
        warehouse_id: Number(warehouse.warehouseId),
        stock,
      });
    }
  }
  return payloadItems.filter((item) => item.offer_id && (item.warehouse_id || item.warehouse_id === undefined));
}

async function yandexRequest(shop, method, pathname, body) {
  if (!shop?.apiKey || !shop?.businessId) {
    const error = new Error("Yandex shop apiKey and businessId must be set in .env");
    error.statusCode = 400;
    throw error;
  }

  const attempts = Math.max(1, Number(process.env.YANDEX_REQUEST_MAX_ATTEMPTS || 3) || 3);
  const timeoutMs = Math.max(1000, Number(process.env.YANDEX_REQUEST_TIMEOUT_MS || 20000) || 20000);
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(`${yandexBaseUrl}${pathname}`, {
        method,
        headers: {
          "Api-Key": shop.apiKey,
          "Content-Type": "application/json",
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      clearTimeout(timer);

      const text = await response.text();
      const data = parseApiResponse(text);

      if (!response.ok) {
        const error = new Error(summarizeApiErrorPayload(data, `Yandex Market API error ${response.status}`));
        error.statusCode = response.status;
        error.yandex = data;
        if (![420, 423, 429, 500, 502, 503, 504].includes(response.status) || attempt >= attempts) throw error;
        lastError = error;
      } else {
        return data;
      }
    } catch (error) {
      clearTimeout(timer);
      lastError = error?.name === "AbortError" ? new Error(`Yandex Market API timeout ${Math.round(timeoutMs / 1000)}s`) : error;
      if (attempt >= attempts) throw lastError;
    }

    const delayMs = Math.min(5000, 500 * attempt * attempt);
    await sleep(delayMs);
  }

  throw lastError || new Error("Yandex Market API request failed");
}

async function getOzonProducts(limit = Number.POSITIVE_INFINITY, account = null) {
  const parsedLimit = Number(limit);
  const maxItems = Number.isFinite(parsedLimit) && parsedLimit > 0 ? parsedLimit : Number.MAX_SAFE_INTEGER;
  const byKey = new Map();
  const visibilityModes = ["ALL", "ARCHIVED"];

  async function loadVisibility(visibility) {
    const items = [];
    let lastId = "";
    const visibilityMax = visibility === "ALL" ? maxItems : Number.MAX_SAFE_INTEGER;

    while (items.length < visibilityMax) {
      const batchLimit = Math.min(1000, visibilityMax - items.length);
      const data = await ozonRequest("/v3/product/list", {
        filter: { visibility },
        limit: batchLimit,
        last_id: lastId,
      }, account);

      const batch = data.result?.items || [];
      items.push(...batch);
      lastId = data.result?.last_id || "";

      if (!batch.length || !lastId) break;
    }
    return items;
  }

  for (const visibility of visibilityModes) {
    try {
      const items = await loadVisibility(visibility);
      for (const item of items) {
        const key = cleanText(item.offer_id || item.product_id || JSON.stringify(item));
        if (!key) continue;
        byKey.set(key, { ...item, visibility: item.visibility || visibility });
      }
    } catch (error) {
      if (visibility === "ALL") throw error;
      logger.warn("ozon archived list failed", { account: account?.id, visibility, detail: error?.message || String(error) });
    }
  }

  return Array.from(byKey.values());
}

function ozonOfferMapKey(value) {
  return cleanText(value).toLowerCase();
}

function setOzonOfferMapValue(map, offerId, value) {
  const exact = cleanText(offerId);
  if (!exact) return;
  map.set(exact, value);
  map.set(ozonOfferMapKey(exact), value);
}

function getOzonOfferMapValue(map, offerId) {
  const exact = cleanText(offerId);
  if (!exact) return undefined;
  return map.get(exact) || map.get(ozonOfferMapKey(exact));
}

async function getOzonProductInfoMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v3/product/info/list", {
        offer_id: chunk,
      }, account);
    } catch (error) {
      logger.warn("ozon product info chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || data.result?.items || []) {
      const offerId = item.offer_id || item.offerId;
      if (offerId) setOzonOfferMapValue(map, offerId, item);
    }
    options.onProgress?.({
      stage: "Детали Ozon",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}

async function getOzonProductInfoMapByProductIds(productIds, account = null, options = {}) {
  const map = new Map();
  const ids = productIds
    .map((productId) => String(productId || "").trim())
    .filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v3/product/info/list", {
        product_id: chunk.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0),
      }, account);
    } catch (error) {
      logger.warn("ozon product info by product id chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || data.result?.items || []) {
      const productId = cleanText(item.product_id || item.productId || item.id);
      if (productId) map.set(productId, item);
      const offerId = item.offer_id || item.offerId;
      if (offerId) setOzonOfferMapValue(map, offerId, item);
    }
    options.onProgress?.({
      stage: "Восстановление Ozon по product_id",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}

async function getOzonStockMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);
  const chunks = chunkArray(ids, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v4/product/info/stocks", {
        filter: { offer_id: chunk, visibility: "ALL" },
        limit: chunk.length,
      }, account);
    } catch (error) {
      logger.warn("ozon stock chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || data.result?.items || []) {
      const offerId = item.offer_id || item.offerId;
      if (!offerId) continue;
      const stocks = Array.isArray(item.stocks) ? item.stocks : [];
      const warehouses = stocks.map(normalizeOzonStockWarehouse).filter(Boolean);
      const present = warehouses.reduce((sum, stock) => sum + Number(stock.present || 0), 0);
      const reserved = warehouses.reduce((sum, stock) => sum + Number(stock.reserved || 0), 0);
      const total = Number.isFinite(Number(item.stock)) ? Number(item.stock) : Math.max(0, present - reserved);
      setOzonOfferMapValue(map, offerId, { ...item, present, reserved, stock: total, warehouses });
    }
    options.onProgress?.({
      stage: "Остатки Ozon",
      processed: Math.min(ids.length, (index + 1) * 100),
      total: ids.length,
    });
  }

  return map;
}

function pickOzonState(product = {}, info = {}, stockInfo = {}) {
  const visibility = cleanText(info.visibility || product.visibility || stockInfo.visibility).toUpperCase();
  const state = cleanText(info.status?.state || info.state || product.status || product.state).toUpperCase();
  const stateName = cleanText(info.status?.state_name || info.state_name || info.status_name);
  const stateDescription = cleanText(info.status?.state_description || info.state_description || info.status_description);
  const archived = Boolean(product.archived || info.archived || visibility === "ARCHIVED" || state === "ARCHIVED");
  const present = Number(stockInfo.present || 0);
  const reserved = Number(stockInfo.reserved || 0);
  const warehouses = Array.isArray(stockInfo.warehouses) ? stockInfo.warehouses : [];
  const stock = Number.isFinite(Number(stockInfo.stock)) ? Number(stockInfo.stock) : Math.max(0, present - reserved);
  const hasStocks = Boolean(product.has_fbs_stocks || product.hasFbsStocks || stock > 0);

  if (archived) {
    return normalizeMarketplaceState({ code: "archived", label: "В архиве Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (visibility === "EMPTY_STOCK" || (!hasStocks && stock <= 0)) {
    return normalizeMarketplaceState({ code: "out_of_stock", label: "Нет в наличии Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(visibility)
    || ["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(state)) {
    return normalizeMarketplaceState({ code: "inactive", label: "Неактивен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  if (visibility || state || hasStocks) {
    return normalizeMarketplaceState({ code: "active", label: "Активен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
  }
  return normalizeMarketplaceState({ code: "unknown", label: "Статус Ozon не загружен", visibility, state, stateName, stateDescription, stock, present, reserved, warehouses, archived, hasStocks });
}

async function getOzonPriceMap(offerIds, account = null, options = {}) {
  const map = new Map();
  const chunks = chunkArray(offerIds, 100);

  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    let data;
    try {
      data = await ozonRequest("/v5/product/info/prices", {
        filter: { offer_id: chunk, visibility: "ALL" },
        limit: chunk.length,
      }, account);
    } catch (error) {
      logger.warn("ozon price info chunk failed", {
        account: account?.id,
        chunk: index + 1,
        totalChunks: chunks.length,
        detail: error?.message || String(error),
      });
      if (options.continueOnError) continue;
      throw error;
    }

    for (const item of data.items || []) {
      setOzonOfferMapValue(map, item.offer_id, item);
    }
    options.onProgress?.({
      stage: "Цены Ozon",
      processed: Math.min(offerIds.length, (index + 1) * 100),
      total: offerIds.length,
    });
  }

  return map;
}

function ozonExistingProductMap(products = [], account = {}) {
  const map = new Map();
  const accountId = cleanText(account.id || "ozon");
  for (const product of products || []) {
    if (cleanText(product.marketplace) !== "ozon") continue;
    const offerId = cleanText(product.offerId || product.ozon?.offerId);
    if (!offerId) continue;
    const target = cleanText(product.target || "ozon");
    if (target !== accountId && target !== "ozon") continue;
    setOzonOfferMapValue(map, offerId, product);
  }
  return map;
}

function ozonProductNeedsDetailRefresh(product = {}) {
  if (!product || !product.id) return true;
  const offerId = cleanText(product.offerId || product.ozon?.offerId);
  if (isWeakProductName(product.name, offerId)) return true;
  if (!cleanText(product.imageUrl || product.ozon?.primaryImage)) return true;
  if (!cleanText(product.productId || product.ozon?.productId)) return true;
  if (!cleanText(product.marketplaceState?.code) || product.marketplaceState?.partial) return true;
  if (!Number(product.marketplacePrice || product.ozon?.price || 0)) return true;
  return false;
}

function isWeakOzonWarehouseProduct(product = {}) {
  if (!product || cleanText(product.marketplace) !== "ozon") return false;
  const offerId = cleanText(product.offerId || product.ozon?.offerId);
  if (!offerId) return false;
  return ozonProductNeedsDetailRefresh(product);
}

function pickWeakOzonProductIds(products = [], maxItems = 400) {
  const limit = Math.max(0, Math.min(1000, Number(maxItems || 0) || 0));
  if (!limit) return [];
  const ids = [];
  const seen = new Set();
  for (const product of products || []) {
    const id = cleanText(product?.id);
    if (!id || seen.has(id) || !isWeakOzonWarehouseProduct(product)) continue;
    ids.push(id);
    seen.add(id);
    if (ids.length >= limit) break;
  }
  return ids;
}

function pickOzonDetailOfferIds(products = [], existingByOffer = new Map(), maxItems = 800) {
  const limit = Math.max(0, Number(maxItems || 0) || 0);
  if (!limit) return [];
  const prioritized = [];
  const seen = new Set();
  for (const product of products || []) {
    const offerId = cleanText(product.offer_id || product.offerId);
    const seenKey = ozonOfferMapKey(offerId);
    if (!offerId || seen.has(seenKey)) continue;
    const existing = getOzonOfferMapValue(existingByOffer, offerId);
    if (!existing || ozonProductNeedsDetailRefresh(existing)) {
      prioritized.push(offerId);
      seen.add(seenKey);
      if (prioritized.length >= limit) break;
    }
  }
  return prioritized;
}

async function getYandexPriceMap(shop, offerIds) {
  const map = new Map();

  for (const chunk of chunkArray(offerIds, 200)) {
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-prices`,
      { offerIds: chunk },
    );

    for (const offer of data.result?.offers || data.offers || []) {
      const value = offer.price?.value ?? offer.basicPrice?.value ?? offer.price;
      map.set(offer.offerId || offer.offer_id, Number(value || 0));
    }
  }

  return map;
}

async function getYandexOfferIdSet(shop, offerIds) {
  const set = new Set();

  const mappings = await getYandexOfferMappingsByOfferIds(shop, offerIds);
  for (const item of mappings) {
    const offerId = yandexOfferIdFromMapping(item);
    if (offerId) set.add(offerId);
  }

  return set;
}

async function getExistingYandexOfferIdSet(offerIds = []) {
  const normalizedOfferIds = Array.from(new Set((offerIds || []).map(cleanText).filter(Boolean)));
  const existing = new Set();
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  if (!normalizedOfferIds.length || !shops.length) return existing;

  for (const shop of shops) {
    const shopExisting = await getYandexOfferIdSet(shop, normalizedOfferIds);
    for (const offerId of shopExisting) {
      const normalized = cleanText(offerId).toLowerCase();
      if (normalized) existing.add(normalized);
    }
  }

  return existing;
}

function uniqueYandexShopsByBusiness(shops = null) {
  const seenBusinesses = new Set();
  const source = Array.isArray(shops) && shops.length ? shops : getYandexShops();
  return source.filter((shop) => {
    if (!shop.apiKey || !shop.businessId) return false;
    const key = String(shop.businessId);
    if (seenBusinesses.has(key)) return false;
    seenBusinesses.add(key);
    return true;
  });
}

async function sendYandexOfferMappings(shop, offers = []) {
  const results = [];
  const deduped = new Map();
  for (const offer of Array.isArray(offers) ? offers : []) {
    const normalizedOfferId = cleanText(offer.offerId).toLowerCase();
    if (!normalizedOfferId) continue;
    deduped.set(normalizedOfferId, { ...offer, offerId: cleanText(offer.offerId) });
  }
  const prepared = Array.from(deduped.values());
  for (const chunk of chunkArray(prepared, 100)) {
    if (!chunk.length) continue;
    try {
      await yandexRequest(
        shop,
        "POST",
        `/v2/businesses/${shop.businessId}/offer-mappings/update`,
        { offerMappings: chunk.map((offer) => ({ offer })) },
      );
      results.push(...chunk.map((offer) => ({ offerId: offer.offerId, ok: true })));
    } catch (error) {
      const detail = error?.message || "yandex_import_failed";
      if (chunk.length === 1) {
        results.push(...chunk.map((offer) => ({ offerId: offer.offerId, ok: false, error: detail })));
        continue;
      }
      logger.warn("yandex offer mappings chunk failed, retrying one by one", {
        shop: shop.id,
        businessId: shop.businessId,
        items: chunk.length,
        detail,
      });
      for (const offer of chunk) {
        try {
          await yandexRequest(
            shop,
            "POST",
            `/v2/businesses/${shop.businessId}/offer-mappings/update`,
            { offerMappings: [{ offer }] },
          );
          results.push({ offerId: offer.offerId, ok: true, recoveredFromChunkError: true });
        } catch (singleError) {
          results.push({
            offerId: offer.offerId,
            ok: false,
            error: singleError?.message || detail,
            chunkError: detail,
          });
        }
      }
    }
  }
  return results;
}

function yandexTargetOfferKey(target = "", offerId = "") {
  return `${cleanText(target).toLowerCase()}::${cleanText(offerId).toLowerCase()}`;
}

function marketplaceProductMarkupOverride(product = {}) {
  const markup = Number(product?.markup || 0);
  if (!Number.isFinite(markup) || markup <= 0) return 0;
  const marketplace = cleanText(product?.marketplace || product?.target || "").toLowerCase();
  if (marketplace === "yandex") {
    const manual = product?.markupSource === "manual" || product?.yandex?.extra?.manualMarkup === true;
    return manual ? markup : 0;
  }
  return markup;
}

function yandexExportProductMarkup(sourceProduct = {}, yandexProduct = null) {
  const yandexMarkup = marketplaceProductMarkupOverride(yandexProduct);
  const sourceMarkup = marketplaceProductMarkupOverride(sourceProduct);
  if (!Number.isFinite(yandexMarkup) || yandexMarkup <= 0) return 0;
  if (sourceMarkup > 0 && Math.abs(yandexMarkup - sourceMarkup) < 0.0001) return 0;
  return yandexMarkup;
}

function yandexPriceUpdateResultKey(result = {}) {
  return yandexTargetOfferKey(result.target || result.shopId || "", result.offerId || result.offer_id || "");
}

function applyYandexPriceSendToWarehouse(warehouse = {}, shop = {}, row = {}, sentAt = new Date().toISOString()) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const offerId = cleanText(row.offerId || row.offer_id);
  const price = Number(row.price?.value || row.price || 0);
  if (!offerId || !shop?.id || !Number.isFinite(price) || price <= 0) return false;
  const product = products.find((item) => {
    const normalized = normalizeWarehouseProduct(item);
    return normalized.marketplace === "yandex"
      && normalized.target === shop.id
      && cleanText(normalized.offerId).toLowerCase() === offerId.toLowerCase();
  });
  if (!product) return false;
  const cabinetPriceAtSend = Number(product.marketplacePrice || product.currentPrice || 0) || null;
  product.marketplacePrice = roundPrice(price);
  product.currentPrice = roundPrice(price);
  product.targetPrice = roundPrice(price);
  if (Number.isFinite(Number(row.targetStock))) product.targetStock = Math.max(0, Math.round(Number(row.targetStock)));
  product.yandex = { ...(product.yandex || {}), offerId, price: roundPrice(price) };
  product.lastYandexPriceSend = {
    status: "success",
    at: sentAt,
    requestedPrice: roundPrice(price),
    cabinetPriceAtSend: cabinetPriceAtSend ? roundPrice(cabinetPriceAtSend) : null,
    detail: "",
    nextRetryAt: null,
  };
  product.updatedAt = sentAt;
  return true;
}

async function buildYandexPriceOverrideLookup(products = [], shops = [], warehouse = {}, yandexPriceLookup = new Map()) {
  const overrides = new Map();
  const sourceProducts = Array.isArray(products) ? products.map(normalizeWarehouseProduct) : [];
  if (!sourceProducts.length || !shops.length) return overrides;

  let appSettings = { defaultMarkups: { yandex: Number(process.env.DEFAULT_YANDEX_MARKUP || 1.6) }, markupRules: [] };
  try {
    appSettings = await readAppSettings();
  } catch (_error) {
    // Default markup is enough for this fallback path.
  }
  const rateSource = appSettings.fixedUsdRate || (await getUsdRate().catch(() => ({ rate: process.env.DEFAULT_USD_RATE || 95 }))).rate;
  const rate = Number(rateSource || process.env.DEFAULT_USD_RATE || 95);
  const allLinks = [];
  for (const product of sourceProducts) {
    allLinks.push(...(Array.isArray(product.links) ? product.links : []));
    for (const shop of shops) {
      const yandexProduct = yandexPriceLookup.get(yandexTargetOfferKey(shop.id, product.offerId));
      allLinks.push(...(Array.isArray(yandexProduct?.links) ? yandexProduct.links : []));
    }
  }
  let matchMap = new Map();
  if (allLinks.length) {
    try {
      matchMap = await getPriceMasterMatchesForLinks(allLinks, warehouse.suppliers || [], rate);
    } catch (error) {
      logger.warn("yandex import PriceMaster price calculation skipped", { detail: error?.message || String(error) });
    }
  }

  for (const product of sourceProducts) {
    if (!product.offerId) continue;
    for (const shop of shops) {
      const lookupKey = yandexTargetOfferKey(shop.id, product.offerId);
      const yandexProduct = yandexPriceLookup.get(lookupKey) || null;
      const yandexLinks = Array.isArray(yandexProduct?.links) && yandexProduct.links.length ? yandexProduct.links : product.links;
      const markupOverride = yandexExportProductMarkup(product, yandexProduct);
      const suppliers = (Array.isArray(yandexLinks) ? yandexLinks.map(normalizeWarehouseLink) : [])
        .flatMap((link) => (matchMap.get(link.id) || []).map((match) => {
          const markupCoefficient = resolveMarkupCoefficient({
            productMarkup: markupOverride,
            marketplace: "yandex",
            supplierUsdPrice: match.price,
            appSettings,
          });
          return {
            ...match,
            markupCoefficient,
            calculatedPrice: calculateRubPrice(match.price, rate, markupCoefficient),
          };
        }));
      const availableSupplierCount = suppliers.filter((supplier) => supplier.available).length;
      const selectedSupplier = pickWarehouseSupplier(suppliers);
      if (selectedSupplier) {
        const availabilityPolicy = resolveAvailabilityPolicy({
          marketplace: "yandex",
          availableSupplierCount,
          baseMarkup: Number(selectedSupplier.markupCoefficient || 0),
          appSettings,
        });
        const markupCoefficient = Number(availabilityPolicy.markupCoefficient || selectedSupplier.markupCoefficient || 0);
        const price = calculateRubPrice(selectedSupplier.price, rate, markupCoefficient);
        if (price > 0) {
          overrides.set(lookupKey, {
            price,
            markupCoefficient,
            targetStock: Number.isFinite(Number(availabilityPolicy.targetStock)) ? availabilityPolicy.targetStock : null,
            source: "pricemaster",
          });
          continue;
        }
      }

      const persistedPrice = Number(
        yandexProduct?.targetPrice ||
          yandexProduct?.nextPrice ||
          yandexProduct?.marketplacePrice ||
          yandexProduct?.currentPrice ||
          0,
      ) || 0;
      if (persistedPrice > 0) {
        overrides.set(lookupKey, {
          price: persistedPrice,
          markupCoefficient: Number(yandexProduct?.markupCoefficient || yandexProduct?.markup || 0) || null,
          targetStock: Number.isFinite(Number(yandexProduct?.targetStock)) ? yandexProduct.targetStock : null,
          source: "yandex_row",
        });
      }
    }
  }

  return overrides;
}

function buildYandexPriceUpdateFromOzonProduct(product = {}, {
  yandexProduct = null,
  priceOverride = null,
  allowSourceFallback = true,
} = {}) {
  const normalized = normalizeWarehouseProduct(yandexProduct || product);
  const hasYandexProduct = Boolean(yandexProduct && Object.keys(yandexProduct).length);
  const forcedPrice = Number(priceOverride || 0) || 0;
  const overridePrice = Number(
    forcedPrice ||
      yandexProduct?.nextPrice ||
      yandexProduct?.targetPrice ||
      yandexProduct?.marketplacePrice ||
      yandexProduct?.currentPrice ||
      0,
  ) || 0;
  const built = buildYandexOfferMapping(normalized, overridePrice > 0 ? { price: overridePrice } : {});
  const offerId = cleanText(built.offer?.offerId || normalized.offerId || normalized.offer_id);
  const priceCandidates = [
    forcedPrice,
    yandexProduct?.nextPrice,
    yandexProduct?.targetPrice,
    yandexProduct?.marketplacePrice,
    yandexProduct?.currentPrice,
  ];
  if (allowSourceFallback || hasYandexProduct) {
    priceCandidates.push(
      built.offer?.basicPrice?.value,
      normalized.nextPrice,
      normalized.targetPrice,
      normalized.marketplacePrice,
      normalized.currentPrice,
      normalized.ozon?.price,
      normalized.price,
    );
  }
  const price = priceCandidates
    .map((value) => Number(value))
    .find((value) => Number.isFinite(value) && value > 0) || 0;
  const rounded = roundPrice(price);
  if (!offerId || !Number.isFinite(rounded) || rounded <= 0) return null;
  return { id: normalized.id, offerId, price: { value: rounded, currencyId: "RUR" } };
}

async function sendYandexPricesFromOzonProducts(products = [], options = {}) {
  const dryRun = options.dryRun === true;
  const warnings = [];
  const shops = Array.isArray(options.shops) && options.shops.length
    ? options.shops
    : uniqueYandexShopsByBusiness();
  const sourceProducts = Array.isArray(products) ? products : [];
  const sourceWarehouse = options.warehouse || (await readWarehouse().catch((error) => {
    logger.warn("read warehouse before yandex price send failed", { detail: error?.message || String(error) });
    return { products: [] };
  }));
  const yandexPriceLookup = new Map();
  for (const item of Array.isArray(sourceWarehouse.products) ? sourceWarehouse.products : []) {
    const normalized = normalizeWarehouseProduct(item);
    if (normalized.marketplace !== "yandex" || !normalized.offerId || !normalized.target) continue;
    yandexPriceLookup.set(yandexTargetOfferKey(normalized.target, normalized.offerId), normalized);
  }
  const priceOverrides = shops.length
    ? await buildYandexPriceOverrideLookup(sourceProducts, shops, sourceWarehouse, yandexPriceLookup)
    : new Map();
  const requestedPriceRows = sourceProducts.length;
  const rows = sourceProducts
    .map((product) => {
      const normalized = normalizeWarehouseProduct(product);
      const primaryShop = shops[0] || {};
      const lookupKey = yandexTargetOfferKey(primaryShop.id, normalized.offerId);
      const override = priceOverrides.get(lookupKey) || null;
      return buildYandexPriceUpdateFromOzonProduct(product, {
        yandexProduct: yandexPriceLookup.get(lookupKey) || null,
        priceOverride: override?.price || null,
        allowSourceFallback: false,
      });
    })
    .filter(Boolean);

  if (!rows.length || !shops.length) {
    if (shops.length && requestedPriceRows && !rows.length) {
      warnings.push("Yandex: price send skipped, no calculated Yandex price was available.");
    }
    return {
      ok: true,
      dryRun,
      sent: 0,
      failed: 0,
      skipped: shops.length ? requestedPriceRows : rows.length,
      warnings: shops.length ? warnings : ["Yandex Market не настроен."],
      results: [],
    };
  }

  let existingOfferIds = options.existingOfferIds instanceof Set ? options.existingOfferIds : null;
  if (!existingOfferIds) {
    const existingCheckTimeoutMs = Math.max(1000, Number(process.env.OZON_YANDEX_EXISTING_CHECK_TIMEOUT_MS || 8000) || 8000);
    try {
      existingOfferIds = await Promise.race([
        getExistingYandexOfferIdSet(rows.map((row) => row.offerId)),
        promiseTimeout(existingCheckTimeoutMs, "yandex_existing_check_timeout"),
      ]);
    } catch (error) {
      const label = error?.message === "yandex_existing_check_timeout"
        ? `таймаут ${Math.round(existingCheckTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие артикулы перед отправкой цен (${label})`);
      existingOfferIds = new Set();
    }
  }

  const selected = rows.filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  let sourceWarehouseMutated = false;
  if (dryRun) {
    return {
      ok: true,
      dryRun,
      sent: 0,
      failed: 0,
      skipped: Math.max(0, requestedPriceRows - selected.length),
      skippedNoPrice: Math.max(0, requestedPriceRows - rows.length),
      planned: selected.length,
      warnings,
      results: selected.map((row) => ({ ...row, ok: true, stage: "price", dryRun: true })),
    };
  }

  for (const shop of shops) {
    const shopRows = sourceProducts
      .map((product) => {
        const normalized = normalizeWarehouseProduct(product);
        const lookupKey = yandexTargetOfferKey(shop.id, normalized.offerId);
        const override = priceOverrides.get(lookupKey) || null;
        const row = buildYandexPriceUpdateFromOzonProduct(product, {
          yandexProduct: yandexPriceLookup.get(lookupKey) || null,
          priceOverride: override?.price || null,
          allowSourceFallback: false,
        });
        return row ? {
          ...row,
          sourceId: normalized.id,
          targetStock: override?.targetStock ?? null,
          markupCoefficient: override?.markupCoefficient ?? null,
          priceSource: override?.source || "",
        } : null;
      })
      .filter(Boolean)
      .filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));

    for (const chunk of chunkArray(shopRows, 500)) {
      if (!chunk.length) continue;
      try {
        const sentAt = new Date().toISOString();
        await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, {
          offers: chunk.map((item) => ({ offerId: item.offerId, price: item.price })),
        });
        for (const item of chunk) {
          if (applyYandexPriceSendToWarehouse(sourceWarehouse, shop, item, sentAt)) sourceWarehouseMutated = true;
        }
        results.push(...chunk.map((item) => ({
          stage: "price",
          sourceId: item.sourceId,
          offerId: item.offerId,
          target: shop.id,
          targetName: shop.name || "Yandex Market",
          price: item.price?.value || null,
          targetStock: item.targetStock,
          markupCoefficient: item.markupCoefficient,
          priceSource: item.priceSource,
          ok: true,
        })));
      } catch (error) {
        const label = error?.message || error?.code || "ошибка API";
        warnings.push(`Yandex «${shop.name || shop.id}»: цены не отправлены (${label})`);
        results.push(...chunk.map((item) => ({
          stage: "price",
          sourceId: item.sourceId,
          offerId: item.offerId,
          target: shop.id,
          targetName: shop.name || "Yandex Market",
          price: item.price?.value || null,
          targetStock: item.targetStock,
          markupCoefficient: item.markupCoefficient,
          priceSource: item.priceSource,
          ok: false,
          error: label,
        })));
      }
    }
  }

  if (sourceWarehouseMutated) {
    await writeWarehouse(sourceWarehouse);
  }

  const failed = results.filter((item) => !item.ok).length;
  return {
    ok: warnings.length === 0 && failed === 0,
    dryRun,
    sent: results.filter((item) => item.ok).length,
    failed,
    skipped: Math.max(0, requestedPriceRows - selected.length),
    skippedNoPrice: Math.max(0, requestedPriceRows - rows.length),
    planned: selected.length,
    warnings,
    results,
  };
}

function pickYandexOfferFromMapping(item = {}) {
  return item.offer || item.mapping?.offer || item.mapping || item;
}

function buildYandexProductUrl(offer = {}, item = {}) {
  const directUrl = cleanText(
    offer.url || offer.marketUrl || offer.publicUrl || item.url || item.marketUrl || item.publicUrl,
  );
  if (directUrl) return directUrl;

  const sku = cleanText(offer.marketSku || offer.modelId || item.marketSku || item.mapping?.marketSku);
  if (sku) return `https://market.yandex.ru/product--/${encodeURIComponent(sku)}`;

  const query = cleanText(offer.name || item.offer?.name || offer.offerId || item.offerId || item.mapping?.offerId);
  return query ? `https://market.yandex.ru/search?text=${encodeURIComponent(query)}` : "";
}

function pickYandexState(item = {}, offer = {}) {
  const rawState = cleanText(
    offer.campaignStatus?.status ||
      offer.processingState?.status ||
      offer.status ||
      offer.state ||
      offer.availability ||
      item.status ||
      item.state ||
      item.offer?.campaignStatus?.status ||
      item.offer?.processingState?.status ||
      item.offer?.status ||
      item.offer?.availability,
  );
  const availability = cleanText(offer.availability || item.offer?.availability || item.availability);
  const state = rawState.toLowerCase();
  const priceValue = Number(offer.basicPrice?.value ?? offer.price?.value ?? item.offer?.basicPrice?.value ?? item.price?.value ?? 0);
  const archived = Boolean(offer.archived || item.archived || item.offer?.archived || state.includes("archive") || availability === "DELISTED");
  const disabled = state.includes("inactive")
    || state.includes("disabled")
    || state.includes("disabled_by_partner")
    || state.includes("disabled_automatically")
    || state.includes("delisted")
    || state.includes("rejected")
    || state.includes("rejected_by_market")
    || state.includes("no_card")
    || state.includes("need_content")
    || state.includes("hidden")
    || state.includes("not published")
    || availability === "INACTIVE";
  const outOfStock = state.includes("out_of_stock")
    || state.includes("out-of-stock")
    || state.includes("no_stocks")
    || state.includes("unavailable")
    || state.includes("not_available")
    || state.includes("нет в наличии");

  if (archived) return { code: "archived", label: "В архиве ЯМ", stateName: rawState || availability || "Архив" };
  if (outOfStock || priceValue <= 0) return { code: "out_of_stock", label: "Нет наличия ЯМ", stateName: rawState || availability || "Нет цены или остатка" };
  if (disabled) return { code: "inactive", label: "Неактивен ЯМ", stateName: rawState || availability || "Неактивен" };
  return { code: "active", label: "Активен ЯМ", stateName: rawState || availability || "Опубликован" };
}

function normalizeYandexWarehouseProduct(item = {}, shop) {
  const offer = pickYandexOfferFromMapping(item);
  const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
  if (!offerId) return null;

  const priceValue = offer.basicPrice?.value ?? offer.price?.value ?? item.offer?.basicPrice?.value ?? item.price?.value;
  const pictures = offer.pictures || offer.urls || item.offer?.pictures || [];
  const barcodes = offer.barcodes || item.offer?.barcodes || [];

  return normalizeWarehouseProduct({
    target: shop.id,
    marketplace: "yandex",
    targetName: shop.name || "Yandex Market",
    offerId,
    name: offer.name || item.offer?.name || offerId,
    source: "marketplace",
    imageUrl: firstImageUrl(pictures),
    productUrl: buildYandexProductUrl(offer, item),
    marketplacePrice: Number(priceValue || 0) || null,
    marketplaceState: pickYandexState(item, offer),
    yandex: {
      offerId,
      name: offer.name || item.offer?.name || offerId,
      url: buildYandexProductUrl(offer, item),
      description: offer.description || "",
      marketCategoryId: offer.marketCategoryId || item.mapping?.marketCategoryId,
      vendor: offer.vendor || "",
      pictures,
      barcodes,
      price: Number(priceValue || 0) || undefined,
      extra: {},
    },
    createdAt: new Date().toISOString(),
  });
}

function yandexWarehouseProductId(shop = {}, offerId = "") {
  const key = `${cleanText(shop.id || shop.businessId || "yandex")}:${cleanText(offerId).toLowerCase()}`;
  return `yandex-${crypto.createHash("sha1").update(key).digest("hex").slice(0, 24)}`;
}

function buildYandexWarehouseProductFromOzonExport(product = {}, shop = {}, exportState = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const offerId = cleanText(normalized.offerId);
  const price = Number(exportState.price || exportState.targetPrice || 0) || null;
  const sentAt = exportState.sentAt || new Date().toISOString();
  const lastYandexPriceSend = price > 0
    ? {
        status: "success",
        at: sentAt,
        requestedPrice: price,
        cabinetPriceAtSend: null,
        detail: "",
        nextRetryAt: null,
      }
    : null;
  const stock = Number.isFinite(Number(exportState.stock))
    ? Math.max(0, Math.round(Number(exportState.stock)))
    : pickOzonProductStockForYandex(normalized);
  const pictures = [
    ...new Set([
      ...([normalized.imageUrl].filter(Boolean)),
      ...(Array.isArray(normalized.ozon?.images) ? normalized.ozon.images : []),
      ...(Array.isArray(normalized.yandex?.pictures) ? normalized.yandex.pictures : []),
    ].map(cleanText).filter(Boolean)),
  ];
  return normalizeWarehouseProduct({
    id: yandexWarehouseProductId(shop, offerId),
    target: shop.id || "yandex",
    marketplace: "yandex",
    targetName: shop.name || "Yandex Market",
    offerId,
    productId: normalized.productId,
    sku: normalized.sku,
    manualGroupId: normalized.manualGroupId,
    name: normalized.name || normalized.ozon?.name || normalized.yandex?.name || offerId,
    keyword: normalized.keyword,
    imageUrl: normalized.imageUrl || firstImageUrl(pictures),
    productUrl: normalized.yandex?.url || "",
    marketplacePrice: price,
    currentPrice: price,
    targetPrice: price,
    targetStock: stock,
    markup: Number(exportState.productMarkup || 0) || 0,
    autoPriceEnabled: normalized.autoPriceEnabled,
    autoPriceMin: normalized.autoPriceMin,
    autoPriceMax: normalized.autoPriceMax,
    lastYandexPriceSend,
    source: "marketplace",
    yandex: {
      ...(normalized.yandex || {}),
      offerId,
      name: normalized.name || normalized.ozon?.name || normalized.yandex?.name || offerId,
      description: normalized.ozon?.description || normalized.yandex?.description || "",
      vendor: normalized.ozon?.vendor || normalized.yandex?.vendor || normalized.brand || "",
      pictures,
      price: price || undefined,
      extra: {
        ...(normalized.yandex?.extra || {}),
        exportedFrom: "ozon",
        sourceProductId: normalized.id,
        shopId: shop.id || "",
        businessId: shop.businessId || "",
        campaignId: shop.campaignId || "",
        exportedAt: sentAt,
      },
    },
    marketplaceState: {
      code: stock > 0 ? "active" : "out_of_stock",
      label: stock > 0 ? "Активен ЯМ" : "Нет наличия ЯМ",
      stock,
      stateName: exportState.status === "sent" ? "Выгружено из Ozon" : "Создано из Ozon",
    },
    exports: {
      ...(normalized.exports || {}),
      yandex: exportState,
      [shop.id || "yandex"]: exportState,
    },
    links: Array.isArray(normalized.links) ? normalized.links : [],
    createdAt: normalized.createdAt,
    updatedAt: sentAt,
  });
}

function materializeYandexExportedProductsForWarehouse(warehouse = {}) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { warehouse: { ...warehouse, products }, added: 0 };

  const existingKeys = new Set(products.map((product) => warehouseProductExactMergeKey(normalizeWarehouseProduct(product))));
  const additions = [];

  for (const product of products) {
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== "ozon") continue;
    if (!normalized.offerId) continue;
    const exports = normalized.exports || {};
    const yandexSent = exports.yandex?.status === "sent";
    const matchingShops = shops.filter((shop) => exports[shop.id]?.status === "sent");
    const targetShops = matchingShops.length ? matchingShops : (yandexSent ? shops.slice(0, 1) : []);
    for (const shop of targetShops) {
      const exactKey = warehouseProductExactMergeKey({ target: shop.id, marketplace: "yandex", offerId: normalized.offerId });
      if (existingKeys.has(exactKey)) continue;
      const exportState = exports[shop.id] || exports.yandex || { status: "sent", targetName: shop.name || "Yandex Market" };
      const yandexProduct = buildYandexWarehouseProductFromOzonExport(normalized, shop, exportState);
      additions.push(yandexProduct);
      existingKeys.add(exactKey);
    }
  }

  if (!additions.length) return { warehouse: { ...warehouse, products }, added: 0 };
  return {
    warehouse: { ...warehouse, products: mergeProducts(products, additions) },
    added: additions.length,
  };
}

async function getYandexOfferMappings(shop, limit = Number.POSITIVE_INFINITY, options = {}) {
  const maxItems = Number.isFinite(Number(limit)) && Number(limit) > 0 ? Number(limit) : Number.MAX_SAFE_INTEGER;
  const items = [];
  let pageToken = "";

  while (items.length < maxItems) {
    const pageLimit = Math.min(100, maxItems - items.length);
    const params = new URLSearchParams({ limit: String(pageLimit) });
    if (pageToken) params.set("pageToken", pageToken);
    const body = options.archived === true || options.archived === false
      ? { archived: options.archived }
      : undefined;

    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings?${params.toString()}`,
      body,
    );
    const pageItems = data.result?.offerMappings || data.result?.offers || data.offerMappings || [];
    items.push(...pageItems);

    pageToken =
      data.result?.paging?.nextPageToken ||
      data.result?.nextPageToken ||
      data.paging?.nextPageToken ||
      data.nextPageToken ||
      "";
    if (!pageToken || !pageItems.length) break;
  }

  return items.slice(0, maxItems);
}

async function getYandexOfferMappingsByOfferIds(shop, offerIds = []) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  const items = [];
  if (!ids.length) return items;

  for (const chunk of chunkArray(ids, 100)) {
    const body = { offerIds: chunk };
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings`,
      body,
    );
    items.push(...(data.result?.offerMappings || data.result?.offers || data.offerMappings || []));
  }

  return items;
}

function normalizeYandexOfferCardQuality(item = {}, shop = {}) {
  const offerId = cleanText(item.offerId || item.offer_id || item.shopSku || item.sku);
  const contentRating = Number(item.contentRating ?? item.content_rating ?? item.rating ?? 0);
  const averageContentRating = Number(item.averageContentRating ?? item.average_content_rating ?? 0);
  const recommendations = Array.isArray(item.recommendations) ? item.recommendations : [];
  const errors = Array.isArray(item.errors) ? item.errors : [];
  const warnings = Array.isArray(item.warnings) ? item.warnings : [];
  return compactObject({
    offerId,
    target: shop.id || "",
    contentRating: Number.isFinite(contentRating) ? contentRating : 0,
    averageContentRating: Number.isFinite(averageContentRating) ? averageContentRating : undefined,
    contentRatingStatus: cleanText(item.contentRatingStatus || item.content_rating_status),
    cardStatus: cleanText(item.cardStatus || item.card_status),
    recommendations,
    errors,
    warnings,
    updatedAt: new Date().toISOString(),
  });
}

async function getYandexOfferCardsContentStatus(shop, offerIds = [], options = {}) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  const items = [];
  if (!ids.length) return items;
  const withRecommendations = options.withRecommendations !== false;

  for (const chunk of chunkArray(ids, 200)) {
    const params = new URLSearchParams({ limit: String(chunk.length) });
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-cards?${params.toString()}`,
      { offerIds: chunk, withRecommendations },
    );
    const rows = data.result?.offerCards || data.result?.offers || data.offerCards || [];
    items.push(...rows.map((item) => normalizeYandexOfferCardQuality(item, shop)).filter((item) => item.offerId));
  }

  return items;
}

function yandexOfferIdFromMapping(item = {}) {
  const offer = pickYandexOfferFromMapping(item);
  return cleanText(offer.offerId || item.offerId || item.mapping?.offerId || item.offer?.offerId);
}

function getLocalYandexExportedOfferIdSet(products = []) {
  const set = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const offerId = cleanText(normalized.offerId || normalized.offer_id).toLowerCase();
    if (!offerId) continue;
    if (normalized.marketplace === "yandex") set.add(offerId);
    const exports = normalized.exports || {};
    if (exports.yandex?.status === "sent") set.add(offerId);
    if (Object.values(exports).some((entry) => entry?.status === "sent" && /yandex/i.test(cleanText(entry.targetName || entry.marketplace || entry.target)))) {
      set.add(offerId);
    }
  }
  return set;
}

async function readYandexExistingOfferIdCache({ maxAgeMs = 24 * 60 * 60 * 1000 } = {}) {
  try {
    const data = JSON.parse(await fs.readFile(yandexExistingOffersCachePath, "utf8"));
    const ageMs = Date.now() - new Date(data.updatedAt || 0).getTime();
    if (maxAgeMs > 0 && (!Number.isFinite(ageMs) || ageMs > maxAgeMs)) return new Set();
    return new Set((Array.isArray(data.offerIds) ? data.offerIds : [])
      .map((id) => cleanText(id).toLowerCase())
      .filter(Boolean));
  } catch (error) {
    if (error.code === "ENOENT") return new Set();
    logger.warn("read Yandex existing offer cache failed", { detail: error?.message || String(error) });
    return new Set();
  }
}

async function writeYandexExistingOfferIdCache(offerIds = []) {
  const normalized = Array.from(new Set(Array.from(offerIds)
    .map((id) => cleanText(id).toLowerCase())
    .filter(Boolean)))
    .sort();
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(
    yandexExistingOffersCachePath,
    JSON.stringify({ updatedAt: new Date().toISOString(), offerIds: normalized }, null, 2),
    "utf8",
  );
  return new Set(normalized);
}

async function refreshYandexExistingOfferIdCache({ limit = Number(process.env.YANDEX_EXISTING_CATALOG_LIMIT || 50000) || 50000 } = {}) {
  const maxItems = Math.max(1, Math.min(100000, Number(limit) || 50000));
  const set = new Set();
  for (const shop of uniqueYandexShopsByBusiness()) {
    for (const archived of [false, true]) {
      const remaining = Math.max(0, maxItems - set.size);
      if (!remaining) break;
      const mappings = await getYandexOfferMappings(shop, remaining, { archived });
      for (const item of mappings) {
        const offerId = yandexOfferIdFromMapping(item).toLowerCase();
        if (offerId) set.add(offerId);
      }
    }
  }
  return writeYandexExistingOfferIdCache(set);
}

async function getKnownYandexExistingOfferIds(offerIds = [], {
  products = [],
  warnings = [],
  allowCatalogRefresh = false,
  allowDirectCheck = true,
} = {}) {
  const set = new Set([
    ...await readYandexExistingOfferIdCache(),
    ...getLocalYandexExportedOfferIdSet(products),
  ]);
  if (allowCatalogRefresh) {
    const refreshTimeoutMs = Math.max(1000, Number(process.env.YANDEX_EXISTING_CATALOG_TIMEOUT_MS || 20000) || 20000);
    try {
      const refreshed = await Promise.race([
        refreshYandexExistingOfferIdCache(),
        promiseTimeout(refreshTimeoutMs, "yandex_existing_catalog_timeout"),
      ]);
      for (const id of refreshed) set.add(id);
    } catch (error) {
      const label = error?.message === "yandex_existing_catalog_timeout"
        ? `таймаут ${Math.round(refreshTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось обновить полный список SKU (${label}), использую локальный кэш.`);
      logger.warn("yandex existing catalog refresh failed", { detail: label });
    }
  }
  const normalizedOfferIds = Array.from(new Set((offerIds || []).map(cleanText).filter(Boolean)));
  if (allowDirectCheck && normalizedOfferIds.length) {
    const checkTimeoutMs = Math.max(1000, Number(process.env.OZON_YANDEX_EXISTING_CHECK_TIMEOUT_MS || 8000) || 8000);
    try {
      const direct = await Promise.race([
        getExistingYandexOfferIdSet(normalizedOfferIds),
        promiseTimeout(checkTimeoutMs, "yandex_existing_check_timeout"),
      ]);
      for (const id of direct) set.add(id);
      if (direct.size) writeYandexExistingOfferIdCache(new Set([...set, ...direct]))
        .catch((error) => logger.warn("write Yandex existing offer cache failed", { detail: error?.message || String(error) }));
    } catch (error) {
      const label = error?.message === "yandex_existing_check_timeout"
        ? `таймаут ${Math.round(checkTimeoutMs / 1000)} с`
        : error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие SKU (${label}), использую локальный кэш.`);
      logger.warn("yandex existing offers check failed", { detail: label });
    }
  }
  return set;
}

async function getPriceMasterOffersByArticle(offerIds, usdRate) {
  if (!offerIds.length) return new Map();

  const map = new Map();

  for (const chunk of chunkArray(offerIds, 500)) {
    const placeholders = chunk.map(() => "?").join(",");
    const [rows] = await pool.query(
      `
      SELECT
        r.NativeID AS article,
        r.NativeName AS name,
        r.NativePrice AS price,
        r.Active AS active,
        r.IsNew AS isNew,
        r.Ignored AS ignored,
        r.RowID AS rowId,
        d.DocDate AS docDate,
        d.PartnerID AS partnerId,
        p.PartnerName AS partnerName
      FROM OfferRows r
      JOIN OfferDocs d ON d.DocID = r.DocID
      LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
      WHERE r.NativeID IN (${placeholders}) AND r.Ignored = 0
      ORDER BY r.NativeID, d.DocDate DESC, r.RowID DESC
      `,
      chunk,
    );

    for (const row of rows) {
      if (!map.has(row.article)) {
        const normalizedPrice = normalizePriceMasterPrice(row.price, usdRate);
        map.set(row.article, {
          ...row,
          ...normalizedPrice,
          active: Boolean(row.active),
          isNew: Boolean(row.isNew),
        });
      }
    }
  }

  return map;
}

async function buildOzonPricePreview({ limit = 500, multiplier = 1, onlyChanged = true } = {}) {
  const safeMultiplier = Number.isFinite(Number(multiplier)) ? Number(multiplier) : 1;
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const ozonProducts = await getOzonProducts(limit);
  const offerIds = ozonProducts.map((item) => item.offer_id).filter(Boolean);
  const [ozonPriceMap, pmOfferMap] = await Promise.all([
    getOzonPriceMap(offerIds),
    getPriceMasterOffersByArticle(offerIds, usdRate),
  ]);

  const rows = ozonProducts.map((product) => {
    const ozonPrice = ozonPriceMap.get(product.offer_id);
    const pmOffer = pmOfferMap.get(product.offer_id);
    const currentOzonPrice = pickOzonCabinetListedPrice(normalizeOzonPriceDetails(ozonPrice || {})) || 0;
    const sourcePrice = Number(pmOffer?.price || 0);
    const nextPrice = pmOffer ? roundPrice(sourcePrice * safeMultiplier) : 0;
    const changed = Boolean(pmOffer && nextPrice > 0 && nextPrice !== currentOzonPrice);

    return {
      offerId: product.offer_id,
      productId: product.product_id,
      archived: Boolean(product.archived),
      hasFbsStocks: Boolean(product.has_fbs_stocks),
      pmFound: Boolean(pmOffer),
      pmName: pmOffer?.name || null,
      pmPartner: pmOffer?.partnerName || null,
      pmDocDate: pmOffer?.docDate || null,
      pmActive: pmOffer?.active ?? null,
      pmPrice: sourcePrice || null,
      ozonPrice: currentOzonPrice || null,
      nextPrice,
      changed,
      ready: Boolean(pmOffer && pmOffer.active && nextPrice > 0),
    };
  });

  return {
    createdAt: new Date().toISOString(),
    multiplier: safeMultiplier,
    totalOzon: ozonProducts.length,
    matched: rows.filter((row) => row.pmFound).length,
    changed: rows.filter((row) => row.changed).length,
    ready: rows.filter((row) => row.ready).length,
    rows: onlyChanged ? rows.filter((row) => row.changed) : rows,
  };
}

async function readOzonProductRules() {
  const defaults = {
    priceMultiplier: Number(process.env.OZON_PRICE_MULTIPLIER || 1),
    currencyCode: "RUB",
    vat: "0",
    categoryId: null,
    dimensionUnit: "mm",
    depth: null,
    width: null,
    height: null,
    weightUnit: "g",
    weight: null,
    primaryImageUrl: "",
    descriptionTemplate: "{name}",
    attributes: [],
  };

  try {
    return { ...defaults, ...JSON.parse(await fs.readFile(ozonProductRulesPath, "utf8")) };
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }

  try {
    return { ...defaults, ...JSON.parse(await fs.readFile(ozonProductRulesExamplePath, "utf8")) };
  } catch (error) {
    if (error.code === "ENOENT") return defaults;
    throw error;
  }
}

async function getOzonOfferIdSet(limit = 5000, account = null) {
  const products = await getOzonProducts(limit, account);
  return new Set(products.map((item) => item.offer_id).filter(Boolean));
}

async function getPriceMasterProductCandidates({ limit = 200, search = "" } = {}) {
  const params = [];
  const conditions = ["r.Ignored = 0", "r.Active = 1", "r.NativeID IS NOT NULL", "r.NativeID <> ''"];

  if (search) {
    conditions.push("(r.NativeID LIKE ? OR r.NativeName LIKE ?)");
    params.push(likeSearch(search), likeSearch(search));
  }

  params.push(limit);
  const [rows] = await pool.query(
    `
    SELECT
      r.NativeID AS offerId,
      r.NativeName AS name,
      r.NativePrice AS price,
      r.BarCode AS barcode,
      r.RowID AS rowId,
      d.DocDate AS docDate,
      p.PartnerName AS partnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE ${conditions.join(" AND ")}
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT ?
    `,
    params,
  );

  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const unique = new Map();
  for (const row of rows) {
    if (!unique.has(row.offerId)) {
      unique.set(row.offerId, { ...row, ...normalizePriceMasterPrice(row.price, usdRate) });
    }
  }
  return Array.from(unique.values());
}

function fillTemplate(template, row) {
  return String(template || "{name}")
    .replaceAll("{name}", row.name || "")
    .replaceAll("{offer_id}", row.offerId || "")
    .replaceAll("{price}", String(row.price || ""));
}

function buildOzonProductPayload(row, rules) {
  const price = roundPrice(Number(row.price || 0) * Number(rules.priceMultiplier || 1));
  const images = String(rules.primaryImageUrl || "").trim() ? [String(rules.primaryImageUrl).trim()] : [];
  const item = {
    offer_id: row.offerId,
    name: row.name,
    description: fillTemplate(rules.descriptionTemplate, row),
    category_id: Number(rules.categoryId || 0),
    price: String(price),
    currency_code: rules.currencyCode || "RUB",
    vat: String(rules.vat ?? "0"),
    depth: Number(rules.depth || 0),
    width: Number(rules.width || 0),
    height: Number(rules.height || 0),
    dimension_unit: rules.dimensionUnit || "mm",
    weight: Number(rules.weight || 0),
    weight_unit: rules.weightUnit || "g",
    images,
    attributes: Array.isArray(rules.attributes) ? rules.attributes : [],
  };

  const missing = [];
  if (!item.offer_id) missing.push("offer_id");
  if (!item.name) missing.push("name");
  if (!item.price || Number(item.price) <= 0) missing.push("price");
  if (!item.category_id) missing.push("category_id");
  if (!item.depth) missing.push("depth");
  if (!item.width) missing.push("width");
  if (!item.height) missing.push("height");
  if (!item.weight) missing.push("weight");

  const warnings = [];
  if (!images.length) warnings.push("images");
  if (!item.attributes.length) warnings.push("attributes");

  return { item, missing, warnings, ready: missing.length === 0 };
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
      marketplaces: ["ozon", "yandex"],
      lookbackHours: 48,
      includeOzonStatuses: ["awaiting_packaging"],
      includeYandexStatuses: ["PROCESSING"],
      includeYandexSubstatuses: ["STARTED"],
    },
  };
}

function normalizeMarkupRule(input = {}) {
  const minUsd = Number(input.minUsd ?? input.min_usd ?? 0);
  const coefficient = Number(input.coefficient ?? input.markup ?? 0);
  const rawMarketplace = cleanText(input.marketplace || input.target || "all").toLowerCase();
  const marketplace = rawMarketplace === "ozon" || rawMarketplace === "yandex" ? rawMarketplace : "all";
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
    ? raw.marketplaces.map((item) => cleanText(item).toLowerCase()).filter((item) => item === "ozon" || item === "yandex")
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
    mode: cleanText(raw.mode).toLowerCase() === "auto" ? "auto" : "draft",
    marketplaces: marketplaces.length ? Array.from(new Set(marketplaces)) : ["ozon", "yandex"],
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

function normalizePriceMasterPartnerRows(rows = []) {
  const unique = new Map();
  for (const row of rows || []) {
    const partnerId = cleanText(row.partnerId ?? row.PartnerID ?? row.id);
    const name = cleanText(row.name ?? row.PartnerName ?? row.partnerName);
    if (!name) continue;
    const key = partnerId || normalizeSupplierName(name);
    if (!unique.has(key)) unique.set(key, { partnerId, name });
  }
  return Array.from(unique.values()).sort((left, right) =>
    String(left.name).localeCompare(String(right.name), "ru", { sensitivity: "base" }),
  );
}

async function listPriceMasterPartners() {
  const queries = [
    {
      label: "partners_with_offers",
      sql: `
        SELECT DISTINCT
          p.PartnerID AS partnerId,
          p.PartnerName AS name
        FROM Partners p
        JOIN OfferDocs d ON d.PartnerID = p.PartnerID
        WHERE p.PartnerName IS NOT NULL AND TRIM(p.PartnerName) <> ''
        ORDER BY p.PartnerName
      `,
    },
    {
      label: "partners_all",
      sql: `
        SELECT DISTINCT
          PartnerID AS partnerId,
          PartnerName AS name
        FROM Partners
        WHERE PartnerName IS NOT NULL AND TRIM(PartnerName) <> ''
        ORDER BY PartnerName
      `,
    },
    {
      label: "offer_docs_partners",
      sql: `
        SELECT DISTINCT
          d.PartnerID AS partnerId,
          COALESCE(NULLIF(TRIM(p.PartnerName), ''), CONCAT('Partner ', d.PartnerID)) AS name
        FROM OfferDocs d
        JOIN OfferRows r ON r.DocID = d.DocID
        LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
        WHERE d.PartnerID IS NOT NULL AND r.Ignored = 0
        ORDER BY name
      `,
      allowEmpty: true,
    },
  ];
  let lastError = null;
  for (const query of queries) {
    try {
      const [rows] = await pool.query(query.sql);
      const partners = normalizePriceMasterPartnerRows(rows);
      if (partners.length || query.allowEmpty) {
        if (query.label !== "partners_with_offers") {
          logger.info("PriceMaster partners loaded with fallback", { source: query.label, partners: partners.length });
        }
        return partners;
      }
      logger.warn("PriceMaster partners query returned no rows, trying fallback", { source: query.label });
    } catch (error) {
      lastError = error;
      logger.warn("PriceMaster partners query failed, trying fallback", { source: query.label, detail: error.message });
    }
  }
  if (lastError) throw lastError;
  return [];
}

async function listBrandFallbackCandidates(query, limit = 40) {
  const q = normalizeSupplierName(query);
  const unique = new Map();
  try {
    const warehouse = await readWarehouse();
    for (const product of warehouse.products || []) {
      const values = [
        cleanText(product?.ozon?.vendor),
        cleanText(product?.yandex?.vendor),
      ].filter(Boolean);
      for (const brand of values) {
        const key = normalizeSupplierName(brand);
        if (!key || (q && !key.includes(q)) || unique.has(key)) continue;
        unique.set(key, brand);
        if (unique.size >= limit) break;
      }
      if (unique.size >= limit) break;
    }
  } catch (_error) {
    // fallback should never block the request
  }
  return Array.from(unique.values()).slice(0, limit);
}

const ozonCategoryCache = new Map();
const ozonCategoryCacheTtlMs = 10 * 60 * 1000;

function flattenOzonCategoryTree(nodes = [], result = []) {
  for (const node of nodes || []) {
    const id = Number(node.description_category_id || node.category_id || node.id || 0);
    const descriptionTypeId = Number(node.description_type_id || node.type_id || node.descriptionTypeId || 0);
    const name = cleanText(node.category_name || node.name || node.title);
    if (id && name) result.push({ id, name, descriptionTypeId });
    const children = node.children || node.child || node.items || [];
    if (Array.isArray(children) && children.length) flattenOzonCategoryTree(children, result);
  }
  return result;
}

async function getOzonCategoryList(account, { force = false } = {}) {
  const cacheKey = account?.id || "ozon";
  const cached = ozonCategoryCache.get(cacheKey);
  if (!force && cached && Date.now() - cached.at < ozonCategoryCacheTtlMs) return cached.items;
  const payload = { language: "DEFAULT" };
  const data = await ozonRequest("/v1/description-category/tree", payload, account);
  const tree = data.result || data.items || data.categories || [];
  const items = flattenOzonCategoryTree(Array.isArray(tree) ? tree : [tree], []);
  ozonCategoryCache.set(cacheKey, { at: Date.now(), items });
  return items;
}

function buildOzonAttributesTemplate(rows = []) {
  return (rows || [])
    .filter((row) => Number(row?.is_required) === 1 || row?.required === true)
    .slice(0, 40)
    .map((row) => ({
      id: Number(row.id || row.attribute_id || 0),
      values: [],
    }))
    .filter((row) => row.id > 0);
}

function buildNoSupplierAlerts(products = [], { limit = 12 } = {}) {
  const rows = (products || [])
    .filter((product) => !product.selectedSupplier && Number(product.supplierCount || 0) > 0)
    .map((product) => ({
      id: product.id,
      offerId: product.offerId,
      name: product.name,
      marketplace: product.marketplace,
      nextPrice: 0,
      supplierCount: Number(product.supplierCount || 0),
      availableSupplierCount: Number(product.availableSupplierCount || 0),
      action: "Проверить наличие",
    }));
  if (Number.isFinite(Number(limit)) && Number(limit) > 0) return rows.slice(0, Number(limit));
  return rows;
}

function syncWarehouseSuppliersFromPriceMaster(warehouse, partners = []) {
  if (!warehouse || !Array.isArray(warehouse.suppliers)) return { changed: false, imported: 0 };
  const byId = new Map();
  const byName = new Map();
  for (const supplier of warehouse.suppliers) {
    if (supplier.partnerId) byId.set(String(supplier.partnerId), supplier);
    byName.set(normalizeSupplierName(supplier.name), supplier);
  }

  let imported = 0;
  for (const partner of partners) {
    const keyName = normalizeSupplierName(partner.name);
    const existing = (partner.partnerId && byId.get(String(partner.partnerId))) || byName.get(keyName);
    if (existing) {
      if (!existing.partnerId && partner.partnerId) existing.partnerId = String(partner.partnerId);
      if (!existing.source) existing.source = "pricemaster";
      continue;
    }
    const id = partner.partnerId ? `pm-${partner.partnerId}` : `pm-${crypto.randomUUID()}`;
    const supplier = normalizeManagedSupplier({
      id,
      partnerId: partner.partnerId,
      source: "pricemaster",
      name: partner.name,
      stopped: false,
    });
    warehouse.suppliers.push(supplier);
    byName.set(keyName, supplier);
    if (supplier.partnerId) byId.set(String(supplier.partnerId), supplier);
    imported += 1;
  }
  return { changed: imported > 0, imported };
}

function splitList(value) {
  if (Array.isArray(value)) return value.map(cleanText).filter(Boolean);
  return String(value || "")
    .split(/\r?\n|,/)
    .map(cleanText)
    .filter(Boolean);
}

function buildOzonManualProductItem(body = {}) {
  const extra = parseJsonField(body.extraJson, {});
  const item = {
    offer_id: cleanText(body.offerId || body.offer_id),
    name: cleanText(body.name),
    description: cleanText(body.description),
    category_id: Number(body.categoryId || body.category_id || 0),
    price: String(roundPrice(body.price)),
    old_price: body.oldPrice ? String(roundPrice(body.oldPrice)) : undefined,
    currency_code: cleanText(body.currencyCode || body.currency_code || "RUB"),
    vat: String(body.vat ?? "0"),
    barcode: cleanText(body.barcode),
    barcodes: splitList(body.barcodes),
    depth: Number(body.depth || 0),
    width: Number(body.width || 0),
    height: Number(body.height || 0),
    dimension_unit: cleanText(body.dimensionUnit || body.dimension_unit || "mm"),
    weight: Number(body.weight || 0),
    weight_unit: cleanText(body.weightUnit || body.weight_unit || "g"),
    primary_image: cleanText(body.primaryImage || body.primary_image),
    images: splitList(body.images),
    images360: splitList(body.images360),
    color_image: cleanText(body.colorImage || body.color_image),
    attributes: parseJsonField(body.attributesJson, []),
    complex_attributes: parseJsonField(body.complexAttributesJson, []),
    ...extra,
  };

  for (const key of Object.keys(item)) {
    if (item[key] === undefined || item[key] === "" || (Array.isArray(item[key]) && !item[key].length)) {
      delete item[key];
    }
  }

  const missing = [];
  if (!item.offer_id) missing.push("offer_id");
  if (!item.name) missing.push("name");
  if (!item.category_id) missing.push("category_id");
  if (!item.price || Number(item.price) <= 0) missing.push("price");
  if (!item.depth) missing.push("depth");
  if (!item.width) missing.push("width");
  if (!item.height) missing.push("height");
  if (!item.weight) missing.push("weight");

  return { item, missing, ready: missing.length === 0 };
}

function buildOzonWarehouseProductItem(product, overrides = {}) {
  const ozon = {
    ...(product.ozon || {}),
    ...(overrides.ozon || {}),
  };
  const body = {
    ...ozon,
    ...overrides,
    offerId: overrides.offerId || ozon.offerId || product.offerId,
    name: overrides.name || ozon.name || product.name,
    categoryId: overrides.categoryId || overrides.category_id || ozon.categoryId || ozon.category_id || ozon.descriptionCategoryId || ozon.description_category_id || ozon.marketCategoryId || ozon.market_category_id,
    attributesJson: overrides.attributesJson ?? ozon.attributes ?? [],
    complexAttributesJson: overrides.complexAttributesJson ?? ozon.complexAttributes ?? [],
    extraJson: overrides.extraJson ?? ozon.extra ?? {},
  };

  return buildOzonManualProductItem(body);
}

function buildOzonAiImagePrompt(product, promptOverride = "", options = {}) {
  const productName = cleanText(product?.name || product?.ozon?.name || product?.offerId || "товар");
  const template = cleanText(promptOverride) || ozonAiImageDefaultPrompt;
  let base = template.includes("{productName}")
    ? template.replaceAll("{productName}", productName)
    : `${template}\n\nНазвание товара: ${productName}`;
  const bottleOnlyInstruction = "Strict marketplace packshot rules: create exactly one standalone image. Do not create a collage, grid, contact sheet, split-screen, multi-panel layout, before/after layout, or multiple variants inside one image. Show one clean perfume bottle only. The full bottle must fit inside the square frame from cap to base, centered, upright, not cropped, with 10-15% empty margin on every side. Do not place the bottle on the far left or far right. Remove or ignore any box, outer packaging, cartons, bags, brochures, accessories, props, hands, faces, and all extra text. Do not create headlines, brand typography, marketing copy, icons, or labels outside the original bottle label. Do not hallucinate packaging, even if the source photo contains a box.";
  base = `${base}\n\n${bottleOnlyInstruction}`;
  const variantIndex = Number(options.variantIndex || 0);
  const variantTotal = Number(options.variantTotal || 0);
  if (!variantIndex || variantTotal <= 1) return base;
  const variantBriefs = [
    "Variant 1: clean white-background packshot, full centered bottle, natural shadow, no text outside the bottle label.",
    "Variant 2: premium soft-shadow packshot, full centered bottle, subtle reflection, safe margins, no text outside the bottle label.",
    "Variant 3: minimal light studio scene, full centered bottle, no props touching the product, no typography.",
    "Variant 4: clean ecommerce packshot with more air, full centered bottle, no logo or headline outside the original label.",
  ];
  return `${base}\n\n${variantBriefs[variantIndex - 1] || variantBriefs[0]}\nThis request generates only variant ${variantIndex} of ${variantTotal}; output exactly one final image, not all variants together. Change only lighting/background subtly. Never crop the bottle and never add text outside the original product label.`;
}

function publicAiImageStudioPresets() {
  return aiImageStudioPresets.map(({ id, label, prompt }) => ({ id, label, prompt }));
}

function normalizeAiImageStudioPresetList(input) {
  const raw = Array.isArray(input) ? input : [];
  const byId = new Map(aiImageStudioPresets.map((preset) => [preset.id, preset]));
  const selected = raw
    .map((item) => {
      if (typeof item === "string") return byId.get(cleanText(item)) || null;
      if (!item || typeof item !== "object") return null;
      const id = cleanText(item.id || item.presetId);
      const existing = byId.get(id);
      if (existing) return existing;
      const prompt = cleanText(item.prompt);
      if (!prompt) return null;
      return {
        id: id || `custom-${crypto.createHash("sha1").update(prompt).digest("hex").slice(0, 8)}`,
        label: cleanText(item.label) || "Custom",
        prompt,
      };
    })
    .filter(Boolean);
  return selected.length ? selected.slice(0, 5) : aiImageStudioPresets.slice(0, 5);
}

function isOpenAiRelayConfigured() {
  return Boolean(openaiRelayUrl && openaiRelaySecret);
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
  if (openaiRelayUrl && !openaiRelaySecret) {
    const error = new Error("Задан OPENAI_RELAY_URL, но не задан OPENAI_RELAY_SECRET.");
    error.statusCode = 400;
    error.code = "openai_relay_secret_missing";
    throw error;
  }
  if (!openaiRelayUrl && openaiRelaySecret) {
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
    const response = await fetch(openaiRelayUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${openaiRelaySecret}`,
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

async function generateOzonAiImageDraftFromPromptOnly(product, { prompt, sourceImageUrl = "", batchId, variantIndex = 1, variantTotal = 1, presetId = "", presetLabel = "" }, request) {
  const aiSettings = await readEffectiveAiSettings();
  assertImageGenerationConfigured(aiSettings);
  const sourceHint = cleanText(sourceImageUrl)
    ? `\n\nReference product image URL for context: ${cleanText(sourceImageUrl)}. Generate a new marketplace-ready image; do not copy watermarks or UI elements from the source.`
    : "";
  const generatedPrompt = buildOzonAiImagePrompt(product, `${cleanText(prompt)}${sourceHint}`, { variantIndex, variantTotal });
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
  const { prompt, sourceImageUrl, batchId, variantIndex = 1, variantTotal = 1, requireSourceImage = false, allowGenerationFallback = true, forceCodexSale = false } = options;
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
    return generateOzonAiImageDraftFromPromptOnly(product, { prompt, batchId, variantIndex, variantTotal, presetId: options.presetId, presetLabel: options.presetLabel }, request);
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

  const generatedPrompt = buildOzonAiImagePrompt(product, prompt, { variantIndex, variantTotal });
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
    presetId: cleanText(options.presetId),
    presetLabel: cleanText(options.presetLabel),
    model: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
    size: ozonAiImageStoredSizeLabel(aiSettings),
    quality: aiSettings.imageQuality || openaiImageQuality,
    format: aiSettings.imageFormat || openaiImageFormat,
  });
}

function buildYandexOfferMapping(product, overrides = {}) {
  const ozon = product.ozon || {};
  const yandex = {
    ...(product.yandex || {}),
    ...(overrides.yandex || {}),
  };
  const pictures = Array.from(
    new Set([
      ...splitList(yandex.pictures),
      ...splitList(yandex.images),
      ...splitList(ozon.primaryImage),
      ...splitList(ozon.images),
    ]),
  );
  const barcodes = Array.from(new Set([...splitList(yandex.barcodes), ...splitList(ozon.barcode), ...splitList(ozon.barcodes)]));
  const price = Number(
    overrides.price ||
      yandex.price ||
      product.targetPrice ||
      product.nextPrice ||
      product.marketplacePrice ||
      ozon.price ||
      0,
  );
  const extra = parseJsonField(yandex.extra, {});
  const offer = compactObject({
    offerId: cleanText(overrides.offerId || yandex.offerId || product.offerId),
    name: cleanText(overrides.name || yandex.name || ozon.name || product.name),
    marketCategoryId: Number(yandex.marketCategoryId || ozon.marketCategoryId || ozon.categoryId || 0) || undefined,
    pictures,
    vendor: cleanText(yandex.vendor || ozon.vendor || "Без бренда"),
    description: cleanText(yandex.description || ozon.description || product.name),
    barcodes,
    basicPrice: price > 0 ? { value: roundPrice(price), currencyId: "RUR" } : undefined,
    ...extra,
  });

  const missing = [];
  if (!offer.offerId) missing.push("offerId");
  if (!offer.name) missing.push("name");
  if (!offer.marketCategoryId) missing.push("marketCategoryId");
  if (!offer.pictures?.length) missing.push("pictures");
  if (!offer.vendor) missing.push("vendor");
  if (!offer.description) missing.push("description");

  return { offer, missing, ready: missing.length === 0 };
}

async function sendApprovedYandexProductContent(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "yandex") {
    return { ok: true, skipped: true, reason: "not_yandex" };
  }
  const shop = getYandexShopByTarget(normalized.target);
  if (!shop) return { ok: false, error: "yandex_shop_not_found" };
  if (!shop.apiKey || !shop.businessId) return { ok: false, error: "yandex_shop_not_configured", target: shop.id || normalized.target };

  const built = buildYandexOfferMapping(normalized);
  const mode = cleanText(options.mode || "content").toLowerCase();
  const pictureOverride = Array.isArray(options.pictures)
    ? options.pictures.map((url) => cleanText(url)).filter(Boolean)
    : [];
  const partialOffer = compactObject({
    offerId: built.offer?.offerId || normalized.offerId,
    name: mode === "image" ? undefined : built.offer?.name,
    vendor: mode === "image" ? undefined : built.offer?.vendor,
    description: mode === "image" ? undefined : built.offer?.description,
    pictures: mode === "content" ? undefined : (pictureOverride.length ? pictureOverride : built.offer?.pictures),
  });
  const missing = [];
  if (!partialOffer.offerId) missing.push("offerId");
  if (mode !== "image" && !partialOffer.description) missing.push("description");
  if (mode !== "content" && !partialOffer.pictures?.length) missing.push("pictures");
  if (missing.length) {
    return {
      ok: false,
      error: `yandex_update_not_ready: ${missing.join(", ")}`,
      mode,
      missing,
      target: shop.id,
      offerId: built.offer?.offerId || normalized.offerId,
    };
  }

  const [result] = await sendYandexOfferMappings(shop, [partialOffer]);
  if (!result?.ok) {
    const failed = {
      ok: false,
      error: result?.error || "yandex_content_send_failed",
      mode,
      target: shop.id,
      businessId: shop.businessId,
      offerId: partialOffer.offerId,
      result,
    };
    logger.warn("approved yandex product content send failed", failed);
    return failed;
  }
  const sent = {
    ok: true,
    mode,
    target: shop.id,
    businessId: shop.businessId,
    offerId: partialOffer.offerId,
    fields: Object.keys(partialOffer).filter((key) => key !== "offerId"),
    result,
  };
  logger.info("approved yandex product content sent", sent);
  return sent;
}

async function sendApprovedOzonProductContent(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "ozon") {
    return { ok: true, skipped: true, reason: "not_ozon" };
  }
  const account = getOzonAccountByTarget(normalized.target || "ozon");
  if (!account) return { ok: false, error: "ozon_account_not_found" };
  if (!account.clientId || !account.apiKey) return { ok: false, error: "ozon_account_not_configured", target: account.id || normalized.target };

  const mode = cleanText(options.mode || "content").toLowerCase() === "image" ? "image" : "content";
  const built = buildOzonWarehouseProductItem(normalized, options.overrides || {});
  if (!built.ready) {
    return {
      ok: false,
      error: "ozon_update_not_ready",
      code: "ozon_update_not_ready",
      mode,
      target: account.id,
      offerId: normalized.ozon?.offerId || normalized.offerId,
      missing: built.missing || [],
    };
  }

  const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
  const sent = {
    ok: true,
    mode,
    target: account.id,
    offerId: built.item.offer_id || normalized.offerId,
    fields: mode === "image" ? ["primary_image", "images"] : ["name", "description"],
    result,
  };
  logger.info("approved ozon product content sent", sent);
  return sent;
}

function marketplaceSendResultError(result = {}, fallbackCode = "marketplace_send_failed") {
  if (result?.ok) return null;
  const error = new Error(result?.error || fallbackCode);
  error.statusCode = 400;
  error.code = result?.code || result?.error || fallbackCode;
  if (Array.isArray(result?.missing)) error.missing = result.missing;
  error.marketplace = result?.marketplace;
  error.result = result;
  return error;
}

function latestAiContentDraft(product = {}, status = "pending") {
  const normalizedStatus = cleanText(status).toLowerCase();
  return [...(normalizeWarehouseProduct(product).aiContentDrafts || [])]
    .filter((draft) => !normalizedStatus || draft.status === normalizedStatus)
    .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))[0] || null;
}

function latestAiImageBatch(product = {}, status = "pending") {
  const normalizedStatus = cleanText(status).toLowerCase();
  const drafts = [...(normalizeWarehouseProduct(product).aiImages || [])]
    .filter((draft) => (!normalizedStatus || draft.status === normalizedStatus) && draft.resultUrl)
    .sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  if (!drafts.length) return { batchId: "", drafts: [] };
  const latest = drafts[drafts.length - 1];
  const batchId = latest.batchId || latest.id;
  return {
    batchId,
    drafts: drafts.filter((draft) => (latest.batchId ? draft.batchId === latest.batchId : draft.id === latest.id)),
  };
}

function buildAiQualityReviewRow(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const imageBatch = latestAiImageBatch(normalized, "pending");
  return {
    product: {
      id: normalized.id,
      offerId: normalized.offerId,
      name: normalized.name,
      marketplace: normalized.marketplace,
      target: normalized.target,
      imageUrl: normalized.imageUrl,
      updatedAt: normalized.updatedAt,
      cardQuality: normalized.yandex?.extra?.cardQuality || null,
    },
    contentDraft: latestAiContentDraft(normalized, "pending"),
    imageBatchId: imageBatch.batchId,
    imageDrafts: imageBatch.drafts,
  };
}

function compactAiText(value = "", maxLength = 6000) {
  return cleanText(value).replace(/\s+/g, " ").slice(0, maxLength);
}

function productContentQuality(product = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const yandex = normalized.yandex || {};
  const name = cleanText(marketplace === "yandex" ? (yandex.name || ozon.name || normalized.name) : (ozon.name || normalized.name));
  const description = cleanText(marketplace === "yandex" ? (yandex.description || ozon.description || normalized.description) : (ozon.description || normalized.description));
  const vendor = cleanText(marketplace === "yandex" ? (yandex.vendor || ozon.vendor || normalized.brand) : (ozon.vendor || normalized.brand));
  const reasons = [];
  if (!name) reasons.push("no_name");
  if (!vendor || /без бренда/i.test(vendor)) reasons.push("weak_vendor");
  if (!description) reasons.push("no_description");
  if (description && description.length < 140) reasons.push("short_description");
  if (description && name && description.toLowerCase() === name.toLowerCase()) reasons.push("description_equals_name");
  if (/^(описание|товар|парфюмерная вода|духи|туалетная вода)$/i.test(description)) reasons.push("generic_description");
  const built = marketplace === "yandex" ? buildYandexOfferMapping(normalized) : { missing: [], ready: true };
  return {
    marketplace,
    ready: Boolean(built.ready && !reasons.includes("no_description") && !reasons.includes("description_equals_name")),
    missing: built.missing || [],
    reasons,
    nameLength: name.length,
    descriptionLength: description.length,
  };
}

function applyAiContentDraftToProduct(product = {}, draft = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const next = { ...normalized };
  const cleanDraft = {
    name: compactAiText(draft.name, 240),
    description: compactAiText(draft.description, 5000),
    vendor: compactAiText(draft.vendor, 120),
    bulletPoints: Array.isArray(draft.bulletPoints || draft.bullets)
      ? (draft.bulletPoints || draft.bullets).map((item) => compactAiText(item, 180)).filter(Boolean).slice(0, 8)
      : [],
    seoKeywords: Array.isArray(draft.seoKeywords || draft.keywords)
      ? (draft.seoKeywords || draft.keywords).map((item) => compactAiText(item, 80)).filter(Boolean).slice(0, 12)
      : [],
  };
  if (marketplace === "yandex") {
    const current = next.yandex || {};
    next.yandex = normalizeYandexDraft({
      ...current,
      name: cleanDraft.name || current.name || next.ozon?.name || next.name,
      description: cleanDraft.description || current.description || next.ozon?.description || next.name,
      vendor: cleanDraft.vendor || current.vendor || next.ozon?.vendor || next.brand || "Без бренда",
      extra: {
        ...parseJsonField(current.extra, {}),
        aiBulletPoints: cleanDraft.bulletPoints,
        aiSeoKeywords: cleanDraft.seoKeywords,
        aiContentUpdatedAt: new Date().toISOString(),
      },
    });
    if (next.marketplace === "yandex") {
      next.name = next.yandex.name || next.name;
      next.description = next.yandex.description || next.description;
      next.brand = next.yandex.vendor || next.brand;
    }
  }
  if (marketplace === "ozon") {
    const current = next.ozon || {};
    next.ozon = normalizeOzonDraft({
      ...current,
      name: cleanDraft.name || current.name || next.yandex?.name || next.name,
      description: cleanDraft.description || current.description || next.yandex?.description || next.description || next.name,
      vendor: cleanDraft.vendor || current.vendor || next.yandex?.vendor || next.brand,
      extra: {
        ...parseJsonField(current.extra, {}),
        aiBulletPoints: cleanDraft.bulletPoints,
        aiSeoKeywords: cleanDraft.seoKeywords,
        aiContentUpdatedAt: new Date().toISOString(),
      },
    });
    if (next.marketplace === "ozon") {
      next.name = next.ozon.name || next.name;
      next.description = next.ozon.description || next.description;
      next.brand = next.ozon.vendor || next.brand;
    }
  }
  return normalizeWarehouseProduct(next);
}

function buildAiContentMessages(product = {}, marketplace = "yandex") {
  const normalized = normalizeWarehouseProduct(product);
  const source = {
    marketplace,
    offerId: normalized.offerId,
    name: normalized.yandex?.name || normalized.ozon?.name || normalized.name,
    description: normalized.yandex?.description || normalized.ozon?.description || normalized.description,
    vendor: normalized.yandex?.vendor || normalized.ozon?.vendor || normalized.brand,
    categoryId: normalized.yandex?.marketCategoryId || normalized.ozon?.marketCategoryId || normalized.ozon?.categoryId,
    price: normalized.nextPrice || normalized.currentPrice,
    volumeMl: extractOzonYandexImportVolumesMl(normalized.name || normalized.ozon?.name || ""),
    attributes: normalized.ozon?.attributes || normalized.yandex?.attributes || [],
  };
  return [
    {
      role: "system",
      content: [
        "Ты редактор карточек маркетплейса для парфюмерии и косметики.",
        "Улучши карточку так, чтобы текст был пригоден для Yandex Market и не нарушал правила.",
        "Описание должно быть подробным: 900-1400 знаков, 2-3 связных абзаца без markdown, списков и эмодзи.",
        "Раскрой характер аромата, звучание верхних/средних/базовых нот, настроение, сезонность, уместные сценарии использования и ощущение от шлейфа, но только если эти данные есть в исходных данных.",
        "Если данных о нотах мало, расширяй описание за счет нейтральных формулировок о стиле, формате, назначении и впечатлении от композиции, не выдумывая факты.",
        "bulletPoints верни отдельным массивом из 5-8 коротких преимуществ для карточки.",
        "seoKeywords верни отдельным массивом из 8-12 поисковых фраз без повторов.",
        "Не выдумывай бренд, объем, концентрацию, страну, пол и ноты, если их нет в исходных данных.",
        "Не добавляй медицинские обещания, слова 'оригинал', '100% гарантия', запрещенные сравнения и агрессивные обещания.",
        "Верни только JSON: name, description, vendor, bulletPoints, seoKeywords.",
      ].join(" "),
    },
    {
      role: "user",
      content: JSON.stringify(source),
    },
  ];
}

async function generateAiProductContentDraft(product = {}, options = {}) {
  const marketplace = cleanText(options.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
  const response = await createOpenAiJsonChat(buildAiContentMessages(product, marketplace));
  const content = response?.choices?.[0]?.message?.content || "";
  const parsed = extractJsonObjectFromText(content);
  const draft = {
    name: compactAiText(parsed.name, 240),
    description: compactAiText(parsed.description, 5000),
    vendor: compactAiText(parsed.vendor, 120),
    bulletPoints: Array.isArray(parsed.bulletPoints || parsed.bullets) ? (parsed.bulletPoints || parsed.bullets) : [],
    seoKeywords: Array.isArray(parsed.seoKeywords || parsed.keywords) ? (parsed.seoKeywords || parsed.keywords) : [],
    model: cleanText(response?.model) || openaiTextModel,
    generatedAt: new Date().toISOString(),
  };
  if (!draft.name && !draft.description) {
    const error = new Error("AI не вернул название или описание. Попробуйте повторить.");
    error.statusCode = 502;
    error.code = "openai_text_empty";
    throw error;
  }
  return draft;
}

function extractOzonYandexImportVolumesMl(name = "") {
  const text = cleanText(name).replace(",", ".");
  const volumes = [];
  const pattern = /(\d+(?:\.\d+)?)\s*(?:мл|ml)(?![a-zа-я])/giu;
  let match;
  while ((match = pattern.exec(text))) {
    const value = Number(match[1]);
    if (Number.isFinite(value)) volumes.push(value);
  }
  return volumes;
}

function ozonYandexImportBlockReasons(product = {}) {
  const name = cleanText(product.name || product.ozon?.name || product.offerId);
  const lower = name.toLowerCase();
  const reasons = [];
  const vendor = cleanText(product.ozon?.vendor || product.yandex?.vendor || product.brand || "");
  const vendorLower = vendor.toLowerCase();
  const marketplaceState = product.marketplaceState || {};
  const stateCode = cleanText(marketplaceState.code).toLowerCase();
  const stateVisibility = cleanText(marketplaceState.visibility).toUpperCase();
  const rawState = cleanText(marketplaceState.state).toUpperCase();
  if (["inactive", "unknown"].includes(stateCode) || ["REMOVED_FROM_SALE", "DISABLED", "BANNED"].includes(stateVisibility) || ["REMOVED_FROM_SALE", "DISABLED", "BANNED"].includes(rawState)) {
    reasons.push("Товар неактивен или статус Ozon не подтвержден");
  }
  if (lower.includes("отливант")) reasons.push("Название содержит «Отливант»");
  if (/без\s+коробк/iu.test(lower)) reasons.push("Название содержит «без коробки»");
  const smallVolumes = extractOzonYandexImportVolumesMl(name).filter((value) => value < 20);
  if (smallVolumes.length) reasons.push(`Объем меньше 20 мл: ${smallVolumes.join(", ")} мл`);
  const volumes = extractOzonYandexImportVolumesMl(name);
  if (!volumes.length) reasons.push("В названии нет объема в мл");
  const hugeVolumes = extractOzonYandexImportVolumesMl(name).filter((value) => value > 500);
  if (hugeVolumes.length || /\d+\s+\d{3}\s*(?:мл|ml)(?![a-zа-я])/iu.test(name)) {
    reasons.push(`Подозрительный объем${hugeVolumes.length ? `: ${hugeVolumes.join(", ")} мл` : ""}`);
  }
  const blockedCategories = [
    "свеч",
    "помад",
    "шампун",
    "бальзам",
    "крем",
    "маск",
    "гель",
    "лак",
    "краск",
    "стик",
    "stick",
    "дезодорант",
  ];
  const matchedCategory = blockedCategories.find((word) => lower.includes(word));
  if (matchedCategory) reasons.push("Категория не подходит для импорта парфюмерии");
  const genericNames = [
    "парфюмерная вода",
    "парфюмерная вода для мужчин",
    "туалетная вода",
    "духи",
  ];
  if ((!vendor || vendorLower.includes("без бренда")) && genericNames.some((genericName) => lower === genericName || lower.startsWith(`${genericName} `))) {
    reasons.push("Слишком общее название без бренда");
  }
  const meaningfulWords = name.match(/[a-zа-яё]{3,}/giu) || [];
  const wordsWithVowels = meaningfulWords.filter((word) => /[aeiouyаеёиоуыэюя]/iu.test(word));
  const alphaCount = (name.match(/[a-zа-яё]/giu) || []).length;
  if (name.length < 10 || alphaCount < 3 || (meaningfulWords.length > 0 && wordsWithVowels.length === 0)) {
    reasons.push("Подозрительное название товара");
  }
  return reasons;
}

function buildOzonYandexImportCandidate(product = {}, options = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const built = buildYandexOfferMapping(normalized);
  const blockReasons = ozonYandexImportBlockReasons(normalized);
  const missing = Array.isArray(built.missing) ? built.missing : [];
  const yandexExistingOfferIds = options.yandexExistingOfferIds instanceof Set ? options.yandexExistingOfferIds : new Set();
  const offerIdKey = cleanText(normalized.offerId).toLowerCase();
  const existingInYandex = Boolean(offerIdKey && yandexExistingOfferIds.has(offerIdKey));
  const imageUrl = normalized.imageUrl || firstImageUrl(ozon.primaryImage || ozon.images || normalized.images);
  return {
    id: normalized.id,
    marketplace: normalized.marketplace,
    target: normalized.target,
    offerId: normalized.offerId,
    productId: normalized.productId,
    sku: normalized.sku || ozon.sku || "",
    name: normalized.name || ozon.name || normalized.offerId || "",
    vendor: built.offer?.vendor || ozon.vendor || "",
    imageUrl,
    price: Number(built.offer?.basicPrice?.value || normalized.marketplacePrice || ozon.price || 0) || null,
    categoryId: built.offer?.marketCategoryId || null,
    picturesCount: Array.isArray(built.offer?.pictures) ? built.offer.pictures.length : 0,
    hasDescription: Boolean(built.offer?.description),
    blockReasons,
    missing,
    existingInYandex,
    yandexReady: Boolean(built.ready),
    eligible: !existingInYandex && !blockReasons.length && Boolean(built.ready),
    offerPreview: built.offer,
  };
}

function summarizeOzonYandexImportPreview(rows = []) {
  return {
    total: rows.length,
    eligible: rows.filter((row) => row.eligible).length,
    blocked: rows.filter((row) => row.blockReasons?.length).length,
    existingInYandex: rows.filter((row) => row.existingInYandex).length,
    missingRequired: rows.filter((row) => !row.yandexReady).length,
  };
}

function parseProtectedBrandList(input) {
  const values = Array.isArray(input)
    ? input
    : String(input || "").split(/[\n,;]+/u);
  return Array.from(new Set(values
    .map((item) => cleanText(item))
    .filter((item) => item.length >= 2)));
}

function normalizeBrandSearchText(value) {
  return cleanText(value)
    .toLowerCase()
    .replaceAll("ё", "е")
    .replace(/[^0-9a-zа-я]+/giu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function collectYandexOfferTextParts(value, parts = [], depth = 0) {
  if (value == null || depth > 6) return parts;
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    const text = cleanText(value);
    if (text) parts.push(text);
    return parts;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectYandexOfferTextParts(item, parts, depth + 1);
    return parts;
  }
  if (typeof value === "object") {
    for (const item of Object.values(value)) collectYandexOfferTextParts(item, parts, depth + 1);
  }
  return parts;
}

function buildYandexCleanupCandidate(item = {}, shop = {}, protectedBrandsInput = []) {
  const offer = pickYandexOfferFromMapping(item);
  const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
  const name = cleanText(offer.name || item.offer?.name || offerId);
  const vendor = cleanText(offer.vendor || item.offer?.vendor || item.mapping?.vendor);
  const state = pickYandexState(item, offer);
  const archived = state.code === "archived" || Boolean(offer.archived || item.archived || item.offer?.archived);
  const brands = parseProtectedBrandList(protectedBrandsInput);
  const searchableTextRaw = collectYandexOfferTextParts(item).join(" ");
  const searchText = normalizeBrandSearchText(searchableTextRaw);
  const matchedBrands = brands.filter((brand) => {
    const normalizedBrand = normalizeBrandSearchText(brand);
    return normalizedBrand && searchText.includes(normalizedBrand);
  });
  const volumesMl = extractOzonYandexImportVolumesMl(searchableTextRaw);
  const minVolumeMl = volumesMl.length ? Math.min(...volumesMl) : null;
  const smallVolume = minVolumeMl !== null && minVolumeMl < 20;
  const protectedByBrand = matchedBrands.length > 0 && !smallVolume;
  return {
    id: `${shop.businessId || shop.id || "yandex"}:${offerId}`,
    shopId: shop.id || "yandex",
    shopName: shop.name || "Yandex Market",
    offerId,
    name,
    vendor,
    archived,
    state: state.code,
    stateLabel: state.label,
    stateName: state.stateName || "",
    matchedBrands,
    minVolumeMl,
    smallVolume,
    protected: protectedByBrand,
    action: protectedByBrand ? "keep" : "delete",
  };
}

function summarizeYandexCleanupPreview(rows = []) {
  const toDelete = rows.filter((row) => row.action === "delete").length;
  const alreadyArchived = rows.filter((row) => row.archived).length;
  return {
    total: rows.length,
    protected: rows.filter((row) => row.protected).length,
    toDelete,
    toArchive: toDelete,
    alreadyArchived,
  };
}

async function buildYandexCleanupPreview({ protectedBrands = [], limit = 50000 } = {}) {
  const seenBusinesses = new Set();
  const shops = getYandexShops().filter((shop) => {
    if (!shop.apiKey || !shop.businessId) return false;
    const businessKey = String(shop.businessId);
    if (seenBusinesses.has(businessKey)) return false;
    seenBusinesses.add(businessKey);
    return true;
  });
  const maxItems = Math.max(1, Math.min(50000, Number(limit || 50000) || 50000));
  const warnings = [];
  const rows = [];
  if (!shops.length) {
    return { ok: true, warnings: ["Yandex Market не настроен."], rows, summary: summarizeYandexCleanupPreview(rows) };
  }

  for (const shop of shops) {
    const remaining = Math.max(0, maxItems - rows.length);
    if (!remaining) break;
    try {
      const active = await getYandexOfferMappings(shop, remaining, { archived: false });
      const archivedRemaining = Math.max(0, maxItems - rows.length - active.length);
      const archived = archivedRemaining > 0
        ? await getYandexOfferMappings(shop, archivedRemaining, { archived: true })
        : [];
      const byOfferId = new Map();
      for (const item of [...active, ...archived]) {
        const offer = pickYandexOfferFromMapping(item);
        const offerId = cleanText(offer.offerId || item.offerId || item.mapping?.offerId);
        if (offerId) byOfferId.set(`${shop.businessId}:${offerId}`, item);
      }
      for (const item of byOfferId.values()) {
        rows.push(buildYandexCleanupCandidate(item, shop, protectedBrands));
        if (rows.length >= maxItems) break;
      }
    } catch (error) {
      const label = error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex «${shop.name || shop.id}»: не удалось загрузить товары (${label})`);
      logger.warn("yandex cleanup preview failed", { shop: shop.id, detail: label });
    }
  }

  return {
    ok: true,
    generatedAt: new Date().toISOString(),
    protectedBrands: parseProtectedBrandList(protectedBrands),
    warnings,
    summary: summarizeYandexCleanupPreview(rows),
    rows,
  };
}

async function deleteYandexOfferIds(shop, offerIds = []) {
  const results = [];
  for (const chunk of chunkArray(offerIds.map(cleanText).filter(Boolean), 500)) {
    if (!chunk.length) continue;
    try {
      const payload = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-mappings/delete`, { offerIds: chunk });
      const notDeleted = new Set(
        [
          ...(Array.isArray(payload?.notDeletedOfferIds) ? payload.notDeletedOfferIds : []),
          ...(Array.isArray(payload?.result?.notDeletedOfferIds) ? payload.result.notDeletedOfferIds : []),
        ].map(cleanText).filter(Boolean),
      );
      results.push(...chunk.map((offerId) => (
        notDeleted.has(offerId)
          ? { offerId, ok: false, method: "delete", error: "not_deleted_by_yandex" }
          : { offerId, ok: true, method: "delete" }
      )));
    } catch (error) {
      const detail = error?.message || "delete_failed";
      results.push(...chunk.map((offerId) => ({ offerId, ok: false, method: "delete", error: detail })));
    }
  }
  return results;
}

async function deleteYandexCleanupRows(rows = []) {
  const byShop = new Map();
  for (const row of rows) {
    if (row.action !== "delete" || !row.offerId || !row.shopId) continue;
    if (!byShop.has(row.shopId)) byShop.set(row.shopId, []);
    byShop.get(row.shopId).push(row.offerId);
  }
  const results = [];
  for (const [shopId, offerIds] of byShop.entries()) {
    const shop = getYandexShopByTarget(shopId);
    if (!shop) {
      results.push(...offerIds.map((offerId) => ({ offerId, shopId, ok: false, error: "shop_not_found" })));
      continue;
    }
    const deleted = await deleteYandexOfferIds(shop, offerIds);
    results.push(...deleted.map((item) => ({ ...item, shopId })));
  }
  return results;
}

function pickOzonProductStockForYandex(product = {}) {
  const state = product.marketplaceState || {};
  const stateCode = cleanText(state.code || state.state).toLowerCase();
  const visibility = cleanText(state.visibility || product.visibility).toUpperCase();
  if (stateCode === "archived" || visibility === "ARCHIVED" || state.archived || product.archived) return 0;
  const direct = Number(state.stock ?? state.present ?? product.targetStock ?? 0);
  if (Number.isFinite(direct) && direct > 0) return Math.max(0, Math.round(direct));
  const warehouses = Array.isArray(state.warehouses) ? state.warehouses : [];
  const sum = warehouses.reduce((total, warehouse) => {
    const value = Number(warehouse.stock ?? warehouse.present ?? 0);
    return total + (Number.isFinite(value) ? Math.max(0, value) : 0);
  }, 0);
  return Math.max(0, Math.round(sum));
}

function buildYandexStockUpdatePayload(rows = [], updatedAt = new Date().toISOString()) {
  const skus = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      sku: cleanText(row.sku || row.offerId || row.offer_id),
      count: Math.max(0, Math.round(Number(row.count ?? row.stock ?? 0) || 0)),
    }))
    .filter((row) => row.sku)
    .map((row) => ({
      sku: row.sku,
      items: [{ type: "FIT", count: row.count, updatedAt }],
    }));
  return { skus };
}

function buildYandexStockRestoreProducts(rows = [], shops = null) {
  const positiveRows = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      offerId: cleanText(row.offerId || row.offer_id),
      stock: Math.max(0, Math.round(Number(row.stock ?? row.targetStock ?? 0) || 0)),
    }))
    .filter((row) => row.offerId && row.stock > 0);
  const restoreShops = uniqueYandexShopsByBusiness(shops);
  return restoreShops.flatMap((shop) => positiveRows.map((row) => ({
    id: yandexWarehouseProductId(shop, row.offerId),
    marketplace: "yandex",
    target: shop.id || "yandex",
    offerId: row.offerId,
  })));
}

async function restoreYandexArchivedStocks(rows = [], shops = null) {
  const restoreProducts = buildYandexStockRestoreProducts(rows, shops);
  if (!restoreProducts.length) return [];
  return unarchiveProductsOnMarketplaces(restoreProducts);
}

function yandexStockShops(shops = null) {
  const source = Array.isArray(shops) && shops.length ? shops : getYandexShops();
  const expanded = source.flatMap((shop) => parseYandexCampaignIds(shop.campaignId || shop.campaign_id).map((campaignId, index) => ({
    ...shop,
    id: index === 0 ? shop.id : `${shop.id}-${campaignId}`,
    name: index === 0 ? shop.name : `${shop.name || shop.id} #${campaignId}`,
    campaignId,
  }))).filter((shop) => (
    shop.apiKey
    && shop.businessId
    && shop.campaignId
  ));
  const allowed = expanded.filter((shop) => yandexStockCampaignIds.has(String(shop.campaignId)));
  const skipped = expanded.filter((shop) => !yandexStockCampaignIds.has(String(shop.campaignId)));
  if (skipped.length) {
    logger.warn("yandex stock campaign skipped", {
      allowedCampaignIds: Array.from(yandexStockCampaignIds),
      skippedCampaignIds: [...new Set(skipped.map((shop) => String(shop.campaignId)))],
    });
  }
  return allowed;
}

function parseYandexCampaignIds(value) {
  return [...new Set(cleanText(value)
    .split(/[\s,;]+/)
    .map(cleanText)
    .filter(Boolean))];
}

function yandexMissingStockCampaignWarning(count) {
  return `Yandex: ${count} магазин(ов) без campaignId пропущены для остатков. Добавьте Campaign ID в кабинетах маркетплейсов.`;
}

async function sendYandexStockChunk(shop, rows = []) {
  if (!shop?.campaignId) {
    const error = new Error("Yandex campaignId is required for stock updates");
    error.statusCode = 400;
    throw error;
  }
  const payload = buildYandexStockUpdatePayload(rows);
  if (!payload.skus.length) return null;
  const result = await yandexRequest(shop, "PUT", `/v2/campaigns/${shop.campaignId}/offers/stocks`, payload);
  if (apiPayloadHasErrors(result)) {
    const error = new Error(summarizeApiErrorPayload(result, "Yandex stock update failed"));
    error.statusCode = 400;
    error.yandex = result;
    throw error;
  }
  return result;
}

async function sendYandexStockRowsWithFallback(shop, rows = []) {
  const chunk = (Array.isArray(rows) ? rows : []).filter((row) => cleanText(row.offerId || row.sku));
  if (!chunk.length) return { sent: 0, failed: 0, results: [] };
  const target = shop.id;
  const targetName = shop.name || "Yandex Market";
  const targetCampaignId = cleanText(shop.campaignId || shop.campaign_id);
  try {
    await sendYandexStockChunk(shop, chunk);
    return {
      sent: chunk.length,
      failed: 0,
      results: [{ target, targetName, targetCampaignId, sent: chunk.length, ok: true }],
    };
  } catch (error) {
    const chunkError = error?.message || error?.code || "Yandex stock update failed";
    if (chunk.length === 1) {
      return {
        sent: 0,
        failed: 1,
        fallbackError: chunkError,
        results: chunk.map((item) => ({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          ok: false,
          error: chunkError,
        })),
      };
    }
    const results = [];
    let sent = 0;
    let failed = 0;
    for (const item of chunk) {
      try {
        await sendYandexStockChunk(shop, [item]);
        sent += 1;
        results.push({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          sent: 1,
          ok: true,
        });
      } catch (itemError) {
        failed += 1;
        results.push({
          stage: "stock",
          offerId: item.offerId,
          target,
          targetName,
          targetCampaignId,
          stock: item.stock,
          ok: false,
          error: itemError?.message || itemError?.code || chunkError,
        });
      }
    }
    return { sent, failed, fallbackError: chunkError, results };
  }
}

async function sendYandexOfferArchiveState(shop, offerIds = [], archived = false) {
  const ids = [...new Set((Array.isArray(offerIds) ? offerIds : [])
    .map(cleanText)
    .filter(Boolean))];
  if (!ids.length) return [];
  const endpoint = `/v2/businesses/${shop.businessId}/offer-mappings/${archived ? "archive" : "unarchive"}`;
  const type = archived ? "archive" : "unarchive";
  const results = [];
  for (const chunk of chunkArray(ids, 200)) {
    try {
      const result = await yandexRequest(shop, "POST", endpoint, { offerIds: chunk });
      if (apiPayloadHasErrors(result)) {
        const error = new Error(summarizeApiErrorPayload(result, `Yandex ${type} failed`));
        error.statusCode = 400;
        throw error;
      }
      results.push(...chunk.map((offerId) => ({ offerId, ok: true })));
    } catch (error) {
      const detail = error?.message || `${type}_failed`;
      results.push(...chunk.map((offerId) => ({ offerId, ok: false, error: detail })));
    }
  }
  return results;
}

function isExpectedMarketplaceArchiveBlock(detail = "") {
  const value = cleanText(detail).toLowerCase();
  return Boolean(
    value.includes("item has fbo stock")
      || value.includes("fbo stock")
      || value.includes("has stock")
      || value.includes("нельзя архив")
      || value.includes("остат"),
  );
}

async function sendYandexStocksFromOzonProducts(products = [], options = {}) {
  const dryRun = options.dryRun === true;
  const warnings = [];
  const allShops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const shops = yandexStockShops(allShops);
  const missingCampaignShops = allShops.filter((shop) => !shop.campaignId);
  if (missingCampaignShops.length) {
    warnings.push(yandexMissingStockCampaignWarning(missingCampaignShops.length));
  }
  const rows = (Array.isArray(products) ? products : [])
    .map((product) => ({
      id: product.id,
      offerId: cleanText(product.offerId || product.offer_id),
      productId: cleanText(product.productId || product.product_id),
      stock: pickOzonProductStockForYandex(product),
    }))
    .filter((row) => row.offerId);
  if (!rows.length) {
    return { ok: true, dryRun, sent: 0, failed: 0, skipped: 0, warnings, results: [] };
  }
  if (!shops.length) {
    const detail = warnings.length ? warnings.join(" ") : "Yandex Market не настроен для остатков.";
    return {
      ok: false,
      dryRun,
      sent: 0,
      failed: rows.length,
      skipped: 0,
      planned: rows.length,
      warnings: warnings.length ? warnings : [detail],
      results: rows.map((row) => ({
        stage: "stock",
        offerId: row.offerId,
        stock: row.stock,
        ok: false,
        error: detail,
      })),
    };
  }

  let existingOfferIds = options.existingOfferIds instanceof Set
    ? new Set(Array.from(options.existingOfferIds).map((id) => cleanText(id).toLowerCase()).filter(Boolean))
    : null;
  if (!existingOfferIds) {
    try {
      existingOfferIds = await getExistingYandexOfferIdSet(rows.map((row) => row.offerId));
    } catch (error) {
      const label = error?.message || error?.code || "ошибка API";
      warnings.push(`Yandex: не удалось проверить существующие артикулы (${label})`);
      existingOfferIds = getLocalYandexExportedOfferIdSet(options.warehouseProducts || products);
      if (existingOfferIds.size) warnings.push(`Yandex: использую локальный каталог для остатков (${existingOfferIds.size} SKU).`);
    }
  }

  const selected = rows.filter((row) => existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  if (dryRun) {
    return { ok: true, dryRun, sent: 0, skipped: rows.length - selected.length, planned: selected.length, warnings, results: selected };
  }
  const restoreActions = await restoreYandexArchivedStocks(selected, allShops);
  const restoreFailed = restoreActions.filter((item) => !item.ok);
  if (restoreFailed.length) {
    warnings.push(`Yandex: не удалось разархивировать ${restoreFailed.length} карточек перед отправкой остатков`);
  }

  const failedStockWarningKeys = new Set();
  for (const shop of shops) {
    const stockChunkSize = Math.max(1, Math.min(500, Number(process.env.YANDEX_STOCK_CHUNK_SIZE || 100) || 100));
    for (const chunk of chunkArray(selected, stockChunkSize)) {
      if (!chunk.length) continue;
      const stockResult = await sendYandexStockRowsWithFallback(shop, chunk);
      results.push(...stockResult.results);
      if (stockResult.failed > 0) {
        const label = stockResult.fallbackError || "ошибка API";
        const warningKey = `${shop.id || shop.name}:${label}`;
        if (!failedStockWarningKeys.has(warningKey)) {
          failedStockWarningKeys.add(warningKey);
          warnings.push(`Yandex «${shop.name || shop.id}»: остатки не отправлены (${label})`);
        }
      }
    }
  }

  const failed = results.filter((item) => item.ok === false).length;
  const sent = results.reduce((total, item) => total + Number(item.sent || 0), 0);
  const partial = failed > 0 && sent > 0;
  return {
    ok: failed === 0,
    partial,
    dryRun,
    sent,
    failed,
    skipped: rows.length - selected.length,
    planned: selected.length,
    warnings,
    results,
  };
}

async function sendYandexStocksForExportedOzonProducts(products = [], options = {}) {
  const warnings = [];
  const allShops = Array.isArray(options.stockShops) && options.stockShops.length
    ? options.stockShops
    : getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const shops = yandexStockShops(allShops);
  const missingCampaignShops = allShops.filter((shop) => shop.apiKey && shop.businessId && !shop.campaignId);
  if (missingCampaignShops.length) {
    warnings.push(yandexMissingStockCampaignWarning(missingCampaignShops.length));
  }
  const existingOfferIds = options.existingOfferIds instanceof Set ? options.existingOfferIds : new Set();
  const rows = (Array.isArray(products) ? products : [])
    .map((product) => ({
      offerId: cleanText(product.offerId || product.offer_id),
      stock: pickOzonProductStockForYandex(product),
    }))
    .filter((row) => row.offerId && existingOfferIds.has(row.offerId.toLowerCase()));
  const results = [];
  if (!rows.length) {
    return { ok: true, sent: 0, failed: 0, skipped: Math.max(0, (products || []).length - rows.length), warnings, results };
  }
  if (!shops.length) {
    const detail = warnings.length ? warnings.join(" ") : "Yandex Market не настроен для остатков.";
    return {
      ok: false,
      sent: 0,
      failed: rows.length,
      skipped: Math.max(0, (products || []).length - rows.length),
      planned: rows.length,
      warnings: warnings.length ? warnings : [detail],
      results: rows.map((row) => ({
        stage: "stock",
        offerId: row.offerId,
        stock: row.stock,
        ok: false,
        error: detail,
      })),
    };
  }

  for (const shop of shops) {
    const stockChunkSize = Math.max(1, Math.min(500, Number(process.env.YANDEX_STOCK_CHUNK_SIZE || 100) || 100));
    for (const chunk of chunkArray(rows, stockChunkSize)) {
      if (!chunk.length) continue;
      const stockResult = await sendYandexStockRowsWithFallback(shop, chunk);
      results.push(...stockResult.results);
      if (stockResult.failed > 0) {
        const label = stockResult.fallbackError || "ошибка API";
        warnings.push(`Yandex «${shop.name || shop.id}»: остатки не отправлены (${label})`);
      }
    }
  }

  const failed = results.filter((item) => !item.ok).length;
  const sent = results.reduce((total, item) => total + Number(item.sent || (item.ok && item.stage === "stock" ? 1 : 0)), 0);
  return {
    ok: warnings.length === 0 && failed === 0,
    sent,
    failed,
    skipped: Math.max(0, (products || []).length - rows.length),
    planned: rows.length,
    warnings,
    results,
  };
}

async function buildOzonProductPreview({ limit = 200, search = "" } = {}) {
  const [rules, existingOfferIds, candidates] = await Promise.all([
    readOzonProductRules(),
    getOzonOfferIdSet(5000),
    getPriceMasterProductCandidates({ limit, search }),
  ]);

  const rows = candidates.map((candidate) => {
    const existing = existingOfferIds.has(candidate.offerId);
    const built = buildOzonProductPayload(candidate, rules);
    return {
      ...candidate,
      ozonExists: existing,
      nextPrice: Number(built.item.price || 0),
      missing: built.missing,
      warnings: built.warnings,
      ready: !existing && built.ready,
    };
  });

  return {
    createdAt: new Date().toISOString(),
    total: rows.length,
    existing: rows.filter((row) => row.ozonExists).length,
    ready: rows.filter((row) => row.ready).length,
    blocked: rows.filter((row) => !row.ready).length,
    rows,
  };
}

function likeSearch(value) {
  return `%${String(value || "").trim()}%`;
}

async function readSnapshot() {
  if (priceMasterSnapshotMemoryCache) return priceMasterSnapshotMemoryCache;
  try {
    priceMasterSnapshotMemoryCache = JSON.parse(await fs.readFile(snapshotPath, "utf8"));
    return priceMasterSnapshotMemoryCache;
  } catch (error) {
    if (error.code === "ENOENT") {
      priceMasterSnapshotMemoryCache = { createdAt: null, items: {}, changes: [] };
      return priceMasterSnapshotMemoryCache;
    }
    throw error;
  }
}

async function writeSnapshot(snapshot) {
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(snapshotPath, JSON.stringify(snapshot, null, 2), "utf8");
  priceMasterSnapshotMemoryCache = snapshot;
  priceMasterArticleIndexCache = null;
  priceMasterSnapshotIndexCache = null;
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  await writePriceMasterSnapshotToPostgres(snapshot).catch((error) => {
    logger.warn("PriceMaster postgres snapshot write failed", { detail: error?.message || String(error) });
  });
  invalidateWarehouseViewCache();
}

function stablePriceMasterSnapshotId(row = {}) {
  return crypto
    .createHash("sha1")
    .update([
      cleanText(row.article || row.NativeID || row.nativeId),
      cleanText(row.partnerId || row.PartnerID),
      cleanText(row.rowId || row.RowID),
      cleanText(row.name || row.nativeName || row.NativeName),
      cleanText(row.docDate || row.DocDate),
    ].join("|"))
    .digest("hex");
}

function priceMasterSnapshotArticleKey(row = {}) {
  const article = cleanText(row.article || row.NativeID || row.nativeId);
  if (article) return article;
  const rowId = cleanText(row.rowId || row.RowID);
  if (rowId) return `__no_article__:${rowId}`;
  return `__no_article__:${stablePriceMasterSnapshotId(row)}`;
}

function normalizePriceMasterSnapshotItemForPostgres(row = {}, updatedAt = new Date()) {
  const article = priceMasterSnapshotArticleKey(row);
  const rawPrice = row.price ?? row.NativePrice;
  const price = rawPrice === undefined || rawPrice === null || rawPrice === "" ? null : String(rawPrice);
  const currency = cleanText(row.currency || row.priceCurrency).toUpperCase();
  return {
    id: stablePriceMasterSnapshotId(row),
    rowId: cleanText(row.rowId || row.RowID) || null,
    article,
    partnerId: cleanText(row.partnerId || row.PartnerID) || null,
    partnerName: cleanText(row.partnerName || row.PartnerName) || null,
    nativeName: cleanText(row.name || row.nativeName || row.NativeName) || null,
    price,
    currency: currency === "RUB" || currency === "RUR" ? "RUB" : "USD",
    docDate: toDateOrNull(row.docDate || row.DocDate),
    active: row.active !== false && row.Active !== false && row.Active !== 0,
    raw: row,
    updatedAt,
  };
}

async function writePriceMasterSnapshotToPostgres(snapshot = {}) {
  if (!shouldUsePostgresStorage()) return { skipped: true, reason: "postgres_disabled" };
  const prisma = getPrisma();
  if (!prisma) return { skipped: true, reason: "no_prisma" };
  const rows = Object.values(snapshot.items || {});
  if (!rows.length) return { skipped: true, reason: "empty_snapshot" };

  const existingCount = await prisma.priceMasterSnapshotItem.count();
  const changes = Array.isArray(snapshot.changes) ? snapshot.changes.length : 0;
  if (existingCount === rows.length && changes === 0) {
    return { skipped: true, reason: "unchanged", items: rows.length };
  }

  const updatedAt = toDateOrNull(snapshot.createdAt) || new Date();
  const normalizedRows = rows
    .map((row) => normalizePriceMasterSnapshotItemForPostgres(row, updatedAt))
    .filter(Boolean);
  await prisma.priceMasterSnapshotItem.deleteMany({});
  for (const chunk of chunkArray(normalizedRows, 2000)) {
    await prisma.priceMasterSnapshotItem.createMany({ data: chunk, skipDuplicates: true });
  }
  logger.info("PriceMaster postgres snapshot written", {
    items: normalizedRows.length,
    changes,
    previousItems: existingCount,
  });
  return { items: normalizedRows.length, changes };
}

async function getPriceMasterSnapshotMeta() {
  const snapshot = await readSnapshot();
  const items = snapshot.items || {};
  const changes = Array.isArray(snapshot.changes) ? snapshot.changes : [];
  return {
    syncId: snapshot.syncId || null,
    updatedAt: snapshot.createdAt || null,
    items: Object.keys(items).length,
    changes: changes.length,
  };
}

async function getPriceMasterSnapshotMetaFast() {
  if (priceMasterSnapshotMemoryCache) return getPriceMasterSnapshotMeta();
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      if (prisma) {
        const [items, aggregate] = await Promise.all([
          prisma.priceMasterSnapshotItem.count(),
          prisma.priceMasterSnapshotItem.aggregate({ _max: { updatedAt: true } }),
        ]);
        return {
          syncId: null,
          updatedAt: aggregate?._max?.updatedAt ? aggregate._max.updatedAt.toISOString() : null,
          items,
          changes: 0,
        };
      }
    } catch (error) {
      logger.warn("fast PriceMaster snapshot meta failed", { detail: error?.message || String(error) });
    }
  }
  return { syncId: null, updatedAt: null, items: 0, changes: 0 };
}

function sortPriceMasterSnapshotRows(rows = []) {
  rows.sort((a, b) => new Date(b.docDate || 0) - new Date(a.docDate || 0) || Number(b.rowId || 0) - Number(a.rowId || 0));
  return rows;
}

async function getPriceMasterSnapshotIndexes() {
  const snapshot = await readSnapshot();
  if (priceMasterSnapshotIndexCache?.syncId === snapshot.syncId && priceMasterSnapshotIndexCache?.createdAt === snapshot.createdAt) {
    return priceMasterSnapshotIndexCache.indexes;
  }
  const indexes = {
    byArticle: new Map(),
    byName: new Map(),
    byRowId: new Map(),
    rows: Object.values(snapshot.items || {}),
  };
  for (const row of indexes.rows) {
    const article = cleanText(row.article || row.NativeID || row.nativeId);
    if (article) {
      if (!indexes.byArticle.has(article)) indexes.byArticle.set(article, []);
      indexes.byArticle.get(article).push(row);
    }
    const name = cleanText(row.name || row.nativeName || row.NativeName).toLowerCase();
    if (name) {
      if (!indexes.byName.has(name)) indexes.byName.set(name, []);
      indexes.byName.get(name).push(row);
    }
    const rowId = cleanText(row.rowId || row.RowID);
    if (rowId) {
      if (!indexes.byRowId.has(rowId)) indexes.byRowId.set(rowId, []);
      indexes.byRowId.get(rowId).push(row);
    }
  }
  for (const rows of indexes.byArticle.values()) sortPriceMasterSnapshotRows(rows);
  for (const rows of indexes.byName.values()) sortPriceMasterSnapshotRows(rows);
  for (const rows of indexes.byRowId.values()) sortPriceMasterSnapshotRows(rows);
  priceMasterSnapshotIndexCache = {
    syncId: snapshot.syncId || null,
    createdAt: snapshot.createdAt || null,
    indexes,
  };
  priceMasterArticleIndexCache = {
    syncId: snapshot.syncId || null,
    createdAt: snapshot.createdAt || null,
    index: indexes.byArticle,
  };
  return indexes;
}

async function getPriceMasterArticleIndex() {
  const snapshot = await readSnapshot();
  if (priceMasterArticleIndexCache?.syncId === snapshot.syncId && priceMasterArticleIndexCache?.createdAt === snapshot.createdAt) {
    return priceMasterArticleIndexCache.index;
  }
  return (await getPriceMasterSnapshotIndexes()).byArticle;
}

async function readPriceRetryQueue() {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().priceRetryQueueItem.findMany({
        where: { status: { in: ["pending", "processing", "failed", "delayed"] } },
        orderBy: { createdAt: "desc" },
        take: 5000,
      });
      const updatedAt = rows.reduce((latest, row) => {
        const time = row.updatedAt ? row.updatedAt.getTime() : 0;
        return time > latest ? time : latest;
      }, 0);
      return {
        updatedAt: updatedAt ? new Date(updatedAt).toISOString() : null,
        items: rows.map(priceRetryQueueItemFromPostgres),
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read price retry queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const text = await fs.readFile(priceRetryQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, items: [] };
    const parsed = JSON.parse(text);
    return {
      updatedAt: parsed.updatedAt || null,
      items: Array.isArray(parsed.items) ? parsed.items : [],
    };
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("price retry queue is invalid, resetting in memory", { detail: error.message });
      return { updatedAt: null, items: [] };
    }
    throw error;
  }
}

async function writePriceRetryQueue(queue) {
  const payload = {
    updatedAt: new Date().toISOString(),
    items: Array.isArray(queue.items) ? queue.items : [],
  };
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const queueKeys = payload.items.map((item) => priceRetryQueueKey(item)).filter(Boolean);
      await prisma.$transaction(async (tx) => {
        if (queueKeys.length) {
          await tx.priceRetryQueueItem.deleteMany({ where: { queueKey: { notIn: queueKeys } } });
        } else {
          await tx.priceRetryQueueItem.deleteMany({});
        }
        for (const item of payload.items) {
          const data = priceRetryQueueItemToPostgres(item);
          await tx.priceRetryQueueItem.upsert({
            where: { queueKey: data.queueKey },
            create: data,
            update: {
              marketplace: data.marketplace,
              target: data.target,
              productId: data.productId,
              offerId: data.offerId,
              price: data.price,
              oldPrice: data.oldPrice,
              status: data.status,
              attempts: data.attempts,
              error: data.error,
              payload: data.payload,
              nextRetryAt: data.nextRetryAt,
              lastAttemptAt: data.lastAttemptAt,
            },
          });
        }
      });
      if (!jsonFallbackEnabled()) return payload;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write price retry queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const tmpPath = `${priceRetryQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, priceRetryQueuePath);
  return payload;
}

function ozonUnarchiveDateKey(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const ms = Number.isFinite(value.getTime()) ? value.getTime() : Date.now();
  return new Date(ms + 3 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function nextOzonUnarchiveRetryAt(date = new Date()) {
  const value = date instanceof Date ? date : new Date(date);
  const base = Number.isFinite(value.getTime()) ? value : new Date();
  const moscow = new Date(base.getTime() + 3 * 60 * 60 * 1000);
  moscow.setUTCHours(24, 5, 0, 0);
  return new Date(moscow.getTime() - 3 * 60 * 60 * 1000).toISOString();
}

function ozonUnarchiveQueueKey(item = {}) {
  return [
    cleanText(item.target),
    cleanText(item.productId || item.product_id),
    cleanText(item.offerId || item.offer_id),
    cleanText(item.id || item.productUuid),
  ].filter(Boolean).join(":");
}

function normalizeOzonUnarchiveQueueItem(item = {}, fallback = {}) {
  const now = new Date().toISOString();
  return {
    id: cleanText(item.id || fallback.id),
    productId: cleanText(item.productId || item.product_id || fallback.productId),
    offerId: cleanText(item.offerId || item.offer_id || fallback.offerId),
    target: cleanText(item.target || fallback.target),
    marketplace: "ozon",
    status: cleanText(item.status || "pending") || "pending",
    queuedAt: cleanText(item.queuedAt || item.queued_at) || now,
    nextRetryAt: cleanText(item.nextRetryAt || item.next_retry_at) || nextOzonUnarchiveRetryAt(),
    lastAttemptAt: cleanText(item.lastAttemptAt || item.last_attempt_at),
    attempts: Math.max(0, Number(item.attempts || 0) || 0),
    warning: cleanText(item.warning || "ozon_unarchive_daily_limit_queued"),
    error: cleanText(item.error),
  };
}

function normalizeOzonUnarchiveQueue(queue = {}) {
  const items = Array.isArray(queue.items) ? queue.items : [];
  const daily = queue.daily && typeof queue.daily === "object" ? queue.daily : {};
  const byKey = new Map();
  for (const item of items) {
    const normalized = normalizeOzonUnarchiveQueueItem(item);
    const key = ozonUnarchiveQueueKey(normalized);
    if (key && normalized.status !== "done") byKey.set(key, normalized);
  }
  return {
    updatedAt: cleanText(queue.updatedAt),
    daily,
    items: Array.from(byKey.values()),
  };
}

function ozonUnarchiveQueueItemToPostgres(item = {}) {
  const normalized = normalizeOzonUnarchiveQueueItem(item);
  return {
    queueKey: ozonUnarchiveQueueKey(normalized) || crypto.randomUUID(),
    productId: cleanText(normalized.productId || normalized.id) || null,
    offerId: cleanText(normalized.offerId || normalized.id) || "unknown",
    target: cleanText(normalized.target) || null,
    status: normalized.status === "done" || normalized.status === "success" ? "success" : (["pending", "processing", "failed", "delayed"].includes(normalized.status) ? normalized.status : "pending"),
    queuedAt: toDateOrNull(normalized.queuedAt) || new Date(),
    nextRetryAt: toDateOrNull(normalized.nextRetryAt),
    lastAttemptAt: toDateOrNull(normalized.lastAttemptAt),
    attempts: Math.max(0, Number(normalized.attempts || 0) || 0),
    warning: cleanText(normalized.warning) || null,
    error: cleanText(normalized.error) || null,
    raw: normalized,
  };
}

function ozonUnarchiveQueueItemFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeOzonUnarchiveQueueItem({
    ...raw,
    id: raw.id || row.productId || row.offerId,
    productId: row.productId,
    offerId: row.offerId,
    target: row.target,
    status: row.status === "success" ? "done" : row.status,
    queuedAt: row.queuedAt?.toISOString?.() || raw.queuedAt,
    nextRetryAt: row.nextRetryAt?.toISOString?.() || raw.nextRetryAt,
    lastAttemptAt: row.lastAttemptAt?.toISOString?.() || raw.lastAttemptAt,
    attempts: row.attempts,
    warning: row.warning || raw.warning,
    error: row.error || raw.error,
  });
}

async function readOzonUnarchiveQueue() {
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = await prisma.ozonUnarchiveQueueItem.findMany({
        where: { status: { in: ["pending", "processing", "failed", "delayed"] } },
        orderBy: [{ nextRetryAt: "asc" }, { queuedAt: "asc" }],
        take: 5000,
      });
      const jsonDaily = await fs.readFile(ozonUnarchiveQueuePath, "utf8")
        .then((text) => JSON.parse(text || "{}")?.daily || {})
        .catch(() => ({}));
      const updatedAt = rows.reduce((latest, row) => {
        const time = row.updatedAt ? row.updatedAt.getTime() : 0;
        return time > latest ? time : latest;
      }, 0);
      return normalizeOzonUnarchiveQueue({
        updatedAt: updatedAt ? new Date(updatedAt).toISOString() : null,
        daily: jsonDaily,
        items: rows.map(ozonUnarchiveQueueItemFromPostgres),
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read ozon unarchive queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const text = await fs.readFile(ozonUnarchiveQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, daily: {}, items: [] };
    return normalizeOzonUnarchiveQueue(JSON.parse(text));
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, daily: {}, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("ozon unarchive queue is invalid, resetting in memory", { detail: error.message });
      return { updatedAt: null, daily: {}, items: [] };
    }
    throw error;
  }
}

async function writeOzonUnarchiveQueue(queue = {}) {
  const payload = normalizeOzonUnarchiveQueue({
    ...queue,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const queueKeys = payload.items.map(ozonUnarchiveQueueKey).filter(Boolean);
      await prisma.$transaction(async (tx) => {
        if (queueKeys.length) {
          await tx.ozonUnarchiveQueueItem.deleteMany({ where: { queueKey: { notIn: queueKeys }, status: { not: "success" } } });
        } else {
          await tx.ozonUnarchiveQueueItem.deleteMany({ where: { status: { not: "success" } } });
        }
        for (const item of payload.items) {
          const data = ozonUnarchiveQueueItemToPostgres(item);
          await tx.ozonUnarchiveQueueItem.upsert({
            where: { queueKey: data.queueKey },
            create: data,
            update: {
              productId: data.productId,
              offerId: data.offerId,
              target: data.target,
              status: data.status,
              queuedAt: data.queuedAt,
              nextRetryAt: data.nextRetryAt,
              lastAttemptAt: data.lastAttemptAt,
              attempts: data.attempts,
              warning: data.warning,
              error: data.error,
              raw: data.raw,
            },
          });
        }
      });
      if (!jsonFallbackEnabled()) return payload;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write ozon unarchive queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const tmpPath = `${ozonUnarchiveQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, ozonUnarchiveQueuePath);
  return payload;
}

function ozonUnarchiveDailyUsed(queue = {}, target = "", date = new Date()) {
  const key = ozonUnarchiveDateKey(date);
  const targetKey = cleanText(target) || "default";
  return Math.max(0, Number(queue.daily?.[key]?.[targetKey] || 0) || 0);
}

function setOzonUnarchiveDailyUsed(queue = {}, target = "", used = 0, date = new Date()) {
  const key = ozonUnarchiveDateKey(date);
  const targetKey = cleanText(target) || "default";
  queue.daily = queue.daily && typeof queue.daily === "object" ? queue.daily : {};
  queue.daily[key] = queue.daily[key] && typeof queue.daily[key] === "object" ? queue.daily[key] : {};
  queue.daily[key][targetKey] = Math.max(0, Math.round(Number(used || 0) || 0));
  for (const dateKey of Object.keys(queue.daily)) {
    if (dateKey < key) delete queue.daily[dateKey];
  }
  return queue;
}

function queueOzonUnarchiveItems(queue = {}, products = [], { nextRetryAt = nextOzonUnarchiveRetryAt(), warning = "ozon_unarchive_daily_limit_queued" } = {}) {
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  const byKey = new Map(normalizedQueue.items.map((item) => [ozonUnarchiveQueueKey(item), item]));
  for (const product of Array.isArray(products) ? products : []) {
    const item = normalizeOzonUnarchiveQueueItem({
      id: product.id,
      productId: product.productId,
      offerId: product.offerId,
      target: product.target,
      nextRetryAt,
      warning,
      status: "pending",
    });
    const key = ozonUnarchiveQueueKey(item);
    if (!key) continue;
    const existing = byKey.get(key);
    byKey.set(key, {
      ...(existing || {}),
      ...item,
      queuedAt: existing?.queuedAt || item.queuedAt,
      attempts: existing?.attempts || item.attempts,
      lastAttemptAt: existing?.lastAttemptAt || item.lastAttemptAt,
    });
  }
  normalizedQueue.items = Array.from(byKey.values());
  return normalizedQueue;
}

function removeOzonUnarchiveQueueItems(queue = {}, products = []) {
  const removeKeys = new Set((Array.isArray(products) ? products : []).map(ozonUnarchiveQueueKey).filter(Boolean));
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  normalizedQueue.items = normalizedQueue.items.filter((item) => !removeKeys.has(ozonUnarchiveQueueKey(item)));
  return normalizedQueue;
}

function ozonUnarchiveQueuedActions(products = [], queue = {}, { warning = "ozon_unarchive_daily_limit_queued", nextRetryAt = nextOzonUnarchiveRetryAt() } = {}) {
  const normalizedQueue = normalizeOzonUnarchiveQueue(queue);
  return (Array.isArray(products) ? products : []).map((item) => ({
    id: item.id,
    type: "unarchive",
    target: item.target,
    offerId: item.offerId,
    ok: true,
    pending: true,
    warning,
    queuedByDailyLimit: true,
    nextRetryAt,
    dailyLimit: ozonUnarchiveDailyLimit,
    queueSize: normalizedQueue.items.length,
  }));
}

function ozonUnarchiveQueuePublic(queue = {}, { limit = 1000 } = {}) {
  const normalized = normalizeOzonUnarchiveQueue(queue);
  const now = Date.now();
  const dailyLimit = ozonUnarchiveDailyLimit;
  const targets = new Map();
  const items = normalized.items
    .filter((item) => item.status !== "done")
    .map((item) => {
      const target = cleanText(item.target) || "default";
      const nextMs = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
      const due = !nextMs || !Number.isFinite(nextMs) || nextMs <= now;
      const existing = targets.get(target) || {
        target,
        dailyLimit,
        dailyUsed: ozonUnarchiveDailyUsed(normalized, target),
        availableToday: Math.max(0, dailyLimit - ozonUnarchiveDailyUsed(normalized, target)),
        due: 0,
        future: 0,
        total: 0,
      };
      existing.total += 1;
      if (due) existing.due += 1;
      else existing.future += 1;
      targets.set(target, existing);
      return {
        ...item,
        queueKey: ozonUnarchiveQueueKey(item),
        due,
        dailyLimit,
        dailyUsed: ozonUnarchiveDailyUsed(normalized, target),
        availableToday: Math.max(0, dailyLimit - ozonUnarchiveDailyUsed(normalized, target)),
      };
    })
    .sort((a, b) => {
      if (a.due !== b.due) return a.due ? -1 : 1;
      return new Date(a.nextRetryAt || a.queuedAt || 0) - new Date(b.nextRetryAt || b.queuedAt || 0);
    });
  return {
    ok: true,
    updatedAt: normalized.updatedAt,
    dailyLimit,
    total: items.length,
    due: items.filter((item) => item.due).length,
    future: items.filter((item) => !item.due).length,
    availableToday: Array.from(targets.values()).reduce((sum, item) => sum + item.availableToday, 0),
    nextRetryAt: items.filter((item) => !item.due).map((item) => item.nextRetryAt).filter(Boolean).sort()[0] || null,
    targets: Array.from(targets.values()),
    items: items.slice(0, cleanLimit(limit, 1000, 5000)),
  };
}

function ozonUnarchiveQueueAutomationPublic() {
  return {
    autoEnabled: ozonUnarchiveQueueAutoEnabled,
    autoRunning: ozonUnarchiveQueueAutoRunning || ozonUnarchiveQueueProcessQueued,
    autoQueued: ozonUnarchiveQueueProcessQueued,
    lastAutoRunAt: ozonUnarchiveQueueAutoLastRunAt,
    nextAutoRunAt: ozonUnarchiveQueueAutoNextRunAt,
    lastAutoResult: ozonUnarchiveQueueAutoLastResult,
  };
}

function normalizeMarketplaceEnum(value) {
  const text = cleanText(value).toLowerCase();
  return text === "yandex" ? "yandex" : "ozon";
}

function normalizeQueueStatusEnum(value, item = {}) {
  const text = cleanText(value).toLowerCase();
  if (["pending", "processing", "success", "failed", "delayed"].includes(text)) return text;
  if (item.nextRetryAt && new Date(item.nextRetryAt).getTime() > Date.now()) return "delayed";
  return item.error ? "failed" : "pending";
}

function priceRetryQueueItemToPostgres(item = {}) {
  const queueKey = priceRetryQueueKey(item) || crypto.randomUUID();
  const offerId = cleanText(item.offerId || item.offer_id || item.sku || item.id || item.productId || queueKey);
  const price = roundPrice(item.price ?? item.newPrice ?? item.targetPrice ?? 0) || 0;
  return {
    queueKey,
    marketplace: normalizeMarketplaceEnum(item.marketplace || item.target || "ozon"),
    target: cleanText(item.target || item.account || item.marketplace) || null,
    productId: cleanText(item.productId || item.id) || null,
    offerId,
    price,
    oldPrice: item.oldPrice === undefined && item.old_price === undefined ? null : (roundPrice(item.oldPrice ?? item.old_price) || 0),
    status: normalizeQueueStatusEnum(item.status, item),
    attempts: Math.max(0, Number(item.attempts || 0) || 0),
    error: cleanText(item.error || item.detail || ""),
    payload: cloneAuditValue(item) || {},
    nextRetryAt: toDateOrNull(item.nextRetryAt),
    lastAttemptAt: toDateOrNull(item.lastAttemptAt),
    createdAt: toDateOrNull(item.queuedAt || item.createdAt) || new Date(),
    updatedAt: toDateOrNull(item.updatedAt) || new Date(),
  };
}

function priceRetryQueueItemFromPostgres(row = {}) {
  const payload = row.payload && typeof row.payload === "object" && !Array.isArray(row.payload) ? row.payload : {};
  return {
    ...payload,
    id: payload.id || row.productId || row.offerId,
    productId: row.productId || payload.productId || payload.id || null,
    offerId: row.offerId || payload.offerId || null,
    marketplace: row.marketplace || payload.marketplace || "ozon",
    target: row.target || payload.target || row.marketplace || "ozon",
    price: row.price ?? payload.price ?? null,
    oldPrice: row.oldPrice ?? payload.oldPrice ?? null,
    status: row.status || payload.status || "pending",
    attempts: row.attempts || 0,
    error: row.error || payload.error || "",
    queueKey: row.queueKey || payload.queueKey || null,
    queuedAt: payload.queuedAt || (row.createdAt ? row.createdAt.toISOString() : null),
    lastAttemptAt: row.lastAttemptAt ? row.lastAttemptAt.toISOString() : (payload.lastAttemptAt || null),
    nextRetryAt: row.nextRetryAt ? row.nextRetryAt.toISOString() : (payload.nextRetryAt || null),
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : (payload.updatedAt || null),
  };
}

function priceRetryQueueKey(item = {}) {
  return cleanText(item.queueKey || `${item.id || item.productId || item.offerId}:${item.target || item.marketplace || "ozon"}`);
}

function isActiveDelayedPriceRetry(item = {}, now = new Date()) {
  const nextRetryAt = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
  if (!nextRetryAt || !Number.isFinite(nextRetryAt) || nextRetryAt <= now.getTime()) return false;
  const status = cleanText(item.status).toLowerCase();
  return (status === "delayed" || status === "pending")
    && (item.retryReason === "ozon_per_item_price_limit" || isOzonPerItemPriceLimitError({ message: item.error }));
}

function findActiveDelayedPriceRetry(queueItems = [], item = {}, now = new Date()) {
  const keys = new Set([
    priceRetryQueueKey(item),
    priceRetryQueueKey({ ...item, id: item.productId }),
    priceRetryQueueKey({ ...item, id: item.offerId }),
  ].filter(Boolean));
  return (Array.isArray(queueItems) ? queueItems : []).find((queueItem) =>
    keys.has(priceRetryQueueKey(queueItem)) && isActiveDelayedPriceRetry(queueItem, now)
  ) || null;
}

function priceRetryDelayMs(attempts = 1, error = null) {
  if (isOzonPerItemPriceLimitError(error)) {
    return Math.max(3_600_000, Number(process.env.OZON_PRICE_ITEM_LIMIT_RETRY_MS || 3_900_000) || 3_900_000);
  }
  if (isOzonOldPriceLessError(error)) {
    return Math.max(5_000, Number(process.env.OZON_OLD_PRICE_RETRY_MS || 15_000) || 15_000);
  }
  const base = Math.max(30_000, Number(process.env.OZON_PRICE_RETRY_BASE_DELAY_MS || 180_000) || 180_000);
  const max = Math.max(base, Number(process.env.OZON_PRICE_RETRY_MAX_DELAY_MS || 1_800_000) || 1_800_000);
  const attempt = Math.max(1, Number(attempts || 1) || 1);
  return Math.min(max, base * attempt * attempt);
}

function buildPriceRetryItem(item = {}, error = null, now = new Date()) {
  const attempts = Number(item.attempts || 0) + 1;
  const delayMs = priceRetryDelayMs(attempts, error);
  const nextRetryAt = new Date(now.getTime() + delayMs).toISOString();
  const delayedByLimit = isOzonPerItemPriceLimitError(error);
  const oldPriceAdjusted = isOzonOldPriceLessError(error);
  const price = roundPrice(item.price);
  return {
    ...item,
    error: error?.message || item.error || "retry_failed",
    oldPrice: oldPriceAdjusted ? resolveOzonOldPrice(price, item) : item.oldPrice,
    forceOldPrice: oldPriceAdjusted ? true : item.forceOldPrice,
    queueKey: priceRetryQueueKey(item),
    status: delayedByLimit ? "delayed" : (oldPriceAdjusted ? "pending" : "failed"),
    queuedAt: item.queuedAt || now.toISOString(),
    lastAttemptAt: now.toISOString(),
    attempts,
    nextRetryAt,
    retryReason: delayedByLimit ? "ozon_per_item_price_limit" : (oldPriceAdjusted ? "ozon_old_price_adjusted" : "send_failed"),
  };
}

async function appendPriceHistoryRows(rows = []) {
  if (!shouldUsePostgresStorage()) return 0;
  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      productId: cleanText(row.productId || row.id) || null,
      marketplace: normalizeMarketplaceEnum(row.marketplace || "ozon"),
      target: cleanText(row.target || row.marketplace) || null,
      offerId: cleanText(row.offerId || row.offer_id),
      oldPrice: row.oldPrice === undefined || row.oldPrice === null ? null : (roundPrice(row.oldPrice) || 0),
      newPrice: roundPrice(row.newPrice ?? row.price ?? 0) || 0,
      status: normalizeQueueStatusEnum(row.status || (row.error ? "failed" : "success")),
      response: cloneAuditValue(row.response || row.result || null),
      error: cleanText(row.error || ""),
      createdAt: toDateOrNull(row.createdAt || row.at) || new Date(),
    }))
    .filter((row) => row.offerId && row.newPrice > 0);
  if (!normalizedRows.length) return 0;
  try {
    const windowMs = priceHistoryDedupeWindowMs();
    const dedupedRows = [];
    const seenRows = new Set();
    for (const row of normalizedRows) {
      const createdAt = row.createdAt || new Date();
      const recentSince = new Date(createdAt.getTime() - windowMs);
      const rowKey = [
        row.productId || "",
        row.marketplace,
        row.target || "",
        row.offerId,
        row.oldPrice ?? "",
        row.newPrice,
        row.status,
        row.error || "",
        Math.floor(createdAt.getTime() / windowMs),
      ].join("|");
      if (seenRows.has(rowKey)) continue;
      seenRows.add(rowKey);
      const existing = await getPrisma().priceHistory.findFirst({
        where: {
          productId: row.productId || null,
          marketplace: row.marketplace,
          target: row.target || null,
          offerId: row.offerId,
          oldPrice: row.oldPrice === undefined ? null : row.oldPrice,
          newPrice: row.newPrice,
          status: row.status,
          OR: [{ error: row.error || "" }, { error: null }],
          createdAt: {
            gte: recentSince,
            lte: new Date(createdAt.getTime() + windowMs),
          },
        },
        select: { id: true },
      });
      if (!existing) dedupedRows.push(row);
    }
    if (!dedupedRows.length) return 0;
    const result = await getPrisma().priceHistory.createMany({
      data: dedupedRows,
      skipDuplicates: true,
    });
    return result.count || 0;
  } catch (error) {
    logger.warn("postgres price history append failed", { detail: error?.message || String(error), rows: normalizedRows.length });
    return 0;
  }
}

function priceHistoryRowFromPostgres(row = {}) {
  return {
    id: row.id || null,
    productId: row.productId || null,
    marketplace: row.marketplace || "ozon",
    target: row.target || null,
    offerId: row.offerId || null,
    oldPrice: row.oldPrice ?? null,
    newPrice: row.newPrice ?? null,
    status: row.status || "pending",
    response: row.response || null,
    error: row.error || "",
    at: row.createdAt ? row.createdAt.toISOString() : null,
    createdAt: row.createdAt ? row.createdAt.toISOString() : null,
  };
}

async function readPriceHistory({ productId, offerId, marketplace, status, dateFrom, dateTo, limit = 100, offset = 0 } = {}) {
  const productIds = splitList(productId);
  const offerIds = splitList(offerId);
  const statuses = splitList(status)
    .map((item) => item.toLowerCase() === "error" ? "failed" : item.toLowerCase())
    .filter((item) => item !== "all");
  const marketplaceFilter = cleanText(marketplace).toLowerCase();
  const from = toDateOrNull(dateFrom);
  const to = toDateOrNull(dateTo);
  const safeLimit = Math.max(1, Math.min(500, Number(limit || 100) || 100));
  const safeOffset = Math.max(0, Number(offset || 0) || 0);

  if (shouldUsePostgresStorage()) {
    try {
      const where = {};
      if (productIds.length) where.productId = { in: productIds };
      if (offerIds.length) where.offerId = { in: offerIds };
      if (marketplaceFilter && marketplaceFilter !== "all") where.marketplace = normalizeMarketplaceEnum(marketplaceFilter);
      if (statuses.length) where.status = { in: statuses.map((item) => normalizeQueueStatusEnum(item)) };
      if (from || to) {
        where.createdAt = {};
        if (from) where.createdAt.gte = from;
        if (to) where.createdAt.lte = to;
      }
      const [total, rows] = await Promise.all([
        getPrisma().priceHistory.count({ where }),
        getPrisma().priceHistory.findMany({
          where,
          orderBy: { createdAt: "desc" },
          skip: safeOffset,
          take: safeLimit,
        }),
      ]);
      return {
        source: "postgres",
        total,
        limit: safeLimit,
        offset: safeOffset,
        items: rows.map(priceHistoryRowFromPostgres),
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read price history postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }

  const warehouse = await readWarehouse();
  const rows = [];
  for (const product of warehouse.products || []) {
    if (productIds.length && !productIds.includes(String(product.id))) continue;
    if (offerIds.length && !offerIds.includes(String(product.offerId))) continue;
    if (marketplaceFilter && marketplaceFilter !== "all" && cleanText(product.marketplace) !== marketplaceFilter) continue;
    for (const entry of product.priceHistory || []) {
      const at = toDateOrNull(entry.at || entry.createdAt);
      const normalizedStatus = normalizeQueueStatusEnum(entry.status === "error" ? "failed" : entry.status);
      if (statuses.length && !statuses.includes(normalizedStatus)) continue;
      if (from && (!at || at < from)) continue;
      if (to && (!at || at > to)) continue;
      rows.push({
        productId: product.id,
        marketplace: product.marketplace,
        target: entry.target || product.target || product.marketplace,
        offerId: entry.offerId || product.offerId,
        oldPrice: entry.oldPrice ?? null,
        newPrice: entry.newPrice ?? null,
        status: normalizedStatus,
        response: null,
        error: entry.error || "",
        supplierName: entry.supplierName || "",
        supplierArticle: entry.supplierArticle || "",
        reason: entry.reason || "",
        at: at ? at.toISOString() : null,
        createdAt: at ? at.toISOString() : null,
      });
    }
  }
  rows.sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0));
  return {
    source: "json",
    total: rows.length,
    limit: safeLimit,
    offset: safeOffset,
    items: rows.slice(safeOffset, safeOffset + safeLimit),
  };
}

function schedulePriceRetryProcessing(delayMs = null) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return;
  if (priceRetryTimer) return;
  const waitMs = Math.max(5_000, Number(delayMs ?? process.env.OZON_PRICE_RETRY_POLL_MS ?? 60_000) || 60_000);
  priceRetryTimer = setTimeout(async () => {
    priceRetryTimer = null;
    try {
      const result = await processPriceRetryQueue({
        limit: Math.max(1, Number(process.env.OZON_PRICE_RETRY_AUTO_LIMIT || 25) || 25),
        respectNextRetryAt: true,
        trigger: "auto",
      });
      if (result.processed || result.failed) {
        logger.info("price retry auto run complete", {
          processed: result.processed,
          retried: result.retried,
          failed: result.failed,
          remaining: result.remaining,
        });
      }
    } catch (error) {
      logger.warn("price retry auto run failed", { detail: error?.message || String(error) });
    } finally {
      const queue = await readPriceRetryQueue().catch(() => ({ items: [] }));
      if ((queue.items || []).length) {
        const nextAt = Math.min(...queue.items.map((item) => new Date(item.nextRetryAt || 0).getTime()).filter(Number.isFinite));
        const nextDelay = Number.isFinite(nextAt) ? Math.max(5_000, nextAt - Date.now()) : null;
        schedulePriceRetryProcessing(nextDelay);
      }
    }
  }, waitMs);
}

function schedulePriceRetryItems(items = []) {
  const retryItems = Array.isArray(items) ? items : [];
  if (!retryItems.length) return;
  const nextAt = Math.min(...retryItems
    .map((item) => new Date(item.nextRetryAt || 0).getTime())
    .filter(Number.isFinite));
  const delayMs = Number.isFinite(nextAt) ? Math.max(5_000, nextAt - Date.now()) : null;
  schedulePriceRetryProcessing(delayMs);
}

function productToPostgresData(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const images = compactObject({
    imageUrl: normalized.imageUrl || null,
    images: normalized.ozon?.images || normalized.yandex?.pictures || [],
  });
  return {
    id: normalized.id,
    marketplace: normalizeMarketplaceEnum(normalized.marketplace),
    target: normalized.target || normalized.marketplace || null,
    offerId: normalized.offerId || normalized.sku || normalized.id,
    productId: normalized.productId || null,
    name: normalized.name || normalized.offerId || normalized.id,
    brand: resolveWarehouseBrand(normalized) || null,
    images: cloneAuditValue(images) || {},
    marketplaceState: cloneAuditValue(normalized.marketplaceState) || {},
    currentPrice: roundPrice(normalized.marketplacePrice || 0) || null,
    targetPrice: roundPrice(normalized.nextPrice || normalized.targetPrice || normalized.calculatedPrice || 0) || null,
    targetStock: Number.isFinite(Number(normalized.targetStock)) ? Number(normalized.targetStock) : null,
    status: normalized.marketplaceState?.code || normalized.marketplaceState?.state || normalized.status || null,
    archived: Boolean(normalized.marketplaceState?.archived || normalized.archived),
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
}

function supplierToPostgresData(supplier = {}) {
  const normalized = normalizeManagedSupplier(supplier);
  return {
    partnerId: normalized.partnerId || normalized.id || normalizeSupplierName(normalized.name),
    name: normalized.name || normalized.partnerId || normalized.id,
    active: !normalized.stopped,
    defaultCurrency: normalized.priceCurrency === "RUB" ? "RUB" : "USD",
    stopReason: normalized.stopReason || null,
    note: normalized.note || normalized.inactiveComment || null,
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
}

function linkToPostgresData(product, link = {}) {
  const normalized = normalizeWarehouseLink(link);
  const supplierArticle = normalized.article || normalized.sourceRowId || normalized.exactName;
  const data = {
    id: "",
    productId: product.id,
    supplierArticle,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    priceCurrency: normalized.priceCurrency === "RUB" ? "RUB" : "USD",
    keyword: normalized.keyword || null,
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
  data.id = crypto.createHash("sha1").update(productLinkPostgresIdentityKey(data)).digest("hex");
  return data;
}

function productLinkPostgresIdentityKey(data = {}) {
  return [
    cleanText(data.productId),
    cleanText(data.supplierArticle).toLowerCase(),
    cleanText(data.partnerId).toLowerCase(),
    normalizeSupplierName(data.supplierName),
    cleanText(data.keyword).toLowerCase(),
    cleanText(data.priceCurrency || "USD").toUpperCase(),
  ].join("|");
}

function dedupeProductLinkRows(rows = []) {
  const byIdentity = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || !row.productId || !row.supplierArticle) continue;
    const key = productLinkPostgresIdentityKey(row);
    const existing = byIdentity.get(key);
    if (!existing) {
      byIdentity.set(key, row);
      continue;
    }
    const existingUpdatedAt = toDateOrNull(existing.updatedAt)?.getTime() || 0;
    const rowUpdatedAt = toDateOrNull(row.updatedAt)?.getTime() || 0;
    byIdentity.set(key, rowUpdatedAt >= existingUpdatedAt ? row : existing);
  }
  return Array.from(byIdentity.values());
}

function productFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const imageState = row.images && typeof row.images === "object" && !Array.isArray(row.images) ? row.images : {};
  const rowName = cleanText(row.name);
  const rawName = cleanText(raw.name || raw.ozon?.name || raw.yandex?.name);
  const rowMarketplace = cleanText(row.marketplace || raw.marketplace || row.target || raw.target).toLowerCase();
  const effectiveName = isWeakProductName(rowName, row.offerId) && rawName && !isWeakProductName(rawName, row.offerId)
    ? rawName
    : rowName;
  const effectiveImageUrl = firstImageUrl(raw.imageUrl || raw.ozon?.primaryImage || raw.ozon?.images || raw.yandex?.pictures || imageState.imageUrl || imageState.images);
  const postgresLinksLoaded = Array.isArray(row.links);
  const postgresLinks = (postgresLinksLoaded ? row.links : []).map((link) => {
    const linkRaw = link.raw && typeof link.raw === "object" && !Array.isArray(link.raw) ? link.raw : {};
    return normalizeWarehouseLink({
      ...linkRaw,
      id: link.id,
      article: linkRaw.article || (linkRaw.matchType ? "" : link.supplierArticle),
      supplierName: link.supplierName,
      partnerId: link.partnerId,
      priceCurrency: link.priceCurrency,
      keyword: link.keyword,
      createdAt: link.createdAt ? link.createdAt.toISOString() : undefined,
      updatedAt: link.updatedAt ? link.updatedAt.toISOString() : undefined,
      createdBy: linkRaw.createdBy,
      updatedBy: linkRaw.updatedBy,
    });
  });
  const rawLinks = Array.isArray(raw.links) ? raw.links.map(normalizeWarehouseLink) : [];
  const links = postgresLinksLoaded
    ? (postgresLinks.length > 0 || rowMarketplace !== "yandex" ? postgresLinks : rawLinks)
    : rawLinks;
  return normalizeWarehouseProduct({
    ...raw,
    id: row.id,
    marketplace: row.marketplace,
    target: row.target || row.marketplace,
    offerId: row.offerId,
    productId: row.productId,
    name: effectiveName,
    brand: row.brand || raw.brand,
    imageUrl: effectiveImageUrl,
    marketplacePrice: row.currentPrice ?? raw.marketplacePrice,
    currentPrice: row.currentPrice ?? raw.currentPrice ?? raw.marketplacePrice,
    targetPrice: row.targetPrice ?? raw.targetPrice ?? raw.nextPrice,
    targetStock: row.targetStock ?? raw.targetStock,
    marketplaceState: row.marketplaceState || raw.marketplaceState,
    status: row.status || raw.status,
    archived: row.archived ?? raw.archived,
    links,
    createdAt: row.createdAt ? row.createdAt.toISOString() : raw.createdAt,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : raw.updatedAt,
  });
}

function warehouseProductPostgresUpdateData(data = {}) {
  return {
    marketplace: data.marketplace,
    target: data.target,
    offerId: data.offerId,
    productId: data.productId,
    name: data.name,
    brand: data.brand,
    images: data.images,
    marketplaceState: data.marketplaceState,
    currentPrice: data.currentPrice,
    targetPrice: data.targetPrice,
    targetStock: data.targetStock,
    status: data.status,
    archived: data.archived,
    raw: data.raw,
    updatedAt: data.updatedAt,
  };
}

async function upsertWarehouseProductPostgres(client, product) {
  const data = productToPostgresData(product);
  await client.warehouseProduct.upsert({
    where: { id: data.id },
    create: data,
    update: warehouseProductPostgresUpdateData(data),
  });
}

async function runWithLimitedConcurrency(items = [], concurrency = 1, worker) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  const limit = Math.max(1, Math.min(list.length, Number(concurrency) || 1));
  const results = new Array(list.length);
  let nextIndex = 0;
  await Promise.all(Array.from({ length: limit }, async () => {
    while (nextIndex < list.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await worker(list[currentIndex], currentIndex);
    }
  }));
  return results;
}

function warehousePostgresWriteConcurrency() {
  return Math.max(1, Math.min(2, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CONCURRENCY || 1) || 1));
}

async function replaceProductLinksInPostgres(prisma, products = []) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return { products: 0, links: 0 };
  let linksWritten = 0;
  const chunkSize = Math.max(1, Math.min(100, Number(process.env.WAREHOUSE_POSTGRES_LINK_WRITE_CHUNK_SIZE || 50) || 50));
  const writeConcurrency = warehousePostgresWriteConcurrency();
  for (const productChunk of chunkArray(normalizedProducts, chunkSize)) {
    await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
      await upsertWarehouseProductPostgres(prisma, product);
    });
    const productIds = productChunk.map((product) => product.id).filter(Boolean);
    const linkRows = [];
    for (const product of productChunk) {
      linkRows.push(...(product.links || [])
        .map((link) => linkToPostgresData(product, link))
        .filter((linkData) => linkData.supplierArticle));
    }
    if (productIds.length) {
      await prisma.productLink.deleteMany({ where: { productId: { in: productIds } } });
    }
    for (const linkChunk of chunkArray(dedupeProductLinkRows(linkRows), 1000)) {
      if (!linkChunk.length) continue;
      const result = await prisma.productLink.createMany({ data: linkChunk, skipDuplicates: true });
      linksWritten += result.count || 0;
    }
    markWarehousePostgresProductsWritten(productChunk);
  }
  return { products: normalizedProducts.length, links: linksWritten };
}

async function getWarehousePostgresSuppliers(prisma) {
  if (
    warehousePostgresSuppliersCache
    && Date.now() - warehousePostgresSuppliersCache.at < warehousePostgresDetailCacheTtlMs
  ) {
    return warehousePostgresSuppliersCache.value;
  }
  const suppliers = await prisma.managedSupplier.findMany({ orderBy: { name: "asc" } });
  const normalized = suppliers.map(supplierFromPostgres);
  warehousePostgresSuppliersCache = { at: Date.now(), value: normalized };
  return normalized;
}

function warehousePostgresCachedDetail(key, build) {
  const cacheKey = cleanText(key);
  if (!cacheKey) return build();
  const cached = warehousePostgresDetailCache.get(cacheKey);
  if (cached && Date.now() - cached.at < warehousePostgresDetailCacheTtlMs) return cloneAuditValue(cached.value);
  return Promise.resolve(build()).then((value) => {
    if (value) warehousePostgresDetailCache.set(cacheKey, { at: Date.now(), value: cloneAuditValue(value) });
    return value;
  });
}

async function getWarehouseBrandListFromPostgres(prisma) {
  if (
    warehouseBrandListCache
    && Date.now() - warehouseBrandListCache.at < warehouseBrandListCacheTtlMs
  ) {
    return warehouseBrandListCache.value.slice();
  }
  if (prisma?.brandIndexItem) {
    try {
      let rows = await prisma.brandIndexItem.findMany({
        select: { normalizedBrand: true, displayBrand: true },
        distinct: ["normalizedBrand"],
        orderBy: { displayBrand: "asc" },
        take: 10000,
      });
      if (!rows.length) {
        await rebuildWarehouseBrandIndexPostgres(prisma, { limit: Number(process.env.WAREHOUSE_BRAND_INDEX_REBUILD_LIMIT || 100000) || 100000 });
        rows = await prisma.brandIndexItem.findMany({
          select: { normalizedBrand: true, displayBrand: true },
          distinct: ["normalizedBrand"],
          orderBy: { displayBrand: "asc" },
          take: 10000,
        });
      }
      if (rows.length) {
        const brands = rows.map((row) => cleanText(row.displayBrand)).filter(Boolean).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
        warehouseBrandListCache = { at: Date.now(), value: brands };
        return brands.slice();
      }
    } catch (error) {
      logger.warn("warehouse brand index list failed, using product brands", { detail: error?.message || String(error) });
    }
  }
  await ensureWarehousePostgresBrandsBackfilled(prisma);
  const rows = await prisma.warehouseProduct.findMany({
    where: {
      AND: [
        enabledWarehouseTargetWhere(),
        { brand: { not: null } },
        { brand: { not: "" } },
      ],
    },
    select: {
      brand: true,
    },
    distinct: ["brand"],
    orderBy: { brand: "asc" },
  });
  const unique = new Map();
  for (const row of rows) {
    const brand = cleanText(row.brand);
    if (!brand) continue;
    const key = brand.toLowerCase();
    if (!unique.has(key)) unique.set(key, brand);
  }
  const brands = Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
  warehouseBrandListCache = { at: Date.now(), value: brands };
  return brands.slice();
}

function resolveWarehouseBrandFromPostgresRow(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return resolveWarehouseBrand({
    ...raw,
    id: row.id,
    marketplace: row.marketplace,
    target: row.target,
    offerId: row.offerId,
    productId: row.productId,
    name: row.name || raw.name,
    brand: row.brand || raw.brand,
  });
}

async function ensureWarehousePostgresBrandsBackfilled(prisma, { force = false } = {}) {
  if (!prisma) return { updated: 0, scanned: 0, skipped: true };
  if (warehousePostgresBrandBackfillDone && !force) return { updated: 0, scanned: 0, skipped: true };
  if (warehousePostgresBrandBackfillPromise && !force) return warehousePostgresBrandBackfillPromise;
  const maxRows = Math.max(1000, Math.min(100000, Number(process.env.WAREHOUSE_BRAND_BACKFILL_LIMIT || 50000) || 50000));
  warehousePostgresBrandBackfillPromise = (async () => {
    const rows = await prisma.warehouseProduct.findMany({
      where: {
        AND: [
          enabledWarehouseTargetWhere(),
          force ? {} : {
            OR: [
              { brand: null },
              { brand: "" },
            ],
          },
        ].filter((item) => Object.keys(item || {}).length),
      },
      select: { id: true, name: true, brand: true, raw: true, marketplace: true, target: true, offerId: true, productId: true },
      take: maxRows,
      orderBy: [{ updatedAt: "desc" }],
    });
    let updated = 0;
    const updates = [];
    for (const row of rows) {
      const brand = cleanText(resolveWarehouseBrandFromPostgresRow(row));
      if (!brand) continue;
      if (cleanText(row.brand).toLowerCase() === brand.toLowerCase()) continue;
      updates.push({ id: row.id, brand });
    }
    for (const batch of chunkArray(updates, 250)) {
      await runWithLimitedConcurrency(batch, 2, async (item) => {
        await prisma.warehouseProduct.update({
          where: { id: item.id },
          data: { brand: item.brand },
        });
        updated += 1;
      });
    }
    warehousePostgresBrandBackfillDone = rows.length < maxRows;
    warehouseBrandListCache = null;
    if (updated || rows.length) {
      logger.info("warehouse postgres brands backfilled from raw", {
        scanned: rows.length,
        updated,
        complete: warehousePostgresBrandBackfillDone,
      });
    }
    return { updated, scanned: rows.length, skipped: false, complete: warehousePostgresBrandBackfillDone };
  })().catch((error) => {
    logger.warn("warehouse postgres brand backfill failed", { detail: error?.message || String(error) });
    return { updated: 0, scanned: 0, skipped: false, error: error?.message || String(error) };
  }).finally(() => {
    warehousePostgresBrandBackfillPromise = null;
  });
  return warehousePostgresBrandBackfillPromise;
}

async function ensureWarehousePostgresLinksBackfilled(prisma) {
  if (warehousePostgresLinkBackfillDone) return { created: 0, skipped: true };
  if (warehousePostgresLinkBackfillPromise) return warehousePostgresLinkBackfillPromise;
  warehousePostgresLinkBackfillPromise = (async () => {
    const productsWithRawLinksPromise = prisma.$queryRaw`
      SELECT id, raw
      FROM "warehouse_products"
      WHERE jsonb_typeof(raw->'links') = 'array'
        AND jsonb_array_length(raw->'links') > 0
    `.catch((error) => {
      logger.warn("warehouse postgres raw-link prefilter failed, using full backfill scan", { detail: error?.message || String(error) });
      return prisma.warehouseProduct.findMany({ select: { id: true, raw: true } });
    });
    const [products, existingLinks] = await Promise.all([
      productsWithRawLinksPromise,
      prisma.productLink.findMany({
        select: {
          productId: true,
          supplierArticle: true,
          supplierName: true,
          partnerId: true,
          keyword: true,
          priceCurrency: true,
        },
      }),
    ]);
    const existingByProduct = new Map();
    for (const link of existingLinks) {
      if (!link.productId) continue;
      if (!existingByProduct.has(link.productId)) existingByProduct.set(link.productId, new Set());
      existingByProduct.get(link.productId).add(warehouseLinkIdentityKey({
        article: link.supplierArticle,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        keyword: link.keyword,
        priceCurrency: link.priceCurrency,
      }));
    }
    const rows = [];
    for (const product of products) {
      const raw = product.raw && typeof product.raw === "object" && !Array.isArray(product.raw) ? product.raw : {};
      const rawLinks = Array.isArray(raw.links) ? raw.links : [];
      const existingKeys = existingByProduct.get(product.id) || new Set();
      for (const rawLink of rawLinks) {
        const row = linkToPostgresData({ id: product.id }, rawLink);
        if (!row.supplierArticle) continue;
        const identity = warehouseLinkIdentityKey({
          article: row.supplierArticle,
          supplierName: row.supplierName,
          partnerId: row.partnerId,
          keyword: row.keyword,
          priceCurrency: row.priceCurrency,
        });
        if (existingKeys.has(identity)) continue;
        existingKeys.add(identity);
        rows.push(row);
      }
    }
    let created = 0;
    const chunkSize = 1000;
    for (let index = 0; index < rows.length; index += chunkSize) {
      const batch = rows.slice(index, index + chunkSize);
      if (!batch.length) continue;
      const result = await prisma.productLink.createMany({ data: batch, skipDuplicates: true });
      created += result.count || 0;
    }
    warehousePostgresLinkBackfillDone = true;
    if (created) logger.info("warehouse postgres links backfilled from raw", { created });
    return { created, skipped: false };
  })().catch((error) => {
    warehousePostgresLinkBackfillPromise = null;
    logger.warn("warehouse postgres links backfill failed", { detail: error?.message || String(error) });
    return { created: 0, skipped: false, error: error?.message || String(error) };
  });
  return warehousePostgresLinkBackfillPromise;
}

function supplierFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeManagedSupplier({
    ...raw,
    id: raw.id || row.partnerId || row.id,
    partnerId: row.partnerId,
    name: row.name,
    stopped: row.active === false,
    priceCurrency: row.defaultCurrency,
    stopReason: row.stopReason,
    note: row.note,
    createdAt: row.createdAt ? row.createdAt.toISOString() : raw.createdAt,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : raw.updatedAt,
  });
}

function refreshWarehouseHashCache(warehouse = {}) {
  warehousePostgresHashCache = new Map();
  warehousePostgresUpdatedAtCache = new Map();
  for (const product of warehouse.products || []) {
    warehousePostgresHashCache.set(product.id, true);
    warehousePostgresUpdatedAtCache.set(product.id, cleanText(product.updatedAt));
  }
}

function markWarehousePostgresProductsWritten(products = []) {
  for (const product of products || []) {
    if (!product?.id) continue;
    warehousePostgresHashCache.set(product.id, true);
    warehousePostgresUpdatedAtCache.set(product.id, cleanText(product.updatedAt));
  }
}

async function readWarehouseFromPostgres(prisma) {
  await ensureWarehousePostgresLinksBackfilled(prisma);
  const [products, suppliers] = await Promise.all([
    prisma.warehouseProduct.findMany({
      include: { links: true },
      orderBy: { updatedAt: "desc" },
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
  ]);
  if (!products.length && !suppliers.length) return null;
  const updatedAtMs = Math.max(
    ...products.map((item) => item.updatedAt?.getTime() || 0),
    ...suppliers.map((item) => item.updatedAt?.getTime() || 0),
    0,
  );
  const warehouse = {
    createdAt: products[0]?.createdAt?.toISOString() || new Date().toISOString(),
    updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : null,
    products: products.map(productFromPostgres),
    suppliers: suppliers.map(supplierFromPostgres),
  };
  refreshWarehouseHashCache(warehouse);
  return warehouse;
}

async function getWarehouseMetaFast() {
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const [productCount, supplierCount, productAgg, supplierAgg] = await Promise.all([
        prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
        prisma.managedSupplier.count(),
        prisma.warehouseProduct.aggregate({
          where: enabledWarehouseTargetWhere(),
          _max: { updatedAt: true },
          _min: { createdAt: true },
        }),
        prisma.managedSupplier.aggregate({
          _max: { updatedAt: true },
          _min: { createdAt: true },
        }),
      ]);
      const updatedAtMs = Math.max(
        productAgg._max.updatedAt?.getTime?.() || 0,
        supplierAgg._max.updatedAt?.getTime?.() || 0,
      );
      const createdAtMs = Math.min(
        ...[
          productAgg._min.createdAt?.getTime?.() || 0,
          supplierAgg._min.createdAt?.getTime?.() || 0,
        ].filter(Boolean),
      );
      return {
        updatedAt: updatedAtMs ? new Date(updatedAtMs).toISOString() : null,
        createdAt: Number.isFinite(createdAtMs) ? new Date(createdAtMs).toISOString() : null,
        products: productCount,
        suppliers: supplierCount,
        source: "postgres",
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("warehouse postgres meta failed, using memory/json fallback", { detail: error?.message || String(error) });
    }
  }
  const warehouse = await readWarehouse();
  return {
    updatedAt: warehouse.updatedAt || warehouse.createdAt || null,
    createdAt: warehouse.createdAt || null,
    products: Array.isArray(warehouse.products) ? warehouse.products.length : 0,
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
    source: shouldUsePostgresStorage() ? "fallback" : "json",
  };
}

async function writeWarehouseToPostgres(prisma, payload) {
  const products = Array.isArray(payload.products) ? payload.products : [];
  const suppliers = Array.isArray(payload.suppliers) ? payload.suppliers : [];
  const chunkSize = Math.max(25, Math.min(250, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 100) || 100));
  const changedProducts = products.filter((product) =>
    !warehousePostgresHashCache.has(product.id)
    || cleanText(product.updatedAt) !== warehousePostgresUpdatedAtCache.get(product.id)
  );
  if (changedProducts.length) {
    logger.info("warehouse postgres write delta", { products: changedProducts.length, suppliers: suppliers.length, chunkSize });
  }
  const writeConcurrency = warehousePostgresWriteConcurrency();
  const supplierRows = suppliers.map(supplierToPostgresData);
  await runWithLimitedConcurrency(supplierRows, writeConcurrency, async (data) => {
    await prisma.managedSupplier.upsert({
      where: { partnerId: data.partnerId || data.name },
      create: data,
      update: {
        name: data.name,
        active: data.active,
        defaultCurrency: data.defaultCurrency,
        stopReason: data.stopReason,
        note: data.note,
        raw: data.raw,
        updatedAt: data.updatedAt,
      },
    });
  });
  for (const productChunk of chunkArray(changedProducts, chunkSize)) {
    await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
      await upsertWarehouseProductPostgres(prisma, product);
    });
    markWarehousePostgresProductsWritten(productChunk);
  }
}

function scheduleWarehousePostgresWrite(prisma, payload) {
  warehousePostgresWriteQueuedPayload = payload;
  if (warehousePostgresWriteRunning) return;
  warehousePostgresWriteRunning = true;
  setImmediate(async () => {
    try {
      while (warehousePostgresWriteQueuedPayload) {
        const nextPayload = warehousePostgresWriteQueuedPayload;
        warehousePostgresWriteQueuedPayload = null;
        await writeWarehouseToPostgres(prisma, nextPayload);
      }
    } catch (error) {
      logger.warn("write warehouse postgres failed, keeping JSON fallback", { detail: error?.message || String(error) });
    } finally {
      warehousePostgresWriteRunning = false;
      if (warehousePostgresWriteQueuedPayload) scheduleWarehousePostgresWrite(prisma, warehousePostgresWriteQueuedPayload);
    }
  });
}

async function readWarehouse() {
  if (warehouseMemoryCache) return warehouseMemoryCache;
  if (shouldUsePostgresStorage()) {
    try {
      const warehouse = await readWarehouseFromPostgres(getPrisma());
      if (warehouse) {
        const materialized = materializeYandexExportedProductsForWarehouse(warehouse);
        warehouseMemoryCache = materialized.warehouse;
        if (materialized.added > 0) {
          await writeWarehouse(materialized.warehouse);
        }
        return warehouseMemoryCache;
      }
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read warehouse postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const warehouse = JSON.parse(await fs.readFile(warehousePath, "utf8"));
    const normalized = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: warehouse.updatedAt || null,
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
    const materialized = materializeYandexExportedProductsForWarehouse(normalized);
    warehouseMemoryCache = materialized.warehouse;
    if (materialized.added > 0) {
      await writeWarehouse(materialized.warehouse, { writePostgres: false });
    }
    refreshWarehouseHashCache(warehouseMemoryCache);
    return warehouseMemoryCache;
  } catch (error) {
    if (error.code === "ENOENT") {
      warehouseMemoryCache = { createdAt: new Date().toISOString(), updatedAt: null, products: [], suppliers: [] };
      refreshWarehouseHashCache(warehouseMemoryCache);
      return warehouseMemoryCache;
    }
    throw error;
  }
}

async function writeWarehouseJsonPayload(payload) {
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${warehousePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, warehousePath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
}

function normalizeWarehousePayload(warehouse) {
  return {
    createdAt: warehouse.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
  };
}

async function writeWarehouse(warehouse, { writePostgres = true } = {}) {
  invalidateWarehouseViewCache();
  warehouseWritePromise = warehouseWritePromise.then(async () => {
    const materialized = materializeYandexExportedProductsForWarehouse(warehouse);
    const payload = normalizeWarehousePayload(materialized.warehouse);
    if (materialized.added > 0) {
      logger.info("materialized yandex exported products into warehouse", { added: materialized.added });
    }
    warehouseMemoryCache = payload;
    if (writePostgres && shouldUsePostgresStorage()) {
      scheduleWarehousePostgresWrite(getPrisma(), payload);
    } else if (!shouldUsePostgresStorage()) {
      refreshWarehouseHashCache(payload);
    }
    await writeWarehouseJsonPayload(payload);
    return payload;
  });
  return warehouseWritePromise;
}

async function writeWarehouseProductPatch(products = [], { reason = "warehouse_product_patch", writeLinks = true } = {}) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return null;
  invalidateWarehouseViewCache();
  const warehouse = await readWarehouse();
  const byId = new Map(normalizedProducts.map((product) => [String(product.id), product]));
  let changed = 0;
  const productIndex = warehouseProductIndexFor(warehouse);
  if (normalizedProducts.length <= Math.max(50, Math.floor((warehouse.products || []).length / 10))) {
    for (const product of normalizedProducts) {
      const index = productIndex.get(String(product.id));
      if (index === undefined) continue;
      warehouse.products[index] = product;
      changed += 1;
    }
  } else {
    warehouse.products = (warehouse.products || []).map((product) => {
      const replacement = byId.get(String(product.id));
      if (!replacement) return product;
      changed += 1;
      return replacement;
    });
    warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
  }
  if (!changed) return warehouseMemoryCache || warehouse;
  const payload = normalizeWarehousePayload(warehouse);
  warehouseMemoryCache = payload;
  if (shouldUsePostgresStorage()) {
    if (writeLinks) {
      await replaceProductLinksInPostgres(getPrisma(), normalizedProducts);
    } else {
      const chunkSize = Math.max(25, Math.min(250, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 100) || 100));
      const writeConcurrency = warehousePostgresWriteConcurrency();
      for (const productChunk of chunkArray(normalizedProducts, chunkSize)) {
        await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
          await upsertWarehouseProductPostgres(getPrisma(), product);
        });
        markWarehousePostgresProductsWritten(productChunk);
      }
    }
  } else {
    refreshWarehouseHashCache(payload);
  }
  writeWarehouse(payload, { writePostgres: false }).catch((error) => {
    logger.warn("warehouse product patch JSON fallback write failed", { reason, detail: error?.message || String(error) });
  });
  return payload;
}

function writeWarehouseInBackground(warehouse, reason = "warehouse_background_write") {
  writeWarehouse(warehouse).catch((error) => {
    logger.error("warehouse background write failed", { reason, detail: error?.message || String(error) });
  });
}

async function readDailySyncState() {
  try {
    return JSON.parse(await fs.readFile(dailySyncPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return {
        status: "idle",
        enabled: dailySyncEnabled,
        time: dailySyncTime,
        lastRunAt: null,
        nextRunAt: dailySyncNextRunAt,
      };
    }
    throw error;
  }
}

async function writeDailySyncState(state) {
  await fs.mkdir(dataDir, { recursive: true });
  const current = await readDailySyncState().catch(() => ({}));
  const payload = {
    enabled: dailySyncEnabled,
    time: dailySyncTime,
    nextRunAt: dailySyncNextRunAt,
    updatedAt: new Date().toISOString(),
    logs: Array.isArray(current.logs) ? current.logs : [],
    ...state,
  };
  if (Array.isArray(state.logs)) {
    payload.logs = [...state.logs, ...(Array.isArray(current.logs) ? current.logs : [])].slice(0, 30);
  }
  await fs.writeFile(dailySyncPath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

function withDailySyncLog(state) {
  const logs = Array.isArray(state.logs) ? state.logs : [];
  const entry = {
    at: state.lastRunAt || new Date().toISOString(),
    status: state.status,
    trigger: state.trigger,
    priceMasterItems: state.priceMaster?.items || 0,
    priceMasterChanges: state.priceMaster?.changes || 0,
    warehouseTotal: state.warehouse?.total || 0,
    warehouseReady: state.warehouse?.ready || 0,
    warehouseChanged: state.warehouse?.changed || 0,
    withoutSupplier: state.warehouse?.withoutSupplier || 0,
    pricePushSent: state.warehouse?.pricePush?.sent ?? null,
    pricePushFailed: state.warehouse?.pricePush?.failed ?? null,
    pricePushSkipped: state.warehouse?.pricePush?.skipped ?? null,
    error: state.error || state.warehouse?.sourceError || state.warehouse?.pricePush?.error || null,
  };
  return { ...state, logs: [entry, ...logs].slice(0, 30) };
}

async function getDailySyncStatus() {
  const state = await readDailySyncState();
  return {
    ...state,
    enabled: dailySyncEnabled,
    time: dailySyncTime,
    nextRunAt: dailySyncNextRunAt,
    running: Boolean(dailySyncPromise),
  };
}

function warehouseProductExactMergeKey(product = {}) {
  return [
    cleanText(product.target || product.marketplace || "default").toLowerCase(),
    cleanText(product.offerId || product.offer_id || "").toLowerCase(),
  ].join(":");
}

function warehouseProductLooseMergeKeys(product = {}) {
  const marketplace = cleanText(product.marketplace || "ozon").toLowerCase();
  const offerId = cleanText(product.offerId || product.offer_id || "").toLowerCase();
  const productId = cleanText(product.productId || product.product_id || "").toLowerCase();
  const sku = cleanText(product.sku || "").toLowerCase();
  return [
    productId ? `${marketplace}:product:${productId}` : "",
    sku ? `${marketplace}:sku:${sku}` : "",
    offerId ? `${marketplace}:offer:${offerId}` : "",
  ].filter(Boolean);
}

function mergeOzonDraftForWarehouseProduct(currentProduct = {}, importedProduct = {}, preserveRichFields = false) {
  const current = currentProduct?.ozon || {};
  const imported = importedProduct?.ozon || {};
  const merged = { ...current, ...imported };
  if (!preserveRichFields) return merged;

  const offerId = cleanText(importedProduct.offerId || currentProduct.offerId || imported.offerId || current.offerId);
  const importedNameWeak = isWeakProductName(imported.name || importedProduct.name, offerId);
  if (importedNameWeak && cleanText(current.name)) merged.name = current.name;

  for (const field of [
    "vendor",
    "description",
    "categoryId",
    "typeId",
    "barcode",
    "primaryImage",
    "colorImage",
  ]) {
    if (!cleanText(imported[field]) && cleanText(current[field])) merged[field] = current[field];
  }

  for (const field of ["barcodes", "images", "images360", "attributes", "complexAttributes"]) {
    const importedValue = Array.isArray(imported[field]) ? imported[field] : [];
    const currentValue = Array.isArray(current[field]) ? current[field] : [];
    if (!importedValue.length && currentValue.length) merged[field] = currentValue;
  }

  if (current.extra && typeof current.extra === "object" && (!imported.extra || !Object.keys(imported.extra).length)) {
    merged.extra = current.extra;
  }
  return merged;
}

function mergeYandexDraftForWarehouseProduct(currentProduct = {}, importedProduct = {}) {
  const current = currentProduct?.yandex || {};
  const imported = importedProduct?.yandex || {};
  const merged = { ...current, ...imported };
  const currentHasLivePrice = Boolean(currentProduct?.lastYandexPriceSend?.status || currentProduct?.lastYandexPriceSend?.at);
  if ((Number(imported.price || 0) <= 0 && Number(current.price || 0) > 0) || (currentHasLivePrice && Number(current.price || 0) > 0)) {
    merged.price = current.price;
  }
  if (!cleanText(imported.name) && cleanText(current.name)) merged.name = current.name;
  if (!cleanText(imported.description) && cleanText(current.description)) merged.description = current.description;
  if (!cleanText(imported.vendor) && cleanText(current.vendor)) merged.vendor = current.vendor;
  const importedPictures = Array.isArray(imported.pictures) ? imported.pictures : [];
  const currentPictures = Array.isArray(current.pictures) ? current.pictures : [];
  if (!importedPictures.length && currentPictures.length) merged.pictures = currentPictures;
  const importedBarcodes = Array.isArray(imported.barcodes) ? imported.barcodes : [];
  const currentBarcodes = Array.isArray(current.barcodes) ? current.barcodes : [];
  if (!importedBarcodes.length && currentBarcodes.length) merged.barcodes = currentBarcodes;
  if (current.extra && typeof current.extra === "object") {
    merged.extra = {
      ...current.extra,
      ...(imported.extra && typeof imported.extra === "object" ? imported.extra : {}),
    };
  }
  return merged;
}

function mergeProducts(existingProducts, importedProducts) {
  const map = new Map();
  const looseIndex = new Map();
  const rememberLooseKeys = (product, exactKey) => {
    for (const key of warehouseProductLooseMergeKeys(product)) {
      if (!looseIndex.has(key)) looseIndex.set(key, new Set());
      looseIndex.get(key).add(exactKey);
    }
  };

  for (const product of existingProducts) {
    const normalized = normalizeWarehouseProduct(product);
    const exactKey = warehouseProductExactMergeKey(normalized);
    map.set(exactKey, normalized);
    rememberLooseKeys(normalized, exactKey);
  }

  for (const imported of importedProducts) {
    if (!imported.offerId) continue;
    const importedNormalized = normalizeWarehouseProduct(imported);
    const exactKey = warehouseProductExactMergeKey(importedNormalized);
    let matchedKey = map.has(exactKey) ? exactKey : "";
    if (!matchedKey) {
      for (const looseKey of warehouseProductLooseMergeKeys(importedNormalized)) {
        const candidates = Array.from(looseIndex.get(looseKey) || []);
        if (candidates.length === 1) {
          matchedKey = candidates[0];
          break;
        }
      }
    }
    const current = matchedKey ? map.get(matchedKey) : null;
    if (matchedKey && matchedKey !== exactKey) map.delete(matchedKey);
    const currentState = current?.marketplaceState || {};
    const importedState = importedNormalized.marketplaceState || {};
    const preserveCurrentState = Boolean(
      currentState.code
        && currentState.code !== "unknown"
        && (importedState.partial || importedState.code === "unknown"),
    );
    const offerId = cleanText(importedNormalized.offerId || current?.offerId);
    const importedNameWeak = importedNormalized.marketplace === "ozon" && isWeakProductName(importedNormalized.name, offerId);
    const currentNameWeak = current?.marketplace === "ozon" && isWeakProductName(current?.name, offerId);
    const preserveCurrentRichOzonFields = Boolean(
      current
        && importedNormalized.marketplace === "ozon"
        && (importedState.partial || importedNameWeak || !importedNormalized.imageUrl),
    );
    const preserveCurrentName = Boolean(current?.name && !currentNameWeak && importedNameWeak);
    const preserveCurrentImage = Boolean(current?.imageUrl && !importedNormalized.imageUrl);
    const preserveCurrentProductUrl = Boolean(current?.productUrl && !importedNormalized.productUrl);
    const preserveCurrentSku = Boolean(current?.sku && !importedNormalized.sku);
    const currentHasLiveYandexPrice = Boolean(
      current?.marketplace === "yandex"
        && importedNormalized.marketplace === "yandex"
        && (current?.lastYandexPriceSend?.status || current?.lastYandexPriceSend?.at),
    );
    const preserveCurrentYandexMarkup = Boolean(
      current?.marketplace === "yandex"
        && Number(current.markup || 0) > 0
        && (
          current?.markupSource === "manual"
          || current?.yandex?.extra?.manualMarkup === true
          || currentHasLiveYandexPrice
        ),
    );
    const preserveCurrentPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.marketplacePrice && !importedNormalized.marketplacePrice),
    );
    const preserveCurrentCurrentPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.currentPrice && !importedNormalized.currentPrice),
    );
    const preserveCurrentTargetPrice = Boolean(
      currentHasLiveYandexPrice
        || (current?.targetPrice && !importedNormalized.targetPrice),
    );
    const preserveCurrentTargetStock = Boolean(
      current?.targetStock !== null
        && current?.targetStock !== undefined
        && (importedNormalized.targetStock === null || importedNormalized.targetStock === undefined),
    );
    const preserveCurrentMinPrice = Boolean(current?.marketplaceMinPrice && !importedNormalized.marketplaceMinPrice);
    const merged = normalizeWarehouseProduct({
      ...current,
      ...importedNormalized,
      id: current?.id || importedNormalized.id,
      name: preserveCurrentName ? current.name : importedNormalized.name,
      imageUrl: preserveCurrentImage ? current.imageUrl : importedNormalized.imageUrl,
      productUrl: preserveCurrentProductUrl ? current.productUrl : importedNormalized.productUrl,
      sku: preserveCurrentSku ? current.sku : importedNormalized.sku,
      marketplacePrice: preserveCurrentPrice ? current.marketplacePrice : importedNormalized.marketplacePrice,
      currentPrice: preserveCurrentCurrentPrice ? current.currentPrice : importedNormalized.currentPrice,
      targetPrice: preserveCurrentTargetPrice ? current.targetPrice : importedNormalized.targetPrice,
      targetStock: preserveCurrentTargetStock ? current.targetStock : importedNormalized.targetStock,
      marketplaceMinPrice: preserveCurrentMinPrice ? current.marketplaceMinPrice : importedNormalized.marketplaceMinPrice,
      marketplaceState: preserveCurrentState ? currentState : importedState,
      ozon: mergeOzonDraftForWarehouseProduct(current, importedNormalized, preserveCurrentRichOzonFields),
      yandex: mergeYandexDraftForWarehouseProduct(current, importedNormalized),
      keyword: current?.keyword || importedNormalized.keyword,
      markup: preserveCurrentYandexMarkup ? current.markup : (current?.markup || importedNormalized.markup),
      autoPriceEnabled: current?.autoPriceEnabled !== undefined ? current.autoPriceEnabled : importedNormalized.autoPriceEnabled,
      autoPriceMin: current?.autoPriceMin ?? importedNormalized.autoPriceMin,
      autoPriceMax: current?.autoPriceMax ?? importedNormalized.autoPriceMax,
      lastOzonPriceSend: importedNormalized.lastOzonPriceSend || current?.lastOzonPriceSend,
      lastYandexPriceSend: importedNormalized.lastYandexPriceSend || current?.lastYandexPriceSend,
      links: Array.isArray(current?.links) ? current.links : (Array.isArray(importedNormalized.links) ? importedNormalized.links : []),
      createdAt: current?.createdAt || importedNormalized.createdAt,
    });
    const mergedKey = warehouseProductExactMergeKey(merged);
    map.set(mergedKey, merged);
    rememberLooseKeys(merged, mergedKey);
  }

  return Array.from(map.values()).sort((a, b) => a.targetName.localeCompare(b.targetName) || a.name.localeCompare(b.name));
}

async function importOzonWarehouseProducts(limit = Number.POSITIVE_INFINITY, existingProducts = [], options = {}) {
  const accounts = getOzonAccounts().filter((account) => account.clientId && account.apiKey);
  const imported = [];
  const warnings = [];
  if (!accounts.length) return { imported, warnings };

  const perAccountLimit = Number.isFinite(Number(limit)) && Number(limit) > 0
    ? Math.max(1, Math.ceil(Number(limit) / accounts.length))
    : Number.POSITIVE_INFINITY;

  for (const account of accounts) {
    try {
      const products = await getOzonProducts(perAccountLimit, account);
      const existingByOffer = ozonExistingProductMap(existingProducts, account);
      let infoMap = new Map();
      let stockMap = new Map();
      let priceMap = new Map();
      try {
        const configuredDetailLimit = process.env.OZON_SYNC_DETAIL_LIMIT !== undefined
          ? Number(process.env.OZON_SYNC_DETAIL_LIMIT)
          : Number(process.env.OZON_SYNC_INFO_LIMIT || 800);
        const infoLimit = Math.max(0, Number.isFinite(configuredDetailLimit) ? configuredDetailLimit : 800);
        const infoOfferIds = pickOzonDetailOfferIds(products, existingByOffer, infoLimit);
        logger.info("ozon product list loaded", {
          account: account.id,
          listed: products.length,
          detailRefresh: infoOfferIds.length,
          detailLimit: infoLimit,
        });
        options.onProgress?.({
          percent: 32,
          stage: "Ozon список",
          meta: `Ozon вернул ${formatRuNumber(products.length)} карточек. Детально обновить: ${formatRuNumber(infoOfferIds.length)}.`,
          processed: products.length,
          total: products.length,
        });
        if (infoOfferIds.length) {
          infoMap = await getOzonProductInfoMap(infoOfferIds, account, {
            continueOnError: true,
            onProgress: (progress) => options.onProgress?.({
              percent: 32 + Math.round((progress.processed / Math.max(1, progress.total)) * 16),
              stage: progress.stage,
              meta: `Загружаю названия и фото Ozon: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
              processed: progress.processed,
              total: progress.total,
            }),
          });
          stockMap = await getOzonStockMap(infoOfferIds, account, {
            continueOnError: true,
            onProgress: (progress) => options.onProgress?.({
              percent: 48 + Math.round((progress.processed / Math.max(1, progress.total)) * 12),
              stage: progress.stage,
              meta: `Загружаю остатки Ozon: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
              processed: progress.processed,
              total: progress.total,
            }),
          });
          priceMap = await getOzonPriceMap(infoOfferIds, account, {
            continueOnError: true,
            onProgress: (progress) => options.onProgress?.({
              percent: 60 + Math.round((progress.processed / Math.max(1, progress.total)) * 10),
              stage: progress.stage,
              meta: `Загружаю цены Ozon: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
              processed: progress.processed,
              total: progress.total,
            }),
          });
          const detailOfferSet = new Set(infoOfferIds.map(ozonOfferMapKey));
          const missingProductIds = products
            .filter((product) => detailOfferSet.has(ozonOfferMapKey(product.offer_id || product.offerId)))
            .filter((product) => !getOzonOfferMapValue(infoMap, product.offer_id || product.offerId))
            .map((product) => cleanText(product.product_id || product.productId))
            .filter(Boolean);
          if (missingProductIds.length) {
            const infoByProductId = await getOzonProductInfoMapByProductIds(missingProductIds, account, {
              continueOnError: true,
              onProgress: (progress) => options.onProgress?.({
                percent: 70 + Math.round((progress.processed / Math.max(1, progress.total)) * 4),
                stage: progress.stage,
                meta: `Добираю детали Ozon по product_id: ${formatRuNumber(progress.processed)} из ${formatRuNumber(progress.total)}.`,
                processed: progress.processed,
                total: progress.total,
              }),
            });
            for (const product of products) {
              const productId = cleanText(product.product_id || product.productId);
              const info = infoByProductId.get(productId);
              if (info) setOzonOfferMapValue(infoMap, product.offer_id || product.offerId || info.offer_id || info.offerId, info);
            }
            logger.info("ozon product details recovered by product id", {
              account: account.id,
              requested: missingProductIds.length,
              recovered: infoByProductId.size,
            });
          }
          logger.info("ozon product detail maps loaded", {
            account: account.id,
            requested: infoOfferIds.length,
            info: infoMap.size,
            stock: stockMap.size,
            price: priceMap.size,
          });
        }
      } catch (error) {
        infoMap = new Map();
        stockMap = new Map();
        priceMap = new Map();
        const label = error?.message || error?.code || "ошибка API";
        warnings.push(`Ozon «${account.name || account.id}»: не загружены детали/цены (${label})`);
        logger.warn("ozon info/price batch failed", { account: account.id, detail: label });
      }

      imported.push(...products.map((product) => {
        const info = getOzonOfferMapValue(infoMap, product.offer_id) || {};
        const stockInfo = getOzonOfferMapValue(stockMap, product.offer_id) || {};
        const priceInfo = getOzonOfferMapValue(priceMap, product.offer_id) || {};
        const hasInfo = Boolean(Object.keys(info).length);
        const hasStock = Boolean(Object.keys(stockInfo).length);
        const hasPrice = Boolean(Object.keys(priceInfo).length);
        const priceDetails = normalizeOzonPriceDetails(priceInfo);
        const cabinetPrice =
          pickOzonCabinetListedPrice(priceDetails) || parseMoneyValue(info.price) || parseMoneyValue(product.price) || null;
        const sourceSku = info.sources?.find((source) => source.sku)?.sku;
        const sku = product.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku;
        const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image);
        const productUrl = info.product_url || info.url || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : "");
        const marketplaceState = pickOzonState(product, info, stockInfo);
        if (!hasInfo && !hasStock) marketplaceState.partial = true;
        return normalizeWarehouseProduct({
          target: account.id,
          marketplace: "ozon",
          targetName: account.name || "Ozon",
          offerId: product.offer_id,
          productId: product.product_id || info.product_id || info.id,
          sku,
          productUrl,
          imageUrl: primaryImage,
          marketplacePrice: cabinetPrice,
          marketplaceMinPrice: priceDetails.minPrice || null,
          name: info.name || product.name || product.offer_id || `Ozon ${product.product_id}`,
          marketplaceState,
          ozon: {
            offerId: product.offer_id,
            vendor: cleanText(info.brand || info.vendor || ""),
            name: info.name || product.name || product.offer_id,
            description: info.description || "",
            categoryId: info.description_category_id || info.category_id,
            typeId: info.type_id || info.description_type_id,
            price: hasPrice ? cabinetPrice || undefined : undefined,
            minPrice: hasPrice ? priceDetails.minPrice || undefined : undefined,
            oldPrice: hasPrice ? priceDetails.oldPrice || parseMoneyValue(info.old_price) || undefined : undefined,
            marketingSellerPrice: hasPrice ? priceDetails.marketingSellerPrice || undefined : undefined,
            marketingPrice: hasPrice ? priceDetails.marketingPrice || undefined : undefined,
            retailPrice: hasPrice ? priceDetails.retailPrice || undefined : undefined,
            barcode: (info.barcodes || [])[0] || "",
            barcodes: info.barcodes || [],
            primaryImage,
            images: info.images || [],
            images360: info.images360 || [],
            colorImage: firstImageUrl(info.color_image),
          },
          createdAt: new Date().toISOString(),
        });
      }));
    } catch (error) {
      const label = error?.message || error?.code || "ошибка";
      warnings.push(`Ozon «${account.name || account.id}»: ${label}`);
      logger.warn("ozon account import failed", { account: account.id, detail: label });
    }
  }

  return { imported, warnings };
}

async function importYandexWarehouseProducts(limit = Number.POSITIVE_INFINITY) {
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const imported = [];
  const warnings = [];
  if (!shops.length) return { imported, warnings };

  const perShopLimit = Number.isFinite(Number(limit)) && Number(limit) > 0
    ? Math.max(1, Math.ceil(Number(limit) / shops.length))
    : Number.POSITIVE_INFINITY;

  for (const shop of shops) {
    try {
      const mappings = await getYandexOfferMappings(shop, perShopLimit);
      imported.push(
        ...mappings
          .map((item) => normalizeYandexWarehouseProduct(item, shop))
          .filter(Boolean),
      );
    } catch (error) {
      const label = error?.message || error?.code || "ошибка";
      warnings.push(`Yandex «${shop.name || shop.id}»: ${label}`);
      logger.warn("yandex shop import failed", { shop: shop.id, detail: label });
    }
  }

  return { imported, warnings };
}

async function syncWarehouseProductsFromMarketplaces(warehouse, limit = Number.POSITIVE_INFINITY, options = {}) {
  const warnings = [];
  let imported = [];
  try {
    const oz = await importOzonWarehouseProducts(limit, warehouse.products || [], options);
    imported = imported.concat(oz.imported);
    warnings.push(...oz.warnings);
  } catch (error) {
    const label = error?.message || error?.code || "ошибка";
    warnings.push(`Ozon: ${label}`);
    logger.warn("ozon import failed", { detail: label });
  }
  try {
    const ya = await importYandexWarehouseProducts(limit);
    imported = imported.concat(ya.imported);
    warnings.push(...ya.warnings);
  } catch (error) {
    const label = error?.message || error?.code || "ошибка";
    warnings.push(`Yandex Market: ${label}`);
    logger.warn("yandex import failed", { detail: label });
  }
  return {
    warehouse: { ...warehouse, products: mergeProducts(warehouse.products, imported) },
    warnings,
  };
}

function isWeakProductName(name, offerId) {
  const current = cleanText(name);
  const article = cleanText(offerId);
  if (!current) return true;
  if (/^товар\s+ozon$/i.test(current)) return true;
  if (/^ozon\s+\d+$/i.test(current)) return true;
  if (article && current.toLowerCase() === article.toLowerCase()) return true;
  return /^[A-ZА-Я0-9._-]{4,}$/i.test(current) && !/\s/.test(current);
}

function applyOzonInfoToWarehouseProduct(product, info = {}, account = {}, stockInfo = {}, priceInfo = {}) {
  const sourceSku = info.sources?.find((source) => source.sku)?.sku;
  const sku = product.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku;
  const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image);
  const productUrl = info.product_url || info.url || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : product.productUrl);
  const betterName = cleanText(info.name);
  const nextName = betterName && !isWeakProductName(betterName, product.offerId)
    ? betterName
    : (betterName && isWeakProductName(product.name, product.offerId) ? betterName : product.name || betterName);
  const priceDetails = normalizeOzonPriceDetails(priceInfo);
  const cabinetPrice =
    pickOzonCabinetListedPrice(priceDetails) || parseMoneyValue(info.price) || product.marketplacePrice || null;
  const hasStockInfo = Boolean(stockInfo && Object.keys(stockInfo).length);
  const marketplaceState = hasStockInfo || info.visibility || info.status || info.state
    ? pickOzonState(product, info, stockInfo)
    : product.marketplaceState;
  return normalizeWarehouseProduct({
    ...product,
    target: product.target || account.id,
    marketplace: "ozon",
    targetName: account.name || product.targetName || "Ozon",
    name: nextName,
    productId: product.productId || info.product_id || info.id,
    sku: sku || product.sku,
    productUrl,
    imageUrl: primaryImage || product.imageUrl,
    marketplacePrice: cabinetPrice,
    marketplaceMinPrice: priceDetails.minPrice || product.marketplaceMinPrice || null,
    marketplaceState,
    ozon: {
      ...(product.ozon || {}),
      offerId: product.offerId,
      vendor: cleanText(info.brand || info.vendor || (product.ozon || {}).vendor || ""),
      name: betterName || product.ozon?.name || nextName,
      description: info.description || product.ozon?.description || "",
      categoryId: info.description_category_id || info.category_id || product.ozon?.categoryId,
      typeId: info.type_id || info.description_type_id || product.ozon?.typeId,
      price: cabinetPrice || undefined,
      minPrice: priceDetails.minPrice || product.ozon?.minPrice || undefined,
      oldPrice: priceDetails.oldPrice || parseMoneyValue(info.old_price) || product.ozon?.oldPrice || undefined,
      marketingSellerPrice: priceDetails.marketingSellerPrice || product.ozon?.marketingSellerPrice || undefined,
      marketingPrice: priceDetails.marketingPrice || product.ozon?.marketingPrice || undefined,
      retailPrice: priceDetails.retailPrice || product.ozon?.retailPrice || undefined,
      barcode: (info.barcodes || [])[0] || product.ozon?.barcode || "",
      barcodes: info.barcodes || product.ozon?.barcodes || [],
      primaryImage: primaryImage || product.ozon?.primaryImage || "",
      images: info.images || product.ozon?.images || [],
      images360: info.images360 || product.ozon?.images360 || [],
      colorImage: firstImageUrl(info.color_image) || product.ozon?.colorImage || "",
    },
  });
}

async function enrichWarehouseProducts(productIds = []) {
  const ids = new Set((Array.isArray(productIds) ? productIds : []).map(String));
  if (!ids.size) return [];

  const warehouse = await readWarehouse();
  const updated = [];

  for (const account of getOzonAccounts()) {
    const products = warehouse.products.filter(
      (product) => ids.has(product.id) && product.marketplace === "ozon" && matchesOzonTarget(product.target, account.id) && product.offerId,
    );
    if (!products.length) continue;

    const offerIds = products.map((product) => product.offerId);
    const [infoMap, stockMap, priceMap] = await Promise.all([
      getOzonProductInfoMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon info skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
      getOzonStockMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon stock skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
      getOzonPriceMap(offerIds, account).catch((error) => {
        logger.warn("warehouse enrich Ozon price skipped", { account: account.id, detail: error?.message || String(error) });
        return new Map();
      }),
    ]);
    for (const product of products) {
      const info = infoMap.get(product.offerId) || {};
      const stockInfo = stockMap.get(product.offerId) || {};
      const priceInfo = priceMap.get(product.offerId) || {};
      if (!Object.keys(info).length && !Object.keys(stockInfo).length && !Object.keys(priceInfo).length) continue;
      const index = warehouse.products.findIndex((item) => item.id === product.id);
      if (index < 0) continue;
      warehouse.products[index] = applyOzonInfoToWarehouseProduct(
        warehouse.products[index],
        info,
        account,
        stockInfo,
        priceInfo,
      );
      updated.push(warehouse.products[index]);
    }
  }

  if (updated.length) {
    await writeWarehouseProductPatch(updated, { reason: "warehouse_ozon_enrich", writeLinks: false });
  }
  return updated;
}

async function enrichWeakOzonProductsForPage(products = []) {
  if (process.env.WAREHOUSE_PAGE_AUTO_ENRICH_ENABLED !== "true") return products;
  const limit = Math.max(0, Number(process.env.WAREHOUSE_PAGE_AUTO_ENRICH_LIMIT || 60) || 60);
  if (!limit) return products;
  const candidates = (products || [])
    .filter((product) => product?.marketplace === "ozon" && product.offerId && ozonProductNeedsDetailRefresh(product))
    .slice(0, limit);
  if (!candidates.length) return products;

  const updatedById = new Map();
  for (const account of getOzonAccounts()) {
    const accountProducts = candidates.filter((product) => matchesOzonTarget(product.target, account.id));
    if (!accountProducts.length) continue;
    const offerIds = accountProducts.map((product) => product.offerId);
    try {
      const [infoMap, stockMap, priceMap] = await Promise.all([
        getOzonProductInfoMap(offerIds, account, { continueOnError: true }),
        getOzonStockMap(offerIds, account, { continueOnError: true }),
        getOzonPriceMap(offerIds, account, { continueOnError: true }),
      ]);
      const missingProductIds = accountProducts
        .filter((product) => !getOzonOfferMapValue(infoMap, product.offerId))
        .map((product) => cleanText(product.productId || product.ozon?.productId))
        .filter(Boolean);
      if (missingProductIds.length) {
        const infoByProductId = await getOzonProductInfoMapByProductIds(missingProductIds, account, { continueOnError: true });
        for (const product of accountProducts) {
          const info = infoByProductId.get(cleanText(product.productId || product.ozon?.productId));
          if (info) setOzonOfferMapValue(infoMap, product.offerId || info.offer_id || info.offerId, info);
        }
      }
      for (const product of accountProducts) {
        const info = getOzonOfferMapValue(infoMap, product.offerId) || {};
        const stockInfo = getOzonOfferMapValue(stockMap, product.offerId) || {};
        const priceInfo = getOzonOfferMapValue(priceMap, product.offerId) || {};
        if (!Object.keys(info).length && !Object.keys(stockInfo).length && !Object.keys(priceInfo).length) continue;
        updatedById.set(product.id, applyOzonInfoToWarehouseProduct(product, info, account, stockInfo, priceInfo));
      }
    } catch (error) {
      logger.warn("warehouse page Ozon detail auto-enrich failed", { account: account.id, detail: error?.message || String(error) });
    }
  }

  if (!updatedById.size) return products;
  try {
    await writeWarehouseProductPatch(Array.from(updatedById.values()), { reason: "warehouse_page_ozon_auto_enrich", writeLinks: false });
  } catch (error) {
    logger.warn("warehouse page Ozon detail auto-enrich persist failed", { detail: error?.message || String(error) });
  }
  return products.map((product) => updatedById.get(product.id) || product);
}

function stoppedSupplierMap(suppliers = []) {
  return new Map(
    suppliers
      .filter((supplier) => supplier.stopped && supplier.name)
      .map((supplier) => [normalizeSupplierName(supplier.name), supplier]),
  );
}

function managedSupplierMaps(suppliers = []) {
  const byName = new Map();
  const byPartnerId = new Map();
  for (const supplier of suppliers || []) {
    const normalized = normalizeManagedSupplier(supplier);
    if (normalized.name) byName.set(normalizeSupplierName(normalized.name), normalized);
    if (normalized.partnerId) byPartnerId.set(String(normalized.partnerId), normalized);
  }
  return { byName, byPartnerId };
}

function findManagedSupplierForPriceMasterRow(row = {}, maps = managedSupplierMaps()) {
  return maps.byPartnerId.get(String(row.partnerId || "")) || maps.byName.get(normalizeSupplierName(row.partnerName)) || null;
}

function priceMasterSupplierPricingMeta(row = {}, maps = managedSupplierMaps()) {
  const supplier = findManagedSupplierForPriceMasterRow(row, maps);
  const pricingMode = normalizeSupplierPricingMode(supplier || {});
  const stockOnly = pricingMode === "stock_only";
  return {
    managedSupplier: supplier || null,
    pricingMode,
    stockOnly,
    priceEligible: !stockOnly,
    stockEligible: true,
    trustFactor: normalizeSupplierTrustFactor(supplier?.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(supplier?.orderCutoffTime || ""),
    reseller: Boolean(supplier?.reseller),
  };
}

function supplierOrderCutoffPassed(orderCutoffTime = "", now = new Date()) {
  const cutoff = normalizeSupplierOrderCutoff(orderCutoffTime);
  if (!cutoff) return false;
  const [hour, minute] = cutoff.split(":").map((item) => Number(item) || 0);
  const moscow = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Moscow",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const currentHour = Number(moscow.find((part) => part.type === "hour")?.value || 0);
  const currentMinute = Number(moscow.find((part) => part.type === "minute")?.value || 0);
  return currentHour * 60 + currentMinute > hour * 60 + minute;
}

function supplierCartOrderScore(row = {}, now = new Date()) {
  const price = Number(row.price || Number.POSITIVE_INFINITY);
  const trust = normalizeSupplierTrustFactor(row.trustFactor, 100);
  const trustPenalty = (100 - trust) / 100 * 0.18;
  const resellerPenalty = row.reseller ? 0.12 : 0;
  const cutoffPenalty = supplierOrderCutoffPassed(row.orderCutoffTime, now) ? 1000 : 0;
  return price * (1 + trustPenalty + resellerPenalty) + cutoffPenalty;
}

function resolvePriceMasterRowCurrency(row = {}, link = {}, maps = managedSupplierMaps()) {
  const supplier = findManagedSupplierForPriceMasterRow(row, maps);
  return supplier?.priceCurrency || link.priceCurrency || "USD";
}

async function getPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();

  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const snapshotIndexes = await getPriceMasterSnapshotIndexes();

  const map = new Map();
  for (const link of normalizedLinks) {
    let candidateRows = [];
    if (link.matchType === "article") {
      candidateRows = snapshotIndexes.byArticle.get(link.article) || [];
    } else if (link.matchType === "selected_row" && link.sourceRowId) {
      candidateRows = snapshotIndexes.byRowId.get(cleanText(link.sourceRowId)) || [];
    } else {
      const exactName = cleanText(link.exactName || link.article).toLowerCase();
      candidateRows = exactName ? (snapshotIndexes.byName.get(exactName) || []) : snapshotIndexes.rows;
    }
    const matches = candidateRows
      .filter((row) => priceMasterRowMatchesLink(row, link))
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
        const pricingMeta = priceMasterSupplierPricingMeta(row, supplierMaps);
        const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps);
        const normalizedPrice = normalizePriceMasterPrice(row.price, usdRate, priceCurrency);
        const price = stoppedSupplier ? 0 : normalizedPrice.price;
        const active = stoppedSupplier ? false : Boolean(row.active);
        return {
          ...link,
          rowId: row.rowId,
          article: row.article,
          name: row.name,
          partnerId: row.partnerId,
          partnerName: row.partnerName,
          price,
          priceCurrency,
          originalPrice: normalizedPrice.originalPrice,
          sourceCurrency: normalizedPrice.sourceCurrency,
          convertedFromRub: normalizedPrice.convertedFromRub,
          priceSource: "snapshot",
          active,
          stopped: Boolean(stoppedSupplier),
          stopReason: stoppedSupplier?.note || null,
          pricingMode: pricingMeta.pricingMode,
          stockOnly: pricingMeta.stockOnly,
          priceEligible: pricingMeta.priceEligible,
          stockEligible: pricingMeta.stockEligible,
          trustFactor: pricingMeta.trustFactor,
          orderCutoffTime: pricingMeta.orderCutoffTime,
          reseller: pricingMeta.reseller,
          available: active && (pricingMeta.stockOnly || price > 0),
          docDate: row.docDate,
        };
      });
    map.set(link.id, matches);
  }

  return map;
}

async function findPriceMasterRowsForLink(linkInput, usdRate, managedSuppliers = []) {
  const link = normalizeWarehouseLink(linkInput);
  if (!link.article && !link.exactName && !link.sourceRowId) return [];
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const conditions = ["r.Ignored = 0"];
  const params = [];
  if (link.matchType === "selected_row" && link.sourceRowId) {
    conditions.push("r.RowID = ?");
    params.push(Number(link.sourceRowId));
  } else if (link.matchType === "selected_row" || link.matchType === "exact_name") {
    conditions.push("r.NativeName IS NOT NULL AND TRIM(r.NativeName) <> ''");
    if (link.exactName || link.article) {
      conditions.push("LOWER(TRIM(r.NativeName)) = LOWER(TRIM(?))");
      params.push(link.exactName || link.article);
    }
  } else {
    conditions.push("BINARY TRIM(r.NativeID) = BINARY ?");
    params.push(link.article);
  }
  if (link.partnerId) {
    conditions.push("d.PartnerID = ?");
    params.push(Number(link.partnerId));
  }
  const [rows] = await pool.query(
    `
    SELECT
      r.NativeID AS article,
      r.NativeName AS name,
      r.NativePrice AS price,
      r.Active AS active,
      r.Ignored AS ignored,
      r.RowID AS rowId,
      d.DocDate AS docDate,
      d.PartnerID AS partnerId,
      p.PartnerName AS partnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE ${conditions.join(" AND ")}
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 200
    `,
    params,
  );
  return rows
    .filter((row) => priceMasterRowMatchesLink(row, link))
    .map((row) => {
      const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps);
      const pricingMeta = priceMasterSupplierPricingMeta(row, supplierMaps);
      const priceData = normalizePriceMasterPrice(row.price, usdRate, priceCurrency);
      return {
        ...row,
        priceCurrency,
        ...priceData,
        pricingMode: pricingMeta.pricingMode,
        stockOnly: pricingMeta.stockOnly,
        priceEligible: pricingMeta.priceEligible,
        stockEligible: pricingMeta.stockEligible,
        trustFactor: pricingMeta.trustFactor,
        orderCutoffTime: pricingMeta.orderCutoffTime,
        reseller: pricingMeta.reseller,
        active: Boolean(row.active),
        ignored: Boolean(row.ignored),
        available: Boolean(row.active) && (pricingMeta.stockOnly || Number(priceData.price || 0) > 0),
      };
    });
}

function priceMasterSnapshotLinkRow(row = {}, link = {}, usdRate, supplierMaps = managedSupplierMaps()) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const base = {
    article: cleanText(row.article || raw.article || raw.NativeID || raw.offerId || ""),
    name: cleanText(row.nativeName || raw.name || raw.NativeName || ""),
    price: row.price ?? raw.price ?? raw.NativePrice ?? 0,
    active: row.active !== false,
    ignored: Boolean(raw.ignored || raw.Ignored),
    rowId: cleanText(row.rowId || raw.rowId || raw.RowID || row.id),
    docDate: row.docDate instanceof Date ? row.docDate.toISOString() : cleanText(row.docDate || raw.docDate || raw.DocDate || ""),
    partnerId: cleanText(row.partnerId || raw.partnerId || raw.PartnerID || ""),
    partnerName: cleanText(row.partnerName || raw.partnerName || raw.PartnerName || raw.name || ""),
  };
  const priceCurrency = resolvePriceMasterRowCurrency(base, link, supplierMaps);
  const pricingMeta = priceMasterSupplierPricingMeta(base, supplierMaps);
  const priceData = normalizePriceMasterPrice(base.price, usdRate, priceCurrency);
  return {
    ...base,
    priceCurrency,
    ...priceData,
    priceSource: "postgres_snapshot",
    pricingMode: pricingMeta.pricingMode,
    stockOnly: pricingMeta.stockOnly,
    priceEligible: pricingMeta.priceEligible,
    stockEligible: pricingMeta.stockEligible,
    trustFactor: pricingMeta.trustFactor,
    orderCutoffTime: pricingMeta.orderCutoffTime,
    reseller: pricingMeta.reseller,
    available: Boolean(base.active) && (pricingMeta.stockOnly || Number(priceData.price || 0) > 0),
  };
}

async function findPriceMasterSnapshotRowsForLink(linkInput, usdRate, managedSuppliers = []) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return [];

  const and = [];
  if (link.matchType === "selected_row" && link.sourceRowId) {
    and.push({ rowId: cleanText(link.sourceRowId) });
  } else if (link.matchType === "selected_row" || link.matchType === "exact_name") {
    const exactName = cleanText(link.exactName || link.article);
    if (exactName) and.push({ nativeName: { equals: exactName, mode: "insensitive" } });
  } else {
    and.push({ article: cleanText(link.article) });
  }
  if (link.partnerId) and.push({ partnerId: cleanText(link.partnerId) });
  if (!and.length) return [];

  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const rows = await prisma.priceMasterSnapshotItem.findMany({
    where: { AND: and },
    orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
    take: 200,
  });
  return rows
    .map((row) => priceMasterSnapshotLinkRow(row, link, usdRate, supplierMaps))
    .filter((row) => priceMasterRowMatchesLink(row, link));
}

async function getLivePriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();
  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const map = new Map();
  for (const link of normalizedLinks) {
    const rows = await findPriceMasterRowsForLink(link, usdRate, managedSuppliers);
    map.set(link.id, rows.map((row) => {
      const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
      const price = stoppedSupplier ? 0 : row.price;
      const active = stoppedSupplier ? false : Boolean(row.active);
      return {
        ...link,
        rowId: row.rowId,
        article: row.article,
        name: row.name,
        partnerId: row.partnerId,
        partnerName: row.partnerName,
        price,
        priceCurrency: row.priceCurrency,
        originalPrice: row.originalPrice,
        sourceCurrency: row.sourceCurrency,
        convertedFromRub: row.convertedFromRub,
        priceSource: "live",
        active,
        stopped: Boolean(stoppedSupplier),
        stopReason: stoppedSupplier?.note || null,
        pricingMode: row.pricingMode,
        stockOnly: row.stockOnly,
        priceEligible: row.priceEligible,
        stockEligible: row.stockEligible,
        trustFactor: row.trustFactor,
        orderCutoffTime: row.orderCutoffTime,
        reseller: row.reseller,
        available: active && (row.stockOnly || price > 0),
        docDate: row.docDate,
      };
    }));
  }
  return map;
}

function priceMasterLinkLookupCacheKey(linkInput, usdRate) {
  const link = normalizeWarehouseLink(linkInput);
  return [
    String(link.matchType || "article"),
    cleanText(link.article).toLowerCase(),
    cleanText(link.exactName).toLowerCase(),
    cleanText(link.sourceRowId),
    cleanText(link.partnerId),
    normalizeSupplierName(link.supplierName),
    cleanText(link.keyword).toLowerCase(),
    cleanText(link.priceCurrency || "USD").toUpperCase(),
    Number(usdRate || 0).toFixed(4),
  ].join("|");
}

function setPriceMasterLinkLookupCache(key, rows) {
  if (!key) return;
  if (priceMasterLinkLookupCache.size >= priceMasterLinkLookupCacheMax) {
    const oldest = priceMasterLinkLookupCache.keys().next().value;
    if (oldest) priceMasterLinkLookupCache.delete(oldest);
  }
  priceMasterLinkLookupCache.set(key, { at: Date.now(), rows: Array.isArray(rows) ? rows : [] });
}

function getPriceMasterSearchCache(key) {
  const cached = priceMasterSearchCache.get(key);
  if (!cached || Date.now() - cached.at >= priceMasterSearchCacheTtlMs) return null;
  return Array.isArray(cached.value) ? cloneAuditValue(cached.value) : cached.value;
}

function setPriceMasterSearchCache(key, value) {
  if (!key) return;
  if (priceMasterSearchCache.size >= priceMasterSearchCacheMax) {
    const oldest = priceMasterSearchCache.keys().next().value;
    if (oldest) priceMasterSearchCache.delete(oldest);
  }
  priceMasterSearchCache.set(key, { at: Date.now(), value: cloneAuditValue(value) });
}

async function findPriceMasterRowsForLinkFast(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return [];
  const key = priceMasterLinkLookupCacheKey(link, usdRate);
  const cached = priceMasterLinkLookupCache.get(key);
  if (cached && Date.now() - cached.at < priceMasterLinkLookupCacheTtlMs) {
    if (cached.rows.length || options.live === false) return cached.rows;
  }

  const snapshotMap = await getPriceMasterMatchesForLinks([link], managedSuppliers, usdRate).catch((error) => {
    logger.warn("PriceMaster snapshot link lookup skipped", { detail: error?.message || String(error) });
    return new Map();
  });
  const snapshotRows = snapshotMap.get(link.id) || [];
  if (snapshotRows.length) {
    setPriceMasterLinkLookupCache(key, snapshotRows);
    return snapshotRows;
  }

  const postgresRows = await findPriceMasterSnapshotRowsForLink(link, usdRate, managedSuppliers).catch((error) => {
    logger.warn("PriceMaster postgres snapshot link lookup skipped", { detail: error?.message || String(error) });
    return null;
  });
  if (postgresRows?.length || options.live === false) {
    const rows = postgresRows || [];
    setPriceMasterLinkLookupCache(key, rows);
    return rows;
  }

  const timeoutMs = Math.max(250, Number(options.timeoutMs || process.env.LINK_SAVE_PM_TIMEOUT_MS || 1200));
  try {
    const liveRows = await Promise.race([
      findPriceMasterRowsForLink(link, usdRate, managedSuppliers),
      promiseTimeout(timeoutMs, "link_save_pm_timeout"),
    ]);
    setPriceMasterLinkLookupCache(key, liveRows);
    return liveRows;
  } catch (error) {
    logger.warn("PriceMaster link lookup skipped", {
      article: link.article,
      matchType: link.matchType,
      detail: error?.message || String(error),
    });
    if (options.cacheEmpty !== false) setPriceMasterLinkLookupCache(key, []);
    return [];
  }
}

async function getBatchPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate, { timeoutMs } = {}) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article || link.exactName || link.sourceRowId);
  if (!normalizedLinks.length) return new Map();
  const specialLinks = normalizedLinks.filter((link) => link.matchType !== "article");
  const articleLinks = normalizedLinks.filter((link) => link.matchType === "article" && link.article);
  const map = new Map();
  if (specialLinks.length) {
    for (const link of specialLinks) {
      map.set(link.id, await findPriceMasterRowsForLink(link, usdRate, managedSuppliers));
    }
  }
  if (!articleLinks.length) return map;
  const articles = Array.from(new Set(articleLinks.map((link) => link.article))).slice(0, 500);
  const placeholders = articles.map(() => "?").join(",");
  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const [rows] = await pool.query({
    sql: `
    SELECT
      r.NativeID AS article,
      r.NativeName AS name,
      r.NativePrice AS price,
      r.Active AS active,
      r.Ignored AS ignored,
      r.RowID AS rowId,
      d.DocDate AS docDate,
      d.PartnerID AS partnerId,
      p.PartnerName AS partnerName
    FROM OfferRows r
    JOIN OfferDocs d ON d.DocID = r.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE BINARY TRIM(r.NativeID) IN (${placeholders}) AND r.Ignored = 0
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 5000
    `,
    values: articles,
    timeout: Math.max(250, Number(timeoutMs || process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 1500)),
  });
  const rowsByArticle = new Map();
  for (const row of rows || []) {
    const article = cleanText(row.article);
    if (!rowsByArticle.has(article)) rowsByArticle.set(article, []);
    rowsByArticle.get(article).push(row);
  }
  for (const link of articleLinks) {
    const matches = (rowsByArticle.get(link.article) || [])
      .filter((row) => priceMasterRowMatchesLink(row, link))
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
        const pricingMeta = priceMasterSupplierPricingMeta(row, supplierMaps);
        const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps);
        const normalizedPrice = normalizePriceMasterPrice(row.price, usdRate, priceCurrency);
        const price = stoppedSupplier ? 0 : normalizedPrice.price;
        const active = stoppedSupplier ? false : Boolean(row.active);
        return {
          ...link,
          rowId: row.rowId,
          article: row.article,
          name: row.name,
          partnerId: row.partnerId,
          partnerName: row.partnerName,
          price,
          priceCurrency,
          originalPrice: normalizedPrice.originalPrice,
          sourceCurrency: normalizedPrice.sourceCurrency,
          convertedFromRub: normalizedPrice.convertedFromRub,
          priceSource: "live",
          active,
          stopped: Boolean(stoppedSupplier),
          stopReason: stoppedSupplier?.note || null,
          pricingMode: pricingMeta.pricingMode,
          stockOnly: pricingMeta.stockOnly,
          priceEligible: pricingMeta.priceEligible,
          stockEligible: pricingMeta.stockEligible,
          trustFactor: pricingMeta.trustFactor,
          orderCutoffTime: pricingMeta.orderCutoffTime,
          reseller: pricingMeta.reseller,
          available: active && (pricingMeta.stockOnly || price > 0),
          docDate: row.docDate,
        };
      });
    map.set(link.id, matches);
  }
  return map;
}

async function assertPriceMasterLinkExists(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  const matches = await findPriceMasterRowsForLinkFast(link, usdRate, managedSuppliers, options);
  if (matches.length) return matches;
  const articleRows = await findPriceMasterRowsForLinkFast({ ...link, supplierName: "", partnerId: "", keyword: "" }, usdRate, managedSuppliers, options);
  const label = link.matchType === "article"
    ? `артикул "${link.article}" должен совпадать с PriceMaster точно`
    : `выбранная строка "${link.exactName || link.article || link.sourceRowId}" должна существовать в PriceMaster`;
  const detailParts = [label];
  if (!articleRows.length) {
    detailParts.push(link.matchType === "article" ? "в PriceMaster нет строки с таким точным артикулом" : "строка PriceMaster не найдена");
  } else {
    if (link.supplierName) detailParts.push(`поставщик должен быть "${link.supplierName}"`);
    if (link.keyword) detailParts.push(`название должно содержать ключ "${link.keyword}"`);
  }
  const error = new Error(`Привязка не сохранена: ${detailParts.join(", ")}.`);
  error.statusCode = 400;
  error.code = "PM_LINK_NOT_FOUND";
  error.matches = articleRows.slice(0, 10).map((row) => ({
    article: row.article,
    name: row.name,
    partnerName: row.partnerName,
    price: row.price,
    active: row.active,
  }));
  throw error;
}

function priceMasterLinkValidationFailure(error, linkInput = {}, index = 0) {
  const link = normalizeWarehouseLink(linkInput);
  return {
    index,
    article: link.article,
    exactName: link.exactName,
    sourceRowId: link.sourceRowId,
    supplierName: link.supplierName,
    partnerId: link.partnerId,
    priceCurrency: link.priceCurrency,
    code: error?.code || "PM_LINK_NOT_FOUND",
    detail: error?.message || String(error),
    matches: Array.isArray(error?.matches) ? error.matches : [],
  };
}

async function resolvePriceMasterLinkForSave(linkInput, usdRate, managedSuppliers = [], options = {}) {
  const link = normalizeWarehouseLink(linkInput);
  if (!warehouseLinkHasMatchTarget(link)) return link;
  const matches = await findPriceMasterRowsForLinkFast(link, usdRate, managedSuppliers, options);
  if (matches.length) {
    const best = matches[0];
    return normalizeWarehouseLink({
      ...link,
      exactName: link.exactName || (link.matchType !== "article" ? best.name : ""),
      sourceRowId: link.sourceRowId || (link.matchType === "selected_row" ? best.rowId : ""),
      supplierName: link.supplierName || best.partnerName,
      partnerId: link.partnerId || best.partnerId,
    });
  }

  if (link.matchType === "article" && link.article) {
    const nameLink = normalizeWarehouseLink({
      ...link,
      article: "",
      matchType: "exact_name",
      exactName: link.exactName || link.article,
    });
    const nameMatches = await findPriceMasterRowsForLinkFast(nameLink, usdRate, managedSuppliers, options);
    if (nameMatches.length) {
      const best = nameMatches[0];
      return normalizeWarehouseLink({
        ...nameLink,
        supplierName: nameLink.supplierName || best.partnerName,
        partnerId: nameLink.partnerId || best.partnerId,
      });
    }
  }

  return link;
}

function pickWarehouseSupplier(matches) {
  return [...matches]
    .filter((match) => match.available && match.priceEligible !== false && match.stockOnly !== true)
    .sort(
      (a, b) =>
        Number(a.effectiveFinalPrice || a.calculatedPrice || 0) - Number(b.effectiveFinalPrice || b.calculatedPrice || 0)
        || Number(a.price || 0) - Number(b.price || 0)
        || String(b.docDate).localeCompare(String(a.docDate)),
    )[0] || null;
}

function pickWarehouseStockOnlySupplier(matches) {
  return [...matches]
    .filter((match) => match.available && (match.stockOnly === true || match.priceEligible === false))
    .sort(
      (a, b) =>
        String(b.docDate).localeCompare(String(a.docDate))
        || String(a.partnerName || a.supplierName || "").localeCompare(String(b.partnerName || b.supplierName || "")),
    )[0] || null;
}

function resolveMarkupCoefficient({ productMarkup, marketplace, supplierUsdPrice, appSettings }) {
  if (Number(productMarkup) > 0) return Number(productMarkup);
  const defaults = appSettings?.defaultMarkups || {};
  const fallback = marketplace === "ozon"
    ? Number(defaults.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
    : Number(defaults.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
  const usd = Number(supplierUsdPrice || 0);
  const rules = Array.isArray(appSettings?.markupRules) ? appSettings.markupRules : [];
  if (!Number.isFinite(usd) || usd <= 0 || !rules.length) return fallback;
  const scopedRules = rules.filter((rule) => !rule.marketplace || rule.marketplace === "all" || rule.marketplace === marketplace);
  if (!scopedRules.length) return fallback;
  const sorted = [...scopedRules].sort((a, b) => b.minUsd - a.minUsd);
  const matched = sorted.find((rule) => usd >= Number(rule.minUsd || 0));
  return Number(matched?.coefficient || fallback);
}

function resolveAvailabilityPolicy({ marketplace, availableSupplierCount = 0, baseMarkup = 0, appSettings } = {}) {
  const count = Math.max(0, Number(availableSupplierCount || 0));
  const rules = Array.isArray(appSettings?.availabilityRules) ? appSettings.availabilityRules : [];
  const scopedRules = rules.filter((rule) => !rule.marketplace || rule.marketplace === "all" || rule.marketplace === marketplace);
  const sorted = [...scopedRules].sort((a, b) => Number(b.minAvailableSuppliers || 0) - Number(a.minAvailableSuppliers || 0));
  const matched = sorted.find((rule) => count >= Number(rule.minAvailableSuppliers || 0)) || null;
  const base = Number(baseMarkup || 0);
  const delta = Number(matched?.coefficientDelta || 0);
  const markupCoefficient = base > 0 ? Math.max(0.0001, Number((base + delta).toFixed(4))) : base;
  const targetStock = matched ? Math.max(0, Math.round(Number(matched.targetStock || 0))) : null;
  return {
    rule: matched,
    baseMarkup: base,
    coefficientDelta: delta,
    markupCoefficient,
    targetStock,
  };
}

function enrichSupplierPriceCandidates(suppliers = [], {
  productMarkupOverride = 0,
  marketplace = "",
  rate = 0,
  appSettings = {},
  fallbackMarkup = 0,
  availableSupplierCount = 0,
  stockOnlyAvailableSupplierCount = 0,
} = {}) {
  const policySupplierCount = availableSupplierCount || stockOnlyAvailableSupplierCount;
  return (Array.isArray(suppliers) ? suppliers : []).map((supplier) => {
    const baseMarkupCoefficient = Number(productMarkupOverride || supplier.markupCoefficient || fallbackMarkup || 0);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace,
      availableSupplierCount: policySupplierCount,
      baseMarkup: baseMarkupCoefficient,
      appSettings,
    });
    const markupCoefficient = Number(availabilityPolicy.markupCoefficient || baseMarkupCoefficient);
    const priceEligible = supplier.priceEligible !== false && supplier.stockOnly !== true;
    const effectiveFinalPrice = supplier.available && priceEligible
      ? calculateRubPrice(supplier.price, rate, markupCoefficient)
      : null;
    return {
      ...supplier,
      baseMarkupCoefficient,
      markupCoefficient,
      effectiveMarkupCoefficient: markupCoefficient,
      availabilityRule: availabilityPolicy.rule,
      prePolicyCalculatedPrice: Number(supplier.calculatedPrice || 0) || null,
      calculatedPrice: effectiveFinalPrice || supplier.calculatedPrice || null,
      effectiveFinalPrice,
      priceSelectionReason: effectiveFinalPrice
        ? "cheapest_effective_final_price"
        : (supplier.stockOnly || supplier.priceEligible === false ? "stock_only_excluded" : "not_available"),
    };
  });
}

function supplierAlternativesForDiagnostics(suppliers = [], limit = 5) {
  return [...(Array.isArray(suppliers) ? suppliers : [])]
    .sort((a, b) =>
      Number(a.effectiveFinalPrice || a.calculatedPrice || Number.MAX_SAFE_INTEGER)
      - Number(b.effectiveFinalPrice || b.calculatedPrice || Number.MAX_SAFE_INTEGER)
      || Number(a.price || 0) - Number(b.price || 0)
      || String(b.docDate).localeCompare(String(a.docDate)))
    .slice(0, Math.max(1, Number(limit || 5) || 5))
    .map((supplier) => ({
      partnerName: supplier.partnerName || supplier.supplierName || "",
      supplierName: supplier.supplierName || supplier.partnerName || "",
      partnerId: supplier.partnerId || "",
      article: supplier.article || "",
      rowId: supplier.rowId || "",
      available: Boolean(supplier.available),
      active: supplier.active !== false,
      stopped: Boolean(supplier.stopped),
      stockOnly: Boolean(supplier.stockOnly || supplier.priceEligible === false),
      priceEligible: supplier.priceEligible !== false && supplier.stockOnly !== true,
      price: supplier.price,
      originalPrice: supplier.originalPrice,
      priceCurrency: supplier.priceCurrency || supplier.currency || "",
      sourceCurrency: supplier.sourceCurrency || supplier.priceCurrency || "",
      markupCoefficient: supplier.markupCoefficient,
      baseMarkupCoefficient: supplier.baseMarkupCoefficient,
      effectiveFinalPrice: supplier.effectiveFinalPrice || supplier.calculatedPrice || null,
      calculatedPrice: supplier.calculatedPrice || null,
      priceSource: supplier.priceSource || supplier.source || "snapshot",
      exclusionReason: supplier.available
        ? (supplier.stockOnly || supplier.priceEligible === false ? "stock_only_excluded" : null)
        : (supplier.stopped ? "supplier_stopped" : "not_available"),
    }));
}

function storedMarketplacePrice(product = {}) {
  const ozonPrice = Number(product.ozon?.price || 0);
  const yandexPrice = Number(product.yandex?.price || 0);
  return Number(product.marketplacePrice || 0) || (product.marketplace === "ozon" ? ozonPrice : yandexPrice) || null;
}

async function getWarehousePriceMaps(products, { refresh = false } = {}) {
  const result = new Map();
  let mutated = false;
  for (const product of products) result.set(product.id, storedMarketplacePrice(product));
  if (!refresh) return { map: result, mutated };

  for (const account of getOzonAccounts()) {
    const accountProducts = products.filter((product) => product.target === account.id && product.marketplace === "ozon");
    const ozonOfferIds = accountProducts.map((product) => product.offerId).filter(Boolean);
    if (!ozonOfferIds.length) continue;
    try {
      const priceMap = await getOzonPriceMap(ozonOfferIds, account);
      for (const product of accountProducts) {
        const raw = priceMap.get(product.offerId);
        const details = normalizeOzonPriceDetails(raw || {});
        const listed = pickOzonCabinetListedPrice(details);
        const fallback = storedMarketplacePrice(product);
        const value = listed ?? fallback;
        result.set(product.id, value);
        if (listed != null) {
          if (product.marketplacePrice !== listed) {
            product.marketplacePrice = listed;
            mutated = true;
          }
          const oz = product.ozon || {};
          product.ozon = {
            ...oz,
            price: listed,
            minPrice: details.minPrice ?? oz.minPrice,
            oldPrice: details.oldPrice ?? oz.oldPrice,
            marketingSellerPrice: details.marketingSellerPrice ?? oz.marketingSellerPrice,
            marketingPrice: details.marketingPrice ?? oz.marketingPrice,
            retailPrice: details.retailPrice ?? oz.retailPrice,
          };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  for (const shop of getYandexShops()) {
    const shopProducts = products.filter((product) => product.target === shop.id);
    const offerIds = shopProducts.map((product) => product.offerId).filter(Boolean);
    if (!offerIds.length) continue;
    try {
      const priceMap = await getYandexPriceMap(shop, offerIds);
      for (const product of shopProducts) {
        const yPrice = Number(priceMap.get(product.offerId) || 0) || null;
        const fallback = storedMarketplacePrice(product);
        const value = yPrice ?? fallback;
        result.set(product.id, value);
        if (yPrice != null) {
          if (product.marketplacePrice !== yPrice) {
            product.marketplacePrice = yPrice;
            mutated = true;
          }
          product.yandex = { ...(product.yandex || {}), price: yPrice };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  return { map: result, mutated };
}

async function getWarehouseMinPriceMaps(products, { refresh = false } = {}) {
  const result = new Map();
  let mutated = false;
  for (const product of products) {
    result.set(product.id, Number(product.marketplaceMinPrice || product.ozon?.minPrice || 0) || null);
  }

  if (!refresh) return { map: result, mutated };

  for (const account of getOzonAccounts()) {
    const accountProducts = products.filter((product) => product.target === account.id && product.marketplace === "ozon");
    const ozonOfferIds = accountProducts.map((product) => product.offerId).filter(Boolean);
    if (!ozonOfferIds.length) continue;
    try {
      const priceMap = await getOzonPriceMap(ozonOfferIds, account);
      for (const product of accountProducts) {
        const details = normalizeOzonPriceDetails(priceMap.get(product.offerId) || {});
        const minPrice = details.minPrice || null;
        result.set(product.id, minPrice);
        if (minPrice !== null && product.marketplaceMinPrice !== minPrice) {
          product.marketplaceMinPrice = minPrice;
          product.ozon = { ...(product.ozon || {}), minPrice };
          mutated = true;
        }
      }
    } catch (_error) {
      // Keep stored min prices when Ozon request fails.
    }
  }

  return { map: result, mutated };
}

async function buildWarehouseView({ sync = false, usdRate, targetMarkups = {}, limit = Number.POSITIVE_INFINITY, refreshPrices = false, onProgress = null } = {}) {
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || (await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
  let warehouse = await readWarehouse();
  const supplierSync = { ok: false, partners: 0, imported: 0, changed: false, error: null };
  try {
    const partners = await listPriceMasterPartners();
    const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
    supplierSync.ok = true;
    supplierSync.partners = partners.length;
    supplierSync.imported = syncedSuppliers.imported;
    supplierSync.changed = syncedSuppliers.changed;
    if (syncedSuppliers.changed) {
      await writeWarehouse(warehouse);
      logger.info("imported suppliers from PriceMaster", { imported: syncedSuppliers.imported });
    }
  } catch (error) {
    supplierSync.error = error.message;
    logger.warn("supplier import from PriceMaster failed", { detail: error.message });
  }
  const autoReactivated = applySupplierAutoReactivate(warehouse);
  if (autoReactivated.length) {
    await writeWarehouse(warehouse);
    logger.info("supplier auto-reactivated by date", { count: autoReactivated.length, suppliers: autoReactivated });
  }
  let syncWarnings = [];
  let marketplaceSyncChangedProductIds = [];
  if (sync) {
    const beforeSyncProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
    const synced = await syncWarehouseProductsFromMarketplaces(warehouse, limit, { onProgress });
    warehouse = synced.warehouse;
    marketplaceSyncChangedProductIds = changedWarehouseProductIdsByAutomationFingerprint(
      beforeSyncProducts,
      warehouse.products || [],
    );
    syncWarnings = synced.warnings || [];
    onProgress?.({
      percent: 76,
      stage: "Запись склада",
      meta: `Сохраняю ${formatRuNumber(warehouse.products?.length || 0)} карточек склада.`,
      processed: Number(warehouse.products?.length || 0),
      total: Number(warehouse.products?.length || 0),
    });
    await writeWarehouse(warehouse);
    syncWarnings.forEach((detail) => logger.warn("warehouse sync warning", { detail }));
  }

  const links = warehouse.products.flatMap((product) => product.links || []);
  let matchMap = new Map();
  let sourceError = null;
  try {
    matchMap = await getPriceMasterMatchesForLinks(links, warehouse.suppliers, rate);
  } catch (error) {
    sourceError = error.code || error.message;
  }

  const [priceMapResult, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(warehouse.products, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(warehouse.products, { refresh: refreshPrices }),
  ]);
  const priceMap = priceMapResult.map;
  const minPriceMap = minPriceResult.map;
  if (refreshPrices && (priceMapResult.mutated || minPriceResult.mutated)) {
    await writeWarehouse(warehouse);
  }
  const products = warehouse.products.filter(isWarehouseProductTargetEnabled).map((product) => {
    const productMarkupOverride = marketplaceProductMarkupOverride(product);
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const rawSuppliers = normalizedLinks.flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => ({
        ...match,
        markupCoefficient: resolveMarkupCoefficient({
          productMarkup: productMarkupOverride,
          marketplace: product.marketplace,
          supplierUsdPrice: match.price,
          appSettings: {
            ...appSettings,
            defaultMarkups: {
              ozon: Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7),
              yandex: Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
            },
          },
        }),
        calculatedPrice: calculateRubPrice(
          match.price,
          rate,
          resolveMarkupCoefficient({
            productMarkup: productMarkupOverride,
            marketplace: product.marketplace,
            supplierUsdPrice: match.price,
            appSettings: {
              ...appSettings,
              defaultMarkups: {
                ozon: Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7),
                yandex: Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
              },
            },
          }),
        ),
      })),
    );
    const links = normalizedLinks.map((link) => {
      const matched = matchMap.get(link.id) || [];
      return {
        ...link,
        matchedCount: matched.length,
        availableCount: matched.filter((item) => item.available).length,
        priceEligibleCount: matched.filter((item) => item.available && item.priceEligible !== false && item.stockOnly !== true).length,
        stockOnlyCount: matched.filter((item) => item.available && (item.stockOnly === true || item.priceEligible === false)).length,
        stockOnly: matched.some((item) => item.stockOnly === true || item.priceEligible === false),
        priceEligible: matched.some((item) => item.priceEligible !== false && item.stockOnly !== true),
        missingInPriceMaster: matched.length === 0,
      };
    });
    const availableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && supplier.priceEligible !== false && supplier.stockOnly !== true).length;
    const stockOnlyAvailableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && (supplier.stockOnly === true || supplier.priceEligible === false)).length;
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const suppliers = enrichSupplierPriceCandidates(rawSuppliers, {
      productMarkupOverride,
      marketplace: product.marketplace,
      rate,
      appSettings,
      fallbackMarkup,
      availableSupplierCount,
      stockOnlyAvailableSupplierCount,
    });
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const stockOnlySupplier = selectedSupplier ? null : pickWarehouseStockOnlySupplier(suppliers);
    const stockOnlyFallbackActive = Boolean(!selectedSupplier && stockOnlySupplier);
    const stockOnlyManualPrice = stockOnlyFallbackActive ? stockOnlyManualPriceForProduct(product) : null;
    const baseMarkupCoefficient = Number(productMarkupOverride || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace: product.marketplace,
      availableSupplierCount: availableSupplierCount || stockOnlyAvailableSupplierCount,
      baseMarkup: baseMarkupCoefficient,
      appSettings,
    });
    const markupCoefficient = Number(availabilityPolicy.markupCoefficient || baseMarkupCoefficient);
    const selectedSupplierWithPolicy = selectedSupplier
      ? {
          ...selectedSupplier,
          baseMarkupCoefficient,
          markupCoefficient,
          availabilityRule: availabilityPolicy.rule,
          calculatedPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient)),
          effectiveFinalPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient)),
        }
      : (stockOnlySupplier
          ? {
              ...stockOnlySupplier,
              baseMarkupCoefficient,
              markupCoefficient,
              availabilityRule: availabilityPolicy.rule,
              calculatedPrice: null,
              manualPrice: stockOnlyManualPrice,
            }
          : null);
    const rawNextPrice = selectedSupplier && selectedSupplierWithPolicy
      ? Number(selectedSupplierWithPolicy.calculatedPrice || calculateRubPrice(selectedSupplierWithPolicy.price, rate, markupCoefficient))
      : (stockOnlyManualPrice || 0);
    const minAuto = Number(product.autoPriceMin || 0);
    const maxAuto = Number(product.autoPriceMax || 0);
    const persistedCurrentPrice = Number(product.currentPrice || product.marketplacePrice || 0) || null;
    const persistedNextPrice = Number(product.targetPrice || product.nextPrice || 0) || 0;
    let nextPrice = rawNextPrice;
    if (!stockOnlyFallbackActive && (!Number.isFinite(nextPrice) || nextPrice <= 0) && product.marketplace === "yandex" && persistedNextPrice > 0) {
      nextPrice = persistedNextPrice;
    }
    if (nextPrice > 0 && minAuto > 0 && nextPrice < minAuto) nextPrice = minAuto;
    if (nextPrice > 0 && maxAuto > 0 && nextPrice > maxAuto) nextPrice = maxAuto;
    const currentPrice = priceMap.get(product.id) || persistedCurrentPrice;
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;

    return {
      ...product,
      brand: resolveWarehouseBrand(product),
      markupCoefficient,
      usdRate: rate,
      priceFormula: {
        marketplace: product.marketplace,
        usdRate: rate,
        selectedSupplierPrice: selectedSupplierWithPolicy && !stockOnlyFallbackActive ? Number(selectedSupplierWithPolicy.price || 0) : null,
        selectedSupplierCurrency: selectedSupplierWithPolicy?.priceCurrency || selectedSupplierWithPolicy?.currency || "",
        baseMarkupCoefficient,
        markupCoefficient,
        calculatedPrice: rawNextPrice || null,
        targetPrice: nextPrice || null,
        currentPrice,
        availabilityRule: availabilityPolicy.rule || null,
        targetStock: availabilityPolicy.targetStock ?? null,
        stockOnlyFallbackActive,
        stockOnlyManualPrice,
        priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (sourceError ? "timeout" : "snapshot")),
        priceSelectionReason: selectedSupplierWithPolicy?.priceSelectionReason || null,
      },
      autoPriceEnabled: normalizedLinks.length > 0 ? true : product.autoPriceEnabled !== false,
      autoPriceMin: minAuto > 0 ? minAuto : null,
      autoPriceMax: maxAuto > 0 ? maxAuto : null,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplierWithPolicy && (nextPrice > 0 || stockOnlyFallbackActive)),
      selectedSupplier: selectedSupplierWithPolicy,
      fallbackSuppliers: suppliers
        .filter((supplier) => supplier.available)
        .slice(0, 3)
        .map((supplier) => ({
          partnerName: supplier.partnerName || supplier.supplierName || "",
          article: supplier.article || "",
          price: supplier.price,
          calculatedPrice: supplier.calculatedPrice,
          stockOnly: Boolean(supplier.stockOnly || supplier.priceEligible === false),
        })),
      stockOnlyFallbackActive,
      supplierAlternatives: supplierAlternativesForDiagnostics(suppliers, 5),
      stockOnlyManualPriceMissing: Boolean(stockOnlyFallbackActive && !stockOnlyManualPrice),
      stockOnlyManualPrices: normalizeStockOnlyManualPrices(product.stockOnlyManualPrices),
      stockOnlyAvailableSupplierCount,
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной рассчитанной ценой."
        : "Нет доступного поставщика.",
      priceSelectionReason: selectedSupplier ? "cheapest_effective_final_price" : (stockOnlyFallbackActive ? "stock_only_fallback" : "no_supplier"),
      priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (sourceError ? "timeout" : "snapshot")),
      links,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount,
      availabilityRule: availabilityPolicy.rule,
      targetStock: selectedSupplierWithPolicy
        ? availabilityPolicy.targetStock
        : (product.marketplace === "yandex" ? Number(product.targetStock || 0) || null : null),
      hasLinks: links.length > 0,
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : (stockOnlyFallbackActive ? (stockOnlyManualPrice ? "stock_only_fallback" : "stock_only_manual_price_missing") : "no_supplier"),
    };
  });

  return {
    createdAt: new Date().toISOString(),
    updatedAt: warehouse.updatedAt || warehouse.createdAt || null,
    usdRate: rate,
    sourceError,
    supplierSync,
    priceMaster: await getPriceMasterSnapshotMeta(),
    targets: marketplaceTargets(),
    suppliers: warehouse.suppliers,
    products,
    total: products.length,
    ready: products.filter((product) => product.ready).length,
    changed: products.filter((product) => product.changed).length,
    withoutSupplier: products.filter((product) => !product.selectedSupplier && Number(product.supplierCount || 0) > 0).length,
    linkedArchived: products.filter((product) => product.hasLinks && product.marketplace === "ozon" && product.marketplaceState?.code === "archived").length,
    ozonArchived: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "archived").length,
    ozonInactive: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "inactive").length,
    ozonOutOfStock: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "out_of_stock").length,
    noSupplierAlerts: buildNoSupplierAlerts(products, { limit: 12 }),
    autoArchiveAlerts: products
      .filter((product) => !product.hasLinks)
      .slice(0, 30)
      .map((product) => ({
        id: product.id,
        offerId: product.offerId,
        name: product.name,
        marketplace: product.marketplace,
        action: "Автоархив кандидат",
      })),
    syncWarnings,
    marketplaceSyncChangedProductIds,
    marketplaceSyncChanged: marketplaceSyncChangedProductIds.length,
  };
}

async function buildWarehouseViewCached(params = {}) {
  if (params.sync || params.refreshPrices) return buildWarehouseView(params);
  const key = warehouseViewCacheKey(params);
  const cached = warehouseViewCache.get(key);
  const ttlMs = warehouseViewCacheMs;
  if (cached && Date.now() - cached.at < ttlMs) return cached.data;
  const existingBuild = warehouseViewBuilds.get(key);
  if (existingBuild) return existingBuild;
  const build = buildWarehouseView(params)
    .then((data) => {
      lastWarehouseViewSnapshot = data;
      warehouseViewCache.set(key, { at: Date.now(), data });
      return data;
    })
    .finally(() => {
      warehouseViewBuilds.delete(key);
    });
  warehouseViewBuilds.set(key, build);
  return build;
}

async function buildFreshWarehouseProductsForWarehouse(warehouse, productIds = [], { refreshPrices = false, persistMutations = false, livePriceMaster = false, batchPriceMaster = false, usdRate } = {}) {
  const wanted = new Set((productIds || []).map((id) => String(id)));
  if (!wanted.size) return [];
  const appSettings = await readAppSettings();
  const rateSource = appSettings.fixedUsdRate || usdRate || (batchPriceMaster ? process.env.DEFAULT_USD_RATE : (await getUsdRate()).rate);
  const rate = Number(rateSource || process.env.DEFAULT_USD_RATE || 95);
  const productsToBuild = (warehouse.products || []).filter((product) => wanted.has(String(product.id)));
  if (!productsToBuild.length) return [];
  const links = productsToBuild.flatMap((product) => product.links || []);
  const priceMasterTimeoutMs = Number(process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 1500);
  let priceMasterSourceError = null;
  let matchMap = new Map();
  if (livePriceMaster) {
    if (batchPriceMaster) {
      matchMap = await Promise.race([
        getBatchPriceMasterMatchesForLinks(links, warehouse.suppliers, rate, { timeoutMs: priceMasterTimeoutMs }),
        promiseTimeout(priceMasterTimeoutMs + 100, "warehouse_page_pm_timeout"),
      ]).catch((error) => {
        priceMasterSourceError = error?.message || String(error);
        logger.warn("warehouse page PriceMaster enrichment skipped", { detail: priceMasterSourceError });
        return new Map();
      });
    } else {
      matchMap = await getLivePriceMasterMatchesForLinks(links, warehouse.suppliers, rate).catch((error) => {
        priceMasterSourceError = error?.message || String(error);
        logger.warn("warehouse live PriceMaster enrichment skipped", { detail: priceMasterSourceError });
        return new Map();
      });
    }
  } else {
    matchMap = await getPriceMasterMatchesForLinks(links, warehouse.suppliers, rate);
  }
  const [priceMapResult, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(productsToBuild, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(productsToBuild, { refresh: refreshPrices }),
  ]);
  if (persistMutations && (priceMapResult.mutated || minPriceResult.mutated)) await writeWarehouse(warehouse);
  const priceMap = priceMapResult.map;
  const minPriceMap = minPriceResult.map;

  return productsToBuild.map((product) => {
    const productMarkupOverride = marketplaceProductMarkupOverride(product);
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const rawSuppliers = normalizedLinks.flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => {
        const markupCoefficient = resolveMarkupCoefficient({
          productMarkup: productMarkupOverride,
          marketplace: product.marketplace,
          supplierUsdPrice: match.price,
          appSettings,
        });
        return {
          ...match,
          markupCoefficient,
          calculatedPrice: calculateRubPrice(match.price, rate, markupCoefficient),
        };
      }),
    );
    const links = normalizedLinks.map((link) => {
      const matched = matchMap.get(link.id) || [];
      return {
        ...link,
        matchedCount: matched.length,
        availableCount: matched.filter((item) => item.available).length,
        priceEligibleCount: matched.filter((item) => item.available && item.priceEligible !== false && item.stockOnly !== true).length,
        stockOnlyCount: matched.filter((item) => item.available && (item.stockOnly === true || item.priceEligible === false)).length,
        stockOnly: matched.some((item) => item.stockOnly === true || item.priceEligible === false),
        priceEligible: matched.some((item) => item.priceEligible !== false && item.stockOnly !== true),
        missingInPriceMaster: matched.length === 0,
      };
    });
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const availableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && supplier.priceEligible !== false && supplier.stockOnly !== true).length;
    const stockOnlyAvailableSupplierCount = rawSuppliers.filter((supplier) => supplier.available && (supplier.stockOnly === true || supplier.priceEligible === false)).length;
    const suppliers = enrichSupplierPriceCandidates(rawSuppliers, {
      productMarkupOverride,
      marketplace: product.marketplace,
      rate,
      appSettings,
      fallbackMarkup,
      availableSupplierCount,
      stockOnlyAvailableSupplierCount,
    });
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const stockOnlySupplier = selectedSupplier ? null : pickWarehouseStockOnlySupplier(suppliers);
    const stockOnlyFallbackActive = Boolean(!selectedSupplier && stockOnlySupplier);
    const stockOnlyManualPrice = stockOnlyFallbackActive ? stockOnlyManualPriceForProduct(product) : null;
    const baseMarkupCoefficient = Number(productMarkupOverride || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace: product.marketplace,
      availableSupplierCount: availableSupplierCount || stockOnlyAvailableSupplierCount,
      baseMarkup: baseMarkupCoefficient,
      appSettings,
    });
    const markupCoefficient = Number(availabilityPolicy.markupCoefficient || baseMarkupCoefficient);
    const selectedSupplierWithPolicy = selectedSupplier
      ? {
          ...selectedSupplier,
          baseMarkupCoefficient,
          markupCoefficient,
          availabilityRule: availabilityPolicy.rule,
          calculatedPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient)),
          effectiveFinalPrice: Number(selectedSupplier.effectiveFinalPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient)),
        }
      : (stockOnlySupplier
          ? {
              ...stockOnlySupplier,
              baseMarkupCoefficient,
              markupCoefficient,
              availabilityRule: availabilityPolicy.rule,
              calculatedPrice: null,
              manualPrice: stockOnlyManualPrice,
            }
          : null);
    const rawNextPrice = selectedSupplier && selectedSupplierWithPolicy
      ? Number(selectedSupplierWithPolicy.calculatedPrice || calculateRubPrice(selectedSupplierWithPolicy.price, rate, markupCoefficient))
      : (stockOnlyManualPrice || 0);
    const minAuto = Number(product.autoPriceMin || 0);
    const maxAuto = Number(product.autoPriceMax || 0);
    const persistedCurrentPrice = Number(product.currentPrice || product.marketplacePrice || 0) || null;
    const persistedNextPrice = Number(product.targetPrice || product.nextPrice || 0) || 0;
    let nextPrice = rawNextPrice;
    if (!stockOnlyFallbackActive && (!Number.isFinite(nextPrice) || nextPrice <= 0) && product.marketplace === "yandex" && persistedNextPrice > 0) {
      nextPrice = persistedNextPrice;
    }
    if (nextPrice > 0 && minAuto > 0 && nextPrice < minAuto) nextPrice = minAuto;
    if (nextPrice > 0 && maxAuto > 0 && nextPrice > maxAuto) nextPrice = maxAuto;
    const currentPrice = priceMap.get(product.id) || persistedCurrentPrice;
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;

    return {
      ...product,
      brand: resolveWarehouseBrand(product),
      markupCoefficient,
      usdRate: rate,
      priceFormula: {
        marketplace: product.marketplace,
        usdRate: rate,
        selectedSupplierPrice: selectedSupplierWithPolicy && !stockOnlyFallbackActive ? Number(selectedSupplierWithPolicy.price || 0) : null,
        selectedSupplierCurrency: selectedSupplierWithPolicy?.priceCurrency || selectedSupplierWithPolicy?.currency || "",
        baseMarkupCoefficient,
        markupCoefficient,
        calculatedPrice: rawNextPrice || null,
        targetPrice: nextPrice || null,
        currentPrice,
        availabilityRule: availabilityPolicy.rule || null,
        targetStock: availabilityPolicy.targetStock ?? null,
        stockOnlyFallbackActive,
        stockOnlyManualPrice,
        priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || "snapshot"),
        priceSelectionReason: selectedSupplierWithPolicy?.priceSelectionReason || null,
      },
      autoPriceEnabled: normalizedLinks.length > 0 ? true : product.autoPriceEnabled !== false,
      autoPriceMin: minAuto > 0 ? minAuto : null,
      autoPriceMax: maxAuto > 0 ? maxAuto : null,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplierWithPolicy && (nextPrice > 0 || stockOnlyFallbackActive)),
      selectedSupplier: selectedSupplierWithPolicy,
      fallbackSuppliers: suppliers
        .filter((supplier) => supplier.available)
        .slice(0, 3)
        .map((supplier) => ({
          partnerName: supplier.partnerName || supplier.supplierName || "",
          article: supplier.article || "",
          price: supplier.price,
          calculatedPrice: supplier.calculatedPrice,
          stockOnly: Boolean(supplier.stockOnly || supplier.priceEligible === false),
        })),
      stockOnlyFallbackActive,
      supplierAlternatives: supplierAlternativesForDiagnostics(suppliers, 5),
      stockOnlyManualPriceMissing: Boolean(stockOnlyFallbackActive && !stockOnlyManualPrice),
      stockOnlyManualPrices: normalizeStockOnlyManualPrices(product.stockOnlyManualPrices),
      stockOnlyAvailableSupplierCount,
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной расчётной ценой."
        : "Нет доступного поставщика.",
      priceSelectionReason: selectedSupplier ? "cheapest_effective_final_price" : (stockOnlyFallbackActive ? "stock_only_fallback" : "no_supplier"),
      priceSource: stockOnlyFallbackActive ? "stock_only_manual" : (selectedSupplierWithPolicy?.priceSource || (priceMasterSourceError ? "timeout" : "snapshot")),
      links,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount,
      availabilityRule: availabilityPolicy.rule,
      targetStock: selectedSupplierWithPolicy
        ? availabilityPolicy.targetStock
        : (product.marketplace === "yandex" ? Number(product.targetStock || 0) || null : null),
      hasLinks: links.length > 0,
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : (stockOnlyFallbackActive ? (stockOnlyManualPrice ? "stock_only_fallback" : "stock_only_manual_price_missing") : "no_supplier"),
    };
  });
}

function summarizeWarehouseCounterStats({ totalProducts = 0, linkedProducts = [], builtLinkedProducts = [] } = {}) {
  const linkedCount = Array.isArray(linkedProducts) ? linkedProducts.length : 0;
  const built = Array.isArray(builtLinkedProducts) ? builtLinkedProducts : [];
  const ready = built.filter((product) => product.ready).length;
  const changed = built.filter((product) => product.changed).length;
  return {
    linkedProducts: linkedCount,
    ready,
    changed,
    withoutSupplier: Math.max(0, Number(totalProducts || 0) - linkedCount),
    linkedNotReady: Math.max(0, linkedCount - ready),
  };
}

async function buildWarehouseCounterStatsFromLinkedProducts(products = [], suppliers = [], { totalProducts = 0, usdRate } = {}) {
  const linkedProducts = (Array.isArray(products) ? products : [])
    .map(normalizeWarehouseProduct)
    .filter((product) => Array.isArray(product.links) && product.links.length > 0);
  if (!linkedProducts.length) {
    return summarizeWarehouseCounterStats({ totalProducts, linkedProducts, builtLinkedProducts: [] });
  }
  const builtLinkedProducts = await buildFreshWarehouseProductsForWarehouse(
    { products: linkedProducts, suppliers: Array.isArray(suppliers) ? suppliers : [] },
    linkedProducts.map((product) => product.id),
    { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate },
  );
  return summarizeWarehouseCounterStats({ totalProducts, linkedProducts, builtLinkedProducts });
}

async function buildFreshWarehouseProducts(productIds = [], {
  refreshPrices = false,
  persistMutations = true,
  livePriceMaster = false,
  batchPriceMaster = false,
  usdRate,
} = {}) {
  const warehouse = await readWarehouse();
  return buildFreshWarehouseProductsForWarehouse(warehouse, productIds, {
    refreshPrices,
    persistMutations,
    livePriceMaster,
    batchPriceMaster,
    usdRate,
  });
}

async function buildFreshWarehouseProductsFromKnownProducts(warehouse = {}, products = [], options = {}) {
  const normalizedProducts = (Array.isArray(products) ? products : [products])
    .filter((product) => product && product.id)
    .map(normalizeWarehouseProduct);
  if (!normalizedProducts.length) return [];
  return buildFreshWarehouseProductsForWarehouse(
    {
      createdAt: warehouse.createdAt || null,
      updatedAt: warehouse.updatedAt || null,
      products: normalizedProducts,
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [],
    },
    normalizedProducts.map((product) => product.id),
    {
      refreshPrices: false,
      persistMutations: false,
      livePriceMaster: false,
      batchPriceMaster: false,
      ...options,
    },
  );
}

function normalizeWarehouseSearchToken(value) {
  return cleanText(value)
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/[\s\-_/\\.:;#№]+/g, "");
}

function isWarehouseArticleLikeQuery(query) {
  const text = cleanText(query);
  if (text.length < 2) return false;
  if (/\s/.test(text)) return false;
  return /\d/.test(text) || /[\-_/\\#№]/.test(text);
}

function isWarehouseStrictIdentitySearch(filters = {}) {
  return isWarehouseArticleLikeQuery(filters.q || "");
}

function warehouseProductSearchIdentityTokens(product = {}) {
  const links = Array.isArray(product.links) ? product.links : [];
  return [
    product.id,
    product.offerId,
    product.productId,
    product.sku,
    product.barcode,
    product.ozon?.offerId,
    product.ozon?.productId,
    product.ozon?.sku,
    product.ozon?.barcode,
    product.yandex?.offerId,
    product.yandex?.productId,
    product.yandex?.sku,
    product.yandex?.barcode,
    ...links.flatMap((link) => [link.article, link.sourceRowId]),
  ]
    .map(normalizeWarehouseSearchToken)
    .filter(Boolean);
}

function warehouseProductSearchRank(product = {}, query = "") {
  const normalizedQuery = normalizeWarehouseSearchToken(query);
  if (!normalizedQuery) return 999;
  const groups = [
    [product.offerId, product.id, product.productId],
    [product.sku, product.barcode, product.ozon?.offerId, product.yandex?.offerId],
    [product.ozon?.productId, product.yandex?.productId, product.ozon?.sku, product.yandex?.sku, product.ozon?.barcode, product.yandex?.barcode],
    ...(Array.isArray(product.links)
      ? product.links.map((link) => [link.article, link.sourceRowId])
      : []),
  ];
  for (let index = 0; index < groups.length; index += 1) {
    if (groups[index].some((value) => normalizeWarehouseSearchToken(value) === normalizedQuery)) return index;
  }
  return 999;
}

function sortWarehouseProductsForSearch(products = [], filters = {}) {
  if (!isWarehouseStrictIdentitySearch(filters)) return products;
  const query = filters.q || "";
  return [...products].sort((left, right) => {
    const rankDiff = warehouseProductSearchRank(left, query) - warehouseProductSearchRank(right, query);
    if (rankDiff) return rankDiff;
    return String(left.offerId || left.name || left.id || "").localeCompare(
      String(right.offerId || right.name || right.id || ""),
      "ru",
      { sensitivity: "base" },
    );
  });
}

function preferWarehousePrimaryIdentityMatches(products = [], filters = {}) {
  const rows = Array.isArray(products) ? products : [];
  if (!isWarehouseStrictIdentitySearch(filters)) return rows;
  const query = filters.q || "";
  const primaryMatches = rows.filter((product) => warehouseProductSearchRank(product, query) <= 2);
  return primaryMatches.length ? primaryMatches : rows;
}

function warehouseProductMatchesSearchQuery(product = {}, query = "") {
  const q = cleanText(query || "");
  if (!q) return true;
  if (isWarehouseArticleLikeQuery(q)) {
    const normalizedQuery = normalizeWarehouseSearchToken(q);
    return warehouseProductSearchIdentityTokens(product).some((token) => token === normalizedQuery);
  }
  const qLower = q.toLowerCase();
  const haystack = [
    product.id,
    product.offerId,
    product.name,
    resolveWarehouseBrand(product),
    product.categoryName,
    product.sku,
    product.barcode,
    ...(Array.isArray(product.links)
      ? product.links.flatMap((link) => [link.article, link.supplierName, link.partnerId, link.keyword, link.exactName, link.sourceRowId])
      : []),
  ]
    .map((value) => cleanText(value || "").toLowerCase())
    .join(" ");
  return haystack.includes(qLower);
}

function warehousePageProductMatches(product = {}, filters = {}) {
  if (!isWarehouseProductTargetEnabled(product)) return false;
  if (!warehouseProductMatchesSearchQuery(product, filters.q || "")) return false;
  const linked = cleanText(filters.linked || "all");
  const hasLinks = Array.isArray(product.links) && product.links.length > 0;
  if (linked === "linked" && !hasLinks) return false;
  if (linked === "ready" && (!hasLinks || !product.ready)) return false;
  if (linked === "unlinked" && hasLinks) return false;
  if (linked === "changed" && (!hasLinks || !product.changed)) return false;
  if (linked === "linked_archived" && (!hasLinks || cleanText(product.marketplace) !== "ozon" || cleanText(product.marketplaceState?.code) !== "archived")) return false;
  const marketplace = cleanText(filters.marketplace || "all");
  if (marketplace !== "all" && cleanText(product.marketplace) !== marketplace) return false;
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all" && cleanText(product.marketplaceState?.code) !== stateCode) return false;
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter && !warehouseBrandMatches(product, brandFilter)) return false;
  if (filters.autoOnly && product.autoPriceEnabled === false) return false;
  return true;
}

function enabledWarehouseTargetWhere() {
  const or = [];
  const ozonAccounts = getOzonAccounts();
  if (ozonAccounts.length) {
    for (const account of ozonAccounts) {
      or.push({
        marketplace: "ozon",
        OR: [
          { target: account.id },
          { target: "ozon" },
        ],
      });
    }
  } else {
    or.push({ marketplace: "ozon" });
  }
  const yandexTargetFilters = [
    { target: "yandex" },
    { target: { startsWith: "yandex" } },
  ];
  for (const shop of getYandexShops({ includeSyncDisabled: true })) {
    yandexTargetFilters.push({ target: shop.id });
  }
  if (yandexTargetFilters.length) {
    or.push({
      marketplace: "yandex",
      OR: yandexTargetFilters,
    });
  }
  return or.length ? { OR: or } : {};
}

function warehousePagePostgresWhere(filters = {}) {
  const and = [enabledWarehouseTargetWhere()];
  const marketplace = cleanText(filters.marketplace || "all");
  if (marketplace !== "all" && ["ozon", "yandex"].includes(marketplace)) and.push({ marketplace });
  const linked = cleanText(filters.linked || "all");
  if (linked === "linked") and.push({ links: { some: {} } });
  if (linked === "ready") and.push({ links: { some: {} } });
  if (linked === "unlinked") and.push({ links: { none: {} } });
  if (linked === "changed") and.push({ links: { some: {} } });
  if (linked === "linked_archived") and.push({ links: { some: {} } }, { marketplace: "ozon" });
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all") and.push({ status: stateCode });
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter) {
    and.push({
      OR: [
        { brand: { contains: brandFilter, mode: "insensitive" } },
        { name: { contains: brandFilter, mode: "insensitive" } },
      ],
    });
  }
  const q = cleanText(filters.q || "");
  if (q) {
    if (isWarehouseArticleLikeQuery(q)) {
      and.push({
        OR: [
          { id: { equals: q, mode: "insensitive" } },
          { offerId: { equals: q, mode: "insensitive" } },
          { productId: { equals: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { equals: q, mode: "insensitive" } } } },
        ],
      });
    } else {
      and.push({
        OR: [
          { id: { contains: q, mode: "insensitive" } },
          { offerId: { contains: q, mode: "insensitive" } },
          { productId: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { brand: { contains: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { contains: q, mode: "insensitive" } } } },
          { links: { some: { supplierName: { contains: q, mode: "insensitive" } } } },
          { links: { some: { partnerId: { contains: q, mode: "insensitive" } } } },
          { links: { some: { keyword: { contains: q, mode: "insensitive" } } } },
        ],
      });
    }
  }
  return { AND: and.filter((item) => Object.keys(item || {}).length) };
}

function warehousePagePostgresPrimaryIdentityWhere(filters = {}) {
  const q = cleanText(filters.q || "");
  if (!q || !isWarehouseArticleLikeQuery(q)) return warehousePagePostgresWhere(filters);
  const base = warehousePagePostgresWhere({ ...filters, q: "" });
  return {
    AND: [
      ...(Array.isArray(base.AND) ? base.AND : []),
      {
        OR: [
          { id: { equals: q, mode: "insensitive" } },
          { offerId: { equals: q, mode: "insensitive" } },
          { productId: { equals: q, mode: "insensitive" } },
        ],
      },
    ].filter((item) => Object.keys(item || {}).length),
  };
}

function warehouseStateCounter(products = [], marketplace = "") {
  const rows = Array.isArray(products) ? products : [];
  const market = cleanText(marketplace).toLowerCase();
  const filtered = market ? rows.filter((product) => cleanText(product.marketplace).toLowerCase() === market) : rows;
  return {
    archived: filtered.filter((product) => cleanText(product.marketplaceState?.code).toLowerCase() === "archived" || product.marketplaceState?.archived).length,
    inactive: filtered.filter((product) => /inactive/i.test(cleanText(product.marketplaceState?.code))).length,
    outOfStock: filtered.filter((product) => cleanText(product.marketplaceState?.code).toLowerCase() === "out_of_stock").length,
  };
}

async function warehouseStateCounterFromPostgres(prisma, marketplace) {
  const base = { AND: [enabledWarehouseTargetWhere(), { marketplace }] };
  const [archived, inactive, outOfStock] = await Promise.all([
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { OR: [{ archived: true }, { status: "archived" }] },
        ],
      },
    }),
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { status: { contains: "inactive", mode: "insensitive" } },
        ],
      },
    }),
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { status: "out_of_stock" },
        ],
      },
    }),
  ]);
  return { archived, inactive, outOfStock };
}

function warehousePagePostgresOrderBy() {
  return [
    { archived: "asc" },
    { status: "asc" },
    { marketplace: "asc" },
    { target: "asc" },
    { name: "asc" },
    { offerId: "asc" },
    { id: "asc" },
  ];
}

function warehouseFastPageCacheKey({ page = 1, pageSize = 60, usdRate, filters = {} } = {}) {
  return JSON.stringify({
    page: Number(page) || 1,
    pageSize: Number(pageSize) || 60,
    usdRate: Number.isFinite(Number(usdRate)) && Number(usdRate) > 0 ? Number(usdRate) : "default",
    filters: {
      q: cleanText(filters.q || "").toLowerCase(),
      autoOnly: Boolean(filters.autoOnly),
      linked: cleanText(filters.linked || "all"),
      marketplace: cleanText(filters.marketplace || "all"),
      state: cleanText(filters.state || "all"),
      brand: cleanText(filters.brand || "").toLowerCase(),
    },
    storage: shouldUsePostgresStorage() ? "postgres" : "json",
  });
}

function getWarehouseFastPageCache(params = {}) {
  const key = warehouseFastPageCacheKey(params);
  const cached = warehouseFastPageCache.get(key);
  if (!cached || Date.now() - cached.at > warehouseFastPageCacheTtlMs) {
    warehouseFastPageCache.delete(key);
    return { key, value: null };
  }
  return { key, value: cloneAuditValue(cached.value) };
}

function setWarehouseFastPageCache(key, value) {
  if (!key || !value) return;
  warehouseFastPageCache.set(key, { at: Date.now(), value: cloneAuditValue(value) });
  if (warehouseFastPageCache.size > warehouseFastPageCacheMax) {
    const oldestKey = warehouseFastPageCache.keys().next().value;
    if (oldestKey) warehouseFastPageCache.delete(oldestKey);
  }
}

function warehouseProductPageGroupKey(product = {}) {
  const raw = product.raw && typeof product.raw === "object" && !Array.isArray(product.raw) ? product.raw : {};
  const manualGroupId = cleanText(product.manualGroupId || product.manual_group_id || raw.manualGroupId || raw.manual_group_id).toLowerCase();
  if (manualGroupId) return `manual:${manualGroupId}`;
  const offerId = cleanText(product.offerId || product.offer_id).toLowerCase();
  if (offerId) return `offer:${offerId}`;
  return "";
}

function addWarehousePageGroupSiblings(sourceProducts = [], pageProducts = []) {
  const groupKeys = new Set((pageProducts || []).map(warehouseProductPageGroupKey).filter(Boolean));
  if (!groupKeys.size) return pageProducts || [];
  const byId = new Map();
  for (const product of pageProducts || []) {
    if (product?.id) byId.set(String(product.id), product);
  }
  for (const product of sourceProducts || []) {
    if (!product?.id) continue;
    if (groupKeys.has(warehouseProductPageGroupKey(product))) byId.set(String(product.id), product);
  }
  return Array.from(byId.values());
}

function expandWarehouseProductsToGroups(sourceProducts = [], seedProducts = []) {
  const seeds = Array.isArray(seedProducts) ? seedProducts : [];
  const seedIds = new Set(seeds.map((product) => String(product?.id || "")).filter(Boolean));
  const groupKeys = new Set(seeds.map(warehouseProductPageGroupKey).filter(Boolean));
  const byId = new Map();
  for (const product of Array.isArray(sourceProducts) ? sourceProducts : []) {
    if (!product?.id) continue;
    const id = String(product.id);
    const groupKey = warehouseProductPageGroupKey(product);
    if (seedIds.has(id) || (groupKey && groupKeys.has(groupKey))) byId.set(id, product);
  }
  return Array.from(byId.values());
}

function warehouseProductsForGroupKey(sourceProducts = [], groupKey = "") {
  const key = cleanText(groupKey);
  if (!key) return [];
  return (Array.isArray(sourceProducts) ? sourceProducts : [])
    .filter((product) => warehouseProductPageGroupKey(product) === key);
}

function syncWarehouseProductGroupLinks(products = [], { now = new Date().toISOString(), username = "system" } = {}) {
  const targetProducts = Array.isArray(products) ? products.filter((product) => product?.id) : [];
  const commonLinks = buildCommonWarehouseGroupLinks(targetProducts, [], { now, username });
  const changedProducts = [];
  const oldValues = [];
  for (const product of targetProducts) {
    const beforeSignature = warehouseProductLinkDetailsSignature(product);
    const beforeValue = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    product.links = commonLinks.map((link) => normalizeWarehouseLink({
      ...link,
      createdAt: link.createdAt || now,
      updatedAt: now,
      createdBy: link.createdBy || username,
      updatedBy: username,
    }));
    if (commonLinks.length) product.autoPriceEnabled = true;
    if (warehouseProductLinkDetailsSignature(product) !== beforeSignature) {
      product.updatedAt = now;
      changedProducts.push(product);
      oldValues.push(beforeValue);
    }
  }
  return {
    products: targetProducts,
    changedProducts,
    changedIds: changedProducts.map((product) => product.id),
    oldValues,
    commonLinks,
    groupLinkSignature: warehouseGroupLinkSignature(targetProducts),
  };
}

function buildWarehousePageProductGroups(products = []) {
  const groups = new Map();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const groupKey = warehouseProductPageGroupKey(normalized) || `id:${normalized.id}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        groupKey,
        offerId: normalized.offerId || "",
        manualGroupId: normalized.manualGroupId || normalized.raw?.manualGroupId || "",
        name: normalized.name || normalized.offerId || normalized.id,
        brand: resolveWarehouseBrand(normalized) || normalized.brand || "",
        imageUrl: normalized.imageUrl || "",
        marketplaces: [],
        products: [],
        links: [],
        statusSummary: {
          total: 0,
          linked: 0,
          archived: 0,
          ready: 0,
          changed: 0,
          withoutSupplier: 0,
          marketplaces: [],
        },
      });
    }
    const group = groups.get(groupKey);
    group.products.push(normalized);
    if (!group.imageUrl && normalized.imageUrl) group.imageUrl = normalized.imageUrl;
    if (!group.brand && (resolveWarehouseBrand(normalized) || normalized.brand)) group.brand = resolveWarehouseBrand(normalized) || normalized.brand;
    const marketplaceRaw = cleanText(normalized.marketplace || normalized.target || "marketplace").toLowerCase();
    const marketplace = marketplaceRaw.includes("ozon") ? "Ozon" : marketplaceRaw.includes("yandex") ? "Yandex" : marketplaceRaw;
    if (marketplace && !group.marketplaces.includes(marketplace)) group.marketplaces.push(marketplace);
    for (const link of normalized.links || []) {
      const linkKey = warehouseLinkTargetKey(link);
      if (!group.links.some((item) => warehouseLinkTargetKey(item) === linkKey)) {
        group.links.push(link);
      }
    }
    const stateCode = cleanText(normalized.marketplaceState?.code || normalized.status).toLowerCase();
    const archived = Boolean(normalized.archived || stateCode.includes("archiv"));
    const linked = Array.isArray(normalized.links) && normalized.links.length > 0;
    const ready = linked && !archived && Number(normalized.targetStock || normalized.stock || 0) > 0;
    const changed = Number(normalized.nextPrice || normalized.newPrice || normalized.targetPrice || 0) > 0
      && Number(normalized.marketplacePrice || normalized.currentPrice || 0) !== Number(normalized.nextPrice || normalized.newPrice || normalized.targetPrice || 0);
    group.statusSummary.total += 1;
    if (linked) group.statusSummary.linked += 1;
    if (archived) group.statusSummary.archived += 1;
    if (ready) group.statusSummary.ready += 1;
    if (changed) group.statusSummary.changed += 1;
    if (!normalized.selectedSupplier && linked) group.statusSummary.withoutSupplier += 1;
    group.statusSummary.marketplaces = group.marketplaces;
  }
  return Array.from(groups.values()).map((group) => ({
    ...group,
    marketplaces: group.marketplaces.sort(),
    products: group.products.sort((a, b) => String(a.marketplace || "").localeCompare(String(b.marketplace || "")) || String(a.target || "").localeCompare(String(b.target || ""))),
  }));
}

function linkedRecoveryCandidateProducts(products = [], limit = 30000) {
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 30000) || 30000)));
  const rows = (Array.isArray(products) ? products : [])
    .filter((product) => product?.id)
    .map(normalizeWarehouseProduct);
  const linkedByGroup = new Map();
  for (const product of rows) {
    if (!Array.isArray(product.links) || !product.links.length) continue;
    const groupKey = warehouseProductPageGroupKey(product) || `id:${product.id}`;
    if (!linkedByGroup.has(groupKey)) linkedByGroup.set(groupKey, product);
  }
  if (!linkedByGroup.size) return [];

  const byId = new Map();
  for (const product of rows) {
    const groupKey = warehouseProductPageGroupKey(product) || `id:${product.id}`;
    const donor = linkedByGroup.get(groupKey);
    if (!donor) continue;
    const links = Array.isArray(product.links) && product.links.length
      ? product.links
      : donor.links;
    byId.set(String(product.id), normalizeWarehouseProduct({
      ...product,
      links,
    }));
    if (byId.size >= max) break;
  }

  return Array.from(byId.values());
}

function mergeWarehousePostgresRows(...rowLists) {
  const byId = new Map();
  for (const rows of rowLists) {
    for (const row of rows || []) {
      if (row?.id) byId.set(String(row.id), row);
    }
  }
  return Array.from(byId.values());
}

async function addWarehousePostgresPageGroupSiblings(prisma, baseWhere, pageRows = []) {
  const offerIds = Array.from(new Set((pageRows || []).map((row) => cleanText(row.offerId)).filter(Boolean)));
  if (!offerIds.length) return pageRows || [];
  const siblings = await prisma.warehouseProduct.findMany({
    where: {
      AND: [
        baseWhere,
        {
          OR: offerIds.map((offerId) => ({
            offerId: { equals: offerId, mode: "insensitive" },
          })),
        },
      ],
    },
    include: { links: true },
    orderBy: warehousePagePostgresOrderBy(),
  });
  return mergeWarehousePostgresRows(pageRows, siblings);
}

function marketplaceStateCodeFromPostgresRow(row = {}) {
  const state = row.marketplaceState && typeof row.marketplaceState === "object" && !Array.isArray(row.marketplaceState)
    ? row.marketplaceState
    : {};
  return cleanText(state.code || row.status || state.state).toLowerCase();
}

async function getMarketplaceStateCountsFromPostgres(prisma, marketplace = "ozon") {
  return warehouseStateCounterFromPostgres(prisma, marketplace);
}

async function getOzonStateCountsFromPostgres(prisma) {
  return getMarketplaceStateCountsFromPostgres(prisma, "ozon");
}

async function getWarehousePostgresSummary(prisma, rate) {
  const cacheKey = Number(rate || 0).toFixed(4);
  if (
    warehousePostgresSummaryCache
    && warehousePostgresSummaryCache.key === cacheKey
    && Date.now() - warehousePostgresSummaryCache.at < warehousePostgresSummaryCacheTtlMs
  ) {
    return warehousePostgresSummaryCache.value;
  }
  const [totalAll, ozonStateCounts, yandexStateCounts, linkedRows, suppliers] = await Promise.all([
    prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
    getOzonStateCountsFromPostgres(prisma),
    getMarketplaceStateCountsFromPostgres(prisma, "yandex"),
    prisma.warehouseProduct.findMany({
      where: { AND: [enabledWarehouseTargetWhere(), { links: { some: {} } }] },
      include: { links: true },
      orderBy: { updatedAt: "desc" },
    }),
    getWarehousePostgresSuppliers(prisma),
  ]);
  const normalizedSuppliers = suppliers;
  const linkedProducts = linkedRows.map(productFromPostgres);
  const counterStats = await buildWarehouseCounterStatsFromLinkedProducts(
    linkedProducts,
    normalizedSuppliers,
    { totalProducts: totalAll, usdRate: rate },
  );
  const value = {
    totalAll,
    ozonStateCounts,
    yandexStateCounts,
    normalizedSuppliers,
    counterStats,
    linkedArchived: linkedProducts
      .filter((product) => product.marketplace === "ozon" && Array.isArray(product.links) && product.links.length && product.marketplaceState?.code === "archived").length,
  };
  warehousePostgresSummaryCache = { key: cacheKey, at: Date.now(), value };
  return value;
}

async function buildFastWarehousePageFromPostgres({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  const traceStartedAt = Date.now();
  if (filters.autoOnly) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  pageTrace("postgres:start", traceStartedAt);
  await ensureWarehousePostgresLinksBackfilled(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const linkedFilter = cleanText(filters.linked || "all");
  const needsComputedLinkFilter = linkedFilter === "ready" || linkedFilter === "changed" || linkedFilter === "linked_archived";
  const brandFilter = cleanText(filters.brand || "");
  let brandIndexProductIds = [];
  if (brandFilter) {
    await ensureWarehousePostgresBrandsBackfilled(prisma);
    brandIndexProductIds = await brandIndexProductIdsForFilterPostgres(prisma, brandFilter).catch((error) => {
      logger.warn("warehouse brand index filter failed, using product brand fallback", { detail: error?.message || String(error) });
      return [];
    });
  }
  const strictIdentitySearch = isWarehouseStrictIdentitySearch(filters);
  const needsInMemoryPage = needsComputedLinkFilter || strictIdentitySearch;
  const postgresFilters = {
    ...(needsComputedLinkFilter ? { ...filters, state: "all" } : filters),
    ...(brandIndexProductIds.length ? { brand: "" } : {}),
  };
  const where = warehousePagePostgresWhere(postgresFilters);
  if (brandIndexProductIds.length) where.AND.push({ id: { in: brandIndexProductIds } });
  const strictPrimaryWhere = strictIdentitySearch
    ? warehousePagePostgresPrimaryIdentityWhere(postgresFilters)
    : null;
  const offset = (page - 1) * pageSize;
  pageTrace("postgres:before-query", traceStartedAt);
  const [summary, dbTotal, initialDbRows] = await Promise.all([
    getWarehousePostgresSummary(prisma, rate),
    needsInMemoryPage ? Promise.resolve(0) : prisma.warehouseProduct.count({ where }),
    prisma.warehouseProduct.findMany({
      where: strictPrimaryWhere || where,
      include: { links: true },
      orderBy: warehousePagePostgresOrderBy(),
      skip: needsInMemoryPage ? 0 : offset,
      take: needsInMemoryPage ? undefined : pageSize,
    }),
  ]);
  pageTrace("postgres:after-query", traceStartedAt);
  let dbRows = initialDbRows;
  if (strictIdentitySearch && dbRows.length === 0) {
    dbRows = await prisma.warehouseProduct.findMany({
      where,
      include: { links: true },
      orderBy: warehousePagePostgresOrderBy(),
    });
    pageTrace("postgres:after-strict-fallback-query", traceStartedAt);
  }
  let pageBaseCount = dbRows.length;
  if (!needsComputedLinkFilter && !strictIdentitySearch) {
    dbRows = await addWarehousePostgresPageGroupSiblings(prisma, where, dbRows);
  }
  const normalizedSuppliers = summary.normalizedSuppliers;
  const counterStats = summary.counterStats;
  const siblingSourceProducts = dbRows.map(productFromPostgres);
  let allProducts = sortWarehouseProductsForSearch(siblingSourceProducts, filters);
  if (needsComputedLinkFilter) {
    allProducts = await buildFreshWarehouseProductsForWarehouse(
      { products: allProducts, suppliers: normalizedSuppliers },
      allProducts.map((product) => product.id),
      { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
    );
  }
  if (needsInMemoryPage) {
    allProducts = sortWarehouseProductsForSearch(
      preferWarehousePrimaryIdentityMatches(
        allProducts.filter((product) => warehousePageProductMatches(product, filters)),
        filters,
      ),
      filters,
    );
  }
  const total = needsInMemoryPage ? allProducts.length : dbTotal;
  let visibleProducts = allProducts;
  if (needsInMemoryPage) {
    const pageSlice = allProducts.slice(offset, offset + pageSize);
    pageBaseCount = pageSlice.length;
    visibleProducts = strictIdentitySearch ? pageSlice : addWarehousePageGroupSiblings(siblingSourceProducts, pageSlice);
  }
  const pageProducts = await enrichWeakOzonProductsForPage(visibleProducts);
  const pageWarehouse = {
    createdAt: dbRows[0]?.createdAt?.toISOString() || null,
    updatedAt: dbRows[0]?.updatedAt?.toISOString() || null,
    products: pageProducts,
    suppliers: normalizedSuppliers,
  };
  const built = await buildFreshWarehouseProductsForWarehouse(
    pageWarehouse,
    pageWarehouse.products.map((product) => product.id),
    { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
  );
  pageTrace("postgres:after-build", traceStartedAt);
  const builtMap = new Map(built.map((product) => [product.id, product]));
  const items = pageWarehouse.products.map((product) => {
    const item = builtMap.get(product.id) || normalizeWarehouseProduct(product);
    return {
      ...item,
      autoPriceEnabled: item.autoPriceEnabled !== false,
      links: Array.isArray(item.links) ? item.links : [],
      suppliers: Array.isArray(item.suppliers) ? item.suppliers : [],
      selectedSupplier: item.selectedSupplier || null,
      noSupplierAutomation: item.noSupplierAutomation || {},
      marketplaceState: item.marketplaceState || {},
      partial: false,
    };
  });
  return {
    createdAt: pageWarehouse.createdAt,
    updatedAt: pageWarehouse.updatedAt,
    totalAll: summary.totalAll,
    ready: counterStats.ready,
    changed: counterStats.changed,
    withoutSupplier: counterStats.withoutSupplier,
    linkedProducts: counterStats.linkedProducts,
    linkedNotReady: counterStats.linkedNotReady,
    linkedArchived: summary.linkedArchived,
    ozonArchived: summary.ozonStateCounts.archived,
    ozonInactive: summary.ozonStateCounts.inactive,
    ozonOutOfStock: summary.ozonStateCounts.outOfStock,
    yandexArchived: summary.yandexStateCounts.archived,
    yandexInactive: summary.yandexStateCounts.inactive,
    yandexOutOfStock: summary.yandexStateCounts.outOfStock,
    usdRate: rate,
    priceMaster: await getPriceMasterSnapshotMetaFast(),
    sourceError: "",
    noSupplierAlerts: [],
    page,
    pageSize,
    total,
    hasMore: offset + pageBaseCount < total,
    items,
  };
}

async function buildWarehouseProductDetailFromPostgres(productId, { usdRate } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;
  await ensureWarehousePostgresLinksBackfilled(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  return warehousePostgresCachedDetail(`product:${productId}:${rate}`, async () => {
    const row = await prisma.warehouseProduct.findFirst({
      where: { AND: [enabledWarehouseTargetWhere(), { id: productId }] },
      include: { links: true },
    });
    if (!row) return null;
    const normalizedSuppliers = await getWarehousePostgresSuppliers(prisma);
    const warehouse = {
      createdAt: row.createdAt?.toISOString?.() || null,
      updatedAt: row.updatedAt?.toISOString?.() || null,
      products: [productFromPostgres(row)],
      suppliers: normalizedSuppliers,
    };
    const built = await buildFreshWarehouseProductsForWarehouse(
      warehouse,
      [row.id],
      { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
    );
    const product = built[0] || warehouse.products[0];
    return {
      createdAt: warehouse.createdAt,
      product: {
        ...product,
        autoPriceEnabled: product.autoPriceEnabled !== false,
        links: Array.isArray(product.links) ? product.links : [],
        suppliers: Array.isArray(product.suppliers) ? product.suppliers : [],
        selectedSupplier: product.selectedSupplier || null,
        noSupplierAutomation: product.noSupplierAutomation || {},
        marketplaceState: product.marketplaceState || {},
        partial: false,
      },
    };
  });
}

function normalizeWarehouseDetailProduct(product = {}) {
  return {
    ...product,
    autoPriceEnabled: product.autoPriceEnabled !== false,
    links: Array.isArray(product.links) ? product.links : [],
    suppliers: Array.isArray(product.suppliers) ? product.suppliers : [],
    selectedSupplier: product.selectedSupplier || null,
    noSupplierAutomation: product.noSupplierAutomation || {},
    marketplaceState: product.marketplaceState || {},
    partial: false,
  };
}

function publicSelectedSupplierDiagnostics(supplier = null) {
  if (!supplier) return null;
  return {
    supplierName: supplier.supplierName || supplier.name || "",
    article: supplier.article || supplier.supplierArticle || "",
    partnerId: supplier.partnerId || "",
    available: supplier.available !== false,
    price: supplier.price ?? supplier.calculatedPrice ?? null,
    priceRub: supplier.priceRub ?? supplier.calculatedPriceRub ?? null,
    calculatedPrice: supplier.calculatedPrice ?? supplier.priceRub ?? supplier.calculatedPriceRub ?? null,
    markupCoefficient: supplier.markupCoefficient ?? null,
    baseMarkupCoefficient: supplier.baseMarkupCoefficient ?? null,
    currency: supplier.currency || supplier.priceCurrency || "",
  };
}

function latestProductCommand(product = {}, kind = "stock") {
  return kind === "archive"
    ? normalizeLastMarketplaceCommand(product.lastArchiveSend || {})
    : normalizeLastMarketplaceCommand(product.lastStockSend || {});
}

function buildWarehouseProductAutomationDiagnostics(product = {}, contextProducts = []) {
  const candidates = Array.isArray(contextProducts) && contextProducts.length ? contextProducts : [product];
  const noSupplier = pickNoSupplierAutomationCandidates(candidates, { includeNoLinks: true });
  const toZero = new Set(noSupplier.toZeroStock.map((item) => String(item.id)));
  const toArchive = new Set(noSupplier.toArchive.map((item) => String(item.id)));
  const hasDirectLinks = product.hasLinks || (Array.isArray(product.links) && product.links.length > 0);
  return {
    wouldSendTargetStock: shouldSendTargetStockForProduct(product),
    wouldRecoverSupplier: pickSupplierRecoveryCandidates(candidates, { productIds: [product.id] }).some((item) => String(item.id) === String(product.id)),
    wouldZeroStockAsNoSupplier: toZero.has(String(product.id)),
    wouldArchiveAsNoSupplier: toArchive.has(String(product.id)),
    protectedFromNoSupplierArchive: Boolean(hasDirectLinks || product.noSupplierAutomation?.manualSellableAt),
    sameOfferGroupProtected: Boolean(
      marketplaceOfferAutomationKey(product)
      && candidates.some((item) =>
        String(item.id) !== String(product.id)
        && marketplaceOfferAutomationKey(item) === marketplaceOfferAutomationKey(product)
        && (item.hasLinks || (Array.isArray(item.links) && item.links.length > 0) || item.noSupplierAutomation?.manualSellableAt)
      )
    ),
    needsSalesRecovery: marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true }),
  };
}

function marketplaceCommandHasError(command = {}) {
  const normalized = command && typeof command === "object" ? command : {};
  const status = cleanText(normalized.status || "").toLowerCase();
  return Boolean(status === "error" || status === "failed" || status.includes("fail") || normalized.error);
}

function marketplaceCommandIsPending(command = {}) {
  const normalized = command && typeof command === "object" ? command : {};
  const status = cleanText(normalized.status || "").toLowerCase();
  const detail = cleanText(normalized.detail || normalized.warning || normalized.error || "").toLowerCase();
  return Boolean(
    status === "pending"
      || status === "queued"
      || status === "accepted"
      || status === "processing"
      || detail.includes("pending")
      || detail.includes("not_visible")
  );
}

function warehouseProductDiagnosticSaleState(product = {}, contextProducts = []) {
  const state = product.marketplaceState || {};
  const lastStockSend = latestProductCommand(product, "stock");
  const lastArchiveSend = latestProductCommand(product, "archive");
  const lastPriceSend = normalizeWarehouseProduct(product).marketplace === "yandex"
    ? normalizeLastPriceSend(product.lastYandexPriceSend || {})
    : normalizeLastPriceSend(product.lastOzonPriceSend || product.lastYandexPriceSend || {});
  const hasLinks = Boolean(product.hasLinks || (Array.isArray(product.links) && product.links.length));
  const selectedSupplier = Boolean(product.selectedSupplier || Number(product.availableSupplierCount || 0) > 0);
  const archived = Boolean(productLooksArchived(product));
  const stock = Number(product.targetStock ?? product.stock ?? state.stock ?? state.availableStock ?? 0);
  const stateCode = cleanText(state.code || product.status || "").toLowerCase();
  const marketplaceStock = Number(state.stock ?? state.present ?? state.availableStock ?? NaN);
  const lastError = cleanText(
    product.noSupplierAutomation?.lastError
      || lastStockSend?.error
      || lastArchiveSend?.error
      || lastPriceSend?.detail
      || "",
  );

  if (marketplaceCommandHasError(lastStockSend) || marketplaceCommandHasError(lastArchiveSend) || marketplaceCommandHasError(lastPriceSend) || lastError) {
    return { code: "api_error", label: "API error", reason: lastError || "last_marketplace_command_failed" };
  }
  if (marketplaceCommandIsPending(lastStockSend) || marketplaceCommandIsPending(lastArchiveSend) || marketplaceCommandIsPending(lastPriceSend)) {
    return { code: "api_pending", label: "API pending", reason: "marketplace_status_not_visible_yet" };
  }
  if (archived) return { code: "archived", label: "Архив", reason: "marketplace_card_archived" };
  if (!hasLinks) return { code: "no_links", label: "Нет привязки", reason: "no_pricemaster_links" };
  if (!selectedSupplier) return { code: "no_supplier", label: "Нет поставщика", reason: "linked_but_supplier_not_selected" };
  if (!Number.isFinite(stock) || stock <= 0) return { code: "no_stock", label: "Нет остатка", reason: "target_stock_is_zero" };
  if (
    stock > 0
    && (stateCode === "out_of_stock" || stateCode === "inactive" || stateCode.includes("out_of_stock"))
    && (!Number.isFinite(marketplaceStock) || marketplaceStock <= 0)
  ) {
    return { code: "stock_stale", label: "Stock push needed", reason: "linked_supplier_target_stock_positive_but_marketplace_stock_is_zero" };
  }
  return { code: "ready", label: "Готов к продаже", reason: "linked_supplier_stock_and_not_archived" };
}

function buildWarehouseDiagnosticsGroupSummary(products = []) {
  const summary = {
    total: 0,
    linked: 0,
    ready: 0,
    archived: 0,
    noLinks: 0,
    noSupplier: 0,
    noStock: 0,
    stockStale: 0,
    apiPending: 0,
    apiError: 0,
    marketplaces: [],
  };
  const marketplaces = new Set();
  for (const product of Array.isArray(products) ? products : []) {
    const normalized = normalizeWarehouseProduct(product);
    const saleState = warehouseProductDiagnosticSaleState(normalized, products);
    summary.total += 1;
    if (normalized.hasLinks || (Array.isArray(normalized.links) && normalized.links.length)) summary.linked += 1;
    if (saleState.code === "ready") summary.ready += 1;
    if (saleState.code === "archived") summary.archived += 1;
    if (saleState.code === "no_links") summary.noLinks += 1;
    if (saleState.code === "no_supplier") summary.noSupplier += 1;
    if (saleState.code === "no_stock") summary.noStock += 1;
    if (saleState.code === "stock_stale") summary.stockStale += 1;
    if (saleState.code === "api_pending") summary.apiPending += 1;
    if (saleState.code === "api_error") summary.apiError += 1;
    const marketplace = cleanText(normalized.marketplace || normalized.target || "");
    if (marketplace) marketplaces.add(marketplace);
  }
  summary.marketplaces = Array.from(marketplaces).sort();
  return summary;
}

function publicWarehouseDiagnosticProduct(product = {}, contextProducts = []) {
  const state = product.marketplaceState || {};
  const saleState = warehouseProductDiagnosticSaleState(product, contextProducts);
  return {
    id: product.id,
    groupKey: warehouseProductPageGroupKey(product) || "",
    marketplace: product.marketplace || "",
    target: product.target || "",
    targetName: product.targetName || "",
    offerId: product.offerId || "",
    productId: product.productId || "",
    sku: product.sku || product.ozon?.sku || product.yandex?.sku || "",
    barcode: product.barcode || product.ozon?.barcode || product.yandex?.barcode || "",
    name: product.name || "",
    brand: resolveWarehouseBrand(product),
    hasLinks: Boolean(product.hasLinks || (Array.isArray(product.links) && product.links.length)),
    ready: Boolean(product.ready),
    changed: Boolean(product.changed),
    supplierCount: Number(product.supplierCount || 0),
    availableSupplierCount: Number(product.availableSupplierCount || 0),
    selectedSupplier: publicSelectedSupplierDiagnostics(product.selectedSupplier),
    markupCoefficient: product.markupCoefficient ?? null,
    usdRate: product.usdRate ?? product.priceFormula?.usdRate ?? null,
    priceFormula: product.priceFormula || null,
    currentPrice: product.currentPrice ?? null,
    targetPrice: product.targetPrice ?? null,
    targetStock: product.targetStock ?? null,
    lastOzonPriceSend: product.lastOzonPriceSend || null,
    lastYandexPriceSend: product.lastYandexPriceSend || null,
    lastStockSend: latestProductCommand(product, "stock"),
    lastArchiveSend: latestProductCommand(product, "archive"),
    automation: buildWarehouseProductAutomationDiagnostics(product, contextProducts),
    autoPriceEnabled: product.autoPriceEnabled !== false,
    status: product.status || state.code || "",
    archived: Boolean(product.archived || state.archived || cleanText(state.code).toLowerCase() === "archived"),
    saleState,
    saleStateCode: saleState.code,
    saleStateLabel: saleState.label,
    saleReason: saleState.reason,
    marketplaceState: state,
    noSupplierAutomation: product.noSupplierAutomation || {},
    links: (Array.isArray(product.links) ? product.links : []).map((link) => ({
      id: link.id || "",
      article: link.article || "",
      supplierName: link.supplierName || "",
      partnerId: link.partnerId || "",
      keyword: link.keyword || "",
      matchType: link.matchType || "",
      sourceRowId: link.sourceRowId || "",
      updatedAt: link.updatedAt || "",
      updatedBy: link.updatedBy || "",
    })),
  };
}

async function buildWarehouseSkuDiagnostics(sku = "", { limit = 50, auditLimit = 30, usdRate } = {}) {
  const query = cleanText(sku);
  if (!query) {
    const error = new Error("Укажите sku.");
    error.statusCode = 400;
    throw error;
  }
  const normalizedQuery = normalizeWarehouseSearchToken(query);
  const warehouse = await readWarehouse();
  const products = Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [];
  const strictMatches = products.filter((product) => warehouseProductMatchesSearchQuery(product, query));
  const focusedMatches = preferWarehousePrimaryIdentityMatches(strictMatches, { q: query });
  const hiddenSupplierOnlyMatches = Math.max(0, strictMatches.length - focusedMatches.length);
  const primaryOfferIds = new Set(
    focusedMatches
      .filter((product) => warehouseProductSearchRank(product, query) <= 2)
      .map((product) => cleanText(product.offerId).toLowerCase())
      .filter(Boolean),
  );
  const productIds = new Set(focusedMatches.map((product) => String(product.id)));
  if (primaryOfferIds.size) {
    for (const product of products) {
      const offerId = cleanText(product.offerId).toLowerCase();
      if (offerId && primaryOfferIds.has(offerId)) productIds.add(String(product.id));
    }
  }
  const matchedGroupKeys = new Set(
    products
      .filter((product) => productIds.has(String(product.id)))
      .map(warehouseProductPageGroupKey)
      .filter(Boolean),
  );
  if (matchedGroupKeys.size) {
    for (const product of products) {
      if (matchedGroupKeys.has(warehouseProductPageGroupKey(product))) productIds.add(String(product.id));
    }
  }
  const matchedProducts = products
    .filter((product) => productIds.has(String(product.id)))
    .slice(0, Math.max(1, Math.min(100, Math.round(Number(limit || 50) || 50))));
  const built = matchedProducts.length
    ? await buildFreshWarehouseProductsFromKnownProducts(
      warehouse,
      matchedProducts,
      { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate },
    )
    : [];
  const builtById = new Map(built.map((product) => [String(product.id), normalizeWarehouseProduct(product)]));
  const diagnosticProducts = matchedProducts.map((product) => {
    const builtProduct = builtById.get(String(product.id));
    if (!builtProduct) return normalizeWarehouseDetailProduct(product);
    return normalizeWarehouseDetailProduct({
      ...builtProduct,
      selectedSupplier: builtProduct.selectedSupplier || product.selectedSupplier || null,
      supplierCount: Number(builtProduct.supplierCount || product.supplierCount || 0),
      availableSupplierCount: Number(builtProduct.availableSupplierCount || product.availableSupplierCount || 0),
      targetStock: Number(builtProduct.targetStock || 0) > 0 ? builtProduct.targetStock : product.targetStock,
      links: Array.isArray(builtProduct.links) && builtProduct.links.length ? builtProduct.links : product.links,
    });
  });
  const latestAudit = await readAuditFiltered({ q: query }, Math.max(1, Math.min(100, Math.round(Number(auditLimit || 30) || 30))));
  const warnings = [];
  if (!diagnosticProducts.length) warnings.push("sku_not_found");
  if (hiddenSupplierOnlyMatches > 0) warnings.push("supplier_only_matches_hidden_by_primary_identity");
  const firstGroupKey = diagnosticProducts.map(warehouseProductPageGroupKey).find(Boolean) || "";
  const groupProducts = firstGroupKey
    ? diagnosticProducts.filter((product) => warehouseProductPageGroupKey(product) === firstGroupKey)
    : diagnosticProducts;
  const groupSummary = buildWarehouseDiagnosticsGroupSummary(groupProducts);
  return {
    sku: query,
    normalizedSku: normalizedQuery,
    groupKey: firstGroupKey,
    group: {
      groupKey: firstGroupKey,
      offerId: cleanText(groupProducts[0]?.offerId || query),
      manualGroupId: cleanText(groupProducts[0]?.manualGroupId || groupProducts[0]?.raw?.manualGroupId || ""),
      name: cleanText(groupProducts[0]?.name || groupProducts[0]?.offerId || query),
      marketplaces: groupSummary.marketplaces,
      statusSummary: groupSummary,
    },
    statusSummary: groupSummary,
    matched: diagnosticProducts.length,
    hiddenSupplierOnlyMatches,
    warnings,
    products: diagnosticProducts.map((product) => publicWarehouseDiagnosticProduct(product, diagnosticProducts)),
    audit: latestAudit.map((entry) => ({
      at: entry.at,
      user: entry.user,
      action: entry.action,
      productIds: auditEntryProductIds(entry),
      details: entry.details || {},
    })),
  };
}

function warehouseGroupKeyParts(groupKey = "") {
  const text = cleanText(groupKey);
  const [kind, ...rest] = text.split(":");
  return { kind: cleanText(kind).toLowerCase(), value: cleanText(rest.join(":")).toLowerCase() };
}

async function buildWarehouseGroupDetailFromPostgres(groupKey, { usdRate, filters = {} } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;
  const { kind, value } = warehouseGroupKeyParts(groupKey);
  if (!value) return null;
  await ensureWarehousePostgresLinksBackfilled(prisma);
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const cacheKey = `group:${groupKey}:${rate}:all-marketplaces`;
  return warehousePostgresCachedDetail(cacheKey, async () => {
    const baseWhere = warehousePagePostgresWhere({ marketplace: "all", state: "all", q: "", linked: "all", brand: "" });
    let rows = [];
    if (kind === "offer") {
      rows = await prisma.warehouseProduct.findMany({
        where: { AND: [baseWhere, { offerId: { equals: value, mode: "insensitive" } }] },
        include: { links: true },
        orderBy: warehousePagePostgresOrderBy(),
      });
    } else if (kind === "manual") {
      rows = await prisma.warehouseProduct.findMany({
        where: {
          AND: [
            baseWhere,
            { raw: { path: ["manualGroupId"], equals: value } },
          ],
        },
        include: { links: true },
        orderBy: warehousePagePostgresOrderBy(),
      }).catch(async (error) => {
        logger.warn("warehouse manual group postgres direct lookup failed, using fallback scan", { detail: error?.message || String(error) });
        const candidates = await prisma.warehouseProduct.findMany({
          where: baseWhere,
          include: { links: true },
          orderBy: warehousePagePostgresOrderBy(),
        });
        return candidates.filter((row) => warehouseProductPageGroupKey(productFromPostgres(row)) === groupKey);
      });
    }
    if (!rows.length) return null;
    const normalizedSuppliers = await getWarehousePostgresSuppliers(prisma);
    const products = rows.map(productFromPostgres);
    const pageProducts = await enrichWeakOzonProductsForPage(products);
    const built = await buildFreshWarehouseProductsForWarehouse(
      { products: pageProducts, suppliers: normalizedSuppliers },
      pageProducts.map((product) => product.id),
      { refreshPrices: false, persistMutations: false, livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
    );
    const builtMap = new Map(built.map((product) => [product.id, product]));
    const detailProducts = pageProducts.map((product) => normalizeWarehouseDetailProduct(builtMap.get(product.id) || normalizeWarehouseProduct(product)));
    return {
      products: detailProducts,
      suppliers: normalizedSuppliers,
      groupLinkSignature: warehouseGroupLinkSignature(detailProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(detailProducts),
    };
  });
}

async function buildWarehouseGroupDetail(groupKey, { usdRate, filters = {} } = {}) {
  if (shouldUsePostgresStorage()) {
    const postgresDetail = await buildWarehouseGroupDetailFromPostgres(groupKey, { usdRate, filters });
    if (postgresDetail) return postgresDetail;
  }
  const warehouse = await readWarehouse();
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const enabledProducts = (Array.isArray(warehouse.products) ? warehouse.products : []).filter(isWarehouseProductTargetEnabled);
  const products = enabledProducts.filter((product) => warehouseProductPageGroupKey(product) === groupKey);
  if (!products.length) return null;
  const pageProducts = await enrichWeakOzonProductsForPage(products);
  const built = await buildFreshWarehouseProductsForWarehouse(
    { ...warehouse, products: pageProducts },
    pageProducts.map((product) => product.id),
    { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
  );
  const builtMap = new Map(built.map((product) => [product.id, product]));
  const detailProducts = pageProducts.map((product) => normalizeWarehouseDetailProduct(builtMap.get(product.id) || normalizeWarehouseProduct(product)));
  return {
    products: detailProducts,
    suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [],
    groupLinkSignature: warehouseGroupLinkSignature(detailProducts),
    marketplacePriceBreakdown: marketplacePriceBreakdown(detailProducts),
  };
}

async function buildFastWarehousePage({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  const cacheParams = { page, pageSize, usdRate, filters };
  const cached = getWarehouseFastPageCache(cacheParams);
  if (cached.value) return cached.value;
  let result = null;
  if (shouldUsePostgresStorage()) {
    const postgresPage = await buildFastWarehousePageFromPostgres({ page, pageSize, usdRate, filters });
    if (postgresPage) {
      setWarehouseFastPageCache(cached.key, postgresPage);
      return postgresPage;
    }
  }
  const warehouse = await readWarehouse();
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const sourceProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
  const enabledProducts = sourceProducts.filter(isWarehouseProductTargetEnabled);
  const siblingSourceProducts = enabledProducts.map(normalizeWarehouseProduct);
  const filtered = sortWarehouseProductsForSearch(
    enabledProducts.filter((product) => warehousePageProductMatches(product, filters)),
    filters,
  );
  const total = filtered.length;
  const offset = (page - 1) * pageSize;
  const pageSlice = filtered.slice(offset, offset + pageSize);
  const strictIdentitySearch = isWarehouseStrictIdentitySearch(filters);
  const pageProducts = await enrichWeakOzonProductsForPage(
    strictIdentitySearch ? pageSlice : addWarehousePageGroupSiblings(siblingSourceProducts, pageSlice),
  );
  const built = await buildFreshWarehouseProductsForWarehouse(
    { ...warehouse, products: pageProducts },
    pageProducts.map((product) => product.id),
    { livePriceMaster: false, batchPriceMaster: false, usdRate: rate },
  );
  const builtMap = new Map(built.map((product) => [product.id, product]));
  const items = pageProducts.map((product) => {
    const item = builtMap.get(product.id) || normalizeWarehouseProduct(product);
    return {
      ...item,
      autoPriceEnabled: item.autoPriceEnabled !== false,
      links: Array.isArray(item.links) ? item.links : [],
      suppliers: Array.isArray(item.suppliers) ? item.suppliers : [],
      selectedSupplier: item.selectedSupplier || null,
      noSupplierAutomation: item.noSupplierAutomation || {},
      marketplaceState: item.marketplaceState || {},
      partial: false,
    };
  });
  const counterStats = await buildWarehouseCounterStatsFromLinkedProducts(
    enabledProducts,
    warehouse.suppliers,
    { totalProducts: enabledProducts.length, usdRate: rate },
  );
  const ozonStateCounts = warehouseStateCounter(enabledProducts, "ozon");
  const yandexStateCounts = warehouseStateCounter(enabledProducts, "yandex");
  result = {
    createdAt: warehouse.createdAt || null,
    updatedAt: warehouse.updatedAt || null,
    totalAll: enabledProducts.length,
    ready: counterStats.ready,
    changed: counterStats.changed,
    withoutSupplier: counterStats.withoutSupplier,
    linkedProducts: counterStats.linkedProducts,
    linkedNotReady: counterStats.linkedNotReady,
    linkedArchived: enabledProducts.filter((product) => product.marketplace === "ozon" && Array.isArray(product.links) && product.links.length && product.marketplaceState?.code === "archived").length,
    ozonArchived: ozonStateCounts.archived,
    ozonInactive: ozonStateCounts.inactive,
    ozonOutOfStock: ozonStateCounts.outOfStock,
    yandexArchived: yandexStateCounts.archived,
    yandexInactive: yandexStateCounts.inactive,
    yandexOutOfStock: yandexStateCounts.outOfStock,
    usdRate: rate,
    priceMaster: await getPriceMasterSnapshotMetaFast(),
    sourceError: "",
    noSupplierAlerts: [],
    page,
    pageSize,
    total,
    hasMore: offset + pageSlice.length < total,
    items,
  };
  setWarehouseFastPageCache(cached.key, result);
  return result;
}

async function appendHistory(syncResult) {
  await fs.mkdir(dataDir, { recursive: true });
  const lines = syncResult.changes.map((change) =>
    JSON.stringify({
      syncId: syncResult.syncId,
      createdAt: syncResult.createdAt,
      type: change.type,
      article: change.current?.article || change.previous?.article || null,
      barcode: change.current?.barcode || change.previous?.barcode || null,
      name: change.current?.name || change.previous?.name || null,
      partnerId: change.current?.partnerId || change.previous?.partnerId || null,
      partnerName: change.current?.partnerName || change.previous?.partnerName || null,
      oldPrice: change.previous?.price ?? null,
      newPrice: change.current?.price ?? null,
      oldActive: change.previous?.active ?? null,
      newActive: change.current?.active ?? null,
      previousDocDate: change.previous?.docDate || null,
      currentDocDate: change.current?.docDate || null,
    }),
  );

  if (lines.length) {
    await fs.appendFile(historyPath, `${lines.join("\n")}\n`, "utf8");
  }
}

async function readHistory(limit = 300) {
  try {
    const content = await fs.readFile(historyPath, "utf8");
    const lines = content.trim().split("\n").filter(Boolean);
    return lines
      .slice(-limit)
      .reverse()
      .map((line) => JSON.parse(line));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function offerKey(row) {
  const identity = [row.article || "", row.barcode || "", row.name || ""]
    .map((value) => String(value).trim().toLowerCase())
    .join("|");
  return `${row.partnerId}:${identity}`;
}

async function getCurrentOffers(connection) {
  const [rows] = await connection.query(`
    WITH latest_docs AS (
      SELECT PartnerID, MAX(DocDate) AS LatestDocDate
      FROM OfferDocs
      GROUP BY PartnerID
    ),
    latest_doc_ids AS (
      SELECT d.PartnerID, MAX(d.DocID) AS DocID
      FROM OfferDocs d
      JOIN latest_docs l
        ON l.PartnerID = d.PartnerID
       AND l.LatestDocDate = d.DocDate
      GROUP BY d.PartnerID
    )
    SELECT
      r.RowID AS rowId,
      r.NativeID AS article,
      r.BarCode AS barcode,
      r.NativeName AS name,
      r.ProductID AS productId,
      r.NativePrice AS price,
      r.Active AS active,
      r.IsNew AS isNew,
      r.Ignored AS ignored,
      d.DocDate AS docDate,
      d.PartnerID AS partnerId,
      p.PartnerName AS partnerName
    FROM latest_doc_ids ld
    JOIN OfferDocs d ON d.DocID = ld.DocID
    JOIN OfferRows r ON r.DocID = d.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE r.Ignored = 0
    ORDER BY d.DocDate DESC, p.PartnerName, r.NativeName, r.RowID DESC
  `);

  return rows.map((row) => ({
    ...row,
    key: offerKey(row),
    price: Number(row.price || 0),
    active: Boolean(row.active),
    isNew: Boolean(row.isNew),
    ignored: Boolean(row.ignored),
  }));
}

function compareSnapshots(previousItems, currentOffers) {
  const currentItems = {};
  const changes = [];

  for (const offer of currentOffers) {
    if (!currentItems[offer.key]) {
      currentItems[offer.key] = offer;
    }
  }

  for (const offer of Object.values(currentItems)) {
    const previous = previousItems[offer.key];

    if (!previous) {
      changes.push({ type: "new", current: offer });
      continue;
    }

    if (Number(previous.price) !== Number(offer.price)) {
      changes.push({
        type: "price_changed",
        previous,
        current: offer,
        delta: Number(offer.price) - Number(previous.price),
      });
    }

    if (Boolean(previous.active) !== Boolean(offer.active)) {
      changes.push({
        type: offer.active ? "returned" : "inactive",
        previous,
        current: offer,
      });
    }
  }

  for (const [key, previous] of Object.entries(previousItems || {})) {
    if (!currentItems[key]) {
      changes.push({ type: "missing", previous });
    }
  }

  changes.sort((a, b) => {
    const rank = { missing: 0, price_changed: 1, new: 2, inactive: 3, returned: 4 };
    return (rank[a.type] ?? 9) - (rank[b.type] ?? 9);
  });

  return { currentItems, changes };
}

app.get("/api/health", async (_request, response) => {
  const health = await collectHealthDetails({ deep: true });
  response.status(health.ok ? 200 : 503).json(health);
});

app.get("/api/summary", async (_request, response, next) => {
  try {
    const snapshot = await readSnapshot();
    const [[products], [offerDocs], [latestDoc], [partners]] = await Promise.all([
      pool.query("SELECT COUNT(*) AS count FROM Products WHERE ProductID <> 0"),
      pool.query("SELECT COUNT(*) AS count FROM OfferDocs"),
      pool.query("SELECT MAX(DocDate) AS docDate FROM OfferDocs"),
      pool.query("SELECT COUNT(DISTINCT PartnerID) AS count FROM OfferDocs"),
    ]);

    const changeCounts = (snapshot.changes || []).reduce((acc, change) => {
      acc[change.type] = (acc[change.type] || 0) + 1;
      return acc;
    }, {});

    response.json({
      products: products[0].count,
      offerDocs: offerDocs[0].count,
      partners: partners[0].count,
      latestDocDate: latestDoc[0].docDate,
      snapshotCreatedAt: snapshot.createdAt,
      snapshotItems: Object.keys(snapshot.items || {}).length,
      changeCounts,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/products", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 100, 500);
    const search = String(request.query.search || "").trim();
    const params = [];
    let where = "WHERE p.ProductID <> 0";

    if (search) {
      where += " AND (p.ProductName LIKE ? OR p.ExtID LIKE ?)";
      params.push(likeSearch(search), likeSearch(search));
    }

    params.push(limit);
    const [rows] = await pool.query(
      `
      SELECT
        p.ProductID AS id,
        p.ProductName AS name,
        p.SalePrice AS salePrice,
        p.Stor AS stock,
        p.ExtID AS externalId,
        p.Vol AS volume,
        t.ProductTypeNameShort AS type,
        pack.PackName AS pack
      FROM Products p
      LEFT JOIN ProductTypes t ON t.ProductTypeID = p.ProductTypeID
      LEFT JOIN Packs pack ON pack.PackID = p.PackID
      ${where}
      ORDER BY p.ProductName
      LIMIT ?
      `,
      params,
    );

    response.json(rows);
  } catch (error) {
    next(error);
  }
});

function priceMasterSnapshotRaw(row = {}) {
  const raw = row.raw;
  return raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
}

function priceMasterSnapshotPartner(row = {}) {
  const raw = priceMasterSnapshotRaw(row);
  const id = cleanText(row.partnerId || raw.partnerId || raw.PartnerID || "");
  const name = cleanText(row.partnerName || raw.partnerName || raw.PartnerName || raw.name || "");
  if (!id && !name) return null;
  return { id, partnerId: id, name, partnerName: name };
}

function priceMasterSnapshotOffer(row = {}, usdRate) {
  const raw = priceMasterSnapshotRaw(row);
  const currency = cleanText(row.currency || raw.priceCurrency || raw.currency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
  const price = row.price ?? raw.price ?? raw.NativePrice ?? 0;
  const normalized = normalizePriceMasterPrice(price, usdRate, currency);
  const rowId = cleanText(row.rowId || raw.rowId || raw.RowID || row.id);
  const article = cleanText(row.article || raw.article || raw.NativeID || raw.offerId || "");
  const name = cleanText(row.nativeName || raw.name || raw.NativeName || "");
  const partner = priceMasterSnapshotPartner(row) || {};
  const docDate = row.docDate instanceof Date
    ? row.docDate.toISOString()
    : cleanText(row.docDate || raw.docDate || raw.DocDate || "");

  return {
    rowId,
    article,
    offerId: article,
    barcode: cleanText(raw.barcode || raw.BarCode || ""),
    name,
    productId: cleanText(raw.productId || raw.ProductID || ""),
    active: row.active !== false,
    isNew: Boolean(raw.isNew || raw.IsNew),
    ignored: Boolean(raw.ignored || raw.Ignored),
    docDate,
    partnerId: partner.partnerId || "",
    partnerName: partner.partnerName || "",
    priceCurrency: normalized.sourceCurrency,
    source: "postgres_snapshot",
    ...normalized,
  };
}

async function searchPriceMasterSnapshotPartners(query, limit = 25) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const q = cleanText(query);
  if (!q) return [];
  const take = Math.min(Math.max(Number(limit) * 4, 40), 320);
  try {
    const rows = await prisma.priceMasterSnapshotItem.findMany({
      where: {
        active: true,
        partnerName: { contains: q, mode: "insensitive" },
      },
      select: { partnerId: true, partnerName: true, raw: true },
      orderBy: [{ partnerName: "asc" }],
      take,
    });
    const unique = new Map();
    for (const row of rows) {
      const partner = priceMasterSnapshotPartner(row);
      if (!partner?.name) continue;
      const key = partner.partnerId || normalizeSupplierName(partner.name);
      if (!unique.has(key)) unique.set(key, partner);
      if (unique.size >= limit) break;
    }
    return Array.from(unique.values());
  } catch (error) {
    logger.warn("PriceMaster snapshot partner search failed, trying live", { detail: error?.message || String(error) });
    return null;
  }
}

async function searchPriceMasterSnapshotOffers({ search = "", partner = "", limit = 150, usdRate } = {}) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const q = cleanText(search);
  const partnerId = cleanText(partner);
  const take = cleanLimit(limit, 150, 500);
  const and = [{ active: true }];

  if (q) {
    and.push({
      OR: [
        { article: { contains: q, mode: "insensitive" } },
        { nativeName: { contains: q, mode: "insensitive" } },
        { partnerName: { contains: q, mode: "insensitive" } },
      ],
    });
  }

  if (partnerId) {
    and.push({ partnerId });
  }

  try {
    const rows = await prisma.priceMasterSnapshotItem.findMany({
      where: { AND: and },
      orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
      take,
    });
    return rows.map((row) => priceMasterSnapshotOffer(row, usdRate));
  } catch (error) {
    logger.warn("PriceMaster snapshot offer search failed, trying live", { detail: error?.message || String(error) });
    return null;
  }
}

app.get("/api/partners/search", async (request, response, next) => {
  try {
    const q = String(request.query.q || "").trim();
    const limit = cleanLimit(request.query.limit, 25, 80);
    if (!q) {
      return response.json({ items: [] });
    }

    const cacheKey = `partners:${q.toLowerCase()}:${limit}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    const snapshotRows = await searchPriceMasterSnapshotPartners(q, limit);
    if (snapshotRows) {
      const payload = { items: snapshotRows, source: "postgres_snapshot" };
      setPriceMasterSearchCache(cacheKey, payload);
      return response.json(payload);
    }

    const [rows] = await pool.query(
      `
      SELECT PartnerID AS id, PartnerName AS name
      FROM Partners
      WHERE PartnerName IS NOT NULL AND TRIM(PartnerName) <> '' AND PartnerName LIKE ?
      ORDER BY PartnerName ASC
      LIMIT ?
      `,
      [likeSearch(q), limit],
    );

    const payload = { items: rows };
    setPriceMasterSearchCache(cacheKey, payload);
    response.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon/brands/suggest", async (request, response, next) => {
  try {
    const query = cleanText(request.query.q);
    const categoryId = Number(request.query.categoryId || 0);
    const target = cleanText(request.query.target || "ozon");
    const limit = cleanLimit(request.query.limit, 20, 100);
    if (!query) return response.json({ brands: [] });
    if (!categoryId) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    const categories = await getOzonCategoryList(account);
    const selectedCategory = categories.find((item) => Number(item.id) === categoryId);
    const descriptionTypeId = Number(selectedCategory?.descriptionTypeId || 0);
    if (!descriptionTypeId) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }

    const payload = {
      attribute_id: 85,
      description_category_id: categoryId,
      type_id: descriptionTypeId,
      language: "DEFAULT",
      limit,
      last_value_id: 0,
      value: query,
    };
    const data = await ozonRequest("/v1/description-category/attribute/values", payload, account);
    const raw = Array.isArray(data.result)
      ? data.result
      : Array.isArray(data.result?.values)
        ? data.result.values
        : data.values || [];
    const brands = Array.isArray(raw)
      ? raw
          .map((item) => cleanText(item.value || item.name))
          .filter(Boolean)
          .slice(0, 40)
      : [];
    if (!brands.length) {
      const fallback = await listBrandFallbackCandidates(query, Math.min(limit, 40));
      return response.json({ brands: fallback, source: "fallback" });
    }
    response.json({ brands, source: "ozon" });
  } catch (error) {
    logger.warn("ozon brand suggest failed", { detail: error?.message || String(error) });
    const fallback = await listBrandFallbackCandidates(request.query.q, 40);
    response.json({ brands: fallback, source: "fallback" });
  }
});

app.get("/api/ozon/categories/suggest", async (request, response, next) => {
  try {
    const query = cleanText(request.query.q);
    const target = cleanText(request.query.target || "ozon");
    if (query.length < 2) return response.json({ categories: [] });
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) return response.json({ categories: [] });
    const all = await getOzonCategoryList(account);
    const q = normalizeSupplierName(query);
    const categories = all
      .filter((item) => normalizeSupplierName(item.name).includes(q))
      .slice(0, 50);
    response.json({ categories });
  } catch (error) {
    logger.warn("ozon category suggest failed", { detail: error?.message || String(error) });
    response.json({ categories: [] });
  }
});

app.get("/api/ozon/categories/:id/attributes-template", async (request, response, next) => {
  try {
    const categoryId = Number(request.params.id || 0);
    const target = cleanText(request.query.target || "ozon");
    if (!categoryId) return response.json({ template: [] });
    const account = getOzonAccountByTarget(target) || getOzonAccountByTarget("ozon");
    if (!account) return response.json({ template: [] });
    const categories = await getOzonCategoryList(account);
    const selectedCategory = categories.find((item) => Number(item.id) === categoryId);
    const descriptionTypeId = Number(selectedCategory?.descriptionTypeId || 0);
    const data = await ozonRequest("/v1/description-category/attribute", {
      description_category_id: categoryId,
      ...(descriptionTypeId ? { type_id: descriptionTypeId } : {}),
      language: "DEFAULT",
    }, account);
    const rows = data.result || data.attributes || [];
    response.json({ template: buildOzonAttributesTemplate(rows) });
  } catch (error) {
    logger.warn("ozon attribute template failed", { detail: error?.message || String(error) });
    response.json({ template: [] });
  }
});

app.get("/api/offers", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 150, 500);
    const search = String(request.query.search || "").trim();
    const partner = String(request.query.partner || "").trim();
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const cacheKey = `offers:${search.toLowerCase()}:${partner}:${limit}:${usdRate.toFixed(4)}`;
    const cached = getPriceMasterSearchCache(cacheKey);
    if (cached) return response.json(cached);

    const snapshotRows = await searchPriceMasterSnapshotOffers({
      search,
      partner,
      limit,
      usdRate,
    });
    if (snapshotRows) {
      setPriceMasterSearchCache(cacheKey, snapshotRows);
      return response.json(snapshotRows);
    }

    const params = [];
    const conditions = ["r.Ignored = 0"];

    if (search) {
      conditions.push("(r.NativeName LIKE ? OR r.NativeID LIKE ? OR r.BarCode LIKE ?)");
      params.push(likeSearch(search), likeSearch(search), likeSearch(search));
    }

    if (partner) {
      conditions.push("d.PartnerID = ?");
      params.push(Number(partner));
    }

    params.push(limit);
    const [rows] = await pool.query(
      `
      SELECT
        r.RowID AS rowId,
        r.NativeID AS article,
        r.BarCode AS barcode,
        r.NativeName AS name,
        r.ProductID AS productId,
        r.NativePrice AS price,
        r.Active AS active,
        r.IsNew AS isNew,
        d.DocDate AS docDate,
        d.PartnerID AS partnerId,
        p.PartnerName AS partnerName
      FROM OfferRows r
      JOIN OfferDocs d ON d.DocID = r.DocID
      LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
      WHERE ${conditions.join(" AND ")}
      ORDER BY d.DocDate DESC, r.RowID DESC
      LIMIT ?
      `,
      params,
    );

    const payload = rows.map((row) => ({ ...row, ...normalizePriceMasterPrice(row.price, usdRate) }));
    setPriceMasterSearchCache(cacheKey, payload);
    response.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/partners", async (_request, response, next) => {
  try {
    const rows = await listPriceMasterPartners();
    response.json(rows.map((row) => ({ id: row.partnerId, name: row.name })));
  } catch (error) {
    next(error);
  }
});

app.get("/api/changes", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const snapshot = await readSnapshot();
    response.json({
      createdAt: snapshot.createdAt,
      changes: (snapshot.changes || []).slice(0, limit),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 300, 2000);
    response.json({ history: await readHistory(limit) });
  } catch (error) {
    next(error);
  }
});

function auditEntryProductIds(entry = {}) {
  const details = entry.details || {};
  const values = [
    entry.productId,
    details.productId,
    details.id,
    details.entityId,
    details.productIds,
  ];
  return values
    .flatMap((value) => (Array.isArray(value) ? value : [value]))
    .map((value) => cleanText(value || ""))
    .filter(Boolean);
}

function auditEntrySearchText(entry = {}) {
  const details = entry.details || {};
  const links = Array.isArray(details.links) ? details.links : [];
  return [
    entry.user,
    entry.action,
    entry.productId,
    details.productId,
    details.productIds,
    details.offerId,
    details.name,
    details.article,
    details.supplierName,
    details.linkId,
    ...links.flatMap((link) => [link.article, link.supplierName, link.partnerId, link.keyword]),
  ]
    .flatMap((value) => (Array.isArray(value) ? value : [value]))
    .map((value) => cleanText(value || "").toLowerCase())
    .filter(Boolean)
    .join(" ");
}

function auditEntryMatchesFilters(entry = {}, filters = {}) {
  const username = cleanText(filters.user || "").toLowerCase();
  if (username && cleanText(entry.user || "").toLowerCase() !== username) return false;
  const action = cleanText(filters.action || "");
  if (action && action !== "all" && cleanText(entry.action || "") !== action) return false;
  const query = cleanText(filters.q || "").toLowerCase();
  if (query && !auditEntrySearchText(entry).includes(query)) return false;
  const fromMs = filters.dateFrom ? new Date(filters.dateFrom).getTime() : 0;
  const toMs = filters.dateTo ? new Date(filters.dateTo).getTime() : 0;
  const atMs = new Date(entry.at || 0).getTime();
  if (Number.isFinite(fromMs) && fromMs > 0 && atMs < fromMs) return false;
  if (Number.isFinite(toMs) && toMs > 0 && atMs > toMs + 24 * 60 * 60 * 1000 - 1) return false;
  return true;
}

function auditPostgresWhereFromFilters(filters = {}) {
  const where = {};
  const username = cleanText(filters.user || "");
  if (username) {
    where.username = { equals: username, mode: "insensitive" };
  }
  const action = cleanText(filters.action || "");
  if (action && action !== "all") {
    where.action = action;
  }
  const createdAt = {};
  const dateFrom = toDateOrNull(filters.dateFrom);
  if (dateFrom) createdAt.gte = dateFrom;
  const dateTo = toDateOrNull(filters.dateTo);
  if (dateTo) {
    dateTo.setHours(23, 59, 59, 999);
    createdAt.lte = dateTo;
  }
  if (Object.keys(createdAt).length) where.createdAt = createdAt;
  return where;
}

async function readAuditFiltered(filters = {}, limit = 200) {
  const normalizedLimit = cleanLimit(limit, 200, 1000);
  const hasFilters = Object.values(filters).some((value) => cleanText(value || ""));
  const query = cleanText(filters.q || "");

  if (shouldUsePostgresStorage()) {
    try {
      const take = query ? Math.min(5000, Math.max(normalizedLimit * 10, 1000)) : normalizedLimit;
      const rows = await getPrisma().auditLog.findMany({
        where: auditPostgresWhereFromFilters(filters),
        take,
        orderBy: { createdAt: "desc" },
      });
      const entries = rows.map(auditRowToEntry);
      return query
        ? entries.filter((entry) => auditEntryMatchesFilters(entry, filters)).slice(0, normalizedLimit)
        : entries;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read filtered audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }

  const audit = await readAudit(hasFilters ? Math.max(normalizedLimit * 5, 1000) : normalizedLimit);
  return hasFilters
    ? audit.filter((entry) => auditEntryMatchesFilters(entry, filters)).slice(0, normalizedLimit)
    : audit;
}

app.get("/api/audit-log", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const filters = {
      user: request.query.user,
      action: request.query.action,
      q: request.query.q || request.query.product,
      dateFrom: request.query.dateFrom,
      dateTo: request.query.dateTo,
    };
    const audit = await readAuditFiltered(filters, limit);
    response.json({ audit, total: audit.length, filters });
  } catch (error) {
    next(error);
  }
});

function publicLinkAuditEntry(entry = {}) {
  const details = entry.details || {};
  const action = cleanText(entry.action || "");
  const productIds = auditEntryProductIds(entry);
  const links = Array.isArray(details.links)
    ? details.links.map((link) => ({
        article: cleanText(link.article || ""),
        supplierName: cleanText(link.supplierName || ""),
        partnerId: cleanText(link.partnerId || ""),
        priceCurrency: cleanText(link.priceCurrency || ""),
        keyword: cleanText(link.keyword || ""),
      })).filter((link) => link.article || link.supplierName || link.partnerId)
    : [];
  return {
    at: entry.at || null,
    user: entry.user || "system",
    action,
    productIds,
    offerId: details.offerId || "",
    name: details.name || "",
    article: details.article || links[0]?.article || "",
    supplierName: details.supplierName || links[0]?.supplierName || "",
    linkId: details.linkId || "",
    links,
    linksCount: links.length || null,
  };
}

app.get("/api/warehouse/products/audit", async (request, response, next) => {
  try {
    const productIds = cleanText(request.query.productId || request.query.productIds || "")
      .split(",")
      .map((id) => cleanText(id))
      .filter(Boolean);
    if (!productIds.length) return response.json({ items: [] });
    const idSet = new Set(productIds.map(String));
    const limit = cleanLimit(request.query.limit, 10, 50);
    const actions = new Set(["warehouse.link.save", "warehouse.links.bulk_save", "warehouse.link.delete"]);
    const audit = await readAudit(Math.max(200, limit * 20));
    const items = audit
      .filter((entry) => actions.has(entry.action))
      .filter((entry) => auditEntryProductIds(entry).some((id) => idSet.has(String(id))))
      .slice(0, limit)
      .map(publicLinkAuditEntry);
    response.json({ items });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon/prices/preview", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 500, 5000);
    const multiplier = Number(request.query.multiplier || process.env.OZON_PRICE_MULTIPLIER || 1);
    const onlyChanged = String(request.query.onlyChanged || "true") !== "false";
    response.json(await buildOzonPricePreview({ limit, multiplier, onlyChanged }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/prices/send", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon prices were not sent because manual confirmation is required.",
      });
    }

    const items = Array.isArray(request.body.items) ? request.body.items : [];
    const prices = items
      .map((item) => buildOzonPricePayload(item))
      .filter((item) => item.offer_id && Number(item.price) > 0);

    if (!prices.length) {
      return response.status(400).json({ error: "No valid selected prices to send." });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const sent = await sendOzonPricePayloadChunks(account, prices);
    if (sent.failed.length) {
      return response.status(502).json({
        ok: false,
        sent: prices.length - sent.failed.length,
        failed: sent.failed.length,
        detail: sent.failed[0]?.error?.message || "Ozon price send failed",
        results: sent.results,
      });
    }

    response.json({
      ok: true,
      sent: prices.length,
      results: sent.results,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon/products/preview", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const search = String(request.query.search || "").trim();
    response.json(await buildOzonProductPreview({ limit, search }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/products/create", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon product was not created because manual confirmation is required.",
      });
    }

    const built = buildOzonManualProductItem(request.body);
    if (!built.ready) {
      return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const data = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
    response.json({ ok: true, target: account.id, item: built.item, result: data });
  } catch (error) {
    next(error);
  }
});

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

app.get("/api/system/status", requireAdmin, async (_request, response, next) => {
  try {
    const [health, dailySync, priceRetry, ozonQueue, operations] = await Promise.all([
      collectHealthDetails({ deep: true }).catch((error) => ({ ok: false, error: error?.message || String(error) })),
      readDailySyncState().catch((error) => ({ status: "error", error: error?.message || String(error) })),
      readPriceRetryQueue().catch((error) => ({ items: [], error: error?.message || String(error) })),
      readOzonUnarchiveQueue().catch((error) => ({ items: [], error: error?.message || String(error) })),
      readOperationJobs(20).catch((error) => ({ jobs: [], error: error?.message || String(error) })),
    ]);
    const jobs = Array.isArray(operations.jobs) ? operations.jobs : Array.isArray(operations) ? operations : [];
    response.json({
      ok: health.ok !== false,
      time: new Date().toISOString(),
      health,
      dailySync,
      queues: {
        priceRetry: { total: Array.isArray(priceRetry.items) ? priceRetry.items.length : 0, updatedAt: priceRetry.updatedAt || null, error: priceRetry.error || "" },
        ozonUnarchive: ozonUnarchiveQueuePublic(ozonQueue, { limit: 20 }),
      },
      operations: {
        latest: jobs.slice(0, 10),
        failed: jobs.filter((job) => ["failed", "error"].includes(cleanText(job.status).toLowerCase())).slice(0, 10),
        error: operations.error || "",
      },
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const limit = request.query.limit ? Number(request.query.limit) : Number.POSITIVE_INFINITY;
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const refreshPrices = request.query.refreshPrices === "true";
    const data = await buildWarehouseViewCached({ sync, limit, usdRate, refreshPrices });
    if (!sync && !refreshPrices) queueChangedWarehousePrices(data.products, "warehouse_view_detected_changed_prices");
    response.json(data);
  } catch (error) {
    logger.warn("warehouse view failed, serving snapshot if available", { detail: error?.message || String(error) });
    if (lastWarehouseViewSnapshot) {
      return response.json({
        ...lastWarehouseViewSnapshot,
        sourceError: error?.message || String(error),
        stale: true,
      });
    }
    next(error);
  }
});

app.get("/api/warehouse/brands", async (request, response, next) => {
  try {
    if (shouldUsePostgresStorage()) {
      const brands = await getWarehouseBrandListFromPostgres(getPrisma());
      return response.json({ brands, source: "postgres", count: brands.length });
    }
    const warehouse = await readWarehouse();
    const unique = new Map();
    for (const product of warehouse.products || []) {
      const b = resolveWarehouseBrand(product);
      if (!b) continue;
      const key = b.toLowerCase();
      if (!unique.has(key)) unique.set(key, b);
    }
    const brands = Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    response.json({ brands, count: brands.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/brands/refresh", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body?.limit || request.query?.limit, 300, 2000);
    clearWarehouseViewCache();
    warehouseBrandListCache = null;
    warehousePostgresBrandBackfillDone = false;
    let scanned = 0;
    let weakBrand = 0;
    if (shouldUsePostgresStorage()) {
      const prisma = getPrisma();
      const backfill = await ensureWarehousePostgresBrandsBackfilled(prisma, { force: true });
      const rows = await prisma.warehouseProduct.findMany({
        where: enabledWarehouseTargetWhere(),
        select: { id: true, name: true, brand: true, raw: true, marketplace: true, target: true, offerId: true, productId: true },
        take: limit,
        orderBy: [{ updatedAt: "desc" }],
      });
      scanned = rows.length;
      weakBrand = rows.filter((row) => !resolveWarehouseBrand({ ...(row.raw || {}), name: row.name, brand: row.brand, marketplace: row.marketplace, target: row.target, offerId: row.offerId, productId: row.productId })).length;
      const brands = await getWarehouseBrandListFromPostgres(prisma);
      return response.json({ ok: true, source: "postgres", scanned, weakBrand, brands, brandCount: brands.length, refreshed: Number(backfill.updated || 0), backfill, note: "brand_cache_cleared" });
    }
    const warehouse = await readWarehouse();
    scanned = Math.min(limit, (warehouse.products || []).length);
    weakBrand = (warehouse.products || []).slice(0, limit).filter((product) => !resolveWarehouseBrand(product)).length;
    const unique = new Map();
    for (const product of warehouse.products || []) {
      for (const brand of resolveWarehouseBrandCandidates(product)) {
        const key = brand.toLowerCase();
        if (!unique.has(key)) unique.set(key, brand);
      }
    }
    return response.json({ ok: true, source: "json", scanned, weakBrand, brands: Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" })), brandCount: unique.size, refreshed: 0, note: "brand_cache_cleared" });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/brands/index-status", requireAdmin, async (_request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.json({ ok: true, source: "json", indexed: 0, products: 0, ready: false });
    }
    const prisma = getPrisma();
    const [indexed, products] = await Promise.all([
      prisma.brandIndexItem.count().catch(() => 0),
      prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }).catch(() => 0),
    ]);
    response.json({ ok: true, source: "postgres", indexed, products, ready: indexed > 0, stale: indexed === 0 && products > 0 });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/brands/rebuild-index", requireAdmin, async (request, response, next) => {
  try {
    if (!shouldUsePostgresStorage()) {
      return response.status(400).json({ error: "Brand index requires PostgreSQL storage.", code: "postgres_required" });
    }
    const limit = cleanLimit(request.body?.limit || request.query?.limit, 100000, 200000);
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "brand-index-rebuild",
      title: operationTitle("brand-index-rebuild"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: { limit },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, job: operationJobPublic(job), source: "postgres" });
  } catch (error) {
    next(error);
  }
});

async function processOzonUnarchiveQueue({ source = "manual", limit = ozonUnarchiveQueueBatchLimit, force = false } = {}) {
  if (ozonUnarchiveQueueAutoRunning) {
    return {
      ok: true,
      skipped: true,
      reason: "already_running",
      selected: 0,
      result: { recovered: 0, unarchivePending: 0 },
      queue: ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 }),
      ...ozonUnarchiveQueueAutomationPublic(),
    };
  }
  ozonUnarchiveQueueProcessQueued = false;
  ozonUnarchiveQueueAutoRunning = true;
  const startedAt = new Date().toISOString();
  try {
    const normalizedLimit = Math.max(1, Math.min(5000, Math.round(Number(limit || ozonUnarchiveQueueBatchLimit) || ozonUnarchiveQueueBatchLimit)));
    const queue = await readOzonUnarchiveQueue();
    const publicQueue = ozonUnarchiveQueuePublic(queue, { limit: 5000 });
    const perTargetTaken = new Map();
    const dueItems = [];
    for (const item of publicQueue.items || []) {
      if (!item.due) continue;
      const target = cleanText(item.target) || "default";
      const available = force ? normalizedLimit : Math.max(0, Number(item.availableToday || 0) || 0);
      const taken = perTargetTaken.get(target) || 0;
      if (taken >= available) continue;
      dueItems.push(item);
      perTargetTaken.set(target, taken + 1);
      if (dueItems.length >= normalizedLimit) break;
    }
    const ids = dueItems.map((item) => cleanText(item.id)).filter(Boolean);
    if (!ids.length) {
      const empty = {
        ok: true,
        source,
        startedAt,
        finishedAt: new Date().toISOString(),
        selected: 0,
        result: { recovered: 0, unarchivePending: publicQueue.due, queueSize: publicQueue.total },
        queue: publicQueue,
      };
      ozonUnarchiveQueueAutoLastResult = {
        source,
        selected: 0,
        recovered: 0,
        unarchivePending: publicQueue.due,
        queueSize: publicQueue.total,
        at: empty.finishedAt,
      };
      return { ...empty, ...ozonUnarchiveQueueAutomationPublic() };
    }
    const products = await buildFreshWarehouseProducts(ids, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true });
    const result = await runSupplierRecoveryAutomation({ products }, {
      productIds: ids,
      source,
      force,
      forceOzonDailyLimit: Boolean(force),
    });
    const freshQueue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 });
    const finishedAt = new Date().toISOString();
    ozonUnarchiveQueueAutoLastResult = {
      source,
        selected: ids.length,
        force: Boolean(force),
        recovered: Number(result.recovered || 0),
      restoredStocks: Number(result.restoredStocks || 0),
      unarchived: Number(result.unarchived || 0),
      unarchivePending: Number(result.unarchivePending || 0),
      queuedByDailyLimit: Number(result.queuedByDailyLimit || 0),
      queueSize: Number(freshQueue.total || 0),
      errors: Array.isArray(result.errors) ? result.errors.length : 0,
      at: finishedAt,
    };
    logger.info("ozon unarchive queue processed", ozonUnarchiveQueueAutoLastResult);
    return {
      ok: true,
      source,
      startedAt,
      finishedAt,
      selected: ids.length,
      force: Boolean(force),
      productIds: ids,
      result,
      queue: freshQueue,
      ...ozonUnarchiveQueueAutomationPublic(),
    };
  } catch (error) {
    const finishedAt = new Date().toISOString();
    ozonUnarchiveQueueAutoLastResult = {
      source,
      selected: 0,
      error: error?.message || String(error),
      at: finishedAt,
    };
    logger.warn("ozon unarchive queue process failed", { source, detail: error?.message || String(error) });
    throw error;
  } finally {
    ozonUnarchiveQueueAutoRunning = false;
    ozonUnarchiveQueueAutoLastRunAt = new Date().toISOString();
  }
}

app.get("/api/ozon/unarchive-queue", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 1000, 5000);
    response.json({
      ...ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit }),
      ...ozonUnarchiveQueueAutomationPublic(),
    });
  } catch (error) {
    next(error);
  }
});

function enqueueOzonUnarchiveQueueProcess({ request, source, limit, force }) {
  if (ozonUnarchiveQueueAutoRunning || ozonUnarchiveQueueProcessQueued) return false;
  const queuedAt = new Date().toISOString();
  const queuedThroughBullmq = Boolean(marketplaceQueue);
  ozonUnarchiveQueueProcessQueued = true;
  ozonUnarchiveQueueAutoLastResult = {
    source,
    selected: 0,
    queued: true,
    at: queuedAt,
  };
  setTimeout(() => {
    queueMarketplaceJob("ozon-unarchive-queue-process", { source, limit, force }, { priority: 1 })
      .then((result) => {
        if (result && typeof result === "object" && Object.prototype.hasOwnProperty.call(result, "selected") && !result.skipped) {
          appendAudit(request, "ozon.unarchive_queue.process", {
            selected: result.selected || 0,
            productIds: result.productIds || [],
            result,
          }).catch((auditError) => logger.warn("ozon queue audit append failed", { detail: auditError?.message || String(auditError) }));
        }
      })
      .catch((error) => {
        ozonUnarchiveQueueProcessQueued = false;
        ozonUnarchiveQueueAutoLastResult = {
          source,
          selected: 0,
          error: error?.message || String(error),
          at: new Date().toISOString(),
        };
        logger.warn("ozon queue background enqueue failed", { detail: error?.message || String(error) });
      })
      .finally(() => {
        if (!queuedThroughBullmq && !ozonUnarchiveQueueAutoRunning) {
          ozonUnarchiveQueueProcessQueued = false;
        }
      });
  }, 0);
  return true;
}

app.post("/api/ozon/unarchive-queue/process", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body?.limit || request.query?.limit, ozonUnarchiveQueueBatchLimit, 1000);
    const source = "ozon_unarchive_queue_manual";
    const accepted = enqueueOzonUnarchiveQueueProcess({ request, source, limit, force: true });
    const queue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 1000 });
    response.status(202).json({
      ok: true,
      accepted,
      queued: accepted,
      skipped: !accepted,
      reason: accepted ? null : "already_running",
      source,
      limit,
      queue,
      ...queue,
      ...ozonUnarchiveQueueAutomationPublic(),
    });
    appendAudit(request, "ozon.unarchive_queue.queued", {
      accepted,
      source,
      limit,
      force: true,
    }).catch((auditError) => logger.warn("ozon queue audit append failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

async function repairWarehouseProductGroup(productId, request = null) {
  const initialWarehouse = await readWarehouse();
  const seed = (initialWarehouse.products || []).find((product) => String(product.id) === String(productId));
  if (!seed) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    throw error;
  }
  const initialGroup = expandWarehouseProductsToGroups(initialWarehouse.products || [], [seed]);
  const initialIds = initialGroup.map((product) => String(product.id)).filter(Boolean);
  return withWarehouseProductMutationLock(initialIds, async () => {
    const warehouse = await readWarehouse();
    const currentSeed = (warehouse.products || []).find((product) => String(product.id) === String(productId));
    if (!currentSeed) {
      const error = new Error("Warehouse product not found.");
      error.statusCode = 404;
      throw error;
    }
    const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [currentSeed]);
    const productIds = groupProducts.map((product) => String(product.id)).filter(Boolean);
    const syncResult = syncWarehouseProductGroupLinks(groupProducts, { now: new Date().toISOString(), username: requestUsername(request) });
    if ((syncResult.changedProducts || []).length) {
      await writeWarehouseProductPatch(syncResult.changedProducts, { reason: "warehouse_product_repair_links_sync" });
    }
    const priceResult = await sendWarehousePrices({
      productIds,
      dryRun: false,
      force: true,
      onlyChanged: false,
      refreshMarketplacePrices: true,
      livePriceMaster: true,
      marketplace: "all",
    });
    const freshProducts = await buildFreshWarehouseProducts(productIds, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true });
    const recoveryResult = await runSupplierRecoveryAutomation({ products: freshProducts }, { productIds, source: "product_repair", force: true });
    const diagnostics = await buildWarehouseSkuDiagnostics(currentSeed.offerId || currentSeed.sku || currentSeed.productId || currentSeed.id, { limit: 50, auditLimit: 30 });
    const payload = {
      ok: true,
      productIds,
      linksSynced: (syncResult.changedProducts || []).length,
      priceSent: Number(priceResult.sent || priceResult.items?.length || 0) || 0,
      stockSent: Number(priceResult.stockSent || recoveryResult.restoredStocks || 0) || 0,
      unarchiveStatus: recoveryResult.unarchivePending ? "pending" : "done",
      pending: Number(recoveryResult.unarchivePending || 0) > 0,
      errors: [...(priceResult.failed || []), ...(recoveryResult.errors || [])],
      nextRetryAt: recoveryResult.nextRetryAt || null,
      priceResult,
      recoveryResult,
      diagnostics,
    };
    appendAudit(request || { session: { username: "system", role: "admin" } }, "warehouse.product.repair", {
      productId: currentSeed.id,
      offerId: currentSeed.offerId,
      productIds,
      result: payload,
    }).catch((auditError) => logger.warn("product repair audit append failed", { detail: auditError?.message || String(auditError) }));
    return payload;
  });
}

app.post("/api/warehouse/products/:id/repair", requireAdmin, async (request, response, next) => {
  try {
    const initialWarehouse = await readWarehouse();
    const seed = (initialWarehouse.products || []).find((product) => String(product.id) === String(request.params.id));
    if (!seed) return response.status(404).json({ error: "Warehouse product not found." });
    const initialGroup = expandWarehouseProductsToGroups(initialWarehouse.products || [], [seed]);
    const initialIds = initialGroup.map((product) => String(product.id)).filter(Boolean);
    return await withWarehouseProductMutationLock(initialIds, async () => {
      const warehouse = await readWarehouse();
      const currentSeed = (warehouse.products || []).find((product) => String(product.id) === String(request.params.id));
      if (!currentSeed) return response.status(404).json({ error: "Warehouse product not found." });
      const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [currentSeed]);
      const productIds = groupProducts.map((product) => String(product.id)).filter(Boolean);
      const syncResult = syncWarehouseProductGroupLinks(groupProducts, { now: new Date().toISOString(), username: requestUsername(request) });
      if ((syncResult.changedProducts || []).length) {
        await writeWarehouseProductPatch(syncResult.changedProducts, { reason: "warehouse_product_repair_links_sync" });
      }
      const priceResult = await sendWarehousePrices({
        productIds,
        dryRun: false,
        force: true,
        onlyChanged: false,
        refreshMarketplacePrices: true,
        livePriceMaster: true,
        marketplace: "all",
      });
      const freshProducts = await buildFreshWarehouseProducts(productIds, { refreshPrices: true, livePriceMaster: true, batchPriceMaster: true });
      const recoveryResult = await runSupplierRecoveryAutomation({ products: freshProducts }, { productIds, source: "product_repair", force: true });
      const diagnostics = await buildWarehouseSkuDiagnostics(currentSeed.offerId || currentSeed.sku || currentSeed.productId || currentSeed.id, { limit: 50, auditLimit: 30 });
      const payload = {
        ok: true,
        productIds,
        linksSynced: (syncResult.changedProducts || []).length,
        priceSent: Number(priceResult.sent || priceResult.items?.length || 0) || 0,
        stockSent: Number(priceResult.stockSent || recoveryResult.restoredStocks || 0) || 0,
        unarchiveStatus: recoveryResult.unarchivePending ? "pending" : "done",
        pending: Number(recoveryResult.unarchivePending || 0) > 0,
        errors: [...(priceResult.failed || []), ...(recoveryResult.errors || [])],
        nextRetryAt: recoveryResult.nextRetryAt || null,
        priceResult,
        recoveryResult,
        diagnostics,
      };
      response.json(payload);
      appendAudit(request, "warehouse.product.repair", {
        productId: currentSeed.id,
        offerId: currentSeed.offerId,
        productIds,
        result: payload,
      }).catch((auditError) => logger.warn("product repair audit append failed", { detail: auditError?.message || String(auditError) }));
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/diagnostics", async (request, response, next) => {
  try {
    const sku = cleanText(request.query.sku || request.query.q || request.query.offerId || "");
    const limit = cleanLimit(request.query.limit, 50, 100);
    const auditLimit = cleanLimit(request.query.auditLimit, 30, 100);
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    response.json(await buildWarehouseSkuDiagnostics(sku, { limit, auditLimit, usdRate }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/page", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const page = Math.max(1, Number(request.query.page || 1) || 1);
    const pageSize = Math.min(250, Math.max(10, Number(request.query.pageSize || 60) || 60));
    const q = cleanText(request.query.q || "").toLowerCase();
    const autoOnly = request.query.autoOnly === "true";
    const linked = cleanText(request.query.linked || "all");
    const marketplace = cleanText(request.query.marketplace || "all");
    const stateCode = cleanText(request.query.state || "all");
    const brandFilter = cleanText(request.query.brand || "");
    const grouped = parseBooleanSetting(request.query.grouped, false);

    if (shouldUsePostgresStorage() && !sync && !refreshPrices) {
      const fastPage = await buildFastWarehousePage({
        page,
        pageSize,
        usdRate,
        filters: {
          q,
          autoOnly,
          linked,
          marketplace,
          state: stateCode,
          brand: brandFilter,
        },
      });
      queueChangedWarehousePrices(fastPage.items, "warehouse_page_detected_changed_prices");
      if (grouped) {
        const groups = buildWarehousePageProductGroups(fastPage.items);
        return response.json({
          ...fastPage,
          grouped: true,
          rowTotal: fastPage.total,
          total: groups.length,
          groups,
          items: groups,
        });
      }
      return response.json(fastPage);
    }

    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    let rows = Array.isArray(data.products) ? data.products.slice() : [];
    if (!sync && !refreshPrices) queueChangedWarehousePrices(rows, "warehouse_page_detected_changed_prices");

    const filters = {
      q,
      autoOnly,
      linked,
      marketplace,
      state: stateCode,
      brand: brandFilter,
    };
    rows = sortWarehouseProductsForSearch(
      preferWarehousePrimaryIdentityMatches(
        rows.filter((item) => warehousePageProductMatches(item, filters)),
        filters,
      ),
      filters,
    );

    const total = rows.length;
    const offset = (page - 1) * pageSize;
    const items = rows.slice(offset, offset + pageSize).map((item) => ({
      ...item,
      autoPriceEnabled: item.autoPriceEnabled !== false,
      links: Array.isArray(item.links) ? item.links : [],
      suppliers: Array.isArray(item.suppliers) ? item.suppliers : [],
      selectedSupplier: item.selectedSupplier || null,
      noSupplierAutomation: item.noSupplierAutomation || {},
      marketplaceState: item.marketplaceState || {},
      partial: false,
    }));

    const groups = grouped ? buildWarehousePageProductGroups(items) : null;
    response.json({
      createdAt: data.createdAt,
      updatedAt: data.updatedAt || null,
      totalAll: data.total,
      ready: data.ready,
      changed: data.changed,
      withoutSupplier: data.withoutSupplier,
      linkedArchived: data.linkedArchived || 0,
      ozonArchived: data.ozonArchived || 0,
      ozonInactive: data.ozonInactive || 0,
      ozonOutOfStock: data.ozonOutOfStock || 0,
      usdRate: data.usdRate,
      priceMaster: data.priceMaster || await getPriceMasterSnapshotMeta(),
      sourceError: data.sourceError || "",
      noSupplierAlerts: Array.isArray(data.noSupplierAlerts) ? data.noSupplierAlerts.slice(0, 10) : [],
      page,
      pageSize,
      total: grouped ? groups.length : total,
      rowTotal: total,
      grouped,
      groups: groups || undefined,
      hasMore: offset + items.length < total,
      items: groups || items,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/group-detail", async (request, response, next) => {
  try {
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    const group = cleanText(request.query.group || "");
    if (!group) return response.status(400).json({ error: "Укажите group." });
    const detail = await buildWarehouseGroupDetail(group, {
      usdRate,
      filters: {
        marketplace: cleanText(request.query.marketplace || "all"),
        state: cleanText(request.query.state || "all"),
      },
    });
    if (!detail) return response.status(404).json({ error: "Карточка не найдена." });
    response.json(detail);
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/:id/detail", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
    if (shouldUsePostgresStorage() && !sync && !refreshPrices) {
      const detail = await buildWarehouseProductDetailFromPostgres(request.params.id, { usdRate });
      if (!detail) return response.status(404).json({ error: "Товар не найден." });
      return response.json(detail);
    }
    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    const product = (data.products || []).find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар не найден." });
    response.json({ product, createdAt: data.createdAt });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/no-supplier", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const data = await buildWarehouseViewCached({ sync, refreshPrices });
    response.json({
      createdAt: data.createdAt,
      total: data.total,
      withoutSupplier: data.withoutSupplier,
      alerts: buildNoSupplierAlerts(data.products, { limit: Number.POSITIVE_INFINITY }),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/suppliers", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplierSync = { ok: false, partners: 0, imported: 0, changed: false, error: null };
    if (request.query.refresh === "true") {
      try {
        const partners = await listPriceMasterPartners();
        const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
        supplierSync.ok = true;
        supplierSync.partners = partners.length;
        supplierSync.imported = syncedSuppliers.imported;
        supplierSync.changed = syncedSuppliers.changed;
        if (syncedSuppliers.changed) {
          await writeWarehouse(warehouse);
          logger.info("imported suppliers from PriceMaster via suppliers api", { imported: syncedSuppliers.imported });
        }
      } catch (error) {
        supplierSync.error = error.message;
        logger.warn("supplier import from PriceMaster in /api/suppliers failed", { detail: error.message });
      }
    }
    const autoReactivated = applySupplierAutoReactivate(warehouse);
    if (autoReactivated.length) {
      await writeWarehouse(warehouse);
      logger.info("supplier auto-reactivated from suppliers api", { count: autoReactivated.length, suppliers: autoReactivated });
    }
    const impactCounts = supplierImpactCountMap(warehouse, warehouse.suppliers || []);
    response.json({
      suppliers: (warehouse.suppliers || []).map((supplier) => ({
        ...supplier,
        impactProductCount: impactCounts.get(supplier.id) || 0,
      })),
      supplierSync,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/suppliers/:id/profile", requireAdmin, async (request, response, next) => {
  try {
    const supplierId = cleanText(request.params.id);
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const supplier = suppliers.find((item) => cleanText(item.id) === supplierId || cleanText(item.partnerId) === supplierId || normalizeSupplierName(item.name) === normalizeSupplierName(supplierId));
    if (!supplier) return response.status(404).json({ error: "Supplier not found.", code: "supplier_not_found" });
    const picking = await readSupplierPickingState();
    const rows = Object.values(picking.rows || {})
      .map(normalizeSupplierPickingRow)
      .filter((row) =>
        (supplier.partnerId && cleanText(row.partnerId) === cleanText(supplier.partnerId))
        || normalizeSupplierName(row.supplierName) === normalizeSupplierName(supplier.name)
      );
    const picked = rows.filter((row) => row.status === "picked");
    const missing = rows.filter((row) => row.status === "missing");
    const blocks = Object.values((await readSupplierCartState()).supplierBlocks || {})
      .filter((block) => cleanText(block.partnerId) === cleanText(supplier.partnerId));
    const averagePrice = rows.length ? rows.reduce((sum, row) => sum + Number(row.price || 0), 0) / rows.length : 0;
    response.json({
      ok: true,
      supplier,
      stats: {
        picked: picked.length,
        missing: missing.length,
        totalPurchases: rows.length,
        successRate: rows.length ? Math.round((picked.length / rows.length) * 100) : null,
        averagePrice,
      },
      blocks,
      history: rows.slice(0, 200),
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/suppliers/:id/profile", requireAdmin, async (request, response, next) => {
  try {
    const supplierId = cleanText(request.params.id);
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const index = suppliers.findIndex((item) => cleanText(item.id) === supplierId || cleanText(item.partnerId) === supplierId || normalizeSupplierName(item.name) === normalizeSupplierName(supplierId));
    if (index < 0) return response.status(404).json({ error: "Supplier not found.", code: "supplier_not_found" });
    suppliers[index] = normalizeManagedSupplier({
      ...suppliers[index],
      ...request.body,
      raw: {
        ...(suppliers[index].raw || {}),
        ...(request.body?.raw || {}),
      },
    });
    warehouse.suppliers = suppliers;
    await writeWarehouseJsonPayload(warehouse);
    warehousePostgresSuppliersCache = null;
    response.json({ ok: true, supplier: suppliers[index] });
  } catch (error) {
    next(error);
  }
});

app.get("/api/pricemaster/search", async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || request.query.search || "");
    const supplier = cleanText(request.query.supplier || "");
    const limit = cleanLimit(request.query.limit, 30, 100);
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;

    const mapRow = (row = {}) => {
      const priceCurrency = cleanText(row.priceCurrency || row.currency || row.sourceCurrency || "USD") || "USD";
      const normalizedPrice = normalizePriceMasterPrice(row.price ?? row.NativePrice ?? 0, usdRate, priceCurrency);
      const article = cleanText(row.article || row.NativeID || row.offerId || row.nativeId || "");
      const name = cleanText(row.name || row.NativeName || row.nativeName || "");
      const partnerName = cleanText(row.partnerName || row.PartnerName || row.supplierName || "");
      const rowId = cleanText(row.rowId || row.RowID || row.id || "");
      return {
        id: rowId || `${article}:${partnerName}:${name}`,
        rowId,
        article,
        supplierName: partnerName,
        partnerId: cleanText(row.partnerId || row.PartnerID || ""),
        keyword: name,
        name,
        price: normalizedPrice.price || Number(row.price || row.NativePrice || 0) || 0,
        originalPrice: normalizedPrice.originalPrice,
        currency: normalizedPrice.priceCurrency || priceCurrency,
        priceCurrency: normalizedPrice.priceCurrency || priceCurrency,
        available: row.available !== false && row.active !== false && Number(normalizedPrice.price || row.price || row.NativePrice || 0) > 0,
        active: row.active !== false,
        updatedAt: row.docDate || row.DocDate || row.updatedAt || null,
      };
    };

    const rows = [];
    try {
      const params = [];
      const conditions = ["r.Ignored = 0", "r.Active = 1"];
      if (q) {
        conditions.push("(r.NativeID LIKE ? OR r.NativeName LIKE ? OR r.BarCode LIKE ? OR p.PartnerName LIKE ?)");
        const like = likeSearch(q);
        params.push(like, like, like, like);
      }
      if (supplier) {
        conditions.push("p.PartnerName LIKE ?");
        params.push(likeSearch(supplier));
      }
      params.push(limit);
      const [liveRows] = await pool.query(
        `
        SELECT
          r.NativeID AS article,
          r.NativeName AS name,
          r.NativePrice AS price,
          r.Active AS active,
          r.RowID AS rowId,
          d.DocDate AS docDate,
          d.PartnerID AS partnerId,
          p.PartnerName AS partnerName
        FROM OfferRows r
        JOIN OfferDocs d ON d.DocID = r.DocID
        LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
        WHERE ${conditions.join(" AND ")}
        ORDER BY d.DocDate DESC, r.RowID DESC
        LIMIT ?
        `,
        params,
      );
      rows.push(...liveRows.map(mapRow));
    } catch (error) {
      logger.warn("PriceMaster search live query failed, using snapshot", { detail: error?.message || String(error) });
      const qLower = q.toLowerCase();
      const supplierLower = supplier.toLowerCase();
      const indexes = await getPriceMasterSnapshotIndexes();
      const candidates = [];
      for (const row of indexes.rows || []) {
        const mapped = mapRow(row);
        const haystack = [mapped.article, mapped.name, mapped.keyword, mapped.supplierName].join(" ").toLowerCase();
        if (qLower && !haystack.includes(qLower)) continue;
        if (supplierLower && !mapped.supplierName.toLowerCase().includes(supplierLower)) continue;
        candidates.push(mapped);
        if (candidates.length >= limit) break;
      }
      rows.push(...candidates);
    }

    const unique = new Map();
    for (const row of rows) {
      const key = [row.rowId, row.article, row.supplierName, row.keyword].join("|");
      if (!unique.has(key)) unique.set(key, row);
    }
    response.json({ ok: true, rows: Array.from(unique.values()).slice(0, limit), total: unique.size });
  } catch (error) {
    next(error);
  }
});

app.get("/api/live-status", async (_request, response, next) => {
  try {
    const [warehouseMeta, dailySync, priceMaster] = await Promise.all([
      getWarehouseMetaFast(),
      getDailySyncStatus().catch((error) => ({ error: error?.message || String(error) })),
      getPriceMasterSnapshotMetaFast().catch((error) => {
        logger.warn("live status PriceMaster meta failed", { detail: error?.message || String(error) });
        return { syncId: null, updatedAt: null, items: 0, changes: 0, error: error?.message || String(error) };
      }),
    ]);
    response.json({
      ok: true,
      now: new Date().toISOString(),
      warehouse: {
        updatedAt: warehouseMeta.updatedAt || warehouseMeta.createdAt || null,
        createdAt: warehouseMeta.createdAt || null,
        products: Number(warehouseMeta.products || 0),
        suppliers: Number(warehouseMeta.suppliers || 0),
        source: warehouseMeta.source || null,
      },
      priceMaster,
      dailySync: {
        updatedAt: dailySync.updatedAt || dailySync.lastRunAt || null,
        status: dailySync.status || "idle",
        running: Boolean(dailySync.running),
        lastRunAt: dailySync.lastRunAt || null,
        nextRunAt: dailySync.nextRunAt || null,
        error: dailySync.error || null,
      },
      autoSync: {
        running: Boolean(autoSyncRunning),
        nextRunAt: autoSyncNextRunAt || null,
      },
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/suppliers", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = normalizeManagedSupplier(request.body);
    if (!supplier.name) return response.status(400).json({ error: "Укажите название поставщика." });

    const index = warehouse.suppliers.findIndex((item) => item.id === supplier.id);
    const before = index >= 0 ? cloneAuditValue(warehouse.suppliers[index]) : null;
    if (index >= 0) {
      warehouse.suppliers[index] = normalizeManagedSupplier({
        ...warehouse.suppliers[index],
        ...supplier,
        articles: warehouse.suppliers[index].articles,
        createdAt: warehouse.suppliers[index].createdAt,
      });
    } else {
      warehouse.suppliers.push(supplier);
    }
    const after = index >= 0 ? warehouse.suppliers[index] : supplier;

    await appendAudit(request, "supplier.save", {
      id: supplier.id,
      name: supplier.name,
      priceCurrency: supplier.priceCurrency,
      oldValue: before,
      newValue: after,
    });
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
    queueImmediateAutoPricePush([], "supplier_save");
  } catch (error) {
    next(error);
  }
});

app.patch("/api/suppliers/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.id);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });
    const before = cloneAuditValue(supplier);

    Object.assign(supplier, {
      name: request.body.name !== undefined ? cleanText(request.body.name) : supplier.name,
      stopped: request.body.stopped !== undefined ? Boolean(request.body.stopped) : supplier.stopped,
      note: request.body.note !== undefined ? cleanText(request.body.note) : supplier.note,
      stopReason: request.body.stopReason !== undefined ? cleanText(request.body.stopReason) : supplier.stopReason,
      trustFactor: request.body.trustFactor !== undefined || request.body.trust_factor !== undefined
        ? normalizeSupplierTrustFactor(request.body.trustFactor ?? request.body.trust_factor, supplier.trustFactor || 100)
        : normalizeSupplierTrustFactor(supplier.trustFactor, 100),
      orderCutoffTime: request.body.orderCutoffTime !== undefined || request.body.order_cutoff_time !== undefined
        ? normalizeSupplierOrderCutoff(request.body.orderCutoffTime || request.body.order_cutoff_time)
        : normalizeSupplierOrderCutoff(supplier.orderCutoffTime || ""),
      reseller: request.body.reseller !== undefined ? Boolean(request.body.reseller) : Boolean(supplier.reseller),
      priceCurrency: request.body.priceCurrency !== undefined
        ? normalizeManagedSupplier({ priceCurrency: request.body.priceCurrency }).priceCurrency
        : (supplier.priceCurrency || "USD"),
      pricingMode: request.body.pricingMode !== undefined || request.body.pricing_mode !== undefined
        ? normalizeSupplierPricingMode(request.body)
        : normalizeSupplierPricingMode(supplier),
      stockOnly: request.body.pricingMode !== undefined || request.body.pricing_mode !== undefined
        ? normalizeSupplierPricingMode(request.body) === "stock_only"
        : normalizeSupplierPricingMode(supplier) === "stock_only",
      inactiveComment: request.body.inactiveComment !== undefined ? cleanText(request.body.inactiveComment) : (supplier.inactiveComment || ""),
      inactiveUntil: request.body.inactiveUntil !== undefined ? (cleanText(request.body.inactiveUntil) || null) : (supplier.inactiveUntil || null),
      inactiveUntilUnknown: request.body.inactiveUntilUnknown !== undefined ? Boolean(request.body.inactiveUntilUnknown) : Boolean(supplier.inactiveUntilUnknown),
      updatedAt: new Date().toISOString(),
    });

    if (!supplier.stopped) {
      supplier.stopReason = "";
      supplier.inactiveComment = "";
      supplier.inactiveUntil = null;
      supplier.inactiveUntilUnknown = false;
    } else if (!supplier.inactiveUntil) {
      supplier.inactiveUntilUnknown = true;
    }

    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.update", {
      id: supplier.id,
      stopped: supplier.stopped,
      name: supplier.name,
      priceCurrency: supplier.priceCurrency,
      oldValue: before,
      newValue: supplier,
    });
    response.json({ ok: true, warehouse: saved });
    const affectedProductIds = supplierImpactProductIds(warehouse, before, supplier);
    if (affectedProductIds.length) {
      queueMarketplaceJob("no-supplier-automation", { productIds: affectedProductIds }, { priority: 1 });
      queueMarketplaceJob("supplier-recovery-automation", { productIds: affectedProductIds }, { priority: 2 });
    }
    queueImmediateAutoPricePush([], "supplier_update");
  } catch (error) {
    next(error);
  }
});

app.delete("/api/suppliers/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const before = warehouse.suppliers.find((supplier) => supplier.id === request.params.id) || null;
    const affectedProductIds = supplierImpactProductIds(warehouse, before);
    warehouse.suppliers = warehouse.suppliers.filter((supplier) => supplier.id !== request.params.id);
    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.delete", { id: request.params.id, oldValue: before });
    response.json({ ok: true, warehouse: saved });
    if (affectedProductIds.length) {
      queueMarketplaceJob("no-supplier-automation", { productIds: affectedProductIds }, { priority: 1 });
    }
  } catch (error) {
    next(error);
  }
});

app.post("/api/suppliers/:id/articles", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.id);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });

    const article = normalizeSupplierArticle(request.body);
    if (!article.article) return response.status(400).json({ error: "Укажите артикул поставщика." });
    supplier.articles = Array.isArray(supplier.articles) ? supplier.articles : [];
    const index = supplier.articles.findIndex((item) => item.id === article.id);
    const before = index >= 0 ? cloneAuditValue(supplier.articles[index]) : null;
    if (index >= 0) supplier.articles[index] = article;
    else supplier.articles.push(article);

    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.article.save", { supplierId: supplier.id, article: article.article, oldValue: before, newValue: article });
    response.json({ ok: true, warehouse: saved });
    queueImmediateAutoPricePush([], "supplier_article_save");
  } catch (error) {
    next(error);
  }
});

app.delete("/api/suppliers/:supplierId/articles/:articleId", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const supplier = warehouse.suppliers.find((item) => item.id === request.params.supplierId);
    if (!supplier) return response.status(404).json({ error: "Поставщик не найден." });
    const before = (supplier.articles || []).find((article) => article.id === request.params.articleId) || null;
    supplier.articles = (supplier.articles || []).filter((article) => article.id !== request.params.articleId);
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
    await appendAudit(request, "supplier.article.delete", { supplierId: supplier.id, articleId: request.params.articleId, oldValue: before });
    queueMarketplaceJob("no-supplier-automation", {}, { priority: 1 });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const input = normalizeWarehouseProduct(request.body);
    if (!input.offerId) return response.status(400).json({ error: "Укажите артикул товара маркетплейса." });
    if (!input.name) return response.status(400).json({ error: "Укажите название товара." });

    const index = warehouse.products.findIndex(
      (product) => product.id === input.id || (product.target === input.target && product.offerId === input.offerId),
    );
    const before = index >= 0 ? cloneAuditValue(warehouse.products[index]) : null;
    if (index >= 0) {
      const current = warehouse.products[index];
      warehouse.products[index] = normalizeWarehouseProduct({
        ...current,
        ...input,
        productId: input.productId || current.productId,
        sku: input.sku || current.sku,
        productUrl: input.productUrl || current.productUrl,
        source: current.source || input.source,
        ozon: hasObjectData(input.ozon) ? input.ozon : current.ozon,
        yandex: hasObjectData(input.yandex) ? input.yandex : current.yandex,
        exports: { ...(current.exports || {}), ...(input.exports || {}) },
        aiImages: input.aiImages?.length ? input.aiImages : current.aiImages,
        priceHistory: current.priceHistory || [],
        links: current.links,
        createdAt: current.createdAt,
      });
    } else {
      warehouse.products.push(input);
    }

    await writeWarehouse(warehouse);
    const product = index >= 0 ? warehouse.products[index] : input;
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
    await appendAudit(request, index >= 0 ? "warehouse.product.save" : "warehouse.product.create", {
      productId: product.id,
      offerId: product.offerId,
      oldValue: before,
      newValue: product,
    });
    response.json({ ok: true, product: freshProduct || normalizeWarehouseProduct(product), warehouse });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    response.json({ product });
  } catch (error) {
    next(error);
  }
});

function isRetryableAiImageGenerationError(error = {}) {
  if (isOpenAiBillingLimitError(error)) return false;
  const status = Number(error.statusCode || error.status || 0);
  const code = cleanText(error.code || error.error?.code || error.cause?.code || error.cause?.cause?.code).toLowerCase();
  const detail = cleanText(
    error.message
      || error.error?.message
      || error.detail
      || error.cause?.message
      || error.cause?.cause?.message
      || String(error),
  ).toLowerCase();
  if ([408, 409, 425, 429].includes(status)) return true;
  if (status >= 500 && status < 600) return true;
  return Boolean(
    code.includes("timeout")
      || code.includes("rate_limit")
      || code.includes("temporar")
      || code.includes("overload")
      || code.includes("request_failed")
      || ["eai_again", "enotfound", "econnreset", "econnrefused", "etimedout", "und_err_connect_timeout", "und_err_headers_timeout", "und_err_body_timeout"].includes(code)
      || detail.includes("fetch failed")
      || detail.includes("connection error")
      || detail.includes("network")
      || detail.includes("dns")
      || detail.includes("eai_again")
      || detail.includes("temporar")
  );
}

async function generateOzonAiImageDraftWithRetry(product, options, request, loggerContext = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= aiImageGenerationAttempts; attempt += 1) {
    try {
      return await generateOzonAiImageDraft(product, options, request);
    } catch (error) {
      lastError = error;
      const retryable = isRetryableAiImageGenerationError(error);
      logger.warn("ai image draft generation attempt failed", {
        ...loggerContext,
        attempt,
        attempts: aiImageGenerationAttempts,
        retryable,
        detail: error?.message || String(error),
        code: error?.code,
        statusCode: error?.statusCode || error?.status,
      });
      if (!retryable || attempt >= aiImageGenerationAttempts) break;
      await sleep(aiImageGenerationRetryDelayMs * attempt);
    }
  }
  throw lastError;
}

const activeAiImageJobs = new Map();

function aiImageJobErrorPayload(error = {}) {
  return compactObject({
    detail: cleanText(
      error?.message
        || error?.error?.message
        || error?.detail
        || error?.cause?.message
        || error?.cause?.cause?.message
        || String(error),
    ),
    code: cleanText(error?.code || error?.error?.code || error?.cause?.code || error?.cause?.cause?.code),
    status: Number(error?.statusCode || error?.status || 0) || undefined,
    model: cleanText(error?.model || "gpt-image-2"),
    endpoint: cleanText(error?.endpoint || "https://codex.sale/v1/images/edits"),
  });
}

function normalizeAiImageJob(input = {}) {
  const status = cleanText(input.status || "queued").toLowerCase();
  const allowedStatus = new Set(["queued", "running", "completed", "failed", "partial"]);
  const variantTotal = Math.max(1, Math.min(5, Number(input.variantTotal || input.variant_total || 5) || 5));
  const draftIds = Array.isArray(input.draftIds || input.draft_ids)
    ? (input.draftIds || input.draft_ids).map(cleanText).filter(Boolean)
    : [];
  const job = compactObject({
    id: cleanText(input.id || input.jobId || input.job_id) || crypto.randomUUID(),
    productId: cleanText(input.productId || input.product_id),
    offerId: cleanText(input.offerId || input.offer_id),
    target: cleanText(input.target),
    batchId: cleanText(input.batchId || input.batch_id) || crypto.randomUUID(),
    status: allowedStatus.has(status) ? status : "queued",
    progress: Math.max(0, Math.min(100, Number(input.progress || 0) || 0)),
    variantIndex: Math.max(0, Math.min(variantTotal, Number(input.variantIndex || input.variant_index || 0) || 0)),
    variantTotal,
    draftIds,
    lastError: input.lastError || input.last_error || null,
    model: cleanText(input.model || "gpt-image-2"),
    endpoint: cleanText(input.endpoint || "https://codex.sale/v1/images/edits"),
    presetId: cleanText(input.presetId || input.preset_id),
    presetLabel: cleanText(input.presetLabel || input.preset_label),
    sourceImageUrl: cleanText(input.sourceImageUrl || input.source_image_url),
    prompt: cleanText(input.prompt),
    createdBy: cleanText(input.createdBy || input.created_by || "system"),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    startedAt: input.startedAt || input.started_at || null,
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
    finishedAt: input.finishedAt || input.finished_at || null,
  });
  job.draftIds = draftIds;
  return job;
}

async function readAiImageJobs() {
  try {
    const payload = JSON.parse(await fs.readFile(aiImageJobsPath, "utf8"));
    const jobs = Array.isArray(payload?.jobs) ? payload.jobs : (Array.isArray(payload) ? payload : []);
    return jobs.map(normalizeAiImageJob).filter((job) => job.id && job.productId);
  } catch (error) {
    if (error.code === "ENOENT") return [];
    logger.warn("read ai image jobs failed", { detail: error?.message || String(error) });
    return [];
  }
}

async function writeAiImageJobs(jobs = []) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = (Array.isArray(jobs) ? jobs : [])
    .map(normalizeAiImageJob)
    .filter((job) => job.id && job.productId)
    .sort((left, right) => String(right.updatedAt || "").localeCompare(String(left.updatedAt || "")))
    .slice(0, 250);
  const temporaryPath = `${aiImageJobsPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify({ updatedAt: new Date().toISOString(), jobs: normalized }, null, 2), "utf8");
  await fs.rename(temporaryPath, aiImageJobsPath);
  return normalized;
}

async function upsertAiImageJob(jobInput = {}) {
  const job = normalizeAiImageJob({ ...jobInput, updatedAt: new Date().toISOString() });
  const jobs = await readAiImageJobs();
  const index = jobs.findIndex((item) => item.id === job.id);
  if (index >= 0) jobs[index] = job;
  else jobs.unshift(job);
  await writeAiImageJobs(jobs);
  return job;
}

async function findActiveAiImageJob(productId) {
  const id = cleanText(productId);
  if (!id) return null;
  const jobs = await readAiImageJobs();
  return jobs.find((job) => job.productId === id && ["queued", "running"].includes(job.status) && activeAiImageJobs.has(job.id)) || null;
}

function publicAiImageJob(jobInput = {}) {
  const job = normalizeAiImageJob(jobInput);
  return compactObject({
    id: job.id,
    jobId: job.id,
    productId: job.productId,
    offerId: job.offerId,
    target: job.target,
    batchId: job.batchId,
    status: job.status,
    progress: job.progress,
    variantIndex: job.variantIndex,
    variantTotal: job.variantTotal,
    draftIds: job.draftIds || [],
    lastError: job.lastError || null,
    model: job.model,
    endpoint: job.endpoint,
    presetId: job.presetId,
    presetLabel: job.presetLabel,
    sourceImageUrl: job.sourceImageUrl,
    startedAt: job.startedAt || null,
    updatedAt: job.updatedAt || null,
    finishedAt: job.finishedAt || null,
  });
}

function aiImageGenerationRequestFromBody(product, body = {}) {
  return {
    sourceImageUrl: cleanText(body.sourceImageUrl) || firstImageUrl(product.ozon?.primaryImage || product.ozon?.images || product.imageUrl),
    count: Math.min(5, Math.max(1, Math.floor(Number(body.count || body.imagesCount || 5) || 5))),
    studioPresets: normalizeAiImageStudioPresetList(body.photoPresets || body.presets),
    prompt: cleanText(body.prompt),
  };
}

async function appendAiImageDraftToProduct(productId, draft) {
  const warehouse = await readWarehouse();
  const product = warehouse.products.find((item) => item.id === productId);
  if (!product) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    error.code = "warehouse_product_not_found";
    throw error;
  }
  product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), draft]);
  product.updatedAt = new Date().toISOString();
  const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_generate", writeLinks: false });
  return saved?.products?.find((item) => item.id === productId) || normalizeWarehouseProduct(product);
}

async function appendAiImageDraftsToProduct(productId, drafts = [], reason = "warehouse_premium_image_generate") {
  const normalizedDrafts = (Array.isArray(drafts) ? drafts : [drafts]).map(normalizeAiImageDraft).filter(Boolean);
  if (!normalizedDrafts.length) return null;
  const warehouse = await readWarehouse();
  const product = warehouse.products.find((item) => item.id === productId);
  if (!product) {
    const error = new Error("Warehouse product not found.");
    error.statusCode = 404;
    error.code = "warehouse_product_not_found";
    throw error;
  }
  product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), ...normalizedDrafts]);
  product.updatedAt = new Date().toISOString();
  const saved = await writeWarehouseProductPatch([product], { reason, writeLinks: false });
  return saved?.products?.find((item) => item.id === productId) || normalizeWarehouseProduct(product);
}

function premiumImageBackgrounds() {
  return [
    { id: "white-luxury", label: "White luxury", from: "#fbfcff", to: "#edf3fb", accent: "#d8e2ef", text: "#d7e0ec" },
    { id: "warm-champagne", label: "Warm champagne", from: "#fff8ed", to: "#ead8bd", accent: "#d7b77d", text: "#e5cfaa" },
    { id: "marble-light", label: "Light marble", from: "#f7f8f7", to: "#dce2e2", accent: "#b8c3c8", text: "#d5dbdd" },
    { id: "dark-premium", label: "Dark premium", from: "#111827", to: "#25324a", accent: "#c7a86d", text: "#2f3b53" },
    { id: "soft-gradient", label: "Soft gradient", from: "#f8fbff", to: "#e9ddf2", accent: "#b7c9e9", text: "#e2d9ef" },
  ];
}

function premiumImageBackgroundSvg(theme, size) {
  const dark = theme.id === "dark-premium";
  return Buffer.from(`
<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="${theme.from}"/>
      <stop offset="1" stop-color="${theme.to}"/>
    </linearGradient>
    <radialGradient id="glow" cx="46%" cy="34%" r="58%">
      <stop offset="0" stop-color="${dark ? "#ffffff" : "#ffffff"}" stop-opacity="${dark ? "0.14" : "0.72"}"/>
      <stop offset="1" stop-color="${theme.to}" stop-opacity="0"/>
    </radialGradient>
    <filter id="blur"><feGaussianBlur stdDeviation="18"/></filter>
  </defs>
  <rect width="${size}" height="${size}" fill="url(#bg)"/>
  <rect width="${size}" height="${size}" fill="url(#glow)"/>
  <path d="M${size * 0.12} ${size * 0.77} C${size * 0.32} ${size * 0.68}, ${size * 0.62} ${size * 0.88}, ${size * 0.9} ${size * 0.72}" fill="none" stroke="${theme.accent}" stroke-opacity="${dark ? "0.22" : "0.18"}" stroke-width="3"/>
  <circle cx="${size * 0.78}" cy="${size * 0.20}" r="${size * 0.20}" fill="${theme.accent}" opacity="${dark ? "0.08" : "0.10"}" filter="url(#blur)"/>
  <rect x="${size * 0.10}" y="${size * 0.11}" width="${size * 0.80}" height="${size * 0.78}" rx="34" fill="none" stroke="${dark ? "#ffffff" : theme.accent}" stroke-opacity="${dark ? "0.08" : "0.13"}"/>
</svg>`);
}

function premiumImageShadowSvg(size) {
  return Buffer.from(`
<svg width="${size}" height="${Math.round(size * 0.18)}" viewBox="0 0 ${size} ${Math.round(size * 0.18)}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="shadow" cx="50%" cy="50%" r="50%">
      <stop offset="0" stop-color="#000000" stop-opacity="0.28"/>
      <stop offset="1" stop-color="#000000" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <ellipse cx="${size / 2}" cy="${Math.round(size * 0.09)}" rx="${Math.round(size * 0.36)}" ry="${Math.round(size * 0.07)}" fill="url(#shadow)"/>
</svg>`);
}

function premiumImageLogoBadgeSvg(size) {
  return Buffer.from(`
<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg">
  <rect x="0.5" y="0.5" width="${size - 1}" height="${size - 1}" rx="18" fill="#ffffff" fill-opacity="0.72" stroke="#ffffff" stroke-opacity="0.86"/>
</svg>`);
}

function productImageUrlsForPremium(product = {}) {
  return Array.from(new Set([
    ...splitList(product.imageUrl),
    ...splitList(product.ozon?.primaryImage),
    ...splitList(product.ozon?.images),
    ...splitList(product.yandex?.pictures),
    ...splitList(product.yandex?.images),
  ].map(cleanText).filter(Boolean)));
}

async function readProductSourceImageBuffer(sourceUrl, request) {
  const localPath = localPublicFilePathFromUrl(sourceUrl, request);
  let buffer = null;
  let mimeType = "";
  if (localPath) {
    buffer = await fs.readFile(localPath);
    mimeType = imageMimeFromPath(localPath);
  } else if (/^https?:\/\//i.test(sourceUrl)) {
    const response = await fetch(sourceUrl, { headers: { Accept: "image/*,*/*;q=0.8" } });
    if (!response.ok) {
      const error = new Error(`Source image fetch failed: HTTP ${response.status}.`);
      error.statusCode = 400;
      error.code = "source_image_fetch_failed";
      throw error;
    }
    mimeType = cleanText(response.headers.get("content-type")).split(";")[0];
    buffer = Buffer.from(await response.arrayBuffer());
  }
  if (!buffer || buffer.length < 1024 || !/^image\/(png|jpe?g|webp)$/i.test(mimeType || imageMimeFromPath(sourceUrl))) {
    const error = new Error("Source image is not ready for premium card generation.");
    error.statusCode = 400;
    error.code = "source_image_not_ready";
    throw error;
  }
  try {
    await sharp(buffer).metadata();
  } catch (error) {
    const sourceError = new Error("Source image is damaged or unsupported.");
    sourceError.statusCode = 400;
    sourceError.code = "source_image_not_ready";
    throw sourceError;
  }
  return buffer;
}

function brandingForMarketplace(settings = {}, marketplace = "ozon") {
  const normalized = normalizeAppSettings(settings);
  const marketplaces = normalized.branding?.marketplaces || {};
  const key = marketplace === "yandex" ? "yandex" : "ozon";
  return marketplaces[key] || {};
}

async function readBrandingLogoBuffer(settings = {}, marketplace = "ozon", request = null) {
  const branding = brandingForMarketplace(settings, marketplace);
  const logoUrl = cleanText(branding.logoUrl);
  if (!logoUrl) return null;
  const localPath = localPublicFilePathFromUrl(logoUrl, request);
  if (!localPath) return null;
  try {
    const buffer = await fs.readFile(localPath);
    const meta = await sharp(buffer).metadata();
    if (meta.format !== "png" || Number(meta.width) !== 258 || Number(meta.height) !== 258) return null;
    return buffer;
  } catch (_error) {
    return null;
  }
}

async function marketplaceExtraCardUrls(marketplace = "ozon", request = null) {
  const settings = await readAppSettings();
  const branding = brandingForMarketplace(settings, marketplace);
  const cards = Array.isArray(branding.extraCards) ? branding.extraCards : [];
  const urls = [];
  for (const card of cards.slice(0, 2)) {
    const rawUrl = cleanText(card?.url);
    if (!rawUrl) continue;
    urls.push(await normalizeMarketplaceImageUrlForSend(rawUrl, request));
  }
  return Array.from(new Set(urls));
}

function appendUniqueImages(primary = [], extra = []) {
  return Array.from(new Set([...(Array.isArray(primary) ? primary : []), ...(Array.isArray(extra) ? extra : [])].map(cleanText).filter(Boolean)));
}

async function buildPremiumPerfumeImageBuffer(sourceBuffer, { theme, logoBuffer = null, size = 1000 } = {}) {
  const targetSize = Math.max(512, Math.min(2048, Number(size || 1000) || 1000));
  const productMaxHeight = Math.round(targetSize * 0.78);
  const productMaxWidth = Math.round(targetSize * 0.68);
  let productPipeline = sharp(sourceBuffer).rotate();
  try {
    productPipeline = productPipeline.trim({ threshold: 10 });
  } catch (_error) {
    productPipeline = sharp(sourceBuffer).rotate();
  }
  const productBuffer = await productPipeline
    .resize(productMaxWidth, productMaxHeight, {
      fit: "inside",
      withoutEnlargement: false,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    })
    .png()
    .toBuffer();
  const productMeta = await sharp(productBuffer).metadata();
  if (!productMeta.width || !productMeta.height || productMeta.width < 80 || productMeta.height < 80) {
    const error = new Error("Source image is too small after processing.");
    error.statusCode = 400;
    error.code = "source_image_not_ready";
    throw error;
  }
  const productLeft = Math.max(0, Math.round((targetSize - productMeta.width) / 2));
  const productTop = Math.max(0, Math.round(targetSize - productMeta.height - targetSize * 0.12));
  const shadowSize = Math.round(targetSize * 0.78);
  const composites = [
    {
      input: premiumImageShadowSvg(shadowSize),
      left: Math.round((targetSize - shadowSize) / 2),
      top: Math.round(targetSize * 0.79),
    },
    { input: productBuffer, left: productLeft, top: productTop },
  ];
  if (logoBuffer) {
    const badgeSize = Math.round(targetSize * 0.094);
    const badgeMargin = Math.round(targetSize * 0.055);
    const logoInset = Math.round(badgeSize * 0.15);
    const badgeLeft = targetSize - badgeMargin - badgeSize;
    const badgeTop = badgeMargin;
    const logo = await sharp(logoBuffer)
      .resize(badgeSize - logoInset * 2, badgeSize - logoInset * 2, { fit: "contain" })
      .png()
      .toBuffer();
    composites.push(
      { input: premiumImageLogoBadgeSvg(badgeSize), left: badgeLeft, top: badgeTop },
      { input: logo, left: badgeLeft + logoInset, top: badgeTop + logoInset },
    );
  }
  return sharp(premiumImageBackgroundSvg(theme, targetSize))
    .composite(composites)
    .png({ compressionLevel: 9 })
    .toBuffer();
}

async function generatePremiumPerfumeImageDrafts(product, options = {}, request) {
  const count = Math.max(1, Math.min(5, Math.floor(Number(options.count || 5) || 5)));
  const marketplace = cleanText(options.marketplace || "ozon").toLowerCase() === "yandex" ? "yandex" : "ozon";
  const useLogo = options.useLogo !== false;
  const settings = await readAppSettings();
  const logoBuffer = useLogo ? await readBrandingLogoBuffer(settings, marketplace, request) : null;
  const sources = productImageUrlsForPremium(product);
  if (!sources.length) {
    const error = new Error("Product has no source images for premium card generation.");
    error.statusCode = 400;
    error.code = "source_image_required";
    throw error;
  }
  const themes = premiumImageBackgrounds();
  const targetSize = ozonAiImageTargetPx || 1000;
  await fs.mkdir(aiImageDir, { recursive: true });
  const batchId = crypto.randomUUID();
  const drafts = [];
  const warnings = [];
  for (let index = 1; index <= count; index += 1) {
    const sourceUrl = sources[(index - 1) % sources.length];
    const theme = themes[(index - 1) % themes.length];
    let sourceBuffer = null;
    try {
      sourceBuffer = await readProductSourceImageBuffer(sourceUrl, request);
    } catch (error) {
      if (index === 1) throw error;
      warnings.push({ sourceImageUrl: sourceUrl, code: error?.code || "source_image_not_ready", detail: error?.message || String(error) });
      sourceBuffer = await readProductSourceImageBuffer(sources[0], request);
    }
    const outBuffer = await buildPremiumPerfumeImageBuffer(sourceBuffer, { theme, logoBuffer, size: targetSize });
    const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}.png`;
    const filePath = path.join(aiImageDir, fileName);
    await fs.writeFile(filePath, outBuffer);
    const relativeUrl = `/uploads/ai-images/${fileName}`;
    drafts.push(normalizeAiImageDraft({
      status: "pending",
      prompt: `Premium deterministic perfume card: ${theme.label}`,
      productName: product.name || product.ozon?.name,
      sourceImageUrl: sourceUrl,
      resultUrl: `${uploadBaseUrl(request)}${relativeUrl}`,
      batchId,
      variantIndex: index,
      variantTotal: count,
      presetId: `premium-${theme.id}`,
      presetLabel: theme.label,
      layout: "premium-template",
      model: "premium-template",
      size: `${targetSize}x${targetSize}`,
      quality: "deterministic",
      format: "png",
    }));
  }
  return { drafts, warnings, batchId };
}

async function generateAiImageDraftsSynchronously(product, generation, request, batchId = crypto.randomUUID()) {
  const drafts = [];
  for (let index = 1; index <= generation.count; index += 1) {
    const preset = generation.studioPresets[(index - 1) % generation.studioPresets.length];
    const prompt = [generation.prompt, preset?.prompt].filter(Boolean).join("\n\n");
    const draft = await generateOzonAiImageDraftWithRetry(product, {
      prompt,
      sourceImageUrl: generation.sourceImageUrl,
      batchId,
      variantIndex: index,
      variantTotal: generation.count,
      presetId: preset?.id,
      presetLabel: preset?.label,
      requireSourceImage: true,
      allowGenerationFallback: false,
      forceCodexSale: true,
    }, request, {
      productId: product.id,
      offerId: product.offerId,
      variantIndex: index,
      variantTotal: generation.count,
    });
    drafts.push(draft);
    if (index < generation.count && aiImageGenerationSequenceDelayMs > 0) await sleep(aiImageGenerationSequenceDelayMs);
  }
  return drafts;
}

async function runAiImageGenerationJob(jobInput, generation, requestContext = {}) {
  let job = normalizeAiImageJob({ ...jobInput, status: "running", progress: 2, startedAt: new Date().toISOString(), lastError: null });
  activeAiImageJobs.set(job.id, true);
  await upsertAiImageJob(job);
  const auditRequest = {
    session: {
      username: cleanText(requestContext.username || job.createdBy || "system") || "system",
      role: cleanText(requestContext.role || "admin") || "admin",
    },
  };
  let savedProduct = null;
  try {
    const initialWarehouse = await readWarehouse();
    const product = initialWarehouse.products.find((item) => item.id === job.productId);
    if (!product) {
      const error = new Error("Warehouse product not found.");
      error.statusCode = 404;
      error.code = "warehouse_product_not_found";
      throw error;
    }
    logger.info("ai image drafts job started", {
      jobId: job.id,
      productId: product.id,
      offerId: product.offerId,
      target: product.target,
      count: generation.count,
      provider: "codexsale",
      sequential: true,
      attempts: aiImageGenerationAttempts,
    });
    for (let index = 1; index <= generation.count; index += 1) {
      const preset = generation.studioPresets[(index - 1) % generation.studioPresets.length];
      job = await upsertAiImageJob({
        ...job,
        status: "running",
        variantIndex: index,
        variantTotal: generation.count,
        progress: Math.max(5, Math.floor(((index - 1) / generation.count) * 90)),
        presetId: preset?.id,
        presetLabel: preset?.label,
        lastError: null,
      });
      const prompt = [generation.prompt, preset?.prompt].filter(Boolean).join("\n\n");
      const draft = await generateOzonAiImageDraftWithRetry(product, {
        prompt,
        sourceImageUrl: generation.sourceImageUrl,
        batchId: job.batchId,
        variantIndex: index,
        variantTotal: generation.count,
        presetId: preset?.id,
        presetLabel: preset?.label,
        requireSourceImage: true,
        allowGenerationFallback: false,
        forceCodexSale: true,
      }, requestContext, {
        jobId: job.id,
        productId: product.id,
        offerId: product.offerId,
        variantIndex: index,
        variantTotal: generation.count,
      });
      savedProduct = await appendAiImageDraftToProduct(product.id, draft);
      job = await upsertAiImageJob({
        ...job,
        draftIds: Array.from(new Set([...(job.draftIds || []), draft.id])),
        progress: Math.floor((index / generation.count) * 100),
        variantIndex: index,
        variantTotal: generation.count,
        status: "running",
      });
      if (index < generation.count && aiImageGenerationSequenceDelayMs > 0) await sleep(aiImageGenerationSequenceDelayMs);
    }
    job = await upsertAiImageJob({
      ...job,
      status: "completed",
      progress: 100,
      variantIndex: generation.count,
      variantTotal: generation.count,
      finishedAt: new Date().toISOString(),
      lastError: null,
    });
    logger.info("ai image drafts job complete", {
      jobId: job.id,
      productId: job.productId,
      offerId: job.offerId,
      drafts: (job.draftIds || []).length,
      batchId: job.batchId,
    });
    appendAudit(auditRequest, "warehouse.ai_image.generate", {
      productId: job.productId,
      offerId: job.offerId,
      draftIds: job.draftIds,
      batchId: job.batchId,
      count: generation.count,
      jobId: job.id,
      oldValue: requestContext.before || null,
      newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
    }).catch((auditError) => logger.warn("ai image generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    const errorPayload = aiImageJobErrorPayload(error);
    job = await upsertAiImageJob({
      ...job,
      status: (job.draftIds || []).length ? "partial" : "failed",
      progress: (job.draftIds || []).length ? job.progress : 0,
      lastError: errorPayload,
      finishedAt: new Date().toISOString(),
    });
    logger.warn("ai image drafts job failed", {
      jobId: job.id,
      productId: job.productId,
      offerId: job.offerId,
      drafts: (job.draftIds || []).length,
      ...errorPayload,
    });
  } finally {
    activeAiImageJobs.delete(job.id);
  }
  return job;
}

app.post("/api/warehouse/products/:id/premium-images/generate", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Warehouse product not found.", code: "warehouse_product_not_found" });
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });
    const generation = await generatePremiumPerfumeImageDrafts(product, {
      count: request.body?.count,
      marketplace: request.body?.marketplace,
      useLogo: request.body?.useLogo !== false,
    }, request);
    const savedProduct = await appendAiImageDraftsToProduct(product.id, generation.drafts, "warehouse_premium_image_generate");
    appendAudit(request, "warehouse.premium_image.generate", {
      productId: product.id,
      offerId: product.offerId,
      draftIds: generation.drafts.map((draft) => draft.id),
      batchId: generation.batchId,
      count: generation.drafts.length,
      warnings: generation.warnings,
      oldValue: before,
      newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
    }).catch((auditError) => logger.warn("premium image generate audit failed", { detail: auditError?.message || String(auditError) }));
    response.json({
      ok: true,
      product: savedProduct || product,
      drafts: generation.drafts,
      warnings: generation.warnings,
      batchId: generation.batchId,
    });
  } catch (error) {
    logger.warn("premium image generation failed", {
      productId: request.params.id,
      detail: error?.message || String(error),
      code: error?.code,
      statusCode: error?.statusCode || error?.status,
    });
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/generate", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "РўРѕРІР°СЂ СЃРєР»Р°РґР° РЅРµ РЅР°Р№РґРµРЅ." });
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });
    const generation = aiImageGenerationRequestFromBody(product, request.body);
    if (!generation.sourceImageUrl) {
      return response.status(400).json({
        error: "Р”Р»СЏ РіРµРЅРµСЂР°С†РёРё С‡РµСЂРµР· Codex РЅСѓР¶РЅРѕ РёСЃС…РѕРґРЅРѕРµ С„РѕС‚Рѕ С‚РѕРІР°СЂР°.",
        code: "source_image_required",
      });
    }
    assertImageGenerationConfigured(forceCodexSaleAiImageSettings(await readEffectiveAiSettings()));

    const syncMode = request.body?.sync === true || cleanText(request.body?.mode).toLowerCase() === "sync";
    if (syncMode) {
      const batchId = crypto.randomUUID();
      const drafts = await generateAiImageDraftsSynchronously(product, generation, request, batchId);
      const draft = drafts[drafts.length - 1];
      let savedProduct = null;
      for (const item of drafts) savedProduct = await appendAiImageDraftToProduct(product.id, item);
      response.json({ ok: true, draft, drafts, batchId, product: savedProduct || product });
      appendAudit(request, "warehouse.ai_image.generate", {
        productId: product.id,
        offerId: product.offerId,
        draftId: draft.id,
        batchId,
        count: generation.count,
        oldValue: before,
        newValue: savedProduct ? { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt } : null,
      }).catch((auditError) => logger.warn("ai image generate audit failed", { detail: auditError?.message || String(auditError) }));
      return;
    }

    const activeJob = await findActiveAiImageJob(product.id);
    if (activeJob) {
      return response.status(202).json({
        ok: true,
        jobId: activeJob.id,
        status: activeJob.status,
        productId: product.id,
        batchId: activeJob.batchId,
        job: publicAiImageJob(activeJob),
      });
    }

    const job = await upsertAiImageJob({
      productId: product.id,
      offerId: product.offerId,
      target: product.target,
      batchId: crypto.randomUUID(),
      status: "queued",
      progress: 0,
      variantTotal: generation.count,
      model: "gpt-image-2",
      endpoint: "https://codex.sale/v1/images/edits",
      sourceImageUrl: generation.sourceImageUrl,
      prompt: generation.prompt,
      createdBy: requestUsername(request),
    });
    activeAiImageJobs.set(job.id, true);
    response.status(202).json({ ok: true, jobId: job.id, status: "queued", productId: product.id, batchId: job.batchId, job: publicAiImageJob(job) });
    setImmediate(() => {
      runAiImageGenerationJob(job, generation, {
        session: { username: requestUsername(request), role: request.session?.role || "admin" },
        headers: { host: request.headers.host, "x-forwarded-proto": request.headers["x-forwarded-proto"] },
        protocol: request.protocol,
        get: (name) => request.get(name),
        username: requestUsername(request),
        role: request.session?.role || "admin",
        before,
      }).catch((error) => logger.warn("ai image background job launcher failed", { jobId: job.id, detail: error?.message || String(error) }));
    });
  } catch (error) {
    logger.warn("ai image drafts generation failed", {
      productId: request.params.id,
      detail: error?.message || String(error),
      code: error?.code,
      statusCode: error?.statusCode || error?.status,
    });
    next(error);
  }
});

app.get("/api/warehouse/products/:id/ai-images/jobs/:jobId", async (request, response, next) => {
  try {
    const jobs = await readAiImageJobs();
    const job = jobs.find((item) => item.id === request.params.jobId && item.productId === request.params.id);
    if (!job) return response.status(404).json({ error: "AI image job not found.", code: "ai_image_job_not_found" });
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id) || null;
    response.json({ ok: true, job: publicAiImageJob(job), product: product ? normalizeWarehouseProduct(product) : null });
  } catch (error) {
    next(error);
  }
});

function buildAiAssistantChecklist(product = {}, validation = {}, draft = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const hasSourceImage = Boolean(firstImageUrl(normalized.ozon?.primaryImage || normalized.ozon?.images || normalized.imageUrl));
  return [
    {
      id: "description",
      label: "Проверить новое описание",
      ok: cleanText(draft.description).length >= 300,
      detail: cleanText(draft.description).length ? `${cleanText(draft.description).length} символов` : "описание не создано",
    },
    {
      id: "seo",
      label: "Проверить SEO-фразы",
      ok: Array.isArray(draft.seoKeywords) && draft.seoKeywords.length >= 5,
      detail: `${Array.isArray(draft.seoKeywords) ? draft.seoKeywords.length : 0} фраз`,
    },
    {
      id: "photo",
      label: "Сгенерировать 5 фото по studio presets",
      ok: hasSourceImage,
      detail: hasSourceImage ? "исходное фото есть" : "нет исходного фото",
    },
    {
      id: "marketplace",
      label: "Проверить требования маркетплейса",
      ok: Boolean(validation.ready),
      detail: (validation.missing || validation.reasons || []).join(", ") || "критичных замечаний нет",
    },
    {
      id: "manual-review",
      label: "Одобрить вручную перед отправкой",
      ok: false,
      detail: "AI ничего не публикует без кнопки подтверждения",
    },
  ];
}

app.post("/api/warehouse/products/:id/ai-assistant", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const marketplace = cleanText(request.body.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const beforeValidation = productContentQuality(product, marketplace);
    const draft = await generateAiProductContentDraft(product, { marketplace });
    const previewProduct = applyAiContentDraftToProduct(product, draft, marketplace);
    const afterValidation = productContentQuality(previewProduct, marketplace);
    const aiSettings = await readEffectiveAiSettings();
    const savedDraft = normalizeAiContentDraft({
      ...draft,
      marketplace,
      source: "assistant",
      model: draft.model || aiSettings.textModel || openaiTextModel,
    });
    let savedProduct = normalizeWarehouseProduct(product);
    if (savedDraft) {
      product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      product.updatedAt = new Date().toISOString();
      const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_assistant_draft", writeLinks: false });
      savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    }
    response.json({
      ok: true,
      productId: product.id,
      offerId: product.offerId,
      marketplace,
      draft: savedDraft || draft,
      product: savedProduct,
      before: beforeValidation,
      after: afterValidation,
      reasons: beforeValidation.reasons || [],
      checklist: buildAiAssistantChecklist(previewProduct, afterValidation, draft),
      photoPresets: publicAiImageStudioPresets(),
      provider: {
        providerId: aiSettings.providerId,
        baseUrl: normalizeOpenAiCompatibleBaseUrl(aiSettings.baseUrl),
        textModel: aiSettings.textModel || openaiTextModel,
        imageModel: effectiveOpenAiImageModel(aiSettings.imageModel, aiSettings),
      },
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/generate", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const apply = request.body.apply !== false;
    if (apply) {
      const conflict = productConflict(product, request.body.expectedUpdatedAt);
      if (conflict) return conflictResponse(response, [conflict]);
    }
    const marketplace = cleanText(request.body.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const before = cloneAuditValue({
      id: product.id,
      marketplace: product.marketplace,
      offerId: product.offerId,
      yandex: product.yandex || {},
      updatedAt: product.updatedAt,
    });
    const draft = await generateAiProductContentDraft(product, { marketplace });
    const enhancedProduct = applyAiContentDraftToProduct(product, draft, marketplace);
    const validation = productContentQuality(enhancedProduct, marketplace);
    const built = marketplace === "yandex" ? buildYandexOfferMapping(enhancedProduct) : { ready: true, missing: [] };

    if (!apply) {
      if (request.body.saveDraft === true) {
        const savedDraft = normalizeAiContentDraft({
          ...draft,
          marketplace,
          source: cleanText(request.body.source || "manual"),
          model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
        });
        if (savedDraft) {
          product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
          product.updatedAt = new Date().toISOString();
          const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_draft", writeLinks: false });
          const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
          return response.json({
            ok: true,
            applied: false,
            saved: true,
            draft: savedDraft,
            validation: { ...validation, yandexReady: Boolean(built.ready), missing: built.missing || validation.missing || [] },
            product: savedProduct,
          });
        }
      }
      return response.json({
        ok: true,
        applied: false,
        draft,
        validation: { ...validation, yandexReady: Boolean(built.ready), missing: built.missing || validation.missing || [] },
      });
    }

    Object.assign(product, enhancedProduct, { updatedAt: new Date().toISOString() });
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_generate", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const savedValidation = productContentQuality(savedProduct, marketplace);
    response.json({
      ok: true,
      applied: true,
      draft,
      validation: { ...savedValidation, yandexReady: Boolean(buildYandexOfferMapping(savedProduct).ready) },
      product: savedProduct,
    });
    appendAudit(request, "warehouse.ai_content.generate", {
      productId: product.id,
      offerId: product.offerId,
      marketplace,
      oldValue: before,
      newValue: cloneAuditValue({
        id: savedProduct.id,
        marketplace: savedProduct.marketplace,
        offerId: savedProduct.offerId,
        yandex: savedProduct.yandex || {},
        updatedAt: savedProduct.updatedAt,
      }),
    }).catch((auditError) => logger.warn("ai content generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/yandex-quality-candidates", requireAdmin, async (request, response, next) => {
  try {
    const threshold = Math.max(0, Math.min(100, Math.round(Number(request.query.threshold ?? 40) || 40)));
    const limit = cleanLimit(request.query.limit, 30000, 50000);
    const resultLimit = cleanLimit(request.query.resultLimit, 300, 1000);
    const warehouse = await readWarehouse();
    const yandexProducts = (warehouse.products || [])
      .map((product) => normalizeWarehouseProduct(product))
      .filter((product) => product.marketplace === "yandex" && cleanText(product.offerId))
      .slice(0, limit);

    if (parseBooleanSetting(request.query.cached || request.query.cacheOnly, false)) {
      const lowQuality = (warehouse.products || [])
        .filter((product) => {
          const normalized = normalizeWarehouseProduct(product);
          const rating = Number(normalized.yandex?.extra?.cardQuality?.contentRating);
          return normalized.marketplace === "yandex" && Number.isFinite(rating) && rating <= threshold;
        })
        .sort((a, b) => {
          const qa = Number(normalizeWarehouseProduct(a).yandex?.extra?.cardQuality?.contentRating || 0);
          const qb = Number(normalizeWarehouseProduct(b).yandex?.extra?.cardQuality?.contentRating || 0);
          return qa - qb || String(a.offerId || "").localeCompare(String(b.offerId || ""));
        });
      return response.json({
        ok: true,
        cached: true,
        threshold,
        checked: yandexProducts.length,
        qualityLoaded: (warehouse.products || []).filter((product) => Number.isFinite(Number(normalizeWarehouseProduct(product).yandex?.extra?.cardQuality?.contentRating))).length,
        total: lowQuality.length,
        errors: [],
        products: lowQuality.slice(0, resultLimit).map(buildAiQualityReviewRow),
      });
    }

    const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
    if (!shops.length) return response.status(400).json({ error: "Yandex Market is not configured." });

    const qualityByTargetOffer = new Map();
    const errors = [];

    for (const shop of shops) {
      const offerIds = yandexProducts
        .filter((product) => matchesYandexTarget(product.target, shop.id))
        .map((product) => product.offerId);
      for (const chunk of chunkArray(offerIds, 200)) {
        try {
          const rows = await getYandexOfferCardsContentStatus(shop, chunk, { withRecommendations: true });
          for (const row of rows) qualityByTargetOffer.set(yandexTargetOfferKey(shop.id, row.offerId), row);
        } catch (error) {
          errors.push({ target: shop.id, error: error?.message || "yandex_card_quality_failed" });
        }
      }
    }

    const now = new Date().toISOString();
    const changedProducts = [];
    const lowQuality = [];
    for (const product of warehouse.products || []) {
      const normalized = normalizeWarehouseProduct(product);
      if (normalized.marketplace !== "yandex" || !normalized.offerId) continue;
      const shop = getYandexShopByTarget(normalized.target);
      const quality = shop ? qualityByTargetOffer.get(yandexTargetOfferKey(shop.id, normalized.offerId)) : null;
      if (!quality) continue;
      product.yandex = normalizeYandexDraft({
        ...(product.yandex || {}),
        extra: {
          ...(product.yandex?.extra || {}),
          cardQuality: quality,
        },
      });
      product.updatedAt = now;
      changedProducts.push(product);
      if (Number(quality.contentRating || 0) <= threshold) lowQuality.push(product);
    }
    if (changedProducts.length) {
      await writeWarehouseProductPatch(changedProducts, { reason: "yandex_card_quality_find", writeLinks: false });
    }

    lowQuality.sort((a, b) => {
      const qa = Number(normalizeWarehouseProduct(a).yandex?.extra?.cardQuality?.contentRating || 0);
      const qb = Number(normalizeWarehouseProduct(b).yandex?.extra?.cardQuality?.contentRating || 0);
      return qa - qb || String(a.offerId || "").localeCompare(String(b.offerId || ""));
    });

    response.json({
      ok: true,
      threshold,
      checked: yandexProducts.length,
      qualityLoaded: qualityByTargetOffer.size,
      total: lowQuality.length,
      errors,
      products: lowQuality.slice(0, resultLimit).map(buildAiQualityReviewRow),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/yandex-quality-draft/generate", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== "yandex") return response.status(400).json({ error: "Генерация качества доступна только для Yandex Market." });

    const count = Math.min(5, Math.max(1, Math.floor(Number(request.body.imagesCount || request.body.count || 5) || 5)));
    const quality = normalized.yandex?.extra?.cardQuality || {};
    const before = cloneAuditValue({
      id: product.id,
      aiContentDrafts: product.aiContentDrafts || [],
      aiImages: product.aiImages || [],
      updatedAt: product.updatedAt,
    });
    let savedDraft = null;
    const errors = [];

    try {
      const draft = await generateAiProductContentDraft(normalized, { marketplace: "yandex" });
      savedDraft = normalizeAiContentDraft({
        ...draft,
        marketplace: "yandex",
        source: "yandex_card_quality_manual",
        qualityBefore: quality.contentRating,
        recommendations: quality.recommendations,
        model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
      });
      if (savedDraft) {
        product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      }
    } catch (error) {
      errors.push({ type: "content", error: error?.message || "ai_content_draft_failed" });
      if (isOpenAiBillingLimitError(error)) {
        return response.status(400).json({ error: error.message, code: error.code || "ai_billing_limit", errors });
      }
    }

    const batchId = crypto.randomUUID();
    const imageDrafts = [];
    for (let index = 1; index <= count; index += 1) {
      try {
        const imageDraft = await generateOzonAiImageDraft(normalized, {
          prompt: `Create exactly one marketplace-ready product photo ${index} of ${count} for ${normalized.name || normalized.offerId}. This is one image only; do not create a collage, grid, contact sheet, split-screen, multi-panel layout, or multiple bottles in one image. Clean square ecommerce packshot, realistic perfume bottle only, full bottle visible from cap to base, centered, upright, not cropped, 10-15% empty margin on every side. No box, no outer packaging, no cartons, no props, no headline, no typography, no text overlays, no extra objects. Text is allowed only if it already exists on the original bottle label.`,
          sourceImageUrl: request.body.sourceImageUrl || "",
          batchId,
          variantIndex: index,
          variantTotal: count,
        }, request);
        imageDrafts.push(imageDraft);
      } catch (error) {
        errors.push({ type: "image", index, error: error?.message || "ai_image_draft_failed" });
        if (isOpenAiBillingLimitError(error)) break;
      }
    }
    if (imageDrafts.length) {
      product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), ...imageDrafts]);
    }
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "yandex_card_quality_manual_draft", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const row = buildAiQualityReviewRow(savedProduct);
    response.json({
      ok: Boolean(savedDraft && imageDrafts.length === count),
      partial: Boolean(errors.length && (savedDraft || imageDrafts.length)),
      contentDraft: savedDraft,
      imageDrafts,
      batchId,
      errors,
      row,
      product: savedProduct,
    });
    appendAudit(request, "yandex.card_quality.manual_generate", {
      productId: product.id,
      offerId: normalized.offerId,
      batchId,
      imagesRequested: count,
      imagesCreated: imageDrafts.length,
      oldValue: before,
      newValue: { id: savedProduct.id, aiContentDrafts: savedProduct.aiContentDrafts || [], aiImages: savedProduct.aiImages || [] },
    }).catch((auditError) => logger.warn("yandex quality manual generate audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/yandex-quality-draft/send", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const normalizedBefore = normalizeWarehouseProduct(product);
    if (normalizedBefore.marketplace !== "yandex") return response.status(400).json({ error: "Отправка качества доступна только для Yandex Market." });

    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const contentDraftId = cleanText(request.body.contentDraftId);
    const imageBatchId = cleanText(request.body.imageBatchId);
    const primaryImageDraftId = cleanText(request.body.primaryImageDraftId);
    const contentDraft = contentDraftId
      ? product.aiContentDrafts.find((draft) => draft.id === contentDraftId)
      : latestAiContentDraft(product, "pending");
    let imageBatch = imageBatchId
      ? product.aiImages.filter((draft) => draft.batchId === imageBatchId && draft.resultUrl)
      : latestAiImageBatch(product, "pending").drafts;
    let primaryImageDraft = primaryImageDraftId
      ? imageBatch.find((draft) => draft.id === primaryImageDraftId)
      : imageBatch[0];
    if (!contentDraft && !imageBatch.length) return response.status(400).json({ error: "Нет черновика текста или фото для отправки." });

    const before = cloneAuditValue({
      id: product.id,
      yandex: product.yandex || {},
      aiContentDrafts: product.aiContentDrafts || [],
      aiImages: product.aiImages || [],
      imageUrl: product.imageUrl || "",
      updatedAt: product.updatedAt,
    });
    if (contentDraft) {
      const enhanced = applyAiContentDraftToProduct(product, contentDraft, "yandex");
      Object.assign(product, enhanced);
      product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
      const draftToApprove = product.aiContentDrafts.find((draft) => draft.id === contentDraft.id);
      if (draftToApprove) {
        draftToApprove.status = "approved";
        draftToApprove.reviewedAt = new Date().toISOString();
      }
      product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
      imageBatch = imageBatchId
        ? product.aiImages.filter((draft) => draft.batchId === imageBatchId && draft.resultUrl)
        : latestAiImageBatch(product, "pending").drafts;
      primaryImageDraft = primaryImageDraftId
        ? imageBatch.find((draft) => draft.id === primaryImageDraftId)
        : imageBatch[0];
    }
    let yandexPicturesForSend = [];
    if (imageBatch.length) {
      for (const draft of imageBatch) {
        draft.resultUrl = await normalizeMarketplaceImageUrlForSend(draft.resultUrl, request);
      }
      if (primaryImageDraft) {
        primaryImageDraft.resultUrl = await normalizeMarketplaceImageUrlForSend(primaryImageDraft.resultUrl, request);
      }
      const primaryUrl = primaryImageDraft?.resultUrl || imageBatch[0]?.resultUrl;
      if (!primaryUrl) return response.status(400).json({ error: "В выбранных AI-фото нет URL результата." });
      const reviewedAt = new Date().toISOString();
      imageBatch.forEach((draft) => {
        if (draft.status === "pending") {
          draft.status = "approved";
          draft.reviewedAt = reviewedAt;
        }
      });
      const batchUrls = [
        primaryUrl,
        ...imageBatch.map((draft) => draft.resultUrl).filter((url) => url && url !== primaryUrl),
      ].slice(0, 10);
      const extraCardUrls = await marketplaceExtraCardUrls("yandex", request);
      yandexPicturesForSend = appendUniqueImages(batchUrls, extraCardUrls).slice(0, 10);
      const yandex = product.yandex || {};
      const currentPictures = splitList(yandex.pictures);
      product.yandex = normalizeYandexDraft({
        ...yandex,
        pictures: [...yandexPicturesForSend, ...currentPictures.filter((url) => !yandexPicturesForSend.includes(url))],
      });
      product.imageUrl = primaryUrl;
    }
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "yandex_card_quality_manual_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const mode = contentDraft && imageBatch.length ? "both" : (imageBatch.length ? "image" : "content");
    const yandexSend = await sendApprovedYandexProductContent(savedProduct, { mode, pictures: yandexPicturesForSend });
    response.json({ ok: Boolean(yandexSend.ok), product: savedProduct, row: buildAiQualityReviewRow(savedProduct), yandexSend });
    appendAudit(request, "yandex.card_quality.manual_send", {
      productId: product.id,
      offerId: normalizedBefore.offerId,
      mode,
      yandexSend,
      oldValue: before,
      newValue: { id: savedProduct.id, yandex: savedProduct.yandex || {}, aiContentDrafts: savedProduct.aiContentDrafts || [], aiImages: savedProduct.aiImages || [] },
    }).catch((auditError) => logger.warn("yandex quality manual send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/ai-drafts", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const marketplace = cleanText(request.query.marketplace || "").toLowerCase();
    const status = cleanText(request.query.status || "pending").toLowerCase();
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const rows = [];
    for (const product of warehouse.products || []) {
      const normalized = normalizeWarehouseProduct(product);
      if (marketplace && normalized.marketplace !== marketplace) continue;
      const allImageDrafts = normalized.aiImages || [];
      const latestImageDraft = allImageDrafts
        .filter((draft) => !status || draft.status === status)
        .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))[0] || null;
      const contentDrafts = (normalized.aiContentDrafts || [])
        .filter((draft) => !status || draft.status === status)
        .map((draft) => ({ type: "content", draft, relatedImageDraft: latestImageDraft }));
      const imageDrafts = allImageDrafts
        .filter((draft) => !status || draft.status === status)
        .map((draft) => ({ type: "image", draft }));
      for (const item of [...contentDrafts, ...imageDrafts]) {
        rows.push({
          product: {
            id: normalized.id,
            offerId: normalized.offerId,
            name: normalized.name,
            marketplace: normalized.marketplace,
            target: normalized.target,
            imageUrl: normalized.imageUrl,
            updatedAt: normalized.updatedAt,
            cardQuality: normalized.yandex?.extra?.cardQuality || null,
          },
          type: item.type,
          draft: item.draft,
          relatedImageDraft: item.relatedImageDraft || null,
        });
      }
    }
    rows.sort((a, b) => String(b.draft.createdAt || "").localeCompare(String(a.draft.createdAt || "")));
    response.json({ ok: true, drafts: rows.slice(0, limit), total: rows.length });
  } catch (error) {
    next(error);
  }
});

function findWarehouseProductForMarketplace(warehouse = {}, seedProduct = {}, marketplace = "") {
  const wanted = cleanText(marketplace || seedProduct.marketplace || "").toLowerCase();
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const siblings = expandWarehouseProductsToGroups(products, [seedProduct]);
  return siblings.find((item) => cleanText(item.marketplace).toLowerCase() === wanted)
    || (cleanText(seedProduct.marketplace).toLowerCase() === wanted ? seedProduct : null);
}

function applyAiImageDraftToProduct(product = {}, draftId = "", options = {}) {
  const next = { ...normalizeWarehouseProduct(product) };
  next.aiImages = normalizeAiImageDrafts(next.aiImages || []);
  const draft = next.aiImages.find((item) => item.id === draftId);
  if (!draft) {
    const error = new Error("AI image draft not found.");
    error.statusCode = 404;
    error.code = "ai_image_draft_not_found";
    throw error;
  }
  if (!draft.resultUrl) {
    const error = new Error("AI image draft has no result URL.");
    error.statusCode = 400;
    error.code = "ai_image_result_url_missing";
    throw error;
  }
  const batchDrafts = draft.batchId
    ? next.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
    : [draft];
  const batchUrls = [
    draft.resultUrl,
    ...batchDrafts.map((item) => item.resultUrl).filter((url) => url && url !== draft.resultUrl),
  ];
  const now = new Date().toISOString();
  draft.status = "approved";
  draft.reviewedAt = now;
  if (options.sentMarketplace) {
    draft.sentAt = now;
    draft.sentMarketplace = cleanText(options.sentMarketplace);
  }
  batchDrafts.forEach((item) => {
    if (item.status === "pending") {
      item.status = "approved";
      item.reviewedAt = now;
    }
    if (options.sentMarketplace) {
      item.sentAt = now;
      item.sentMarketplace = cleanText(options.sentMarketplace);
    }
  });

  if (next.marketplace === "yandex") {
    const yandex = next.yandex || {};
    const pictures = splitList(yandex.pictures);
    next.yandex = normalizeYandexDraft({
      ...yandex,
      pictures: [...batchUrls, ...pictures.filter((url) => !batchUrls.includes(url))],
    });
  } else {
    const ozon = next.ozon || {};
    const images = splitList(ozon.images);
    next.ozon = normalizeOzonDraft({
      ...ozon,
      primaryImage: draft.resultUrl,
      images: [...batchUrls, ...images.filter((url) => !batchUrls.includes(url))],
    });
  }
  next.imageUrl = draft.resultUrl;
  next.updatedAt = now;
  return { product: normalizeWarehouseProduct(next), draft, batchDrafts, batchUrls };
}

async function normalizeAiImageDraftUrlsForMarketplaceSend(product = {}, draftId = "", request = null) {
  product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
  const draft = product.aiImages.find((item) => item.id === draftId);
  if (!draft) {
    const error = new Error("AI image draft not found.");
    error.statusCode = 404;
    error.code = "ai_image_draft_not_found";
    throw error;
  }
  const batchDrafts = draft.batchId
    ? product.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
    : [draft];
  if (!batchDrafts.length || !draft.resultUrl) {
    throw marketplaceImageError("ai_image_result_url_missing", "В AI-черновике нет URL фото для отправки.");
  }
  for (const item of batchDrafts) {
    item.resultUrl = await normalizeMarketplaceImageUrlForSend(item.resultUrl, request);
  }
  draft.resultUrl = await normalizeMarketplaceImageUrlForSend(draft.resultUrl, request);
  return { draft, batchDrafts };
}

app.post("/api/warehouse/products/:id/ai-content/:draftId/approve", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Product not found." });
    const draft = (product.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI content draft not found." });
    if (draft.status !== "pending") return response.status(400).json({ error: "AI content draft is already reviewed." });
    const marketplace = cleanText(draft.marketplace || product.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const enhanced = applyAiContentDraftToProduct(product, draft, marketplace);
    Object.assign(product, enhanced);
    draft.status = "approved";
    draft.reviewedAt = new Date().toISOString();
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_approve", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const yandexSend = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(savedProduct, { mode: "content" })
      : { ok: true, skipped: true, reason: "not_yandex" };
    response.json({ ok: true, draft, product: savedProduct, yandexSend });
    appendAudit(request, "warehouse.ai_content.approve", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      marketplace,
      yandexSend,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {} }),
    }).catch((auditError) => logger.warn("ai content approve audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/:draftId/send", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const seedProduct = warehouse.products.find((item) => item.id === request.params.id);
    if (!seedProduct) return response.status(404).json({ error: "Product not found." });
    const sourceDraft = (seedProduct.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!sourceDraft) return response.status(404).json({ error: "AI content draft not found." });
    if (sourceDraft.status === "rejected") return response.status(400).json({ error: "AI content draft is rejected." });
    const marketplace = cleanText(request.body?.marketplace || sourceDraft.marketplace || seedProduct.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const product = findWarehouseProductForMarketplace(warehouse, seedProduct, marketplace);
    if (!product) return response.status(404).json({ error: `Marketplace product not found: ${marketplace}.`, code: "marketplace_product_not_found", marketplace });

    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    if (!product.aiContentDrafts.some((item) => item.id === sourceDraft.id)) {
      product.aiContentDrafts.push(normalizeAiContentDraft({ ...sourceDraft, marketplace }));
    }
    const draft = product.aiContentDrafts.find((item) => item.id === sourceDraft.id) || sourceDraft;
    const enhanced = applyAiContentDraftToProduct(product, draft, marketplace);
    const sendResult = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(enhanced, { mode: "content" })
      : await sendApprovedOzonProductContent(enhanced, { mode: "content" });
    const sendError = marketplaceSendResultError(sendResult, `${marketplace}_content_send_failed`);
    if (sendError) return next(sendError);

    Object.assign(product, enhanced);
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    const savedDraft = product.aiContentDrafts.find((item) => item.id === sourceDraft.id);
    if (savedDraft) {
      savedDraft.status = "approved";
      savedDraft.reviewedAt = savedDraft.reviewedAt || new Date().toISOString();
      savedDraft.sentAt = new Date().toISOString();
      savedDraft.sentMarketplace = marketplace;
      savedDraft.sendResult = sendResult;
    }
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, marketplace, mode: "content", target: savedProduct.target || marketplace, offerId: savedProduct.offerId, sentFields: sendResult.fields || ["name", "description"], result: sendResult, product: savedProduct, draft: savedDraft });
    appendAudit(request, "warehouse.ai_content.send", {
      productId: product.id,
      offerId: product.offerId,
      draftId: sourceDraft.id,
      marketplace,
      sendResult,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, aiContentDrafts: savedProduct.aiContentDrafts || [] }),
    }).catch((auditError) => logger.warn("ai content send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-content/:draftId/reject", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Product not found." });
    const draft = (product.aiContentDrafts || []).find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI content draft not found." });
    draft.status = "rejected";
    draft.reviewedAt = new Date().toISOString();
    product.aiContentDrafts = normalizeAiContentDrafts(product.aiContentDrafts || []);
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_content_reject", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, product: savedProduct });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/:draftId/approve", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], yandex: product.yandex || {}, ozon: product.ozon || {}, imageUrl: product.imageUrl || "", updatedAt: product.updatedAt });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    let draft = product.aiImages.find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI-черновик изображения не найден." });
    if (!draft.resultUrl) return response.status(400).json({ error: "В AI-черновике нет URL результата." });

    let batchDrafts = draft.batchId
      ? product.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
      : [draft];
    if (normalizeWarehouseProduct(product).marketplace === "yandex") {
      const prepared = await normalizeAiImageDraftUrlsForMarketplaceSend(product, draft.id, request);
      draft = prepared.draft;
      batchDrafts = prepared.batchDrafts;
    }
    draft.status = "approved";
    draft.reviewedAt = new Date().toISOString();
    batchDrafts.forEach((item) => {
      if (item.status === "pending") {
        item.status = "approved";
        item.reviewedAt = draft.reviewedAt;
      }
    });
    const batchUrls = [draft.resultUrl, ...batchDrafts.map((item) => item.resultUrl).filter((url) => url && url !== draft.resultUrl)];
    const marketplace = normalizeWarehouseProduct(product).marketplace === "yandex" ? "yandex" : "ozon";
    const extraCardUrls = await marketplaceExtraCardUrls(marketplace, request);
    const sendBatchUrls = appendUniqueImages(batchUrls, extraCardUrls).slice(0, 10);
    if (normalizeWarehouseProduct(product).marketplace === "yandex") {
      const yandex = product.yandex || {};
      const pictures = splitList(yandex.pictures);
      product.yandex = normalizeYandexDraft({
        ...yandex,
        pictures: [...sendBatchUrls, ...pictures.filter((url) => !sendBatchUrls.includes(url))],
      });
    } else {
      const ozon = product.ozon || {};
      const images = splitList(ozon.images);
      product.ozon = normalizeOzonDraft({
        ...ozon,
        primaryImage: draft.resultUrl,
        images: [...sendBatchUrls, ...images.filter((url) => !sendBatchUrls.includes(url))],
      });
    }
    product.imageUrl = draft.resultUrl;
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_approve", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const yandexSend = normalizeWarehouseProduct(savedProduct).marketplace === "yandex"
      ? await sendApprovedYandexProductContent(savedProduct, { mode: "image", pictures: sendBatchUrls })
      : { ok: true, skipped: true, reason: "not_yandex" };
    response.json({ ok: true, draft, product: savedProduct, yandexSend });
    appendAudit(request, "warehouse.ai_image.approve", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      oldValue: before,
      yandexSend,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, imageUrl: savedProduct.imageUrl || "", updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image approve audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/:draftId/send", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const seedProduct = warehouse.products.find((item) => item.id === request.params.id);
    if (!seedProduct) return response.status(404).json({ error: "Product not found." });
    const sourceDraft = (normalizeAiImageDrafts(seedProduct.aiImages || [])).find((item) => item.id === request.params.draftId);
    if (!sourceDraft) return response.status(404).json({ error: "AI image draft not found." });
    if (!sourceDraft.resultUrl) return response.status(400).json({ error: "AI image draft has no result URL.", code: "ai_image_result_url_missing" });
    if (sourceDraft.status === "rejected") return response.status(400).json({ error: "AI image draft is rejected." });
    const marketplace = cleanText(request.body?.marketplace || seedProduct.marketplace || "yandex").toLowerCase() === "ozon" ? "ozon" : "yandex";
    const product = findWarehouseProductForMarketplace(warehouse, seedProduct, marketplace);
    if (!product) return response.status(404).json({ error: `Marketplace product not found: ${marketplace}.`, code: "marketplace_product_not_found", marketplace });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    if (!product.aiImages.some((item) => item.id === sourceDraft.id)) {
      const sourceBatch = sourceDraft.batchId
        ? normalizeAiImageDrafts(seedProduct.aiImages || []).filter((item) => item.batchId === sourceDraft.batchId)
        : [sourceDraft];
      product.aiImages.push(...sourceBatch.map((item) => normalizeAiImageDraft(item)).filter(Boolean));
    }
    await normalizeAiImageDraftUrlsForMarketplaceSend(product, sourceDraft.id, request);
    const applied = applyAiImageDraftToProduct(product, sourceDraft.id, { sentMarketplace: marketplace });
    const extraCardUrls = await marketplaceExtraCardUrls(marketplace, request);
    if (extraCardUrls.length) {
      if (marketplace === "yandex") {
        const yandex = applied.product.yandex || {};
        applied.product.yandex = normalizeYandexDraft({
          ...yandex,
          pictures: appendUniqueImages(splitList(yandex.pictures), extraCardUrls),
        });
      } else {
        const ozon = applied.product.ozon || {};
        applied.product.ozon = normalizeOzonDraft({
          ...ozon,
          images: appendUniqueImages(splitList(ozon.images), extraCardUrls),
        });
      }
    }
    const sendResult = marketplace === "yandex"
      ? await sendApprovedYandexProductContent(applied.product, { mode: "image", pictures: appendUniqueImages(applied.batchUrls, extraCardUrls) })
      : await sendApprovedOzonProductContent(applied.product, { mode: "image" });
    const sendError = marketplaceSendResultError(sendResult, `${marketplace}_image_send_failed`);
    if (sendError) return next(sendError);

    Object.assign(product, applied.product);
    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const sentIds = new Set([sourceDraft.id, ...applied.batchDrafts.map((item) => item.id)]);
    product.aiImages.forEach((item) => {
      if (!sentIds.has(item.id)) return;
      item.status = "approved";
      item.reviewedAt = item.reviewedAt || new Date().toISOString();
      item.sentAt = new Date().toISOString();
      item.sentMarketplace = marketplace;
      item.sendResult = sendResult;
    });
    product.updatedAt = new Date().toISOString();
    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_send", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    const savedDraft = (savedProduct.aiImages || []).find((item) => item.id === sourceDraft.id) || sourceDraft;
    response.json({ ok: true, marketplace, mode: "image", target: savedProduct.target || marketplace, offerId: savedProduct.offerId, sentFields: sendResult.fields || ["images"], result: sendResult, product: savedProduct, draft: savedDraft });
    appendAudit(request, "warehouse.ai_image.send", {
      productId: product.id,
      offerId: product.offerId,
      draftId: sourceDraft.id,
      marketplace,
      sendResult,
      newValue: cloneAuditValue({ id: savedProduct.id, yandex: savedProduct.yandex || {}, ozon: savedProduct.ozon || {}, aiImages: savedProduct.aiImages || [] }),
    }).catch((auditError) => logger.warn("ai image send audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/ai-images/:draftId/reject", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const draft = product.aiImages.find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI-черновик изображения не найден." });

    draft.status = "rejected";
    draft.reviewedAt = new Date().toISOString();
    if (draft.batchId) {
      product.aiImages
        .filter((item) => item.batchId === draft.batchId && item.status === "pending")
        .forEach((item) => {
          item.status = "rejected";
          item.reviewedAt = draft.reviewedAt;
        });
    }
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouseProductPatch([product], { reason: "warehouse_ai_image_reject", writeLinks: false });
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, product: savedProduct });
    appendAudit(request, "warehouse.ai_image.reject", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      oldValue: before,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image reject audit failed", { detail: auditError?.message || String(auditError) }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/enrich", async (request, response, next) => {
  try {
    const products = await enrichWarehouseProducts(request.body.productIds || request.body.ids || []);
    response.json({ ok: true, products });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/repair-weak-ozon", async (request, response, next) => {
  try {
    const limit = Math.max(1, Math.min(1000, Number(request.body?.limit || 400) || 400));
    const warehouse = await readWarehouse();
    const totalWeak = (warehouse.products || []).filter(isWeakOzonWarehouseProduct).length;
    const productIds = pickWeakOzonProductIds(warehouse.products || [], limit);
    if (!productIds.length) {
      return response.json({ ok: true, totalWeak: 0, processed: 0, updated: 0, remainingWeak: 0, products: [] });
    }
    const products = await enrichWarehouseProducts(productIds);
    const nextWarehouse = await readWarehouse();
    const remainingWeak = (nextWarehouse.products || []).filter(isWeakOzonWarehouseProduct).length;
    response.json({
      ok: true,
      totalWeak,
      processed: productIds.length,
      updated: products.length,
      remainingWeak,
      products,
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const expectedUpdatedAt = cleanText(request.body.expectedUpdatedAt || "");
    if (expectedUpdatedAt && cleanText(product.updatedAt || "") !== expectedUpdatedAt) {
      return response.status(409).json({
        error: "Конфликт обновления: карточка уже изменена другим пользователем.",
        code: "warehouse_product_conflict",
        currentUpdatedAt: product.updatedAt || null,
      });
    }
    const before = cloneAuditValue(product);

    if (request.body.markup !== undefined) {
      const markup = Number(request.body.markup);
      product.markup = Number.isFinite(markup) && markup > 0 ? markup : 0;
    }
    if (request.body.autoPriceEnabled !== undefined) product.autoPriceEnabled = Boolean(request.body.autoPriceEnabled);
    if (request.body.autoPriceMin !== undefined) {
      const value = Number(request.body.autoPriceMin);
      product.autoPriceMin = Number.isFinite(value) && value > 0 ? roundPrice(value) : null;
    }
    if (request.body.autoPriceMax !== undefined) {
      const value = Number(request.body.autoPriceMax);
      product.autoPriceMax = Number.isFinite(value) && value > 0 ? roundPrice(value) : null;
    }
    if (request.body.keyword !== undefined) product.keyword = cleanText(request.body.keyword);
    product.updatedAt = new Date().toISOString();

    await writeWarehouseProductPatch([product], { reason: "warehouse_product_update", writeLinks: false });
    const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
    await appendAudit(request, "warehouse.product.update", {
      productId: product.id,
      offerId: product.offerId,
      oldValue: before,
      newValue: product,
    });
    response.json({ ok: true, product: freshProduct || normalizeWarehouseProduct(product) });
    queueImmediateAutoPricePush([product.id], "product_patch");
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/markups/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    const optimisticLocks = new Map(
      (Array.isArray(request.body.optimisticLocks) ? request.body.optimisticLocks : [])
        .map((item) => [String(item?.id || ""), cleanText(item?.expectedUpdatedAt || "")]),
    );
    const markup = Number(request.body.markup);
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для изменения наценки." });
    if (!Number.isFinite(markup) || markup <= 0) return response.status(400).json({ error: "Укажите наценку больше нуля." });

    const warehouse = await readWarehouse();
    const conflicts = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      const expectedUpdatedAt = optimisticLocks.get(product.id);
      if (!expectedUpdatedAt) continue;
      if (cleanText(product.updatedAt || "") !== expectedUpdatedAt) {
        conflicts.push({
          id: product.id,
          offerId: product.offerId || "",
          expectedUpdatedAt,
          currentUpdatedAt: product.updatedAt || null,
        });
      }
    }
    if (conflicts.length) {
      return response.status(409).json({
        error: "Конфликт обновления: часть карточек уже изменена другим пользователем.",
        code: "warehouse_bulk_conflict",
        conflicts,
      });
    }
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, markup: product.markup, updatedAt: product.updatedAt }));
      product.markup = markup;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }

    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_markup_bulk_update", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.markups.bulk_update", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: products.map((product) => ({ id: product.id, markup: product.markup, updatedAt: product.updatedAt })),
    });
    response.json({ ok: true, changed, products });
    queueImmediateAutoPricePush(Array.from(ids), "bulk_markup_patch");
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/auto-price/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для изменения AUTO-режима." });
    const enabled = Boolean(request.body.enabled);

    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, autoPriceEnabled: product.autoPriceEnabled, updatedAt: product.updatedAt }));
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }

    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_auto_price_bulk_update", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.auto_price.bulk_update", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: products.map((product) => ({ id: product.id, autoPriceEnabled: product.autoPriceEnabled, updatedAt: product.updatedAt })),
    });
    response.json({ ok: true, changed, products });
    if (enabled) queueImmediateAutoPricePush(Array.from(ids), "bulk_auto_enable");
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/auto-price/all", async (request, response, next) => {
  try {
    const enabled = Boolean(request.body.enabled);
    const warehouse = await readWarehouse();
    let changed = 0;
    const changedIds = [];
    for (const product of warehouse.products) {
      if (Boolean(product.autoPriceEnabled !== false) === enabled) continue;
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouse(warehouse);
    await appendAudit(request, "warehouse.auto_price.all_update", { productIds: changedIds, newValue: { enabled } });
    response.json({ ok: true, changed, products: [] });
    if (enabled) queueImmediateAutoPricePush([], "auto_all_enable");
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/group", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (Object.prototype.hasOwnProperty.call(request.body || {}, "stockOnlyManualPrices")) {
      if (!ids.size) return response.status(400).json({ error: "Select a product group for stock-only fallback prices." });
      const warehouse = await readWarehouse();
      const seedProducts = warehouse.products.filter((product) => ids.has(String(product.id)));
      const productsToChange = expandWarehouseProductsToGroups(warehouse.products, seedProducts);
      const prices = normalizeStockOnlyManualPrices(request.body.stockOnlyManualPrices);
      const oldValues = [];
      const now = new Date().toISOString();
      for (const product of productsToChange) {
        oldValues.push(cloneAuditValue({ id: product.id, stockOnlyManualPrices: product.stockOnlyManualPrices || null, updatedAt: product.updatedAt }));
        product.stockOnlyManualPrices = prices;
        product.updatedAt = now;
      }
      if (productsToChange.length) {
        await writeWarehouseProductPatch(productsToChange, { reason: "warehouse_stock_only_manual_prices", writeLinks: false });
      }
      const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsToChange);
      await appendAudit(request, "warehouse.stock_only_manual_prices.save", { productIds: productsToChange.map((product) => product.id), oldValue: oldValues, newValue: prices });
      return response.json({ ok: true, changed: productsToChange.length, stockOnlyManualPrices: prices, products });
    }
    if (ids.size < 2) return response.status(400).json({ error: "Выберите минимум два товара для объединения." });
    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    const groupId = cleanText(request.body.groupId) || `manual-${crypto.randomUUID()}`;
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, manualGroupId: product.manualGroupId, updatedAt: product.updatedAt }));
      product.manualGroupId = groupId;
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_group", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.group", { productIds: changedIds, oldValue: oldValues, newValue: { groupId } });
    response.json({ ok: true, groupId, changed, products });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/ungroup", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для разъединения." });
    const warehouse = await readWarehouse();
    const productsToChange = warehouse.products.filter((product) => ids.has(product.id));
    const conflicts = collectProductConflicts(productsToChange, productLocksFromRequest(request.body));
    if (conflicts.length) return conflictResponse(response, conflicts);
    let changed = 0;
    const changedIds = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, manualGroupId: product.manualGroupId, updatedAt: product.updatedAt }));
      product.manualGroupId = "";
      product.updatedAt = new Date().toISOString();
      changed += 1;
      changedIds.push(product.id);
    }
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => changedIds.includes(product.id)),
      { reason: "warehouse_ungroup", writeLinks: false },
    );
    const changedProducts = warehouse.products.filter((product) => changedIds.includes(product.id));
    const products = await buildFreshWarehouseProductsFromKnownProducts(warehouse, changedProducts);
    await appendAudit(request, "warehouse.ungroup", { productIds: changedIds, oldValue: oldValues, newValue: { groupId: "" } });
    response.json({ ok: true, changed, products });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    warehouse.products = warehouse.products.filter((product) => product.id !== request.params.id);
    await writeWarehouse(warehouse);
    await appendAudit(request, "warehouse.product.delete", { productId: product.id, offerId: product.offerId, oldValue: product });
    response.json({ ok: true, deletedId: request.params.id });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/links/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : [])
      .map((id) => String(id || "").trim())
      .filter(Boolean));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для привязки." });

    const initialWarehouse = await readWarehouse();
    const initialSeeds = (initialWarehouse.products || []).filter((product) => ids.has(String(product.id)));
    if (!initialSeeds.length) return response.status(404).json({ error: "Warehouse products not found." });
    const expandedProductIds = expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds).map((product) => String(product.id));

    return await withWarehouseProductMutationLock(expandedProductIds, async () => {
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const warehouse = await readWarehouse();
    const linkSaveLookupOptions = { live: true, timeoutMs: 2500, cacheEmpty: false };
    const rawLinks = Array.isArray(request.body.links) && request.body.links.length ? request.body.links : [request.body];
    const submittedLinks = Array.from(new Map(rawLinks
      .map((link) => normalizeWarehouseLink(link))
      .filter(warehouseLinkHasMatchTarget)
      .map((link) => [warehouseLinkTargetKey(link), link])).values());
    const resolvedLinks = (await Promise.all(submittedLinks.map((submittedLink) =>
      resolvePriceMasterLinkForSave(submittedLink, usdRate, warehouse.suppliers, linkSaveLookupOptions),
    ))).filter(warehouseLinkHasMatchTarget);
    const baseLinks = Array.from(new Map(resolvedLinks
      .map((link) => [warehouseLinkTargetKey(link), link])).values());
    const baseLink = baseLinks[0] || normalizeWarehouseLink({});
    if (!baseLink.article && !baseLink.exactName && !baseLink.sourceRowId) {
      return response.status(400).json({ error: "Укажите артикул PriceMaster или выберите строку PriceMaster по названию." });
    }
    const seedProducts = (warehouse.products || []).filter((product) => ids.has(String(product.id)));
    const targetProducts = expandWarehouseProductsToGroups(warehouse.products || [], seedProducts);
    const expandedIds = new Set(targetProducts.map((product) => String(product.id)));
    if (!targetProducts.length) return response.status(404).json({ error: "РўРѕРІР°СЂС‹ СЃРєР»Р°РґР° РЅРµ РЅР°Р№РґРµРЅС‹." });
    const now = new Date().toISOString();
    const username = requestUsername(request);
    const commonLinks = buildCommonWarehouseGroupLinks(targetProducts, baseLinks, { now, username });
    const commonSignature = commonLinks.map((link) => warehouseLinkTargetKey(link)).sort().join("||");
    const locks = productLocksFromRequest(request.body);
    const conflicts = collectProductConflictsExceptBackground(targetProducts, locks, { mergeOnly: true });
    const blockingConflicts = conflicts.filter((conflict) => {
      const product = targetProducts.find((item) => String(item.id) === String(conflict.id));
      return !canIgnoreStaleLinkSaveConflict(product, commonLinks, locks.get(String(conflict.id)));
    });
    if (blockingConflicts.length) {
      const alreadyApplied = targetProducts.length > 0 && targetProducts.every((product) => warehouseProductLinksSignature(product) === commonSignature);
      if (!alreadyApplied) return conflictResponse(response, blockingConflicts);
      const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, targetProducts, { usdRate });
      return response.json({
        ok: true,
        changed: savedProducts.length || targetProducts.length,
        products: savedProducts,
        persisted: "already_written",
        alreadyWritten: true,
        expandedProductIds: targetProducts.map((product) => product.id),
        groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
      });
    }
    const failedLinks = [];
    for (const [index, linkToValidate] of baseLinks.entries()) {
      try {
        await assertPriceMasterLinkExists(linkToValidate, usdRate, warehouse.suppliers, linkSaveLookupOptions);
      } catch (error) {
        failedLinks.push(priceMasterLinkValidationFailure(error, linkToValidate, index));
      }
    }
    if (failedLinks.length) {
      return response.status(400).json({
        error: `PriceMaster validation failed for ${failedLinks.length} link(s). Nothing was saved.`,
        code: "PM_LINK_BULK_VALIDATION_FAILED",
        failedLinks,
      });
    }
    const updatedIds = [];
    const oldValues = [];

    for (const product of warehouse.products) {
      if (!expandedIds.has(String(product.id))) continue;
      const beforeDetailsSignature = warehouseProductLinkDetailsSignature(product);
      const beforeValue = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
      product.links = commonLinks.map((link) => normalizeWarehouseLink({
        ...link,
        createdAt: link.createdAt || now,
        updatedAt: now,
        createdBy: link.createdBy || username,
        updatedBy: username,
      }));
      if (warehouseProductLinkDetailsSignature(product) === beforeDetailsSignature) continue;
      oldValues.push(beforeValue);
      product.autoPriceEnabled = true;
      product.updatedAt = now;
      updatedIds.push(product.id);
    }

    if (!targetProducts.length) return response.status(404).json({ error: "Товары склада не найдены." });
    const groupSignatureAfterMutation = warehouseGroupLinkSignature(targetProducts);
    if (!groupSignatureAfterMutation.ok) {
      return response.status(409).json({
        error: "PriceMaster links were not synchronized for the whole group. Nothing was queued for marketplace updates.",
        code: "warehouse_group_links_not_synced",
        groupLinkSignature: groupSignatureAfterMutation,
      });
    }

    if (!updatedIds.length) {
      const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, targetProducts, { usdRate });
      return response.json({
        ok: true,
        changed: 0,
        products: savedProducts,
        persisted: "unchanged",
        unchanged: true,
        expandedProductIds: targetProducts.map((product) => product.id),
        groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
      });
    }

    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => updatedIds.includes(product.id)),
      { reason: "warehouse_links_bulk_save" },
    );
    const updatedProducts = targetProducts;
    const savedProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, updatedProducts, { usdRate });
    response.json({
      ok: true,
      changed: savedProducts.length || updatedIds.length,
      products: savedProducts,
      persisted: "written",
      expandedProductIds: targetProducts.map((product) => product.id),
      groupLinkSignature: warehouseGroupLinkSignature(savedProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(savedProducts),
    });
    appendAudit(request, "warehouse.links.bulk_save", {
      productIds: updatedIds,
      links: baseLinks.map((link) => ({
        article: link.article,
        matchType: link.matchType,
        exactName: link.exactName,
        sourceRowId: link.sourceRowId,
        keyword: link.keyword,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        priceCurrency: link.priceCurrency,
      })),
      article: baseLink.article,
      matchType: baseLink.matchType,
      exactName: baseLink.exactName,
      sourceRowId: baseLink.sourceRowId,
      keyword: baseLink.keyword,
      supplierName: baseLink.supplierName,
      priceCurrency: baseLink.priceCurrency,
      oldValue: oldValues,
      newValue: savedProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    const expandedUpdatedIds = targetProducts.map((product) => product.id);
    queueMarketplaceJob("supplier-recovery-automation", { productIds: expandedUpdatedIds }, { priority: 1 });
    queueImmediateAutoPricePush(expandedUpdatedIds, "link_bulk_add_or_update");
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/links", async (request, response, next) => {
  try {
    return await withWarehouseProductMutationLock([request.params.id], async () => {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body?.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    const beforeDetailsSignature = warehouseProductLinkDetailsSignature(product);

    let link = normalizeWarehouseLink(request.body);
    if (!link.article && !link.exactName && !link.sourceRowId) {
      return response.status(400).json({ error: "Укажите артикул PriceMaster или выберите строку PriceMaster по названию." });
    }
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const linkSaveLookupOptions = { live: true, timeoutMs: 2500, cacheEmpty: false };
    link = await resolvePriceMasterLinkForSave(link, usdRate, warehouse.suppliers, linkSaveLookupOptions);
    await assertPriceMasterLinkExists(link, usdRate, warehouse.suppliers, linkSaveLookupOptions);
    const now = new Date().toISOString();
    const username = requestUsername(request);
    product.links = Array.isArray(product.links) ? product.links : [];
    const index = product.links.findIndex((item) => item.id === link.id || warehouseLinksEqualForSave(item, link));
    if (index >= 0) {
      product.links[index] = mergeWarehouseLinkForSave(product.links[index], link, { now, username });
    }
    else product.links.push(normalizeWarehouseLink({
      ...link,
      createdAt: link.createdAt || now,
      updatedAt: now,
      createdBy: link.createdBy || username,
      updatedBy: username,
    }));
    product.links = compactWarehouseLinks(product.links);
    if (warehouseProductLinkDetailsSignature(product) === beforeDetailsSignature) {
      const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product], { usdRate });
      const normalized = freshProduct || normalizeWarehouseProduct(product);
      return response.json({ ok: true, product: normalized, links: normalized.links || [], persisted: "unchanged", unchanged: true });
    }
    if (product.links.length > 0) product.autoPriceEnabled = true;
    product.updatedAt = now;
    await writeWarehouseProductPatch([product], { reason: "warehouse_link_save" });
    const [savedProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product], { usdRate });
    response.json({ ok: true, product: savedProduct || normalizeWarehouseProduct(product), links: (savedProduct || product).links || [], persisted: "written" });
    appendAudit(request, "warehouse.link.save", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      article: link.article,
      matchType: link.matchType,
      exactName: link.exactName,
      sourceRowId: link.sourceRowId,
      keyword: link.keyword,
      supplierName: link.supplierName,
      priceCurrency: link.priceCurrency,
      oldValue: before,
      newValue: { id: savedProduct?.id || product.id, links: (savedProduct || product).links || [], updatedAt: (savedProduct || product).updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("supplier-recovery-automation", { productIds: [product.id] }, { priority: 1 });
    queueImmediateAutoPricePush([product.id], "link_add_or_update");
    });
  } catch (error) {
    next(error);
  }
});

async function buildWarehouseLinkMutationResponseProducts(warehouse, products = [], options = {}) {
  const targetProducts = Array.isArray(products) ? products.filter((product) => product?.id) : [];
  const productsWithLinks = targetProducts.filter((product) => (product.links || []).length);
  const builtProducts = productsWithLinks.length
    ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsWithLinks, options)
    : [];
  const builtById = new Map(builtProducts.map((product) => [String(product.id), product]));
  return targetProducts.map((product) => {
    const built = builtById.get(String(product.id));
    if (built) return built;
    return {
      ...normalizeWarehouseProduct(product),
      links: [],
      suppliers: [],
      selectedSupplier: null,
      selectedSupplierReason: "No saved PriceMaster links.",
      ready: false,
      changed: false,
      hasLinks: false,
      status: "no_links",
    };
  });
}

async function deleteWarehouseGroupLinkRefs(request, response, refsInput = []) {
  const refs = (Array.isArray(refsInput) ? refsInput : [])
    .map((ref) => {
      const normalized = {
        ...ref,
        productId: cleanText(ref.productId),
        linkId: cleanText(ref.linkId),
        linkTargetKey: cleanText(ref.linkTargetKey || ref.targetKey),
        expectedUpdatedAt: cleanText(ref.expectedUpdatedAt),
        expectedLinksSignature: cleanText(ref.expectedLinksSignature),
        article: cleanText(ref.article || ref.supplierArticle),
        supplierArticle: cleanText(ref.supplierArticle || ref.article),
        supplierName: cleanText(ref.supplierName),
        partnerId: cleanText(ref.partnerId),
        rowId: cleanText(ref.rowId),
        sourceRowId: cleanText(ref.sourceRowId || ref.rowId),
        exactName: cleanText(ref.exactName || ref.name),
        matchType: cleanText(ref.matchType),
        keyword: cleanText(ref.keyword),
        priceCurrency: cleanText(ref.priceCurrency || "USD"),
      };
      normalized.linkTargetKey = normalized.linkTargetKey || (warehouseLinkHasMatchTarget(normalized) ? warehouseLinkTargetKey(normalized) : "");
      return normalized;
    })
    .filter((ref) => ref.productId && (ref.linkId || ref.linkTargetKey));
  if (!refs.length) return response.status(400).json({ error: "No PriceMaster links selected for delete." });

  const requestedIds = new Set(refs.map((ref) => String(ref.productId)));
  const initialWarehouse = await readWarehouse();
  const initialSeeds = (initialWarehouse.products || []).filter((product) => requestedIds.has(String(product.id)));
  const expandedProductIds = expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds)
    .map((product) => String(product.id));
  if (!expandedProductIds.length) {
    return response.json({ ok: true, changed: 0, products: [], persisted: "already_deleted", alreadyDeleted: true, deletedRefs: [], alreadyDeletedRefs: refs });
  }

  return await withWarehouseProductMutationLock(expandedProductIds, async () => {
    const warehouse = await readWarehouse();
    const seedProducts = (warehouse.products || []).filter((product) => requestedIds.has(String(product.id)));
    const targetProducts = expandWarehouseProductsToGroups(warehouse.products || [], seedProducts);
    const targetIds = new Set(targetProducts.map((product) => String(product.id)));
    const deleteKeys = new Set();
    const deletedRefs = [];
    const alreadyDeletedRefs = [];
    const conflicts = [];

    for (const ref of refs) {
      const product = warehouse.products.find((item) => String(item.id) === String(ref.productId));
      if (!product) {
        alreadyDeletedRefs.push(ref);
        continue;
      }
      const productLinks = Array.isArray(product.links) ? product.links : [];
      const link = productLinks.find((item) => ref.linkId && String(item.id) === String(ref.linkId))
        || productLinks.find((item) => ref.linkTargetKey && warehouseLinkTargetKey(item) === ref.linkTargetKey);
      if (!link) {
        const siblingHasLink = targetProducts.some((targetProduct) => (targetProduct.links || [])
          .some((item) => ref.linkTargetKey && warehouseLinkTargetKey(item) === ref.linkTargetKey));
        if (siblingHasLink && ref.linkTargetKey) {
          deleteKeys.add(ref.linkTargetKey);
          deletedRefs.push(ref);
        } else {
          alreadyDeletedRefs.push(ref);
        }
        continue;
      }
      const conflict = productConflict(product, {
        expectedUpdatedAt: ref.expectedUpdatedAt,
        expectedLinksSignature: ref.expectedLinksSignature,
      });
      if (conflict) {
        conflicts.push(conflict);
        continue;
      }
      deleteKeys.add(ref.linkTargetKey || warehouseLinkTargetKey(link));
      deletedRefs.push(ref);
    }

    if (conflicts.length) return conflictResponse(response, conflicts);
    const expandedIds = targetProducts.map((product) => product.id);
    if (!deleteKeys.size) {
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
      return response.json({
        ok: true,
        changed: 0,
        products: responseProducts,
        persisted: "already_deleted",
        alreadyDeleted: true,
        deletedRefs,
        alreadyDeletedRefs,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      });
    }

    const changedProducts = [];
    const oldValues = [];
    for (const product of warehouse.products) {
      if (!targetIds.has(String(product.id))) continue;
      const previousLinks = Array.isArray(product.links) ? product.links : [];
      const nextLinks = compactWarehouseLinks(previousLinks.filter((link) => !deleteKeys.has(warehouseLinkTargetKey(link))));
      if (warehouseProductLinkDetailsSignature({ ...product, links: nextLinks }) === warehouseProductLinkDetailsSignature(product)) continue;
      oldValues.push(cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt }));
      product.links = nextLinks;
      product.updatedAt = new Date().toISOString();
      changedProducts.push(product);
    }

    if (!changedProducts.length) {
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
      return response.json({
        ok: true,
        changed: 0,
        products: responseProducts,
        persisted: "already_deleted",
        alreadyDeleted: true,
        deletedRefs,
        alreadyDeletedRefs,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      });
    }

    await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_group_delete" });
    const changedIds = changedProducts.map((product) => product.id);
    const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
    response.json({
      ok: true,
      changed: changedProducts.length,
      products: responseProducts,
      persisted: "written",
      deletedRefs,
      alreadyDeletedRefs,
      expandedProductIds: expandedIds,
      groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
      marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
    });
    appendAudit(request, "warehouse.links.bulk_delete", {
      productIds: changedIds,
      oldValue: oldValues,
      newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("no-supplier-automation", { productIds: changedIds }, { priority: 1 });
    const idsWithRemainingLinks = responseProducts.filter((product) => (product.links || []).length).map((product) => product.id);
    if (idsWithRemainingLinks.length) queueImmediateAutoPricePush(idsWithRemainingLinks, "link_delete");
  });
}

app.post("/api/warehouse/products/links/delete", async (request, response, next) => {
  try {
    const refs = (Array.isArray(request.body?.refs) ? request.body.refs : [])
      .map((ref) => ({
        ...ref,
        productId: cleanText(ref.productId),
        linkId: cleanText(ref.linkId),
        linkTargetKey: cleanText(ref.linkTargetKey || ref.targetKey),
        expectedUpdatedAt: cleanText(ref.expectedUpdatedAt),
        expectedLinksSignature: cleanText(ref.expectedLinksSignature),
      }))
      .filter((ref) => ref.productId && (ref.linkId || ref.linkTargetKey || warehouseLinkHasMatchTarget(ref)));
    if (!refs.length) return response.status(400).json({ error: "Не выбраны привязки для удаления." });
    return await deleteWarehouseGroupLinkRefs(request, response, refs);
    const refsByProduct = new Map();
    for (const ref of refs) {
      if (!refsByProduct.has(ref.productId)) refsByProduct.set(ref.productId, []);
      refsByProduct.get(ref.productId).push(ref);
    }

    return await withWarehouseProductMutationLock(Array.from(refsByProduct.keys()), async () => {
      const warehouse = await readWarehouse();
      const changedProducts = [];
      const changedIds = [];
      const oldValues = [];
      const deletedRefs = [];
      const alreadyDeletedRefs = [];
      const conflicts = [];

      for (const [productId, productRefs] of refsByProduct.entries()) {
        const product = warehouse.products.find((item) => String(item.id) === productId);
        if (!product) continue;
        const previousLinks = Array.isArray(product.links) ? product.links : [];
        const linkIds = new Set(productRefs.map((ref) => String(ref.linkId)));
        const removed = previousLinks.some((link) => linkIds.has(String(link.id)));
        if (!removed) {
          alreadyDeletedRefs.push(...productRefs);
          continue;
        }
        const lockRef = productRefs.find((ref) => ref.expectedUpdatedAt || ref.expectedLinksSignature) || productRefs[0] || {};
        const conflict = productConflict(product, {
          expectedUpdatedAt: lockRef.expectedUpdatedAt,
          expectedLinksSignature: lockRef.expectedLinksSignature,
        });
        if (conflict) {
          conflicts.push(conflict);
          continue;
        }
        oldValues.push(cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt }));
        product.links = compactWarehouseLinks(previousLinks.filter((link) => !linkIds.has(String(link.id))));
        product.updatedAt = new Date().toISOString();
        changedProducts.push(product);
        changedIds.push(product.id);
        deletedRefs.push(...productRefs);
      }

      if (conflicts.length) return conflictResponse(response, conflicts);
      if (!changedProducts.length) {
        return response.json({ ok: true, changed: 0, products: [], persisted: "already_deleted", alreadyDeleted: true, deletedRefs, alreadyDeletedRefs });
      }

      await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_bulk_delete" });
      const idsWithRemainingLinks = changedProducts.filter((product) => (product.links || []).length).map((product) => product.id);
      const productsWithRemainingLinks = changedProducts.filter((product) => (product.links || []).length);
      const builtProducts = productsWithRemainingLinks.length ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, productsWithRemainingLinks) : [];
      const builtById = new Map(builtProducts.map((product) => [String(product.id), product]));
      const responseProducts = changedProducts.map((product) => {
        const built = builtById.get(String(product.id));
        if (built) return built;
        return {
          ...normalizeWarehouseProduct(product),
          links: [],
          suppliers: [],
          selectedSupplier: null,
          selectedSupplierReason: "Нет сохранённых привязок.",
          ready: false,
          changed: false,
          hasLinks: false,
          status: "no_links",
        };
      });

      response.json({ ok: true, changed: changedProducts.length, products: responseProducts, persisted: "written", deletedRefs, alreadyDeletedRefs });
      appendAudit(request, "warehouse.links.bulk_delete", {
        productIds: changedIds,
        oldValue: oldValues,
        newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
      }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
      queueMarketplaceJob("no-supplier-automation", { productIds: changedIds }, { priority: 1 });
      if (idsWithRemainingLinks.length) queueImmediateAutoPricePush(idsWithRemainingLinks, "link_delete");
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:productId/links/:linkId", async (request, response, next) => {
  try {
    return await deleteWarehouseGroupLinkRefs(request, response, [{
      ...(request.body || {}),
      productId: request.params.productId,
      linkId: request.params.linkId,
      linkTargetKey: request.body?.linkTargetKey || request.body?.targetKey || request.query?.linkTargetKey || request.query?.targetKey,
      expectedUpdatedAt: request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt,
      expectedLinksSignature: request.body?.expectedLinksSignature || request.query?.expectedLinksSignature,
    }]);
    return await withWarehouseProductMutationLock([request.params.productId], async () => {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.productId);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    const previousLinks = Array.isArray(product.links) ? product.links : [];
    const removed = previousLinks.some((link) => String(link.id) === String(request.params.linkId));
    const conflict = productConflict(product, {
      expectedUpdatedAt: request.body?.expectedUpdatedAt || request.query?.expectedUpdatedAt,
      expectedLinksSignature: request.body?.expectedLinksSignature || request.query?.expectedLinksSignature,
    });
    if (conflict && !removed) return conflictResponse(response, [conflict]);
    if (!removed) {
      const [freshProduct] = await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]);
      const responseProduct = freshProduct || normalizeWarehouseProduct(product);
      return response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "already_deleted", alreadyDeleted: true });
    }
    product.links = previousLinks.filter((link) => String(link.id) !== String(request.params.linkId));
    product.links = compactWarehouseLinks(product.links);
    product.updatedAt = new Date().toISOString();
    await writeWarehouseProductPatch([product], { reason: "warehouse_link_delete" });
    const [savedProduct] = product.links.length ? await buildFreshWarehouseProductsFromKnownProducts(warehouse, [product]) : [];
    const responseProduct = savedProduct || {
      ...normalizeWarehouseProduct(product),
      links: [],
      suppliers: [],
      selectedSupplier: null,
      selectedSupplierReason: "Нет сохранённых привязок.",
      ready: false,
      changed: false,
      hasLinks: false,
      status: "no_links",
    };
    response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "written" });
    appendAudit(request, "warehouse.link.delete", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      linkId: request.params.linkId,
      oldValue: before,
      newValue: { id: responseProduct.id, links: responseProduct.links || [], updatedAt: responseProduct.updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("no-supplier-automation", { productIds: [request.params.productId] }, { priority: 1 });
    if ((responseProduct.links || []).length) queueImmediateAutoPricePush([request.params.productId], "link_delete");
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/links/sync-group", async (request, response, next) => {
  try {
    const productIds = (Array.isArray(request.body?.productIds) ? request.body.productIds : [])
      .map((id) => cleanText(id))
      .filter(Boolean);
    const groupKey = cleanText(request.body?.groupKey);
    const initialWarehouse = await readWarehouse();
    const initialSeeds = groupKey
      ? warehouseProductsForGroupKey(initialWarehouse.products || [], groupKey)
      : (initialWarehouse.products || []).filter((product) => productIds.includes(String(product.id)));
    if (!initialSeeds.length) return response.status(404).json({ error: "Warehouse group not found." });
    const expandedProductIds = expandWarehouseProductsToGroups(initialWarehouse.products || [], initialSeeds)
      .map((product) => String(product.id));

    return await withWarehouseProductMutationLock(expandedProductIds, async () => {
      const warehouse = await readWarehouse();
      const seedProducts = groupKey
        ? warehouseProductsForGroupKey(warehouse.products || [], groupKey)
        : (warehouse.products || []).filter((product) => productIds.includes(String(product.id)));
      const targetProducts = expandWarehouseProductsToGroups(warehouse.products || [], seedProducts);
      if (!targetProducts.length) return response.status(404).json({ error: "Warehouse group not found." });

      const now = new Date().toISOString();
      const username = requestUsername(request);
      const syncResult = syncWarehouseProductGroupLinks(targetProducts, { now, username });
      const changedProducts = syncResult.changedProducts || [];
      const expandedIds = targetProducts.map((product) => product.id);
      if (changedProducts.length) {
        await writeWarehouseProductPatch(changedProducts, { reason: "warehouse_links_sync_group" });
      }
      const responseProducts = await buildWarehouseLinkMutationResponseProducts(warehouse, targetProducts);
      response.json({
        ok: true,
        changed: changedProducts.length,
        products: responseProducts,
        persisted: changedProducts.length ? "written" : "unchanged",
        unchanged: !changedProducts.length,
        expandedProductIds: expandedIds,
        groupLinkSignature: warehouseGroupLinkSignature(responseProducts),
        marketplacePriceBreakdown: marketplacePriceBreakdown(responseProducts),
      });
      if (changedProducts.length) {
        appendAudit(request, "warehouse.links.sync_group", {
          productIds: changedProducts.map((product) => product.id),
          oldValue: syncResult.oldValues || [],
          newValue: responseProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
        }).catch((auditError) => logger.warn("link sync audit append failed", { detail: auditError?.message || String(auditError) }));
        queueMarketplaceJob("supplier-recovery-automation", { productIds: expandedIds }, { priority: 1 });
        queueImmediateAutoPricePush(expandedIds, "link_sync_group");
      }
    });
  } catch (error) {
    next(error);
  }
});

function warehousePriceMarketplaceStats(items = [], failed = [], skipped = []) {
  const failedIds = new Set((Array.isArray(failed) ? failed : []).map((item) => String(item.id || item.productId || "")));
  const successItems = (Array.isArray(items) ? items : []).filter((item) => !failedIds.has(String(item.id || item.productId || "")));
  const count = (rows, marketplace) => rows.filter((item) => cleanText(item.marketplace).toLowerCase() === marketplace).length;
  return {
    ozonSent: count(successItems, "ozon"),
    ozonFailed: count(Array.isArray(failed) ? failed : [], "ozon"),
    ozonSkipped: count(Array.isArray(skipped) ? skipped : [], "ozon"),
    yandexSent: count(successItems, "yandex"),
    yandexFailed: count(Array.isArray(failed) ? failed : [], "yandex"),
    yandexSkipped: count(Array.isArray(skipped) ? skipped : [], "yandex"),
  };
}

function salesAutomationReason(value = "") {
  const reason = cleanText(value || "ok");
  const map = {
    no_pricemaster_link: "no_supplier",
    not_ready: "no_supplier",
    no_next_price: "no_price",
    ozon_price_delayed: "in_retry",
    ozon_unarchive_daily_limit_queued: "ozon_limit",
    stock_only_manual_price_missing: "stock_only_manual_price_missing",
    unchanged: "unchanged",
    send_failed: "api_error",
  };
  return map[reason] || reason || "ok";
}

async function upsertSalesAutomationSkuStates(rows = []) {
  if (!shouldUsePostgresStorage() || !rows.length) return { updated: 0, skipped: true };
  try {
    const prisma = getPrisma();
    let updated = 0;
    for (const batch of chunkArray(rows, 250)) {
      const validBatch = batch.filter((row) => cleanText(row.offerId));
      await prisma.$transaction(validBatch.map((row) => {
        const marketplace = cleanText(row.marketplace).toLowerCase() === "yandex" ? "yandex" : "ozon";
        const target = cleanText(row.target) || "default";
        const offerId = cleanText(row.offerId);
        updated += 1;
        return prisma.salesAutomationSkuState.upsert({
          where: { marketplace_target_offerId: { marketplace, target, offerId } },
          create: {
            productId: cleanText(row.productId || row.id) || null,
            marketplace,
            target,
            offerId,
            currentPrice: Number(row.currentPrice ?? row.oldPrice ?? 0) || null,
            targetPrice: Number(row.targetPrice ?? row.price ?? 0) || null,
            targetStock: Number(row.targetStock ?? row.stock ?? 0) || null,
            priceStatus: row.priceStatus || "pending",
            stockStatus: row.stockStatus || "pending",
            unarchiveStatus: row.unarchiveStatus || "pending",
            reason: salesAutomationReason(row.reason || "ok"),
            lastCalculatedAt: toDateOrNull(row.lastCalculatedAt) || new Date(),
            lastPriceSentAt: toDateOrNull(row.lastPriceSentAt),
            lastStockSentAt: toDateOrNull(row.lastStockSentAt),
            lastError: cleanText(row.lastError || row.error) || null,
            raw: row,
          },
          update: {
            productId: cleanText(row.productId || row.id) || null,
            currentPrice: Number(row.currentPrice ?? row.oldPrice ?? 0) || null,
            targetPrice: Number(row.targetPrice ?? row.price ?? 0) || null,
            targetStock: Number(row.targetStock ?? row.stock ?? 0) || null,
            priceStatus: row.priceStatus || "pending",
            stockStatus: row.stockStatus || "pending",
            unarchiveStatus: row.unarchiveStatus || "pending",
            reason: salesAutomationReason(row.reason || "ok"),
            lastCalculatedAt: toDateOrNull(row.lastCalculatedAt) || new Date(),
            lastPriceSentAt: toDateOrNull(row.lastPriceSentAt),
            lastStockSentAt: toDateOrNull(row.lastStockSentAt),
            lastError: cleanText(row.lastError || row.error) || null,
            raw: row,
          },
        });
      }));
    }
    return { updated };
  } catch (error) {
    if (!jsonFallbackEnabled()) throw error;
    logger.warn("sales automation state upsert failed", { detail: error?.message || String(error) });
    return { updated: 0, error: error?.message || String(error) };
  }
}

async function updateSalesAutomationFromPriceResult({ items = [], failed = [], skipped = [], stockActions = [], sentAt = new Date().toISOString() } = {}) {
  const failedById = new Map((failed || []).map((item) => [cleanText(item.id || item.productId), item]));
  const stockById = new Map((stockActions || []).map((item) => [cleanText(item.id), item]));
  const rows = [];
  for (const item of items || []) {
    const failedItem = failedById.get(cleanText(item.id || item.productId));
    const stock = stockById.get(cleanText(item.id || item.productId));
    rows.push({
      ...item,
      productId: item.productId || item.id,
      currentPrice: item.oldPrice,
      targetPrice: item.price,
      targetStock: stock?.stock ?? null,
      priceStatus: failedItem ? (isOzonPerItemPriceLimitError({ message: failedItem.error }) ? "delayed" : "failed") : "success",
      stockStatus: stock ? (stock.ok ? "success" : "failed") : "pending",
      unarchiveStatus: stock?.queuedByDailyLimit ? "delayed" : "pending",
      reason: failedItem ? (isOzonPerItemPriceLimitError({ message: failedItem.error }) ? "in_retry" : "api_error") : "ok",
      lastPriceSentAt: failedItem ? null : sentAt,
      lastStockSentAt: stock?.ok ? sentAt : null,
      lastError: failedItem?.error || stock?.error || "",
      lastCalculatedAt: sentAt,
    });
  }
  for (const row of skipped || []) {
    rows.push({
      ...row,
      productId: row.productId || row.id,
      priceStatus: row.reason === "unchanged" ? "success" : (row.reason === "ozon_price_delayed" ? "delayed" : "pending"),
      stockStatus: "pending",
      unarchiveStatus: row.reason === "ozon_price_delayed" ? "delayed" : "pending",
      reason: row.reason,
      lastError: row.error || "",
      lastCalculatedAt: sentAt,
    });
  }
  return upsertSalesAutomationSkuStates(rows);
}

async function sendWarehousePrices({
  productIds,
  usdRate,
  minDiffRub = 0,
  minDiffPct = 0,
  dryRun = false,
  force = false,
  refreshMarketplacePrices = false,
  marketplace = "all",
  onlyChanged = false,
  livePriceMaster = false,
  reason = "auto-price-push",
  limit,
} = {}) {
  const ids = Array.isArray(productIds) ? new Set(productIds.map(String)) : null;
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const normalizedMarketplaceFilter = ["ozon", "yandex"].includes(marketplaceFilter) ? marketplaceFilter : "all";
  const normalizedLimit = Math.max(0, Math.round(Number(limit || 0) || 0));
  let preview = null;
  let selected = [];
  if (ids) {
    const settings = await readAppSettings();
    const rate = Number(settings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    preview = { usdRate: rate };
    selected = await buildFreshWarehouseProducts(Array.from(ids), {
      refreshPrices: Boolean(refreshMarketplacePrices),
      usdRate: rate,
      livePriceMaster: Boolean(livePriceMaster),
      batchPriceMaster: Boolean(livePriceMaster),
    });
  } else {
    preview = await buildWarehouseView({ usdRate: Number(usdRate || 0) || undefined });
    selected = preview.products;
  }
  if (normalizedMarketplaceFilter !== "all") {
    selected = selected.filter((product) => cleanText(product.marketplace).toLowerCase() === normalizedMarketplaceFilter);
  }
  if (normalizedLimit > 0) selected = selected.slice(0, normalizedLimit);
  const skipped = [];
  const items = [];
  const stockItems = [];
  const sentAt = new Date().toISOString();
  const queueState = dryRun ? { items: [] } : await readPriceRetryQueue().catch((error) => {
    logger.warn("price retry queue read failed before price send", { detail: error?.message || String(error) });
    return { items: [] };
  });
  const delayedQueueUpdates = [];

  for (const product of selected) {
    if (!product.hasLinks) {
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason: "no_pricemaster_link" });
      continue;
    }
    if (shouldSendTargetStockForProduct(product)) stockItems.push(product);
    if (!product.ready) {
      const reason = product.priceSource === "timeout"
        ? "pm_live_timeout"
        : (product.hasLinks && !product.selectedSupplier ? "no_supplier" : "not_ready");
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason, priceSource: product.priceSource || null });
      continue;
    }
    if (product.stockOnlyFallbackActive && !(Number(product.nextPrice || 0) > 0)) {
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: "stock_only_manual_price_missing",
        supplier: product.selectedSupplier,
      });
      continue;
    }
    if (onlyChanged && !force && product.changed === false) {
      skipped.push({ id: product.id, offerId: product.offerId, marketplace: product.marketplace, reason: "unchanged" });
      continue;
    }
    const current = Number(product.currentPrice || 0);
    const nextValue = Number(product.nextPrice || 0);
    const skipDecision = shouldSkipWarehousePriceSend({
      currentPrice: current,
      nextPrice: nextValue,
      minDiffRub,
      minDiffPct,
      force,
    });
    if (skipDecision.skip) {
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: skipDecision.reason,
        diffRub: skipDecision.diffRub,
        diffPct: skipDecision.diffPct,
      });
      continue;
    }
    const priceItem = {
      id: product.id,
      productId: product.id,
      target: product.target,
      offerId: product.offerId,
      price: product.nextPrice,
      oldPrice: product.currentPrice,
      markup: product.markupCoefficient,
      supplier: product.selectedSupplier,
      marketplace: product.marketplace,
    };
    const delayedRetry = product.marketplace === "ozon"
      ? findActiveDelayedPriceRetry(queueState.items, priceItem, new Date(sentAt))
      : null;
    if (delayedRetry) {
      skipped.push({
        id: product.id,
        offerId: product.offerId,
        marketplace: product.marketplace,
        reason: "ozon_price_delayed",
        nextRetryAt: delayedRetry.nextRetryAt,
        error: delayedRetry.error || "ozon_per_item_price_limit",
      });
      delayedQueueUpdates.push({
        ...delayedRetry,
        id: product.id,
        productId: product.id,
        target: product.target,
        offerId: product.offerId,
        price: product.nextPrice,
        oldPrice: product.currentPrice,
        status: "delayed",
        retryReason: delayedRetry.retryReason || "ozon_per_item_price_limit",
        updatedAt: sentAt,
      });
      continue;
    }
    items.push(priceItem);
  }

  if (!dryRun && items.length) {
    logger.info("warehouse price push selection", {
      reason: cleanText(reason) || "auto-price-push",
      selected: selected.length,
      readyToSend: items.length,
      productIds: items.slice(0, 25).map((item) => item.id),
      firstOfferId: items[0]?.offerId || null,
      selectedSupplier: items[0]?.supplier?.partnerName || items[0]?.supplier?.supplierName || null,
      effectiveFinalPrice: Number(items[0]?.supplier?.effectiveFinalPrice || items[0]?.price || 0) || null,
      alternativesCount: Array.isArray(selected[0]?.supplierAlternatives) ? selected[0].supplierAlternatives.length : 0,
      pmSource: selected[0]?.priceSource || items[0]?.supplier?.priceSource || null,
    });
  }

  if (dryRun) {
    const stats = warehousePriceMarketplaceStats(items, [], skipped);
    return {
      ok: true,
      dryRun: true,
      selected: selected.length,
      readyToSend: items.length,
      stockReadyToSend: stockItems.length,
      skipped,
      items,
      ...stats,
    };
  }

  const results = [];
  const failed = [];
  for (const account of getOzonAccounts()) {
    const targetItems = items.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
    const ozonItems = targetItems
      .map((item) => ({ item, payload: buildOzonPricePayload(item) }))
      .filter((entry) => entry.payload.offer_id && Number(entry.payload.price) > 0);
    if (!ozonItems.length) continue;
    const sent = await sendOzonPricePayloadChunks(account, ozonItems.map((entry) => entry.payload));
    results.push(...sent.results.map((entry) => ({ target: account.id, response: entry.response, count: entry.count })));
    const failedOfferIds = new Map(sent.failed.map((entry) => [String(entry.payload.offer_id), entry.error]));
    failed.push(...ozonItems
      .filter((entry) => failedOfferIds.has(String(entry.payload.offer_id)))
      .map((entry) => ({
        ...entry.item,
        error: failedOfferIds.get(String(entry.payload.offer_id))?.message || "send_failed",
        marketplace: "ozon",
      })));
  }

  for (const shop of getYandexShops()) {
    const targetItems = items.filter((item) => item.marketplace === "yandex" && matchesYandexTarget(item.target, shop.id));
    const yandexItems = targetItems
      .map((item) => ({
        offerId: String(item.offerId || "").trim(),
        price: { value: roundPrice(item.price), currencyId: "RUR" },
      }))
      .filter((item) => item.offerId && item.price.value > 0);
    if (!yandexItems.length) continue;
    try {
      for (const chunk of chunkArray(yandexItems, 500)) {
        results.push({
          target: shop.id,
          response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }),
        });
      }
    } catch (error) {
      const detail = error?.message || "send_failed";
      failed.push(...targetItems.map((item) => ({ ...item, error: detail, marketplace: "yandex" })));
    }
  }

  const stockActions = await sendTargetStocksToMarketplace(stockItems);

  const warehouse = await readWarehouse();
  const successIds = new Set(items.map((item) => item.id));
  for (const failedItem of failed) successIds.delete(failedItem.id);
  const postgresPriceHistoryRows = [];
  const touchedProductIds = new Set();
  for (const item of items) {
    const product = warehouse.products.find((entry) => entry.id === item.id);
    if (!product) continue;
    touchedProductIds.add(product.id);
    const success = successIds.has(item.id);
    const failedEntryForItem = failed.find((entry) => entry.id === item.id);
    const delayedByLimitForItem = failedEntryForItem ? isOzonPerItemPriceLimitError({ message: failedEntryForItem.error }) : false;
    const oldPriceAdjustedForItem = failedEntryForItem ? isOzonOldPriceLessError({ message: failedEntryForItem.error }) : false;
    const sendStatus = success ? "success" : (delayedByLimitForItem ? "delayed" : (oldPriceAdjustedForItem ? "pending" : "failed"));
    const retryNextAt = failedEntryForItem
      ? new Date(new Date(sentAt).getTime() + priceRetryDelayMs(Number(failedEntryForItem.attempts || 1), { message: failedEntryForItem.error })).toISOString()
      : null;
    if (success) {
      const sentPrice = roundPrice(item.price);
      product.marketplacePrice = sentPrice;
      if (item.marketplace === "ozon") {
        product.ozon = {
          ...(product.ozon || {}),
          price: sentPrice,
        };
      } else if (item.marketplace === "yandex") {
        product.yandex = {
          ...(product.yandex || {}),
          offerId: product.yandex?.offerId || item.offerId,
          price: sentPrice,
        };
      }
    }
    product.priceHistory = Array.isArray(product.priceHistory) ? product.priceHistory : [];
    const previous = product.priceHistory[product.priceHistory.length - 1] || null;
    const reasons = [];
    if (previous?.supplierArticle && previous.supplierArticle !== (item.supplier?.article || null)) reasons.push("смена поставщика");
    if (Number(previous?.usdRate || 0) !== Number(preview.usdRate || 0)) reasons.push("изменение курса");
    if (Number(previous?.usdPrice || 0) !== Number(item.supplier?.price || 0)) reasons.push("изменение прайса поставщика");
    if (!reasons.length) reasons.push("регулярный пересчет");
    const historyEntry = {
      at: sentAt,
      marketplace: item.marketplace,
      target: item.target,
      offerId: item.offerId,
      oldPrice: item.oldPrice || null,
      newPrice: roundPrice(item.price),
      markup: item.markup || null,
      supplierName: item.supplier?.partnerName || item.supplier?.supplierName || null,
      supplierArticle: item.supplier?.article || null,
      usdPrice: item.supplier?.price || null,
      usdRate: Number(preview.usdRate || 0) || null,
      reason: reasons.join(", "),
      status: sendStatus === "pending" ? "pending" : (success ? "success" : (delayedByLimitForItem ? "delayed" : "error")),
      error: success ? null : (failedEntryForItem?.error || "send_failed"),
    };
    const duplicateLocalHistory = product.priceHistory
      .slice(-20)
      .some((entry) => isDuplicatePriceHistoryEntry(entry, historyEntry, { now: new Date(sentAt) }));
    if (!duplicateLocalHistory) product.priceHistory.push(historyEntry);
    product.priceHistory = product.priceHistory.slice(-100);
    postgresPriceHistoryRows.push({
      productId: item.id,
      marketplace: item.marketplace,
      target: item.target,
      offerId: item.offerId,
      oldPrice: item.oldPrice || null,
      newPrice: roundPrice(item.price),
      status: sendStatus,
      error: historyEntry.error || "",
      at: sentAt,
    });
    const lastPriceSendBase = {
      status: sendStatus === "failed" ? "error" : sendStatus,
      at: sentAt,
      requestedPrice: roundPrice(item.price),
      cabinetPriceAtSend: Number(item.oldPrice || 0) || null,
      detail: failedEntryForItem ? failedEntryForItem.error : "ok",
      nextRetryAt: failedEntryForItem ? retryNextAt : null,
    };
    if (item.marketplace === "ozon") {
      product.lastOzonPriceSend = {
        ...lastPriceSendBase,
        oldPriceForRetry: oldPriceAdjustedForItem ? resolveOzonOldPrice(roundPrice(item.price), item) : null,
        detail: oldPriceAdjustedForItem
          ? "Ozon rejected old_price; old_price adjusted to 120% and retry queued."
          : lastPriceSendBase.detail,
      };
    } else if (item.marketplace === "yandex") {
      product.lastYandexPriceSend = {
        ...lastPriceSendBase,
      };
    }
  }
  for (const action of stockActions) {
    const product = warehouse.products.find((entry) => entry.id === action.id);
    if (!product) continue;
    touchedProductIds.add(product.id);
    product.lastStockSend = marketplaceCommandFromAction(action, product, sentAt);
    if (!action.ok) continue;
    const stock = Math.max(0, Math.round(Number(action.stock || 0)));
    const marketplaceState = {
      ...(product.marketplaceState || {}),
      stock,
    };
    if (product.marketplace === "yandex" && stock > 0) {
      marketplaceState.code = "active";
      marketplaceState.status = "active";
      marketplaceState.archived = false;
      product.status = "active";
      product.archived = false;
    }
    product.marketplaceState = {
      ...marketplaceState,
    };
  }
  if (touchedProductIds.size) {
    await writeWarehouseProductPatch(
      warehouse.products.filter((product) => touchedProductIds.has(product.id)),
      { reason: "warehouse_price_stock_send", writeLinks: false },
    );
  }
  appendPriceHistoryRows(postgresPriceHistoryRows).catch((error) => logger.warn("price history background append failed", { detail: error?.message || String(error) }));

  const failedQueued = failed.map((item) => buildPriceRetryItem({
    ...item,
    queueKey: `${item.id}:${item.target}`,
    queuedAt: sentAt,
  }, { message: item.error }, new Date(sentAt)));
  const merged = [...(queueState.items || []), ...delayedQueueUpdates, ...failedQueued];
  const deduped = Array.from(new Map(merged.map((item) => [priceRetryQueueKey(item), item])).values()).slice(0, 5000);
  if (failedQueued.length || delayedQueueUpdates.length) await writePriceRetryQueue({ items: deduped });
  schedulePriceRetryItems([...failedQueued, ...delayedQueueUpdates]);

  const stats = warehousePriceMarketplaceStats(items, failed, skipped);
  updateSalesAutomationFromPriceResult({ items, failed, skipped, stockActions, sentAt })
    .catch((error) => logger.warn("sales automation state background update failed", { detail: error?.message || String(error) }));
  return {
    ok: true,
    selected: selected.length,
    sent: items.length - failed.length,
    failed: failed.length,
    stockSent: stockActions.filter((item) => item.ok).length,
    stockFailed: stockActions.filter((item) => !item.ok).length,
    queued: deduped.length,
    delayed: delayedQueueUpdates.length,
    skipped,
    failedItems: failed,
    results,
    stockActions,
    ...stats,
  };
}

async function processMarketplaceJob(name, data = {}) {
  if (name === "auto-price-push") {
    return sendWarehousePrices({
      productIds: Array.isArray(data.productIds) ? data.productIds : undefined,
      usdRate: data.usdRate,
      minDiffRub: 0,
      minDiffPct: 0,
      force: data.force === true,
      dryRun: false,
      marketplace: data.marketplace || "all",
      onlyChanged: data.onlyChanged === true,
      refreshMarketplacePrices: data.refreshMarketplacePrices === true,
      livePriceMaster: data.livePriceMaster === true,
      reason: data.reason || "auto-price-push",
      limit: data.limit,
    });
  }
  if (name === "no-supplier-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds);
      return runNoSupplierMarketplaceAutomation({ products }, {
        productIds,
        includeNoLinks: true,
        source: "targeted",
      });
    }
    const preview = await buildWarehouseView({ sync: true });
    return runNoSupplierMarketplaceAutomation(preview, { source: "full_sync" });
  }
  if (name === "supplier-recovery-automation") {
    const productIds = Array.isArray(data.productIds)
      ? data.productIds.map((id) => String(id || "").trim()).filter(Boolean)
      : [];
    if (productIds.length) {
      const products = await buildFreshWarehouseProducts(productIds);
      return runSupplierRecoveryAutomation({ products }, { productIds, source: "targeted", force: data.force === true });
    }
    const preview = await buildWarehouseView({ sync: false });
    return runSupplierRecoveryAutomation(preview, { source: "full" });
  }
  if (name === "ozon-unarchive-queue-process") {
    return processOzonUnarchiveQueue({
      source: data.source || "ozon_unarchive_queue_auto",
      limit: data.limit || ozonUnarchiveQueueBatchLimit,
      force: data.force === true,
    });
  }
  return null;
}

async function sendPriceMasterDeltaWarehousePrices(priceMaster = {}, warehouse = {}, options = {}) {
  const usdRate = options.usdRate;
  if (!priceMasterDeltaPricePushEnabled) {
    return {
      ok: true,
      sent: 0,
      failed: 0,
      skipped: [],
      delta: { productIds: [], skipped: true, reason: "disabled" },
    };
  }
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  invalidateWarehouseViewCache();
  const delta = priceMasterChangeImpactProductIds(warehouse, priceMaster.changedRows || [], {
    maxChanges: options.maxChanges || priceMasterDeltaMaxChanges,
    maxProducts: options.maxProducts || priceMasterDeltaMaxProducts,
    fullReconcileOnTooMany: options.fullReconcileOnTooMany !== false,
    fullReconcileMaxProducts: options.fullReconcileMaxProducts || autoPriceReconcileMaxProducts,
  });
  if (delta.skipped) {
    logger.warn("PriceMaster delta price push limited", {
      reason: delta.reason,
      scannedChanges: delta.scannedChanges,
      impactedProducts: delta.productIds.length,
    });
  }
  if (!delta.productIds.length) {
    return {
      ok: true,
      sent: 0,
      failed: 0,
      skipped: [],
      delta,
    };
  }
  if (delta.fallbackFullReconcile) {
    const batches = chunkArray(delta.productIds, autoPriceReconcileBatchSize);
    const queuedProducts = delta.productIds.length;
    void (async () => {
      for (const batch of batches) {
        try {
          await queueMarketplaceJob(
            "auto-price-push",
            {
              productIds: batch,
              usdRate,
              refreshMarketplacePrices: true,
              livePriceMaster: true,
              marketplace: "all",
              onlyChanged: true,
              reason: delta.reason,
            },
            { priority: 2 },
          );
        } catch (error) {
          logger.warn("PriceMaster full reconcile price batch failed", {
            reason: delta.reason,
            batchSize: batch.length,
            detail: error?.message || String(error),
          });
        }
      }
    })();
    logger.info("PriceMaster delta price push queued full reconcile", {
      reason: delta.reason,
      scannedChanges: delta.scannedChanges,
      products: queuedProducts,
      batches: batches.length,
      batchSize: autoPriceReconcileBatchSize,
    });
    return {
      ok: true,
      sent: 0,
      failed: 0,
      queued: queuedProducts,
      queuedBatches: batches.length,
      skipped: [],
      delta,
    };
  }
  const result = await processMarketplaceJob("auto-price-push", {
    productIds: delta.productIds,
    usdRate,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
    marketplace: "all",
    onlyChanged: options.onlyChanged !== false,
    reason: delta.reason,
  });
  return {
    ...result,
    delta,
  };
}

function emptyNoSupplierAutomationResult(reason = "no_changed_products") {
  return {
    zeroStockSent: 0,
    archived: 0,
    errors: [],
    productStatuses: [],
    skipped: true,
    reason,
    source: "targeted",
  };
}

function emptySupplierRecoveryResult(reason = "no_changed_products") {
  return {
    recovered: 0,
    restoredStocks: 0,
    unarchived: 0,
    errors: [],
    productStatuses: [],
    skipped: true,
    reason,
    source: "targeted",
  };
}

async function runTargetedBackgroundSupplierAutomations(priceMaster = {}, warehouse = {}, options = {}) {
  const scope = backgroundAutomationProductIds(priceMaster, warehouse, options);
  if (!scope.productIds.length) {
    return {
      automation: emptyNoSupplierAutomationResult("no_changed_products"),
      recovery: emptySupplierRecoveryResult("no_changed_products"),
      scope,
    };
  }

  const ids = new Set(scope.productIds.map(String));
  const products = (Array.isArray(warehouse.products) ? warehouse.products : [])
    .filter((product) => ids.has(String(product.id)));
  if (!products.length) {
    return {
      automation: emptyNoSupplierAutomationResult("changed_products_not_in_view"),
      recovery: emptySupplierRecoveryResult("changed_products_not_in_view"),
      scope,
    };
  }

  const automation = await runNoSupplierMarketplaceAutomation(
    { products },
    { productIds: scope.productIds, includeNoLinks: true, source: "targeted" },
  );
  const recovery = await runSupplierRecoveryAutomation(
    { products },
    { productIds: scope.productIds, source: "targeted" },
  );
  return { automation, recovery, scope };
}

function marketplaceJobId(name, data = {}) {
  const productIds = Array.isArray(data?.productIds)
    ? data.productIds.map((id) => String(id)).filter(Boolean).sort()
    : [];
  const scope = productIds.length ? productIds.join("|") : "all";
  return crypto
    .createHash("sha1")
    .update(`${name}|${scope}`)
    .digest("hex");
}

function queueMarketplaceJob(name, data = {}, { priority = 5 } = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return Promise.resolve(null);
  if (name === "auto-price-push" && !marketplaceQueueAutoPricePushEnabled) {
    return processMarketplaceJob(name, data).catch((error) => {
      logger.warn("inline auto price push failed", { detail: error?.message || String(error) });
      throw error;
    });
  }
  if (marketplaceQueue) {
    return marketplaceQueue.add(name, data, {
      jobId: marketplaceJobId(name, data),
      priority,
      removeOnComplete: 2000,
      removeOnFail: 2000,
    }).catch((error) => {
      logger.warn("queue add failed, falling back to inline mode", { name, detail: error?.message || String(error) });
      return processMarketplaceJob(name, data);
    });
  }
  return processMarketplaceJob(name, data).catch((error) => {
    logger.warn("inline marketplace job failed", { name, detail: error?.message || String(error) });
    throw error;
  });
}

function initMarketplaceQueue() {
  if (!bullmqEnabled || !redisUrl) {
    logger.info("marketplace queue disabled, using inline mode");
    return;
  }
  try {
    const connection = { url: redisUrl, maxRetriesPerRequest: null };
    marketplaceQueue = new Queue("marketplace-tasks", { connection });
    marketplaceWorker = new Worker(
      "marketplace-tasks",
      async (job) => processMarketplaceJob(job.name, job.data || {}),
      {
        connection,
        concurrency: bullmqWorkerConcurrency,
        lockDuration: bullmqLockDurationMs,
        lockRenewTime: Math.floor(bullmqLockDurationMs / 2),
        stalledInterval: bullmqStalledIntervalMs,
        maxStalledCount: bullmqMaxStalledCount,
      },
    );
    marketplaceWorker.on("failed", (job, error) => {
      logger.warn("marketplace job failed", { job: job?.name, detail: error?.message || String(error) });
    });
    marketplaceWorker.on("completed", (job, result) => {
      if (job?.name === "auto-price-push" && result && typeof result === "object") {
        logger.info("immediate auto price push complete", {
          reason: job.data?.reason || "bullmq",
          scope: Array.isArray(job.data?.productIds) ? job.data.productIds.length : "all",
          sent: result.sent || 0,
          failed: result.failed || 0,
          stockSent: result.stockSent || 0,
          stockFailed: result.stockFailed || 0,
          ozonSent: result.ozonSent || 0,
          ozonFailed: result.ozonFailed || 0,
          ozonSkipped: result.ozonSkipped || 0,
          yandexSent: result.yandexSent || 0,
          yandexFailed: result.yandexFailed || 0,
          yandexSkipped: result.yandexSkipped || 0,
          skipped: Array.isArray(result.skipped) ? result.skipped.length : 0,
        });
        return;
      }
      logger.info("marketplace job complete", { job: job?.name || "unknown" });
    });
    marketplaceWorker.on("error", (error) => {
      logger.warn("marketplace worker error", { detail: error?.message || String(error) });
    });
    marketplaceQueue.on("error", (error) => {
      logger.warn("marketplace queue error", { detail: error?.message || String(error) });
    });
    logger.info("marketplace queue enabled", {
      mode: "bullmq",
      concurrency: bullmqWorkerConcurrency,
      lockDurationMs: bullmqLockDurationMs,
      stalledIntervalMs: bullmqStalledIntervalMs,
    });
  } catch (error) {
    marketplaceQueue = null;
    marketplaceWorker = null;
    logger.warn("marketplace queue init failed, fallback to inline mode", { detail: error?.message || String(error) });
  }
}

function queueImmediateAutoPricePush(productIds = [], reason = "price_change_detected", options = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return;
  if (options.force === true) immediateAutoPushForce = true;
  if (Array.isArray(productIds) && productIds.length) {
    productIds.forEach((id) => immediateAutoPushIds.add(String(id)));
  } else {
    immediateAutoPushAll = true;
  }
  if (immediateAutoPushTimer) return;
  immediateAutoPushTimer = setTimeout(() => {
    const ids = immediateAutoPushAll ? undefined : Array.from(immediateAutoPushIds);
    const force = immediateAutoPushForce;
    immediateAutoPushAll = false;
    immediateAutoPushForce = false;
    immediateAutoPushIds.clear();
    immediateAutoPushTimer = null;
    immediateAutoPushChain = immediateAutoPushChain
      .then(async () => {
        logger.info("immediate auto price push queued", { reason, scope: ids ? ids.length : "all" });
        const result = await queueMarketplaceJob(
          "auto-price-push",
          {
            productIds: ids,
            usdRate: undefined,
            minDiffRub: 0,
            minDiffPct: 0,
            force,
            reason,
            marketplace: options.marketplace || "all",
            onlyChanged: options.onlyChanged !== undefined ? options.onlyChanged === true : !force,
            refreshMarketplacePrices: options.refreshMarketplacePrices !== false,
            livePriceMaster: options.livePriceMaster !== false,
            limit: options.limit,
          },
          { priority: 1 },
        );
        if (result && typeof result === "object" && "sent" in result) {
          const skippedReasons = Array.isArray(result.skipped)
            ? result.skipped.reduce((acc, item) => {
              const reason = item.reason || "unknown";
              acc[reason] = (acc[reason] || 0) + 1;
              return acc;
            }, {})
            : {};
          logger.info("immediate auto price push complete", {
            reason,
            scope: ids ? ids.length : "all",
            force,
            sent: result.sent,
            failed: result.failed,
            stockSent: result.stockSent,
            stockFailed: result.stockFailed,
            ozonSent: result.ozonSent || 0,
            ozonFailed: result.ozonFailed || 0,
            ozonSkipped: result.ozonSkipped || 0,
            yandexSent: result.yandexSent || 0,
            yandexFailed: result.yandexFailed || 0,
            yandexSkipped: result.yandexSkipped || 0,
            skipped: Array.isArray(result.skipped) ? result.skipped.length : 0,
            skippedReasons,
          });
        }
      })
      .catch((error) => {
        logger.warn("immediate auto price push failed", { reason, detail: error?.message || String(error) });
      });
  }, 1200);
}

function queueChangedWarehousePrices(products = [], reason = "warehouse_changed_prices_detected") {
  const isPassiveWarehouseView = String(reason || "").startsWith("warehouse_page_")
    || String(reason || "").startsWith("warehouse_view_");
  if (isPassiveWarehouseView && process.env.AUTO_PRICE_FROM_WAREHOUSE_PAGE_ENABLED !== "true") return 0;
  const now = Date.now();
  const cooldownMs = Math.max(
    10_000,
    Number(process.env.AUTO_PRICE_CHANGED_COOLDOWN_MS || detectedPriceAutoPushDefaultCooldownMs)
      || detectedPriceAutoPushDefaultCooldownMs,
  );
  const batchCooldownMs = Math.max(
    1_000,
    Number(process.env.AUTO_PRICE_CHANGED_BATCH_COOLDOWN_MS || detectedPriceAutoPushDefaultBatchCooldownMs)
      || detectedPriceAutoPushDefaultBatchCooldownMs,
  );
  if (changedPriceAutoPushLastBatchAt && now - changedPriceAutoPushLastBatchAt < batchCooldownMs) return 0;
  if (changedPriceAutoPushAt.size > 20_000) {
    for (const [id, last] of changedPriceAutoPushAt.entries()) {
      if (!last || now - Number(last) > cooldownMs * 2) changedPriceAutoPushAt.delete(id);
    }
  }
  const ids = (Array.isArray(products) ? products : [])
    .filter((product) => {
      if (!product?.hasLinks) return false;
      if (product.changed && Number(product.nextPrice || 0) > 0) return true;
      return shouldSendTargetStockForProduct(product);
    })
    .map((product) => product.id)
    .filter(Boolean)
    .filter((id) => {
      const last = Number(changedPriceAutoPushAt.get(String(id)) || 0);
      if (last && now - last < cooldownMs) return false;
      changedPriceAutoPushAt.set(String(id), now);
      return true;
    });
  if (!ids.length) return 0;
  changedPriceAutoPushLastBatchAt = now;
  queueImmediateAutoPricePush(ids, reason);
  return ids.length;
}

async function processPriceRetryQueue({ queueKeys = [], limit = 1000, respectNextRetryAt = false, trigger = "manual" } = {}) {
  if (priceRetryRunning) return { ok: true, skipped: true, reason: "already_running", processed: 0, retried: 0, failed: 0, remaining: 0 };
  priceRetryRunning = true;
  try {
    const queue = await readPriceRetryQueue();
    if (!queue.items.length) return { ok: true, processed: 0, retried: 0, failed: 0, remaining: 0, results: [] };
    const requestedKeys = new Set((Array.isArray(queueKeys) ? queueKeys : []).map(String));
    const now = new Date();
    const selected = (requestedKeys.size
      ? queue.items.filter((item) => requestedKeys.has(String(priceRetryQueueKey(item))))
      : queue.items.filter((item) => !respectNextRetryAt || !item.nextRetryAt || new Date(item.nextRetryAt).getTime() <= now.getTime()))
      .slice(0, Math.max(1, Number(limit || 1000) || 1000));
    if (!selected.length) return { ok: true, processed: 0, retried: 0, failed: 0, remaining: queue.items.length, results: [] };

    const results = [];
  const failed = [];
  const historyRows = [];

  for (const account of getOzonAccounts()) {
      const targetItems = selected.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
      const ozonItems = targetItems.map((item) => ({ item, payload: buildOzonPricePayload(item) }))
        .filter((entry) => entry.payload.offer_id && Number(entry.payload.price) > 0);
      if (!ozonItems.length) continue;
      const sent = await sendOzonPricePayloadChunks(account, ozonItems.map((entry) => entry.payload));
      results.push(...sent.results.map((entry) => ({ target: account.id, response: entry.response, count: entry.count })));
      const failedOfferIds = new Map(sent.failed.map((entry) => [String(entry.payload.offer_id), entry.error]));
      const failedOfferIdSet = new Set(failedOfferIds.keys());
      for (const entry of ozonItems) {
        const error = failedOfferIds.get(String(entry.payload.offer_id));
        const delayed = error ? isOzonPerItemPriceLimitError(error) : false;
        const oldPriceAdjusted = error ? isOzonOldPriceLessError(error) : false;
        historyRows.push({
          productId: entry.item.productId || entry.item.id,
          marketplace: "ozon",
          target: entry.item.target,
          offerId: entry.item.offerId,
          oldPrice: entry.item.oldPrice,
          newPrice: entry.item.price,
          status: error ? (delayed ? "delayed" : (oldPriceAdjusted ? "pending" : "failed")) : "success",
          error: error?.message || "",
          at: now.toISOString(),
        });
      }
      failed.push(...ozonItems
        .filter((entry) => failedOfferIdSet.has(String(entry.payload.offer_id)))
        .map((entry) => buildPriceRetryItem(entry.item, failedOfferIds.get(String(entry.payload.offer_id)), now)));
    }

    for (const shop of getYandexShops()) {
      const targetItems = selected.filter((item) => item.marketplace === "yandex" && matchesYandexTarget(item.target, shop.id));
      const yandexItems = targetItems.map((item) => ({ offerId: String(item.offerId || "").trim(), price: { value: roundPrice(item.price), currencyId: "RUR" } }))
        .filter((item) => item.offerId && item.price.value > 0);
      if (!yandexItems.length) continue;
      try {
        for (const chunk of chunkArray(yandexItems, 500)) {
          results.push({ target: shop.id, response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }) });
        }
        historyRows.push(...targetItems.map((item) => ({
          productId: item.productId || item.id,
          marketplace: "yandex",
          target: item.target,
          offerId: item.offerId,
          oldPrice: item.oldPrice,
          newPrice: item.price,
          status: "success",
          error: "",
          at: now.toISOString(),
        })));
      } catch (error) {
        historyRows.push(...targetItems.map((item) => ({
          productId: item.productId || item.id,
          marketplace: "yandex",
          target: item.target,
          offerId: item.offerId,
          oldPrice: item.oldPrice,
          newPrice: item.price,
          status: "failed",
          error: error?.message || "send_failed",
          at: now.toISOString(),
        })));
        failed.push(...targetItems.map((item) => buildPriceRetryItem(item, error, now)));
      }
    }

    const processedKeys = new Set(selected.map((item) => String(priceRetryQueueKey(item))));
    const untouched = queue.items.filter((item) => !processedKeys.has(String(priceRetryQueueKey(item))));
    const remaining = [...failed, ...untouched];
    await writePriceRetryQueue({ items: remaining.slice(0, 5000) });
    appendPriceHistoryRows(historyRows).catch((error) => logger.warn("retry price history append failed", { detail: error?.message || String(error) }));
    schedulePriceRetryItems(remaining);
    return {
      ok: true,
      trigger,
      processed: selected.length,
      retried: selected.length - failed.length,
      failed: failed.length,
      remaining: remaining.length,
      results,
    };
  } finally {
    priceRetryRunning = false;
  }
}

app.post("/api/warehouse/prices/send", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Prices were not sent because manual confirmation is required." });
    }
    response.json(await sendWarehousePrices({
      productIds: Array.isArray(request.body.productIds) ? request.body.productIds : [],
      usdRate: Number(request.body.usdRate || 0) || undefined,
      minDiffRub: Number(request.body.minDiffRub || 0),
      minDiffPct: Number(request.body.minDiffPct || 0),
      dryRun: request.body.dryRun === true,
      force: request.body.force === true,
      refreshMarketplacePrices: request.body.refreshMarketplacePrices === true,
      marketplace: request.body.marketplace || "all",
      onlyChanged: request.body.onlyChanged === true,
      livePriceMaster: request.body.livePriceMaster === true,
      limit: request.body.limit,
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/preview", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const onlyChanged = String(request.query.onlyChanged ?? "true") !== "false";
    const limit = cleanLimit(request.query.limit, 200, 2000);
    response.json(await sendWarehousePrices({
      dryRun: true,
      marketplace,
      onlyChanged,
      limit,
      refreshMarketplacePrices: request.query.refreshMarketplacePrices === "true",
      livePriceMaster: request.query.livePriceMaster !== "false",
      force: request.query.force === "true",
    }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/sales-automation/summary", requireAdmin, async (_request, response, next) => {
  try {
    const [retryQueue, ozonQueue] = await Promise.all([
      readPriceRetryQueue().catch(() => ({ items: [] })),
      readOzonUnarchiveQueue().catch(() => ({ items: [] })),
    ]);
    if (shouldUsePostgresStorage()) {
      try {
        const prisma = getPrisma();
        const [total, reasons, statuses, latest] = await Promise.all([
          prisma.salesAutomationSkuState.count(),
          prisma.salesAutomationSkuState.groupBy({ by: ["reason"], _count: { _all: true } }),
          prisma.salesAutomationSkuState.groupBy({ by: ["marketplace", "priceStatus"], _count: { _all: true } }),
          prisma.salesAutomationSkuState.findFirst({ orderBy: { updatedAt: "desc" } }),
        ]);
        return response.json({
          ok: true,
          source: "postgres",
          autoEnabled: true,
          total,
          updatedAt: latest?.updatedAt?.toISOString?.() || null,
          retryTotal: retryQueue.items?.length || 0,
          ozonUnarchiveQueued: normalizeOzonUnarchiveQueue(ozonQueue).items.length,
          reasons: Object.fromEntries(reasons.map((row) => [row.reason || "unknown", row._count?._all || 0])),
          statuses: statuses.map((row) => ({ marketplace: row.marketplace, priceStatus: row.priceStatus, count: row._count?._all || 0 })),
        });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("sales automation summary postgres failed, using live preview fallback", { detail: error?.message || String(error) });
      }
    }
    const preview = await sendWarehousePrices({ dryRun: true, marketplace: "all", onlyChanged: false, limit: 500, livePriceMaster: true });
    response.json({
      ok: true,
      source: "preview",
      autoEnabled: true,
      total: Number(preview.selected || 0),
      updatedAt: new Date().toISOString(),
      retryTotal: retryQueue.items?.length || 0,
      ozonUnarchiveQueued: normalizeOzonUnarchiveQueue(ozonQueue).items.length,
      reasons: (preview.skipped || []).reduce((acc, row) => {
        const key = salesAutomationReason(row.reason || "unknown");
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {}),
      statuses: [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/sales-automation/items", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const reason = cleanText(request.query.reason || "");
    const q = cleanText(request.query.q || "");
    const limit = cleanLimit(request.query.limit, 200, 2000);
    if (shouldUsePostgresStorage()) {
      try {
        const where = {
          AND: [
            marketplace !== "all" && ["ozon", "yandex"].includes(marketplace) ? { marketplace } : {},
            reason ? { reason } : {},
            q ? {
              OR: [
                { offerId: { contains: q, mode: "insensitive" } },
                { productId: { contains: q, mode: "insensitive" } },
                { lastError: { contains: q, mode: "insensitive" } },
              ],
            } : {},
          ].filter((item) => Object.keys(item || {}).length),
        };
        const [total, items] = await Promise.all([
          getPrisma().salesAutomationSkuState.count({ where }),
          getPrisma().salesAutomationSkuState.findMany({ where, orderBy: { updatedAt: "desc" }, take: limit }),
        ]);
        return response.json({
          ok: true,
          source: "postgres",
          total,
          items: items.map((item) => ({
            id: item.id,
            productId: item.productId,
            marketplace: item.marketplace,
            target: item.target,
            offerId: item.offerId,
            currentPrice: item.currentPrice,
            targetPrice: item.targetPrice,
            targetStock: item.targetStock,
            priceStatus: item.priceStatus,
            stockStatus: item.stockStatus,
            unarchiveStatus: item.unarchiveStatus,
            reason: item.reason,
            lastCalculatedAt: item.lastCalculatedAt?.toISOString?.() || null,
            lastPriceSentAt: item.lastPriceSentAt?.toISOString?.() || null,
            lastStockSentAt: item.lastStockSentAt?.toISOString?.() || null,
            lastError: item.lastError || "",
            updatedAt: item.updatedAt?.toISOString?.() || null,
            raw: item.raw || {},
          })),
        });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("sales automation items postgres failed, using live preview fallback", { detail: error?.message || String(error) });
      }
    }
    const preview = await sendWarehousePrices({ dryRun: true, marketplace, onlyChanged: false, limit, livePriceMaster: true });
    const items = [
      ...(preview.items || []).map((item) => ({ ...item, reason: "ok", priceStatus: "pending" })),
      ...(preview.skipped || []).map((item) => ({ ...item, reason: salesAutomationReason(item.reason), priceStatus: item.reason === "unchanged" ? "success" : "pending" })),
    ].filter((item) => !reason || salesAutomationReason(item.reason) === reason);
    response.json({ ok: true, source: "preview", total: items.length, items: items.slice(0, limit) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/sales-automation/run", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.body?.marketplace || "all").toLowerCase();
    const force = request.body?.force === true;
    const onlyChanged = request.body?.onlyChanged !== false;
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "sales-automation-run",
      title: operationTitle("sales-automation-run"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: {
        marketplace,
        force,
        onlyChanged,
        limit: cleanLimit(request.body?.limit, 1000, 50000),
        reason: "sales_automation_manual",
      },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});

function problemProductCategoryFromAutomation(row = {}) {
  const reason = salesAutomationReason(row.reason || row.raw?.reason || "");
  if (reason === "no_supplier") return "no_supplier";
  if (reason === "no_price") return "no_price";
  if (reason === "stock_only_manual_price_missing") return "stock_only_manual_price_missing";
  if (reason === "api_error") return "api_error";
  if (reason === "in_retry") return "price_retry";
  if (reason === "ozon_limit") return "ozon_autoarchive";
  if (row.priceStatus === "failed" || row.stockStatus === "failed") return "api_error";
  return reason && reason !== "ok" && reason !== "unchanged" ? reason : "";
}

app.get("/api/problem-products", requireAdmin, async (request, response, next) => {
  try {
    const category = cleanText(request.query.category || "all");
    const q = cleanText(request.query.q || "");
    const limit = cleanLimit(request.query.limit, 200, 2000);
    let items = [];
    if (shouldUsePostgresStorage()) {
      try {
        const rows = await getPrisma().salesAutomationSkuState.findMany({
          where: {
            AND: [
              q ? {
                OR: [
                  { offerId: { contains: q, mode: "insensitive" } },
                  { productId: { contains: q, mode: "insensitive" } },
                  { lastError: { contains: q, mode: "insensitive" } },
                ],
              } : {},
              category !== "all" ? { reason: category === "price_retry" ? "in_retry" : category } : {},
            ].filter((item) => Object.keys(item || {}).length),
          },
          orderBy: { updatedAt: "desc" },
          take: limit,
        });
        items = rows
          .map((row) => ({
            id: row.id,
            productId: row.productId,
            marketplace: row.marketplace,
            target: row.target,
            offerId: row.offerId,
            category: problemProductCategoryFromAutomation(row),
            reason: row.reason,
            currentPrice: row.currentPrice,
            targetPrice: row.targetPrice,
            targetStock: row.targetStock,
            lastError: row.lastError || "",
            updatedAt: row.updatedAt?.toISOString?.() || null,
          }))
          .filter((row) => row.category);
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("problem products postgres failed, using preview fallback", { detail: error?.message || String(error) });
      }
    }
    if (!items.length) {
      const preview = await sendWarehousePrices({ dryRun: true, marketplace: "all", onlyChanged: false, limit, livePriceMaster: true });
      items = (preview.skipped || [])
        .map((row) => ({
          ...row,
          productId: row.productId || row.id,
          category: problemProductCategoryFromAutomation(row),
          reason: salesAutomationReason(row.reason),
        }))
        .filter((row) => row.category && (category === "all" || row.category === category));
    }
    const summary = items.reduce((acc, row) => {
      acc[row.category] = (acc[row.category] || 0) + 1;
      return acc;
    }, {});
    response.json({ ok: true, total: items.length, summary, items: items.slice(0, limit) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/problem-products/repair", requireAdmin, async (request, response, next) => {
  try {
    const productIds = Array.isArray(request.body?.productIds) ? request.body.productIds.map(cleanText).filter(Boolean) : [];
    if (!productIds.length) return response.status(400).json({ error: "No productIds selected.", code: "problem_products_empty" });
    const job = await upsertOperationJob({
      id: crypto.randomUUID(),
      type: "problem-products-repair",
      title: operationTitle("problem-products-repair"),
      status: "queued",
      user: request.session?.username || "system",
      role: request.session?.role || "admin",
      payload: { productIds: productIds.slice(0, 100) },
      progress: 0,
    });
    startOperationJob(job);
    response.status(202).json({ ok: true, accepted: true, job: operationJobPublic(job) });
  } catch (error) {
    next(error);
  }
});

function normalizeFinanceMoney(value, fallback = 0) {
  const n = Number(value ?? fallback);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : Number(fallback || 0);
}

function financeOrderProfit(row = {}) {
  const payout = normalizeFinanceMoney(row.payoutAmount ?? row.payout_amount ?? row.saleAmount ?? row.sale_amount, 0);
  const purchase = normalizeFinanceMoney(row.purchaseCost ?? row.purchase_cost, 0);
  const fees = normalizeFinanceMoney(row.feesAmount ?? row.fees_amount, 0);
  const tax = normalizeFinanceMoney(row.taxAmount ?? row.tax_amount, 0);
  const penalties = normalizeFinanceMoney(row.penaltiesAmount ?? row.penalties_amount, 0);
  const refunds = normalizeFinanceMoney(row.refundsAmount ?? row.refunds_amount, 0);
  return normalizeFinanceMoney(payout - purchase - fees - tax - penalties - refunds, 0);
}

function normalizeFinanceExpense(input = {}) {
  const spentAt = toDateOrNull(input.spentAt || input.spent_at) || new Date();
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    type: cleanText(input.type || "manual_purchase") || "manual_purchase",
    supplierName: cleanText(input.supplierName || input.supplier_name),
    partnerId: cleanText(input.partnerId || input.partner_id),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    amount: normalizeFinanceMoney(input.amount, 0),
    currency: cleanText(input.currency || "RUB").toUpperCase() || "RUB",
    note: cleanText(input.note),
    source: cleanText(input.source || "manual") || "manual",
    status: cleanText(input.status || "confirmed") || "confirmed",
    spentAt: spentAt.toISOString(),
    raw: input.raw && typeof input.raw === "object" ? input.raw : input,
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
}

function normalizeFinanceOrder(input = {}) {
  const marketplaceText = cleanText(input.marketplace).toLowerCase();
  const marketplace = marketplaceText === "ozon" || marketplaceText === "yandex" ? marketplaceText : "";
  const saleAmount = input.saleAmount ?? input.sale_amount;
  const payoutAmount = input.payoutAmount ?? input.payout_amount;
  const purchaseCost = input.purchaseCost ?? input.purchase_cost;
  const feesAmount = input.feesAmount ?? input.fees_amount;
  const taxAmount = input.taxAmount ?? input.tax_amount;
  const penaltiesAmount = input.penaltiesAmount ?? input.penalties_amount;
  const refundsAmount = input.refundsAmount ?? input.refunds_amount;
  const row = {
    id: cleanText(input.id) || crypto.randomUUID(),
    marketplace,
    target: cleanText(input.target),
    orderId: cleanText(input.orderId || input.order_id) || cleanText(input.postingNumber || input.posting_number) || `manual-${crypto.randomUUID()}`,
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    saleAmount: saleAmount === undefined || saleAmount === null || saleAmount === "" ? null : normalizeFinanceMoney(saleAmount, 0),
    payoutAmount: payoutAmount === undefined || payoutAmount === null || payoutAmount === "" ? null : normalizeFinanceMoney(payoutAmount, 0),
    purchaseCost: purchaseCost === undefined || purchaseCost === null || purchaseCost === "" ? null : normalizeFinanceMoney(purchaseCost, 0),
    feesAmount: feesAmount === undefined || feesAmount === null || feesAmount === "" ? null : normalizeFinanceMoney(feesAmount, 0),
    taxAmount: taxAmount === undefined || taxAmount === null || taxAmount === "" ? null : normalizeFinanceMoney(taxAmount, 0),
    penaltiesAmount: penaltiesAmount === undefined || penaltiesAmount === null || penaltiesAmount === "" ? null : normalizeFinanceMoney(penaltiesAmount, 0),
    refundsAmount: refundsAmount === undefined || refundsAmount === null || refundsAmount === "" ? null : normalizeFinanceMoney(refundsAmount, 0),
    supplierName: cleanText(input.supplierName || input.supplier_name),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual") || "manual",
    status: cleanText(input.status || "open") || "open",
    soldAt: toDateOrNull(input.soldAt || input.sold_at)?.toISOString?.() || null,
    receivedAt: toDateOrNull(input.receivedAt || input.received_at)?.toISOString?.() || null,
    raw: input.raw && typeof input.raw === "object" ? input.raw : input,
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    updatedAt: input.updatedAt || input.updated_at || new Date().toISOString(),
  };
  row.profitAmount = normalizeFinanceMoney(input.profitAmount ?? input.profit_amount ?? financeOrderProfit(row), 0);
  return row;
}

function financeOrderFromPostgres(row = {}) {
  return normalizeFinanceOrder({
    id: row.id,
    marketplace: row.marketplace || "",
    target: row.target || "",
    orderId: row.orderId,
    postingNumber: row.postingNumber,
    offerId: row.offerId,
    productName: row.productName,
    quantity: row.quantity,
    saleAmount: row.saleAmount === null || row.saleAmount === undefined ? null : Number(row.saleAmount),
    payoutAmount: row.payoutAmount === null || row.payoutAmount === undefined ? null : Number(row.payoutAmount),
    purchaseCost: row.purchaseCost === null || row.purchaseCost === undefined ? null : Number(row.purchaseCost),
    feesAmount: row.feesAmount === null || row.feesAmount === undefined ? null : Number(row.feesAmount),
    taxAmount: row.taxAmount === null || row.taxAmount === undefined ? null : Number(row.taxAmount),
    penaltiesAmount: row.penaltiesAmount === null || row.penaltiesAmount === undefined ? null : Number(row.penaltiesAmount),
    refundsAmount: row.refundsAmount === null || row.refundsAmount === undefined ? null : Number(row.refundsAmount),
    profitAmount: row.profitAmount === null || row.profitAmount === undefined ? null : Number(row.profitAmount),
    supplierName: row.supplierName,
    partnerId: row.partnerId,
    source: row.source,
    status: row.status,
    soldAt: row.soldAt?.toISOString?.() || null,
    receivedAt: row.receivedAt?.toISOString?.() || null,
    raw: row.raw || {},
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  });
}

function financeExpenseFromPostgres(row = {}) {
  return normalizeFinanceExpense({
    id: row.id,
    type: row.type,
    supplierName: row.supplierName,
    partnerId: row.partnerId,
    offerId: row.offerId,
    productName: row.productName,
    quantity: row.quantity,
    amount: Number(row.amount || 0),
    currency: row.currency,
    note: row.note,
    source: row.source,
    status: row.status,
    spentAt: row.spentAt?.toISOString?.() || null,
    raw: row.raw || {},
    createdAt: row.createdAt?.toISOString?.() || null,
    updatedAt: row.updatedAt?.toISOString?.() || null,
  });
}

async function readFinanceJsonFallback() {
  try {
    const parsed = JSON.parse(await fs.readFile(financeStatePath, "utf8"));
    return {
      orders: Array.isArray(parsed.orders) ? parsed.orders.map(normalizeFinanceOrder) : [],
      expenses: Array.isArray(parsed.expenses) ? parsed.expenses.map(normalizeFinanceExpense) : [],
      updatedAt: parsed.updatedAt || null,
    };
  } catch (error) {
    if (error.code === "ENOENT") return { orders: [], expenses: [], updatedAt: null };
    throw error;
  }
}

async function writeFinanceJsonFallback(state = {}) {
  const payload = {
    updatedAt: new Date().toISOString(),
    orders: Array.isArray(state.orders) ? state.orders.map(normalizeFinanceOrder) : [],
    expenses: Array.isArray(state.expenses) ? state.expenses.map(normalizeFinanceExpense) : [],
  };
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${financeStatePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(temporaryPath, financeStatePath);
  return payload;
}

function financePeriodWhere(period = "30d", field = "createdAt") {
  const normalized = cleanText(period || "30d").toLowerCase();
  if (normalized === "all") return {};
  const days = normalized === "7d" ? 7 : normalized === "90d" ? 90 : 30;
  return { [field]: { gte: new Date(Date.now() - days * 24 * 60 * 60 * 1000) } };
}

function financeSummaryFromRows(orders = [], expenses = []) {
  const orderIncome = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.payoutAmount ?? row.saleAmount, 0), 0);
  const purchaseCost = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.purchaseCost, 0), 0);
  const fees = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.feesAmount, 0), 0);
  const tax = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.taxAmount, 0), 0);
  const penalties = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.penaltiesAmount, 0), 0);
  const refunds = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.refundsAmount, 0), 0);
  const manualExpenses = expenses.reduce((sum, row) => sum + normalizeFinanceMoney(row.amount, 0), 0);
  const orderProfit = orders.reduce((sum, row) => sum + normalizeFinanceMoney(row.profitAmount ?? financeOrderProfit(row), 0), 0);
  return {
    orders: orders.length,
    expenses: expenses.length,
    orderIncome: normalizeFinanceMoney(orderIncome, 0),
    purchaseCost: normalizeFinanceMoney(purchaseCost, 0),
    fees: normalizeFinanceMoney(fees, 0),
    tax: normalizeFinanceMoney(tax, 0),
    penalties: normalizeFinanceMoney(penalties, 0),
    refunds: normalizeFinanceMoney(refunds, 0),
    manualExpenses: normalizeFinanceMoney(manualExpenses, 0),
    orderProfit: normalizeFinanceMoney(orderProfit, 0),
    netProfit: normalizeFinanceMoney(orderProfit - manualExpenses, 0),
  };
}

async function listFinanceOrders({ period = "30d", q = "", limit = 200 } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (shouldUsePostgresStorage()) {
    try {
      const where = {
        AND: [
          financePeriodWhere(period, "createdAt"),
          q ? {
            OR: [
              { orderId: { contains: q, mode: "insensitive" } },
              { postingNumber: { contains: q, mode: "insensitive" } },
              { offerId: { contains: q, mode: "insensitive" } },
              { productName: { contains: q, mode: "insensitive" } },
              { supplierName: { contains: q, mode: "insensitive" } },
            ],
          } : {},
        ].filter((item) => Object.keys(item || {}).length),
      };
      const [total, rows] = await Promise.all([
        getPrisma().financeOrder.count({ where }),
        getPrisma().financeOrder.findMany({ where, orderBy: { createdAt: "desc" }, take: normalizedLimit }),
      ]);
      return { source: "postgres", total, orders: rows.map(financeOrderFromPostgres) };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance orders postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const needle = cleanText(q).toLowerCase();
  const orders = state.orders
    .filter((row) => !needle || [row.orderId, row.postingNumber, row.offerId, row.productName, row.supplierName].join(" ").toLowerCase().includes(needle))
    .slice(0, normalizedLimit);
  return { source: "json", total: orders.length, orders };
}

async function listFinanceExpenses({ period = "30d", q = "", limit = 200 } = {}) {
  const normalizedLimit = Math.max(1, Math.min(2000, Number(limit || 200) || 200));
  if (shouldUsePostgresStorage()) {
    try {
      const where = {
        AND: [
          financePeriodWhere(period, "spentAt"),
          q ? {
            OR: [
              { offerId: { contains: q, mode: "insensitive" } },
              { productName: { contains: q, mode: "insensitive" } },
              { supplierName: { contains: q, mode: "insensitive" } },
              { note: { contains: q, mode: "insensitive" } },
            ],
          } : {},
        ].filter((item) => Object.keys(item || {}).length),
      };
      const [total, rows] = await Promise.all([
        getPrisma().financeExpense.count({ where }),
        getPrisma().financeExpense.findMany({ where, orderBy: { spentAt: "desc" }, take: normalizedLimit }),
      ]);
      return { source: "postgres", total, expenses: rows.map(financeExpenseFromPostgres) };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("finance expenses postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const state = await readFinanceJsonFallback();
  const needle = cleanText(q).toLowerCase();
  const expenses = state.expenses
    .filter((row) => !needle || [row.offerId, row.productName, row.supplierName, row.note].join(" ").toLowerCase().includes(needle))
    .slice(0, normalizedLimit);
  return { source: "json", total: expenses.length, expenses };
}

app.get("/api/finance/summary", requireAdmin, async (request, response, next) => {
  try {
    const period = cleanText(request.query.period || "30d").toLowerCase();
    const [ordersResult, expensesResult] = await Promise.all([
      listFinanceOrders({ period, limit: 2000 }),
      listFinanceExpenses({ period, limit: 2000 }),
    ]);
    response.json({
      ok: true,
      period,
      source: ordersResult.source === expensesResult.source ? ordersResult.source : "mixed",
      summary: financeSummaryFromRows(ordersResult.orders, expensesResult.expenses),
      updatedAt: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/finance/orders", requireAdmin, async (request, response, next) => {
  try {
    const result = await listFinanceOrders({
      period: cleanText(request.query.period || "30d").toLowerCase(),
      q: cleanText(request.query.q || ""),
      limit: cleanLimit(request.query.limit, 200, 2000),
    });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/finance/orders/:id", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const patch = normalizeFinanceOrder({ ...request.body, id });
    if (shouldUsePostgresStorage()) {
      try {
        const row = await getPrisma().financeOrder.upsert({
          where: { id },
          create: {
            id,
            marketplace: patch.marketplace || null,
            target: patch.target || null,
            orderId: patch.orderId,
            postingNumber: patch.postingNumber || null,
            offerId: patch.offerId || null,
            productName: patch.productName || null,
            quantity: patch.quantity,
            saleAmount: patch.saleAmount,
            payoutAmount: patch.payoutAmount,
            purchaseCost: patch.purchaseCost,
            feesAmount: patch.feesAmount,
            taxAmount: patch.taxAmount,
            penaltiesAmount: patch.penaltiesAmount,
            refundsAmount: patch.refundsAmount,
            profitAmount: financeOrderProfit(patch),
            supplierName: patch.supplierName || null,
            partnerId: patch.partnerId || null,
            source: patch.source,
            status: patch.status,
            soldAt: toDateOrNull(patch.soldAt),
            receivedAt: toDateOrNull(patch.receivedAt),
            raw: patch,
          },
          update: {
            saleAmount: patch.saleAmount,
            payoutAmount: patch.payoutAmount,
            purchaseCost: patch.purchaseCost,
            feesAmount: patch.feesAmount,
            taxAmount: patch.taxAmount,
            penaltiesAmount: patch.penaltiesAmount,
            refundsAmount: patch.refundsAmount,
            profitAmount: financeOrderProfit(patch),
            supplierName: patch.supplierName || null,
            partnerId: patch.partnerId || null,
            status: patch.status,
            soldAt: toDateOrNull(patch.soldAt),
            receivedAt: toDateOrNull(patch.receivedAt),
            raw: patch,
          },
        });
        await appendAudit(request, "finance.order.update", { entityType: "finance_order", entityId: id, newValue: patch });
        return response.json({ ok: true, order: financeOrderFromPostgres(row) });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("finance order postgres write failed, using JSON fallback", { detail: error?.message || String(error) });
      }
    }
    const state = await readFinanceJsonFallback();
    const nextOrders = [...state.orders.filter((row) => row.id !== id), patch];
    await writeFinanceJsonFallback({ ...state, orders: nextOrders });
    response.json({ ok: true, source: "json", order: patch });
  } catch (error) {
    next(error);
  }
});

app.get("/api/finance/expenses", requireAdmin, async (request, response, next) => {
  try {
    const result = await listFinanceExpenses({
      period: cleanText(request.query.period || "30d").toLowerCase(),
      q: cleanText(request.query.q || ""),
      limit: cleanLimit(request.query.limit, 200, 2000),
    });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/finance/expenses", requireAdmin, async (request, response, next) => {
  try {
    const expense = normalizeFinanceExpense(request.body || {});
    if (!(expense.amount > 0)) return response.status(400).json({ error: "Expense amount must be greater than zero.", code: "finance_amount_required" });
    if (shouldUsePostgresStorage()) {
      try {
        const row = await getPrisma().financeExpense.create({
          data: {
            id: expense.id,
            type: expense.type,
            supplierName: expense.supplierName || null,
            partnerId: expense.partnerId || null,
            offerId: expense.offerId || null,
            productName: expense.productName || null,
            quantity: expense.quantity,
            amount: expense.amount,
            currency: expense.currency,
            note: expense.note || null,
            source: expense.source,
            status: expense.status,
            spentAt: toDateOrNull(expense.spentAt) || new Date(),
            raw: expense,
          },
        });
        await appendAudit(request, "finance.expense.create", { entityType: "finance_expense", entityId: row.id, newValue: expense });
        return response.status(201).json({ ok: true, expense: financeExpenseFromPostgres(row) });
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("finance expense postgres write failed, using JSON fallback", { detail: error?.message || String(error) });
      }
    }
    const state = await readFinanceJsonFallback();
    await writeFinanceJsonFallback({ ...state, expenses: [expense, ...state.expenses] });
    response.status(201).json({ ok: true, source: "json", expense });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/prices/retry", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Retry was not sent because manual confirmation is required." });
    }
    const result = await processPriceRetryQueue({
      queueKeys: Array.isArray(request.body.queueKeys) ? request.body.queueKeys : [],
      limit: 1000,
      respectNextRetryAt: false,
      trigger: "manual",
    });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/retry-queue", requireAdmin, async (_request, response, next) => {
  try {
    const queue = await readPriceRetryQueue();
    const items = (queue.items || [])
      .map((item) => ({
        ...item,
        queueKey: priceRetryQueueKey(item),
      }))
      .sort((a, b) => new Date(b.queuedAt || 0) - new Date(a.queuedAt || 0));
    response.json({ ok: true, updatedAt: queue.updatedAt, total: items.length, items });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/prices/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 100, 500);
    const offset = Math.max(0, Number.parseInt(request.query.offset || "0", 10) || 0);
    response.json({
      ok: true,
      ...await readPriceHistory({
        productId: request.query.productId,
        offerId: request.query.offerId,
        marketplace: request.query.marketplace,
        status: request.query.status,
        dateFrom: request.query.dateFrom,
        dateTo: request.query.dateTo,
        limit,
        offset,
      }),
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/prices/retry-queue", requireAdmin, async (request, response, next) => {
  try {
    const queueKeys = new Set((Array.isArray(request.body?.queueKeys) ? request.body.queueKeys : [])
      .map((key) => String(key || "").trim())
      .filter(Boolean));
    if (!queueKeys.size) {
      await writePriceRetryQueue({ items: [] });
      return response.json({ ok: true, removed: "all" });
    }
    const queue = await readPriceRetryQueue();
    const items = (queue.items || []).filter((item) => !queueKeys.has(String(priceRetryQueueKey(item))));
    await writePriceRetryQueue({ items });
    response.json({ ok: true, removed: queueKeys.size, remaining: items.length });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon-yandex-import/preview", async (request, response, next) => {
  try {
    const requestedLimit = Number(request.query.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const refresh = String(request.query.refresh || "") === "true";
    const warehouse = await readWarehouse();
    let products = (warehouse.products || []).filter((product) => product.marketplace === "ozon");
    const warnings = [];

    if (refresh) {
      const requestedDetailLimit = Number(process.env.OZON_YANDEX_IMPORT_DETAIL_LIMIT || 1000);
      const imported = await importOzonWarehouseProducts(limit, warehouse.products || [], {
        detailRefreshLimit: Math.min(limit, Number.isFinite(requestedDetailLimit) ? requestedDetailLimit : 1000),
      });
      products = imported.imported || [];
      warnings.push(...(imported.warnings || []));
    } else {
      products = products.slice(0, limit);
    }

    const initialRows = products.map((product) => buildOzonYandexImportCandidate(product));
    const checkableOfferIds = initialRows
      .filter((row) => !row.blockReasons?.length && row.yandexReady)
      .map((row) => row.offerId)
      .map(cleanText)
      .filter(Boolean);
    const yandexExistingOfferIds = await getKnownYandexExistingOfferIds(checkableOfferIds, {
      products: warehouse.products || [],
      warnings,
      allowCatalogRefresh: refresh,
      allowDirectCheck: false,
    });

    const rows = products.map((product) => buildOzonYandexImportCandidate(product, { yandexExistingOfferIds }));
    response.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      source: refresh ? "ozon_api" : "warehouse",
      limit,
      summary: summarizeOzonYandexImportPreview(rows),
      warnings,
      rows,
    });
  } catch (error) {
    next(error);
  }
});

async function runOzonYandexImportSend(payload = {}, auditRequest = { session: { username: "system", role: "admin" } }) {
  if (payload?.confirmed !== true) {
    const error = new Error("Yandex import requires confirmed=true.");
    error.statusCode = 400;
    throw error;
  }
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const sendLimit = Math.max(1, Math.min(yandexImportSendLimit, Number(payload?.sendLimit || yandexImportSendLimit) || yandexImportSendLimit));
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) {
    const error = new Error("Yandex Market is not configured.");
    error.statusCode = 400;
    throw error;
  }

  const warehouse = await readWarehouse();
  const products = (warehouse.products || [])
    .filter((product) => product.marketplace === "ozon")
    .slice(0, limit);
  const initialRows = products.map((product) => buildOzonYandexImportCandidate(product));
  const candidateOfferIds = initialRows
    .filter((row) => !row.blockReasons?.length && row.yandexReady)
    .map((row) => cleanText(row.offerId))
    .filter(Boolean);
  const warnings = [];
  const yandexExistingOfferIds = await getKnownYandexExistingOfferIds(candidateOfferIds, {
    products: warehouse.products || [],
    warnings,
    allowCatalogRefresh: false,
    allowDirectCheck: false,
  });
  const rows = products.map((product) => buildOzonYandexImportCandidate(product, { yandexExistingOfferIds }));
  const eligibleRows = rows.filter((row) => row.eligible);
  const selectedRows = eligibleRows.slice(0, sendLimit);
  const selectedIds = new Set(selectedRows.map((row) => row.id));
  const productsById = new Map(products.map((product) => [product.id, product]));
  const selectedProducts = selectedRows.map((row) => productsById.get(row.id)).filter(Boolean);
  const offers = selectedProducts
    .map((product) => buildYandexOfferMapping(normalizeWarehouseProduct(product)).offer)
    .filter((offer) => offer?.offerId);

  const cardResults = [];
  for (const shop of shops) {
    const sent = await sendYandexOfferMappings(shop, offers);
    cardResults.push(...sent.map((item) => ({
      ...item,
      stage: "card",
      target: shop.id,
      targetName: shop.name || "Yandex Market",
    })));
  }

  const failedRows = cardResults.filter((item) => !item.ok);
  const sentCount = cardResults.filter((item) => item.ok).length;
  const skippedExisting = rows.filter((row) => row.existingInYandex).length;
  const skippedBlocked = rows.filter((row) => row.blockReasons?.length).length;
  const skippedMissing = rows.filter((row) => !row.yandexReady).length;
  const sentOfferIds = new Set(cardResults
    .filter((item) => item.ok)
    .map((item) => cleanText(item.offerId).toLowerCase())
    .filter(Boolean));
  if (sentOfferIds.size) {
    writeYandexExistingOfferIdCache(new Set([...yandexExistingOfferIds, ...sentOfferIds]))
      .catch((error) => logger.warn("write Yandex existing offer cache failed", { detail: error?.message || String(error) }));
  }

  const exportedProducts = selectedProducts
    .filter((product) => sentOfferIds.has(cleanText(product.offerId).toLowerCase()));
  const priceStage = exportedProducts.length
    ? await sendYandexPricesFromOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds, warehouse })
    : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };
  const stockStage = exportedProducts.length
    ? await sendYandexStocksForExportedOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds })
    : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };
  const stageWarnings = [
    ...warnings,
    ...(Array.isArray(priceStage.warnings) ? priceStage.warnings : []),
    ...(Array.isArray(stockStage.warnings) ? stockStage.warnings : []),
  ];
  const results = [
    ...cardResults,
    ...(Array.isArray(priceStage.results) ? priceStage.results : []),
    ...(Array.isArray(stockStage.results) ? stockStage.results : []),
  ];

  if (sentCount > 0) {
    const now = new Date().toISOString();
    const yandexProducts = [];
    const priceResultByTargetOffer = new Map((Array.isArray(priceStage.results) ? priceStage.results : [])
      .filter((item) => item.ok && item.target && item.offerId)
      .map((item) => [yandexPriceUpdateResultKey(item), item]));
    for (const product of warehouse.products || []) {
      if (!selectedIds.has(product.id) || !sentOfferIds.has(cleanText(product.offerId).toLowerCase())) continue;
      product.exports = product.exports || {};
      const baseExportState = {
        status: "sent",
        targetName: shops.map((shop) => shop.name || shop.id).join(", "),
        sentAt: now,
        priceSent: Number(priceStage.sent || 0),
        stockSent: Number(stockStage.sent || 0),
      };
      for (const shop of shops) {
        const key = yandexTargetOfferKey(shop.id, product.offerId);
        const priceResult = priceResultByTargetOffer.get(key) || null;
        const exportState = {
          ...baseExportState,
          targetName: shop.name || shop.id,
          price: Number(priceResult?.price || 0) || undefined,
          stock: pickOzonProductStockForYandex(product),
          markupCoefficient: Number(priceResult?.markupCoefficient || 0) || undefined,
          priceSource: priceResult?.priceSource || undefined,
        };
        product.exports[shop.id] = exportState;
        yandexProducts.push(buildYandexWarehouseProductFromOzonExport(product, shop, exportState));
      }
      product.exports.yandex = baseExportState;
      product.updatedAt = now;
    }
    if (yandexProducts.length) {
      warehouse.products = mergeProducts(warehouse.products || [], yandexProducts);
    }
    await writeWarehouse(warehouse);
  }

  const responsePayload = {
    ok: failedRows.length === 0 && Number(priceStage.failed || 0) === 0 && Number(stockStage.failed || 0) === 0,
    generatedAt: new Date().toISOString(),
    limit,
    sendLimit,
    targets: shops.map((shop) => ({ id: shop.id, name: shop.name, businessId: shop.businessId })),
    planned: eligibleRows.length,
    plannedNow: selectedRows.length,
    sent: sentCount,
    failed: failedRows.length,
    priceSent: Number(priceStage.sent || 0),
    priceFailed: Number(priceStage.failed || 0),
    priceSkipped: Number(priceStage.skipped || 0),
    priceSkippedNoPrice: Number(priceStage.skippedNoPrice || 0),
    stockSent: Number(stockStage.sent || 0),
    stockFailed: Number(stockStage.failed || 0),
    stockSkipped: Number(stockStage.skipped || 0),
    skippedExisting,
    skippedBlocked,
    skippedMissing,
    skippedByLimit: Math.max(0, eligibleRows.length - selectedRows.length),
    warnings: stageWarnings,
    results,
    summary: summarizeOzonYandexImportPreview(rows),
  };

  await appendAudit(auditRequest, "yandex.import.send", {
    entityType: "yandex_import",
    entityId: "ozon_to_yandex",
    limit,
    sendLimit,
    targets: responsePayload.targets,
    planned: eligibleRows.length,
    plannedNow: selectedRows.length,
    sent: sentCount,
    failed: failedRows.length,
    priceSent: responsePayload.priceSent,
    priceFailed: responsePayload.priceFailed,
    priceSkippedNoPrice: responsePayload.priceSkippedNoPrice,
    stockSent: responsePayload.stockSent,
    stockFailed: responsePayload.stockFailed,
    skippedExisting,
    skippedBlocked,
    skippedMissing,
    warnings: stageWarnings.slice(0, 100),
    failedOfferIds: results.filter((item) => !item.ok).map((item) => item.offerId).filter(Boolean).slice(0, 500),
    newValue: {
      planned: eligibleRows.length,
      plannedNow: selectedRows.length,
      sent: sentCount,
      failed: failedRows.length,
      priceSent: responsePayload.priceSent,
      priceFailed: responsePayload.priceFailed,
      priceSkippedNoPrice: responsePayload.priceSkippedNoPrice,
      stockSent: responsePayload.stockSent,
      stockFailed: responsePayload.stockFailed,
    },
  });

  return responsePayload;
}

const activeOperationJobs = new Map();

function normalizeOperationJob(input = {}) {
  const id = cleanText(input.id) || crypto.randomUUID();
  const status = ["queued", "running", "completed", "failed"].includes(input.status) ? input.status : "queued";
  return {
    id,
    type: cleanText(input.type || "unknown"),
    title: cleanText(input.title || input.type || "Operation"),
    status,
    user: cleanText(input.user || "system"),
    role: cleanText(input.role || "admin"),
    createdAt: input.createdAt || new Date().toISOString(),
    startedAt: input.startedAt || null,
    finishedAt: input.finishedAt || null,
    progress: Math.max(0, Math.min(100, Number(input.progress || 0) || 0)),
    payload: input.payload && typeof input.payload === "object" ? input.payload : {},
    result: input.result && typeof input.result === "object" ? input.result : null,
    error: cleanText(input.error || ""),
  };
}

async function readOperationJobs(limit = 100) {
  try {
    const parsed = JSON.parse(await fs.readFile(operationJobsPath, "utf8"));
    const jobs = Array.isArray(parsed.jobs) ? parsed.jobs.map(normalizeOperationJob) : [];
    return jobs
      .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))
      .slice(0, Math.max(1, Math.min(500, Number(limit || 100) || 100)));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

async function writeOperationJobs(jobs = []) {
  await fs.mkdir(dataDir, { recursive: true });
  const normalized = (Array.isArray(jobs) ? jobs : [])
    .map(normalizeOperationJob)
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))
    .slice(0, 300);
  const temporaryPath = `${operationJobsPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify({ updatedAt: new Date().toISOString(), jobs: normalized }, null, 2), "utf8");
  await fs.rename(temporaryPath, operationJobsPath);
  return normalized;
}

async function upsertOperationJob(job) {
  const normalized = normalizeOperationJob(job);
  activeOperationJobs.set(normalized.id, normalized);
  const existing = await readOperationJobs(300);
  const next = [normalized, ...existing.filter((item) => item.id !== normalized.id)];
  await writeOperationJobs(next);
  return normalized;
}

function operationJobPublic(job = {}) {
  const normalized = normalizeOperationJob(job);
  const result = normalized.result || {};
  return {
    id: normalized.id,
    type: normalized.type,
    title: normalized.title,
    status: normalized.status,
    user: normalized.user,
    createdAt: normalized.createdAt,
    startedAt: normalized.startedAt,
    finishedAt: normalized.finishedAt,
    progress: normalized.progress,
    error: normalized.error,
    summary: result.summary || null,
    result: normalized.status === "completed" || normalized.status === "failed" ? result : null,
  };
}

function normalizeSupplierCartState(input = {}) {
  const processed = input.processed && typeof input.processed === "object" && !Array.isArray(input.processed)
    ? input.processed
    : {};
  const supplierBlocks = input.supplierBlocks && typeof input.supplierBlocks === "object" && !Array.isArray(input.supplierBlocks)
    ? input.supplierBlocks
    : {};
  const draft = input.draft && typeof input.draft === "object" && !Array.isArray(input.draft)
    ? input.draft
    : null;
  const history = Array.isArray(input.history) ? input.history : [];
  return {
    updatedAt: input.updatedAt || null,
    processed,
    supplierBlocks,
    draft: draft ? {
      id: cleanText(draft.id) || crypto.randomUUID(),
      generatedAt: draft.generatedAt || null,
      generatedBy: cleanText(draft.generatedBy),
      params: draft.params && typeof draft.params === "object" ? draft.params : {},
      rows: Array.isArray(draft.rows) ? draft.rows.map(normalizeSupplierCartPreviewRow) : [],
      summary: draft.summary && typeof draft.summary === "object" ? draft.summary : {},
    } : null,
    history: history
      .filter((item) => item && typeof item === "object")
      .slice(-1000),
  };
}

function supplierCartDraftRowToPostgres(draftId, row = {}) {
  const normalized = normalizeSupplierCartPreviewRow(row);
  return {
    draftId,
    cartKey: normalized.key,
    marketplace: normalized.marketplace || null,
    accountName: normalized.accountName || null,
    orderId: normalized.orderId || null,
    postingNumber: normalized.postingNumber || null,
    offerId: normalized.offerId || null,
    productName: normalized.productName || null,
    quantity: normalized.quantity,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    offerRowId: normalized.offerRowId || null,
    price: normalized.price || null,
    priceCurrency: normalized.priceCurrency || null,
    supplierScore: normalized.supplierScore || null,
    ready: Boolean(normalized.ready),
    alreadyCommitted: Boolean(normalized.alreadyCommitted),
    skipReason: normalized.skipReason || null,
    requestDocId: normalized.requestDocId || null,
    requestRowId: normalized.requestRowId || null,
    raw: normalized,
  };
}

function supplierCartDraftRowFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeSupplierCartPreviewRow({
    ...raw,
    key: row.cartKey || raw.key,
    marketplace: row.marketplace || raw.marketplace,
    accountName: row.accountName || raw.accountName,
    orderId: row.orderId || raw.orderId,
    postingNumber: row.postingNumber || raw.postingNumber,
    offerId: row.offerId || raw.offerId,
    productName: row.productName || raw.productName,
    quantity: row.quantity ?? raw.quantity,
    supplierName: row.supplierName || raw.supplierName,
    partnerId: row.partnerId || raw.partnerId,
    offerRowId: row.offerRowId || raw.offerRowId,
    price: row.price === null || row.price === undefined ? raw.price : Number(row.price),
    priceCurrency: row.priceCurrency || raw.priceCurrency,
    supplierScore: row.supplierScore === null || row.supplierScore === undefined ? raw.supplierScore : Number(row.supplierScore),
    ready: row.ready,
    alreadyCommitted: row.alreadyCommitted,
    skipReason: row.skipReason || raw.skipReason,
    requestDocId: row.requestDocId || raw.requestDocId,
    requestRowId: row.requestRowId || raw.requestRowId,
  });
}

function supplierBlockFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return {
    ...raw,
    key: row.blockKey,
    offerId: row.offerId,
    partnerId: row.partnerId,
    supplierName: row.supplierName || raw.supplierName || "",
    reason: row.reason || raw.reason || "",
    sourceKey: row.sourceKey || raw.sourceKey || "",
    blockedBy: row.blockedBy || raw.blockedBy || "",
    blockedAt: row.blockedAt?.toISOString?.() || raw.blockedAt || "",
    expiresAt: row.expiresAt?.toISOString?.() || raw.expiresAt || "",
    active: row.active !== false,
  };
}

async function readSupplierCartState() {
  if (shouldUsePostgresStorage()) {
    try {
      const [draft, blocks] = await Promise.all([
        getPrisma().supplierCartDraft.findFirst({
          where: { active: true },
          include: { rows: { orderBy: { createdAt: "asc" } } },
          orderBy: { generatedAt: "desc" },
        }),
        getPrisma().supplierBlock.findMany({
          where: { active: true, expiresAt: { gt: new Date() } },
          orderBy: { expiresAt: "asc" },
          take: 5000,
        }),
      ]);
      const jsonState = await fs.readFile(supplierCartStatePath, "utf8")
        .then((text) => normalizeSupplierCartState(JSON.parse(text || "{}")))
        .catch(() => normalizeSupplierCartState());
      const supplierBlocks = { ...(jsonState.supplierBlocks || {}) };
      for (const block of blocks.map(supplierBlockFromPostgres)) {
        if (block.key) supplierBlocks[block.key] = block;
      }
      return normalizeSupplierCartState({
        ...jsonState,
        supplierBlocks,
        draft: draft ? {
          id: draft.id,
          generatedAt: draft.generatedAt?.toISOString?.() || null,
          generatedBy: draft.generatedBy || "",
          params: draft.params || {},
          rows: (draft.rows || []).map(supplierCartDraftRowFromPostgres),
          summary: draft.summary || {},
        } : jsonState.draft,
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read supplier cart state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    return normalizeSupplierCartState(JSON.parse(await fs.readFile(supplierCartStatePath, "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return normalizeSupplierCartState();
    throw error;
  }
}

async function writeSupplierCartState(state = {}) {
  const normalized = normalizeSupplierCartState({
    ...state,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      if (normalized.draft?.id) {
        await prisma.$transaction(async (tx) => {
          await tx.supplierCartDraft.updateMany({ where: { active: true, id: { not: normalized.draft.id } }, data: { active: false } });
          await tx.supplierCartDraft.upsert({
            where: { id: normalized.draft.id },
            create: {
              id: normalized.draft.id,
              generatedAt: toDateOrNull(normalized.draft.generatedAt) || new Date(),
              generatedBy: normalized.draft.generatedBy || null,
              marketplace: cleanText(normalized.draft.params?.marketplace || "all") || "all",
              from: toDateOrNull(normalized.draft.params?.from),
              to: toDateOrNull(normalized.draft.params?.to),
              summary: normalized.draft.summary || {},
              params: normalized.draft.params || {},
              active: true,
            },
            update: {
              generatedAt: toDateOrNull(normalized.draft.generatedAt) || new Date(),
              generatedBy: normalized.draft.generatedBy || null,
              marketplace: cleanText(normalized.draft.params?.marketplace || "all") || "all",
              from: toDateOrNull(normalized.draft.params?.from),
              to: toDateOrNull(normalized.draft.params?.to),
              summary: normalized.draft.summary || {},
              params: normalized.draft.params || {},
              active: true,
            },
          });
          await tx.supplierCartDraftRow.deleteMany({ where: { draftId: normalized.draft.id } });
          const rows = (normalized.draft.rows || []).map((row) => supplierCartDraftRowToPostgres(normalized.draft.id, row));
          for (const batch of chunkArray(rows, 500)) {
            if (batch.length) await tx.supplierCartDraftRow.createMany({ data: batch, skipDuplicates: true });
          }
        });
      }
      const blocks = Object.values(normalized.supplierBlocks || {}).filter((block) => block && typeof block === "object");
      for (const block of blocks) {
        const key = cleanText(block.key || supplierBlockKey(block.offerId, block.partnerId));
        const offerId = cleanText(block.offerId);
        const partnerId = cleanText(block.partnerId);
        const expiresAt = toDateOrNull(block.expiresAt);
        if (!key || !offerId || !partnerId || !expiresAt) continue;
        await prisma.supplierBlock.upsert({
          where: { blockKey: key },
          create: {
            blockKey: key,
            offerId,
            partnerId,
            supplierName: cleanText(block.supplierName) || null,
            reason: cleanText(block.reason) || null,
            sourceKey: cleanText(block.sourceKey) || null,
            blockedBy: cleanText(block.blockedBy) || null,
            blockedAt: toDateOrNull(block.blockedAt) || new Date(),
            expiresAt,
            active: block.active !== false,
            raw: block,
          },
          update: {
            supplierName: cleanText(block.supplierName) || null,
            reason: cleanText(block.reason) || null,
            sourceKey: cleanText(block.sourceKey) || null,
            blockedBy: cleanText(block.blockedBy) || null,
            blockedAt: toDateOrNull(block.blockedAt) || new Date(),
            expiresAt,
            active: block.active !== false,
            raw: block,
          },
        });
      }
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write supplier cart state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${supplierCartStatePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(normalized, null, 2), "utf8");
  await fs.rename(temporaryPath, supplierCartStatePath);
  return normalized;
}

function supplierBlockKey(offerId = "", partnerId = "") {
  return `${cleanText(offerId).toLowerCase()}|${cleanText(partnerId).toLowerCase()}`;
}

function activeSupplierBlocksForOffer(state = {}, offerId = "", now = new Date()) {
  const normalizedOffer = cleanText(offerId).toLowerCase();
  const blocks = state.supplierBlocks && typeof state.supplierBlocks === "object" ? state.supplierBlocks : {};
  const active = new Set();
  for (const block of Object.values(blocks)) {
    if (!block || typeof block !== "object") continue;
    if (cleanText(block.offerId).toLowerCase() !== normalizedOffer) continue;
    const expiresAt = toDateOrNull(block.expiresAt);
    if (expiresAt && expiresAt.getTime() <= now.getTime()) continue;
    const partnerId = cleanText(block.partnerId).toLowerCase();
    if (partnerId) active.add(partnerId);
  }
  return active;
}

function normalizeSupplierPickingRow(input = {}) {
  const key = cleanText(input.key || supplierCartItemKey(input));
  const status = ["picked", "missing", "reordered"].includes(cleanText(input.status).toLowerCase())
    ? cleanText(input.status).toLowerCase()
    : "open";
  return {
    key,
    marketplace: cleanText(input.marketplace).toLowerCase(),
    accountName: cleanText(input.accountName || input.account_name),
    orderId: cleanText(input.orderId || input.order_id),
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    offerId: cleanText(input.offerId || input.offer_id),
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity: Math.max(1, Math.round(Number(input.quantity || 1) || 1)),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    offerRowId: cleanText(input.offerRowId || input.rowId || input.sourceRowId),
    price: Number(input.price || 0) || 0,
    priceCurrency: cleanText(input.priceCurrency || input.currency || "USD").toUpperCase(),
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time),
    reseller: Boolean(input.reseller),
    supplierScore: Number(input.supplierScore || input.score || 0) || 0,
    requestDocId: cleanText(input.requestDocId || input.docId),
    requestRowId: cleanText(input.requestRowId || input.rowId),
    status,
    createdAt: input.createdAt || new Date().toISOString(),
    createdBy: cleanText(input.createdBy || input.committedBy),
    pickedBy: cleanText(input.pickedBy),
    pickedAt: input.pickedAt || null,
    missingBy: cleanText(input.missingBy),
    missingAt: input.missingAt || null,
    missingReason: cleanText(input.missingReason || input.reason),
    nextRetryAt: input.nextRetryAt || null,
    replacementFor: cleanText(input.replacementFor),
    replacementKey: cleanText(input.replacementKey),
  };
}

function normalizeSupplierPickingState(input = {}) {
  const rows = input.rows && typeof input.rows === "object" && !Array.isArray(input.rows) ? input.rows : {};
  const invoices = Array.isArray(input.invoices) ? input.invoices : [];
  const normalizedRows = {};
  for (const [key, row] of Object.entries(rows)) {
    const normalized = normalizeSupplierPickingRow({ ...row, key: row?.key || key });
    if (normalized.key) normalizedRows[normalized.key] = normalized;
  }
  return {
    updatedAt: input.updatedAt || null,
    rows: normalizedRows,
    invoices: invoices.filter((item) => item && typeof item === "object").slice(-1000),
  };
}

function supplierPickingRowToPostgres(row = {}) {
  const normalized = normalizeSupplierPickingRow(row);
  return {
    pickingKey: normalized.key,
    marketplace: normalized.marketplace || null,
    accountName: normalized.accountName || null,
    orderId: normalized.orderId || null,
    postingNumber: normalized.postingNumber || null,
    offerId: normalized.offerId || null,
    productName: normalized.productName || null,
    quantity: normalized.quantity,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    offerRowId: normalized.offerRowId || null,
    price: normalized.price || null,
    priceCurrency: normalized.priceCurrency || null,
    trustFactor: normalized.trustFactor,
    orderCutoffTime: normalized.orderCutoffTime || null,
    reseller: Boolean(normalized.reseller),
    supplierScore: normalized.supplierScore || null,
    requestDocId: normalized.requestDocId || null,
    requestRowId: normalized.requestRowId || null,
    status: normalized.status,
    createdBy: normalized.createdBy || null,
    pickedBy: normalized.pickedBy || null,
    pickedAt: toDateOrNull(normalized.pickedAt),
    missingBy: normalized.missingBy || null,
    missingAt: toDateOrNull(normalized.missingAt),
    missingReason: normalized.missingReason || null,
    nextRetryAt: toDateOrNull(normalized.nextRetryAt),
    replacementFor: normalized.replacementFor || null,
    replacementKey: normalized.replacementKey || null,
    raw: normalized,
  };
}

function supplierPickingRowFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeSupplierPickingRow({
    ...raw,
    key: row.pickingKey || raw.key,
    marketplace: row.marketplace || raw.marketplace,
    accountName: row.accountName || raw.accountName,
    orderId: row.orderId || raw.orderId,
    postingNumber: row.postingNumber || raw.postingNumber,
    offerId: row.offerId || raw.offerId,
    productName: row.productName || raw.productName,
    quantity: row.quantity ?? raw.quantity,
    supplierName: row.supplierName || raw.supplierName,
    partnerId: row.partnerId || raw.partnerId,
    offerRowId: row.offerRowId || raw.offerRowId,
    price: row.price === null || row.price === undefined ? raw.price : Number(row.price),
    priceCurrency: row.priceCurrency || raw.priceCurrency,
    trustFactor: row.trustFactor ?? raw.trustFactor,
    orderCutoffTime: row.orderCutoffTime || raw.orderCutoffTime,
    reseller: row.reseller,
    supplierScore: row.supplierScore === null || row.supplierScore === undefined ? raw.supplierScore : Number(row.supplierScore),
    requestDocId: row.requestDocId || raw.requestDocId,
    requestRowId: row.requestRowId || raw.requestRowId,
    status: row.status || raw.status,
    createdAt: row.createdAt?.toISOString?.() || raw.createdAt,
    createdBy: row.createdBy || raw.createdBy,
    pickedBy: row.pickedBy || raw.pickedBy,
    pickedAt: row.pickedAt?.toISOString?.() || raw.pickedAt,
    missingBy: row.missingBy || raw.missingBy,
    missingAt: row.missingAt?.toISOString?.() || raw.missingAt,
    missingReason: row.missingReason || raw.missingReason,
    nextRetryAt: row.nextRetryAt?.toISOString?.() || raw.nextRetryAt,
    replacementFor: row.replacementFor || raw.replacementFor,
    replacementKey: row.replacementKey || raw.replacementKey,
  });
}

async function readSupplierPickingState() {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().supplierPickingRow.findMany({
        orderBy: [{ createdAt: "desc" }],
        take: 10000,
      });
      const normalizedRows = {};
      for (const row of rows.map(supplierPickingRowFromPostgres)) {
        if (row.key) normalizedRows[row.key] = row;
      }
      return normalizeSupplierPickingState({
        updatedAt: rows[0]?.updatedAt?.toISOString?.() || null,
        rows: normalizedRows,
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read supplier picking state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    return normalizeSupplierPickingState(JSON.parse(await fs.readFile(supplierPickingListPath, "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return normalizeSupplierPickingState();
    throw error;
  }
}

async function writeSupplierPickingState(state = {}) {
  const normalized = normalizeSupplierPickingState({
    ...state,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = Object.values(normalized.rows || {}).map(supplierPickingRowToPostgres).filter((row) => row.pickingKey);
      for (const batch of chunkArray(rows, 250)) {
        await prisma.$transaction(batch.map((row) => prisma.supplierPickingRow.upsert({
          where: { pickingKey: row.pickingKey },
          create: row,
          update: {
            marketplace: row.marketplace,
            accountName: row.accountName,
            orderId: row.orderId,
            postingNumber: row.postingNumber,
            offerId: row.offerId,
            productName: row.productName,
            quantity: row.quantity,
            supplierName: row.supplierName,
            partnerId: row.partnerId,
            offerRowId: row.offerRowId,
            price: row.price,
            priceCurrency: row.priceCurrency,
            trustFactor: row.trustFactor,
            orderCutoffTime: row.orderCutoffTime,
            reseller: row.reseller,
            supplierScore: row.supplierScore,
            requestDocId: row.requestDocId,
            requestRowId: row.requestRowId,
            status: row.status,
            createdBy: row.createdBy,
            pickedBy: row.pickedBy,
            pickedAt: row.pickedAt,
            missingBy: row.missingBy,
            missingAt: row.missingAt,
            missingReason: row.missingReason,
            nextRetryAt: row.nextRetryAt,
            replacementFor: row.replacementFor,
            replacementKey: row.replacementKey,
            raw: row.raw,
          },
        })));
      }
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write supplier picking state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${supplierPickingListPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(normalized, null, 2), "utf8");
  await fs.rename(temporaryPath, supplierPickingListPath);
  return normalized;
}

async function createSupplierPickingRows(inserted = [], request = null) {
  const rows = inserted.map(normalizeSupplierCartPreviewRow).filter((row) => row.key);
  if (!rows.length) return [];
  const state = await readSupplierPickingState();
  const created = [];
  for (const row of rows) {
    const existing = state.rows[row.key] ? normalizeSupplierPickingRow(state.rows[row.key]) : null;
    if (existing && existing.status !== "missing") continue;
    const pickingKey = existing?.status === "missing"
      ? `${row.key}|retry:${row.requestRowId || Date.now()}`
      : row.key;
    if (state.rows[pickingKey]) continue;
    const pickingRow = normalizeSupplierPickingRow({
      ...row,
      key: pickingKey,
      status: "open",
      createdAt: row.committedAt || new Date().toISOString(),
      createdBy: requestUsername(request),
      replacementFor: existing?.status === "missing" ? existing.key : "",
    });
    state.rows[pickingRow.key] = pickingRow;
    if (existing?.status === "missing") {
      state.rows[existing.key] = normalizeSupplierPickingRow({
        ...existing,
        status: "reordered",
        replacementKey: pickingRow.key,
      });
    }
    created.push(pickingRow);
  }
  if (created.length) await writeSupplierPickingState(state);
  for (const row of created) {
    await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_picking.created", {
      entityType: "supplier_picking",
      entityId: row.key,
      newValue: row,
    }).catch((error) => logger.warn("supplier picking audit failed", { detail: error?.message || String(error) }));
  }
  return created;
}

function supplierPickingInvoiceRows(state = {}, period = "30d") {
  const now = new Date();
  const normalizedPeriod = cleanText(period || "30d").toLowerCase();
  const from = normalizedPeriod === "all"
    ? null
    : new Date(now.getTime() - Math.max(1, Number.parseInt(normalizedPeriod, 10) || 30) * 24 * 60 * 60 * 1000);
  return Object.values(state.rows || {})
    .map(normalizeSupplierPickingRow)
    .filter((row) => row.status === "picked")
    .filter((row) => {
      if (!from) return true;
      const pickedAt = toDateOrNull(row.pickedAt || row.createdAt);
      return pickedAt && pickedAt.getTime() >= from.getTime();
    })
    .sort((left, right) => String(right.pickedAt || right.createdAt).localeCompare(String(left.pickedAt || left.createdAt)));
}

function supplierCartRange(input = {}, settings = defaultAppSettings().supplierCart) {
  const now = new Date();
  const to = toDateOrNull(input.to) || now;
  const from = toDateOrNull(input.from) || new Date(to.getTime() - Math.max(1, Number(settings.lookbackHours || 48)) * 60 * 60 * 1000);
  return { from, to };
}

function supplierCartItemKey(line = {}) {
  return [
    cleanText(line.marketplace).toLowerCase(),
    cleanText(line.accountId || line.campaignId || line.target).toLowerCase(),
    cleanText(line.orderId || line.postingNumber || line.externalOrderId).toLowerCase(),
    cleanText(line.itemId || line.offerId || line.sku).toLowerCase(),
  ].join("|");
}

function normalizeSupplierCartLine(input = {}) {
  const marketplace = cleanText(input.marketplace).toLowerCase();
  const offerId = cleanText(input.offerId || input.offer_id || input.sku);
  const quantity = Math.max(1, Math.round(Number(input.quantity || input.count || 1) || 1));
  const line = {
    key: cleanText(input.key),
    marketplace,
    accountId: cleanText(input.accountId || input.account_id || input.target || input.campaignId),
    accountName: cleanText(input.accountName || input.account_name),
    campaignId: cleanText(input.campaignId || input.campaign_id),
    orderId: cleanText(input.orderId || input.order_id || input.postingNumber || input.posting_number),
    postingNumber: cleanText(input.postingNumber || input.posting_number),
    externalOrderId: cleanText(input.externalOrderId || input.external_order_id),
    itemId: cleanText(input.itemId || input.item_id || input.id || offerId),
    offerId,
    productName: cleanText(input.productName || input.product_name || input.name),
    quantity,
    orderedAt: input.orderedAt || input.createdAt || input.in_process_at || null,
    status: cleanText(input.status),
    raw: input.raw && typeof input.raw === "object" ? input.raw : undefined,
  };
  line.key = line.key || supplierCartItemKey(line);
  return line;
}

function normalizeSupplierCartPreviewRow(input = {}) {
  const line = normalizeSupplierCartLine(input);
  return {
    ...line,
    warehouseProductId: cleanText(input.warehouseProductId || input.productId),
    groupKey: cleanText(input.groupKey),
    groupOfferId: cleanText(input.groupOfferId || input.group_offer_id || line.offerId),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    offerRowId: cleanText(input.offerRowId || input.rowId || input.sourceRowId),
    price: Number(input.price || 0) || 0,
    originalPrice: Number(input.originalPrice || 0) || 0,
    priceCurrency: cleanText(input.priceCurrency || input.currency || "USD").toUpperCase(),
    trustFactor: normalizeSupplierTrustFactor(input.trustFactor, 100),
    orderCutoffTime: normalizeSupplierOrderCutoff(input.orderCutoffTime || input.order_cutoff_time),
    reseller: Boolean(input.reseller),
    supplierScore: Number(input.supplierScore || input.score || 0) || 0,
    available: input.available !== false,
    ready: Boolean(input.ready),
    alreadyCommitted: Boolean(input.alreadyCommitted),
    skipReason: cleanText(input.skipReason || input.reason),
    requestDocId: cleanText(input.requestDocId || input.docId),
    requestRowId: cleanText(input.requestRowId || input.rowId),
  };
}

function normalizeOzonSupplierCartPostings(data = {}, account = {}) {
  const postings = Array.isArray(data?.result?.postings)
    ? data.result.postings
    : (Array.isArray(data?.postings) ? data.postings : []);
  const lines = [];
  for (const posting of postings) {
    const products = Array.isArray(posting.products) ? posting.products : [];
    for (const product of products) {
      const line = normalizeSupplierCartLine({
        marketplace: "ozon",
        accountId: account.id || account.clientId || "ozon",
        accountName: account.name || "Ozon",
        orderId: posting.order_id || posting.orderId || posting.posting_number,
        postingNumber: posting.posting_number,
        externalOrderId: posting.posting_number,
        itemId: product.sku || product.offer_id || product.offerId,
        offerId: product.offer_id || product.offerId,
        productName: product.name,
        quantity: product.quantity,
        orderedAt: posting.in_process_at || posting.created_at,
        status: posting.status,
        raw: { postingNumber: posting.posting_number, product },
      });
      if (line.offerId) lines.push(line);
    }
  }
  return lines;
}

function normalizeYandexSupplierCartOrders(data = {}, shop = {}) {
  const orders = Array.isArray(data?.orders)
    ? data.orders
    : (Array.isArray(data?.result?.orders) ? data.result.orders : []);
  const lines = [];
  for (const order of orders) {
    const items = Array.isArray(order.items) ? order.items : [];
    for (const item of items) {
      const itemStatus = cleanText(item.itemStatus || item.status).toUpperCase();
      if (itemStatus === "REJECTED" || itemStatus === "RETURNED") continue;
      const line = normalizeSupplierCartLine({
        marketplace: "yandex",
        accountId: shop.id || shop.campaignId || "yandex",
        accountName: shop.name || "Yandex Market",
        campaignId: order.campaignId || shop.campaignId,
        orderId: order.id || order.orderId,
        externalOrderId: order.externalOrderId,
        itemId: item.id || item.offerId,
        offerId: item.offerId,
        productName: item.offerName || item.name,
        quantity: item.count,
        orderedAt: order.creationDate || order.creationDateTime || order.updateDate,
        status: order.status,
        raw: { orderId: order.id, item },
      });
      if (line.offerId) lines.push(line);
    }
  }
  return lines;
}

async function fetchOzonSupplierCartLines({ from, to, limit, statuses } = {}) {
  const accounts = getOzonAccounts();
  const lines = [];
  const statusList = (Array.isArray(statuses) && statuses.length ? statuses : ["awaiting_packaging"])
    .map((item) => cleanText(item).toLowerCase())
    .filter(Boolean);
  for (const account of accounts) {
    for (const status of statusList) {
      let offset = 0;
      while (lines.length < limit) {
        const pageLimit = Math.min(1000, Math.max(1, limit - lines.length));
        const data = await ozonRequest("/v3/posting/fbs/list", {
          dir: "ASC",
          filter: {
            since: from.toISOString(),
            to: to.toISOString(),
            status,
          },
          limit: pageLimit,
          offset,
          with: { analytics_data: false, financial_data: false },
        }, account);
        const pageLines = normalizeOzonSupplierCartPostings(data, account);
        lines.push(...pageLines);
        const postings = Array.isArray(data?.result?.postings) ? data.result.postings : [];
        if (postings.length < pageLimit) break;
        offset += postings.length;
      }
    }
  }
  return lines.slice(0, limit);
}

async function fetchYandexSupplierCartLines({ from, to, limit, statuses, substatuses } = {}) {
  const shops = uniqueYandexShopsByBusiness();
  const lines = [];
  const statusList = (Array.isArray(statuses) && statuses.length ? statuses : ["PROCESSING"])
    .map((item) => cleanText(item).toUpperCase())
    .filter(Boolean);
  const substatusList = (Array.isArray(substatuses) && substatuses.length ? substatuses : ["STARTED"])
    .map((item) => cleanText(item).toUpperCase())
    .filter(Boolean);
  for (const shop of shops) {
    let pageToken = "";
    while (lines.length < limit) {
      const campaignIds = parseYandexCampaignIds(shop.campaignId).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0);
      const query = new URLSearchParams({ limit: String(Math.min(50, Math.max(1, limit - lines.length))) });
      if (pageToken) query.set("pageToken", pageToken);
      const data = await yandexRequest(shop, "POST", `/v1/businesses/${shop.businessId}/orders?${query.toString()}`, {
        ...(campaignIds.length ? { campaignIds } : {}),
        statuses: statusList,
        substatuses: substatusList,
        dates: {
          updateDateFrom: from.toISOString(),
          updateDateTo: to.toISOString(),
        },
        fake: false,
        sourcePlatforms: ["MARKET"],
      });
      lines.push(...normalizeYandexSupplierCartOrders(data, shop));
      pageToken = cleanText(data?.paging?.nextPageToken || data?.result?.paging?.nextPageToken || data?.nextPageToken);
      if (!pageToken) break;
    }
  }
  return lines.slice(0, limit);
}

function findSupplierCartWarehouseProduct(warehouse = {}, line = {}) {
  const offer = cleanText(line.offerId).toLowerCase();
  if (!offer) return null;
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const candidates = products.filter((product) => cleanText(product.offerId).toLowerCase() === offer);
  if (!candidates.length) return null;
  const marketplace = cleanText(line.marketplace).toLowerCase();
  const target = cleanText(line.accountId || line.campaignId).toLowerCase();
  return candidates.find((product) => {
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== marketplace) return false;
    if (!target) return true;
    if (marketplace === "ozon") return matchesOzonTarget(product.target, target) || cleanText(product.target).toLowerCase() === target;
    return matchesYandexTarget(product.target, target) || cleanText(product.target).toLowerCase() === target;
  }) || candidates.find((product) => normalizeWarehouseProduct(product).marketplace === marketplace) || candidates[0];
}

async function resolveSupplierCartRow(warehouse = {}, line = {}, state = {}) {
  const normalizedLine = normalizeSupplierCartLine(line);
  const processed = state.processed?.[normalizedLine.key];
  const product = findSupplierCartWarehouseProduct(warehouse, normalizedLine);
  if (!product) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      ready: false,
      skipReason: "product_not_found",
      alreadyCommitted: Boolean(processed),
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  const groupProducts = expandWarehouseProductsToGroups(warehouse.products || [], [product]);
  const groupLinks = buildCommonWarehouseGroupLinks(groupProducts, []);
  if (!groupLinks.length) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      ready: false,
      skipReason: "no_pricemaster_link",
      alreadyCommitted: Boolean(processed),
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  const usdRate = await getUsdRate();
  const matches = await getLivePriceMasterMatchesForLinks(groupLinks, warehouse.suppliers || [], usdRate);
  const candidates = [];
  const blockedPartnerIds = activeSupplierBlocksForOffer(state, normalizedLine.offerId);
  let blockedAvailable = 0;
  let cutoffPassedAvailable = 0;
  for (const [linkId, rows] of matches.entries()) {
    for (const row of rows || []) {
      if (!row.available || !row.active || Number(row.price || 0) <= 0) continue;
      if (blockedPartnerIds.has(cleanText(row.partnerId).toLowerCase())) {
        blockedAvailable += 1;
        continue;
      }
      if (supplierOrderCutoffPassed(row.orderCutoffTime)) {
        cutoffPassedAvailable += 1;
        continue;
      }
      candidates.push({ ...row, linkId });
    }
  }
  candidates.sort((left, right) =>
    supplierCartOrderScore(left) - supplierCartOrderScore(right)
    || Number(left.price || Number.POSITIVE_INFINITY) - Number(right.price || Number.POSITIVE_INFINITY)
    || normalizeSupplierTrustFactor(right.trustFactor, 100) - normalizeSupplierTrustFactor(left.trustFactor, 100)
    || String(left.partnerName || "").localeCompare(String(right.partnerName || ""), "ru", { sensitivity: "base" }),
  );
  const selected = candidates[0] || null;
  if (!selected) {
    return normalizeSupplierCartPreviewRow({
      ...normalizedLine,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      ready: false,
      skipReason: blockedAvailable
        ? "supplier_blocked_no_alternative"
        : (cutoffPassedAvailable ? "supplier_cutoff_passed_no_alternative" : "supplier_not_available"),
      alreadyCommitted: Boolean(processed),
      requestDocId: processed?.requestDocId,
      requestRowId: processed?.requestRowId,
    });
  }
  return normalizeSupplierCartPreviewRow({
    ...normalizedLine,
    warehouseProductId: product.id,
    groupKey: warehouseProductPageGroupKey(product),
    groupOfferId: product.offerId,
    supplierName: selected.partnerName,
    partnerId: selected.partnerId,
    offerRowId: selected.rowId,
    price: selected.price,
    originalPrice: selected.originalPrice,
    priceCurrency: selected.priceCurrency,
    trustFactor: selected.trustFactor,
    orderCutoffTime: selected.orderCutoffTime,
    reseller: selected.reseller,
    supplierScore: supplierCartOrderScore(selected),
    available: true,
    ready: true,
    alreadyCommitted: Boolean(processed),
    requestDocId: processed?.requestDocId,
    requestRowId: processed?.requestRowId,
  });
}

async function buildSupplierCartPreview(params = {}) {
  const appSettings = await readAppSettings();
  const settings = normalizeSupplierCartSettings(appSettings.supplierCart || {});
  const marketplace = cleanText(params.marketplace || "all").toLowerCase();
  const limit = Math.max(1, Math.min(1000, Number(params.limit || 100) || 100));
  const range = supplierCartRange(params, settings);
  const enabledMarketplaces = new Set(settings.marketplaces || ["ozon", "yandex"]);
  const lines = [];
  const warnings = [];
  if ((marketplace === "all" || marketplace === "ozon") && enabledMarketplaces.has("ozon")) {
    try {
      lines.push(...await fetchOzonSupplierCartLines({
        ...range,
        limit: Math.max(1, limit - lines.length),
        statuses: settings.includeOzonStatuses,
      }));
    } catch (error) {
      warnings.push({ marketplace: "ozon", error: error?.message || String(error) });
    }
  }
  if ((marketplace === "all" || marketplace === "yandex") && enabledMarketplaces.has("yandex") && lines.length < limit) {
    try {
      lines.push(...await fetchYandexSupplierCartLines({
        ...range,
        limit: Math.max(1, limit - lines.length),
        statuses: settings.includeYandexStatuses,
        substatuses: settings.includeYandexSubstatuses,
      }));
    } catch (error) {
      warnings.push({ marketplace: "yandex", error: error?.message || String(error) });
    }
  }
  const uniqueLines = Array.from(new Map(lines.map((line) => [line.key, line])).values()).slice(0, limit);
  const warehouse = await readWarehouse();
  const state = await readSupplierCartState();
  const rows = [];
  for (const line of uniqueLines) {
    try {
      rows.push(await resolveSupplierCartRow(warehouse, line, state));
    } catch (error) {
      rows.push(normalizeSupplierCartPreviewRow({
        ...line,
        ready: false,
        skipReason: `pricemaster_error: ${error?.message || String(error)}`,
        alreadyCommitted: Boolean(state.processed?.[line.key]),
        requestDocId: state.processed?.[line.key]?.requestDocId,
        requestRowId: state.processed?.[line.key]?.requestRowId,
      }));
    }
  }
  const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
  const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
  const skipped = rows.length - ready - alreadyCommitted;
  return {
    ok: true,
    draftId: cleanText(params.draftId),
    mode: settings.mode,
    from: range.from.toISOString(),
    to: range.to.toISOString(),
    limit,
    rows,
    warnings,
    total: rows.length,
    ready,
    skipped,
    alreadyCommitted,
    summary: `Supplier cart preview: ${rows.length} rows; ready ${ready}; already ${alreadyCommitted}; skipped ${skipped}.`,
  };
}

async function generateSupplierCartDraft(params = {}, request = null) {
  const preview = await buildSupplierCartPreview(params);
  const state = await readSupplierCartState();
  const draft = {
    id: crypto.randomUUID(),
    generatedAt: new Date().toISOString(),
    generatedBy: requestUsername(request),
    params: {
      marketplace: cleanText(params.marketplace || "all"),
      from: params.from || preview.from,
      to: params.to || preview.to,
      limit: preview.limit,
    },
    rows: preview.rows || [],
    summary: {
      total: preview.total,
      ready: preview.ready,
      skipped: preview.skipped,
      alreadyCommitted: preview.alreadyCommitted,
      warnings: preview.warnings || [],
    },
  };
  state.draft = draft;
  await writeSupplierCartState(state);
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.draft_generate", {
    entityType: "supplier_cart",
    entityId: draft.id,
    newValue: draft.summary,
  }).catch((error) => logger.warn("supplier cart draft audit failed", { detail: error?.message || String(error) }));
  return { ...preview, draftId: draft.id, generatedAt: draft.generatedAt, generatedBy: draft.generatedBy };
}

function supplierCartAutomationPublic() {
  return {
    autoRunning: supplierCartAutoRunning,
    lastAutoRunAt: supplierCartAutoLastRunAt,
    nextAutoRunAt: supplierCartAutoNextRunAt,
    lastAutoResult: supplierCartAutoLastResult,
  };
}

function nextMoscowScheduleTimeIso(times = ["09:30", "12:00", "15:00"], now = new Date()) {
  const schedule = (Array.isArray(times) ? times : [])
    .map(normalizeSupplierOrderCutoff)
    .filter(Boolean)
    .sort();
  const source = schedule.length ? schedule : ["09:30", "12:00", "15:00"];
  const moscowNow = new Date(now.getTime() + 3 * 60 * 60 * 1000);
  for (let day = 0; day < 8; day += 1) {
    const base = new Date(Date.UTC(moscowNow.getUTCFullYear(), moscowNow.getUTCMonth(), moscowNow.getUTCDate() + day, 0, 0, 0, 0));
    for (const time of source) {
      const [hours, minutes] = time.split(":").map((item) => Number(item) || 0);
      const moscowCandidate = new Date(base);
      moscowCandidate.setUTCHours(hours, minutes, 0, 0);
      const utcCandidate = new Date(moscowCandidate.getTime() - 3 * 60 * 60 * 1000);
      if (utcCandidate.getTime() > now.getTime() + 30_000) return utcCandidate.toISOString();
    }
  }
  return new Date(now.getTime() + 30 * 60 * 1000).toISOString();
}

async function processSupplierCartAutoGenerate({ source = "scheduler" } = {}) {
  if (supplierCartAutoRunning) return { ok: true, skipped: true, reason: "already_running", ...supplierCartAutomationPublic() };
  supplierCartAutoRunning = true;
  supplierCartAutoLastRunAt = new Date().toISOString();
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    if (settings.enabled === false || settings.autoEnabled === false) {
      supplierCartAutoLastResult = { ok: true, skipped: true, reason: "disabled", at: new Date().toISOString() };
      return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
    }
    const result = await generateSupplierCartDraft({ marketplace: "all", limit: Number(process.env.SUPPLIER_CART_AUTO_LIMIT || 300) || 300 }, { session: { username: "system", role: "admin" } });
    supplierCartAutoLastResult = {
      ok: true,
      source,
      draftId: result.draftId,
      total: result.total,
      ready: result.ready,
      skipped: result.skipped,
      alreadyCommitted: result.alreadyCommitted,
      at: new Date().toISOString(),
    };
    logger.info("supplier cart auto draft generated", supplierCartAutoLastResult);
    return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
  } catch (error) {
    supplierCartAutoLastResult = { ok: false, source, error: error?.message || String(error), at: new Date().toISOString() };
    logger.warn("supplier cart auto draft failed", { detail: error?.message || String(error) });
    return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
  } finally {
    supplierCartAutoRunning = false;
  }
}

async function scheduleSupplierCartAuto(delayMs = null) {
  if (supplierCartAutoTimer) clearTimeout(supplierCartAutoTimer);
  const settings = normalizeSupplierCartSettings((await readAppSettings().catch(() => defaultAppSettings())).supplierCart || {});
  if (settings.enabled === false || settings.autoEnabled === false) {
    supplierCartAutoNextRunAt = null;
    return;
  }
  const nextIso = delayMs === null
    ? nextMoscowScheduleTimeIso(settings.scheduleTimes)
    : new Date(Date.now() + Math.max(30_000, Number(delayMs) || 30_000)).toISOString();
  supplierCartAutoNextRunAt = nextIso;
  supplierCartAutoTimer = setTimeout(async () => {
    supplierCartAutoTimer = null;
    await processSupplierCartAutoGenerate({ source: "scheduler" });
    await scheduleSupplierCartAuto();
  }, Math.max(30_000, new Date(nextIso).getTime() - Date.now()));
}

async function insertSupplierCartRowsIntoPriceMaster(rows = [], request = null) {
  const readyRows = rows.map(normalizeSupplierCartPreviewRow).filter((row) => row.ready && !row.alreadyCommitted && row.offerRowId && row.partnerId);
  if (!readyRows.length) return { inserted: [], skipped: rows.length, docIds: [] };
  const state = await readSupplierCartState();
  const freshRows = readyRows.filter((row) => !state.processed?.[row.key]);
  if (!freshRows.length) return { inserted: [], skipped: readyRows.length, docIds: [] };
  const byPartner = new Map();
  for (const row of freshRows) {
    const partnerId = cleanText(row.partnerId);
    if (!byPartner.has(partnerId)) byPartner.set(partnerId, []);
    byPartner.get(partnerId).push(row);
  }
  const connection = await pool.getConnection();
  const inserted = [];
  const docIds = [];
  let lockAcquired = false;
  try {
    const [lockRows] = await connection.query("SELECT GET_LOCK('davidsklad_supplier_cart', 10) AS locked");
    lockAcquired = Number(lockRows?.[0]?.locked || 0) === 1;
    if (!lockAcquired) {
      const error = new Error("PriceMaster cart is busy. Try again in a few seconds.");
      error.statusCode = 409;
      throw error;
    }
    await connection.beginTransaction();
    const [[docMax]] = await connection.query("SELECT COALESCE(MAX(DocID), 0) AS maxDocId FROM RequestDocs");
    const [[rowMax]] = await connection.query("SELECT COALESCE(MAX(RowID), 0) AS maxRowId FROM RequestRows");
    let nextDocId = Number(docMax?.maxDocId || 0) + 1;
    let nextRowId = Number(rowMax?.maxRowId || 0) + 1;
    for (const [partnerId, partnerRows] of byPartner.entries()) {
      const docId = nextDocId++;
      const comment = `ДавидСклад автокорзина ${new Date().toLocaleString("ru-RU")}`;
      await connection.query(
        "INSERT INTO RequestDocs (DocID, DocDate, PartnerID, Sended, Recieved, Comment, Registered) VALUES (?, NOW(), ?, 0, 0, ?, 1)",
        [docId, Number(partnerId), comment],
      );
      docIds.push(docId);
      for (const row of partnerRows) {
        const requestRowId = nextRowId++;
        const rowComment = [
          "ДавидСклад",
          row.marketplace,
          row.orderId || row.postingNumber,
          row.offerId,
        ].filter(Boolean).join(" · ").slice(0, 250);
        await connection.query(
          "INSERT INTO RequestRows (RowID, OfferRowID, RequestQuant, RequestPrice, RequestComment, DocID) VALUES (?, ?, ?, 0.00, ?, ?)",
          [requestRowId, Number(row.offerRowId), Math.max(1, Math.round(Number(row.quantity || 1))), rowComment, docId],
        );
        inserted.push({
          ...row,
          requestDocId: String(docId),
          requestRowId: String(requestRowId),
          committedAt: new Date().toISOString(),
        });
      }
    }
    await connection.commit();
  } catch (error) {
    try { await connection.rollback(); } catch (_rollbackError) {}
    throw error;
  } finally {
    if (lockAcquired) {
      try { await connection.query("SELECT RELEASE_LOCK('davidsklad_supplier_cart')"); } catch (_releaseError) {}
    }
    connection.release();
  }

  const nextState = await readSupplierCartState();
  for (const row of inserted) {
    nextState.processed[row.key] = {
      key: row.key,
      marketplace: row.marketplace,
      orderId: row.orderId,
      postingNumber: row.postingNumber,
      offerId: row.offerId,
      quantity: row.quantity,
      supplierName: row.supplierName,
      partnerId: row.partnerId,
      offerRowId: row.offerRowId,
      trustFactor: row.trustFactor,
      orderCutoffTime: row.orderCutoffTime,
      reseller: row.reseller,
      supplierScore: row.supplierScore,
      requestDocId: row.requestDocId,
      requestRowId: row.requestRowId,
      committedAt: row.committedAt,
      committedBy: requestUsername(request),
    };
  }
  if (nextState.draft?.rows?.length) {
    const processedByKey = nextState.processed || {};
    nextState.draft.rows = nextState.draft.rows.map((row) => {
      const normalized = normalizeSupplierCartPreviewRow(row);
      const processed = processedByKey[normalized.key];
      return processed
        ? normalizeSupplierCartPreviewRow({
            ...normalized,
            alreadyCommitted: true,
            requestDocId: processed.requestDocId || normalized.requestDocId,
            requestRowId: processed.requestRowId || normalized.requestRowId,
          })
        : normalized;
    });
    const rows = nextState.draft.rows;
    nextState.draft.summary = {
      ...(nextState.draft.summary || {}),
      total: rows.length,
      ready: rows.filter((row) => row.ready && !row.alreadyCommitted).length,
      alreadyCommitted: rows.filter((row) => row.alreadyCommitted).length,
      skipped: rows.filter((row) => !row.ready && !row.alreadyCommitted).length,
    };
  }
  nextState.history = [
    ...(nextState.history || []),
    {
      at: new Date().toISOString(),
      user: requestUsername(request),
      inserted: inserted.length,
      docIds,
      rows: inserted.map((row) => ({
        key: row.key,
        marketplace: row.marketplace,
        orderId: row.orderId,
        offerId: row.offerId,
        quantity: row.quantity,
        supplierName: row.supplierName,
        partnerId: row.partnerId,
        trustFactor: row.trustFactor,
        orderCutoffTime: row.orderCutoffTime,
        reseller: row.reseller,
        requestDocId: row.requestDocId,
        requestRowId: row.requestRowId,
      })),
    },
  ].slice(-1000);
  await writeSupplierCartState(nextState);
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.commit", {
    entityType: "supplier_cart",
    entityId: "pricemaster",
    newValue: { inserted: inserted.length, docIds, rows: inserted },
  });
  const pickingCreated = await createSupplierPickingRows(inserted, request);
  return { inserted, skipped: readyRows.length - inserted.length, docIds, pickingCreated };
}

async function runSupplierCartPreviewOperation(payload = {}) {
  const result = await buildSupplierCartPreview(payload);
  return { ...result, ok: result.warnings.length === 0 || result.rows.length > 0 };
}

async function runSupplierCartCommitOperation(payload = {}, request = null) {
  const sourceRows = Array.isArray(payload.rows) && payload.rows.length
    ? payload.rows
    : (await buildSupplierCartPreview(payload)).rows;
  const keys = new Set(Array.isArray(payload.keys) ? payload.keys.map(cleanText).filter(Boolean) : []);
  const rows = keys.size ? sourceRows.filter((row) => keys.has(cleanText(row.key))) : sourceRows;
  const result = await insertSupplierCartRowsIntoPriceMaster(rows, request);
  return {
    ok: true,
    inserted: result.inserted.length,
    skipped: result.skipped,
    docIds: result.docIds,
    pickingCreated: result.pickingCreated?.length || 0,
    rows: result.inserted,
    summary: `Supplier cart committed ${result.inserted.length}; skipped ${result.skipped}.`,
  };
}

function operationTitle(type = "") {
  const titles = {
    "yandex-import-send": "Ozon -> Yandex import",
    "yandex-stock-sync": "Ozon -> Yandex stock sync",
    "yandex-price-push": "Send new Yandex prices",
    "linked-supplier-recovery": "Restore linked marketplace cards",
    "ozon-linked-unarchive": "Restore linked Ozon autoarchive",
    "restore-archived-stock": "Restore archived stock",
    "yandex-card-quality-ai-drafts": "Yandex card quality AI drafts",
    "repair-pricemaster-group-links": "Repair PriceMaster group links",
    "marketplace-supplier-cart-preview": "Supplier cart preview",
    "marketplace-supplier-cart-commit": "Supplier cart commit",
    "ozon-unarchive-queue-process": "Process Ozon autoarchive queue",
    "sales-automation-run": "Run sales automation",
    "problem-products-repair": "Repair problem products",
    "brand-index-rebuild": "Rebuild brand index",
    "health-deep": "Deep health check",
  };
  return titles[type] || type || "Operation";
}

async function runSalesAutomationOperation(payload = {}) {
  const result = await sendWarehousePrices({
    marketplace: payload.marketplace || "all",
    force: payload.force === true,
    onlyChanged: payload.onlyChanged !== false,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
    limit: cleanLimit(payload.limit, 1000, 50000),
    reason: payload.reason || "sales_automation_operation",
  });
  return {
    ok: result.ok !== false,
    ...result,
    summary: `Sales automation processed ${result.selected || 0}; sent ${result.sent || result.readyToSend || 0}; skipped ${Array.isArray(result.skipped) ? result.skipped.length : 0}.`,
  };
}

async function runProblemProductsRepairOperation(payload = {}, request = null, options = {}) {
  const productIds = Array.isArray(payload.productIds) ? payload.productIds.map(cleanText).filter(Boolean).slice(0, 100) : [];
  const results = [];
  let processed = 0;
  for (const id of productIds) {
    processed += 1;
    await options.onProgress?.({
      progress: 5 + (processed / Math.max(1, productIds.length)) * 90,
      summary: `Repairing ${processed} of ${productIds.length} problem products.`,
    });
    try {
      results.push(await repairWarehouseProductGroup(id, request));
    } catch (error) {
      results.push({ ok: false, productId: id, error: error?.message || String(error) });
    }
  }
  return {
    ok: results.every((item) => item.ok !== false),
    repaired: results.filter((item) => item.ok !== false).length,
    failed: results.filter((item) => item.ok === false).length,
    results,
    summary: `Problem products repaired ${results.filter((item) => item.ok !== false).length}; failed ${results.filter((item) => item.ok === false).length}.`,
  };
}

async function runBrandIndexRebuildOperation(payload = {}) {
  if (!shouldUsePostgresStorage()) {
    return { ok: false, error: "postgres_required", summary: "Brand index requires PostgreSQL storage." };
  }
  const limit = cleanLimit(payload.limit, 100000, 200000);
  const result = await rebuildWarehouseBrandIndexPostgres(getPrisma(), { limit });
  return {
    ok: result.ok !== false,
    ...result,
    source: "postgres",
    summary: `Brand index rebuilt: indexed ${result.indexed || result.created || 0}; scanned ${result.scanned || 0}.`,
  };
}

async function runYandexPricePushOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const force = payload?.force === true;
  const onlyChanged = payload?.onlyChanged !== false;
  const result = await sendWarehousePrices({
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    refreshMarketplacePrices: true,
    livePriceMaster: true,
  });
  return {
    ok: result.ok,
    marketplace: "yandex",
    limit,
    force,
    onlyChanged,
    processed: result.selected || limit,
    sent: result.sent || 0,
    failed: result.failed || 0,
    skipped: Array.isArray(result.skipped) ? result.skipped.length : Number(result.skipped || 0) || 0,
    yandexSent: result.yandexSent || 0,
    yandexFailed: result.yandexFailed || 0,
    yandexSkipped: result.yandexSkipped || 0,
    errors: Array.isArray(result.failedItems) ? result.failedItems : [],
    ...result,
    summary: `Yandex price push sent ${result.yandexSent || result.sent || 0}; failed ${result.yandexFailed || result.failed || 0}; skipped ${result.yandexSkipped || (Array.isArray(result.skipped) ? result.skipped.length : 0)}.`,
  };
}

async function runLinkedSupplierRecoveryOperation(payload = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const warehouse = await readWarehouse();
  const marketplaceFilter = cleanText(payload?.marketplace || "all").toLowerCase();
  const productIdSet = Array.isArray(payload?.productIds) && payload.productIds.length
    ? new Set(payload.productIds.map((id) => cleanText(id)).filter(Boolean))
    : null;
  const offerIdSet = Array.isArray(payload?.offerIds) && payload.offerIds.length
    ? new Set(payload.offerIds.map((id) => cleanText(id).toLowerCase()).filter(Boolean))
    : null;
  const candidateLimit = productIdSet || offerIdSet ? Math.max(limit, (warehouse.products || []).length) : limit;
  const candidates = linkedRecoveryCandidateProducts(warehouse.products || [], candidateLimit)
    .filter((product) => {
      if (marketplaceFilter !== "all" && cleanText(product.marketplace).toLowerCase() !== marketplaceFilter) return false;
      if (productIdSet && !productIdSet.has(String(product.id))) return false;
      if (offerIdSet && !offerIdSet.has(cleanText(product.offerId).toLowerCase())) return false;
      return true;
    })
    .slice(0, limit);

  if (!candidates.length) {
    return {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      recovered: 0,
      restoredStocks: 0,
      unarchived: 0,
      unarchivePending: 0,
      queuedByDailyLimit: 0,
      queueSize: (await readOzonUnarchiveQueue().catch(() => ({ items: [] }))).items.length || 0,
      errors: [],
      summary: "Нет привязанных карточек, которым нужно восстановление.",
    };
  }

  const rebuilt = [];
  for (const chunk of chunkArray(candidates, 200)) {
    const products = await buildFreshWarehouseProductsFromKnownProducts(
      warehouse,
      chunk,
      {
        refreshPrices: false,
        persistMutations: false,
        livePriceMaster: false,
        batchPriceMaster: false,
      },
    );
    rebuilt.push(...products);
  }

  const ready = rebuilt.filter((product) => product.hasLinks && product.selectedSupplier);
  const forceRecovery = payload.force !== false;
  const needsRecovery = forceRecovery
    ? ready
    : ready.filter((product) => (
        marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true })
        || Boolean(product.noSupplierAutomation?.stockZeroAt)
        || Boolean(product.noSupplierAutomation?.archivedAt)
      ));
  const notReady = candidates.length - ready.length;
  const alreadySellable = Math.max(0, ready.length - needsRecovery.length);
  const result = await runSupplierRecoveryAutomation(
    { products: needsRecovery },
    { productIds: needsRecovery.map((product) => product.id), source: "targeted", force: true },
  );
  const sellableRecovered = Number(result.sellableRecovered || 0);
  const unarchiveFailed = Number(result.unarchiveFailed || 0);
  const unarchivePending = Number(result.unarchivePending || 0);
  const queuedByDailyLimit = Number(result.queuedByDailyLimit || 0);
  const stockFailed = Number(result.stockFailed || 0);
  return {
    ok: result.errors?.length ? false : true,
    partial: Boolean(result.errors?.length && (result.recovered || result.restoredStocks || result.unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    ready: ready.length,
    notReady,
    alreadySellable,
    needsRecovery: needsRecovery.length,
    recovered: result.recovered || 0,
    sellableRecovered,
    restoredStocks: result.restoredStocks || 0,
    unarchived: result.unarchived || 0,
    unarchivePending,
    queuedByDailyLimit,
    nextRetryAt: result.nextRetryAt || null,
    queueSize: result.queueSize || 0,
    queuedSamples: result.queuedSamples || [],
    unarchiveFailed,
    stockFailed,
    errors: result.errors || [],
    productStatuses: result.productStatuses || [],
    summary: `Проверено ${candidates.length}; с доступным поставщиком ${ready.length}; уже продавались ${alreadySellable}; нужно восстановить ${needsRecovery.length}; полностью восстановлено ${sellableRecovered}; без поставщика ${notReady}; ошибки разархива ${unarchiveFailed}; ошибки остатков ${stockFailed}.`,
  };
}

function productLooksArchived(product = {}) {
  const state = product.marketplaceState || {};
  const code = cleanText(state.code || product.status).toLowerCase();
  const visibility = cleanText(state.visibility || product.visibility).toUpperCase();
  return Boolean(
    product.archived
      || state.archived
      || code === "archived"
      || visibility === "ARCHIVED"
      || product.noSupplierAutomation?.archivedAt
  );
}

function pickArchivedStockRestoreCandidates(products = [], { marketplace = "all", limit = 30000 } = {}) {
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 30000) || 30000)));
  return (Array.isArray(products) ? products : [])
    .filter((product) => {
      const productMarketplace = cleanText(product.marketplace).toLowerCase();
      if (!["ozon", "yandex"].includes(productMarketplace)) return false;
      if (marketplaceFilter !== "all" && productMarketplace !== marketplaceFilter) return false;
      if (!cleanText(product.offerId || product.offer_id)) return false;
      if (productMarketplace === "ozon" && !Number(product.productId || product.product_id || 0)) return false;
      return productLooksArchived(product);
    })
    .slice(0, max);
}

async function applyArchivedStockRestoreLocalPatch(warehouse, targetProducts, stockActions, unarchiveActions, stock, now = new Date().toISOString()) {
  const restoredStockIds = new Set((Array.isArray(stockActions) ? stockActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const unarchivedIds = new Set((Array.isArray(unarchiveActions) ? unarchiveActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const stockActionById = new Map((Array.isArray(stockActions) ? stockActions : []).map((item) => [String(item.id), item]));
  const unarchiveActionById = new Map((Array.isArray(unarchiveActions) ? unarchiveActions : []).map((item) => [String(item.id), item]));
  const touchedIds = new Set((Array.isArray(targetProducts) ? targetProducts : []).map((product) => String(product.id)));
  const changedProducts = [];
  for (const product of warehouse.products || []) {
    if (!touchedIds.has(String(product.id))) continue;
    const stockAction = stockActionById.get(String(product.id));
    const unarchiveAction = unarchiveActionById.get(String(product.id));
    if (stockAction) product.lastStockSend = marketplaceCommandFromAction(stockAction, product, now);
    if (unarchiveAction) product.lastArchiveSend = marketplaceCommandFromAction(unarchiveAction, product, now);
    product.targetStock = stock;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    product.noSupplierAutomation.stockZeroAt = null;
    product.noSupplierAutomation.archivedAt = null;
    product.noSupplierAutomation.recoveredAt = now;
    product.noSupplierAutomation.manualSellableAt = now;
    product.noSupplierAutomation.lastError = null;
    if (restoredStockIds.has(String(product.id)) || unarchivedIds.has(String(product.id))) {
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        code: "active",
        status: "active",
        archived: false,
        stock,
      };
      product.status = "active";
      product.archived = false;
    }
    product.updatedAt = now;
    changedProducts.push(product);
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "archived_stock_restore", writeLinks: false });
  }
  return changedProducts.length;
}

async function runArchivedStockRestoreOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const stock = Math.max(1, Math.min(9999, Math.round(Number(payload?.stock || 3) || 3)));
  const requestedMarketplace = cleanText(payload?.marketplace || "yandex").toLowerCase();
  const marketplace = ["yandex", "ozon", "all"].includes(requestedMarketplace) ? requestedMarketplace : "yandex";
  const batchSize = Math.max(20, Math.min(300, Math.round(Number(payload?.batchSize || 100) || 100)));
  const reportProgress = async (progress, summary) => {
    if (typeof options.onProgress !== "function") return;
    await options.onProgress({
      progress: Math.max(5, Math.min(99, Math.round(Number(progress || 5) || 5))),
      summary,
    });
  };
  const warehouse = await readWarehouse();
  const candidates = pickArchivedStockRestoreCandidates(warehouse.products || [], { marketplace, limit });
  if (!candidates.length) {
    const result = {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      marketplace,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      errors: [],
      summary: "Архивных товаров для восстановления не найдено.",
    };
    logger.info("archived stock restore complete", {
      candidates: 0,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      sellableRecovered: 0,
      stockFailed: 0,
      unarchiveFailed: 0,
      errors: 0,
    });
    return result;
  }

  const targetProducts = candidates.map((product) => normalizeWarehouseProduct({
    ...product,
    targetStock: stock,
    marketplaceState: {
      ...(product.marketplaceState || {}),
      stock,
    },
  }));
  await reportProgress(8, `Найдено архивных товаров: ${targetProducts.length}. Запускаю восстановление пачками по ${batchSize}.`);
  const firstStockActions = [];
  const unarchiveActions = [];
  const secondStockActions = [];
  let localPatched = 0;
  const batches = chunkArray(targetProducts, batchSize);
  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index];
    const processedBefore = index * batchSize;
    const processedAfter = Math.min(targetProducts.length, processedBefore + batch.length);
    logger.info("archived stock restore batch started", {
      batch: index + 1,
      batches: batches.length,
      products: batch.length,
      processed: processedBefore,
      total: targetProducts.length,
    });
    const batchFirstStockActions = await restoreStocksOnMarketplaces(batch);
    firstStockActions.push(...batchFirstStockActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor(batch.length / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Восстанавливаю остатки: ${processedBefore + Math.floor(batch.length / 3)} из ${targetProducts.length}.`,
    );
    const batchUnarchiveActions = await verifyYandexUnarchiveActions(
      batch,
      await unarchiveProductsOnMarketplaces(batch),
    );
    unarchiveActions.push(...batchUnarchiveActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor((batch.length * 2) / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Разархивирую карточки: ${processedBefore + Math.floor((batch.length * 2) / 3)} из ${targetProducts.length}.`,
    );
    const batchSecondStockActions = await restoreStocksOnMarketplaces(batch);
    secondStockActions.push(...batchSecondStockActions);
    localPatched += await applyArchivedStockRestoreLocalPatch(
      warehouse,
      batch,
      [...batchFirstStockActions, ...batchSecondStockActions],
      batchUnarchiveActions,
      stock,
      new Date().toISOString(),
    );
    await reportProgress(
      10 + (processedAfter / Math.max(1, targetProducts.length)) * 80,
      `Обработано ${processedAfter} из ${targetProducts.length}.`,
    );
    logger.info("archived stock restore batch complete", {
      batch: index + 1,
      batches: batches.length,
      processed: processedAfter,
      total: targetProducts.length,
      localPatched,
    });
  }
  const stockActions = [...firstStockActions, ...secondStockActions];
  const productStatuses = summarizeSupplierRecoveryProducts(targetProducts, stockActions, unarchiveActions);

  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => String(item.id)));
  const unarchiveOkIds = new Set(unarchiveActions.filter((item) => item.ok).map((item) => String(item.id)));
  const sellableRecovered = targetProducts.filter((product) => stockOkIds.has(String(product.id)) && unarchiveOkIds.has(String(product.id))).length;
  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, offerId: item.offerId, type: item.type, target: item.target, error: item.error }));
  const restoredStocks = stockActions.filter((item) => item.ok).length;
  const unarchived = unarchiveActions.filter((item) => item.ok).length;
  const unarchivePending = unarchiveActions.filter((item) => item.ok && item.pending).length;
  const stockFailed = stockActions.filter((item) => !item.ok).length;
  const unarchiveFailed = unarchiveActions.filter((item) => !item.ok).length;
  const result = {
    ok: errors.length === 0,
    partial: Boolean(errors.length && (restoredStocks || unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    marketplace,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors,
    productStatuses,
    summary: `Архивных товаров ${candidates.length}; остаток ${stock}; отправок остатка ${restoredStocks}; разархивировано ${unarchived}; готово к продаже ${sellableRecovered}; ошибки остатков ${stockFailed}; ошибки разархива ${unarchiveFailed}.`,
  };
  logger.info("archived stock restore complete", {
    candidates: result.candidates,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors: errors.length,
  });
  return result;
}

async function runYandexCardQualityAiDraftOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const threshold = Math.max(0, Math.min(100, Math.round(Number(payload?.threshold ?? 40) || 40)));
  const draftLimit = Math.max(0, Math.min(100, Math.round(Number(payload?.draftLimit ?? 20) || 20)));
  const generateImages = payload?.generateImages !== false;
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  if (!shops.length) {
    const error = new Error("Yandex Market is not configured.");
    error.statusCode = 400;
    throw error;
  }

  const warehouse = await readWarehouse();
  const yandexProducts = (warehouse.products || [])
    .map((product) => normalizeWarehouseProduct(product))
    .filter((product) => product.marketplace === "yandex" && cleanText(product.offerId))
    .slice(0, limit);
  await options.onProgress?.({ progress: 8, summary: `Checking Yandex card quality for ${yandexProducts.length} products.` });

  const qualityByTargetOffer = new Map();
  const qualityErrors = [];
  for (const shop of shops) {
    const offerIds = yandexProducts
      .filter((product) => matchesYandexTarget(product.target, shop.id))
      .map((product) => product.offerId);
    for (const chunk of chunkArray(offerIds, 200)) {
      try {
        const rows = await getYandexOfferCardsContentStatus(shop, chunk, { withRecommendations: true });
        for (const row of rows) {
          qualityByTargetOffer.set(yandexTargetOfferKey(shop.id, row.offerId), row);
        }
      } catch (error) {
        qualityErrors.push({
          target: shop.id,
          type: "quality",
          error: error?.message || "yandex_card_quality_failed",
        });
      }
    }
  }

  const now = new Date().toISOString();
  const changedProducts = [];
  const lowQualityProducts = [];
  for (const product of warehouse.products || []) {
    const normalized = normalizeWarehouseProduct(product);
    if (normalized.marketplace !== "yandex" || !normalized.offerId) continue;
    const shop = getYandexShopByTarget(normalized.target);
    const quality = shop ? qualityByTargetOffer.get(yandexTargetOfferKey(shop.id, normalized.offerId)) : null;
    if (!quality) continue;
    product.yandex = normalizeYandexDraft({
      ...(product.yandex || {}),
      extra: {
        ...(product.yandex?.extra || {}),
        cardQuality: quality,
      },
    });
    product.updatedAt = now;
    changedProducts.push(product);
    if (Number(quality.contentRating || 0) < threshold) {
      lowQualityProducts.push({ product, quality });
    }
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "yandex_card_quality_sync", writeLinks: false });
  }
  await options.onProgress?.({ progress: 35, summary: `Low quality cards: ${lowQualityProducts.length}. Creating AI drafts.` });

  const draftResults = [];
  const draftProducts = [];
  let stoppedByBillingLimit = false;
  let imageGenerationStoppedReason = "";
  for (const { product, quality } of lowQualityProducts.slice(0, draftLimit)) {
    const normalized = normalizeWarehouseProduct(product);
    try {
      const draft = await generateAiProductContentDraft(normalized, { marketplace: "yandex" });
      const savedDraft = normalizeAiContentDraft({
        ...draft,
        marketplace: "yandex",
        source: "yandex_card_quality",
        qualityBefore: quality.contentRating,
        recommendations: quality.recommendations,
        model: (await readEffectiveAiSettings()).textModel || openaiTextModel,
      });
      if (savedDraft) {
        product.aiContentDrafts = normalizeAiContentDrafts([...(product.aiContentDrafts || []), savedDraft]);
      }
      let imageDraftCreated = false;
      let imageError = "";
      if (generateImages && !imageGenerationStoppedReason) {
        try {
          const imageDraft = await generateOzonAiImageDraft(normalized, {
            prompt: `Create a clean marketplace product photo for ${normalized.name || normalized.offerId}. White background, realistic perfume product image, no text overlays.`,
          });
          product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), imageDraft]);
          imageDraftCreated = true;
        } catch (imageDraftError) {
          imageError = imageDraftError?.message || "ai_image_draft_failed";
          if (isOpenAiBillingLimitError(imageDraftError)) imageGenerationStoppedReason = imageError;
        }
      }
      product.updatedAt = new Date().toISOString();
      draftProducts.push(product);
      draftResults.push({
        id: product.id,
        offerId: normalized.offerId,
        target: normalized.target,
        contentRating: quality.contentRating,
        ok: Boolean(savedDraft),
        contentDraft: Boolean(savedDraft),
        imageDraft: imageDraftCreated,
        warning: imageError || undefined,
        error: savedDraft ? undefined : (imageError || "ai_content_draft_empty"),
      });
    } catch (error) {
      draftResults.push({
        id: product.id,
        offerId: normalized.offerId,
        target: normalized.target,
        contentRating: quality.contentRating,
        ok: false,
        error: error?.message || "ai_draft_failed",
        fatal: isOpenAiBillingLimitError(error) || undefined,
      });
      if (isOpenAiBillingLimitError(error)) {
        stoppedByBillingLimit = true;
        break;
      }
    }
  }
  if (draftProducts.length) {
    await writeWarehouseProductPatch(draftProducts, { reason: "yandex_card_quality_ai_drafts", writeLinks: false });
  }

  const warnings = draftResults
    .filter((item) => item.ok && item.warning)
    .map((item) => ({ id: item.id, offerId: item.offerId, type: "image", error: item.warning }));
  const failed = [...qualityErrors, ...draftResults.filter((item) => !item.ok)];
  const result = {
    ok: failed.length === 0,
    partial: Boolean(failed.length && draftResults.some((item) => item.ok)),
    limit,
    threshold,
    checked: yandexProducts.length,
    qualityLoaded: qualityByTargetOffer.size,
    lowQuality: lowQualityProducts.length,
    draftsCreated: draftResults.filter((item) => item.ok).length,
    imageDraftsCreated: draftResults.filter((item) => item.imageDraft).length,
    stoppedByBillingLimit,
    imageGenerationStoppedReason,
    warnings,
    failed: failed.length,
    results: draftResults,
    errors: failed,
    summary: `Yandex quality checked ${yandexProducts.length}; below ${threshold}: ${lowQualityProducts.length}; AI drafts: ${draftResults.filter((item) => item.ok).length}; image drafts: ${draftResults.filter((item) => item.imageDraft).length}; warnings: ${warnings.length}; errors: ${failed.length}.`,
  };
  logger.info("yandex card quality ai drafts complete", {
    checked: result.checked,
    qualityLoaded: result.qualityLoaded,
    lowQuality: result.lowQuality,
    draftsCreated: result.draftsCreated,
    imageDraftsCreated: result.imageDraftsCreated,
    stoppedByBillingLimit: result.stoppedByBillingLimit,
    imageGenerationStopped: Boolean(result.imageGenerationStoppedReason),
    warnings: warnings.length,
    failed: result.failed,
    sampleErrors: failed.slice(0, 5).map((item) => ({
      id: item.id,
      offerId: item.offerId,
      error: item.error,
    })),
    sampleWarnings: warnings.slice(0, 5),
  });
  return result;
}

async function runPriceMasterGroupLinksRepairOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 50000);
  const limit = Math.max(1, Math.min(100000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 50000));
  const warehouse = await readWarehouse();
  const groups = new Map();
  for (const product of (warehouse.products || [])) {
    const groupKey = warehouseProductPageGroupKey(product);
    if (!groupKey) continue;
    if (!groups.has(groupKey)) groups.set(groupKey, []);
    groups.get(groupKey).push(product);
  }

  const candidates = Array.from(groups.entries())
    .filter(([, products]) => products.length > 1 && products.some((product) => (product.links || []).length))
    .slice(0, limit);
  const changedProducts = [];
  const changedIds = [];
  const repairedGroups = [];
  let skippedGroups = 0;
  const now = new Date().toISOString();
  let processed = 0;

  for (const [groupKey, products] of candidates) {
    processed += 1;
    const before = warehouseGroupLinkSignature(products);
    if (before.ok) {
      skippedGroups += 1;
    } else {
      const syncResult = syncWarehouseProductGroupLinks(products, { now, username: "operation" });
      if ((syncResult.changedProducts || []).length) {
        changedProducts.push(...syncResult.changedProducts);
        changedIds.push(...syncResult.changedProducts.map((product) => product.id));
        repairedGroups.push({
          groupKey,
          products: products.map((product) => product.id),
          before,
          after: warehouseGroupLinkSignature(products),
        });
      } else {
        skippedGroups += 1;
      }
    }
    if (processed % 50 === 0) {
      await options.onProgress?.({
        progress: 10 + (processed / Math.max(1, candidates.length)) * 80,
        summary: `Checked ${processed} of ${candidates.length} PriceMaster groups.`,
      });
    }
  }

  const uniqueChanged = Array.from(new Map(changedProducts.map((product) => [String(product.id), product])).values());
  if (uniqueChanged.length) {
    for (const chunk of chunkArray(uniqueChanged, 200)) {
      await writeWarehouseProductPatch(chunk, { reason: "warehouse_links_repair_group" });
    }
    const uniqueIds = Array.from(new Set(changedIds.map(String)));
    queueMarketplaceJob("supplier-recovery-automation", { productIds: uniqueIds }, { priority: 2 });
    queueImmediateAutoPricePush(uniqueIds, "link_repair_group");
  }

  return {
    ok: true,
    processedGroups: candidates.length,
    repairedGroups: repairedGroups.length,
    changedProducts: uniqueChanged.length,
    changedProductIds: uniqueChanged.map((product) => product.id),
    skippedGroups,
    groups: repairedGroups.slice(0, 200),
    summary: `PriceMaster groups checked ${candidates.length}; repaired ${repairedGroups.length}; changed products ${uniqueChanged.length}; skipped ${skippedGroups}.`,
  };
}

async function runOperationPayload(job, options = {}) {
  const auditRequest = { session: { username: job.user || "system", role: job.role || "admin" } };
  if (job.type === "yandex-import-send") {
    return runOzonYandexImportSend({ ...(job.payload || {}), confirmed: true }, auditRequest);
  }
  if (job.type === "yandex-stock-sync") {
    const requestedLimit = Number(job.payload?.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const existingOfferIds = getLocalYandexExportedOfferIdSet(warehouse.products || []);
    const result = await sendYandexStocksFromOzonProducts(products, {
      dryRun: job.payload?.dryRun === true,
      warehouseProducts: warehouse.products || [],
      existingOfferIds,
    });
    await appendAudit(auditRequest, "yandex.stock.sync", {
      entityType: "yandex_stock_sync",
      entityId: "ozon_to_yandex",
      limit,
      sent: Number(result.sent || 0),
      failed: Number(result.failed || 0),
      skipped: Number(result.skipped || 0),
      newValue: result,
    });
    return { ok: result.ok, limit, ...result };
  }
  if (job.type === "yandex-price-push") {
    const result = await runYandexPricePushOperation(job.payload || {});
    await appendAudit(auditRequest, "yandex.price.push", {
      entityType: "yandex_price_push",
      entityId: "yandex",
      newValue: result,
    });
    return result;
  }
  if (job.type === "linked-supplier-recovery") {
    const result = await runLinkedSupplierRecoveryOperation(job.payload || {});
    await appendAudit(auditRequest, "marketplace.linked.recovery", {
      entityType: "linked_supplier_recovery",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "ozon-linked-unarchive") {
    const result = await runLinkedSupplierRecoveryOperation({
      ...(job.payload || {}),
      marketplace: "ozon",
      force: true,
    });
    await appendAudit(auditRequest, "marketplace.ozon.linked_unarchive", {
      entityType: "ozon_linked_unarchive",
      entityId: "ozon",
      newValue: result,
    });
    return result;
  }
  if (job.type === "ozon-unarchive-queue-process") {
    const result = await processOzonUnarchiveQueue({
      source: "ozon_unarchive_queue_operation",
      limit: job.payload?.limit || ozonUnarchiveQueueBatchLimit,
      force: job.payload?.force === true,
    });
    await appendAudit(auditRequest, "ozon.unarchive_queue.operation", {
      entityType: "ozon_unarchive_queue",
      entityId: "operation",
      newValue: result,
    });
    return result;
  }
  if (job.type === "sales-automation-run") {
    const result = await runSalesAutomationOperation(job.payload || {});
    await appendAudit(auditRequest, "sales_automation.run", {
      entityType: "sales_automation",
      entityId: "manual",
      newValue: result,
    });
    return result;
  }
  if (job.type === "problem-products-repair") {
    const result = await runProblemProductsRepairOperation(job.payload || {}, auditRequest, options);
    await appendAudit(auditRequest, "problem_products.repair", {
      entityType: "problem_products",
      entityId: "bulk",
      newValue: result,
    });
    return result;
  }
  if (job.type === "brand-index-rebuild") {
    const result = await runBrandIndexRebuildOperation(job.payload || {});
    await appendAudit(auditRequest, "warehouse.brands.rebuild_index", {
      entityType: "brand_index",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "restore-archived-stock") {
    const result = await runArchivedStockRestoreOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "marketplace.archived.restore_stock", {
      entityType: "archived_stock_restore",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "yandex-card-quality-ai-drafts") {
    const result = await runYandexCardQualityAiDraftOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "yandex.card_quality.ai_drafts", {
      entityType: "yandex_card_quality",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "repair-pricemaster-group-links") {
    const result = await runPriceMasterGroupLinksRepairOperation(job.payload || {}, options);
    await appendAudit(auditRequest, "warehouse.links.repair_group", {
      entityType: "warehouse_pricemaster_group_links",
      entityId: "all",
      newValue: result,
    });
    return result;
  }
  if (job.type === "marketplace-supplier-cart-preview") {
    const result = await runSupplierCartPreviewOperation(job.payload || {});
    await appendAudit(auditRequest, "supplier_cart.preview", {
      entityType: "supplier_cart",
      entityId: "preview",
      newValue: result,
    });
    return result;
  }
  if (job.type === "marketplace-supplier-cart-commit") {
    return runSupplierCartCommitOperation(job.payload || {}, auditRequest);
  }
  if (job.type === "health-deep") {
    return collectHealthDetails({ deep: true });
  }
  const error = new Error(`Unsupported operation type: ${job.type}`);
  error.statusCode = 400;
  throw error;
}

function startOperationJob(job) {
  setTimeout(async () => {
    let current = normalizeOperationJob({
      ...job,
      status: "running",
      startedAt: new Date().toISOString(),
      progress: 5,
    });
    await upsertOperationJob(current).catch((error) => logger.warn("operation job start write failed", { detail: error?.message || String(error) }));
    try {
      let lastProgressWriteAt = 0;
      const result = await runOperationPayload(current, {
        onProgress: async (progress = {}) => {
          const nextProgress = Math.max(current.progress || 0, Math.min(99, Number(progress.progress || progress.percent || 5) || 5));
          const nowMs = Date.now();
          if (nextProgress <= current.progress && nowMs - lastProgressWriteAt < 3000) return;
          current = normalizeOperationJob({
            ...current,
            progress: nextProgress,
            result: progress.summary ? { summary: cleanText(progress.summary) } : current.result,
          });
          lastProgressWriteAt = nowMs;
          await upsertOperationJob(current).catch((error) => logger.warn("operation job progress write failed", { detail: error?.message || String(error) }));
        },
      });
      const partial = result?.partial === true;
      current = normalizeOperationJob({
        ...current,
        status: result?.ok === false && !partial ? "failed" : "completed",
        finishedAt: new Date().toISOString(),
        progress: 100,
        result,
        error: result?.ok === false ? (partial ? "operation finished partially" : "operation finished with errors") : "",
      });
    } catch (error) {
      current = normalizeOperationJob({
        ...current,
        status: "failed",
        finishedAt: new Date().toISOString(),
        progress: 100,
        error: error?.message || String(error),
      });
      logger.warn("operation job failed", { id: current.id, type: current.type, detail: current.error });
    }
    await upsertOperationJob(current).catch((error) => logger.warn("operation job finish write failed", { detail: error?.message || String(error) }));
  }, 10);
}

registerOperationsRoutes(app, {
  requireAdmin,
  cleanLimit,
  cleanText,
  crypto,
  readOperationJobs,
  upsertOperationJob,
  operationJobPublic,
  operationTitle,
  startOperationJob,
  activeOperationJobs,
});

app.get("/api/supplier-cart/preview", requireAdmin, async (request, response, next) => {
  try {
    const preview = await buildSupplierCartPreview({
      marketplace: request.query.marketplace,
      from: request.query.from,
      to: request.query.to,
      limit: request.query.limit,
    });
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/draft", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
    const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
    const skipped = rows.length - ready - alreadyCommitted;
    response.json({
      ok: true,
      draftId: state.draft?.id || "",
      generatedAt: state.draft?.generatedAt || null,
      generatedBy: state.draft?.generatedBy || "",
      rows,
      total: rows.length,
      ready,
      skipped,
      alreadyCommitted,
      warnings: state.draft?.summary?.warnings || [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/schedule", requireAdmin, async (_request, response, next) => {
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-cart/schedule", requireAdmin, async (request, response, next) => {
  try {
    const appSettings = await readAppSettings();
    const settings = normalizeSupplierCartSettings({
      ...(appSettings.supplierCart || {}),
      ...(request.body || {}),
    });
    await writeAppSettings({
      ...appSettings,
      supplierCart: settings,
    });
    await scheduleSupplierCartAuto();
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/generate", requireAdmin, async (request, response, next) => {
  try {
    const preview = await generateSupplierCartDraft({
      marketplace: request.body?.marketplace || request.query.marketplace,
      from: request.body?.from || request.query.from,
      to: request.body?.to || request.query.to,
      limit: request.body?.limit || request.query.limit,
    }, request);
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/commit", requireAdmin, async (request, response, next) => {
  try {
    const rows = Array.isArray(request.body?.rows) ? request.body.rows : [];
    const keys = Array.isArray(request.body?.keys) ? request.body.keys : [];
    const state = await readSupplierCartState();
    const sourceRows = rows.length
      ? rows
      : (state.draft?.rows?.length ? state.draft.rows : (await buildSupplierCartPreview(request.body || {})).rows);
    const selectedKeys = new Set(keys.map(cleanText).filter(Boolean));
    const selectedRows = selectedKeys.size ? sourceRows.filter((row) => selectedKeys.has(cleanText(row.key))) : sourceRows;
    const result = await insertSupplierCartRowsIntoPriceMaster(selectedRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/history", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      totalProcessed: Object.keys(state.processed || {}).length,
      history: (state.history || []).slice().reverse(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-picking-list", requireStaff, async (request, response, next) => {
  try {
    const state = await readSupplierPickingState();
    const status = cleanText(request.query.status || "open").toLowerCase();
    const supplier = cleanText(request.query.supplier).toLowerCase();
    const q = cleanText(request.query.q).toLowerCase();
    const limit = cleanLimit(request.query.limit, 500);
    let rows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    if (status && status !== "all") rows = rows.filter((row) => row.status === status);
    if (supplier) rows = rows.filter((row) => cleanText(row.supplierName).toLowerCase().includes(supplier));
    if (q) {
      rows = rows.filter((row) => [
        row.productName,
        row.offerId,
        row.orderId,
        row.postingNumber,
        row.supplierName,
      ].some((value) => cleanText(value).toLowerCase().includes(q)));
    }
    rows.sort((left, right) => String(right.createdAt).localeCompare(String(left.createdAt)));
    const allRows = Object.values(state.rows || {}).map(normalizeSupplierPickingRow);
    const suppliers = Array.from(new Set(allRows.map((row) => row.supplierName).filter(Boolean))).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      rows: rows.slice(0, limit),
      total: rows.length,
      suppliers,
      summary: {
        open: allRows.filter((row) => row.status === "open").length,
        picked: allRows.filter((row) => row.status === "picked").length,
        missing: allRows.filter((row) => row.status === "missing").length,
        reordered: allRows.filter((row) => row.status === "reordered").length,
        suppliers: suppliers.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-picking-list/:key", requireStaff, async (request, response, next) => {
  try {
    const key = cleanText(request.params.key || "");
    const status = cleanText(request.body?.status).toLowerCase();
    if (!["open", "picked", "missing"].includes(status)) {
      return response.status(400).json({ error: "Unsupported picking status.", code: "supplier_picking_status_invalid" });
    }
    const state = await readSupplierPickingState();
    const current = state.rows[key] ? normalizeSupplierPickingRow(state.rows[key]) : null;
    if (!current) return response.status(404).json({ error: "Picking row not found.", code: "supplier_picking_not_found" });
    const now = new Date();
    const username = requestUsername(request);
    const nextRow = normalizeSupplierPickingRow({
      ...current,
      status,
      ...(status === "picked" ? { pickedBy: username, pickedAt: now.toISOString() } : {}),
      ...(status === "missing" ? {
        missingBy: username,
        missingAt: now.toISOString(),
        missingReason: cleanText(request.body?.reason || "employee_missing"),
        nextRetryAt: new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString(),
      } : {}),
      ...(status === "open" ? {
        pickedBy: "",
        pickedAt: null,
        missingBy: "",
        missingAt: null,
        missingReason: "",
        nextRetryAt: null,
      } : {}),
    });
    state.rows[key] = nextRow;
    await writeSupplierPickingState(state);

    const cartState = await readSupplierCartState();
    const blockKey = supplierBlockKey(current.offerId, current.partnerId);
    if (status === "missing" && current.offerId && current.partnerId) {
      cartState.supplierBlocks[blockKey] = {
        offerId: current.offerId,
        partnerId: current.partnerId,
        supplierName: current.supplierName,
        reason: "employee_missing",
        blockedAt: now.toISOString(),
        blockedBy: username,
        expiresAt: nextRow.nextRetryAt,
        sourcePickingKey: current.key,
      };
      const sourceCartKey = current.replacementFor || current.key.replace(/\|retry:.+$/, "");
      if (cartState.processed?.[sourceCartKey]) delete cartState.processed[sourceCartKey];
      await writeSupplierCartState(cartState);
      await appendAudit(request, "supplier_cart.supplier_blocked", {
        entityType: "supplier_cart",
        entityId: blockKey,
        newValue: cartState.supplierBlocks[blockKey],
      });
    } else if (status === "open" && cartState.supplierBlocks?.[blockKey]?.sourcePickingKey === current.key) {
      delete cartState.supplierBlocks[blockKey];
      await writeSupplierCartState(cartState);
    }

    await appendAudit(request, `supplier_picking.${status === "picked" ? "picked" : status === "missing" ? "missing" : "status_update"}`, {
      entityType: "supplier_picking",
      entityId: key,
      oldValue: current,
      newValue: nextRow,
    });
    response.json({ ok: true, row: nextRow });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-picking-list/invoices", requireStaff, async (request, response, next) => {
  try {
    const state = await readSupplierPickingState();
    const period = cleanText(request.query.period || "30d").toLowerCase();
    const rows = supplierPickingInvoiceRows(state, period);
    const groups = [];
    const bySupplierDate = new Map();
    for (const row of rows) {
      const date = String(row.pickedAt || row.createdAt || "").slice(0, 10) || "unknown";
      const key = `${row.supplierName || "-"}|${date}`;
      if (!bySupplierDate.has(key)) bySupplierDate.set(key, { supplierName: row.supplierName || "-", date, rows: [], totalQuantity: 0, totalValue: 0 });
      const group = bySupplierDate.get(key);
      group.rows.push(row);
      group.totalQuantity += Number(row.quantity || 0);
      group.totalValue += Number(row.quantity || 0) * Number(row.price || 0);
    }
    groups.push(...bySupplierDate.values());
    response.json({
      ok: true,
      period,
      total: rows.length,
      groups,
      rows,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-yandex-import/send", async (request, response, next) => {
  try {
    if (request.body?.confirmed !== true) {
      return response.status(400).json({ error: "Для выгрузки в Яндекс нужно подтверждение confirmed=true." });
    }
    const requestedLimit = Number(request.body?.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const sendLimit = Math.max(1, Math.min(yandexImportSendLimit, Number(request.body?.sendLimit || yandexImportSendLimit) || yandexImportSendLimit));
    const shops = uniqueYandexShopsByBusiness();
    if (!shops.length) return response.status(400).json({ error: "Yandex Market не настроен. Добавьте кабинет в настройках." });

    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const initialRows = products.map((product) => buildOzonYandexImportCandidate(product));
    const candidateOfferIds = initialRows
      .filter((row) => !row.blockReasons?.length && row.yandexReady)
      .map((row) => cleanText(row.offerId))
      .filter(Boolean);
    const warnings = [];
    const yandexExistingOfferIds = await getKnownYandexExistingOfferIds(candidateOfferIds, {
      products: warehouse.products || [],
      warnings,
      allowCatalogRefresh: false,
      allowDirectCheck: false,
    });
    const rows = products.map((product) => buildOzonYandexImportCandidate(product, { yandexExistingOfferIds }));
    const eligibleRows = rows.filter((row) => row.eligible);
    const selectedRows = eligibleRows.slice(0, sendLimit);
    const selectedIds = new Set(selectedRows.map((row) => row.id));
    const productsById = new Map(products.map((product) => [product.id, product]));
    const selectedProducts = selectedRows.map((row) => productsById.get(row.id)).filter(Boolean);
    const offers = selectedProducts
      .map((product) => buildYandexOfferMapping(normalizeWarehouseProduct(product)).offer)
      .filter((offer) => offer?.offerId);

    const cardResults = [];
    for (const shop of shops) {
      const sent = await sendYandexOfferMappings(shop, offers);
      cardResults.push(...sent.map((item) => ({
        ...item,
        stage: "card",
        target: shop.id,
        targetName: shop.name || "Yandex Market",
      })));
    }
    const failedRows = cardResults.filter((item) => !item.ok);
    const sentCount = cardResults.filter((item) => item.ok).length;
    const skippedExisting = rows.filter((row) => row.existingInYandex).length;
    const skippedBlocked = rows.filter((row) => row.blockReasons?.length).length;
    const skippedMissing = rows.filter((row) => !row.yandexReady).length;
    const sentOfferIds = new Set(cardResults
      .filter((item) => item.ok)
      .map((item) => cleanText(item.offerId).toLowerCase())
      .filter(Boolean));
    if (sentOfferIds.size) {
      writeYandexExistingOfferIdCache(new Set([...yandexExistingOfferIds, ...sentOfferIds]))
        .catch((error) => logger.warn("write Yandex existing offer cache failed", { detail: error?.message || String(error) }));
    }
    const exportedProducts = selectedProducts
      .filter((product) => sentOfferIds.has(cleanText(product.offerId).toLowerCase()));
    const priceStage = exportedProducts.length
      ? await sendYandexPricesFromOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds, warehouse })
      : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };
    const stockStage = exportedProducts.length
      ? await sendYandexStocksForExportedOzonProducts(exportedProducts, { shops, existingOfferIds: sentOfferIds })
      : { ok: true, sent: 0, failed: 0, skipped: 0, warnings: [], results: [] };
    const stageWarnings = [
      ...warnings,
      ...(Array.isArray(priceStage.warnings) ? priceStage.warnings : []),
      ...(Array.isArray(stockStage.warnings) ? stockStage.warnings : []),
    ];
    const results = [
      ...cardResults,
      ...(Array.isArray(priceStage.results) ? priceStage.results : []),
      ...(Array.isArray(stockStage.results) ? stockStage.results : []),
    ];

    if (sentCount > 0) {
      const now = new Date().toISOString();
      const yandexProducts = [];
      const priceResultByTargetOffer = new Map((Array.isArray(priceStage.results) ? priceStage.results : [])
        .filter((item) => item.ok && item.target && item.offerId)
        .map((item) => [yandexPriceUpdateResultKey(item), item]));
      for (const product of warehouse.products || []) {
        if (!selectedIds.has(product.id) || !sentOfferIds.has(cleanText(product.offerId).toLowerCase())) continue;
        product.exports = product.exports || {};
        const baseExportState = {
          status: "sent",
          targetName: shops.map((shop) => shop.name || shop.id).join(", "),
          sentAt: now,
          priceSent: Number(priceStage.sent || 0),
          stockSent: Number(stockStage.sent || 0),
        };
        for (const shop of shops) {
          const key = yandexTargetOfferKey(shop.id, product.offerId);
          const priceResult = priceResultByTargetOffer.get(key) || null;
          const exportState = {
            ...baseExportState,
            targetName: shop.name || shop.id,
            price: Number(priceResult?.price || 0) || undefined,
            stock: pickOzonProductStockForYandex(product),
            markupCoefficient: Number(priceResult?.markupCoefficient || 0) || undefined,
            priceSource: priceResult?.priceSource || undefined,
          };
          product.exports[shop.id] = exportState;
          yandexProducts.push(buildYandexWarehouseProductFromOzonExport(product, shop, exportState));
        }
        product.exports.yandex = baseExportState;
        product.updatedAt = now;
      }
      if (yandexProducts.length) {
        warehouse.products = mergeProducts(warehouse.products || [], yandexProducts);
      }
      await writeWarehouse(warehouse);
    }

    await appendAudit(request, "yandex.import.send", {
      entityType: "yandex_import",
      entityId: "ozon_to_yandex",
      limit,
      sendLimit,
      targets: shops.map((shop) => ({ id: shop.id, name: shop.name, businessId: shop.businessId })),
      planned: eligibleRows.length,
      plannedNow: selectedRows.length,
      sent: sentCount,
      failed: failedRows.length,
      priceSent: Number(priceStage.sent || 0),
      priceFailed: Number(priceStage.failed || 0),
      priceSkippedNoPrice: Number(priceStage.skippedNoPrice || 0),
      stockSent: Number(stockStage.sent || 0),
      stockFailed: Number(stockStage.failed || 0),
      skippedExisting,
      skippedBlocked,
      skippedMissing,
      warnings: stageWarnings.slice(0, 100),
      failedOfferIds: results.filter((item) => !item.ok).map((item) => item.offerId).filter(Boolean).slice(0, 500),
      newValue: {
        planned: eligibleRows.length,
        plannedNow: selectedRows.length,
        sent: sentCount,
        failed: failedRows.length,
        priceSent: Number(priceStage.sent || 0),
        priceFailed: Number(priceStage.failed || 0),
        priceSkippedNoPrice: Number(priceStage.skippedNoPrice || 0),
        stockSent: Number(stockStage.sent || 0),
        stockFailed: Number(stockStage.failed || 0),
      },
    });

    response.json({
      ok: failedRows.length === 0 && Number(priceStage.failed || 0) === 0 && Number(stockStage.failed || 0) === 0,
      generatedAt: new Date().toISOString(),
      limit,
      sendLimit,
      targets: shops.map((shop) => ({ id: shop.id, name: shop.name, businessId: shop.businessId })),
      planned: eligibleRows.length,
      plannedNow: selectedRows.length,
      sent: sentCount,
      failed: failedRows.length,
      priceSent: Number(priceStage.sent || 0),
      priceFailed: Number(priceStage.failed || 0),
      priceSkipped: Number(priceStage.skipped || 0),
      priceSkippedNoPrice: Number(priceStage.skippedNoPrice || 0),
      stockSent: Number(stockStage.sent || 0),
      stockFailed: Number(stockStage.failed || 0),
      stockSkipped: Number(stockStage.skipped || 0),
      skippedExisting,
      skippedBlocked,
      skippedMissing,
      skippedByLimit: Math.max(0, eligibleRows.length - selectedRows.length),
      warnings: stageWarnings,
      results,
      summary: summarizeOzonYandexImportPreview(rows),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-yandex-import/archive-blocked", async (request, response, next) => {
  try {
    if (request.body?.confirmed !== true) {
      return response.status(400).json({ error: "Нужно подтверждение archived-blocked confirmed=true." });
    }
    const requestedLimit = Number(request.body?.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const candidates = products.map(buildOzonYandexImportCandidate);
    const blockedIds = new Set(candidates
      .filter((row) => row.blockReasons?.length)
      .map((row) => row.id)
      .filter(Boolean));
    const blockedProducts = products.filter((product) => blockedIds.has(product.id));
    const actions = await archiveProductsOnMarketplaces(blockedProducts);
    response.json({
      ok: true,
      requested: blockedProducts.length,
      archived: actions.filter((item) => item.ok).length,
      failed: actions.filter((item) => !item.ok).length,
      actions,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-yandex-import/sync-stocks", async (request, response, next) => {
  try {
    const requestedLimit = Number(request.body?.limit || request.query.limit || 30000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
    const warehouse = await readWarehouse();
    const products = (warehouse.products || [])
      .filter((product) => product.marketplace === "ozon")
      .slice(0, limit);
    const existingOfferIds = getLocalYandexExportedOfferIdSet(warehouse.products || []);
    const result = await sendYandexStocksFromOzonProducts(products, {
      dryRun: request.body?.dryRun === true,
      warehouseProducts: warehouse.products || [],
      existingOfferIds,
    });
    response.json({ ok: result.ok, limit, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/yandex-cleanup/preview", async (request, response, next) => {
  try {
    const protectedBrands = parseProtectedBrandList(request.body?.protectedBrands || request.body?.brands || "");
    const requestedLimit = Number(request.body?.limit || 50000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 50000));
    const preview = await buildYandexCleanupPreview({ protectedBrands, limit });
    const toDelete = (preview.rows || []).filter((row) => row.action === "delete").length;
    response.json({
      ...preview,
      summary: {
        ...(preview.summary || {}),
        deleteLimit: yandexCleanupDeleteLimit,
        deletePlannedNow: Math.min(toDelete, yandexCleanupDeleteLimit),
        deleteSkippedByLimit: Math.max(0, toDelete - yandexCleanupDeleteLimit),
      },
    });
  } catch (error) {
    next(error);
  }
});

function publicYandexCleanupAuditEntry(entry = {}) {
  const details = entry.details || {};
  return {
    at: entry.at || null,
    user: entry.user || "system",
    planned: Number(details.planned || 0),
    plannedNow: Number(details.plannedNow || 0),
    skippedByLimit: Number(details.skippedByLimit || 0),
    deleted: Number(details.deleted || 0),
    failed: Number(details.failed || 0),
    notDeleted: Number(details.notDeleted || 0),
    protectedBrands: Array.isArray(details.protectedBrands) ? details.protectedBrands : [],
    failedOfferIds: Array.isArray(details.failedOfferIds) ? details.failedOfferIds : [],
    summary: details.summary || {},
  };
}

function publicYandexImportAuditEntry(entry = {}) {
  const details = entry.details || {};
  return {
    at: entry.at || null,
    user: entry.user || "system",
    planned: Number(details.planned || 0),
    sent: Number(details.sent || 0),
    failed: Number(details.failed || 0),
    priceSent: Number(details.priceSent || 0),
    priceFailed: Number(details.priceFailed || 0),
    stockSent: Number(details.stockSent || 0),
    stockFailed: Number(details.stockFailed || 0),
    skippedExisting: Number(details.skippedExisting || 0),
    skippedBlocked: Number(details.skippedBlocked || 0),
    skippedMissing: Number(details.skippedMissing || 0),
    warnings: Array.isArray(details.warnings) ? details.warnings : [],
    failedOfferIds: Array.isArray(details.failedOfferIds) ? details.failedOfferIds : [],
    targets: Array.isArray(details.targets) ? details.targets : [],
  };
}

app.get("/api/ozon-yandex-import/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 20, 100);
    const audit = await readAuditFiltered({ action: "yandex.import.send" }, limit);
    response.json({ ok: true, history: audit.map(publicYandexImportAuditEntry), total: audit.length });
  } catch (error) {
    next(error);
  }
});

app.get("/api/yandex-cleanup/history", requireAdmin, async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 20, 100);
    const audit = await readAuditFiltered({ action: "yandex.cleanup.delete" }, limit);
    response.json({ ok: true, history: audit.map(publicYandexCleanupAuditEntry), total: audit.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/yandex-cleanup/archive", async (request, response, next) => {
  try {
    response.status(410).json({ error: "Архивация отключена. Используйте удаление: /api/yandex-cleanup/delete." });
  } catch (error) {
    next(error);
  }
});

app.post("/api/yandex-cleanup/delete", async (request, response, next) => {
  try {
    const dryRun = request.body?.dryRun === true;
    if (!dryRun && (request.body?.confirmed !== true || cleanText(request.body?.confirmationText) !== "УДАЛИТЬ ЯНДЕКС")) {
      return response.status(400).json({ error: "Для удаления товаров Яндекса нужно подтверждение: УДАЛИТЬ ЯНДЕКС." });
    }
    const protectedBrands = parseProtectedBrandList(request.body?.protectedBrands || request.body?.brands || "");
    if (!protectedBrands.length) {
      return response.status(400).json({ error: "Укажите хотя бы один бренд, который нельзя удалять." });
    }
    const requestedLimit = Number(request.body?.limit || 50000);
    const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 50000));
    const preview = await buildYandexCleanupPreview({ protectedBrands, limit });
    const toDelete = (preview.rows || []).filter((row) => row.action === "delete");
    const limitedToDelete = toDelete.slice(0, yandexCleanupDeleteLimit);
    const deleteSummary = {
      ...(preview.summary || {}),
      toDelete: toDelete.length,
      toArchive: toDelete.length,
      deleteLimit: yandexCleanupDeleteLimit,
      deletePlannedNow: limitedToDelete.length,
      deleteSkippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
    };
    if (dryRun) {
      return response.json({
        ok: true,
        dryRun: true,
        generatedAt: new Date().toISOString(),
        protectedBrands,
        summary: deleteSummary,
        planned: toDelete.length,
        plannedNow: limitedToDelete.length,
        skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
        deleted: 0,
        failed: 0,
        notDeleted: 0,
        warnings: preview.warnings || [],
        rows: preview.rows || [],
      });
    }
    const results = await deleteYandexCleanupRows(limitedToDelete);
    const deleted = results.filter((item) => item.ok).length;
    const failedRows = results.filter((item) => !item.ok);
    await appendAudit(request, "yandex.cleanup.delete", {
      entityType: "yandex_cleanup",
      entityId: "business_catalog",
      protectedBrands,
      limit,
      summary: deleteSummary,
      planned: toDelete.length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
      deleted,
      failed: failedRows.length,
      notDeleted: failedRows.filter((item) => item.error === "not_deleted_by_yandex").length,
      failedOfferIds: failedRows.map((item) => item.offerId).filter(Boolean).slice(0, 500),
      newValue: {
        planned: toDelete.length,
        plannedNow: limitedToDelete.length,
        deleted,
        failed: failedRows.length,
        protectedBrands,
      },
    });
    response.json({
      ok: failedRows.length === 0,
      generatedAt: new Date().toISOString(),
      protectedBrands,
      summary: deleteSummary,
      planned: toDelete.length,
      plannedNow: limitedToDelete.length,
      skippedByLimit: Math.max(0, toDelete.length - limitedToDelete.length),
      deleted,
      failed: failedRows.length,
      notDeleted: failedRows.filter((item) => item.error === "not_deleted_by_yandex").length,
      warnings: preview.warnings || [],
      results,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/export", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Product export was not sent because manual confirmation is required." });
    }

    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, exports: product.exports || {}, updatedAt: product.updatedAt });

    const targetId = cleanText(request.body.target || product.target || product.marketplace);
    const targetMeta = targetById(targetId) || { id: targetId, marketplace: targetId, name: targetId };
    product.exports = product.exports || {};

    if (targetMeta.marketplace === "ozon") {
      const account = getOzonAccountByTarget(targetMeta.id || targetId);
      if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

      const built = buildOzonWarehouseProductItem(product, request.body);
      if (!built.ready) {
        return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
      }

      const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
      const exportState = {
        status: "sent",
        targetName: account.name || "Ozon",
        sentAt: new Date().toISOString(),
      };
      product.exports[account.id] = exportState;
      product.exports.ozon = exportState;
      product.updatedAt = new Date().toISOString();
      await writeWarehouse(warehouse);
      const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
      await appendAudit(request, "warehouse.product.export", {
        productId: product.id,
        offerId: product.offerId,
        target: account.id,
        oldValue: before,
        newValue: { id: product.id, exports: product.exports, updatedAt: product.updatedAt },
      });
      return response.json({ ok: true, target: account.id, sent: 1, item: built.item, result, product: freshProduct || normalizeWarehouseProduct(product) });
    }

    if (targetMeta.marketplace === "yandex" || targetId === "yandex") {
      const shop = getYandexShopByTarget(targetMeta.id || targetId);
      if (!shop) return response.status(400).json({ error: "Кабинет Yandex Market не найден. Добавьте его в настройках." });

      const built = buildYandexOfferMapping(product, request.body);
      if (!built.ready) {
        return response.status(400).json({ error: "Не хватает обязательных полей Yandex Market.", missing: built.missing });
      }

      const result = await yandexRequest(
        shop,
        "POST",
        `/v2/businesses/${shop.businessId}/offer-mappings/update`,
        { offerMappings: [{ offer: built.offer }] },
      );
      const exportState = {
        status: "sent",
        targetName: shop.name || "Yandex Market",
        sentAt: new Date().toISOString(),
      };
      product.exports[shop.id] = exportState;
      product.exports.yandex = exportState;
      product.updatedAt = new Date().toISOString();
      warehouse.products = mergeProducts(warehouse.products || [], [
        buildYandexWarehouseProductFromOzonExport(product, shop, exportState),
      ]);
      await writeWarehouse(warehouse);
      const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
      await appendAudit(request, "warehouse.product.export", {
        productId: product.id,
        offerId: product.offerId,
        target: shop.id,
        oldValue: before,
        newValue: { id: product.id, exports: product.exports, updatedAt: product.updatedAt },
      });
      return response.json({ ok: true, target: shop.id, sent: 1, offer: built.offer, result, product: freshProduct || normalizeWarehouseProduct(product) });
    }

    return response.status(400).json({ error: "Неизвестный маркетплейс для выгрузки." });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/publish", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Product was not published because manual confirmation is required." });
    }

    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    if (product.marketplace !== "ozon") {
      return response.status(400).json({ error: "Автосоздание карточки сейчас поддержано только для Ozon." });
    }

    const rules = await readOzonProductRules();
    const built = buildOzonProductPayload(
      {
        offerId: product.offerId,
        name: request.body.name || product.name,
        price: Number(request.body.usdPrice || 0),
      },
      { ...rules, ...(request.body.ozon || {}) },
    );
    if (!built.ready) {
      return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
    }

    const account = getOzonAccountByTarget(product.target || "ozon");
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
    response.json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/products/send", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon products were not sent because manual confirmation is required.",
      });
    }

    const offerIds = new Set(
      (Array.isArray(request.body.offerIds) ? request.body.offerIds : [])
        .map((offerId) => String(offerId || "").trim())
        .filter(Boolean),
    );

    if (!offerIds.size) {
      return response.status(400).json({ error: "No selected products to send." });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const rules = await readOzonProductRules();
    const existingOfferIds = await getOzonOfferIdSet(5000, account);
    const candidates = await getPriceMasterProductCandidates({ limit: 1000 });
    const selected = candidates.filter((row) => offerIds.has(row.offerId));
    const items = [];
    const blocked = [];

    for (const row of selected) {
      const built = buildOzonProductPayload(row, rules);
      if (existingOfferIds.has(row.offerId)) {
        blocked.push({ offerId: row.offerId, reason: "already_exists" });
      } else if (!built.ready) {
        blocked.push({ offerId: row.offerId, reason: "missing_fields", missing: built.missing });
      } else {
        items.push(built.item);
      }
    }

    if (!items.length) {
      return response.status(400).json({ error: "No ready products to send.", blocked });
    }

    const results = [];
    for (const chunk of chunkArray(items, 100)) {
      const data = await ozonRequest("/v2/product/import", { items: chunk }, account);
      results.push(data);
    }

    response.json({
      ok: true,
      sent: items.length,
      blocked,
      results,
    });
  } catch (error) {
    next(error);
  }
});

async function runSync() {
  let connection;
  try {
    connection = await pool.getConnection();
    const previous = await readSnapshot();
    const currentOffers = await getCurrentOffers(connection);
    const { currentItems, changes } = compareSnapshots(previous.items || {}, currentOffers);
    const syncId = crypto.randomUUID();
    const snapshot = {
      syncId,
      createdAt: new Date().toISOString(),
      items: currentItems,
      changes,
    };
    await writeSnapshot(snapshot);
    await appendHistory(snapshot);

    const result = {
      syncId,
      createdAt: snapshot.createdAt,
      items: Object.keys(currentItems).length,
      changes: changes.length,
      changeCounts: changes.reduce((acc, change) => {
        acc[change.type] = (acc[change.type] || 0) + 1;
        return acc;
      }, {}),
    };
    Object.defineProperty(result, "changedRows", {
      value: changes,
      enumerable: false,
      configurable: false,
    });
    return result;
  } finally {
    if (connection) connection.release();
  }
}

async function sendZeroStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) {
      actions.push({
        id: product?.id || "",
        type: "zero_stock",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.offerId ? "missing_offer_id" : "missing_target"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const payload = { stocks: await buildOzonStockPayloadItems(chunk, account, () => 0, { allWarehouses: true }) };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: true })));
        } catch (error) {
          const detail = error?.message || "stock_zero_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: false, error: detail })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      const stockShops = yandexStockShops([shop]);
      if (!stockShops.length) {
        actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target: shop.id, ok: false, error: "yandex_stock_campaign_not_configured" })));
        continue;
      }
      for (const stockShop of stockShops) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({ offerId: item.offerId, stock: 0 })));
            actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", target: stockShop.id, ok: true })));
          } catch (error) {
            const detail = error?.message || "stock_zero_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", target: stockShop.id, ok: false, error: detail })));
          }
        }
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "zero_stock", target, ok: false, error: "unsupported_marketplace" })));
    }
  }

  return actions;
}

async function sendTargetStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of pickTargetStockSendProducts(products)) {
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) continue;
      for (const chunk of chunkArray(items, 100)) {
        const payload = { stocks: await buildOzonStockPayloadItems(chunk, account, (item) => item.targetStock) };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: true })));
        } catch (error) {
          const detail = error?.message || "target_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: false, error: detail })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      const restoreActions = await restoreYandexArchivedStocks(items, [shop]);
      const restoreFailed = restoreActions.filter((item) => !item.ok);
      if (restoreFailed.length) {
        logger.warn("yandex stock restore before target send failed", {
          target: shop.id,
          items: restoreFailed.length,
        });
      }
      for (const stockShop of yandexStockShops([shop])) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({ offerId: item.offerId, stock: item.targetStock })));
            actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", target: stockShop.id, stock: item.targetStock, ok: true })));
          } catch (error) {
            const detail = error?.message || "target_stock_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", target: stockShop.id, stock: item.targetStock, ok: false, error: detail })));
          }
        }
      }
    }
  }

  return actions;
}

async function archiveProductsOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.target || (product.marketplace === "yandex" && !cleanText(product.offerId))) {
      actions.push({
        id: product?.id || "",
        type: "archive",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.target ? "missing_target" : "missing_offer_id"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const productIds = chunk.map((item) => Number(item.productId || 0)).filter((id) => id > 0);
        if (!productIds.length) continue;
        try {
          await ozonRequest("/v1/product/archive", { product_id: productIds }, account);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: true })));
        } catch (error) {
          const detail = error?.message || "archive_failed";
          const expected = isExpectedMarketplaceArchiveBlock(detail);
          actions.push(...chunk.map((item) => ({
            id: item.id,
            type: "archive",
            ok: false,
            skipped: expected,
            reason: expected ? "marketplace_has_stock" : undefined,
            error: detail,
          })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, offerId: item.offerId, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 200)) {
        const archiveResults = await sendYandexOfferArchiveState(shop, chunk.map((item) => item.offerId), true);
        const byOfferId = new Map(archiveResults.map((item) => [cleanText(item.offerId).toLowerCase(), item]));
        actions.push(...chunk.map((item) => {
          const result = byOfferId.get(cleanText(item.offerId).toLowerCase());
          return {
            id: item.id,
            type: "archive",
            target: shop.id,
            offerId: item.offerId,
            ok: Boolean(result?.ok),
            error: result?.ok ? undefined : (result?.error || "archive_failed"),
          };
        }));
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "archive", target, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

async function restoreStocksOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) {
      actions.push({
        id: product?.id || "",
        type: "restore_stock",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.offerId ? "missing_offer_id" : "missing_target"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 100)) {
        const payload = {
          stocks: await buildOzonStockPayloadItems(
            chunk,
            account,
            (item) => Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
          ),
        };
        if (!payload.stocks.length) continue;
        try {
          for (const stockChunk of chunkArray(payload.stocks, 100)) {
            await ozonRequest("/v2/products/stocks", { stocks: stockChunk }, account);
          }
          actions.push(...chunk.map((item) => ({
            id: item.id,
            type: "restore_stock",
            stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
            ok: true,
          })));
        } catch (error) {
          const detail = error?.message || "restore_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: false, error: detail })));
        }
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      const stockShops = yandexStockShops([shop]);
      if (!stockShops.length) {
        actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target: shop.id, ok: false, error: "yandex_stock_campaign_not_configured" })));
        continue;
      }
      for (const stockShop of stockShops) {
        for (const chunk of chunkArray(items, 100)) {
          try {
            await sendYandexStockChunk(stockShop, chunk.map((item) => ({
              offerId: item.offerId,
              stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
            })));
            actions.push(...chunk.map((item) => ({
              id: item.id,
              type: "restore_stock",
              target: stockShop.id,
              stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
              ok: true,
            })));
          } catch (error) {
            const detail = error?.message || "restore_stock_failed";
            actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", target: stockShop.id, ok: false, error: detail })));
          }
        }
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "restore_stock", target, offerId: item.offerId, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

async function unarchiveProductsOnMarketplaces(products = [], options = {}) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.target || (product.marketplace === "yandex" && !cleanText(product.offerId))) {
      actions.push({
        id: product?.id || "",
        type: "unarchive",
        offerId: product?.offerId || "",
        target: product?.target || "",
        ok: false,
        error: !product?.id ? "missing_product_id" : (!product?.target ? "missing_target" : "missing_offer_id"),
      });
      continue;
    }
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push(product);
  }

  for (const [key, items] of byTarget.entries()) {
    const [marketplace, target] = key.split(":");
    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target);
      if (!account) {
        actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, ok: false, error: "ozon_account_not_found" })));
        continue;
      }
      let queueState = await readOzonUnarchiveQueue();
      const forceDailyLimit = options.forceOzonDailyLimit === true;
      const dailyUsed = ozonUnarchiveDailyUsed(queueState, target);
      const effectiveDailyUsed = forceDailyLimit ? 0 : dailyUsed;
      const remainingToday = Math.max(0, ozonUnarchiveDailyLimit - effectiveDailyUsed);
      const runnableItems = items.slice(0, remainingToday);
      const queuedItems = items.slice(remainingToday);
      if (queuedItems.length) {
        const nextRetryAt = nextOzonUnarchiveRetryAt();
        queueState = queueOzonUnarchiveItems(queueState, queuedItems, { nextRetryAt });
        await writeOzonUnarchiveQueue(queueState);
        actions.push(...ozonUnarchiveQueuedActions(queuedItems, queueState, { nextRetryAt }));
      }
      let usedToday = effectiveDailyUsed;
      for (const chunk of chunkArray(runnableItems, 100)) {
        const productIds = chunk.map((item) => Number(item.productId || 0)).filter((id) => id > 0);
        if (!productIds.length) continue;
        try {
          await ozonRequest("/v1/product/unarchive", { product_id: productIds }, account);
          usedToday += productIds.length;
          queueState = removeOzonUnarchiveQueueItems(queueState, chunk);
          setOzonUnarchiveDailyUsed(queueState, target, usedToday);
          await writeOzonUnarchiveQueue(queueState);
          actions.push(...chunk.map((item) => ({
            id: item.id,
            type: "unarchive",
            target,
            offerId: item.offerId,
            ok: true,
            dailyLimit: ozonUnarchiveDailyLimit,
            dailyUsed: usedToday,
            queueSize: queueState.items.length,
          })));
        } catch (error) {
          const detail = error?.message || "unarchive_failed";
          if (/daily|суточ|лимит|limit|quota|auto.?archive|автоархив/i.test(detail)) {
            const nextRetryAt = nextOzonUnarchiveRetryAt();
            queueState = queueOzonUnarchiveItems(queueState, chunk, { nextRetryAt });
            await writeOzonUnarchiveQueue(queueState);
            actions.push(...ozonUnarchiveQueuedActions(chunk, queueState, {
              warning: "ozon_unarchive_daily_limit_queued",
              nextRetryAt,
            }));
          } else {
            actions.push(...chunk.map((item) => ({ id: item.id, type: "unarchive", target, offerId: item.offerId, ok: false, error: detail })));
          }
        }
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, offerId: item.offerId, ok: false, error: "yandex_shop_not_found" })));
        continue;
      }
      for (const chunk of chunkArray(items, 200)) {
        const unarchiveResults = await sendYandexOfferArchiveState(shop, chunk.map((item) => item.offerId), false);
        const byOfferId = new Map(unarchiveResults.map((item) => [cleanText(item.offerId).toLowerCase(), item]));
        actions.push(...chunk.map((item) => {
          const result = byOfferId.get(cleanText(item.offerId).toLowerCase());
          return {
            id: item.id,
            type: "unarchive",
            target: shop.id,
            offerId: item.offerId,
            ok: Boolean(result?.ok),
            error: result?.ok ? undefined : (result?.error || "unarchive_failed"),
          };
        }));
      }
    }
    if (!["ozon", "yandex"].includes(marketplace)) {
      actions.push(...items.map((item) => ({ id: item.id, type: "unarchive", target, offerId: item.offerId, ok: false, error: "unsupported_marketplace" })));
    }
  }
  return actions;
}

async function verifyYandexUnarchiveActions(products = [], actions = [], options = {}) {
  const verified = (Array.isArray(actions) ? actions : []).map((action) => ({ ...action }));
  const productsById = new Map((Array.isArray(products) ? products : [])
    .map((product) => [String(product.id), product]));
  const pendingByTarget = new Map();
  for (const action of verified) {
    if (!action?.ok || action.type !== "unarchive") continue;
    const product = productsById.get(String(action.id));
    if (!product || product.marketplace !== "yandex") continue;
    const offerId = cleanText(action.offerId || product.offerId);
    const target = cleanText(action.target || product.target);
    if (!offerId || !target) continue;
    const key = target;
    if (!pendingByTarget.has(key)) pendingByTarget.set(key, []);
    pendingByTarget.get(key).push({ action, offerId });
  }
  if (!pendingByTarget.size) return verified;

  const attempts = Math.max(1, Math.min(5, Math.round(Number(options.attempts || process.env.YANDEX_UNARCHIVE_VERIFY_ATTEMPTS || 3) || 3)));
  const delayMs = Math.max(0, Math.min(10000, Math.round(Number(options.delayMs ?? process.env.YANDEX_UNARCHIVE_VERIFY_DELAY_MS ?? 1500) || 1500)));
  const activeOfferIds = new Set();
  const archivedOfferIds = new Set();
  const failedTargets = new Map();

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    activeOfferIds.clear();
    archivedOfferIds.clear();
    failedTargets.clear();

    for (const [target, rows] of pendingByTarget.entries()) {
      const shop = getYandexShopByTarget(target);
      if (!shop) {
        failedTargets.set(target, "shop_not_found");
        continue;
      }
      const offerIds = rows.map((row) => row.offerId);
      try {
        const mappings = await getYandexOfferMappingsByOfferIds(shop, offerIds);
        for (const item of mappings) {
          const offerId = yandexOfferIdFromMapping(item).toLowerCase();
          if (!offerId) continue;
          const offer = pickYandexOfferFromMapping(item);
          const state = pickYandexState(item, offer);
          const key = `${target}:${offerId}`;
          if (state.code === "archived") archivedOfferIds.add(key);
          else activeOfferIds.add(key);
        }
      } catch (error) {
        failedTargets.set(target, error?.message || "yandex_unarchive_verify_failed");
      }
    }

    const remaining = [];
    for (const [target, rows] of pendingByTarget.entries()) {
      for (const row of rows) {
        const key = `${target}:${row.offerId.toLowerCase()}`;
        if (!activeOfferIds.has(key)) remaining.push(row);
      }
    }
    if (!remaining.length) break;
    if (attempt < attempts && delayMs > 0) await sleep(delayMs);
  }

  for (const [target, rows] of pendingByTarget.entries()) {
    const targetError = failedTargets.get(target);
    for (const row of rows) {
      const key = `${target}:${row.offerId.toLowerCase()}`;
      if (activeOfferIds.has(key)) {
        row.action.verified = true;
        row.action.pending = false;
        continue;
      }
      row.action.verified = false;
      row.action.ok = true;
      row.action.pending = true;
      row.action.warning = targetError
        ? `unarchive_verify_pending: ${targetError}`
        : (archivedOfferIds.has(key) ? "still_archived_after_unarchive" : "unarchive_not_visible_after_api");
      delete row.action.error;
    }
  }

  return verified;
}

function marketplaceHasPositiveStock(product = {}) {
  const state = product.marketplaceState || {};
  if (Number(state.stock || 0) > 0 || Number(state.present || 0) > 0) return true;
  return (Array.isArray(state.warehouses) ? state.warehouses : [])
    .some((warehouse) => Number(warehouse.stock || warehouse.present || 0) > 0);
}

function marketplaceOfferAutomationKey(product = {}) {
  const marketplace = cleanText(product.marketplace).toLowerCase();
  const target = cleanText(product.target).toLowerCase();
  const offerId = cleanText(product.offerId).toLowerCase();
  return marketplace && target && offerId ? `${marketplace}:${target}:${offerId}` : "";
}

function shouldSendTargetStockForProduct(product = {}) {
  if (!product?.hasLinks || !product.ready || !product.selectedSupplier) return false;
  const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
  if (targetStock <= 0) return false;
  const currentStock = Math.max(0, Math.round(Number(product.marketplaceState?.stock || 0)));
  return targetStock !== currentStock;
}

function pickTargetStockSendProducts(products = []) {
  return (Array.isArray(products) ? products : [])
    .filter((product) =>
      product?.id
      && product.offerId
      && product.target
      && shouldSendTargetStockForProduct(product)
    )
    .map((product) => ({
      ...product,
      targetStock: Math.max(1, Math.round(Number(product.targetStock || 0))),
    }));
}

function marketplaceProductNeedsSalesRecovery(product = {}, { includeUnknown = true } = {}) {
  const state = product.marketplaceState || {};
  const code = cleanText(state.code || product.status).toLowerCase();
  const archived = Boolean(product.archived || state.archived || code === "archived");
  if (archived || code === "out_of_stock" || code === "inactive") return true;
  if (includeUnknown && (!code || code === "unknown" || state.partial)) return true;
  const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
  return targetStock > 0 && !marketplaceHasPositiveStock(product);
}

function pickNoSupplierAutomationCandidates(products = [], options = {}) {
  const list = Array.isArray(products) ? products : [];
  const protectedOfferKeys = new Set(list
    .filter((product) => product.hasLinks || product.noSupplierAutomation?.manualSellableAt)
    .map(marketplaceOfferAutomationKey)
    .filter(Boolean));
  const noLinkProducts = options.includeNoLinks
    && !keepUnlinkedProductsSellable
    ? list.filter((product) => {
        if (product.hasLinks || product.noSupplierAutomation?.manualSellableAt) return false;
        const key = marketplaceOfferAutomationKey(product);
        return !key || !protectedOfferKeys.has(key);
      })
    : [];
  return {
    toZeroStock: autoZeroStockOnNoSupplier
      ? noLinkProducts.filter((product) => !product.noSupplierAutomation?.stockZeroAt || marketplaceHasPositiveStock(product))
      : [],
    toArchive: autoArchiveOnNoLinks
      ? noLinkProducts.filter(
          (product) =>
            !product.noSupplierAutomation?.archivedAt
            && product.marketplaceState?.code !== "archived",
        )
      : [],
  };
}

function pickSupplierRecoveryCandidates(products = [], { productIds, force = false } = {}) {
  const idSet = Array.isArray(productIds) && productIds.length
    ? new Set(productIds.map((id) => String(id || "").trim()).filter(Boolean))
    : null;
  return (Array.isArray(products) ? products : []).filter((product) => {
    if (idSet && !idSet.has(String(product.id))) return false;
    if (!product.hasLinks || !product.selectedSupplier) return false;
    if (force) return true;
    const needsRecovery = marketplaceProductNeedsSalesRecovery(product, { includeUnknown: true });
    if (
      product.noSupplierAutomation?.recoveredAt
      && !product.noSupplierAutomation?.stockZeroAt
      && !product.noSupplierAutomation?.archivedAt
      && !needsRecovery
    ) return false;
    return Boolean(product.noSupplierAutomation?.stockZeroAt)
      || Boolean(product.noSupplierAutomation?.archivedAt)
      || needsRecovery;
  });
}

function summarizeNoSupplierAutomationProducts(products = [], actions = []) {
  const actionsByProduct = new Map();
  for (const action of actions) {
    if (!action?.id) continue;
    if (!actionsByProduct.has(action.id)) actionsByProduct.set(action.id, []);
    actionsByProduct.get(action.id).push(action);
  }
  return products.map((product) => {
    const productActions = actionsByProduct.get(product.id) || [];
    const failed = productActions.find((action) => !action.ok && !action.skipped);
    const skipped = productActions.find((action) => action.skipped);
    return {
      id: product.id,
      offerId: product.offerId || "",
      hasLinks: Boolean(product.hasLinks),
      status: failed ? "error" : (skipped ? "skipped" : (productActions.length ? "processed" : "no_action")),
      actions: Array.from(new Set(productActions.map((action) => action.type).filter(Boolean))),
      error: failed?.error || null,
      skippedReason: skipped?.reason || null,
    };
  });
}

function summarizeSupplierRecoveryProducts(products = [], stockActions = [], unarchiveActions = []) {
  const actionsByProduct = new Map();
  for (const action of [...stockActions, ...unarchiveActions]) {
    if (!action?.id) continue;
    const id = String(action.id);
    if (!actionsByProduct.has(id)) actionsByProduct.set(id, []);
    actionsByProduct.get(id).push(action);
  }
  return (Array.isArray(products) ? products : []).map((product) => {
    const productActions = actionsByProduct.get(String(product.id)) || [];
    const stock = productActions.filter((action) => action.type === "restore_stock");
    const unarchive = productActions.filter((action) => action.type === "unarchive");
    const stockOk = stock.filter((action) => action.ok).length;
    const stockFailed = stock.filter((action) => !action.ok).length;
    const unarchiveOk = unarchive.filter((action) => action.ok).length;
    const unarchiveFailed = unarchive.filter((action) => !action.ok).length;
    const unarchivePending = unarchive.filter((action) => action.ok && action.pending).length;
    const queuedByDailyLimit = unarchive.filter((action) => action.ok && action.queuedByDailyLimit).length;
    const failed = productActions.find((action) => !action.ok);
    const needsUnarchive = productLooksArchived(product);
    const sellable = stockOk > 0
      && stockFailed === 0
      && unarchiveFailed === 0
      && queuedByDailyLimit === 0
      && (!needsUnarchive || unarchiveOk > 0);
    return {
      id: product.id,
      offerId: product.offerId || "",
      marketplace: product.marketplace || "",
      target: product.target || "",
      stockOk,
      stockFailed,
      unarchiveOk,
      unarchiveFailed,
      unarchivePending,
      queuedByDailyLimit,
      nextRetryAt: unarchive.find((action) => action.nextRetryAt)?.nextRetryAt || null,
      sellable,
      status: failed ? "error" : (sellable ? "sellable" : (productActions.length ? "processed" : "no_action")),
      error: failed?.error || null,
      warning: productActions.find((action) => action.warning)?.warning || null,
    };
  });
}

async function runNoSupplierMarketplaceAutomation(preview, options = {}) {
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const now = new Date().toISOString();
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates(products, {
    includeNoLinks: Boolean(options.includeNoLinks),
    now,
  });
  const source = options.source || (Array.isArray(options.productIds) && options.productIds.length ? "targeted" : "full");

  if (!toZeroStock.length && !toArchive.length) {
    const productStatuses = summarizeNoSupplierAutomationProducts(products, []);
    logger.info("no-supplier automation complete", {
      source,
      products: products.length,
      zeroStockSent: 0,
      archived: 0,
      errors: 0,
      statuses: productStatuses.slice(0, 10),
    });
    return { zeroStockSent: 0, archived: 0, errors: [], productStatuses };
  }

  const stockActions = await sendZeroStocksToMarketplace(toZeroStock);
  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => item.id));
  const archiveMap = new Map();
  for (const product of toArchive) archiveMap.set(product.id, product);
  for (const product of toZeroStock) {
    if (stockOkIds.has(product.id) && !product.hasLinks) archiveMap.set(product.id, product);
  }
  const archiveActions = await archiveProductsOnMarketplaces(Array.from(archiveMap.values()));
  const allActions = [...stockActions, ...archiveActions];
  if (!allActions.length) {
    const productStatuses = summarizeNoSupplierAutomationProducts(products, []);
    return { zeroStockSent: 0, archived: 0, errors: [], productStatuses };
  }

  const warehouse = await readWarehouse();
  const changedById = new Map();
  for (const action of allActions) {
    const product = warehouse.products.find((item) => item.id === action.id);
    if (!product) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || { stockZeroAt: null, archivedAt: null, lastError: null };
    if (action.type === "zero_stock") product.lastStockSend = marketplaceCommandFromAction(action, product, now);
    if (action.type === "archive") product.lastArchiveSend = marketplaceCommandFromAction(action, product, now);
    if (action.ok && action.type === "zero_stock") product.noSupplierAutomation.stockZeroAt = now;
    if (action.ok && action.type === "archive") product.noSupplierAutomation.archivedAt = now;
    product.noSupplierAutomation.lastError = action.ok || action.skipped ? null : action.error;
    product.updatedAt = now;
    changedById.set(product.id, product);
  }
  const changedProducts = Array.from(changedById.values());
  if (source === "targeted" && changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "no_supplier_automation" });
  } else {
    await writeWarehouse(warehouse);
  }

  const errors = allActions
    .filter((item) => !item.ok && !item.skipped)
    .map((item) => ({ id: item.id, type: item.type, error: item.error }));
  const skipped = allActions
    .filter((item) => item.skipped)
    .map((item) => ({ id: item.id, type: item.type, reason: item.reason, error: item.error }));
  const productStatuses = summarizeNoSupplierAutomationProducts(products, allActions);
  logger.info("no-supplier automation complete", {
    source,
    products: products.length,
    zeroStockSent: stockActions.filter((item) => item.ok).length,
    archived: archiveActions.filter((item) => item.ok).length,
    skipped: skipped.length,
    errors: errors.length,
    statuses: productStatuses.slice(0, 10),
  });

  return {
    zeroStockSent: stockActions.filter((item) => item.ok).length,
    archived: archiveActions.filter((item) => item.ok).length,
    errors,
    skipped,
    productStatuses,
  };
}

async function runSupplierRecoveryAutomation(preview, options = {}) {
  if (!autoRestoreOnSupplierReturn) {
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [] };
  }
  const products = Array.isArray(preview?.products) ? preview.products : [];
  let recovered = pickSupplierRecoveryCandidates(products, options);
  const source = options.source || (Array.isArray(options.productIds) && options.productIds.length ? "targeted" : "full");
  let queuedOzonProducts = [];
  try {
    const queueState = await readOzonUnarchiveQueue();
    const nowMs = Date.now();
    const perTargetTaken = new Map();
    const queuedIds = [];
    for (const item of queueState.items || []) {
      if (item.status === "done") continue;
      const id = cleanText(item.id);
      if (!id) continue;
      const retryAtMs = item.nextRetryAt ? new Date(item.nextRetryAt).getTime() : 0;
      if (retryAtMs && Number.isFinite(retryAtMs) && retryAtMs > nowMs) continue;
      const target = cleanText(item.target) || "default";
      const remainingToday = Math.max(0, ozonUnarchiveDailyLimit - ozonUnarchiveDailyUsed(queueState, target));
      const taken = perTargetTaken.get(target) || 0;
      if (taken >= remainingToday) continue;
      queuedIds.push(id);
      perTargetTaken.set(target, taken + 1);
      if (queuedIds.length >= ozonUnarchiveQueueBatchLimit) break;
    }
    const missingQueuedIds = queuedIds.filter((id) => !recovered.some((product) => String(product.id) === id));
    if (missingQueuedIds.length) {
      queuedOzonProducts = await buildFreshWarehouseProducts(missingQueuedIds);
      const existingIds = new Set(recovered.map((product) => String(product.id)));
      recovered = [
        ...recovered,
        ...queuedOzonProducts.filter((product) => product?.marketplace === "ozon" && !existingIds.has(String(product.id))),
      ];
    }
  } catch (error) {
    logger.warn("ozon unarchive queue load failed during supplier recovery", { detail: error?.message || String(error) });
  }
  if (!recovered.length) {
    logger.info("supplier recovery automation complete", {
      source,
      products: products.length,
      recovered: 0,
      restoredStocks: 0,
      unarchived: 0,
      errors: 0,
    });
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [], source };
  }
  const firstStockActions = await restoreStocksOnMarketplaces(recovered);
  const unarchiveActions = await verifyYandexUnarchiveActions(
    recovered,
    await unarchiveProductsOnMarketplaces(recovered, { forceOzonDailyLimit: options.forceOzonDailyLimit === true }),
  );
  const secondStockActions = await restoreStocksOnMarketplaces(recovered);
  const stockActions = [...firstStockActions, ...secondStockActions];
  const productStatuses = summarizeSupplierRecoveryProducts(recovered, stockActions, unarchiveActions);
  const warehouse = await readWarehouse();
  const now = new Date().toISOString();
  const recoveredIds = new Set(recovered.map((item) => String(item.id)));
  const sellableIds = new Set(productStatuses.filter((item) => item.sellable).map((item) => String(item.id)));
  const statusById = new Map(productStatuses.map((item) => [String(item.id), item]));
  const stockActionsById = new Map();
  const unarchiveActionsById = new Map();
  for (const action of stockActions) {
    const id = String(action.id || "");
    if (id) stockActionsById.set(id, action);
  }
  for (const action of unarchiveActions) {
    const id = String(action.id || "");
    if (id) unarchiveActionsById.set(id, action);
  }
  const restoredStockById = new Map(stockActions
    .filter((item) => item.ok)
    .map((item) => [String(item.id), Math.max(1, Math.round(Number(item.stock || 1)))]));
  const changedProducts = [];
  for (const product of warehouse.products) {
    const productId = String(product.id);
    if (!recoveredIds.has(productId)) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    const status = statusById.get(productId);
    const stockAction = stockActionsById.get(productId);
    const unarchiveAction = unarchiveActionsById.get(productId);
    if (stockAction) product.lastStockSend = marketplaceCommandFromAction(stockAction, product, now);
    if (unarchiveAction) product.lastArchiveSend = marketplaceCommandFromAction(unarchiveAction, product, now);
    if (status?.sellable) {
      product.noSupplierAutomation.recoveredAt = now;
      product.noSupplierAutomation.manualSellableAt = now;
      product.noSupplierAutomation.stockZeroAt = null;
      product.noSupplierAutomation.archivedAt = null;
      product.noSupplierAutomation.lastError = null;
    } else if (status?.error) {
      product.noSupplierAutomation.lastError = status.error;
    }
    if (sellableIds.has(productId)) {
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        code: "active",
        status: "active",
        archived: false,
        stock: restoredStockById.get(productId) || Math.max(1, Math.round(Number(product.marketplaceState?.stock || 1))),
      };
      product.status = "active";
      product.archived = false;
    }
    product.updatedAt = now;
    changedProducts.push(product);
  }
  if (source === "targeted" && changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "supplier_recovery_automation" });
  } else {
    await writeWarehouse(warehouse);
  }
  queueMarketplaceJob(
    "auto-price-push",
    {
      productIds: recovered.map((item) => item.id),
      usdRate: undefined,
      minDiffRub: 0,
      minDiffPct: 0,
      force: Boolean(options.force),
    },
    { priority: 2 },
  );

  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, type: item.type, error: item.error }));
  const unarchivePending = unarchiveActions.filter((item) => item.ok && item.pending).length;
  const queuedByDailyLimit = unarchiveActions.filter((item) => item.ok && item.queuedByDailyLimit).length;
  const queuedNextRetryAt = unarchiveActions
    .filter((item) => item.queuedByDailyLimit && item.nextRetryAt)
    .map((item) => item.nextRetryAt)
    .sort()[0] || null;
  const ozonQueueState = await readOzonUnarchiveQueue().catch(() => ({ items: [] }));
  const productStatusesTotal = productStatuses.length;
  logger.info("supplier recovery automation complete", {
    source,
    products: products.length,
    recovered: recovered.length,
    queuedPulled: queuedOzonProducts.length,
    sellableRecovered: sellableIds.size,
    restoredStocks: stockActions.filter((item) => item.ok).length,
    unarchived: unarchiveActions.filter((item) => item.ok).length,
    unarchivePending,
    queuedByDailyLimit,
    stockFailed: stockActions.filter((item) => !item.ok).length,
    unarchiveFailed: unarchiveActions.filter((item) => !item.ok).length,
    errors: errors.length,
  });
  return {
    recovered: recovered.length,
    sellableRecovered: sellableIds.size,
    restoredStocks: stockActions.filter((item) => item.ok).length,
    unarchived: unarchiveActions.filter((item) => item.ok).length,
    unarchivePending,
    queuedByDailyLimit,
    nextRetryAt: queuedNextRetryAt,
    queueSize: Array.isArray(ozonQueueState.items) ? ozonQueueState.items.length : 0,
    queuedProcessedThisRun: queuedOzonProducts.length,
    queuedSamples: unarchiveActions
      .filter((item) => item.queuedByDailyLimit)
      .slice(0, 20)
      .map((item) => ({ id: item.id, offerId: item.offerId, target: item.target, nextRetryAt: item.nextRetryAt })),
    stockFailed: stockActions.filter((item) => !item.ok).length,
    unarchiveFailed: unarchiveActions.filter((item) => !item.ok).length,
    errors,
    productStatuses: productStatuses.slice(0, 200),
    productStatusesTotal,
    source,
  };
}

function msUntilNextDailyRun(timeString, now = new Date()) {
  const [rawHour = "11", rawMinute = "0"] = String(timeString || "11:00").split(":");
  const hour = Math.min(Math.max(Number(rawHour) || 11, 0), 23);
  const minute = Math.min(Math.max(Number(rawMinute) || 0, 0), 59);
  const next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  return next.getTime() - now.getTime();
}

async function runDailyRefresh(trigger = "manual") {
  if (dailySyncPromise) return dailySyncPromise;

  dailySyncPromise = (async () => {
    const startedAt = new Date().toISOString();
    await writeDailySyncState({
      status: "running",
      trigger,
      startedAt,
      lastRunAt: startedAt,
    });

    try {
      const priceMaster = await runSync();
      const warehouse = await buildWarehouseView({ sync: true });
      const backgroundAutomation = trigger === "manual"
        ? {
            automation: await runNoSupplierMarketplaceAutomation(warehouse),
            recovery: await runSupplierRecoveryAutomation(warehouse),
            scope: { mode: "full", productIds: null, marketplaceChanged: warehouse.marketplaceSyncChanged || 0 },
          }
        : await runTargetedBackgroundSupplierAutomations(priceMaster, warehouse);
      const automation = backgroundAutomation.automation;
      const recovery = backgroundAutomation.recovery;
      let pricePush = null;
      const shouldSendPrices = trigger === "manual" || (trigger === "schedule" && dailySyncSendPrices);
      if (shouldSendPrices) {
        try {
          pricePush = trigger === "manual"
            ? await sendWarehousePrices({
              usdRate: undefined,
              minDiffRub: 0,
              minDiffPct: 0,
              dryRun: false,
            })
            : await sendPriceMasterDeltaWarehousePrices(priceMaster, warehouse);
        } catch (err) {
          const detail = err?.message || String(err);
          pricePush = { sent: 0, failed: 0, skipped: [], error: detail };
          logger.warn("manual daily sync price push failed", { detail });
        }
      }
      const state = await writeDailySyncState(withDailySyncLog({
        status: "ok",
        trigger,
        startedAt,
        lastRunAt: new Date().toISOString(),
        priceMaster,
        warehouse: {
          total: warehouse.total,
          ready: warehouse.ready,
          changed: warehouse.changed,
          withoutSupplier: warehouse.withoutSupplier,
          sourceError: warehouse.sourceError,
          supplierSync: warehouse.supplierSync,
          zeroStockSent: automation.zeroStockSent,
          autoArchived: automation.archived,
          recovered: recovery.recovered,
          automationScope: backgroundAutomation.scope?.productIds?.length ?? null,
          automationSkippedReason: automation.reason || recovery.reason || null,
          pricePush: pricePush
            ? {
              sent: Number(pricePush.sent || 0),
              failed: Number(pricePush.failed || 0),
              queued: Number(pricePush.queued || 0),
              queuedBatches: Number(pricePush.queuedBatches || 0),
              skipped: Array.isArray(pricePush.skipped) ? pricePush.skipped.length : 0,
              error: pricePush.error || null,
            }
            : null,
        },
      }));
      return state;
    } catch (error) {
      const state = await writeDailySyncState(withDailySyncLog({
        status: "failed",
        trigger,
        startedAt,
        lastRunAt: new Date().toISOString(),
        error: error.code || error.message,
      }));
      return state;
    }
  })().finally(() => {
    dailySyncPromise = null;
  });

  return dailySyncPromise;
}

function scheduleDailySync() {
  if (!dailySyncEnabled) return;
  if (dailySyncTimer) clearTimeout(dailySyncTimer);
  const delay = msUntilNextDailyRun(dailySyncTime);
  dailySyncNextRunAt = new Date(Date.now() + delay).toISOString();
  dailySyncTimer = setTimeout(async () => {
    try {
      const result = await runDailyRefresh("schedule");
      logger.info("daily sync tick", { status: result.status, lastRunAt: result.lastRunAt });
    } catch (error) {
      logger.error("daily sync failed", { detail: error.code || error.message, err: error });
    } finally {
      scheduleDailySync();
    }
  }, delay);
}

async function runAutoSyncCycle(trigger = "auto") {
  if (manualWarehouseSyncPromise) {
    logger.info("auto sync skipped: manual warehouse sync running");
    return { status: "manual_sync_running" };
  }
  if (autoSyncRunning) return { status: "already_running" };
  autoSyncRunning = true;
  try {
    const result = await runSync();
    const warehouse = await buildWarehouseView({ sync: true });
    const backgroundAutomation = await runTargetedBackgroundSupplierAutomations(result, warehouse);
    const automation = backgroundAutomation.automation;
    const recovery = backgroundAutomation.recovery;
    const autoPricePush = await sendPriceMasterDeltaWarehousePrices(result, warehouse);
    logger.info("auto sync complete", {
      trigger,
      items: result.items,
      changes: result.changes,
      at: result.createdAt,
      warehouseTotal: warehouse.total,
      zeroStockSent: automation.zeroStockSent,
      autoArchived: automation.archived,
      recovered: recovery.recovered,
      marketplaceSyncChanged: warehouse.marketplaceSyncChanged || 0,
      automationScope: backgroundAutomation.scope?.productIds?.length || 0,
      automationSkippedReason: automation.reason || recovery.reason || null,
      priceMasterDeltaProducts: autoPricePush.delta?.productIds?.length || 0,
      priceMasterDeltaSkippedReason: autoPricePush.delta?.reason || null,
      autoPriceQueued: autoPricePush.queued || 0,
      autoPriceQueuedBatches: autoPricePush.queuedBatches || 0,
      autoPriceSent: autoPricePush.sent || 0,
      autoPriceFailed: autoPricePush.failed || 0,
      autoPriceSkipped: Array.isArray(autoPricePush.skipped) ? autoPricePush.skipped.length : 0,
    });
    if (automation.errors.length) {
      logger.warn("no-supplier automation errors", { count: automation.errors.length, sample: automation.errors.slice(0, 10) });
    }
    return { status: "ok", result, warehouse, automation, recovery, autoPricePush, automationScope: backgroundAutomation.scope };
  } finally {
    autoSyncRunning = false;
  }
}

async function runManualWarehouseSync(trigger = "manual_sync") {
  setManualWarehouseSyncProgress({
    percent: 8,
    stage: "PriceMaster",
    meta: "Обновляю прайс, поставщиков и snapshot PriceMaster.",
    processed: 0,
    total: 0,
  });
  const priceMaster = await runSync();
  setManualWarehouseSyncProgress({
    percent: 24,
    stage: "Маркетплейсы",
    meta: `PriceMaster готов: ${formatRuNumber(priceMaster?.items || 0)} строк. Загружаю карточки Ozon/Yandex.`,
    processed: Number(priceMaster?.items || 0),
    total: Number(priceMaster?.items || 0),
  });
  const warehouse = await buildWarehouseView({
    sync: true,
    onProgress: (progress) => setManualWarehouseSyncProgress(progress),
  });
  setManualWarehouseSyncProgress({
    percent: 74,
    stage: "Склад",
    meta: `Карточки загружены: ${formatRuNumber(warehouse.total || 0)}. Сверяю поставщиков и правила остатков.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  const automation = await runNoSupplierMarketplaceAutomation(warehouse);
  setManualWarehouseSyncProgress({
    percent: 84,
    stage: "Автоматизация",
    meta: `Проверены пропавшие поставщики. Нулевые остатки: ${formatRuNumber(automation.zeroStockSent || 0)}, архив: ${formatRuNumber(automation.archived || 0)}.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  const recovery = await runSupplierRecoveryAutomation(warehouse);
  setManualWarehouseSyncProgress({
    percent: 94,
    stage: "Финал",
    meta: `Восстановлено товаров: ${formatRuNumber(recovery.recovered || 0)}. Сохраняю результат и обновляю интерфейс.`,
    processed: Number(warehouse.total || 0),
    total: Number(warehouse.total || 0),
  });
  return {
    ok: true,
    trigger,
    priceMaster,
    warehouse: {
      total: warehouse.total,
      ready: warehouse.ready,
      changed: warehouse.changed,
      withoutSupplier: warehouse.withoutSupplier,
      supplierSync: warehouse.supplierSync,
      zeroStockSent: automation.zeroStockSent,
      autoArchived: automation.archived,
      recovered: recovery.recovered,
    },
  };
}

function getManualWarehouseSyncStatus() {
  return {
    ...manualWarehouseSyncState,
    running: manualWarehouseSyncState.status === "running",
  };
}

function startManualWarehouseSync(trigger = "manual") {
  if (manualWarehouseSyncPromise) return { started: false, status: getManualWarehouseSyncStatus() };
  const startedAt = new Date().toISOString();
  manualWarehouseSyncState = {
    status: "running",
    trigger,
    startedAt,
    finishedAt: null,
    result: null,
    error: null,
    progress: {
      percent: 3,
      stage: "Старт",
      meta: "Запуск фоновой синхронизации склада.",
      processed: 0,
      total: 0,
      updatedAt: startedAt,
    },
  };
  manualWarehouseSyncPromise = runManualWarehouseSync(trigger)
    .then((result) => {
      manualWarehouseSyncState = {
        status: "ok",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        result,
        error: null,
        progress: {
          ...(manualWarehouseSyncState.progress || {}),
          percent: 100,
          stage: "Готово",
          meta: `Синхронизация завершена. Карточек: ${formatRuNumber(result?.warehouse?.total || 0)}.`,
          processed: Number(result?.warehouse?.total || manualWarehouseSyncState.progress?.processed || 0),
          total: Number(result?.warehouse?.total || manualWarehouseSyncState.progress?.total || 0),
          updatedAt: new Date().toISOString(),
        },
      };
      return result;
    })
    .catch((error) => {
      const detail = error?.code || error?.message || String(error);
      manualWarehouseSyncState = {
        status: "failed",
        trigger,
        startedAt,
        finishedAt: new Date().toISOString(),
        result: null,
        error: detail,
        progress: {
          ...(manualWarehouseSyncState.progress || {}),
          percent: 100,
          stage: "Ошибка",
          meta: detail,
          updatedAt: new Date().toISOString(),
        },
      };
      logger.error("manual warehouse sync failed", { detail, err: error });
      throw error;
    })
    .finally(() => {
      manualWarehouseSyncPromise = null;
    });
  manualWarehouseSyncPromise.catch(() => {});
  return { started: true, status: getManualWarehouseSyncStatus() };
}

function scheduleAutoSync(delayMs = 10_000) {
  if (autoSyncTimer) clearTimeout(autoSyncTimer);
  autoSyncNextRunAt = new Date(Date.now() + delayMs).toISOString();
  autoSyncTimer = setTimeout(async () => {
    try {
      const settings = await readAppSettings();
      const config = settings.automation || defaultAppSettings().automation;
      if (config.autoSyncEnabled !== false) {
        await runAutoSyncCycle("interval");
      } else {
        logger.info("auto sync skipped: disabled in settings");
      }
      const nextMinutes = Math.max(5, Number(config.autoSyncMinutes || autoSyncMinutes || 30) || 30);
      scheduleAutoSync(nextMinutes * 60 * 1000);
    } catch (error) {
      logger.error("auto sync failed", { detail: error.code || error.message, err: error });
      scheduleAutoSync(Math.max(5, Number(autoSyncMinutes || 30) || 30) * 60 * 1000);
    }
  }, delayMs);
}

async function nextOzonUnarchiveQueueAutoDelayMs(fallbackMs = ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000) {
  try {
    const queue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 });
    if (queue.due > 0 && queue.availableToday > 0) return 1000;
    const nextMs = queue.nextRetryAt ? new Date(queue.nextRetryAt).getTime() - Date.now() : 0;
    if (Number.isFinite(nextMs) && nextMs > 0) return Math.min(fallbackMs, Math.max(30_000, nextMs));
  } catch (error) {
    logger.warn("ozon unarchive queue auto delay check failed", { detail: error?.message || String(error) });
  }
  return fallbackMs;
}

function scheduleOzonUnarchiveQueueAuto(delayMs = ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000) {
  if (!ozonUnarchiveQueueAutoEnabled) {
    ozonUnarchiveQueueAutoNextRunAt = null;
    logger.info("ozon unarchive queue auto disabled");
    return;
  }
  if (ozonUnarchiveQueueAutoTimer) clearTimeout(ozonUnarchiveQueueAutoTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs || 0) || ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000);
  ozonUnarchiveQueueAutoNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  ozonUnarchiveQueueAutoTimer = setTimeout(async () => {
    try {
      await queueMarketplaceJob(
        "ozon-unarchive-queue-process",
        {
          source: "ozon_unarchive_queue_auto",
          limit: ozonUnarchiveQueueBatchLimit,
          force: false,
        },
        { priority: 2 },
      );
    } catch (error) {
      logger.warn("ozon unarchive queue auto tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleOzonUnarchiveQueueAuto(await nextOzonUnarchiveQueueAutoDelayMs());
    }
  }, normalizedDelay);
}

app.post("/api/sync", async (_request, response, next) => {
  try {
    response.json(await runSync());
  } catch (error) {
    next(error);
  }
});

app.get("/api/daily-sync", async (_request, response, next) => {
  try {
    response.json(await getDailySyncStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/daily-sync/run", requireAdmin, async (_request, response, next) => {
  try {
    const alreadyRunning = Boolean(dailySyncPromise);
    if (!alreadyRunning) {
      runDailyRefresh("manual").catch((error) => {
        logger.error("manual daily sync background failed", { detail: error?.code || error?.message || String(error), err: error });
      });
    }
    const status = await getDailySyncStatus();
    response.status(202).json({
      ok: true,
      started: !alreadyRunning,
      running: true,
      status: status.status === "running" ? status : { ...status, status: "running", trigger: "manual" },
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/sync/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(getManualWarehouseSyncStatus());
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/sync/run", requireAdmin, async (_request, response, next) => {
  try {
    const result = startManualWarehouseSync("manual");
    response.status(202).json({
      ok: true,
      started: result.started,
      running: true,
      status: result.status,
    });
  } catch (error) {
    next(error);
  }
});

function requestErrorTitle(error, request) {
  if (error instanceof multer.MulterError) return "Не удалось загрузить изображение";
  const pathName = cleanText(request?.path).toLowerCase();
  const code = cleanText(error?.code).toLowerCase();
  if (
    pathName.includes("/ai/")
    || pathName.includes("/ai-")
    || pathName.includes("/settings/ai")
    || code.startsWith("openai")
  ) return "AI запрос не выполнен";
  if (pathName.includes("pricemaster")) return "Не удалось выполнить запрос к Price Master";
  if (pathName.includes("ozon") || pathName.includes("yandex") || pathName.includes("marketplace")) {
    return "Не удалось выполнить запрос к маркетплейсу";
  }
  return "Не удалось выполнить запрос";
}

app.use((error, request, response, _next) => {
  logger.error("request error", {
    path: request.path,
    method: request.method,
    detail: error.statusCode ? error.message : (error.code || error.message),
    code: error.code || null,
    matches: error.matches || undefined,
    err: error,
  });
  const uploadError = error instanceof multer.MulterError;
  response.status(uploadError ? 400 : error.statusCode || 500).json({
    error: requestErrorTitle(error, request),
    detail: error.statusCode ? error.message : (error.code || error.message),
    code: error.code || null,
    matches: error.matches || undefined,
    ozon: error.ozon,
  });
});

async function startServer() {
  initMarketplaceQueue();
  pruneUploadDirectory().catch((err) => logger.warn("initial upload prune failed", { detail: err?.message || String(err) }));
  app.listen(port, () => {
    logger.info("server started", {
      port,
      url: `http://localhost:${port}`,
      healthPath: "/health",
      trustProxyHops: trustProxyHops || 0,
    });
    logger.info("auto sync scheduler enabled", {
      defaultEveryMinutes: Math.max(5, Number(autoSyncMinutes || 30) || 30),
      initialDelaySeconds: autoSyncInitialDelaySeconds,
    });
    if (dailySyncEnabled) {
      logger.info("daily sync enabled", { time: dailySyncTime, sendPrices: dailySyncSendPrices });
    }
  });

  readWarehouse()
    .then((warehouse) => {
      logger.info("warehouse cache warmed", { products: warehouse.products.length, suppliers: warehouse.suppliers.length });
    })
    .catch((err) => {
      logger.warn("warehouse cache warm failed", { detail: err?.message || String(err) });
    });

  scheduleDailySync();
  schedulePriceRetryProcessing(30_000);
  scheduleOzonUnarchiveQueueAuto(ozonUnarchiveQueueAutoInitialDelaySeconds * 1000);
  scheduleSupplierCartAuto(180_000).catch((error) => logger.warn("supplier cart auto scheduler failed", { detail: error?.message || String(error) }));
  scheduleAutoSync(autoSyncInitialDelaySeconds * 1000);
}

async function shutdownForTests() {
  for (const timer of [
    dailySyncTimer,
    autoSyncTimer,
    ozonUnarchiveQueueAutoTimer,
    immediateAutoPushTimer,
    priceRetryTimer,
    supplierCartAutoTimer,
  ]) {
    if (timer) clearTimeout(timer);
  }
  dailySyncTimer = null;
  autoSyncTimer = null;
  ozonUnarchiveQueueAutoTimer = null;
  immediateAutoPushTimer = null;
  priceRetryTimer = null;
  supplierCartAutoTimer = null;
  await Promise.allSettled([
    marketplaceWorker?.close?.(),
    marketplaceQueue?.close?.(),
    pool?.end?.(),
    closePrisma(),
  ]);
  marketplaceWorker = null;
  marketplaceQueue = null;
}

module.exports = {
  app,
  startServer,
  shutdownForTests,
  collectHealthDetails,
  resolveMarkupCoefficient,
  resolveAvailabilityPolicy,
  normalizeManagedSupplier,
  normalizePriceMasterSnapshotItemForPostgres,
  resolvePriceMasterRowCurrency,
  normalizePriceMasterPrice,
  supplierImpactProductIds,
  priceMasterChangeImpactProductIds,
  warehouseProductAutomationFingerprint,
  changedWarehouseProductIdsByAutomationFingerprint,
  backgroundAutomationProductIds,
  pickNoSupplierAutomationCandidates,
  pickSupplierRecoveryCandidates,
  summarizeSupplierRecoveryProducts,
  runNoSupplierMarketplaceAutomation,
  runSupplierRecoveryAutomation,
  pickWarehouseSupplier,
  pickWarehouseStockOnlySupplier,
  resolveWarehouseBrand,
  warehouseBrandMatches,
  normalizeWarehouseProduct,
  mergeProducts,
  applyOzonInfoToWarehouseProduct,
  productFromPostgres,
  readWarehouse,
  writeWarehouse,
  marketplaceStateCodeFromPostgresRow,
  warehousePageProductMatches,
  warehousePagePostgresWhere,
  warehousePagePostgresPrimaryIdentityWhere,
  sortWarehouseProductsForSearch,
  preferWarehousePrimaryIdentityMatches,
  buildWarehouseSkuDiagnostics,
  addWarehousePageGroupSiblings,
  expandWarehouseProductsToGroups,
  syncWarehouseProductGroupLinks,
  buildWarehousePageProductGroups,
  linkedRecoveryCandidateProducts,
  summarizeWarehouseCounterStats,
  pickOzonDetailOfferIds,
  ozonProductNeedsDetailRefresh,
  isWeakOzonWarehouseProduct,
  pickWeakOzonProductIds,
  buildOzonStockPayloadItems,
  marketplaceHasPositiveStock,
  marketplaceOfferAutomationKey,
  shouldSendTargetStockForProduct,
  pickTargetStockSendProducts,
  priceAffectingSettingsChanged,
  warehouseLinkIdentityKey,
  warehouseLinkTargetKey,
  warehouseLinkSupplierSignature,
  productLinkPostgresIdentityKey,
  dedupeProductLinkRows,
  warehouseProductLinkDetailsSignature,
  mergeWarehouseLinkForSave,
  warehouseProductLinksSignature,
  warehouseGroupLinkSignature,
  buildCommonWarehouseGroupLinks,
  marketplacePriceBreakdown,
  productConflict,
  canIgnoreStaleLinkSaveConflict,
  warehouseLinkHasMatchTarget,
  pickOzonCabinetListedPrice,
  shouldSkipWarehousePriceSend,
  priceHistoryDedupeWindowMs,
  isDuplicatePriceHistoryEntry,
  buildOzonPricePayload,
  isOzonResourceExhaustedError,
  isOzonPerItemPriceLimitError,
  isOzonOldPriceLessError,
  isExpectedMarketplaceArchiveBlock,
  extractOzonPriceResponseFailures,
  buildPriceRetryItem,
  extractOzonYandexImportVolumesMl,
  ozonYandexImportBlockReasons,
  buildOzonYandexImportCandidate,
  summarizeOzonYandexImportPreview,
  productContentQuality,
  applyAiContentDraftToProduct,
  buildYandexOfferMapping,
  sendApprovedYandexProductContent,
  shouldPreferCompatibleOpenAiChatRequest,
  isCodexSaleAiProvider,
  effectiveOpenAiImageModel,
  openAiChatCompletionAttempts,
  isOpenAiBillingLimitError,
  isOpenAiRequestFormatError,
  getLocalYandexExportedOfferIdSet,
  buildYandexWarehouseProductFromOzonExport,
  materializeYandexExportedProductsForWarehouse,
  marketplaceProductMarkupOverride,
  applyYandexPriceSendToWarehouse,
  buildYandexPriceUpdateFromOzonProduct,
  sendYandexPricesFromOzonProducts,
  pickOzonProductStockForYandex,
  buildYandexStockUpdatePayload,
  buildYandexStockRestoreProducts,
  productLooksArchived,
  pickArchivedStockRestoreCandidates,
  parseYandexCampaignIds,
  yandexStockShops,
  summarizeApiErrorPayload,
  apiPayloadHasErrors,
  sendYandexStocksForExportedOzonProducts,
  parseProtectedBrandList,
  buildYandexCleanupCandidate,
  summarizeYandexCleanupPreview,
  sendYandexStocksFromOzonProducts,
  getYandexShopByTarget,
  priceRetryQueueKey,
  findActiveDelayedPriceRetry,
  appendPriceHistoryRows,
  readPriceHistory,
  readAuditFiltered,
  priceHistoryRowFromPostgres,
  readPriceRetryQueue,
  writePriceRetryQueue,
  priceRetryQueuePath,
  unarchiveProductsOnMarketplaces,
  processOzonUnarchiveQueue,
  readOzonUnarchiveQueue,
  writeOzonUnarchiveQueue,
  ozonUnarchiveQueuePath,
  ozonUnarchiveDateKey,
  ozonUnarchiveDailyUsed,
  nextOzonUnarchiveRetryAt,
  normalizeSupplierPickingRow,
  normalizeSupplierPickingState,
  readSupplierPickingState,
  writeSupplierPickingState,
  createSupplierPickingRows,
  supplierPickingInvoiceRows,
  supplierBlockKey,
  activeSupplierBlocksForOffer,
  normalizeSupplierTrustFactor,
  normalizeSupplierOrderCutoff,
  supplierOrderCutoffPassed,
  supplierCartOrderScore,
  readAiImageJobs,
  writeAiImageJobs,
  normalizeAiImageJob,
  publicAiImageJob,
  runAiImageGenerationJob,
  aiImageJobsPath,
};

if (require.main === module) {
  void startServer();
}
