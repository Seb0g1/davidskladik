const fs = require("fs/promises");
const fsSync = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const compression = require("compression");
const rateLimit = require("express-rate-limit");
const multer = require("multer");
const mysql = require("mysql2/promise");
const { Queue, Worker } = require("bullmq");
const ExcelJS = require("exceljs");
const { ProxyAgent: UndiciProxyAgent } = require("undici");
const OpenAI = require("openai");
const sharp = require("sharp");
const { toFile } = require("openai/uploads");
require("dotenv").config();

const logger = require("./lib/logger");
const {
  postgresModeEnabled,
  jsonFallbackEnabled,
  getPrisma,
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
const uploadImageDir = path.join(publicDir, "uploads", "images");
const aiImageDir = path.join(publicDir, "uploads", "ai-images");
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
const priceRetryQueuePath = path.join(dataDir, "price-retry-queue.json");
const ozonProductRulesPath = path.join(configDir, "ozon-product-rules.json");
const ozonProductRulesExamplePath = path.join(configDir, "ozon-product-rules.example.json");
const sessionCookieName = "pm_session";
const sessionTtlMs = 1000 * 60 * 60 * 12;
const autoSyncMinutes = Number(process.env.AUTO_SYNC_MINUTES || process.env.DEFAULT_AUTO_SYNC_MINUTES || 30);
const autoSyncInitialDelaySeconds = Math.max(30, Number(process.env.AUTO_SYNC_INITIAL_DELAY_SECONDS || 120) || 120);
const autoZeroStockOnNoSupplier = process.env.AUTO_ZERO_STOCK_ON_NO_SUPPLIER !== "false";
const autoArchiveOnNoLinks = process.env.AUTO_ARCHIVE_ON_NO_LINKS === "true";
const autoRestoreOnSupplierReturn = process.env.AUTO_RESTORE_ON_SUPPLIER_RETURN !== "false";
const bullmqEnabled = process.env.BULLMQ_ENABLED === "true";
const redisUrl = cleanText(process.env.REDIS_URL);
const bullmqWorkerConcurrency = Math.max(1, Math.min(4, Number(process.env.BULLMQ_WORKER_CONCURRENCY || 1) || 1));
const bullmqLockDurationMs = Math.max(60000, Number(process.env.BULLMQ_LOCK_DURATION_MS || 300000) || 300000);
const bullmqStalledIntervalMs = Math.max(30000, Number(process.env.BULLMQ_STALLED_INTERVAL_MS || 60000) || 60000);
const bullmqMaxStalledCount = Math.max(1, Number(process.env.BULLMQ_MAX_STALLED_COUNT || 1) || 1);
const dailySyncTime = process.env.DAILY_SYNC_TIME || "11:00";
const dailySyncEnabled = process.env.DAILY_SYNC_ENABLED !== "false";
const dailySyncSendPrices = process.env.DAILY_SYNC_SEND_PRICES !== "false";
const pmDbPoolSize = Math.max(1, Number(process.env.PM_DB_POOL_SIZE || 8) || 8);
const pmDbConnectTimeoutMs = Math.max(1000, Number(process.env.PM_DB_CONNECT_TIMEOUT_MS || 10000) || 10000);
const warehouseViewCacheMs = Math.max(1000, Number(process.env.WAREHOUSE_VIEW_CACHE_MS || 120000) || 120000);
const ozonWarehouseListEnabled = process.env.OZON_WAREHOUSE_LIST_ENABLED === "true";
const ozonBaseUrl = "https://api-seller.ozon.ru";
const yandexBaseUrl = "https://api.partner.market.yandex.ru";
const exchangeRateTtlMs = 6 * 60 * 60 * 1000;
const telegramBotToken = cleanText(process.env.TELEGRAM_BOT_TOKEN);
const telegramChatId = cleanText(process.env.TELEGRAM_CHAT_ID);
const telegramNotificationsEnabled = process.env.TELEGRAM_NOTIFICATIONS_ENABLED !== "false";
const telegramDailyReportEnabled = process.env.TELEGRAM_DAILY_REPORT_ENABLED !== "false";
const telegramDailyReportTime = process.env.TELEGRAM_DAILY_REPORT_TIME || "22:00";
const telegramProxyUrl = cleanText(process.env.TELEGRAM_PROXY_URL);
const telegramApiBaseUrl = cleanText(process.env.TELEGRAM_API_BASE_URL || "https://api.telegram.org");
const openaiImageModel = normalizeOpenAiImageModelName(process.env.OPENAI_IMAGE_MODEL || "gpt-image-2");
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
const ozonAiImageDefaultPrompt = cleanText(process.env.OZON_AI_IMAGE_PROMPT)
  || 'Сгенерируй продающее изображение для карточки товара на Ozon. Используй название товара: "{productName}". Сохрани узнаваемость товара с исходного фото, улучшив фон, свет, композицию и визуальную привлекательность для маркетплейса. Не добавляй логотипы, водяные знаки, недостоверные характеристики или лишний текст.';

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
};
let autoSyncTimer = null;
let autoSyncRunning = false;
let autoSyncNextRunAt = null;
let telegramDailyReportTimer = null;
let telegramDailyReportNextRunAt = null;
let warehouseWritePromise = Promise.resolve();
let warehouseMemoryCache = null;
let warehousePostgresHashCache = new Map();
let warehousePostgresUpdatedAtCache = new Map();
let warehousePostgresLinkBackfillPromise = null;
let warehousePostgresLinkBackfillDone = false;
let priceMasterSnapshotMemoryCache = null;
let priceMasterArticleIndexCache = null;
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
const changedPriceAutoPushAt = new Map();
let changedPriceAutoPushLastBatchAt = 0;
let priceRetryTimer = null;
let priceRetryRunning = false;
let marketplaceQueue = null;
let marketplaceWorker = null;
let telegramProxyDispatcher = null;

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

function telegramReady() {
  return telegramNotificationsEnabled && Boolean(telegramBotToken && telegramChatId);
}

function telegramApiUrl(method) {
  return `${telegramApiBaseUrl.replace(/\/+$/, "")}/bot${telegramBotToken}/${method}`;
}

function telegramFetchOptions(options = {}) {
  if (!telegramProxyUrl) return options;
  const url = new URL(telegramProxyUrl);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("TELEGRAM_PROXY_URL supports only HTTP/HTTPS proxy for Bot API. MTProto proxy is for Telegram clients, not api.telegram.org Bot API. Use HTTP proxy or TELEGRAM_API_BASE_URL gateway.");
  }
  if (!telegramProxyDispatcher) telegramProxyDispatcher = new UndiciProxyAgent(telegramProxyUrl);
  return { ...options, dispatcher: telegramProxyDispatcher };
}

function compactTelegramText(value, maxLength = 3500) {
  const text = cleanText(value);
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength - 20)}\n...`;
}

async function sendTelegramNotification(text, extra = {}) {
  if (!telegramReady()) return { ok: false, skipped: true };
  try {
    const response = await fetch(telegramApiUrl("sendMessage"), telegramFetchOptions({
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: telegramChatId,
        text: compactTelegramText(text),
        disable_web_page_preview: true,
        ...extra,
      }),
    }));
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.description || `Telegram API error ${response.status}`);
    }
    return { ok: true };
  } catch (error) {
    const detail = describeFetchError(error);
    logger.warn("telegram notification failed", {
      detail,
      proxyEnabled: Boolean(telegramProxyUrl),
      apiBaseUrl: telegramApiBaseUrl,
    });
    return { ok: false, error: detail };
  }
}

async function sendTelegramDocument({ buffer, filename, caption }) {
  if (!telegramReady()) return { ok: false, skipped: true };
  try {
    const form = new FormData();
    form.append("chat_id", telegramChatId);
    if (caption) form.append("caption", compactTelegramText(caption, 1000));
    form.append(
      "document",
      new Blob([buffer], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }),
      filename,
    );
    const response = await fetch(telegramApiUrl("sendDocument"), telegramFetchOptions({
      method: "POST",
      body: form,
    }));
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.ok === false) {
      const error = new Error(payload.description || `Telegram API error ${response.status}`);
      error.statusCode = response.status;
      error.telegram = payload;
      throw error;
    }
    return { ok: true };
  } catch (error) {
    const detail = describeFetchError(error);
    logger.warn("telegram document failed", {
      detail,
      statusCode: error?.statusCode,
      telegram: error?.telegram,
      proxyEnabled: Boolean(telegramProxyUrl),
      apiBaseUrl: telegramApiBaseUrl,
    });
    return {
      ok: false,
      error: detail,
      statusCode: error?.statusCode || null,
      telegram: error?.telegram || null,
    };
  }
}

function notifyTelegram(text, extra = {}) {
  sendTelegramNotification(text, extra).catch((error) => {
    logger.warn("telegram notification failed", { detail: describeFetchError(error) });
  });
}

function formatTelegramNumber(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return "0";
  return new Intl.NumberFormat("ru-RU").format(number);
}

function formatSyncNotification({ title, trigger, priceMaster, warehouse, automation, recovery, pricePush, error }) {
  const lines = [`${title}`];
  if (trigger) lines.push(`Запуск: ${trigger}`);
  if (priceMaster) lines.push(`PriceMaster: ${formatTelegramNumber(priceMaster.items || 0)} позиций, изменений ${formatTelegramNumber(priceMaster.changes || 0)}`);
  if (warehouse) {
    lines.push(`Склад: ${formatTelegramNumber(warehouse.total || 0)} товаров, готовы к цене ${formatTelegramNumber(warehouse.ready || 0)}, изменились ${formatTelegramNumber(warehouse.changed || 0)}`);
  }
  if (automation) {
    lines.push(`Автоматизация: stock=0 ${formatTelegramNumber(automation.zeroStockSent || 0)}, архив ${formatTelegramNumber(automation.archived || 0)}`);
  }
  if (recovery) lines.push(`Восстановлено: ${formatTelegramNumber(recovery.recovered || 0)}, разархивировано ${formatTelegramNumber(recovery.unarchived || 0)}`);
  if (pricePush) {
    const skippedCount = Array.isArray(pricePush.skipped) ? pricePush.skipped.length : Number(pricePush.skipped || 0);
    lines.push(`Цены: отправлено ${formatTelegramNumber(pricePush.sent || 0)}, ошибок ${formatTelegramNumber(pricePush.failed || 0)}, пропущено ${formatTelegramNumber(skippedCount || 0)}`);
    if (pricePush.error) lines.push(`Ошибка цен: ${pricePush.error}`);
  }
  if (error) lines.push(`Ошибка: ${error}`);
  return lines.join("\n");
}

function warehouseViewCacheKey({ sync = false, limit = Number.POSITIVE_INFINITY, usdRate, refreshPrices = false } = {}) {
  const limitKey = Number.isFinite(Number(limit)) ? Number(limit) : "all";
  const rateKey = Number.isFinite(Number(usdRate)) && Number(usdRate) > 0 ? Number(usdRate) : "default";
  return JSON.stringify({ sync: Boolean(sync), refreshPrices: Boolean(refreshPrices), limit: limitKey, usdRate: rateKey });
}

function invalidateWarehouseViewCache() {
  warehouseViewCache.clear();
  warehouseViewBuilds.clear();
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
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...readStoredAppUsersSync());
  return dedupeAppUsers(users).filter((user) => !user.disabled);
}

async function configuredUsersAsync() {
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...await readStoredAppUsers());
  return dedupeAppUsers(users).filter((user) => !user.disabled);
}

function normalizeAppRole(value, fallback = "manager") {
  return cleanText(value).toLowerCase() === "admin" ? "admin" : fallback;
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

async function readStoredAppUsersFromPostgres(prisma) {
  const rows = await prisma.appUser.findMany({
    where: {
      active: true,
      protected: false,
    },
    orderBy: [
      { role: "asc" },
      { username: "asc" },
    ],
  });
  return rows.map(appUserFromPostgres).filter((item) => item.username && item.password);
}

async function readStoredAppUsers() {
  return runWithPostgresFallback(
    "read app users",
    readStoredAppUsersFromPostgres,
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
              passwordHash: user.password,
              role: user.role === "admin" ? "admin" : "manager",
              active: !user.disabled,
              source: "postgres",
              protected: false,
              createdAt: toDateOrNull(user.createdAt) || new Date(),
              updatedAt: toDateOrNull(user.updatedAt) || new Date(),
            },
            update: {
              passwordHash: user.password,
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
  const payload = { updatedAt: new Date().toISOString(), users: normalized };
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
  const publicPaths = ["/login", "/login.html", "/styles.css", "/login.js", "/app.js", "/product.js", "/product-builder-ui.js", "/ozon-product.js", "/yandex-product.js", "/health"];
  if (publicPaths.includes(request.path)) return next();
  if (request.path.startsWith("/uploads/images/")) return next();
  if (request.path.startsWith("/uploads/ai-images/")) return next();
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

app.get("/health", (_request, response) => {
  response.json({ ok: true, service: "magic-vibes-warehouse", time: new Date().toISOString() });
});

app.post("/api/login", loginLimiter, async (request, response, next) => {
  const username = String(request.body.username || "");
  const password = String(request.body.password || "");
  let users;
  try {
    users = await configuredUsersAsync();
  } catch (error) {
    return next(error);
  }

  if (!users.length || users.every((item) => !item.password)) {
    return response.status(500).json({ error: "APP_PASSWORD или APP_USERS_JSON не задан в .env" });
  }

  const user = users.find((item) => timingSafeEqual(username, item.username) && timingSafeEqual(password, item.password));

  if (!user) {
    return response.status(401).json({ error: "Неверный логин или пароль" });
  }

  const token = createSessionToken(user);
  const secure = String(process.env.PUBLIC_BASE_URL || "").startsWith("https://");
  response.cookie(sessionCookieName, token, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    maxAge: sessionTtlMs,
    path: "/",
  });
  response.json({ ok: true, username: user.username, role: user.role });
});

app.post("/api/logout", (_request, response) => {
  response.clearCookie(sessionCookieName, { path: "/" });
  response.json({ ok: true });
});

app.get("/api/session", (request, response) => {
  const session = readSession(request);
  response.json({
    authenticated: Boolean(session),
    username: session?.username || null,
    role: session?.role || null,
    permissions: {
      admin: isAdminSession(session),
      settings: isAdminSession(session),
      priceMasterAudit: isAdminSession(session),
    },
  });
});

app.use(requireAuth);
app.use(express.static(publicDir));

function cleanLimit(value, fallback = 100, max = 500) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(parsed, max);
}

function uploadBaseUrl(request) {
  return String(process.env.PUBLIC_BASE_URL || `${request.protocol}://${request.get("host")}`).replace(/\/$/, "");
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
  if (parseBooleanSetting(process.env.OZON_PRICE_PUSH_SET_OLD_PRICE, true)) {
    const markupPct = Math.max(0, Number(process.env.OZON_OLD_PRICE_MARKUP_PCT || 20) || 20);
    const oldPrice = Math.max(price + 1, roundPrice(price * (1 + markupPct / 100)));
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

async function readAudit(limit = 200) {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().auditLog.findMany({
        take: limit,
        orderBy: { createdAt: "desc" },
      });
      return rows.map((row) => ({
        at: row.createdAt ? row.createdAt.toISOString() : null,
        user: row.username,
        action: row.action,
        productId: row.entityId || row.details?.productId || null,
        oldValue: row.oldValue,
        newValue: row.newValue,
        details: row.details || {},
      }));
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
      return rows.map((row) => ({
        at: row.createdAt ? row.createdAt.toISOString() : null,
        user: row.username,
        action: row.action,
        productId: row.entityId || row.details?.productId || null,
        oldValue: row.oldValue,
        newValue: row.newValue,
        details: row.details || {},
      }));
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
  if (targetId === "ozon") return accounts[0] || null;
  return accounts.find((account) => account.id === targetId) || null;
}

function getYandexShopByTarget(targetId) {
  const shops = getYandexShops();
  if (targetId === "yandex") return shops[0] || null;
  return shops.find((shop) => shop.id === targetId) || null;
}

function matchesOzonTarget(targetId, accountId) {
  const target = cleanText(targetId || "");
  return target === cleanText(accountId || "") || target === "ozon";
}

function matchesYandexTarget(targetId, shopId) {
  const target = cleanText(targetId || "");
  return target === cleanText(shopId || "") || target === "yandex";
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
  const accounts = marketplace === "yandex" ? getYandexShops() : getOzonAccounts();
  if (!accounts.length) return marketplace !== "yandex";
  return accounts.some((account) => (
    marketplace === "yandex"
      ? matchesYandexTarget(product.target, account.id)
      : matchesOzonTarget(product.target, account.id)
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

function resolveWarehouseBrand(product = {}) {
  return cleanText(
    product.brand ||
      product.vendor ||
      product.brandName ||
      product.ozon?.vendor ||
      product.ozon?.brand ||
      product.yandex?.vendor ||
      product.yandex?.brand ||
      extractBrandFromAttributes(product.ozon?.attributes),
  );
}

function warehouseBrandSearchHaystack(product = {}) {
  return [
    resolveWarehouseBrand(product),
    product.name,
    product.ozon?.name,
    product.yandex?.name,
    flattenAttributeText(product.ozon?.attributes),
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
    layout: cleanText(input.layout),
    model: cleanText(input.model),
    size: cleanText(input.size),
    quality: cleanText(input.quality),
    format: cleanText(input.format),
    createdAt: input.createdAt || input.created_at || new Date().toISOString(),
    reviewedAt: input.reviewedAt || input.reviewed_at || null,
  });
  return draft.resultUrl || draft.sourceImageUrl || draft.prompt ? draft : null;
}

function normalizeAiImageDrafts(input = []) {
  const drafts = Array.isArray(input) ? input : [];
  return drafts.map(normalizeAiImageDraft).filter(Boolean).slice(-50);
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

function normalizeWarehouseProduct(input = {}) {
  const target = cleanText(input.target || input.marketplace || "ozon");
  const inputMarketplace = cleanText(input.marketplace || input.marketplace_id || "").toLowerCase();
  const fallbackMarketplace = inputMarketplace === "yandex" || target === "yandex" ? "yandex" : "ozon";
  const targetMeta = targetById(target) || { id: target, marketplace: fallbackMarketplace, name: target };
  const ozonDraft = normalizeOzonDraft(input.ozon || input.ozonDraft || {});
  const yandexDraft = normalizeYandexDraft(input.yandex || input.yandexDraft || {});
  const imageUrl = firstImageUrl(input.imageUrl || input.image || input.primaryImage || ozonDraft.primaryImage || ozonDraft.images || yandexDraft.pictures);
  const name = cleanText(input.name || ozonDraft.name || yandexDraft.name || input.offerId || input.offer_id);
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
    name,
    keyword: cleanText(input.keyword),
    markup: Number(input.markup || 0),
    autoPriceEnabled: input.autoPriceEnabled !== undefined ? Boolean(input.autoPriceEnabled) : true,
    autoPriceMin: Number.isFinite(Number(input.autoPriceMin)) && Number(input.autoPriceMin) > 0 ? roundPrice(Number(input.autoPriceMin)) : null,
    autoPriceMax: Number.isFinite(Number(input.autoPriceMax)) && Number(input.autoPriceMax) > 0 ? roundPrice(Number(input.autoPriceMax)) : null,
    source: cleanText(input.source || (input.productId || input.product_id ? "marketplace" : "manual")),
    ozon: ozonDraft,
    yandex: yandexDraft,
    marketplaceState: normalizeMarketplaceState(input.marketplaceState || input.marketplace_state || input.ozonState),
    exports: normalizeProductExports(input.exports),
    aiImages: normalizeAiImageDrafts(input.aiImages || input.ai_images || input.imageDrafts),
    priceHistory: Array.isArray(input.priceHistory) ? input.priceHistory.slice(-100) : [],
    noSupplierAutomation: {
      stockZeroAt: input.noSupplierAutomation?.stockZeroAt || null,
      archivedAt: input.noSupplierAutomation?.archivedAt || null,
      recoveredAt: input.noSupplierAutomation?.recoveredAt || null,
      lastError: input.noSupplierAutomation?.lastError || null,
    },
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || new Date().toISOString(),
    links: Array.isArray(input.links) ? input.links.map(normalizeWarehouseLink) : [],
  };
}

function normalizeWarehouseLink(input = {}) {
  const priceCurrency = cleanText(input.priceCurrency || input.price_currency || input.currency).toUpperCase();
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article: cleanText(input.article || input.offerId || input.nativeId),
    keyword: cleanText(input.keyword),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
    priceCurrency: priceCurrency === "RUB" || priceCurrency === "RUR" ? "RUB" : "USD",
    priority: Number.isFinite(Number(input.priority)) ? Number(input.priority) : 100,
    createdAt: input.createdAt || new Date().toISOString(),
  };
}

function warehouseLinkIdentityKey(input = {}) {
  const link = normalizeWarehouseLink(input);
  return [
    link.article.toLowerCase(),
    link.partnerId,
    normalizeSupplierName(link.supplierName),
    link.keyword.toLowerCase(),
    link.priceCurrency,
  ].join("|");
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

function normalizeManagedSupplier(input = {}) {
  const inactiveUntil = cleanText(input.inactiveUntil || input.inactive_until);
  const stopped = Boolean(input.stopped);
  const priceCurrency = cleanText(input.priceCurrency || input.price_currency || input.currency || "USD").toUpperCase();
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual"),
    name: cleanText(input.name),
    priceCurrency: priceCurrency === "RUB" || priceCurrency === "RUR" ? "RUB" : "USD",
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
  const supplierName = normalizeSupplierName(supplier.name);
  const partnerId = cleanText(supplier.partnerId);
  const productIds = new Set();
  for (const product of warehouse.products || []) {
    for (const link of product.links || []) {
      const nameMatches = supplierName && normalizeSupplierName(link.supplierName) === supplierName;
      const idMatches = partnerId && String(link.partnerId || "") === partnerId;
      if (nameMatches || idMatches) productIds.add(product.id);
    }
  }
  return productIds.size;
}

function cloneAuditValue(value) {
  return value == null ? null : JSON.parse(JSON.stringify(value));
}

function productConflict(product, expectedUpdatedAt) {
  const expected = cleanText(expectedUpdatedAt || "");
  if (!expected) return null;
  if (cleanText(product?.updatedAt || "") === expected) return null;
  return {
    id: product.id,
    offerId: product.offerId || product.ozon?.offerId || product.yandex?.offerId || "",
    expectedUpdatedAt: expected,
    currentUpdatedAt: product.updatedAt || null,
  };
}

function productLocksFromRequest(body = {}) {
  const locks = new Map();
  if (body.expectedUpdatedAt && body.productId) locks.set(String(body.productId), cleanText(body.expectedUpdatedAt));
  if (body.expectedUpdatedAt && Array.isArray(body.productIds) && body.productIds.length === 1) {
    locks.set(String(body.productIds[0]), cleanText(body.expectedUpdatedAt));
  }
  for (const item of Array.isArray(body.optimisticLocks) ? body.optimisticLocks : []) {
    const id = cleanText(item?.id);
    if (id) locks.set(id, cleanText(item?.expectedUpdatedAt || ""));
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
  if (!mergeOnly || !conflicts.length) return conflicts;
  return [];
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

async function getUsdRate({ force = false } = {}) {
  const cached = await readCachedExchangeRate();
  if (!force && cached?.rate && Date.now() - new Date(cached.fetchedAt).getTime() < exchangeRateTtlMs) {
    return { ...cached, cached: true };
  }

  try {
    const response = await fetch("https://www.cbr-xml-daily.ru/daily_json.js");
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

function getOzonPriceBatchSize() {
  return Math.max(1, Math.min(100, Number(process.env.OZON_PRICE_BATCH_SIZE || 10) || 10));
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

  const response = await fetch(`${yandexBaseUrl}${pathname}`, {
    method,
    headers: {
      "Api-Key": shop.apiKey,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  const data = parseApiResponse(text);

  if (!response.ok) {
    const error = new Error(data.message || data.error || `Yandex Market API error ${response.status}`);
    error.statusCode = response.status;
    error.yandex = data;
    throw error;
  }

  return data;
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

async function getOzonProductInfoMap(offerIds, account = null) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);

  for (const chunk of chunkArray(ids, 100)) {
    const data = await ozonRequest("/v3/product/info/list", {
      offer_id: chunk,
    }, account);

    for (const item of data.items || data.result?.items || []) {
      const offerId = item.offer_id || item.offerId;
      if (offerId) map.set(offerId, item);
    }
  }

  return map;
}

async function getOzonStockMap(offerIds, account = null) {
  const map = new Map();
  const ids = offerIds.map((offerId) => String(offerId || "").trim()).filter(Boolean);

  for (const chunk of chunkArray(ids, 100)) {
    const data = await ozonRequest("/v4/product/info/stocks", {
      filter: { offer_id: chunk, visibility: "ALL" },
      limit: chunk.length,
    }, account);

    for (const item of data.items || data.result?.items || []) {
      const offerId = item.offer_id || item.offerId;
      if (!offerId) continue;
      const stocks = Array.isArray(item.stocks) ? item.stocks : [];
      const warehouses = stocks.map(normalizeOzonStockWarehouse).filter(Boolean);
      const present = warehouses.reduce((sum, stock) => sum + Number(stock.present || 0), 0);
      const reserved = warehouses.reduce((sum, stock) => sum + Number(stock.reserved || 0), 0);
      const total = Number.isFinite(Number(item.stock)) ? Number(item.stock) : Math.max(0, present - reserved);
      map.set(offerId, { ...item, present, reserved, stock: total, warehouses });
    }
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

async function getOzonPriceMap(offerIds, account = null) {
  const map = new Map();

  for (const chunk of chunkArray(offerIds, 100)) {
    const data = await ozonRequest("/v5/product/info/prices", {
      filter: { offer_id: chunk, visibility: "ALL" },
      limit: chunk.length,
    }, account);

    for (const item of data.items || []) {
      map.set(item.offer_id, item);
    }
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
    map.set(offerId, product);
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

function pickOzonDetailOfferIds(products = [], existingByOffer = new Map(), maxItems = 800) {
  const limit = Math.max(0, Number(maxItems || 0) || 0);
  if (!limit) return [];
  const prioritized = [];
  const seen = new Set();
  for (const product of products || []) {
    const offerId = cleanText(product.offer_id || product.offerId);
    if (!offerId || seen.has(offerId)) continue;
    const existing = existingByOffer.get(offerId);
    if (!existing || ozonProductNeedsDetailRefresh(existing)) {
      prioritized.push(offerId);
      seen.add(offerId);
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

  for (const chunk of chunkArray(offerIds, 100)) {
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings`,
      { offerIds: chunk },
    );

    for (const item of data.result?.offerMappings || data.result?.offers || data.offerMappings || []) {
      const offerId = item.offer?.offerId || item.offerId || item.mapping?.offerId;
      if (offerId) set.add(offerId);
    }
  }

  return set;
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

async function getYandexOfferMappings(shop, limit = Number.POSITIVE_INFINITY) {
  const maxItems = Number.isFinite(Number(limit)) && Number(limit) > 0 ? Number(limit) : Number.MAX_SAFE_INTEGER;
  const items = [];
  let pageToken = "";

  while (items.length < maxItems) {
    const pageLimit = Math.min(100, maxItems - items.length);
    const params = new URLSearchParams({ limit: String(pageLimit) });
    if (pageToken) params.set("pageToken", pageToken);

    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-mappings?${params.toString()}`,
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
    markupRules: [],
    availabilityRules: [
      { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
      { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
    ],
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
    markupRules: rules,
    availabilityRules,
  };
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
    attributesJson: overrides.attributesJson ?? ozon.attributes ?? [],
    complexAttributesJson: overrides.complexAttributesJson ?? ozon.complexAttributes ?? [],
    extraJson: overrides.extraJson ?? ozon.extra ?? {},
  };

  return buildOzonManualProductItem(body);
}

function buildOzonAiImagePrompt(product, promptOverride = "", options = {}) {
  const productName = cleanText(product?.name || product?.ozon?.name || product?.offerId || "товар");
  const template = cleanText(promptOverride) || ozonAiImageDefaultPrompt;
  const base = template.includes("{productName}")
    ? template.replaceAll("{productName}", productName)
    : `${template}\n\nНазвание товара: ${productName}`;
  const variantIndex = Number(options.variantIndex || 0);
  const variantTotal = Number(options.variantTotal || 0);
  if (!variantIndex || variantTotal <= 1) return base;
  const variantBriefs = [
    "Вариант 1: главный продающий слайд. Сделай крупный товар, сильный заголовок из названия, тип товара и объем, минимум лишнего текста.",
    "Вариант 2: слайд преимуществ. Сделай 2-3 аккуратных инфоблока с иконками: стиль, назначение, подарок/ежедневное использование, без медицинских обещаний.",
    "Вариант 3: слайд характера аромата. Если товар парфюмерный, покажи ноты/настроение/характер; если нет, покажи ключевые свойства из названия.",
    "Вариант 4: чистый premium marketplace packshot. Больше воздуха, бренд-зона с логотипом, короткий заголовок и один главный акцент.",
  ];
  return `${base}\n\n${variantBriefs[variantIndex - 1] || variantBriefs[0]}\nЭто вариант ${variantIndex} из ${variantTotal}; композиция должна заметно отличаться от других вариантов.`;
}

function isOpenAiRelayConfigured() {
  return Boolean(openaiRelayUrl && openaiRelaySecret);
}

function isOpenAiDirectConfigured() {
  return Boolean(cleanText(process.env.OPENAI_API_KEY));
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

function assertImageGenerationConfigured() {
  assertOpenAiRelayEnvPair();
  if (isOpenAiRelayConfigured() || isOpenAiDirectConfigured()) return;
  const error = new Error(
    "Генерация недоступна: задайте OPENAI_API_KEY на этом сервере или вынесите вызов OpenAI на VPS в поддерживаемом регионе и укажите OPENAI_RELAY_URL + OPENAI_RELAY_SECRET (см. scripts/openai-relay-server.cjs).",
  );
  error.statusCode = 400;
  error.code = "openai_not_configured";
  throw error;
}

function getOpenAiClient() {
  const apiKey = cleanText(process.env.OPENAI_API_KEY);
  if (!apiKey) {
    const error = new Error("OPENAI_API_KEY не задан для прямого вызова OpenAI.");
    error.statusCode = 400;
    error.code = "openai_api_key_missing";
    throw error;
  }
  const options = { apiKey };
  if (openaiBaseUrl) options.baseURL = openaiBaseUrl;
  return new OpenAI(options);
}

function imageBase64FromOpenAiImageEditResult(result) {
  const first = result?.data?.[0];
  if (!first) return "";
  if (typeof first.b64_json === "string" && first.b64_json.length) return first.b64_json;
  const url = typeof first.url === "string" ? first.url.trim() : "";
  if (!url) return "";
  const dataMatch = /^data:[^;]+;base64,([\s\S]+)$/i.exec(url);
  if (dataMatch) return dataMatch[1].replace(/\s+/g, "");
  return "";
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

function ozonAiImageStoredSizeLabel() {
  return ozonAiImageTargetPx ? `${ozonAiImageTargetPx}x${ozonAiImageTargetPx}` : openaiImageSize;
}

function normalizeOpenAiImageError(error) {
  const detail = cleanText(error?.message || error?.error?.message || error?.detail);
  if (/billing hard limit|hard limit has been reached|quota|insufficient_quota/i.test(detail)) {
    const billingError = new Error("Лимит биллинга OpenAI исчерпан на relay/API-ключе. Пополните баланс или увеличьте hard limit в OpenAI Billing, затем повторите генерацию.");
    billingError.statusCode = 402;
    billingError.code = "openai_billing_limit";
    return billingError;
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

async function generateOzonAiImageDraft(product, { prompt, sourceImageUrl, batchId, variantIndex = 1, variantTotal = 1 }, request) {
  const sourceUrl = cleanText(sourceImageUrl) || firstImageUrl(product.ozon?.primaryImage || product.ozon?.images || product.imageUrl);
  if (!sourceUrl) {
    const error = new Error("Укажите исходное фото товара перед генерацией AI-изображения.");
    error.statusCode = 400;
    error.code = "source_image_required";
    throw error;
  }

  assertImageGenerationConfigured();

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
    if (isOpenAiRelayConfigured()) {
      imageBase64 = await fetchOpenAiImageViaRelay({ prompt: generatedPrompt, sourceBuffer, sourceMimeType, referenceImages });
    } else {
      const client = getOpenAiClient();
      const image = [await toFile(sourceBuffer, sourceFileName, { type: sourceMimeType })];
      if (logoReference) {
        image.push(await toFile(logoReference.sourceBuffer, logoReference.sourceFileName, { type: logoReference.sourceMimeType }));
      }
      const editRequest = {
        model: openaiImageModel,
        image: image.length === 1 ? image[0] : image,
        prompt: generatedPrompt,
        size: openaiImageSize,
        quality: openaiImageQuality,
        output_format: openaiImageFormat,
      };
      if (openAiImageSupportsInputFidelity(openaiImageModel)) editRequest.input_fidelity = "high";
      if (openaiImageConfig && typeof openaiImageConfig === "object") {
        editRequest.image_config = JSON.stringify(openaiImageConfig);
      }
      const result = await client.images.edit(editRequest);
      imageBase64 = imageBase64FromOpenAiImageEditResult(result);
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
  outBuffer = await resizeOzonAiImageOutputBuffer(outBuffer, openaiImageFormat);

  await fs.mkdir(aiImageDir, { recursive: true });
  const extension = aiImageExtension(openaiImageFormat);
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
    model: openaiImageModel,
    size: ozonAiImageStoredSizeLabel(),
    quality: openaiImageQuality,
    format: openaiImageFormat,
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
  const price = Number(yandex.price || ozon.price || overrides.price || 0);
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

function normalizePriceMasterSnapshotItemForPostgres(row = {}, updatedAt = new Date()) {
  const article = cleanText(row.article || row.NativeID || row.nativeId);
  if (!article) return null;
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

async function getPriceMasterArticleIndex() {
  const snapshot = await readSnapshot();
  if (priceMasterArticleIndexCache?.syncId === snapshot.syncId && priceMasterArticleIndexCache?.createdAt === snapshot.createdAt) {
    return priceMasterArticleIndexCache.index;
  }
  const index = new Map();
  for (const row of Object.values(snapshot.items || {})) {
    const article = cleanText(row.article);
    if (!article) continue;
    if (!index.has(article)) index.set(article, []);
    index.get(article).push(row);
  }
  for (const rows of index.values()) {
    rows.sort((a, b) => new Date(b.docDate || 0) - new Date(a.docDate || 0) || Number(b.rowId || 0) - Number(a.rowId || 0));
  }
  priceMasterArticleIndexCache = {
    syncId: snapshot.syncId || null,
    createdAt: snapshot.createdAt || null,
    index,
  };
  return index;
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
  return status === "delayed" || item.retryReason === "ozon_per_item_price_limit" || isOzonPerItemPriceLimitError({ message: item.error });
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
  return {
    ...item,
    error: error?.message || item.error || "retry_failed",
    queueKey: priceRetryQueueKey(item),
    status: delayedByLimit ? "delayed" : "failed",
    queuedAt: item.queuedAt || now.toISOString(),
    lastAttemptAt: now.toISOString(),
    attempts,
    nextRetryAt,
    retryReason: delayedByLimit ? "ozon_per_item_price_limit" : "send_failed",
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
    const result = await getPrisma().priceHistory.createMany({
      data: normalizedRows,
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
  const identity = warehouseLinkIdentityKey(normalized);
  return {
    id: normalized.id || crypto.createHash("sha1").update(`${product.id}:${identity}`).digest("hex"),
    productId: product.id,
    supplierArticle: normalized.article,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    priceCurrency: normalized.priceCurrency === "RUB" ? "RUB" : "USD",
    keyword: normalized.keyword || null,
    raw: cloneAuditValue(normalized) || {},
    createdAt: toDateOrNull(normalized.createdAt) || new Date(),
    updatedAt: toDateOrNull(normalized.updatedAt) || new Date(),
  };
}

function productFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const postgresLinks = (row.links || []).map((link) => normalizeWarehouseLink({
    ...(link.raw && typeof link.raw === "object" ? link.raw : {}),
    id: link.id,
    article: link.supplierArticle,
    supplierName: link.supplierName,
    partnerId: link.partnerId,
    priceCurrency: link.priceCurrency,
    keyword: link.keyword,
    createdAt: link.createdAt ? link.createdAt.toISOString() : undefined,
  }));
  const rawLinks = Array.isArray(raw.links) ? raw.links.map(normalizeWarehouseLink) : [];
  const links = postgresLinks.length ? postgresLinks : rawLinks;
  return normalizeWarehouseProduct({
    ...raw,
    id: row.id,
    marketplace: row.marketplace,
    target: row.target || row.marketplace,
    offerId: row.offerId,
    productId: row.productId,
    name: row.name,
    brand: row.brand || raw.brand,
    marketplacePrice: row.currentPrice ?? raw.marketplacePrice,
    marketplaceState: row.marketplaceState || raw.marketplaceState,
    links,
    createdAt: row.createdAt ? row.createdAt.toISOString() : raw.createdAt,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : raw.updatedAt,
  });
}

async function ensureWarehousePostgresLinksBackfilled(prisma) {
  if (warehousePostgresLinkBackfillDone) return { created: 0, skipped: true };
  if (warehousePostgresLinkBackfillPromise) return warehousePostgresLinkBackfillPromise;
  warehousePostgresLinkBackfillPromise = (async () => {
    const [products, existingLinks] = await Promise.all([
      prisma.warehouseProduct.findMany({ select: { id: true, raw: true } }),
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

async function writeWarehouseToPostgres(prisma, payload) {
  const products = Array.isArray(payload.products) ? payload.products : [];
  const suppliers = Array.isArray(payload.suppliers) ? payload.suppliers : [];
  const changedProducts = products.filter((product) =>
    !warehousePostgresHashCache.has(product.id)
    || cleanText(product.updatedAt) !== warehousePostgresUpdatedAtCache.get(product.id)
  );
  if (changedProducts.length) {
    logger.info("warehouse postgres write delta", { products: changedProducts.length, suppliers: suppliers.length });
  }
  await prisma.$transaction(async (tx) => {
    for (const supplier of suppliers) {
      const data = supplierToPostgresData(supplier);
      await tx.managedSupplier.upsert({
        where: { partnerId: data.partnerId || data.name },
        create: data,
        update: {
          name: data.name,
          active: data.active,
          defaultCurrency: data.defaultCurrency,
          stopReason: data.stopReason,
          note: data.note,
          raw: data.raw,
        },
      });
    }
    for (const product of changedProducts) {
      const data = productToPostgresData(product);
      await tx.warehouseProduct.upsert({
        where: { id: data.id },
        create: data,
        update: {
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
        },
      });
      await tx.productLink.deleteMany({ where: { productId: product.id } });
      for (const link of product.links || []) {
        const linkData = linkToPostgresData(product, link);
        if (!linkData.supplierArticle) continue;
        await tx.productLink.upsert({
          where: { id: linkData.id },
          create: linkData,
          update: {
            supplierArticle: linkData.supplierArticle,
            supplierName: linkData.supplierName,
            partnerId: linkData.partnerId,
            priceCurrency: linkData.priceCurrency,
            keyword: linkData.keyword,
            raw: linkData.raw,
          },
        });
      }
    }
  }, { timeout: 60_000 });
  refreshWarehouseHashCache(payload);
}

async function readWarehouse() {
  if (warehouseMemoryCache) return warehouseMemoryCache;
  if (shouldUsePostgresStorage()) {
    try {
      const warehouse = await readWarehouseFromPostgres(getPrisma());
      if (warehouse) {
        warehouseMemoryCache = warehouse;
        return warehouseMemoryCache;
      }
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read warehouse postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const warehouse = JSON.parse(await fs.readFile(warehousePath, "utf8"));
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: warehouse.updatedAt || null,
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
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

async function writeWarehouse(warehouse) {
  invalidateWarehouseViewCache();
  warehouseWritePromise = warehouseWritePromise.then(async () => {
    await fs.mkdir(dataDir, { recursive: true });
    const payload = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
    warehouseMemoryCache = payload;
    if (shouldUsePostgresStorage()) {
      writeWarehouseToPostgres(getPrisma(), payload).catch((error) => {
        logger.warn("write warehouse postgres failed, keeping JSON fallback", { detail: error?.message || String(error) });
      });
    } else {
      refreshWarehouseHashCache(payload);
    }
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
    return payload;
  });
  return warehouseWritePromise;
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
    const preserveCurrentPrice = Boolean(current?.marketplacePrice && !importedNormalized.marketplacePrice);
    const preserveCurrentMinPrice = Boolean(current?.marketplaceMinPrice && !importedNormalized.marketplaceMinPrice);
    const merged = normalizeWarehouseProduct({
      ...current,
      ...importedNormalized,
      id: current?.id || importedNormalized.id,
      marketplacePrice: preserveCurrentPrice ? current.marketplacePrice : importedNormalized.marketplacePrice,
      marketplaceMinPrice: preserveCurrentMinPrice ? current.marketplaceMinPrice : importedNormalized.marketplaceMinPrice,
      marketplaceState: preserveCurrentState ? currentState : importedState,
      keyword: current?.keyword || importedNormalized.keyword,
      markup: current?.markup || importedNormalized.markup,
      autoPriceEnabled: current?.autoPriceEnabled !== undefined ? current.autoPriceEnabled : importedNormalized.autoPriceEnabled,
      autoPriceMin: current?.autoPriceMin ?? importedNormalized.autoPriceMin,
      autoPriceMax: current?.autoPriceMax ?? importedNormalized.autoPriceMax,
      links: Array.isArray(current?.links) ? current.links : [],
      createdAt: current?.createdAt || importedNormalized.createdAt,
    });
    const mergedKey = warehouseProductExactMergeKey(merged);
    map.set(mergedKey, merged);
    rememberLooseKeys(merged, mergedKey);
  }

  return Array.from(map.values()).sort((a, b) => a.targetName.localeCompare(b.targetName) || a.name.localeCompare(b.name));
}

async function importOzonWarehouseProducts(limit = Number.POSITIVE_INFINITY, existingProducts = []) {
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
        if (infoOfferIds.length) {
          [infoMap, stockMap, priceMap] = await Promise.all([
            getOzonProductInfoMap(infoOfferIds, account),
            getOzonStockMap(infoOfferIds, account),
            getOzonPriceMap(infoOfferIds, account),
          ]);
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
        const hasInfo = infoMap.has(product.offer_id);
        const hasStock = stockMap.has(product.offer_id);
        const hasPrice = priceMap.has(product.offer_id);
        const info = infoMap.get(product.offer_id) || {};
        const stockInfo = stockMap.get(product.offer_id) || {};
        const priceInfo = priceMap.get(product.offer_id) || {};
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

async function syncWarehouseProductsFromMarketplaces(warehouse, limit = Number.POSITIVE_INFINITY) {
  const warnings = [];
  let imported = [];
  try {
    const oz = await importOzonWarehouseProducts(limit, warehouse.products || []);
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
  if (article && current.toLowerCase() === article.toLowerCase()) return true;
  return /^[A-ZА-Я0-9._-]{4,}$/i.test(current) && !/\s/.test(current);
}

function applyOzonInfoToWarehouseProduct(product, info = {}, account = {}, stockInfo = {}, priceInfo = {}) {
  const sourceSku = info.sources?.find((source) => source.sku)?.sku;
  const sku = product.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku;
  const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image);
  const productUrl = info.product_url || info.url || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : product.productUrl);
  const betterName = cleanText(info.name);
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
    name: betterName && isWeakProductName(product.name, product.offerId) ? betterName : product.name || betterName,
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
      name: info.name || product.ozon?.name || product.name,
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

  if (updated.length) await writeWarehouse(warehouse);
  return updated;
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

function resolvePriceMasterRowCurrency(row = {}, link = {}, maps = managedSupplierMaps()) {
  const supplier = findManagedSupplierForPriceMasterRow(row, maps);
  return supplier?.priceCurrency || link.priceCurrency || "USD";
}

async function getPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article);
  if (!normalizedLinks.length) return new Map();

  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const supplierMaps = managedSupplierMaps(managedSuppliers);
  const rowsByArticle = await getPriceMasterArticleIndex();

  const map = new Map();
  for (const link of normalizedLinks) {
    const matches = (rowsByArticle.get(link.article) || [])
      .filter((row) => {
        const supplierOk =
          !link.supplierName ||
          normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
        const partnerOk = !link.partnerId || String(row.partnerId) === String(link.partnerId);
        const keywordOk = includesKeyword(row.name, link.keyword);
        return supplierOk && partnerOk && keywordOk;
      })
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
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
          active,
          stopped: Boolean(stoppedSupplier),
          stopReason: stoppedSupplier?.note || null,
          available: active && price > 0,
          docDate: row.docDate,
        };
      });
    map.set(link.id, matches);
  }

  return map;
}

async function findPriceMasterRowsForLink(linkInput, usdRate, managedSuppliers = []) {
  const link = normalizeWarehouseLink(linkInput);
  if (!link.article) return [];
  const supplierMaps = managedSupplierMaps(managedSuppliers);
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
    WHERE BINARY TRIM(r.NativeID) = BINARY ? AND r.Ignored = 0
    ORDER BY d.DocDate DESC, r.RowID DESC
    LIMIT 200
    `,
    [link.article],
  );
  return rows
    .filter((row) => {
      const supplierOk =
        !link.supplierName ||
        normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
      const partnerOk = !link.partnerId || String(row.partnerId) === String(link.partnerId);
      const keywordOk = includesKeyword(row.name, link.keyword);
      return supplierOk && partnerOk && keywordOk;
    })
    .map((row) => {
      const priceCurrency = resolvePriceMasterRowCurrency(row, link, supplierMaps);
      return {
        ...row,
        priceCurrency,
        ...normalizePriceMasterPrice(row.price, usdRate, priceCurrency),
        active: Boolean(row.active),
        ignored: Boolean(row.ignored),
      };
    });
}

async function getLivePriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article);
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
        active,
        stopped: Boolean(stoppedSupplier),
        stopReason: stoppedSupplier?.note || null,
        available: active && price > 0,
        docDate: row.docDate,
      };
    }));
  }
  return map;
}

async function getBatchPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate, { timeoutMs } = {}) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article);
  if (!normalizedLinks.length) return new Map();
  const articles = Array.from(new Set(normalizedLinks.map((link) => link.article))).slice(0, 500);
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
  const map = new Map();
  for (const link of normalizedLinks) {
    const matches = (rowsByArticle.get(link.article) || [])
      .filter((row) => {
        const supplierOk =
          !link.supplierName ||
          normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
        const partnerOk = !link.partnerId || String(row.partnerId) === String(link.partnerId);
        const keywordOk = includesKeyword(row.name, link.keyword);
        return supplierOk && partnerOk && keywordOk;
      })
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
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
          active,
          stopped: Boolean(stoppedSupplier),
          stopReason: stoppedSupplier?.note || null,
          available: active && price > 0,
          docDate: row.docDate,
        };
      });
    map.set(link.id, matches);
  }
  return map;
}

async function assertPriceMasterLinkExists(linkInput, usdRate, managedSuppliers = []) {
  const link = normalizeWarehouseLink(linkInput);
  const matches = await findPriceMasterRowsForLink(link, usdRate, managedSuppliers);
  if (matches.length) return matches;
  const articleRows = await findPriceMasterRowsForLink({ ...link, supplierName: "", partnerId: "", keyword: "" }, usdRate, managedSuppliers);
  const detailParts = [`артикул "${link.article}" должен совпадать с PriceMaster точно`];
  if (!articleRows.length) {
    detailParts.push("в PriceMaster нет строки с таким точным артикулом");
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

function pickWarehouseSupplier(matches) {
  return [...matches]
    .filter((match) => match.available)
    .sort(
      (a, b) =>
        Number(a.calculatedPrice || 0) - Number(b.calculatedPrice || 0)
        || Number(a.price || 0) - Number(b.price || 0)
        || String(b.docDate).localeCompare(String(a.docDate)),
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

async function buildWarehouseView({ sync = false, usdRate, targetMarkups = {}, limit = Number.POSITIVE_INFINITY, refreshPrices = false } = {}) {
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
  if (sync) {
    const synced = await syncWarehouseProductsFromMarketplaces(warehouse, limit);
    warehouse = synced.warehouse;
    syncWarnings = synced.warnings || [];
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
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const suppliers = normalizedLinks.flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => ({
        ...match,
        markupCoefficient: resolveMarkupCoefficient({
          productMarkup: product.markup,
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
            productMarkup: product.markup,
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
        missingInPriceMaster: matched.length === 0,
      };
    });
    const availableSupplierCount = suppliers.filter((supplier) => supplier.available).length;
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const baseMarkupCoefficient = Number(product.markup || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace: product.marketplace,
      availableSupplierCount,
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
          calculatedPrice: calculateRubPrice(selectedSupplier.price, rate, markupCoefficient),
        }
      : null;
    const rawNextPrice = selectedSupplierWithPolicy
      ? Number(selectedSupplierWithPolicy.calculatedPrice || calculateRubPrice(selectedSupplierWithPolicy.price, rate, markupCoefficient))
      : 0;
    const minAuto = Number(product.autoPriceMin || 0);
    const maxAuto = Number(product.autoPriceMax || 0);
    let nextPrice = rawNextPrice;
    if (nextPrice > 0 && minAuto > 0 && nextPrice < minAuto) nextPrice = minAuto;
    if (nextPrice > 0 && maxAuto > 0 && nextPrice > maxAuto) nextPrice = maxAuto;
    const currentPrice = priceMap.get(product.id) || null;
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;

    return {
      ...product,
      brand: resolveWarehouseBrand(product),
      markupCoefficient,
      autoPriceEnabled: normalizedLinks.length > 0 ? true : product.autoPriceEnabled !== false,
      autoPriceMin: minAuto > 0 ? minAuto : null,
      autoPriceMax: maxAuto > 0 ? maxAuto : null,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplierWithPolicy && nextPrice > 0),
      selectedSupplier: selectedSupplierWithPolicy,
      fallbackSuppliers: suppliers
        .filter((supplier) => supplier.available)
        .slice(0, 3)
        .map((supplier) => ({
          partnerName: supplier.partnerName || supplier.supplierName || "",
          article: supplier.article || "",
          price: supplier.price,
          calculatedPrice: supplier.calculatedPrice,
        })),
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной рассчитанной ценой."
        : "Нет доступного поставщика.",
      links,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount,
      availabilityRule: availabilityPolicy.rule,
      targetStock: selectedSupplierWithPolicy ? availabilityPolicy.targetStock : null,
      hasLinks: links.length > 0,
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : "no_supplier",
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

async function buildFreshWarehouseProductsForWarehouse(warehouse, productIds = [], { refreshPrices = false, persistMutations = false, livePriceMaster = true, batchPriceMaster = false, usdRate } = {}) {
  const wanted = new Set((productIds || []).map((id) => String(id)));
  if (!wanted.size) return [];
  const appSettings = await readAppSettings();
  const rateSource = appSettings.fixedUsdRate || usdRate || (batchPriceMaster ? process.env.DEFAULT_USD_RATE : (await getUsdRate()).rate);
  const rate = Number(rateSource || process.env.DEFAULT_USD_RATE || 95);
  const productsToBuild = (warehouse.products || []).filter((product) => wanted.has(String(product.id)));
  if (!productsToBuild.length) return [];
  const links = productsToBuild.flatMap((product) => product.links || []);
  const priceMasterTimeoutMs = Number(process.env.WAREHOUSE_PAGE_PM_TIMEOUT_MS || 1500);
  const matchMap = livePriceMaster
    ? (batchPriceMaster
        ? await Promise.race([
            getBatchPriceMasterMatchesForLinks(links, warehouse.suppliers, rate, { timeoutMs: priceMasterTimeoutMs }),
            promiseTimeout(priceMasterTimeoutMs + 100, "warehouse_page_pm_timeout"),
          ]).catch((error) => {
            logger.warn("warehouse page PriceMaster enrichment skipped", { detail: error?.message || String(error) });
            return new Map();
          })
        : await getLivePriceMasterMatchesForLinks(links, warehouse.suppliers, rate))
    : await getPriceMasterMatchesForLinks(links, warehouse.suppliers, rate);
  const [priceMapResult, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(productsToBuild, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(productsToBuild, { refresh: refreshPrices }),
  ]);
  if (persistMutations && (priceMapResult.mutated || minPriceResult.mutated)) await writeWarehouse(warehouse);
  const priceMap = priceMapResult.map;
  const minPriceMap = minPriceResult.map;

  return productsToBuild.map((product) => {
    const normalizedLinks = Array.isArray(product.links) ? product.links.map(normalizeWarehouseLink) : [];
    const suppliers = normalizedLinks.flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => {
        const markupCoefficient = resolveMarkupCoefficient({
          productMarkup: product.markup,
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
        missingInPriceMaster: matched.length === 0,
      };
    });
    const availableSupplierCount = suppliers.filter((supplier) => supplier.available).length;
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const baseMarkupCoefficient = Number(product.markup || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const availabilityPolicy = resolveAvailabilityPolicy({
      marketplace: product.marketplace,
      availableSupplierCount,
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
          calculatedPrice: calculateRubPrice(selectedSupplier.price, rate, markupCoefficient),
        }
      : null;
    const rawNextPrice = selectedSupplierWithPolicy
      ? Number(selectedSupplierWithPolicy.calculatedPrice || calculateRubPrice(selectedSupplierWithPolicy.price, rate, markupCoefficient))
      : 0;
    const minAuto = Number(product.autoPriceMin || 0);
    const maxAuto = Number(product.autoPriceMax || 0);
    let nextPrice = rawNextPrice;
    if (nextPrice > 0 && minAuto > 0 && nextPrice < minAuto) nextPrice = minAuto;
    if (nextPrice > 0 && maxAuto > 0 && nextPrice > maxAuto) nextPrice = maxAuto;
    const currentPrice = priceMap.get(product.id) || null;
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;

    return {
      ...product,
      brand: resolveWarehouseBrand(product),
      markupCoefficient,
      autoPriceEnabled: normalizedLinks.length > 0 ? true : product.autoPriceEnabled !== false,
      autoPriceMin: minAuto > 0 ? minAuto : null,
      autoPriceMax: maxAuto > 0 ? maxAuto : null,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplierWithPolicy && nextPrice > 0),
      selectedSupplier: selectedSupplierWithPolicy,
      fallbackSuppliers: suppliers
        .filter((supplier) => supplier.available)
        .slice(0, 3)
        .map((supplier) => ({
          partnerName: supplier.partnerName || supplier.supplierName || "",
          article: supplier.article || "",
          price: supplier.price,
          calculatedPrice: supplier.calculatedPrice,
        })),
      selectedSupplierReason: selectedSupplier
        ? "Выбран доступный поставщик с минимальной расчётной ценой."
        : "Нет доступного поставщика.",
      links,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount,
      availabilityRule: availabilityPolicy.rule,
      targetStock: selectedSupplierWithPolicy ? availabilityPolicy.targetStock : null,
      hasLinks: links.length > 0,
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : "no_supplier",
    };
  });
}

async function buildFreshWarehouseProducts(productIds = [], { refreshPrices = false } = {}) {
  const warehouse = await readWarehouse();
  return buildFreshWarehouseProductsForWarehouse(warehouse, productIds, { refreshPrices, persistMutations: true });
}

function warehousePageProductMatches(product = {}, filters = {}) {
  if (!isWarehouseProductTargetEnabled(product)) return false;
  const q = cleanText(filters.q || "").toLowerCase();
  if (q) {
    const haystack = [
      product.id,
      product.offerId,
      product.name,
      resolveWarehouseBrand(product),
      product.categoryName,
      product.sku,
      product.barcode,
    ]
      .map((value) => cleanText(value || "").toLowerCase())
      .join(" ");
    if (!haystack.includes(q)) return false;
  }
  const linked = cleanText(filters.linked || "all");
  const hasLinks = Array.isArray(product.links) && product.links.length > 0;
  if (linked === "linked" && !hasLinks) return false;
  if (linked === "unlinked" && hasLinks) return false;
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
  for (const shop of getYandexShops()) {
    or.push({
      marketplace: "yandex",
      OR: [
        { target: shop.id },
        { target: "yandex" },
      ],
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
  if (linked === "unlinked") and.push({ links: { none: {} } });
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all") and.push({ status: stateCode });
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter) and.push({ brand: { contains: brandFilter, mode: "insensitive" } });
  const q = cleanText(filters.q || "");
  if (q) {
    and.push({
      OR: [
        { id: { contains: q, mode: "insensitive" } },
        { offerId: { contains: q, mode: "insensitive" } },
        { productId: { contains: q, mode: "insensitive" } },
        { name: { contains: q, mode: "insensitive" } },
        { brand: { contains: q, mode: "insensitive" } },
      ],
    });
  }
  return { AND: and.filter((item) => Object.keys(item || {}).length) };
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

function marketplaceStateCodeFromPostgresRow(row = {}) {
  const state = row.marketplaceState && typeof row.marketplaceState === "object" && !Array.isArray(row.marketplaceState)
    ? row.marketplaceState
    : {};
  return cleanText(state.code || row.status || state.state).toLowerCase();
}

async function getOzonStateCountsFromPostgres(prisma) {
  const rows = await prisma.warehouseProduct.findMany({
    where: { AND: [enabledWarehouseTargetWhere(), { marketplace: "ozon" }] },
    select: { marketplaceState: true, status: true, archived: true },
  });
  let archived = 0;
  let inactive = 0;
  let outOfStock = 0;
  for (const row of rows) {
    const code = marketplaceStateCodeFromPostgresRow(row);
    if (row.archived || code === "archived") archived += 1;
    if (code === "inactive") inactive += 1;
    if (code === "out_of_stock") outOfStock += 1;
  }
  return { archived, inactive, outOfStock };
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
  const needsDeepBrandFilter = Boolean(cleanText(filters.brand || ""));
  const where = warehousePagePostgresWhere(needsDeepBrandFilter ? { ...filters, brand: "" } : filters);
  const offset = (page - 1) * pageSize;
  pageTrace("postgres:before-query", traceStartedAt);
  const [totalAll, dbTotal, withoutSupplier, ozonStateCounts, dbRows, suppliers] = await Promise.all([
    prisma.warehouseProduct.count({ where: enabledWarehouseTargetWhere() }),
    needsDeepBrandFilter ? Promise.resolve(0) : prisma.warehouseProduct.count({ where }),
    prisma.warehouseProduct.count({ where: { AND: [enabledWarehouseTargetWhere(), { links: { none: {} } }] } }),
    getOzonStateCountsFromPostgres(prisma),
    prisma.warehouseProduct.findMany({
      where,
      include: { links: true },
      orderBy: warehousePagePostgresOrderBy(),
      skip: needsDeepBrandFilter ? 0 : offset,
      take: needsDeepBrandFilter ? undefined : pageSize,
    }),
    prisma.managedSupplier.findMany({ orderBy: { name: "asc" } }),
  ]);
  pageTrace("postgres:after-query", traceStartedAt);
  const allProducts = needsDeepBrandFilter
    ? dbRows.map(productFromPostgres).filter((product) => warehousePageProductMatches(product, filters))
    : dbRows.map(productFromPostgres);
  const total = needsDeepBrandFilter ? allProducts.length : dbTotal;
  const pageProducts = needsDeepBrandFilter ? allProducts.slice(offset, offset + pageSize) : allProducts;
  const pageWarehouse = {
    createdAt: dbRows[0]?.createdAt?.toISOString() || null,
    updatedAt: dbRows[0]?.updatedAt?.toISOString() || null,
    products: pageProducts,
    suppliers: suppliers.map(supplierFromPostgres),
  };
  const built = await buildFreshWarehouseProductsForWarehouse(
    pageWarehouse,
    pageWarehouse.products.map((product) => product.id),
    { refreshPrices: false, persistMutations: false, livePriceMaster: true, batchPriceMaster: true, usdRate: rate },
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
    totalAll,
    ready: lastWarehouseViewSnapshot?.ready ?? items.filter((item) => item.ready).length,
    changed: lastWarehouseViewSnapshot?.changed ?? items.filter((item) => item.changed).length,
    withoutSupplier,
    ozonArchived: ozonStateCounts.archived,
    ozonInactive: ozonStateCounts.inactive,
    ozonOutOfStock: ozonStateCounts.outOfStock,
    usdRate: rate,
    priceMaster: await getPriceMasterSnapshotMetaFast(),
    sourceError: "",
    noSupplierAlerts: [],
    page,
    pageSize,
    total,
    hasMore: offset + items.length < total,
    items,
  };
}

async function buildFastWarehousePage({
  page = 1,
  pageSize = 60,
  usdRate,
  filters = {},
} = {}) {
  if (shouldUsePostgresStorage()) {
    const postgresPage = await buildFastWarehousePageFromPostgres({ page, pageSize, usdRate, filters });
    if (postgresPage) return postgresPage;
  }
  const warehouse = await readWarehouse();
  const appSettings = await readAppSettings();
  const rate = Number(appSettings.fixedUsdRate || usdRate || process.env.DEFAULT_USD_RATE || 95);
  const sourceProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
  const enabledProducts = sourceProducts.filter(isWarehouseProductTargetEnabled);
  const filtered = enabledProducts.filter((product) => warehousePageProductMatches(product, filters));
  const total = filtered.length;
  const offset = (page - 1) * pageSize;
  const pageProducts = filtered.slice(offset, offset + pageSize);
  const built = await buildFreshWarehouseProductsForWarehouse(
    { ...warehouse, products: pageProducts },
    pageProducts.map((product) => product.id),
    { livePriceMaster: true, batchPriceMaster: true, usdRate: rate },
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
  const pageReady = items.filter((item) => item.ready).length;
  const pageChanged = items.filter((item) => item.changed).length;
  return {
    createdAt: warehouse.createdAt || null,
    updatedAt: warehouse.updatedAt || null,
    totalAll: enabledProducts.length,
    ready: lastWarehouseViewSnapshot?.ready ?? pageReady,
    changed: lastWarehouseViewSnapshot?.changed ?? pageChanged,
    withoutSupplier: enabledProducts.filter((product) => !(product.links || []).length).length,
    ozonArchived: enabledProducts.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.archived).length,
    ozonInactive: enabledProducts.filter((product) => product.marketplace === "ozon" && /inactive/i.test(cleanText(product.marketplaceState?.code))).length,
    ozonOutOfStock: enabledProducts.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "out_of_stock").length,
    usdRate: rate,
    priceMaster: await getPriceMasterSnapshotMetaFast(),
    sourceError: "",
    noSupplierAlerts: [],
    page,
    pageSize,
    total,
    hasMore: offset + items.length < total,
    items,
  };
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

app.get("/api/health", async (_request, response, next) => {
  try {
    const [rows] = await pool.query("SELECT VERSION() AS version, NOW() AS serverTime");
    response.json({ ok: true, database: rows[0] });
  } catch (error) {
    next(error);
  }
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

app.get("/api/partners/search", async (request, response, next) => {
  try {
    const q = String(request.query.q || "").trim();
    const limit = cleanLimit(request.query.limit, 25, 80);
    if (!q) {
      return response.json({ items: [] });
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

    response.json({ items: rows });
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

    response.json(rows.map((row) => ({ ...row, ...normalizePriceMasterPrice(row.price, usdRate) })));
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

app.get("/api/audit-log", requireAdmin, async (request, response, next) => {
  try {
    response.json({ audit: await readAudit(cleanLimit(request.query.limit, 200, 1000)) });
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

app.get("/api/marketplaces", (_request, response) => {
  readAppSettings()
    .then((settings) => {
      response.json({
        defaults: {
          usdRate: Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95),
          ozonMarkup: Number(settings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7),
          yandexMarkup: Number(settings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
        },
        settings,
        targets: marketplaceTargets(),
        accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
        hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      });
    })
    .catch(() => {
      response.json({
        defaults: {
          usdRate: Number(process.env.DEFAULT_USD_RATE || 95),
          ozonMarkup: Number(process.env.DEFAULT_OZON_MARKUP || 1.7),
          yandexMarkup: Number(process.env.DEFAULT_YANDEX_MARKUP || 1.6),
        },
        settings: defaultAppSettings(),
        targets: marketplaceTargets(),
        accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
        hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      });
    });
});

app.get("/api/users", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ users: (await configuredUsersAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/users", requireAdmin, async (request, response, next) => {
  try {
    const user = normalizeAppUser(request.body || {}, { source: "local", defaultRole: "manager" });
    if (!user.username) return response.status(400).json({ error: "Укажите логин сотрудника." });
    if (!user.password || user.password.length < 6) return response.status(400).json({ error: "Укажите пароль сотрудника минимум 6 символов." });
    const exists = (await configuredUsersAsync()).some((item) => item.username.toLowerCase() === user.username.toLowerCase());
    if (exists) return response.status(409).json({ error: "Пользователь с таким логином уже существует." });
    const users = await readStoredAppUsers();
    users.push({ ...user, source: "local", protected: false, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
    await writeStoredAppUsers(users);
    appendAudit(request, "users.create", {
      username: user.username,
      role: user.role,
      oldValue: null,
      newValue: publicAppUser(user),
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.put("/api/users/:username", requireAdmin, async (request, response, next) => {
  try {
    const username = cleanText(request.params.username);
    const users = await readStoredAppUsers();
    const index = users.findIndex((item) => item.username.toLowerCase() === username.toLowerCase());
    if (index < 0) return response.status(404).json({ error: "Локальный сотрудник не найден. Пользователей из .env можно менять только в .env." });
    const before = publicAppUser(users[index]);
    const nextUser = {
      ...users[index],
      role: normalizeAppRole(request.body.role, users[index].role || "manager"),
      updatedAt: new Date().toISOString(),
    };
    if (request.body.password) {
      const password = cleanText(request.body.password);
      if (password.length < 6) return response.status(400).json({ error: "Пароль должен быть минимум 6 символов." });
      nextUser.password = password;
    }
    users[index] = nextUser;
    await writeStoredAppUsers(users);
    appendAudit(request, "users.update", {
      username,
      role: nextUser.role,
      oldValue: before,
      newValue: publicAppUser(nextUser),
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/users/:username", requireAdmin, async (request, response, next) => {
  try {
    const username = cleanText(request.params.username);
    const users = await readStoredAppUsers();
    const target = users.find((item) => item.username.toLowerCase() === username.toLowerCase());
    if (!target) return response.status(404).json({ error: "Локальный сотрудник не найден. Пользователей из .env удалить нельзя." });
    const remaining = users.filter((item) => item.username.toLowerCase() !== username.toLowerCase());
    await writeStoredAppUsers(remaining);
    appendAudit(request, "users.delete", {
      username,
      oldValue: publicAppUser(target),
      newValue: null,
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/settings", requireAdmin, async (_request, response, next) => {
  try {
    response.json({
      settings: await readAppSettings(),
      telegram: {
        configured: telegramReady(),
        enabled: telegramNotificationsEnabled,
        chatId: telegramChatId ? maskSecret(telegramChatId) : "",
        dailyReportEnabled: telegramDailyReportEnabled,
        dailyReportTime: telegramDailyReportTime,
        dailyReportNextRunAt: telegramDailyReportNextRunAt,
        proxyEnabled: Boolean(telegramProxyUrl),
        apiBaseUrl: telegramApiBaseUrl.replace(/bot.+$/i, "bot***"),
      },
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/telegram/test", requireAdmin, async (_request, response, next) => {
  try {
    if (!telegramReady()) {
      return response.status(400).json({
        ok: false,
        error: "TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID не заданы или уведомления выключены.",
      });
    }
    const result = await sendTelegramNotification("Тестовое уведомление Magic Vibes: Telegram подключен.");
    if (!result.ok) {
      return response.status(400).json({
        ok: false,
        error: result.error || "Telegram notification failed",
        hint: "Проверьте, что бот добавлен в чат, в чате отправлено любое сообщение после добавления бота, а TELEGRAM_CHAT_ID взят из getUpdates. Для ссылки вида web.telegram.org/k/#-3960374694 часто нужен chat_id -1003960374694.",
      });
    }
    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/api/telegram/daily-report/run", requireAdmin, async (_request, response, next) => {
  try {
    if (!telegramReady()) {
      return response.status(400).json({
        ok: false,
        error: "TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID не заданы или уведомления выключены.",
        hint: "Проверьте .env, путь запуска PM2 и выполните pm2 restart davidsklad --update-env.",
      });
    }
    if (!telegramDailyReportEnabled) {
      return response.status(400).json({
        ok: false,
        skipped: true,
        error: "Ежедневные Telegram-отчёты выключены.",
        hint: "Установите TELEGRAM_DAILY_REPORT_ENABLED=true в .env и перезапустите PM2 с --update-env.",
      });
    }
    const result = await sendDailyTelegramReport("manual");
    if (result && result.ok === false) {
      return response.status(400).json({
        ...result,
        error: result.error || "Telegram daily report failed",
        hint: "Проверьте TELEGRAM_CHAT_ID, что бот добавлен в чат и имеет право отправлять файлы. Для супергрупп chat_id часто начинается с -100.",
      });
    }
    response.json(result);
  } catch (error) {
    next(error);
  }
});

async function saveSettingsHandler(request, response, next) {
  try {
    const settings = await writeAppSettings(request.body || {});
    appendAudit(request, "settings.update", {
      fixedUsdRate: settings.fixedUsdRate,
      defaultMarkups: settings.defaultMarkups,
      markupRules: settings.markupRules.length,
      availabilityRules: settings.availabilityRules.length,
    }).catch((auditError) => {
      logger.warn("settings audit append failed", { detail: auditError?.message || String(auditError) });
    });
    try {
      queueImmediateAutoPricePush([], "settings_update");
    } catch (queueError) {
      logger.warn("settings auto price queue failed", { detail: queueError?.message || String(queueError) });
    }
    response.json({ ok: true, settings });
  } catch (error) {
    next(error);
  }
}

app.put("/api/settings", requireAdmin, saveSettingsHandler);
app.post("/api/settings", requireAdmin, saveSettingsHandler);

app.get("/api/marketplace-accounts", (_request, response) => {
  response.json({
    accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
    hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
    targets: marketplaceTargets(),
  });
});

app.post("/api/marketplace-accounts", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const input = normalizeMarketplaceAccount(request.body);
    if (!input.name) return response.status(400).json({ error: "Укажите название кабинета." });
    if (input.marketplace === "ozon" && (!input.clientId || !input.apiKey)) {
      return response.status(400).json({ error: "Для Ozon нужны Client-Id и Api-Key." });
    }
    if (input.marketplace === "yandex" && (!input.businessId || !input.apiKey)) {
      return response.status(400).json({ error: "Для Yandex нужны Business ID и Api-Key." });
    }

    const index = localAccounts.findIndex((account) => account.id === input.id);
    if (index >= 0) localAccounts[index] = normalizeMarketplaceAccount(input, localAccounts[index]);
    else localAccounts.push(input);

    await writeMarketplaceAccounts(localAccounts);
    await appendAudit(request, "marketplace_account.save", { id: input.id, marketplace: input.marketplace, name: input.name });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/marketplace-accounts/:id", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const index = localAccounts.findIndex((account) => account.id === request.params.id);
    const envAccount = [...getEnvOzonAccounts(), ...getEnvYandexShops()].find((account) => account.id === request.params.id);
    if (index < 0 && !envAccount) return response.status(404).json({ error: "Кабинет не найден." });

    if (index >= 0) {
      localAccounts[index] = normalizeMarketplaceAccount(
        accountPayloadWithSecretFallback({ ...localAccounts[index], ...request.body, id: request.params.id, hidden: false }, localAccounts[index]),
        localAccounts[index],
      );
    } else {
      localAccounts.push(
        normalizeMarketplaceAccount(
          accountPayloadWithSecretFallback({ ...envAccount, ...request.body, id: request.params.id, hidden: false }, envAccount),
          envAccount,
        ),
      );
    }

    await writeMarketplaceAccounts(localAccounts);
    await appendAudit(request, "marketplace_account.update", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/marketplace-accounts/:id", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const envAccount = [...getEnvOzonAccounts(), ...getEnvYandexShops()].find((account) => account.id === request.params.id);
    const index = localAccounts.findIndex((account) => account.id === request.params.id);
    let nextAccounts = localAccounts.filter((account) => account.id !== request.params.id);
    if (envAccount) {
      nextAccounts.push(normalizeMarketplaceAccount({ ...envAccount, hidden: true }, envAccount));
    } else if (index < 0) {
      return response.status(404).json({ error: "Кабинет не найден." });
    }
    await writeMarketplaceAccounts(nextAccounts);
    await appendAudit(request, envAccount ? "marketplace_account.hide" : "marketplace_account.delete", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplace-accounts/:id/restore", async (request, response, next) => {
  try {
    const localAccounts = await readMarketplaceAccounts();
    const nextAccounts = localAccounts.filter((account) => !(account.id === request.params.id && account.hidden));
    if (nextAccounts.length === localAccounts.length) return response.status(404).json({ error: "Скрытый кабинет не найден." });
    await writeMarketplaceAccounts(nextAccounts);
    await appendAudit(request, "marketplace_account.restore", { id: request.params.id });
    response.json({
      ok: true,
      accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
      targets: marketplaceTargets(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/exchange-rate", async (request, response, next) => {
  try {
    response.json(await getUsdRate({ force: request.query.force === "true" }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/uploads/images", uploadImages.array("images", 10), async (request, response, next) => {
  try {
    const files = Array.isArray(request.files) ? request.files : [];
    if (!files.length) return response.status(400).json({ error: "Выберите хотя бы одно изображение." });

    consumeUploadQuota(request, files.length);

    await fs.mkdir(uploadImageDir, { recursive: true });
    const saved = [];
    for (const file of files) {
      const extension = imageExtension(file);
      const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}${extension}`;
      const filePath = path.join(uploadImageDir, fileName);
      await fs.writeFile(filePath, file.buffer);
      const relativeUrl = `/uploads/images/${fileName}`;
      saved.push({
        originalName: file.originalname,
        size: file.size,
        mimeType: file.mimetype,
        path: relativeUrl,
        url: `${uploadBaseUrl(request)}${relativeUrl}`,
      });
    }

    response.json({ ok: true, files: saved });
    setImmediate(() => {
      pruneUploadDirectory().catch((err) => logger.warn("upload prune failed", { detail: err?.message || String(err) }));
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
    const warehouse = await readWarehouse();
    const unique = new Map();
    for (const product of warehouse.products || []) {
      const b = resolveWarehouseBrand(product);
      if (!b) continue;
      const key = b.toLowerCase();
      if (!unique.has(key)) unique.set(key, b);
    }
    const brands = Array.from(unique.values()).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
    response.json({ brands });
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
      return response.json(fastPage);
    }

    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    let rows = Array.isArray(data.products) ? data.products.slice() : [];
    if (!sync && !refreshPrices) queueChangedWarehousePrices(rows, "warehouse_page_detected_changed_prices");

    if (q) {
      rows = rows.filter((item) => {
        const haystack = [
          item.id,
          item.offerId,
          item.name,
          item.brand,
          item.categoryName,
          item.sku,
          item.barcode,
        ]
          .map((value) => cleanText(value || "").toLowerCase())
          .join(" ");
        return haystack.includes(q);
      });
    }
    if (autoOnly) rows = rows.filter((item) => item.autoPriceEnabled !== false);
    if (linked === "linked") rows = rows.filter((item) => item.hasLinks);
    if (linked === "unlinked") rows = rows.filter((item) => !item.hasLinks);
    if (marketplace !== "all") rows = rows.filter((item) => cleanText(item.marketplace) === marketplace);
    if (stateCode !== "all") rows = rows.filter((item) => cleanText(item.marketplaceState?.code) === stateCode);
    if (brandFilter) rows = rows.filter((item) => warehouseBrandMatches(item, brandFilter));

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

    response.json({
      createdAt: data.createdAt,
      updatedAt: data.updatedAt || null,
      totalAll: data.total,
      ready: data.ready,
      changed: data.changed,
      withoutSupplier: data.withoutSupplier,
      ozonArchived: data.ozonArchived || 0,
      ozonInactive: data.ozonInactive || 0,
      ozonOutOfStock: data.ozonOutOfStock || 0,
      usdRate: data.usdRate,
      priceMaster: data.priceMaster || await getPriceMasterSnapshotMeta(),
      sourceError: data.sourceError || "",
      noSupplierAlerts: Array.isArray(data.noSupplierAlerts) ? data.noSupplierAlerts.slice(0, 10) : [],
      page,
      pageSize,
      total,
      hasMore: offset + items.length < total,
      items,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/warehouse/products/:id/detail", async (request, response, next) => {
  try {
    const sync = request.query.sync === "true";
    const refreshPrices = request.query.refreshPrices === "true";
    const usdRate = request.query.usdRate ? Number(request.query.usdRate) : undefined;
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

app.get("/api/suppliers", async (_request, response, next) => {
  try {
    const warehouse = await readWarehouse();
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
        logger.info("imported suppliers from PriceMaster via suppliers api", { imported: syncedSuppliers.imported });
      }
    } catch (error) {
      supplierSync.error = error.message;
      logger.warn("supplier import from PriceMaster in /api/suppliers failed", { detail: error.message });
    }
    const autoReactivated = applySupplierAutoReactivate(warehouse);
    if (autoReactivated.length) {
      await writeWarehouse(warehouse);
      logger.info("supplier auto-reactivated from suppliers api", { count: autoReactivated.length, suppliers: autoReactivated });
    }
    response.json({
      suppliers: (warehouse.suppliers || []).map((supplier) => ({
        ...supplier,
        impactProductCount: supplierImpactCount(warehouse, supplier),
      })),
      supplierSync,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/live-status", async (_request, response, next) => {
  try {
    const [warehouse, dailySync, priceMaster] = await Promise.all([
      readWarehouse(),
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
        updatedAt: warehouse.updatedAt || warehouse.createdAt || null,
        createdAt: warehouse.createdAt || null,
        products: Array.isArray(warehouse.products) ? warehouse.products.length : 0,
        suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
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
      priceCurrency: request.body.priceCurrency !== undefined
        ? normalizeManagedSupplier({ priceCurrency: request.body.priceCurrency }).priceCurrency
        : (supplier.priceCurrency || "USD"),
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
    queueMarketplaceJob("no-supplier-automation", {}, { priority: 1 });
    queueMarketplaceJob("supplier-recovery-automation", {}, { priority: 2 });
    queueImmediateAutoPricePush([], "supplier_update");
  } catch (error) {
    next(error);
  }
});

app.delete("/api/suppliers/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const before = warehouse.suppliers.find((supplier) => supplier.id === request.params.id) || null;
    warehouse.suppliers = warehouse.suppliers.filter((supplier) => supplier.id !== request.params.id);
    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.delete", { id: request.params.id, oldValue: before });
    response.json({ ok: true, warehouse: saved });
    queueMarketplaceJob("no-supplier-automation", {}, { priority: 1 });
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
    const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
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

app.post("/api/warehouse/products/:id/ai-images/generate", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], updatedAt: product.updatedAt });

    const count = Math.min(4, Math.max(1, Math.floor(Number(request.body.count || request.body.imagesCount || 1) || 1)));
    const batchId = crypto.randomUUID();
    const drafts = [];
    for (let index = 1; index <= count; index += 1) {
      drafts.push(await generateOzonAiImageDraft(product, {
        prompt: request.body.prompt,
        sourceImageUrl: request.body.sourceImageUrl,
        batchId,
        variantIndex: index,
        variantTotal: count,
      }, request));
    }
    const draft = drafts[drafts.length - 1];
    product.aiImages = normalizeAiImageDrafts([...(product.aiImages || []), ...drafts]);
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouse(warehouse);
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, drafts, batchId, product: savedProduct });
    appendAudit(request, "warehouse.ai_image.generate", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      batchId,
      count,
      oldValue: before,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image generate audit failed", { detail: auditError?.message || String(auditError) }));
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
    const before = cloneAuditValue({ id: product.id, aiImages: product.aiImages || [], ozon: product.ozon || {}, imageUrl: product.imageUrl || "", updatedAt: product.updatedAt });

    product.aiImages = normalizeAiImageDrafts(product.aiImages || []);
    const draft = product.aiImages.find((item) => item.id === request.params.draftId);
    if (!draft) return response.status(404).json({ error: "AI-черновик изображения не найден." });
    if (!draft.resultUrl) return response.status(400).json({ error: "В AI-черновике нет URL результата." });

    const batchDrafts = draft.batchId
      ? product.aiImages.filter((item) => item.batchId === draft.batchId && item.resultUrl)
      : [draft];
    draft.status = "approved";
    draft.reviewedAt = new Date().toISOString();
    batchDrafts.forEach((item) => {
      if (item.status === "pending") {
        item.status = "approved";
        item.reviewedAt = draft.reviewedAt;
      }
    });
    const ozon = product.ozon || {};
    const images = splitList(ozon.images);
    const batchUrls = [draft.resultUrl, ...batchDrafts.map((item) => item.resultUrl).filter((url) => url && url !== draft.resultUrl)];
    product.ozon = normalizeOzonDraft({
      ...ozon,
      primaryImage: draft.resultUrl,
      images: [...batchUrls, ...images.filter((url) => !batchUrls.includes(url))],
    });
    product.imageUrl = draft.resultUrl;
    product.updatedAt = new Date().toISOString();

    const saved = await writeWarehouse(warehouse);
    const savedProduct = saved.products.find((item) => item.id === product.id) || normalizeWarehouseProduct(product);
    response.json({ ok: true, draft, product: savedProduct });
    appendAudit(request, "warehouse.ai_image.approve", {
      productId: product.id,
      offerId: product.offerId,
      draftId: draft.id,
      oldValue: before,
      newValue: { id: savedProduct.id, aiImages: savedProduct.aiImages || [], ozon: savedProduct.ozon || {}, imageUrl: savedProduct.imageUrl || "", updatedAt: savedProduct.updatedAt },
    }).catch((auditError) => logger.warn("ai image approve audit failed", { detail: auditError?.message || String(auditError) }));
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

    const saved = await writeWarehouse(warehouse);
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

    await writeWarehouse(warehouse);
    const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
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

    await writeWarehouse(warehouse);
    const products = await buildFreshWarehouseProducts(changedIds);
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

    await writeWarehouse(warehouse);
    const products = await buildFreshWarehouseProducts(changedIds);
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
    await writeWarehouse(warehouse);
    const products = await buildFreshWarehouseProducts(changedIds);
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
    await writeWarehouse(warehouse);
    const products = await buildFreshWarehouseProducts(changedIds);
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

    const rawLinks = Array.isArray(request.body.links) && request.body.links.length ? request.body.links : [request.body];
    const baseLinks = Array.from(new Map(rawLinks
      .map((link) => normalizeWarehouseLink(link))
      .filter((link) => link.article)
      .map((link) => [warehouseLinkIdentityKey(link), link])).values());
    const baseLink = baseLinks[0] || normalizeWarehouseLink({});
    if (!baseLink.article) return response.status(400).json({ error: "Укажите артикул PriceMaster." });
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    const warehouse = await readWarehouse();
    const targetProducts = warehouse.products.filter((product) => ids.has(String(product.id)));
    const conflicts = collectProductConflictsExceptBackground(targetProducts, productLocksFromRequest(request.body), { mergeOnly: true });
    if (conflicts.length) return conflictResponse(response, conflicts);
    for (const linkToValidate of baseLinks) {
      await assertPriceMasterLinkExists(linkToValidate, usdRate, warehouse.suppliers);
    }
    const now = new Date().toISOString();
    const updatedIds = [];
    const oldValues = [];

    for (const product of warehouse.products) {
      if (!ids.has(String(product.id))) continue;
      oldValues.push(cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt }));
      product.links = Array.isArray(product.links) ? product.links : [];
      for (const linkToSave of baseLinks) {
        const identityKey = warehouseLinkIdentityKey(linkToSave);
        const link = normalizeWarehouseLink({
          ...linkToSave,
          createdAt: linkToSave.createdAt || now,
        });
        const index = product.links.findIndex((item) => item.id === link.id || warehouseLinkIdentityKey(item) === identityKey);
        if (index >= 0) {
          product.links[index] = normalizeWarehouseLink({
            ...product.links[index],
            ...link,
            id: product.links[index].id || link.id,
            createdAt: product.links[index].createdAt || link.createdAt,
          });
        }
        else product.links.push(link);
      }
      product.autoPriceEnabled = true;
      product.updatedAt = now;
      updatedIds.push(product.id);
    }

    if (!updatedIds.length) return response.status(404).json({ error: "Товары склада не найдены." });

    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    await writeWarehouse(warehouse);
    const savedProducts = await buildFreshWarehouseProducts(updatedIds);
    response.json({ ok: true, changed: savedProducts.length || updatedIds.length, products: savedProducts, persisted: "written" });
    appendAudit(request, "warehouse.links.bulk_save", {
      productIds: updatedIds,
      links: baseLinks.map((link) => ({
        article: link.article,
        keyword: link.keyword,
        supplierName: link.supplierName,
        partnerId: link.partnerId,
        priceCurrency: link.priceCurrency,
      })),
      article: baseLink.article,
      keyword: baseLink.keyword,
      supplierName: baseLink.supplierName,
      priceCurrency: baseLink.priceCurrency,
      oldValue: oldValues,
      newValue: savedProducts.map((product) => ({ id: product.id, links: product.links || [], updatedAt: product.updatedAt })),
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("supplier-recovery-automation", { productIds: updatedIds }, { priority: 1 });
    queueImmediateAutoPricePush(updatedIds, "link_bulk_add_or_update");
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/links", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = null;
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });

    const link = normalizeWarehouseLink(request.body);
    if (!link.article) return response.status(400).json({ error: "Укажите артикул PriceMaster." });
    const settings = await readAppSettings();
    const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
    await assertPriceMasterLinkExists(link, usdRate, warehouse.suppliers);
    product.links = Array.isArray(product.links) ? product.links : [];
    const identityKey = warehouseLinkIdentityKey(link);
    const index = product.links.findIndex((item) => item.id === link.id || warehouseLinkIdentityKey(item) === identityKey);
    if (index >= 0) {
      product.links[index] = normalizeWarehouseLink({
        ...product.links[index],
        ...link,
        id: product.links[index].id || link.id,
        createdAt: product.links[index].createdAt || link.createdAt,
      });
    }
    else product.links.push(link);
    if (product.links.length > 0) product.autoPriceEnabled = true;
    product.updatedAt = new Date().toISOString();
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    await writeWarehouse(warehouse);
    const [savedProduct] = await buildFreshWarehouseProducts([product.id]);
    response.json({ ok: true, product: savedProduct || normalizeWarehouseProduct(product), links: (savedProduct || product).links || [], persisted: "written" });
    appendAudit(request, "warehouse.link.save", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      article: link.article,
      keyword: link.keyword,
      supplierName: link.supplierName,
      priceCurrency: link.priceCurrency,
      oldValue: before,
      newValue: { id: savedProduct?.id || product.id, links: (savedProduct || product).links || [], updatedAt: (savedProduct || product).updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    queueMarketplaceJob("supplier-recovery-automation", { productIds: [product.id] }, { priority: 1 });
    queueImmediateAutoPricePush([product.id], "link_add_or_update");
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:productId/links/:linkId", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.productId);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const before = cloneAuditValue({ id: product.id, links: product.links || [], updatedAt: product.updatedAt });
    const previousLinks = Array.isArray(product.links) ? product.links : [];
    const removed = previousLinks.some((link) => String(link.id) === String(request.params.linkId));
    if (!removed) {
      const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
      const responseProduct = freshProduct || normalizeWarehouseProduct(product);
      return response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "already_deleted", alreadyDeleted: true });
    }
    product.links = previousLinks.filter((link) => String(link.id) !== String(request.params.linkId));
    product.updatedAt = new Date().toISOString();
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    await writeWarehouse(warehouse);
    const [savedProduct] = await buildFreshWarehouseProducts([product.id]);
    const responseProduct = savedProduct || normalizeWarehouseProduct(product);
    response.json({ ok: true, product: responseProduct, links: responseProduct.links || [], persisted: "written" });
    appendAudit(request, "warehouse.link.delete", {
      productId: product.id,
      offerId: product.offerId,
      name: product.name,
      linkId: request.params.linkId,
      oldValue: before,
      newValue: { id: responseProduct.id, links: responseProduct.links || [], updatedAt: responseProduct.updatedAt },
    }).catch((auditError) => logger.warn("link audit append failed", { detail: auditError?.message || String(auditError) }));
    if ((responseProduct.links || []).length) queueImmediateAutoPricePush([request.params.productId], "link_delete");
  } catch (error) {
    next(error);
  }
});

async function sendWarehousePrices({ productIds, usdRate, minDiffRub = 0, minDiffPct = 0, dryRun = false } = {}) {
  const ids = Array.isArray(productIds) ? new Set(productIds.map(String)) : null;
  const forceSelectedPrices = Boolean(ids?.size);
  const preview = await buildWarehouseView({ usdRate: Number(usdRate || 0) || undefined });
  const selected = ids
    ? await buildFreshWarehouseProducts(Array.from(ids), { refreshPrices: true })
    : preview.products;
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
      skipped.push({ id: product.id, offerId: product.offerId, reason: "no_pricemaster_link" });
      continue;
    }
    const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
    const currentStock = Math.max(0, Math.round(Number(product.marketplaceState?.stock || 0)));
    if (targetStock !== currentStock) stockItems.push(product);
    if (!product.ready) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "not_ready" });
      continue;
    }
    const current = Number(product.currentPrice || 0);
    const nextValue = Number(product.nextPrice || 0);
    const diffRub = Math.abs(nextValue - current);
    if (!forceSelectedPrices && diffRub <= 0) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "unchanged" });
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

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      selected: selected.length,
      readyToSend: items.length,
      stockReadyToSend: stockItems.length,
      skipped,
      items,
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
  for (const item of items) {
    const product = warehouse.products.find((entry) => entry.id === item.id);
    if (!product) continue;
    const success = successIds.has(item.id);
    const failedEntryForItem = failed.find((entry) => entry.id === item.id);
    const delayedByLimitForItem = failedEntryForItem ? isOzonPerItemPriceLimitError({ message: failedEntryForItem.error }) : false;
    const sendStatus = success ? "success" : (delayedByLimitForItem ? "delayed" : "failed");
    if (success && item.marketplace !== "ozon") product.marketplacePrice = roundPrice(item.price);
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
      status: success ? "success" : (delayedByLimitForItem ? "delayed" : "error"),
      error: success ? null : (failedEntryForItem?.error || "send_failed"),
    };
    product.priceHistory.push(historyEntry);
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
    if (item.marketplace === "ozon") {
      product.lastOzonPriceSend = {
        status: failedEntryForItem ? (delayedByLimitForItem ? "delayed" : "error") : "success",
        at: sentAt,
        requestedPrice: roundPrice(item.price),
        cabinetPriceAtSend: Number(item.oldPrice || 0) || null,
        detail: failedEntryForItem ? failedEntryForItem.error : "ok",
        nextRetryAt: delayedByLimitForItem ? new Date(new Date(sentAt).getTime() + priceRetryDelayMs(Number(failedEntryForItem.attempts || 1), { message: failedEntryForItem.error })).toISOString() : null,
      };
    }
  }
  for (const action of stockActions) {
    if (!action.ok) continue;
    const product = warehouse.products.find((entry) => entry.id === action.id);
    if (!product) continue;
    product.marketplaceState = {
      ...(product.marketplaceState || {}),
      stock: Math.max(0, Math.round(Number(action.stock || 0))),
    };
  }
  await writeWarehouse(warehouse);
  appendPriceHistoryRows(postgresPriceHistoryRows).catch((error) => logger.warn("price history background append failed", { detail: error?.message || String(error) }));

  const failedQueued = failed.map((item) => buildPriceRetryItem({
    ...item,
    queueKey: `${item.id}:${item.target}`,
    queuedAt: sentAt,
  }, { message: item.error }, new Date(sentAt)));
  const merged = [...(queueState.items || []), ...delayedQueueUpdates, ...failedQueued];
  const deduped = Array.from(new Map(merged.map((item) => [priceRetryQueueKey(item), item])).values()).slice(0, 5000);
  if (failedQueued.length || delayedQueueUpdates.length) await writePriceRetryQueue({ items: deduped });
  if (failedQueued.length) schedulePriceRetryProcessing(priceRetryDelayMs(1));

  return {
    ok: true,
    sent: items.length - failed.length,
    failed: failed.length,
    stockSent: stockActions.filter((item) => item.ok).length,
    stockFailed: stockActions.filter((item) => !item.ok).length,
    queued: deduped.length,
    delayed: delayedQueueUpdates.length,
    skipped,
    results,
    stockActions,
  };
}

async function processMarketplaceJob(name, data = {}) {
  if (name === "auto-price-push") {
    return sendWarehousePrices({
      productIds: Array.isArray(data.productIds) ? data.productIds : undefined,
      usdRate: data.usdRate,
      minDiffRub: 0,
      minDiffPct: 0,
      dryRun: false,
    });
  }
  if (name === "no-supplier-automation") {
    const preview = await buildWarehouseView({ sync: true });
    return runNoSupplierMarketplaceAutomation(preview);
  }
  if (name === "supplier-recovery-automation") {
    const preview = await buildWarehouseView({ sync: false });
    return runSupplierRecoveryAutomation(preview, { productIds: data.productIds });
  }
  return null;
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

function queueImmediateAutoPricePush(productIds = [], reason = "price_change_detected") {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return;
  if (Array.isArray(productIds) && productIds.length) {
    productIds.forEach((id) => immediateAutoPushIds.add(String(id)));
  } else {
    immediateAutoPushAll = true;
  }
  if (immediateAutoPushTimer) return;
  immediateAutoPushTimer = setTimeout(() => {
    const ids = immediateAutoPushAll ? undefined : Array.from(immediateAutoPushIds);
    immediateAutoPushAll = false;
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
            reason,
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
            sent: result.sent,
            failed: result.failed,
            stockSent: result.stockSent,
            stockFailed: result.stockFailed,
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
  const now = Date.now();
  const cooldownMs = Math.max(1_000, Number(process.env.AUTO_PRICE_CHANGED_COOLDOWN_MS || 15_000) || 15_000);
  const batchCooldownMs = Math.max(1_000, Number(process.env.AUTO_PRICE_CHANGED_BATCH_COOLDOWN_MS || 5_000) || 5_000);
  if (changedPriceAutoPushLastBatchAt && now - changedPriceAutoPushLastBatchAt < batchCooldownMs) return 0;
  const ids = (Array.isArray(products) ? products : [])
    .filter((product) => {
      if (!product?.hasLinks) return false;
      if (product.changed && Number(product.nextPrice || 0) > 0) return true;
      const targetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
      const currentStock = Math.max(0, Math.round(Number(product.marketplaceState?.stock || 0)));
      return targetStock !== currentStock;
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
        historyRows.push({
          productId: entry.item.productId || entry.item.id,
          marketplace: "ozon",
          target: entry.item.target,
          offerId: entry.item.offerId,
          oldPrice: entry.item.oldPrice,
          newPrice: entry.item.price,
          status: error ? (delayed ? "delayed" : "failed") : "success",
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
    if (remaining.length) schedulePriceRetryProcessing();
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

app.post("/api/warehouse/prices/send", async (request, response, next) => {
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
    }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/prices/retry", async (request, response, next) => {
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

app.get("/api/warehouse/prices/retry-queue", async (_request, response, next) => {
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

app.get("/api/warehouse/prices/history", async (request, response, next) => {
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

app.delete("/api/warehouse/prices/retry-queue", async (_request, response, next) => {
  try {
    await writePriceRetryQueue({ items: [] });
    response.json({ ok: true });
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

    return {
      syncId,
      createdAt: snapshot.createdAt,
      items: Object.keys(currentItems).length,
      changes: changes.length,
      changeCounts: changes.reduce((acc, change) => {
        acc[change.type] = (acc[change.type] || 0) + 1;
        return acc;
      }, {}),
    };
  } finally {
    if (connection) connection.release();
  }
}

async function sendZeroStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) continue;
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
      if (!shop) continue;
      for (const chunk of chunkArray(items, 500)) {
        const payload = {
          offers: chunk.map((item) => ({
            offerId: String(item.offerId || "").trim(),
            stock: 0,
          })),
        };
        try {
          await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offers/stocks`, payload);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: true })));
        } catch (error) {
          const detail = error?.message || "stock_zero_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "zero_stock", ok: false, error: detail })));
        }
      }
    }
  }

  return actions;
}

async function sendTargetStocksToMarketplace(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    const stock = Math.max(0, Math.round(Number(product?.targetStock || 0)));
    if (!product?.id || !product?.offerId || !product?.target) continue;
    const key = `${product.marketplace}:${product.target}`;
    if (!byTarget.has(key)) byTarget.set(key, []);
    byTarget.get(key).push({ ...product, targetStock: stock });
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
      for (const chunk of chunkArray(items, 500)) {
        const payload = {
          offers: chunk.map((item) => ({
            offerId: String(item.offerId || "").trim(),
            stock: item.targetStock,
          })),
        };
        try {
          await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offers/stocks`, payload);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: true })));
        } catch (error) {
          const detail = error?.message || "target_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "target_stock", stock: item.targetStock, ok: false, error: detail })));
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
    if (!product?.id || !product?.target) continue;
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
        const productIds = chunk.map((item) => Number(item.productId || 0)).filter((id) => id > 0);
        if (!productIds.length) continue;
        try {
          await ozonRequest("/v1/product/archive", { product_id: productIds }, account);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: true })));
        } catch (error) {
          const detail = error?.message || "archive_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: false, error: detail })));
        }
      }
      continue;
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      for (const chunk of chunkArray(items, 500)) {
        try {
          await yandexRequest(
            shop,
            "POST",
            `/v2/businesses/${shop.businessId}/offer-mappings/update`,
            { offers: chunk.map((item) => ({ offerId: String(item.offerId || "").trim(), archived: true })) },
          );
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: true })));
        } catch (error) {
          const detail = error?.message || "archive_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "archive", ok: false, error: detail })));
        }
      }
    }
  }
  return actions;
}

async function restoreStocksOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.offerId || !product?.target) continue;
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
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: true })));
        } catch (error) {
          const detail = error?.message || "restore_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: false, error: detail })));
        }
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      for (const chunk of chunkArray(items, 500)) {
        const payload = {
          offers: chunk.map((item) => ({
            offerId: String(item.offerId || "").trim(),
            stock: Math.max(1, Math.round(Number(item.targetStock || item.marketplaceState?.stock || 1))),
          })),
        };
        try {
          await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offers/stocks`, payload);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: true })));
        } catch (error) {
          const detail = error?.message || "restore_stock_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "restore_stock", ok: false, error: detail })));
        }
      }
    }
  }
  return actions;
}

async function unarchiveProductsOnMarketplaces(products = []) {
  const actions = [];
  const byTarget = new Map();
  for (const product of products) {
    if (!product?.id || !product?.target) continue;
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
        const productIds = chunk.map((item) => Number(item.productId || 0)).filter((id) => id > 0);
        if (!productIds.length) continue;
        try {
          await ozonRequest("/v1/product/unarchive", { product_id: productIds }, account);
          actions.push(...chunk.map((item) => ({ id: item.id, type: "unarchive", ok: true })));
        } catch (error) {
          const detail = error?.message || "unarchive_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "unarchive", ok: false, error: detail })));
        }
      }
      continue;
    }
    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target);
      if (!shop) continue;
      for (const chunk of chunkArray(items, 500)) {
        try {
          await yandexRequest(
            shop,
            "POST",
            `/v2/businesses/${shop.businessId}/offer-mappings/update`,
            { offers: chunk.map((item) => ({ offerId: String(item.offerId || "").trim(), archived: false })) },
          );
          actions.push(...chunk.map((item) => ({ id: item.id, type: "unarchive", ok: true })));
        } catch (error) {
          const detail = error?.message || "unarchive_failed";
          actions.push(...chunk.map((item) => ({ id: item.id, type: "unarchive", ok: false, error: detail })));
        }
      }
    }
  }
  return actions;
}

function marketplaceHasPositiveStock(product = {}) {
  const state = product.marketplaceState || {};
  if (Number(state.stock || 0) > 0 || Number(state.present || 0) > 0) return true;
  return (Array.isArray(state.warehouses) ? state.warehouses : [])
    .some((warehouse) => Number(warehouse.stock || warehouse.present || 0) > 0);
}

function pickNoSupplierAutomationCandidates(products = []) {
  const list = Array.isArray(products) ? products : [];
  const linkedNoSupplier = list.filter((product) => product.hasLinks && !product.selectedSupplier);
  return {
    toZeroStock: autoZeroStockOnNoSupplier
      ? linkedNoSupplier.filter((product) => !product.noSupplierAutomation?.stockZeroAt || marketplaceHasPositiveStock(product))
      : [],
    toArchive: autoArchiveOnNoLinks
      ? linkedNoSupplier.filter(
          (product) =>
            !product.noSupplierAutomation?.archivedAt
            && product.marketplaceState?.code !== "archived",
        )
      : [],
  };
}

function pickSupplierRecoveryCandidates(products = [], { productIds } = {}) {
  const idSet = Array.isArray(productIds) && productIds.length
    ? new Set(productIds.map((id) => String(id || "").trim()).filter(Boolean))
    : null;
  return (Array.isArray(products) ? products : []).filter((product) => {
    if (idSet && !idSet.has(String(product.id))) return false;
    if (!product.hasLinks || !product.selectedSupplier) return false;
    if (product.noSupplierAutomation?.recoveredAt && !product.noSupplierAutomation?.stockZeroAt && product.marketplaceState?.code === "active") return false;
    return Boolean(product.noSupplierAutomation?.stockZeroAt)
      || Boolean(product.noSupplierAutomation?.archivedAt)
      || product.marketplaceState?.code === "archived"
      || product.marketplaceState?.code === "out_of_stock";
  });
}

async function runNoSupplierMarketplaceAutomation(preview) {
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const now = new Date().toISOString();
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates(products);

  if (!toZeroStock.length && !toArchive.length) {
    return { zeroStockSent: 0, archived: 0, errors: [] };
  }

  const stockActions = await sendZeroStocksToMarketplace(toZeroStock);
  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => item.id));
  const archiveMap = new Map();
  for (const product of toArchive) archiveMap.set(product.id, product);
  for (const product of toZeroStock) {
    if (stockOkIds.has(product.id)) archiveMap.set(product.id, product);
  }
  const archiveActions = await archiveProductsOnMarketplaces(Array.from(archiveMap.values()));
  const allActions = [...stockActions, ...archiveActions];
  if (!allActions.length) return { zeroStockSent: 0, archived: 0, errors: [] };

  const warehouse = await readWarehouse();
  for (const action of allActions) {
    const product = warehouse.products.find((item) => item.id === action.id);
    if (!product) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || { stockZeroAt: null, archivedAt: null, lastError: null };
    if (action.ok && action.type === "zero_stock") product.noSupplierAutomation.stockZeroAt = now;
    if (action.ok && action.type === "archive") product.noSupplierAutomation.archivedAt = now;
    product.noSupplierAutomation.lastError = action.ok ? null : action.error;
  }
  await writeWarehouse(warehouse);

  return {
    zeroStockSent: stockActions.filter((item) => item.ok).length,
    archived: archiveActions.filter((item) => item.ok).length,
    errors: allActions.filter((item) => !item.ok).map((item) => ({ id: item.id, type: item.type, error: item.error })),
  };
}

async function runSupplierRecoveryAutomation(preview, options = {}) {
  if (!autoRestoreOnSupplierReturn) {
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [] };
  }
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const recovered = pickSupplierRecoveryCandidates(products, options);
  if (!recovered.length) return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [] };
  const [stockActions, unarchiveActions] = await Promise.all([
    restoreStocksOnMarketplaces(recovered),
    unarchiveProductsOnMarketplaces(recovered),
  ]);
  const warehouse = await readWarehouse();
  const now = new Date().toISOString();
  for (const product of warehouse.products) {
    if (!recovered.some((item) => item.id === product.id)) continue;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    product.noSupplierAutomation.recoveredAt = now;
    product.noSupplierAutomation.stockZeroAt = null;
    product.noSupplierAutomation.archivedAt = null;
    product.noSupplierAutomation.lastError = null;
  }
  await writeWarehouse(warehouse);
  queueMarketplaceJob(
    "auto-price-push",
    {
      productIds: recovered.map((item) => item.id),
      usdRate: undefined,
      minDiffRub: 0,
      minDiffPct: 0,
    },
    { priority: 2 },
  );

  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, type: item.type, error: item.error }));
  return {
    recovered: recovered.length,
    restoredStocks: stockActions.filter((item) => item.ok).length,
    unarchived: unarchiveActions.filter((item) => item.ok).length,
    errors,
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

function isWithinDateRange(value, since, until = new Date()) {
  const time = new Date(value || 0).getTime();
  return Number.isFinite(time) && time >= since.getTime() && time <= until.getTime();
}

function dateStamp(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function reportProductRow(product = {}) {
  return {
    marketplace: product.marketplace || "",
    target: product.targetName || product.target || "",
    offerId: product.offerId || "",
    productId: product.productId || "",
    name: product.name || "",
    status: product.marketplaceState?.label || product.marketplaceState?.code || "",
    currentPrice: product.marketplacePrice || product.currentPrice || "",
    links: (product.links || []).map((link) => [link.article, link.keyword, link.supplierName].filter(Boolean).join(" / ")).join("; "),
  };
}

async function collectDailyReportData({ since = new Date(Date.now() - 24 * 60 * 60 * 1000), until = new Date() } = {}) {
  const warehouse = await readWarehouse();
  const audit = await readAuditSince(since);
  const products = warehouse.products || [];
  const productById = new Map(products.map((product) => [product.id, product]));

  const linkEvents = audit
    .filter((entry) => ["warehouse.link.save", "warehouse.links.bulk_save", "warehouse.link.delete"].includes(entry.action))
    .map((entry) => {
      const details = entry.details || {};
      return {
        at: entry.at,
        action: entry.action,
        user: entry.user || "",
        count: Array.isArray(details.productIds) ? details.productIds.length : 1,
        productId: details.productId || (Array.isArray(details.productIds) ? details.productIds.join(", ") : ""),
        offerId: details.offerId || "",
        name: details.name || "",
        article: details.article || "",
        keyword: details.keyword || "",
        supplierName: details.supplierName || "",
        priceCurrency: details.priceCurrency || "",
      };
    });

  const priceEvents = [];
  const supplierLost = [];
  const archiveEvents = [];
  const recoveryEvents = [];
  const errors = [];

  for (const product of products) {
    const base = reportProductRow(product);
    for (const history of product.priceHistory || []) {
      if (!isWithinDateRange(history.at, since, until)) continue;
      priceEvents.push({
        at: history.at,
        ...base,
        oldPrice: history.oldPrice || "",
        newPrice: history.newPrice || "",
        markup: history.markup || "",
        supplierName: history.supplierName || "",
        supplierArticle: history.supplierArticle || "",
        usdPrice: history.usdPrice || "",
        usdRate: history.usdRate || "",
        reason: history.reason || "",
        result: history.status || "",
        error: history.error || "",
      });
      if (history.status === "error") errors.push({ at: history.at, type: "price", ...base, error: history.error || "send_failed" });
    }

    const auto = product.noSupplierAutomation || {};
    if (isWithinDateRange(auto.stockZeroAt, since, until)) {
      supplierLost.push({
        at: auto.stockZeroAt,
        ...base,
        event: "stock_zero",
        selectedSupplier: product.selectedSupplier?.partnerName || product.selectedSupplier?.supplierName || "",
        lastError: auto.lastError || "",
      });
    }
    if (isWithinDateRange(auto.archivedAt, since, until)) {
      archiveEvents.push({ at: auto.archivedAt, ...base, event: "archived", lastError: auto.lastError || "" });
    }
    if (isWithinDateRange(auto.recoveredAt, since, until)) {
      recoveryEvents.push({
        at: auto.recoveredAt,
        ...base,
        event: "recovered",
        selectedSupplier: product.selectedSupplier?.partnerName || product.selectedSupplier?.supplierName || "",
      });
    }
  }

  return {
    since,
    until,
    totals: {
      products: products.length,
      linkedEvents: linkEvents.reduce((sum, item) => sum + Number(item.count || 1), 0),
      priceUpdated: priceEvents.filter((item) => item.result === "success").length,
      priceFailed: priceEvents.filter((item) => item.result === "error").length,
      suppliersLost: supplierLost.length,
      archived: archiveEvents.length,
      recovered: recoveryEvents.length,
      errors: errors.length,
    },
    linkEvents,
    priceEvents,
    supplierLost,
    archiveEvents,
    recoveryEvents,
    errors,
    productById,
  };
}

function addReportSheet(workbook, name, rows, columns) {
  const sheet = workbook.addWorksheet(name);
  sheet.columns = columns.map((column) => ({
    header: column.header,
    key: column.key,
    width: column.width || 18,
  }));
  sheet.getRow(1).font = { bold: true, color: { argb: "FFFFFFFF" } };
  sheet.getRow(1).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF172033" } };
  sheet.addRows(rows);
  sheet.views = [{ state: "frozen", ySplit: 1 }];
  sheet.autoFilter = {
    from: { row: 1, column: 1 },
    to: { row: Math.max(1, rows.length + 1), column: columns.length },
  };
  sheet.eachRow((row) => {
    row.alignment = { vertical: "top", wrapText: true };
  });
  return sheet;
}

async function buildDailyReportWorkbook(report) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = "Magic Vibes";
  workbook.created = new Date();
  workbook.modified = new Date();

  addReportSheet(
    workbook,
    "Сводка",
    [
      { metric: "Период с", value: report.since.toISOString() },
      { metric: "Период по", value: report.until.toISOString() },
      { metric: "Товаров в складе", value: report.totals.products },
      { metric: "Привязано/изменено привязок", value: report.totals.linkedEvents },
      { metric: "Цен успешно обновлено", value: report.totals.priceUpdated },
      { metric: "Ошибок обновления цен", value: report.totals.priceFailed },
      { metric: "Поставщиков пропало", value: report.totals.suppliersLost },
      { metric: "Товаров архивировано", value: report.totals.archived },
      { metric: "Товаров восстановлено", value: report.totals.recovered },
      { metric: "Ошибок", value: report.totals.errors },
    ],
    [
      { header: "Показатель", key: "metric", width: 34 },
      { header: "Значение", key: "value", width: 28 },
    ],
  );

  const productColumns = [
    { header: "Дата", key: "at", width: 22 },
    { header: "Маркетплейс", key: "marketplace", width: 14 },
    { header: "Кабинет", key: "target", width: 18 },
    { header: "Артикул", key: "offerId", width: 22 },
    { header: "Название", key: "name", width: 50 },
    { header: "Статус", key: "status", width: 22 },
  ];

  addReportSheet(workbook, "Привязки", report.linkEvents, [
    { header: "Дата", key: "at", width: 22 },
    { header: "Действие", key: "action", width: 24 },
    { header: "Пользователь", key: "user", width: 16 },
    { header: "Кол-во", key: "count", width: 10 },
    { header: "ID товара", key: "productId", width: 28 },
    { header: "Артикул MP", key: "offerId", width: 22 },
    { header: "Название", key: "name", width: 46 },
    { header: "Артикул PM", key: "article", width: 22 },
    { header: "Ключ", key: "keyword", width: 18 },
    { header: "Поставщик", key: "supplierName", width: 26 },
    { header: "Валюта", key: "priceCurrency", width: 10 },
  ]);

  addReportSheet(workbook, "Цены", report.priceEvents, [
    ...productColumns,
    { header: "Старая цена", key: "oldPrice", width: 14 },
    { header: "Новая цена", key: "newPrice", width: 14 },
    { header: "Наценка", key: "markup", width: 12 },
    { header: "Поставщик", key: "supplierName", width: 26 },
    { header: "Артикул поставщика", key: "supplierArticle", width: 24 },
    { header: "USD цена", key: "usdPrice", width: 12 },
    { header: "Курс", key: "usdRate", width: 12 },
    { header: "Причина", key: "reason", width: 34 },
    { header: "Результат", key: "result", width: 14 },
    { header: "Ошибка", key: "error", width: 34 },
  ]);

  addReportSheet(workbook, "Пропал поставщик", report.supplierLost, [
    ...productColumns,
    { header: "Событие", key: "event", width: 14 },
    { header: "Привязки", key: "links", width: 44 },
    { header: "Ошибка", key: "lastError", width: 34 },
  ]);
  addReportSheet(workbook, "Архив", report.archiveEvents, [...productColumns, { header: "Событие", key: "event", width: 14 }, { header: "Ошибка", key: "lastError", width: 34 }]);
  addReportSheet(workbook, "Восстановлено", report.recoveryEvents, [...productColumns, { header: "Событие", key: "event", width: 14 }, { header: "Поставщик", key: "selectedSupplier", width: 26 }]);
  addReportSheet(workbook, "Ошибки", report.errors, [...productColumns, { header: "Тип", key: "type", width: 14 }, { header: "Ошибка", key: "error", width: 40 }]);

  return workbook.xlsx.writeBuffer();
}

function dailyReportCaption(report) {
  return [
    `Ежедневный отчёт Magic Vibes за ${dateStamp(report.since)} - ${dateStamp(report.until)}`,
    `Привязок: ${formatTelegramNumber(report.totals.linkedEvents)}`,
    `Цен обновлено: ${formatTelegramNumber(report.totals.priceUpdated)}`,
    `Поставщиков пропало: ${formatTelegramNumber(report.totals.suppliersLost)}`,
    `Архивировано: ${formatTelegramNumber(report.totals.archived)}`,
    `Восстановлено: ${formatTelegramNumber(report.totals.recovered)}`,
    `Ошибок: ${formatTelegramNumber(report.totals.errors + report.totals.priceFailed)}`,
  ].join("\n");
}

async function sendDailyTelegramReport(trigger = "schedule") {
  if (!telegramDailyReportEnabled || !telegramReady()) return { ok: false, skipped: true };
  const until = new Date();
  const since = new Date(until.getTime() - 24 * 60 * 60 * 1000);
  const report = await collectDailyReportData({ since, until });
  const buffer = await buildDailyReportWorkbook(report);
  const filename = `magic-vibes-report-${dateStamp(until)}.xlsx`;
  const caption = dailyReportCaption(report);
  const result = await sendTelegramDocument({ buffer, filename, caption });
  logger.info("telegram daily report sent", { trigger, ok: result.ok, filename });
  return { ...result, filename, totals: report.totals };
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
      const automation = await runNoSupplierMarketplaceAutomation(warehouse);
      const recovery = await runSupplierRecoveryAutomation(warehouse);
      let pricePush = null;
      const shouldSendPrices = trigger === "manual" || (trigger === "schedule" && dailySyncSendPrices);
      if (shouldSendPrices) {
        try {
          pricePush = await sendWarehousePrices({
            usdRate: undefined,
            minDiffRub: 0,
            minDiffPct: 0,
            dryRun: false,
          });
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
          pricePush: pricePush
            ? {
                sent: Number(pricePush.sent || 0),
                failed: Number(pricePush.failed || 0),
                skipped: Array.isArray(pricePush.skipped) ? pricePush.skipped.length : 0,
                error: pricePush.error || null,
              }
            : null,
        },
      }));
      notifyTelegram(formatSyncNotification({
        title: trigger === "manual" ? "Ручной цикл завершён" : "Ежедневная синхронизация завершена",
        trigger,
        priceMaster,
        warehouse,
        automation,
        recovery,
        pricePush,
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
      notifyTelegram(formatSyncNotification({
        title: trigger === "manual" ? "Ручной цикл завершился ошибкой" : "Ежедневная синхронизация завершилась ошибкой",
        trigger,
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

function scheduleTelegramDailyReport() {
  if (!telegramDailyReportEnabled || !telegramReady()) return;
  if (telegramDailyReportTimer) clearTimeout(telegramDailyReportTimer);
  const delay = msUntilNextDailyRun(telegramDailyReportTime);
  telegramDailyReportNextRunAt = new Date(Date.now() + delay).toISOString();
  telegramDailyReportTimer = setTimeout(async () => {
    try {
      await sendDailyTelegramReport("schedule");
    } catch (error) {
      logger.warn("telegram daily report failed", { detail: error?.message || String(error) });
      notifyTelegram(`Ежедневный Telegram-отчёт не сформировался\nОшибка: ${error?.message || String(error)}`);
    } finally {
      scheduleTelegramDailyReport();
    }
  }, delay);
}

async function runAutoSyncCycle(trigger = "auto") {
  if (autoSyncRunning) return { status: "already_running" };
  autoSyncRunning = true;
  try {
    const result = await runSync();
    const warehouse = await buildWarehouseView({ sync: true });
    const automation = await runNoSupplierMarketplaceAutomation(warehouse);
    const recovery = await runSupplierRecoveryAutomation(warehouse);
    const autoPricePush = await processMarketplaceJob("auto-price-push", {
      usdRate: undefined,
      minDiffRub: 0,
      minDiffPct: 0,
    });
    logger.info("auto sync complete", {
      trigger,
      items: result.items,
      changes: result.changes,
      at: result.createdAt,
      warehouseTotal: warehouse.total,
      zeroStockSent: automation.zeroStockSent,
      autoArchived: automation.archived,
      recovered: recovery.recovered,
      autoPriceSent: autoPricePush.sent || 0,
      autoPriceFailed: autoPricePush.failed || 0,
      autoPriceSkipped: Array.isArray(autoPricePush.skipped) ? autoPricePush.skipped.length : 0,
    });
    if (automation.errors.length) {
      logger.warn("no-supplier automation errors", { count: automation.errors.length, sample: automation.errors.slice(0, 10) });
    }
    notifyTelegram(formatSyncNotification({
      title: "Фоновая синхронизация завершена",
      trigger,
      priceMaster: result,
      warehouse,
      automation,
      recovery,
      pricePush: autoPricePush,
    }));
    return { status: "ok", result, warehouse, automation, recovery, autoPricePush };
  } finally {
    autoSyncRunning = false;
  }
}

async function runManualWarehouseSync(trigger = "manual_sync") {
  const priceMaster = await runSync();
  const warehouse = await buildWarehouseView({ sync: true });
  const automation = await runNoSupplierMarketplaceAutomation(warehouse);
  const recovery = await runSupplierRecoveryAutomation(warehouse);
  notifyTelegram(formatSyncNotification({
    title: "Склад синхронизирован вручную",
    trigger,
    priceMaster,
    warehouse,
    automation,
    recovery,
  }));
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
      };
      notifyTelegram(formatSyncNotification({
        title: "Р СѓС‡РЅР°СЏ СЃРёРЅС…СЂРѕРЅРёР·Р°С†РёСЏ СЃРєР»Р°РґР° Р·Р°РІРµСЂС€РёР»Р°СЃСЊ РѕС€РёР±РєРѕР№",
        trigger,
        error: detail,
      }));
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
      notifyTelegram(formatSyncNotification({
        title: "Фоновая синхронизация завершилась ошибкой",
        trigger: "interval",
        error: error.code || error.message,
      }));
      scheduleAutoSync(Math.max(5, Number(autoSyncMinutes || 30) || 30) * 60 * 1000);
    }
  }, delayMs);
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
    notifyTelegram(formatSyncNotification({
      title: "Ручная синхронизация склада завершилась ошибкой",
      trigger: "manual",
      error: error.code || error.message,
    }));
    next(error);
  }
});

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
    error: uploadError ? "Не удалось загрузить изображение" : "Не удалось выполнить запрос к Price Master",
    detail: error.statusCode ? error.message : (error.code || error.message),
    code: error.code || null,
    matches: error.matches || undefined,
    ozon: error.ozon,
  });
});

function startServer() {
  initMarketplaceQueue();
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
    if (telegramReady()) {
      logger.info("telegram notifications enabled", {
        dailyReportEnabled: telegramDailyReportEnabled,
        dailyReportTime: telegramDailyReportTime,
        proxyEnabled: Boolean(telegramProxyUrl),
        apiBaseUrl: telegramApiBaseUrl,
      });
    }
    pruneUploadDirectory().catch((err) => logger.warn("initial upload prune failed", { detail: err?.message || String(err) }));
    readWarehouse()
      .then((warehouse) => logger.info("warehouse cache warmed", { products: warehouse.products.length, suppliers: warehouse.suppliers.length }))
      .catch((err) => logger.warn("warehouse cache warm failed", { detail: err?.message || String(err) }));
  });

  scheduleDailySync();
  scheduleTelegramDailyReport();
  schedulePriceRetryProcessing(30_000);
  scheduleAutoSync(autoSyncInitialDelaySeconds * 1000);
}

module.exports = {
  app,
  startServer,
  resolveMarkupCoefficient,
  resolveAvailabilityPolicy,
  normalizeManagedSupplier,
  normalizePriceMasterSnapshotItemForPostgres,
  resolvePriceMasterRowCurrency,
  normalizePriceMasterPrice,
  pickNoSupplierAutomationCandidates,
  pickSupplierRecoveryCandidates,
  pickWarehouseSupplier,
  resolveWarehouseBrand,
  warehouseBrandMatches,
  normalizeWarehouseProduct,
  mergeProducts,
  applyOzonInfoToWarehouseProduct,
  productFromPostgres,
  marketplaceStateCodeFromPostgresRow,
  pickOzonDetailOfferIds,
  ozonProductNeedsDetailRefresh,
  buildOzonStockPayloadItems,
  marketplaceHasPositiveStock,
  warehouseLinkIdentityKey,
  pickOzonCabinetListedPrice,
  buildOzonPricePayload,
  isOzonResourceExhaustedError,
  isOzonPerItemPriceLimitError,
  extractOzonPriceResponseFailures,
  buildPriceRetryItem,
  priceRetryQueueKey,
  findActiveDelayedPriceRetry,
  appendPriceHistoryRows,
  readPriceHistory,
  priceHistoryRowFromPostgres,
  readPriceRetryQueue,
  writePriceRetryQueue,
  priceRetryQueuePath,
};

if (require.main === module) {
  startServer();
}
