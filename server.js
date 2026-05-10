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
require("dotenv").config();

const logger = require("./lib/logger");

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
const snapshotPath = path.join(dataDir, "snapshot.json");
const historyPath = path.join(dataDir, "history.jsonl");
const exchangeRatePath = path.join(dataDir, "exchange-rate.json");
const warehousePath = path.join(dataDir, "personal-warehouse.json");
const dailySyncPath = path.join(dataDir, "daily-sync.json");
const marketplaceAccountsPath = path.join(dataDir, "marketplace-accounts.json");
const auditLogPath = path.join(dataDir, "audit-log.jsonl");
const appSettingsPath = path.join(dataDir, "app-settings.json");
const priceRetryQueuePath = path.join(dataDir, "price-retry-queue.json");
const ozonProductRulesPath = path.join(configDir, "ozon-product-rules.json");
const ozonProductRulesExamplePath = path.join(configDir, "ozon-product-rules.example.json");
const sessionCookieName = "pm_session";
const sessionTtlMs = 1000 * 60 * 60 * 12;
const autoSyncMinutes = Number(process.env.AUTO_SYNC_MINUTES || 0);
const autoZeroStockOnNoSupplier = process.env.AUTO_ZERO_STOCK_ON_NO_SUPPLIER !== "false";
const autoArchiveOnNoLinks = process.env.AUTO_ARCHIVE_ON_NO_LINKS === "true";
const autoRestoreOnSupplierReturn = process.env.AUTO_RESTORE_ON_SUPPLIER_RETURN !== "false";
const bullmqEnabled = process.env.BULLMQ_ENABLED === "true";
const redisUrl = cleanText(process.env.REDIS_URL);
const dailySyncTime = process.env.DAILY_SYNC_TIME || "11:00";
const dailySyncEnabled = process.env.DAILY_SYNC_ENABLED !== "false";
const dailySyncSendPrices = process.env.DAILY_SYNC_SEND_PRICES !== "false";
const pmDbPoolSize = Math.max(1, Number(process.env.PM_DB_POOL_SIZE || 8) || 8);
const pmDbConnectTimeoutMs = Math.max(1000, Number(process.env.PM_DB_CONNECT_TIMEOUT_MS || 10000) || 10000);
const warehouseViewCacheMs = Math.max(1000, Number(process.env.WAREHOUSE_VIEW_CACHE_MS || 120000) || 120000);
const ozonBaseUrl = "https://api-seller.ozon.ru";
const yandexBaseUrl = "https://api.partner.market.yandex.ru";
const exchangeRateTtlMs = 6 * 60 * 60 * 1000;

let dailySyncTimer = null;
let dailySyncNextRunAt = null;
let dailySyncPromise = null;
let warehouseWritePromise = Promise.resolve();
let warehouseMemoryCache = null;
const warehouseViewCache = new Map();
const warehouseViewBuilds = new Map();
let lastWarehouseViewSnapshot = null;
let immediateAutoPushTimer = null;
let immediateAutoPushAll = false;
const immediateAutoPushIds = new Set();
let immediateAutoPushChain = Promise.resolve();
let marketplaceQueue = null;
let marketplaceWorker = null;

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

function createSessionToken(username) {
  const payload = base64Url(
    JSON.stringify({
      username,
      role: process.env.APP_ROLE || "admin",
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
  if (request.path === "/api/login" || request.path === "/api/session") return next();

  const session = readSession(request);
  if (session) {
    request.session = session;
    return next();
  }

  if (request.path.startsWith("/api/")) {
    return response.status(401).json({ error: "Требуется вход" });
  }

  return response.redirect("/login.html");
}

app.get("/health", (_request, response) => {
  response.json({ ok: true, service: "magic-vibes-warehouse", time: new Date().toISOString() });
});

app.post("/api/login", loginLimiter, (request, response) => {
  const username = String(request.body.username || "");
  const password = String(request.body.password || "");
  const expectedUser = process.env.APP_USER || "admin";
  const expectedPassword = process.env.APP_PASSWORD || "";

  if (!expectedPassword) {
    return response.status(500).json({ error: "APP_PASSWORD не задан в .env" });
  }

  const userOk = timingSafeEqual(username, expectedUser);
  const passwordOk = timingSafeEqual(password, expectedPassword);

  if (!userOk || !passwordOk) {
    return response.status(401).json({ error: "Неверный логин или пароль" });
  }

  const token = createSessionToken(username);
  const secure = String(process.env.PUBLIC_BASE_URL || "").startsWith("https://");
  response.cookie(sessionCookieName, token, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    maxAge: sessionTtlMs,
    path: "/",
  });
  response.json({ ok: true, username });
});

app.post("/api/logout", (_request, response) => {
  response.clearCookie(sessionCookieName, { path: "/" });
  response.json({ ok: true });
});

app.get("/api/session", (request, response) => {
  const session = readSession(request);
  response.json({ authenticated: Boolean(session), username: session?.username || null, role: session?.role || null });
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
    details.currentPrice ||
    details.marketingSellerPrice ||
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
  await fs.mkdir(dataDir, { recursive: true });
  const entry = {
    at: new Date().toISOString(),
    user: request.session?.username || "system",
    role: request.session?.role || "admin",
    action,
    details,
  };
  await fs.appendFile(auditLogPath, `${JSON.stringify(entry)}\n`, "utf8");
}

async function readAudit(limit = 200) {
  try {
    const content = await fs.readFile(auditLogPath, "utf8");
    return content.trim().split("\n").filter(Boolean).slice(-limit).reverse().map((line) => JSON.parse(line));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function getOzonAccounts() {
  return getMarketplaceAccounts().filter((account) => account.marketplace === "ozon");
}

function getYandexShops() {
  return getMarketplaceAccounts().filter((account) => account.marketplace === "yandex");
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
      : [{ id: "yandex", marketplace: "yandex", name: "Yandex Market", configured: false }]),
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

function resolveWarehouseBrand(product = {}) {
  return cleanText(product.brand || product.ozon?.vendor || product.yandex?.vendor || "");
}

function firstImageUrl(value) {
  if (Array.isArray(value)) return cleanText(value[0]);
  const text = cleanText(value);
  if (!text) return "";
  return text.split(/\r?\n|,/).map(cleanText).find(Boolean) || "";
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

function normalizeMarketplaceState(input = {}) {
  if (!input || typeof input !== "object") {
    return { code: "unknown", label: "Статус не загружен" };
  }
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
    archived: input.archived !== undefined ? Boolean(input.archived) : undefined,
    hasStocks: input.hasStocks !== undefined ? Boolean(input.hasStocks) : undefined,
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
  const targetMeta = targetById(target) || { id: target, marketplace: target === "ozon" ? "ozon" : "yandex", name: target };
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
    priceHistory: Array.isArray(input.priceHistory) ? input.priceHistory.slice(-100) : [],
    noSupplierAutomation: {
      stockZeroAt: input.noSupplierAutomation?.stockZeroAt || null,
      archivedAt: input.noSupplierAutomation?.archivedAt || null,
      recoveredAt: input.noSupplierAutomation?.recoveredAt || null,
      lastError: input.noSupplierAutomation?.lastError || null,
    },
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
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
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    partnerId: cleanText(input.partnerId || input.partner_id),
    source: cleanText(input.source || "manual"),
    name: cleanText(input.name),
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

async function ozonRequest(pathname, body, account = null) {
  const selectedAccount = account || getOzonAccountByTarget("ozon");
  const clientId = selectedAccount?.clientId;
  const apiKey = selectedAccount?.apiKey;

  if (!clientId || !apiKey) {
    const error = new Error("Добавьте Client-Id и Api-Key Ozon в настройках кабинетов или в .env.");
    error.statusCode = 400;
    throw error;
  }

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
    const error = new Error(data.message || data.error || `Ozon API error ${response.status}`);
    error.statusCode = response.status;
    error.ozon = data;
    throw error;
  }

  return data;
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
  const items = [];
  let lastId = "";

  while (items.length < maxItems) {
    const batchLimit = Math.min(1000, maxItems - items.length);
    const data = await ozonRequest("/v3/product/list", {
      filter: { visibility: "ALL" },
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
      const present = stocks.reduce((sum, stock) => sum + Number(stock.present || 0), 0);
      const reserved = stocks.reduce((sum, stock) => sum + Number(stock.reserved || 0), 0);
      const total = Number.isFinite(Number(item.stock)) ? Number(item.stock) : Math.max(0, present - reserved);
      map.set(offerId, { ...item, present, reserved, stock: total });
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
  const stock = Number.isFinite(Number(stockInfo.stock)) ? Number(stockInfo.stock) : Math.max(0, present - reserved);
  const hasStocks = Boolean(product.has_fbs_stocks || product.hasFbsStocks || stock > 0);

  if (archived) {
    return normalizeMarketplaceState({ code: "archived", label: "В архиве Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, archived, hasStocks });
  }
  if (visibility === "EMPTY_STOCK" || (!hasStocks && stock <= 0)) {
    return normalizeMarketplaceState({ code: "out_of_stock", label: "Нет в наличии Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, archived, hasStocks });
  }
  if (["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(visibility)
    || ["INVISIBLE", "DISABLED", "REMOVED_FROM_SALE", "BANNED", "NOT_MODERATED", "STATE_FAILED", "MODERATION_BLOCK"].includes(state)) {
    return normalizeMarketplaceState({ code: "inactive", label: "Неактивен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, archived, hasStocks });
  }
  if (visibility || state || hasStocks) {
    return normalizeMarketplaceState({ code: "active", label: "Активен Ozon", visibility, state, stateName, stateDescription, stock, present, reserved, archived, hasStocks });
  }
  return normalizeMarketplaceState({ code: "unknown", label: "Статус Ozon не загружен", visibility, state, stateName, stateDescription, stock, present, reserved, archived, hasStocks });
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
    markupRules: [],
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

function normalizeAppSettings(input = {}) {
  const fallback = defaultAppSettings();
  const fixedUsdRate = Number(input.fixedUsdRate ?? input.fixed_usd_rate ?? fallback.fixedUsdRate);
  const defaultMarkups = {
    ozon: Number(input.defaultMarkups?.ozon ?? input.default_ozon_markup ?? fallback.defaultMarkups.ozon),
    yandex: Number(input.defaultMarkups?.yandex ?? input.default_yandex_markup ?? fallback.defaultMarkups.yandex),
  };
  const rules = Array.isArray(input.markupRules)
    ? input.markupRules.map(normalizeMarkupRule).filter(Boolean)
    : [];
  rules.sort((a, b) => a.minUsd - b.minUsd);
  return {
    fixedUsdRate: Number.isFinite(fixedUsdRate) && fixedUsdRate > 0 ? fixedUsdRate : fallback.fixedUsdRate,
    defaultMarkups: {
      ozon: Number.isFinite(defaultMarkups.ozon) && defaultMarkups.ozon > 0 ? defaultMarkups.ozon : fallback.defaultMarkups.ozon,
      yandex: Number.isFinite(defaultMarkups.yandex) && defaultMarkups.yandex > 0 ? defaultMarkups.yandex : fallback.defaultMarkups.yandex,
    },
    markupRules: rules,
  };
}

async function readAppSettings() {
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

async function listPriceMasterPartners() {
  const [rows] = await pool.query(`
    SELECT DISTINCT
      p.PartnerID AS partnerId,
      p.PartnerName AS name
    FROM Partners p
    JOIN OfferDocs d ON d.PartnerID = p.PartnerID
    WHERE p.PartnerName IS NOT NULL AND TRIM(p.PartnerName) <> ''
    ORDER BY p.PartnerName
  `);
  return rows
    .map((row) => ({
      partnerId: cleanText(row.partnerId),
      name: cleanText(row.name),
    }))
    .filter((row) => row.name);
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
  try {
    return JSON.parse(await fs.readFile(snapshotPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return { createdAt: null, items: {}, changes: [] };
    }
    throw error;
  }
}

async function writeSnapshot(snapshot) {
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(snapshotPath, JSON.stringify(snapshot, null, 2), "utf8");
}

async function readPriceRetryQueue() {
  try {
    const parsed = JSON.parse(await fs.readFile(priceRetryQueuePath, "utf8"));
    return {
      updatedAt: parsed.updatedAt || null,
      items: Array.isArray(parsed.items) ? parsed.items : [],
    };
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, items: [] };
    throw error;
  }
}

async function writePriceRetryQueue(queue) {
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    items: Array.isArray(queue.items) ? queue.items : [],
  };
  await fs.writeFile(priceRetryQueuePath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

async function readWarehouse() {
  if (warehouseMemoryCache) return warehouseMemoryCache;
  try {
    const warehouse = JSON.parse(await fs.readFile(warehousePath, "utf8"));
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: warehouse.updatedAt || null,
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
    return warehouseMemoryCache;
  } catch (error) {
    if (error.code === "ENOENT") {
      warehouseMemoryCache = { createdAt: new Date().toISOString(), updatedAt: null, products: [], suppliers: [] };
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

function mergeProducts(existingProducts, importedProducts) {
  const map = new Map();
  for (const product of existingProducts) {
    map.set(`${product.target}:${product.offerId}`, normalizeWarehouseProduct(product));
  }

  for (const imported of importedProducts) {
    if (!imported.offerId) continue;
    const key = `${imported.target}:${imported.offerId}`;
    const current = map.get(key);
    map.set(
      key,
      normalizeWarehouseProduct({
        ...current,
        ...imported,
        id: current?.id || imported.id,
        keyword: current?.keyword || imported.keyword,
        markup: current?.markup || imported.markup,
        autoPriceEnabled: current?.autoPriceEnabled !== undefined ? current.autoPriceEnabled : imported.autoPriceEnabled,
        autoPriceMin: current?.autoPriceMin ?? imported.autoPriceMin,
        autoPriceMax: current?.autoPriceMax ?? imported.autoPriceMax,
        links: current?.links || [],
        createdAt: current?.createdAt || imported.createdAt,
      }),
    );
  }

  return Array.from(map.values()).sort((a, b) => a.targetName.localeCompare(b.targetName) || a.name.localeCompare(b.name));
}

async function importOzonWarehouseProducts(limit = Number.POSITIVE_INFINITY) {
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
      let infoMap = new Map();
      let stockMap = new Map();
      let priceMap = new Map();
      try {
        const infoLimit = Number(process.env.OZON_SYNC_INFO_LIMIT || 300);
        const infoOfferIds = products
          .slice(0, Number.isFinite(infoLimit) && infoLimit > 0 ? infoLimit : 300)
          .map((product) => product.offer_id);
        [infoMap, stockMap, priceMap] = await Promise.all([
          getOzonProductInfoMap(infoOfferIds, account),
          getOzonStockMap(infoOfferIds, account),
          getOzonPriceMap(infoOfferIds, account),
        ]);
      } catch (error) {
        infoMap = new Map();
        stockMap = new Map();
        priceMap = new Map();
        const label = error?.message || error?.code || "ошибка API";
        warnings.push(`Ozon «${account.name || account.id}»: не загружены детали/цены (${label})`);
        logger.warn("ozon info/price batch failed", { account: account.id, detail: label });
      }

      imported.push(...products.map((product) => {
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
          marketplaceState: pickOzonState(product, info, stockInfo),
          ozon: {
            offerId: product.offer_id,
            vendor: cleanText(info.brand || info.vendor || ""),
            name: info.name || product.name || product.offer_id,
            description: info.description || "",
            categoryId: info.description_category_id || info.category_id,
            typeId: info.type_id || info.description_type_id,
            price: cabinetPrice || undefined,
            minPrice: priceDetails.minPrice || undefined,
            oldPrice: priceDetails.oldPrice || parseMoneyValue(info.old_price) || undefined,
            marketingSellerPrice: priceDetails.marketingSellerPrice || undefined,
            marketingPrice: priceDetails.marketingPrice || undefined,
            retailPrice: priceDetails.retailPrice || undefined,
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
    const oz = await importOzonWarehouseProducts(limit);
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
    marketplaceState: pickOzonState(product, info, stockInfo),
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
      (product) => ids.has(product.id) && product.marketplace === "ozon" && product.target === account.id && product.offerId,
    );
    if (!products.length) continue;

    const offerIds = products.map((product) => product.offerId);
    const [infoMap, stockMap, priceMap] = await Promise.all([
      getOzonProductInfoMap(offerIds, account),
      getOzonStockMap(offerIds, account).catch(() => new Map()),
      getOzonPriceMap(offerIds, account).catch(() => new Map()),
    ]);
    for (const product of products) {
      const info = infoMap.get(product.offerId);
      if (!info) continue;
      const index = warehouse.products.findIndex((item) => item.id === product.id);
      if (index < 0) continue;
      warehouse.products[index] = applyOzonInfoToWarehouseProduct(
        warehouse.products[index],
        info,
        account,
        stockMap.get(product.offerId) || {},
        priceMap.get(product.offerId) || {},
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

async function getPriceMasterMatchesForLinks(links, managedSuppliers = [], usdRate) {
  const normalizedLinks = links.map(normalizeWarehouseLink).filter((link) => link.article);
  if (!normalizedLinks.length) return new Map();

  const stoppedMap = stoppedSupplierMap(managedSuppliers);
  const articles = Array.from(new Set(normalizedLinks.map((link) => link.article)));
  const rows = [];
  for (const chunk of chunkArray(articles, 300)) {
    const placeholders = chunk.map(() => "?").join(",");
    const [chunkRows] = await pool.query(
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
      WHERE r.NativeID IN (${placeholders}) AND r.Ignored = 0
      ORDER BY r.NativeID, d.DocDate DESC, r.RowID DESC
      `,
      chunk,
    );
    rows.push(...chunkRows);
  }

  const rowsByArticle = new Map();
  for (const row of rows) {
    const key = cleanText(row.article);
    if (!rowsByArticle.has(key)) rowsByArticle.set(key, []);
    rowsByArticle.get(key).push(row);
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
        const normalizedPrice = normalizePriceMasterPrice(row.price, usdRate, link.priceCurrency);
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
  try {
    const partners = await listPriceMasterPartners();
    const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
    if (syncedSuppliers.changed) {
      await writeWarehouse(warehouse);
      logger.info("imported suppliers from PriceMaster", { imported: syncedSuppliers.imported });
    }
  } catch (error) {
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
  const products = warehouse.products.map((product) => {
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
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const fallbackMarkup = product.marketplace === "ozon"
      ? Number(targetMarkups.ozon || appSettings.defaultMarkups?.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(targetMarkups.yandex || appSettings.defaultMarkups?.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const markupCoefficient = Number(product.markup || selectedSupplier?.markupCoefficient || fallbackMarkup);
    const rawNextPrice = selectedSupplier
      ? Number(selectedSupplier.calculatedPrice || calculateRubPrice(selectedSupplier.price, rate, markupCoefficient))
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
      ready: Boolean(selectedSupplier && nextPrice > 0),
      selectedSupplier,
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
      availableSupplierCount: suppliers.filter((supplier) => supplier.available).length,
      hasLinks: links.length > 0,
      autoArchiveCandidate: links.length === 0,
      status: selectedSupplier ? (nextPrice !== currentPrice ? "price_changed" : "ok") : "no_supplier",
    };
  });

  return {
    createdAt: new Date().toISOString(),
    usdRate: rate,
    sourceError,
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

app.get("/api/history", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 300, 2000);
    response.json({ history: await readHistory(limit) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/audit-log", async (request, response, next) => {
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
      .map((item) => ({
        offer_id: String(item.offerId || "").trim(),
        price: String(roundPrice(item.price)),
        currency_code: "RUB",
      }))
      .filter((item) => item.offer_id && Number(item.price) > 0);

    if (!prices.length) {
      return response.status(400).json({ error: "No valid selected prices to send." });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const results = [];
    for (const chunk of chunkArray(prices, 1000)) {
      const data = await ozonRequest("/v1/product/import/prices", { prices: chunk }, account);
      results.push(data);
    }

    response.json({
      ok: true,
      sent: prices.length,
      results,
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

app.get("/api/settings", async (_request, response, next) => {
  try {
    response.json({ settings: await readAppSettings() });
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

app.put("/api/settings", saveSettingsHandler);
app.post("/api/settings", saveSettingsHandler);

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

    const data = await buildWarehouseViewCached({ sync, usdRate, refreshPrices });
    let rows = Array.isArray(data.products) ? data.products.slice() : [];

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
    if (brandFilter) {
      const needle = brandFilter.toLowerCase();
      rows = rows.filter((item) => resolveWarehouseBrand(item).toLowerCase().includes(needle));
    }

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
      totalAll: data.total,
      ready: data.ready,
      changed: data.changed,
      withoutSupplier: data.withoutSupplier,
      ozonArchived: data.ozonArchived || 0,
      ozonInactive: data.ozonInactive || 0,
      ozonOutOfStock: data.ozonOutOfStock || 0,
      usdRate: data.usdRate,
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
    try {
      const partners = await listPriceMasterPartners();
      const syncedSuppliers = syncWarehouseSuppliersFromPriceMaster(warehouse, partners);
      if (syncedSuppliers.changed) {
        await writeWarehouse(warehouse);
        logger.info("imported suppliers from PriceMaster via suppliers api", { imported: syncedSuppliers.imported });
      }
    } catch (error) {
      logger.warn("supplier import from PriceMaster in /api/suppliers failed", { detail: error.message });
    }
    const autoReactivated = applySupplierAutoReactivate(warehouse);
    if (autoReactivated.length) {
      await writeWarehouse(warehouse);
      logger.info("supplier auto-reactivated from suppliers api", { count: autoReactivated.length, suppliers: autoReactivated });
    }
    response.json({ suppliers: warehouse.suppliers || [] });
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

    await appendAudit(request, "supplier.save", { id: supplier.id, name: supplier.name });
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

    Object.assign(supplier, {
      name: request.body.name !== undefined ? cleanText(request.body.name) : supplier.name,
      stopped: request.body.stopped !== undefined ? Boolean(request.body.stopped) : supplier.stopped,
      note: request.body.note !== undefined ? cleanText(request.body.note) : supplier.note,
      stopReason: request.body.stopReason !== undefined ? cleanText(request.body.stopReason) : supplier.stopReason,
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
    await appendAudit(request, "supplier.update", { id: supplier.id, stopped: supplier.stopped, name: supplier.name });
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
    warehouse.suppliers = warehouse.suppliers.filter((supplier) => supplier.id !== request.params.id);
    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.delete", { id: request.params.id });
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
    if (index >= 0) supplier.articles[index] = article;
    else supplier.articles.push(article);

    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.article.save", { supplierId: supplier.id, article: article.article });
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
    supplier.articles = (supplier.articles || []).filter((article) => article.id !== request.params.articleId);
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
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
        priceHistory: current.priceHistory || [],
        links: current.links,
        createdAt: current.createdAt,
      });
    } else {
      warehouse.products.push(input);
    }

    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
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

    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
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
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      product.markup = markup;
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }

    response.json({ ok: true, changed, warehouse: await writeWarehouse(warehouse) });
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
    let changed = 0;
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }

    response.json({ ok: true, changed, warehouse: await writeWarehouse(warehouse) });
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
    for (const product of warehouse.products) {
      if (Boolean(product.autoPriceEnabled !== false) === enabled) continue;
      product.autoPriceEnabled = enabled;
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }
    response.json({ ok: true, changed, warehouse: await writeWarehouse(warehouse) });
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
    const groupId = cleanText(request.body.groupId) || `manual-${crypto.randomUUID()}`;
    let changed = 0;
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      product.manualGroupId = groupId;
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }
    response.json({ ok: true, groupId, changed, warehouse: await writeWarehouse(warehouse) });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/ungroup", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для разъединения." });
    const warehouse = await readWarehouse();
    let changed = 0;
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      product.manualGroupId = "";
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }
    response.json({ ok: true, changed, warehouse: await writeWarehouse(warehouse) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/warehouse/products/:id", async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    warehouse.products = warehouse.products.filter((product) => product.id !== request.params.id);
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
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

    const baseLink = normalizeWarehouseLink(request.body);
    if (!baseLink.article) return response.status(400).json({ error: "Укажите артикул PriceMaster." });

    const warehouse = await readWarehouse();
    const now = new Date().toISOString();
    const updatedIds = [];

    for (const product of warehouse.products) {
      if (!ids.has(String(product.id))) continue;
      const link = normalizeWarehouseLink({
        ...baseLink,
        id: ids.size > 1 && !request.body.id ? crypto.randomUUID() : baseLink.id,
        createdAt: baseLink.createdAt || now,
      });
      product.links = Array.isArray(product.links) ? product.links : [];
      const index = product.links.findIndex((item) => item.id === link.id);
      if (index >= 0) product.links[index] = link;
      else product.links.push(link);
      product.autoPriceEnabled = true;
      product.updatedAt = now;
      updatedIds.push(product.id);
    }

    if (!updatedIds.length) return response.status(404).json({ error: "Товары склада не найдены." });

    const savedProducts = warehouse.products
      .filter((product) => updatedIds.includes(product.id))
      .map(normalizeWarehouseProduct);
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    writeWarehouseInBackground(warehouse, "link_bulk_add_or_update");
    response.json({ ok: true, changed: savedProducts.length, products: savedProducts, persisted: "queued" });
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

    const link = normalizeWarehouseLink(request.body);
    if (!link.article) return response.status(400).json({ error: "Укажите артикул PriceMaster." });
    product.links = Array.isArray(product.links) ? product.links : [];
    const index = product.links.findIndex((item) => item.id === link.id);
    if (index >= 0) product.links[index] = link;
    else product.links.push(link);
    if (product.links.length > 0) product.autoPriceEnabled = true;
    product.updatedAt = new Date().toISOString();
    const savedProduct = normalizeWarehouseProduct(product);
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    writeWarehouseInBackground(warehouse, "link_add_or_update");
    response.json({ ok: true, product: savedProduct, links: savedProduct.links || [], persisted: "queued" });
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
    product.links = (product.links || []).filter((link) => link.id !== request.params.linkId);
    product.updatedAt = new Date().toISOString();
    const savedProduct = normalizeWarehouseProduct(product);
    warehouseMemoryCache = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: warehouse.products,
      suppliers: warehouse.suppliers,
    };
    writeWarehouseInBackground(warehouse, "link_delete");
    response.json({ ok: true, product: savedProduct, links: savedProduct.links || [], persisted: "queued" });
    if ((savedProduct.links || []).length) queueImmediateAutoPricePush([request.params.productId], "link_delete");
  } catch (error) {
    next(error);
  }
});

async function sendWarehousePrices({ productIds, usdRate, minDiffRub = 0, minDiffPct = 0, dryRun = false } = {}) {
  const ids = Array.isArray(productIds) ? new Set(productIds.map(String)) : null;
  const rubThreshold = Math.max(0, Number(minDiffRub || 0));
  const pctThreshold = Math.max(0, Number(minDiffPct || 0));
  const preview = await buildWarehouseView({ usdRate: Number(usdRate || 0) || undefined });
  const selected = ids ? preview.products.filter((product) => ids.has(product.id)) : preview.products;
  const skipped = [];
  const items = [];

  for (const product of selected) {
    if (!product.hasLinks) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "no_pricemaster_link" });
      continue;
    }
    if (!product.ready) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "not_ready" });
      continue;
    }
    const current = Number(product.currentPrice || 0);
    const nextValue = Number(product.nextPrice || 0);
    const diffRub = Math.abs(nextValue - current);
    const diffPct = current > 0 ? (diffRub / current) * 100 : 100;
    if (diffRub <= 0) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "unchanged" });
      continue;
    }
    if (rubThreshold > 0 && diffRub < rubThreshold) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "below_rub_threshold", diffRub });
      continue;
    }
    if (pctThreshold > 0 && diffPct < pctThreshold) {
      skipped.push({ id: product.id, offerId: product.offerId, reason: "below_pct_threshold", diffPct: roundPrice(diffPct) });
      continue;
    }
    items.push({
      id: product.id,
      target: product.target,
      offerId: product.offerId,
      price: product.nextPrice,
      oldPrice: product.currentPrice,
      markup: product.markupCoefficient,
      supplier: product.selectedSupplier,
      marketplace: product.marketplace,
    });
  }

  if (dryRun) return { ok: true, dryRun: true, selected: selected.length, readyToSend: items.length, skipped, items };

  const results = [];
  const failed = [];
  for (const account of getOzonAccounts()) {
    const targetItems = items.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
    const ozonItems = targetItems
      .map((item) => ({
        offer_id: String(item.offerId || "").trim(),
        price: String(roundPrice(item.price)),
        currency_code: "RUB",
      }))
      .filter((item) => item.offer_id && Number(item.price) > 0);
    if (!ozonItems.length) continue;
    try {
      for (const chunk of chunkArray(ozonItems, 1000)) {
        results.push({ target: account.id, response: await ozonRequest("/v1/product/import/prices", { prices: chunk }, account) });
      }
    } catch (error) {
      const detail = error?.message || "send_failed";
      failed.push(...targetItems.map((item) => ({ ...item, error: detail, marketplace: "ozon" })));
    }
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

  const warehouse = await readWarehouse();
  const sentAt = new Date().toISOString();
  const successIds = new Set(items.map((item) => item.id));
  for (const failedItem of failed) successIds.delete(failedItem.id);
  for (const item of items) {
    const product = warehouse.products.find((entry) => entry.id === item.id);
    if (!product) continue;
    const success = successIds.has(item.id);
    if (success) product.marketplacePrice = roundPrice(item.price);
    product.priceHistory = Array.isArray(product.priceHistory) ? product.priceHistory : [];
    const previous = product.priceHistory[product.priceHistory.length - 1] || null;
    const reasons = [];
    if (previous?.supplierArticle && previous.supplierArticle !== (item.supplier?.article || null)) reasons.push("смена поставщика");
    if (Number(previous?.usdRate || 0) !== Number(preview.usdRate || 0)) reasons.push("изменение курса");
    if (Number(previous?.usdPrice || 0) !== Number(item.supplier?.price || 0)) reasons.push("изменение прайса поставщика");
    if (!reasons.length) reasons.push("регулярный пересчет");
    product.priceHistory.push({
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
      status: success ? "success" : "error",
      error: success ? null : (failed.find((entry) => entry.id === item.id)?.error || "send_failed"),
    });
    product.priceHistory = product.priceHistory.slice(-100);
    if (item.marketplace === "ozon") {
      const failedEntry = failed.find((entry) => entry.id === item.id);
      product.lastOzonPriceSend = {
        status: failedEntry ? "error" : "success",
        at: sentAt,
        detail: failedEntry ? failedEntry.error : "ok",
      };
    }
  }
  await writeWarehouse(warehouse);

  const queueState = await readPriceRetryQueue();
  const failedQueued = failed.map((item) => ({
    ...item,
    queueKey: `${item.id}:${item.target}`,
    queuedAt: sentAt,
    attempts: Number(item.attempts || 0) + 1,
  }));
  const merged = [...(queueState.items || []), ...failedQueued];
  const deduped = Array.from(new Map(merged.map((item) => [item.queueKey || `${item.id}:${item.target}`, item])).values()).slice(0, 5000);
  await writePriceRetryQueue({ items: deduped });

  return { ok: true, sent: items.length - failed.length, failed: failed.length, queued: deduped.length, skipped, results };
}

async function processMarketplaceJob(name, data = {}) {
  if (name === "auto-price-push") {
    return sendWarehousePrices({
      productIds: Array.isArray(data.productIds) ? data.productIds : undefined,
      usdRate: data.usdRate,
      minDiffRub: Number(data.minDiffRub || 0),
      minDiffPct: Number(data.minDiffPct || 0),
      dryRun: false,
    });
  }
  if (name === "no-supplier-automation") {
    const preview = await buildWarehouseView({ sync: true });
    return runNoSupplierMarketplaceAutomation(preview);
  }
  if (name === "supplier-recovery-automation") {
    const preview = await buildWarehouseView({ sync: false });
    return runSupplierRecoveryAutomation(preview);
  }
  return null;
}

function queueMarketplaceJob(name, data = {}, { priority = 5 } = {}) {
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return;
  if (marketplaceQueue) {
    marketplaceQueue.add(name, data, {
      priority,
      removeOnComplete: 2000,
      removeOnFail: 2000,
    }).catch((error) => logger.warn("queue add failed", { name, detail: error?.message || String(error) }));
    return;
  }
  processMarketplaceJob(name, data).catch((error) => logger.warn("inline marketplace job failed", { name, detail: error?.message || String(error) }));
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
      { connection, concurrency: 4 },
    );
    marketplaceWorker.on("failed", (job, error) => {
      logger.warn("marketplace job failed", { job: job?.name, detail: error?.message || String(error) });
    });
    logger.info("marketplace queue enabled", { mode: "bullmq" });
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
        queueMarketplaceJob(
          "auto-price-push",
          {
            productIds: ids,
            usdRate: undefined,
            minDiffRub: Number(process.env.AUTO_PRICE_MIN_DIFF_RUB || 0),
            minDiffPct: Number(process.env.AUTO_PRICE_MIN_DIFF_PCT || 0),
          },
          { priority: 1 },
        );
        logger.info("immediate auto price push queued", { reason, scope: ids ? ids.length : "all" });
      })
      .catch((error) => {
        logger.warn("immediate auto price push failed", { reason, detail: error?.message || String(error) });
      });
  }, 1200);
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
    const queue = await readPriceRetryQueue();
    if (!queue.items.length) return response.json({ ok: true, retried: 0, failed: 0, remaining: 0 });
    const requestedKeys = new Set((Array.isArray(request.body.queueKeys) ? request.body.queueKeys : []).map(String));
    const selected = requestedKeys.size
      ? queue.items.filter((item) => requestedKeys.has(String(item.queueKey || `${item.id}:${item.target}`)))
      : queue.items;
    const items = selected.slice(0, 1000);
    const results = [];
    const failed = [];

    for (const account of getOzonAccounts()) {
      const targetItems = items.filter((item) => item.marketplace === "ozon" && matchesOzonTarget(item.target, account.id));
      const ozonItems = targetItems.map((item) => ({ offer_id: String(item.offerId || "").trim(), price: String(roundPrice(item.price)), currency_code: "RUB" }))
        .filter((item) => item.offer_id && Number(item.price) > 0);
      if (!ozonItems.length) continue;
      try {
        for (const chunk of chunkArray(ozonItems, 1000)) {
          results.push({ target: account.id, response: await ozonRequest("/v1/product/import/prices", { prices: chunk }, account) });
        }
      } catch (error) {
        failed.push(...targetItems.map((item) => ({
          ...item,
          error: error?.message || "retry_failed",
          queueKey: item.queueKey || `${item.id}:${item.target}`,
          queuedAt: item.queuedAt || new Date().toISOString(),
          attempts: Number(item.attempts || 0) + 1,
        })));
      }
    }

    for (const shop of getYandexShops()) {
      const targetItems = items.filter((item) => item.marketplace === "yandex" && matchesYandexTarget(item.target, shop.id));
      const yandexItems = targetItems.map((item) => ({ offerId: String(item.offerId || "").trim(), price: { value: roundPrice(item.price), currencyId: "RUR" } }))
        .filter((item) => item.offerId && item.price.value > 0);
      if (!yandexItems.length) continue;
      try {
        for (const chunk of chunkArray(yandexItems, 500)) {
          results.push({ target: shop.id, response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }) });
        }
      } catch (error) {
        failed.push(...targetItems.map((item) => ({
          ...item,
          error: error?.message || "retry_failed",
          queueKey: item.queueKey || `${item.id}:${item.target}`,
          queuedAt: item.queuedAt || new Date().toISOString(),
          attempts: Number(item.attempts || 0) + 1,
        })));
      }
    }

    const processedKeys = new Set(items.map((item) => String(item.queueKey || `${item.id}:${item.target}`)));
    const untouched = queue.items.filter((item) => !processedKeys.has(String(item.queueKey || `${item.id}:${item.target}`)));
    const remaining = [...failed, ...untouched];
    await writePriceRetryQueue({ items: remaining.slice(0, 5000) });
    response.json({ ok: true, retried: items.length - failed.length, failed: failed.length, remaining: remaining.length, results });
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
        queueKey: item.queueKey || `${item.id}:${item.target}`,
      }))
      .sort((a, b) => new Date(b.queuedAt || 0) - new Date(a.queuedAt || 0));
    response.json({ ok: true, updatedAt: queue.updatedAt, total: items.length, items });
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
      await writeWarehouse(warehouse);
      return response.json({ ok: true, target: account.id, sent: 1, item: built.item, result });
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
      await writeWarehouse(warehouse);
      return response.json({ ok: true, target: shop.id, sent: 1, offer: built.offer, result });
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
        const payload = {
          stocks: chunk.map((item) => ({
            offer_id: String(item.offerId || "").trim(),
            stock: 0,
          })),
        };
        try {
          await ozonRequest("/v2/products/stocks", payload, account);
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
          stocks: chunk.map((item) => ({
            offer_id: String(item.offerId || "").trim(),
            stock: Math.max(1, Number(item.marketplaceState?.stock || 1)),
          })),
        };
        try {
          await ozonRequest("/v2/products/stocks", payload, account);
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
            stock: Math.max(1, Number(item.marketplaceState?.stock || 1)),
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

function pickNoSupplierAutomationCandidates(products = []) {
  const list = Array.isArray(products) ? products : [];
  const linkedNoSupplier = list.filter((product) => product.hasLinks && !product.selectedSupplier);
  return {
    toZeroStock: autoZeroStockOnNoSupplier
      ? linkedNoSupplier.filter((product) => !product.noSupplierAutomation?.stockZeroAt)
      : [],
    toArchive: autoArchiveOnNoLinks
      ? linkedNoSupplier.filter(
          (product) =>
            Boolean(product.noSupplierAutomation?.stockZeroAt)
            && !product.noSupplierAutomation?.archivedAt
            && product.marketplaceState?.code !== "archived",
        )
      : [],
  };
}

async function runNoSupplierMarketplaceAutomation(preview) {
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const now = new Date().toISOString();
  const { toZeroStock, toArchive } = pickNoSupplierAutomationCandidates(products);

  if (!toZeroStock.length && !toArchive.length) {
    return { zeroStockSent: 0, archived: 0, errors: [] };
  }

  const [stockActions, archiveActions] = await Promise.all([
    sendZeroStocksToMarketplace(toZeroStock),
    archiveProductsOnMarketplaces(toArchive),
  ]);
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

async function runSupplierRecoveryAutomation(preview) {
  if (!autoRestoreOnSupplierReturn) {
    return { recovered: 0, restoredStocks: 0, unarchived: 0, errors: [] };
  }
  const products = Array.isArray(preview?.products) ? preview.products : [];
  const recovered = products.filter(
    (product) =>
      product.hasLinks
      && product.selectedSupplier
      && Boolean(product.noSupplierAutomation?.stockZeroAt)
      && !product.noSupplierAutomation?.recoveredAt,
  );
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
      minDiffRub: Number(process.env.AUTO_PRICE_MIN_DIFF_RUB || 0),
      minDiffPct: Number(process.env.AUTO_PRICE_MIN_DIFF_PCT || 0),
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
      return await writeDailySyncState(withDailySyncLog({
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
    } catch (error) {
      return await writeDailySyncState(withDailySyncLog({
        status: "failed",
        trigger,
        startedAt,
        lastRunAt: new Date().toISOString(),
        error: error.code || error.message,
      }));
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

app.post("/api/daily-sync/run", async (_request, response, next) => {
  try {
    response.json(await runDailyRefresh("manual"));
  } catch (error) {
    next(error);
  }
});

app.use((error, request, response, _next) => {
  logger.error("request error", {
    path: request.path,
    method: request.method,
    detail: error.code || error.message,
    err: error,
  });
  const uploadError = error instanceof multer.MulterError;
  response.status(uploadError ? 400 : error.statusCode || 500).json({
    error: uploadError ? "Не удалось загрузить изображение" : "Не удалось выполнить запрос к Price Master",
    detail: error.code || error.message,
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
    if (autoSyncMinutes > 0) {
      logger.info("auto sync enabled", { everyMinutes: autoSyncMinutes });
    }
    if (dailySyncEnabled) {
      logger.info("daily sync enabled", { time: dailySyncTime, sendPrices: dailySyncSendPrices });
    }
    pruneUploadDirectory().catch((err) => logger.warn("initial upload prune failed", { detail: err?.message || String(err) }));
    readWarehouse()
      .then((warehouse) => logger.info("warehouse cache warmed", { products: warehouse.products.length, suppliers: warehouse.suppliers.length }))
      .catch((err) => logger.warn("warehouse cache warm failed", { detail: err?.message || String(err) }));
  });

  scheduleDailySync();

  if (autoSyncMinutes > 0) {
    setInterval(async () => {
      try {
        const result = await runSync();
        const warehouse = await buildWarehouseView({ sync: true });
        const automation = await runNoSupplierMarketplaceAutomation(warehouse);
        const recovery = await runSupplierRecoveryAutomation(warehouse);
        const autoPricePush = await processMarketplaceJob("auto-price-push", {
          usdRate: undefined,
          minDiffRub: Number(process.env.AUTO_PRICE_MIN_DIFF_RUB || 0),
          minDiffPct: Number(process.env.AUTO_PRICE_MIN_DIFF_PCT || 0),
        });
        logger.info("auto sync complete", {
          items: result.items,
          changes: result.changes,
          at: result.createdAt,
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
      } catch (error) {
        logger.error("auto sync failed", { detail: error.code || error.message, err: error });
      }
    }, autoSyncMinutes * 60 * 1000);
  }
}

module.exports = { app, startServer, resolveMarkupCoefficient, normalizePriceMasterPrice, pickNoSupplierAutomationCandidates };

if (require.main === module) {
  startServer();
}
