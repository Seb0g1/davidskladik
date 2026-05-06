const fs = require("fs/promises");
const fsSync = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const multer = require("multer");
const mysql = require("mysql2/promise");
require("dotenv").config();

const app = express();
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
const ozonProductRulesPath = path.join(configDir, "ozon-product-rules.json");
const ozonProductRulesExamplePath = path.join(configDir, "ozon-product-rules.example.json");
const sessionCookieName = "pm_session";
const sessionTtlMs = 1000 * 60 * 60 * 12;
const autoSyncMinutes = Number(process.env.AUTO_SYNC_MINUTES || 0);
const dailySyncTime = process.env.DAILY_SYNC_TIME || "11:00";
const dailySyncEnabled = process.env.DAILY_SYNC_ENABLED !== "false";
const ozonBaseUrl = "https://api-seller.ozon.ru";
const yandexBaseUrl = "https://api.partner.market.yandex.ru";
const exchangeRateTtlMs = 6 * 60 * 60 * 1000;

let dailySyncTimer = null;
let dailySyncNextRunAt = null;
let dailySyncPromise = null;
let warehouseWritePromise = Promise.resolve();

app.use(express.json({ limit: "1mb" }));

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
  connectionLimit: 8,
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

function requireAuth(request, response, next) {
  const publicPaths = ["/login", "/login.html", "/styles.css", "/login.js", "/app.js", "/product.js", "/ozon-product.js", "/yandex-product.js"];
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

app.post("/api/login", (request, response) => {
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

function cleanText(value) {
  return String(value || "").trim();
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
    price: Number(input.price || 0) || undefined,
    oldPrice: Number(input.oldPrice || input.old_price || 0) || undefined,
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
  const price = input.price || input;
  const minPrice = Number(price.min_price ?? price.minPrice ?? input.min_price ?? input.minPrice ?? 0) || null;
  const currentPrice = Number(price.price ?? input.price?.price ?? input.price ?? 0) || null;
  const oldPrice = Number(price.old_price ?? price.oldPrice ?? input.old_price ?? input.oldPrice ?? 0) || null;
  const marketingPrice = Number(price.marketing_price ?? price.marketingPrice ?? input.marketing_price ?? input.marketingPrice ?? 0) || null;
  return compactObject({
    currentPrice,
    minPrice,
    oldPrice,
    marketingPrice,
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
    source: cleanText(input.source || (input.productId || input.product_id ? "marketplace" : "manual")),
    ozon: ozonDraft,
    yandex: yandexDraft,
    marketplaceState: normalizeMarketplaceState(input.marketplaceState || input.marketplace_state || input.ozonState),
    exports: normalizeProductExports(input.exports),
    priceHistory: Array.isArray(input.priceHistory) ? input.priceHistory.slice(-100) : [],
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    links: Array.isArray(input.links) ? input.links.map(normalizeWarehouseLink) : [],
  };
}

function normalizeWarehouseLink(input = {}) {
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    article: cleanText(input.article || input.offerId || input.nativeId),
    keyword: cleanText(input.keyword),
    supplierName: cleanText(input.supplierName || input.partnerName),
    partnerId: cleanText(input.partnerId),
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
  return {
    id: cleanText(input.id) || crypto.randomUUID(),
    name: cleanText(input.name),
    stopped: Boolean(input.stopped),
    note: cleanText(input.note),
    stopReason: cleanText(input.stopReason || input.stop_reason),
    articles: Array.isArray(input.articles) ? input.articles.map(normalizeSupplierArticle) : [],
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function normalizeSupplierName(value) {
  return String(value || "").trim().toLowerCase();
}

function parseSupplierMarkups(value) {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;

  return String(value)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .reduce((acc, line) => {
      const [name, coefficient] = line.split("=").map((part) => part.trim());
      const parsed = Number(coefficient);
      if (name && Number.isFinite(parsed) && parsed > 0) {
        acc[normalizeSupplierName(name)] = parsed;
      }
      return acc;
    }, {});
}

function pickMarkup({ target, partnerName, targetMarkups = {}, supplierMarkups = {} }) {
  const supplierMarkup = supplierMarkups[normalizeSupplierName(partnerName)];
  if (Number.isFinite(Number(supplierMarkup)) && Number(supplierMarkup) > 0) {
    return Number(supplierMarkup);
  }

  const targetMeta = targetById(target) || { marketplace: target };
  if (targetMeta.marketplace === "ozon" || target === "ozon") {
    return Number(targetMarkups.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7);
  }

  return Number(targetMarkups.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
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

async function buildMarketplacePreview({
  limit = 200,
  search = "",
  usdRate = process.env.DEFAULT_USD_RATE || 95,
  targetMarkups = {},
  supplierMarkups = {},
  targets = [...getOzonAccounts().map((account) => account.id), ...getYandexShops().map((shop) => shop.id)],
} = {}) {
  const offers = await getPriceMasterProductCandidates({ limit, search });
  const offerIds = offers.map((offer) => offer.offerId).filter(Boolean);
  const enabledTargets = new Set(targets);
  const includeOzon = enabledTargets.has("ozon");
  const includeYandex = enabledTargets.has("yandex");
  const rows = [];

  const ozonMaps = new Map();
  const ozonOfferSets = new Map();
  for (const account of getOzonAccounts()) {
    if (includeOzon || enabledTargets.has(account.id)) {
      const [priceMap, offerSet] = await Promise.all([
        getOzonPriceMap(offerIds, account),
        getOzonOfferIdSet(10000, account),
      ]);
      ozonMaps.set(account.id, priceMap);
      ozonOfferSets.set(account.id, offerSet);
    }
  }

  const yandexMaps = new Map();
  const yandexOfferSets = new Map();
  for (const shop of getYandexShops()) {
    if (includeYandex || enabledTargets.has(shop.id)) {
      const [priceMap, offerSet] = await Promise.all([
        getYandexPriceMap(shop, offerIds),
        getYandexOfferIdSet(shop, offerIds),
      ]);
      yandexMaps.set(shop.id, priceMap);
      yandexOfferSets.set(shop.id, offerSet);
    }
  }

  for (const offer of offers) {
    for (const account of getOzonAccounts()) {
      if (!includeOzon && !enabledTargets.has(account.id)) continue;
      const markupCoefficient = pickMarkup({
        target: account.id,
        partnerName: offer.partnerName,
        targetMarkups,
        supplierMarkups,
      });
      const nextPrice = calculateRubPrice(offer.price, usdRate, markupCoefficient);
      const currentPrice = Number(ozonMaps.get(account.id)?.get(offer.offerId)?.price?.price || 0);
      const exists = ozonOfferSets.get(account.id)?.has(offer.offerId) || false;
      rows.push({
        target: account.id,
        targetName: account.name || "Ozon",
        marketplace: "ozon",
        offerId: offer.offerId,
        name: offer.name,
        partnerName: offer.partnerName,
        usdPrice: offer.price,
        markupCoefficient,
        exists,
        currentPrice: currentPrice || null,
        currentPriceStatus: currentPrice ? "ok" : exists ? "no_price" : "not_found",
        nextPrice,
        changed: nextPrice > 0 && nextPrice !== currentPrice,
        ready: nextPrice > 0,
      });
    }

    for (const shop of getYandexShops()) {
      if (!includeYandex && !enabledTargets.has(shop.id)) continue;
      const markupCoefficient = pickMarkup({
        target: shop.id,
        partnerName: offer.partnerName,
        targetMarkups,
        supplierMarkups,
      });
      const nextPrice = calculateRubPrice(offer.price, usdRate, markupCoefficient);
      const currentPrice = Number(yandexMaps.get(shop.id)?.get(offer.offerId) || 0);
      const exists = yandexOfferSets.get(shop.id)?.has(offer.offerId) || false;
      rows.push({
        target: shop.id,
        targetName: shop.name,
        marketplace: "yandex",
        businessId: shop.businessId,
        offerId: offer.offerId,
        name: offer.name,
        partnerName: offer.partnerName,
        usdPrice: offer.price,
        markupCoefficient,
        exists,
        currentPrice: currentPrice || null,
        currentPriceStatus: currentPrice ? "ok" : exists ? "no_price" : "not_found",
        nextPrice,
        changed: nextPrice > 0 && nextPrice !== currentPrice,
        ready: nextPrice > 0,
      });
    }
  }

  return {
    createdAt: new Date().toISOString(),
    usdRate: Number(usdRate),
    targetMarkups,
    supplierMarkups,
    sourceItems: offers.length,
    rows,
    ready: rows.filter((row) => row.ready).length,
    changed: rows.filter((row) => row.changed).length,
    notFound: rows.filter((row) => row.currentPriceStatus === "not_found").length,
    noPrice: rows.filter((row) => row.currentPriceStatus === "no_price").length,
    targets: marketplaceTargets(),
  };
}

async function getPriceMasterOffersByArticle(offerIds) {
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
        map.set(row.article, {
          ...row,
          price: Number(row.price || 0),
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
  const ozonProducts = await getOzonProducts(limit);
  const offerIds = ozonProducts.map((item) => item.offer_id).filter(Boolean);
  const [ozonPriceMap, pmOfferMap] = await Promise.all([
    getOzonPriceMap(offerIds),
    getPriceMasterOffersByArticle(offerIds),
  ]);

  const rows = ozonProducts.map((product) => {
    const ozonPrice = ozonPriceMap.get(product.offer_id);
    const pmOffer = pmOfferMap.get(product.offer_id);
    const currentOzonPrice = Number(ozonPrice?.price?.price || 0);
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

  const unique = new Map();
  for (const row of rows) {
    if (!unique.has(row.offerId)) {
      unique.set(row.offerId, { ...row, price: Number(row.price || 0) });
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

async function readWarehouse() {
  try {
    const warehouse = JSON.parse(await fs.readFile(warehousePath, "utf8"));
    return {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: warehouse.updatedAt || null,
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return { createdAt: new Date().toISOString(), updatedAt: null, products: [], suppliers: [] };
    }
    throw error;
  }
}

async function writeWarehouse(warehouse) {
  warehouseWritePromise = warehouseWritePromise.then(async () => {
    await fs.mkdir(dataDir, { recursive: true });
    const payload = {
      createdAt: warehouse.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      products: Array.isArray(warehouse.products) ? warehouse.products.map(normalizeWarehouseProduct) : [],
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.map(normalizeManagedSupplier) : [],
    };
    const temporaryPath = `${warehousePath}.${process.pid}.${Date.now()}.tmp`;
    await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
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
    error: state.error || state.warehouse?.sourceError || null,
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
  if (!accounts.length) return imported;

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
      } catch (_error) {
        infoMap = new Map();
        stockMap = new Map();
        priceMap = new Map();
      }

      imported.push(...products.map((product) => {
        const info = infoMap.get(product.offer_id) || {};
        const stockInfo = stockMap.get(product.offer_id) || {};
        const priceInfo = priceMap.get(product.offer_id) || {};
        const priceDetails = normalizeOzonPriceDetails(priceInfo);
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
          marketplacePrice: Number(priceDetails.currentPrice || info.price || product.price || 0) || null,
          marketplaceMinPrice: priceDetails.minPrice || null,
          name: info.name || product.name || product.offer_id || `Ozon ${product.product_id}`,
          marketplaceState: pickOzonState(product, info, stockInfo),
          ozon: {
            offerId: product.offer_id,
            name: info.name || product.name || product.offer_id,
            description: info.description || "",
            categoryId: info.description_category_id || info.category_id,
            price: Number(priceDetails.currentPrice || info.price || 0) || undefined,
            minPrice: priceDetails.minPrice || undefined,
            oldPrice: Number(info.old_price || 0) || undefined,
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
    } catch (_error) {
      // One Ozon cabinet should not block other accounts or Yandex.
    }
  }

  return imported;
}

async function importYandexWarehouseProducts(limit = Number.POSITIVE_INFINITY) {
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  const imported = [];
  if (!shops.length) return imported;

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
    } catch (_error) {
      // One Yandex cabinet should not block Ozon or local warehouse work.
    }
  }

  return imported;
}

async function syncWarehouseProductsFromMarketplaces(warehouse, limit = Number.POSITIVE_INFINITY) {
  let imported = [];
  try {
    imported = imported.concat(await importOzonWarehouseProducts(limit));
  } catch (_error) {
    // Marketplace sync should not block local warehouse work.
  }
  try {
    imported = imported.concat(await importYandexWarehouseProducts(limit));
  } catch (_error) {
    // Marketplace sync should not block local warehouse work.
  }
  return { ...warehouse, products: mergeProducts(warehouse.products, imported) };
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
    marketplacePrice: Number(priceDetails.currentPrice || info.price || product.marketplacePrice || 0) || null,
    marketplaceMinPrice: priceDetails.minPrice || product.marketplaceMinPrice || null,
    marketplaceState: pickOzonState(product, info, stockInfo),
    ozon: {
      ...(product.ozon || {}),
      offerId: product.offerId,
      name: info.name || product.ozon?.name || product.name,
      description: info.description || product.ozon?.description || "",
      categoryId: info.description_category_id || info.category_id || product.ozon?.categoryId,
      price: Number(priceDetails.currentPrice || info.price || product.ozon?.price || 0) || undefined,
      minPrice: priceDetails.minPrice || product.ozon?.minPrice || undefined,
      oldPrice: Number(info.old_price || product.ozon?.oldPrice || 0) || undefined,
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

async function getPriceMasterMatchesForLinks(links, managedSuppliers = []) {
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

  const map = new Map();
  for (const link of normalizedLinks) {
    const matches = rows
      .filter((row) => {
        const supplierOk =
          !link.supplierName ||
          normalizeSupplierName(row.partnerName) === normalizeSupplierName(link.supplierName);
        const partnerOk = !link.partnerId || String(row.partnerId) === String(link.partnerId);
        const keywordOk = includesKeyword(row.name, link.keyword);
        return row.article === link.article && supplierOk && partnerOk && keywordOk;
      })
      .map((row) => {
        const stoppedSupplier = stoppedMap.get(normalizeSupplierName(row.partnerName));
        const price = stoppedSupplier ? 0 : Number(row.price || 0);
        const active = stoppedSupplier ? false : Boolean(row.active);
        return {
          ...link,
          rowId: row.rowId,
          article: row.article,
          name: row.name,
          partnerId: row.partnerId,
          partnerName: row.partnerName,
          price,
          originalPrice: Number(row.price || 0),
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
    .sort((a, b) => a.priority - b.priority || Number(a.price || 0) - Number(b.price || 0) || String(b.docDate).localeCompare(String(a.docDate)))[0] || null;
}

function storedMarketplacePrice(product = {}) {
  const ozonPrice = Number(product.ozon?.price || 0);
  const yandexPrice = Number(product.yandex?.price || 0);
  return Number(product.marketplacePrice || 0) || (product.marketplace === "ozon" ? ozonPrice : yandexPrice) || null;
}

async function getWarehousePriceMaps(products, { refresh = false } = {}) {
  const result = new Map();
  for (const product of products) result.set(product.id, storedMarketplacePrice(product));
  if (!refresh) return result;

  for (const account of getOzonAccounts()) {
    const accountProducts = products.filter((product) => product.target === account.id && product.marketplace === "ozon");
    const ozonOfferIds = accountProducts.map((product) => product.offerId).filter(Boolean);
    if (!ozonOfferIds.length) continue;
    try {
      const priceMap = await getOzonPriceMap(ozonOfferIds, account);
      for (const product of accountProducts) {
        result.set(product.id, Number(priceMap.get(product.offerId)?.price?.price || 0) || null);
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  for (const shop of getYandexShops()) {
    const offerIds = products.filter((product) => product.target === shop.id).map((product) => product.offerId).filter(Boolean);
    if (!offerIds.length) continue;
    try {
      const priceMap = await getYandexPriceMap(shop, offerIds);
      for (const product of products.filter((item) => item.target === shop.id)) {
        result.set(product.id, Number(priceMap.get(product.offerId) || 0) || null);
      }
    } catch (_error) {
      // Keep stored prices when a marketplace request fails.
    }
  }

  return result;
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
  const rate = Number(usdRate || (await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
  let warehouse = await readWarehouse();
  if (sync) {
    warehouse = await syncWarehouseProductsFromMarketplaces(warehouse, limit);
    await writeWarehouse(warehouse);
  }

  const links = warehouse.products.flatMap((product) => product.links || []);
  let matchMap = new Map();
  let sourceError = null;
  try {
    matchMap = await getPriceMasterMatchesForLinks(links, warehouse.suppliers);
  } catch (error) {
    sourceError = error.code || error.message;
  }

  const [priceMap, minPriceResult] = await Promise.all([
    getWarehousePriceMaps(warehouse.products, { refresh: refreshPrices }),
    getWarehouseMinPriceMaps(warehouse.products, { refresh: refreshPrices }),
  ]);
  const minPriceMap = minPriceResult.map;
  if (refreshPrices && minPriceResult.mutated) {
    await writeWarehouse(warehouse);
  }
  const products = warehouse.products.map((product) => {
    const marketplaceDefaultMarkup = product.marketplace === "ozon"
      ? Number(targetMarkups.ozon || process.env.DEFAULT_OZON_MARKUP || 1.7)
      : Number(targetMarkups.yandex || process.env.DEFAULT_YANDEX_MARKUP || 1.6);
    const markupCoefficient = Number(product.markup || marketplaceDefaultMarkup);
    const suppliers = (product.links || []).flatMap((link) =>
      (matchMap.get(link.id) || []).map((match) => ({
        ...match,
        calculatedPrice: calculateRubPrice(match.price, rate, markupCoefficient),
      })),
    );
    const selectedSupplier = pickWarehouseSupplier(suppliers);
    const nextPrice = selectedSupplier ? calculateRubPrice(selectedSupplier.price, rate, markupCoefficient) : 0;
    const currentPrice = priceMap.get(product.id) || null;
    const ozonMinPrice = product.marketplace === "ozon" ? minPriceMap.get(product.id) || null : null;

    return {
      ...product,
      markupCoefficient,
      currentPrice,
      ozonMinPrice,
      nextPrice,
      changed: nextPrice > 0 && nextPrice !== currentPrice,
      ready: Boolean(selectedSupplier && nextPrice > 0),
      selectedSupplier,
      suppliers,
      supplierCount: suppliers.length,
      availableSupplierCount: suppliers.filter((supplier) => supplier.available).length,
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
    withoutSupplier: products.filter((product) => !product.selectedSupplier).length,
    ozonArchived: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "archived").length,
    ozonInactive: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "inactive").length,
    ozonOutOfStock: products.filter((product) => product.marketplace === "ozon" && product.marketplaceState?.code === "out_of_stock").length,
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

app.get("/api/offers", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.query.limit, 150, 500);
    const search = String(request.query.search || "").trim();
    const partner = String(request.query.partner || "").trim();
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

    response.json(rows);
  } catch (error) {
    next(error);
  }
});

app.get("/api/partners", async (_request, response, next) => {
  try {
    const [rows] = await pool.query(`
      SELECT p.PartnerID AS id, p.PartnerName AS name, MAX(d.DocDate) AS latestDocDate
      FROM Partners p
      JOIN OfferDocs d ON d.PartnerID = p.PartnerID
      GROUP BY p.PartnerID, p.PartnerName
      ORDER BY p.PartnerName
    `);
    response.json(rows);
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
  response.json({
    defaults: {
      usdRate: Number(process.env.DEFAULT_USD_RATE || 95),
      ozonMarkup: Number(process.env.DEFAULT_OZON_MARKUP || 1.7),
      yandexMarkup: Number(process.env.DEFAULT_YANDEX_MARKUP || 1.6),
    },
    targets: marketplaceTargets(),
    accounts: getMarketplaceAccounts().map(sanitizeMarketplaceAccount),
    hiddenAccounts: getHiddenMarketplaceAccounts().map(sanitizeMarketplaceAccount),
  });
});

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
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplaces/prices/preview", async (request, response, next) => {
  try {
    const limit = cleanLimit(request.body.limit, 200, 100000);
    const search = String(request.body.search || "").trim();
    const ratePayload = request.body.usdRate
      ? { rate: Number(request.body.usdRate) }
      : await getUsdRate();
    const usdRate = Number(ratePayload.rate || process.env.DEFAULT_USD_RATE || 95);
    const targetMarkups = {
      ozon: Number(request.body.ozonMarkup || process.env.DEFAULT_OZON_MARKUP || 1.7),
      yandex: Number(request.body.yandexMarkup || process.env.DEFAULT_YANDEX_MARKUP || 1.6),
    };
    const supplierMarkups = parseSupplierMarkups(request.body.supplierMarkups);
    const targets = Array.isArray(request.body.targets) ? request.body.targets : undefined;
    response.json(
      await buildMarketplacePreview({
        limit,
        search,
        usdRate,
        targetMarkups,
        supplierMarkups,
        targets,
      }),
    );
  } catch (error) {
    next(error);
  }
});

app.post("/api/marketplaces/prices/send", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Prices were not sent because manual confirmation is required.",
      });
    }

    const items = Array.isArray(request.body.items) ? request.body.items : [];
    const results = [];

    for (const account of getOzonAccounts()) {
      const ozonItems = items
        .filter((item) => item.target === account.id)
        .map((item) => ({
          offer_id: String(item.offerId || "").trim(),
          price: String(roundPrice(item.price)),
          currency_code: "RUB",
        }))
        .filter((item) => item.offer_id && Number(item.price) > 0);

      for (const chunk of chunkArray(ozonItems, 1000)) {
        results.push({
          target: account.id,
          response: await ozonRequest("/v1/product/import/prices", { prices: chunk }, account),
        });
      }
    }

    for (const shop of getYandexShops()) {
      const yandexItems = items
        .filter((item) => item.target === shop.id)
        .map((item) => ({
          offerId: String(item.offerId || "").trim(),
          price: {
            value: roundPrice(item.price),
            currencyId: "RUR",
          },
        }))
        .filter((item) => item.offerId && item.price.value > 0);

      for (const chunk of chunkArray(yandexItems, 500)) {
        results.push({
          target: shop.id,
          response: await yandexRequest(
            shop,
            "POST",
            `/v2/businesses/${shop.businessId}/offer-prices/updates`,
            { offers: chunk },
          ),
        });
      }
    }

    response.json({
      ok: true,
      sent: items.length,
      results,
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
    response.json(await buildWarehouseView({ sync, limit, usdRate, refreshPrices }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/suppliers", async (_request, response, next) => {
  try {
    const warehouse = await readWarehouse();
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
      updatedAt: new Date().toISOString(),
    });

    const saved = await writeWarehouse(warehouse);
    await appendAudit(request, "supplier.update", { id: supplier.id, stopped: supplier.stopped, name: supplier.name });
    response.json({ ok: true, warehouse: saved });
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

    if (request.body.markup !== undefined) {
      const markup = Number(request.body.markup);
      product.markup = Number.isFinite(markup) && markup > 0 ? markup : 0;
    }
    if (request.body.keyword !== undefined) product.keyword = cleanText(request.body.keyword);
    product.updatedAt = new Date().toISOString();

    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/warehouse/products/markups/bulk", async (request, response, next) => {
  try {
    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    const markup = Number(request.body.markup);
    if (!ids.size) return response.status(400).json({ error: "Выберите товары для изменения наценки." });
    if (!Number.isFinite(markup) || markup <= 0) return response.status(400).json({ error: "Укажите наценку больше нуля." });

    const warehouse = await readWarehouse();
    let changed = 0;
    for (const product of warehouse.products) {
      if (!ids.has(product.id)) continue;
      product.markup = markup;
      product.updatedAt = new Date().toISOString();
      changed += 1;
    }

    response.json({ ok: true, changed, warehouse: await writeWarehouse(warehouse) });
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
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
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
    response.json({ ok: true, warehouse: await writeWarehouse(warehouse) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/prices/send", async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Prices were not sent because manual confirmation is required." });
    }

    const ids = new Set((Array.isArray(request.body.productIds) ? request.body.productIds : []).map(String));
    const preview = await buildWarehouseView({ usdRate: Number(request.body.usdRate || 0) || undefined });
    const items = preview.products
      .filter((product) => ids.has(product.id) && product.ready)
      .map((product) => ({
        id: product.id,
        target: product.target,
        offerId: product.offerId,
        price: product.nextPrice,
        oldPrice: product.currentPrice,
        markup: product.markupCoefficient,
        supplier: product.selectedSupplier,
        marketplace: product.marketplace,
      }));

    request.body.items = items;
    request.body.confirmed = true;

    const results = [];
    for (const account of getOzonAccounts()) {
      const ozonItems = items
        .filter((item) => item.target === account.id)
        .map((item) => ({
          offer_id: String(item.offerId || "").trim(),
          price: String(roundPrice(item.price)),
          currency_code: "RUB",
        }))
        .filter((item) => item.offer_id && Number(item.price) > 0);

      for (const chunk of chunkArray(ozonItems, 1000)) {
        results.push({ target: account.id, response: await ozonRequest("/v1/product/import/prices", { prices: chunk }, account) });
      }
    }

    for (const shop of getYandexShops()) {
      const yandexItems = items
        .filter((item) => item.target === shop.id)
        .map((item) => ({
          offerId: String(item.offerId || "").trim(),
          price: { value: roundPrice(item.price), currencyId: "RUR" },
        }))
        .filter((item) => item.offerId && item.price.value > 0);

      for (const chunk of chunkArray(yandexItems, 500)) {
        results.push({
          target: shop.id,
          response: await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/offer-prices/updates`, { offers: chunk }),
        });
      }
    }

    const warehouse = await readWarehouse();
    const sentAt = new Date().toISOString();
    for (const item of items) {
      const product = warehouse.products.find((entry) => entry.id === item.id);
      if (!product) continue;
      product.marketplacePrice = roundPrice(item.price);
      product.priceHistory = Array.isArray(product.priceHistory) ? product.priceHistory : [];
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
        usdRate: Number(request.body.usdRate || 0) || null,
      });
      product.priceHistory = product.priceHistory.slice(-100);
      product.updatedAt = sentAt;
    }
    await writeWarehouse(warehouse);

    response.json({ ok: true, sent: items.length, results });
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
      console.log(`Daily sync ${result.status}: ${result.lastRunAt}`);
    } catch (error) {
      console.error("Daily sync failed:", error.code || error.message);
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

app.use((error, _request, response, _next) => {
  console.error(error);
  const uploadError = error instanceof multer.MulterError;
  response.status(uploadError ? 400 : error.statusCode || 500).json({
    error: uploadError ? "Не удалось загрузить изображение" : "Не удалось выполнить запрос к Price Master",
    detail: error.code || error.message,
    ozon: error.ozon,
  });
});

app.listen(port, () => {
  console.log(`Price Master site is running at http://localhost:${port}`);
  if (autoSyncMinutes > 0) {
    console.log(`Auto sync is enabled: every ${autoSyncMinutes} minutes`);
  }
  if (dailySyncEnabled) {
    console.log(`Daily sync is enabled at ${dailySyncTime}`);
  }
});

scheduleDailySync();

if (autoSyncMinutes > 0) {
  setInterval(async () => {
    try {
      const result = await runSync();
      console.log(
        `Auto sync complete: ${result.items} items, ${result.changes} changes at ${result.createdAt}`,
      );
    } catch (error) {
      console.error("Auto sync failed:", error.code || error.message);
    }
  }, autoSyncMinutes * 60 * 1000);
}
