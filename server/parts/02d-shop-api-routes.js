// ─── Magic Vibes Shop API ──────────────────────────────────────────────────
const _shopCrypto = require("crypto");
const { ipKeyGenerator: _shopIpKeyGenerator } = require("express-rate-limit");

const shopLoginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req, res) => {
    const email = cleanText((req.body?.email || "")).toLowerCase().slice(0, 100);
    return `${_shopIpKeyGenerator(req, res)}:${email}`;
  },
  handler: (_req, res) => res.status(429).json({ error: "Слишком много попыток. Попробуйте через 15 минут." }),
});
const _shopScryptAsync = require("util").promisify(_shopCrypto.scrypt);

async function _shopHashPassword(pw) {
  const salt = _shopCrypto.randomBytes(16).toString("hex");
  const buf = await _shopScryptAsync(pw, salt, 64);
  return buf.toString("hex") + "." + salt;
}
async function _shopVerifyPassword(pw, stored) {
  const [hex, salt] = stored.split(".");
  if (!hex || !salt) return false;
  const buf = await _shopScryptAsync(pw, salt, 64);
  return _shopCrypto.timingSafeEqual(Buffer.from(hex, "hex"), buf);
}
function signShopToken(payload) {
  const { createHmac } = require("crypto");
  const secret = process.env.APP_SESSION_SECRET || "mv-shop-secret";
  const h = Buffer.from(JSON.stringify({ alg: "HS256" })).toString("base64url");
  const b = Buffer.from(JSON.stringify({ ...payload, iat: Date.now() })).toString("base64url");
  const sig = createHmac("sha256", secret).update(h + "." + b).digest("base64url");
  return h + "." + b + "." + sig;
}
function verifyShopToken(token) {
  const { createHmac } = require("crypto");
  const secret = process.env.APP_SESSION_SECRET || "mv-shop-secret";
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  const [h, b, sig] = parts;
  const expected = createHmac("sha256", secret).update(h + "." + b).digest("base64url");
  if (sig !== expected) return null;
  try { return JSON.parse(Buffer.from(b, "base64url").toString()); } catch { return null; }
}
async function requireShopAuth(request, response, next) {
  const auth = request.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  const payload = verifyShopToken(token);
  if (!payload?.customerId) return response.status(401).json({ error: "Требуется авторизация" });
  request.shopCustomer = payload;
  next();
}

function extractImages(p) {
  // images column can be: array of URLs, { imageUrl, images: [] }, or null
  if (Array.isArray(p.images) && p.images.length) return p.images.filter(Boolean);
  if (p.images && typeof p.images === "object" && !Array.isArray(p.images)) {
    const obj = p.images;
    const urls = [];
    if (obj.imageUrl) urls.push(obj.imageUrl);
    if (Array.isArray(obj.images)) urls.push(...obj.images);
    if (urls.length) return urls.filter(Boolean);
  }
  // Fall back to raw column
  if (p.raw && typeof p.raw === "object") {
    const raw = p.raw;
    if (Array.isArray(raw.ozon?.images) && raw.ozon.images.length) return raw.ozon.images.filter(Boolean);
    if (raw.ozon?.primaryImage) return [raw.ozon.primaryImage].filter(Boolean);
    if (Array.isArray(raw.yandex?.pictures) && raw.yandex.pictures.length) return raw.yandex.pictures.filter(Boolean);
    if (raw.imageUrl) return [raw.imageUrl];
  }
  return [];
}
// Публичные эндпоинты для магазина (без сессии, CORS разрешён для shopOrigin).
// Адм. эндпоинты /api/shop/admin/* требуют requireAdmin.

const SHOP_SETTINGS_KEY = "shopSettings";
const SHOP_BANNERS_KEY = "shopBanners";
const SHOP_CATEGORIES_KEY = "shopCategories";

// Разрешённые Origins для CORS (сам магазин + localhost для разработки)
const SHOP_CORS_ORIGINS = (process.env.SHOP_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);

function shopCors(request, response, next) {
  const origin = request.headers.origin || "";
  if (
    SHOP_CORS_ORIGINS.includes(origin) ||
    /^http:\/\/localhost:\d+$/.test(origin) ||
    /^http:\/\/127\.0\.0\.1:\d+$/.test(origin)
  ) {
    response.setHeader("Access-Control-Allow-Origin", origin);
    response.setHeader("Access-Control-Allow-Credentials", "false");
    response.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
    response.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  }
  if (request.method === "OPTIONS") return response.sendStatus(204);
  return next();
}

// ── helpers ──────────────────────────────────────────────────────────────────

function defaultShopSettings() {
  return {
    markup: Number(process.env.DEFAULT_SHOP_MARKUP || 2.2),
    markupRules: [],
    shopName: "Magic Vibes",
    shopDescription: "Оригинальная парфюмерия и косметика с доставкой по России",
    contactEmail: process.env.SHOP_CONTACT_EMAIL || "",
    contactPhone: process.env.SHOP_CONTACT_PHONE || "",
    deliveryDays: 3,
    freeDeliveryFrom: 3000,
  };
}

function resolveShopMarkup(priceUsd, defaultMarkup, rules) {
  if (!Array.isArray(rules) || !rules.length || !(priceUsd > 0)) return defaultMarkup;
  const sorted = [...rules].filter(r => Number(r.coefficient) > 0).sort((a, b) => b.minUsd - a.minUsd);
  const matched = sorted.find(r => priceUsd >= Number(r.minUsd || 0));
  return Number(matched?.coefficient) > 0 ? Number(matched.coefficient) : defaultMarkup;
}

function normalizeShopMarkupRules(input) {
  if (!Array.isArray(input)) return [];
  return input
    .map(r => ({ minUsd: Math.max(0, Number(r.minUsd ?? r.min_usd ?? 0)), coefficient: Number(r.coefficient ?? 0) }))
    .filter(r => Number.isFinite(r.minUsd) && Number.isFinite(r.coefficient) && r.coefficient > 0)
    .sort((a, b) => a.minUsd - b.minUsd);
}

async function readShopSettings() {
  const appSettings = await readAppSettings();
  return { ...defaultShopSettings(), ...(appSettings[SHOP_SETTINGS_KEY] || {}) };
}

async function readShopBanners() {
  const appSettings = await readAppSettings();
  return Array.isArray(appSettings[SHOP_BANNERS_KEY]) ? appSettings[SHOP_BANNERS_KEY] : [];
}

async function readShopCategories() {
  const appSettings = await readAppSettings();
  return Array.isArray(appSettings[SHOP_CATEGORIES_KEY]) ? appSettings[SHOP_CATEGORIES_KEY] : [];
}

async function writeShopData(key, value) {
  const appSettings = await readAppSettings();
  await writeAppSettings({ ...appSettings, [key]: value });
}

function nanoid8() {
  return Math.random().toString(36).slice(2, 10);
}

// ── price calculation ──────────────────────────────────────────────────────

async function buildShopProductsFromDb({ q, brand, category, inStock, sort, page, pageSize, createdAfter }) {
  const prisma = getPrisma();
  if (!prisma) return { products: [], total: 0, brands: [] };

  const shopSettings = await readShopSettings();
  const defaultMarkup = shopSettings.markup || 2.2;
  const shopMarkupRules = shopSettings.markupRules || [];

  let usdRate = Number(process.env.DEFAULT_USD_RATE || 95);
  try { const r = await getUsdRate(); usdRate = Number(r?.rate || r || 95); } catch (_) {}
  if (!usdRate || usdRate < 1) usdRate = Number(process.env.DEFAULT_USD_RATE || 95);

  const skip = (page - 1) * pageSize;

  // Build where clause — только товары с активной привязкой и ценой
  const where = {
    archived: false,
    marketplace: { in: ["ozon", "yandex"] },
    NOT: { status: "deleted" },
    currentPrice: { gt: 0 },
    links: { some: {} },
    ...(createdAfter ? { createdAt: { gte: createdAfter } } : {}),
  };
  if (q) {
    where.OR = [
      { name: { contains: q, mode: "insensitive" } },
      { brand: { contains: q, mode: "insensitive" } },
      { offerId: { contains: q, mode: "insensitive" } },
    ];
  }
  if (brand) {
    where.OR = [
      ...(where.OR || []),
      { brand: { contains: brand, mode: "insensitive" } },
    ];
    if (!q) {
      where.brand = { contains: brand, mode: "insensitive" };
      delete where.OR;
    }
  }
  if (category && category !== "parfumery") {
    const _catDef = SHOP_CATEGORIES.find((c) => c.slug === category);
    if (_catDef && _catDef.keywords.length) {
      const _catOr = _catDef.keywords.map((kw) => ({ name: { contains: kw, mode: "insensitive" } }));
      where.AND = [{ OR: _catOr }];
    }
  }

  // De-duplicate by offerId: prefer Ozon over Yandex
  // over-fetch 2x для компенсации дублей ozon+yandex
  const [rawProducts, total] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where,
      select: {
        id: true, offerId: true, name: true, brand: true, marketplace: true,
        images: true, raw: true, currentPrice: true, targetStock: true, status: true,
        marketplaceState: true,
        links: { take: 2, select: { supplierArticle: true } },
      },
      orderBy: [
        { marketplace: "asc" }, // ozon < yandex — ensures ozon version wins de-dup
        sort === "price_asc" ? { currentPrice: "asc" } : sort === "price_desc" ? { currentPrice: "desc" } : { name: "asc" },
      ],
      take: pageSize * 2,
      skip,
    }),
    prisma.warehouseProduct.count({ where }),
  ]);

  // De-duplicate offerId: one product card per article
  const seen = new Set();
  const deduped = [];
  for (const p of rawProducts) {
    const key = cleanText(p.offerId).toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(p);
    if (deduped.length >= pageSize) break;
  }

  // Collect supplier articles for PM price lookup
  const articles = deduped
    .flatMap((p) => p.links.map((l) => cleanText(l.supplierArticle)))
    .filter(Boolean);

  const dedupedOfferIds = deduped.map((p) => p.offerId).filter(Boolean);

  const [pmSnaps, reviewGroups] = await Promise.all([
    articles.length ? prisma.priceMasterSnapshotItem.findMany({
      where: { article: { in: articles }, active: true },
      select: { article: true, price: true, currency: true },
    }) : [],
    dedupedOfferIds.length ? prisma.shopReview.groupBy({
      by: ["offerId"],
      where: { approved: true, offerId: { in: dedupedOfferIds } },
      _avg: { rating: true },
      _count: { rating: true },
    }) : [],
  ]);

  const pmMap = new Map();
  for (const snap of pmSnaps) {
    const cur = pmMap.get(snap.article);
    const price = Number(snap.price || 0);
    if (!cur || price < Number(cur.price)) pmMap.set(snap.article, snap);
  }

  const reviewMap = new Map();
  for (const g of reviewGroups) {
    if (g.offerId) reviewMap.set(g.offerId, { avg: g._avg.rating || 0, count: g._count.rating || 0 });
  }

  // Build shop products
  const products = deduped.map((p) => {
    const images = extractImages(p);
    const link = p.links[0];
    const snap = link ? pmMap.get(cleanText(link.supplierArticle)) : null;
    const priceUsd = snap ? Number(snap.price || 0) : 0;
    const currentPriceNum = Number(p.currentPrice || 0);
    const markup = resolveShopMarkup(priceUsd, defaultMarkup, shopMarkupRules);
    const priceRub = priceUsd > 0
      ? Math.round(priceUsd * usdRate * markup)
      : currentPriceNum > 0 ? currentPriceNum : 0;

    const stockQty = p.targetStock ?? 0;
    const name = cleanText(p.name || "");
    const _cat = extractProductCategory(name);

    // Rating: prefer Ozon sync data (stored in marketplaceState), then our own ShopReview aggregate
    const ms = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
    const siteReview = reviewMap.get(p.offerId);
    const rating = Number(ms.ozonRating || 0) || Number(siteReview?.avg || 0) || 0;
    const reviewCount = Number(ms.ozonReviewCount || 0) || Number(siteReview?.count || 0) || 0;

    return {
      id: p.id,
      offerId: p.offerId,
      name: name || cleanText(p.offerId),
      brand: cleanText(p.brand || ""),
      description: "",
      images,
      priceRub,
      inStock: stockQty > 0 || (p.status !== "archived" && currentPriceNum > 0),
      stockQty: Math.max(0, stockQty),
      volume: extractVolume(p.name || ""),
      category: _cat.slug,
      categoryLabel: _cat.label,
      tags: [],
      rating: Math.round(rating * 10) / 10,
      reviewCount,
    };
  // Всегда требуем цену > 0 и нормальное название
  }).filter((p) => p.priceRub > 0 && p.name.length > 1);

  const filtered = inStock ? products.filter((p) => p.inStock) : products;

  // Extract unique brands for filter sidebar
  const brands = [...new Set(
    rawProducts.map((p) => cleanText(p.brand || "")).filter(Boolean)
  )].sort();

  return { products: filtered, total, brands };
}

const SHOP_CATEGORIES = [
  { slug: "testers", label: "Тестеры и отливанты", pattern: /тестер|tester|отливант|decant|пробник/i,                    keywords: ["тестер", "tester", "отливант", "decant", "пробник"] },
  { slug: "parfum",  label: "Духи",               pattern: /духи|extrait|pure[\s-]parfum/i,                              keywords: ["духи", "extrait", "pure parfum"] },
  { slug: "edp",     label: "Парфюмерная вода",   pattern: /парфюм[\s-]?(ерная)?\s*вода|eau[\s-]de[\s-]parfum|\bedp\b/i, keywords: ["парфюмерная вода", "eau de parfum"] },
  { slug: "edt",    label: "Туалетная вода",    pattern: /туалет\w*\s*вода|eau[\s-]de[\s-]toilette|\bedt\b/i,           keywords: ["туалетная вода", "eau de toilette"] },
  { slug: "edc",    label: "Одеколон",          pattern: /одеколон|eau[\s-]de[\s-]cologne|\bedc\b/i,                    keywords: ["одеколон", "eau de cologne"] },
  { slug: "deo",    label: "Дезодоранты",       pattern: /дезодорант|антиперспирант|deodorant/i,                        keywords: ["дезодорант", "антиперспирант", "deodorant"] },
  { slug: "home",   label: "Ароматы для дома",  pattern: /свеч[аи]|аромасвеч|candle/i,                                 keywords: ["свеча", "свечи", "аромасвеча", "candle"] },
  { slug: "sets",   label: "Подарочные наборы", pattern: /набор|gift[\s-]set/i,                                         keywords: ["набор", "gift set"] },
  { slug: "body",   label: "Уход за телом",     pattern: /крем|лосьон|масло.{0,8}тел|гель.{0,8}душ|шампун/i,           keywords: ["крем", "лосьон", "масло для тела", "гель для душа", "шампунь"] },
];

function extractVolume(name = "") {
  const m = name.match(/(\d+\s*(?:мл|ml|г|g|oz)\b)/i);
  return m ? m[1] : undefined;
}

function extractProductCategory(name = "") {
  for (const cat of SHOP_CATEGORIES) {
    if (cat.pattern.test(name)) return { slug: cat.slug, label: cat.label };
  }
  return { slug: "parfumery", label: "Парфюмерия" };
}

async function findShopProductByOfferId(offerId) {
  const prisma = getPrisma();
  if (!prisma) return null;

  const shopSettings = await readShopSettings();
  const defaultMarkupSingle = shopSettings.markup || 2.2;
  const shopMarkupRulesSingle = shopSettings.markupRules || [];
  let usdRate = Number(process.env.DEFAULT_USD_RATE || 95);
  try { const r = await getUsdRate(); usdRate = Number(r?.rate || r || 95); } catch (_) {}
  if (!usdRate || usdRate < 1) usdRate = Number(process.env.DEFAULT_USD_RATE || 95);

  const products = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: offerId, mode: "insensitive" }, archived: false },
    select: {
      id: true, offerId: true, name: true, brand: true, marketplace: true,
      images: true, raw: true, currentPrice: true, targetStock: true, status: true,
      marketplaceState: true,
      links: { take: 3, select: { supplierArticle: true } },
    },
    take: 5,
  });

  if (!products.length) return null;

  // prefer Ozon
  const p = products.find((pr) => pr.marketplace === "ozon") || products[0];
  const images = extractImages(p);

  let description = "";
  try {
    const state = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
    description = cleanText(state.description || state.desc || state.productDescription || "");
    if (!description) {
      const raw = p.raw && typeof p.raw === "object" ? p.raw : {};
      description = cleanText(raw.ozon?.description || raw.yandex?.description || "");
    }
  } catch (_) {}

  const articles = p.links.map((l) => cleanText(l.supplierArticle)).filter(Boolean);
  let priceUsd = 0;
  if (articles.length) {
    const snaps = await prisma.priceMasterSnapshotItem.findMany({
      where: { article: { in: articles }, active: true },
      select: { price: true },
      orderBy: { price: "asc" },
      take: 1,
    });
    if (snaps[0]) priceUsd = Number(snaps[0].price || 0);
  }

  const currentPriceNum = Number(p.currentPrice || 0);
  const resolvedMarkup = resolveShopMarkup(priceUsd, defaultMarkupSingle, shopMarkupRulesSingle);
  const priceRub = priceUsd > 0
    ? Math.round(priceUsd * usdRate * resolvedMarkup)
    : currentPriceNum > 0 ? currentPriceNum : 0;
  const _pCat = extractProductCategory(cleanText(p.name || ""));

  // Rating: Ozon sync data from marketplaceState, or aggregate from ShopReview
  const ms2 = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
  let singleRating = Number(ms2.ozonRating || 0);
  let singleReviewCount = Number(ms2.ozonReviewCount || 0);
  if (!singleRating) {
    const rg = await prisma.shopReview.aggregate({
      where: { offerId: p.offerId, approved: true },
      _avg: { rating: true },
      _count: { rating: true },
    });
    singleRating = Number(rg._avg.rating || 0);
    singleReviewCount = Number(rg._count.rating || 0);
  }

  return {
    id: p.id,
    offerId: p.offerId,
    name: cleanText(p.name || p.offerId),
    brand: cleanText(p.brand || ""),
    description,
    images,
    priceRub,
    inStock: (p.targetStock ?? 0) > 0 || (p.status !== "archived" && currentPriceNum > 0),
    stockQty: Math.max(0, p.targetStock ?? 0),
    volume: extractVolume(p.name || ""),
    category: _pCat.slug,
    categoryLabel: _pCat.label,
    tags: [],
    rating: Math.round(singleRating * 10) / 10,
    reviewCount: singleReviewCount,
  };
}

// ── CORS middleware for all shop routes (incl. OPTIONS preflight) ──────────
app.use("/api/shop", shopCors);

// ── Public routes ─────────────────────────────────────────────────────────

app.get("/api/shop/catalog", shopCors, async (request, response, next) => {
  try {
    const page = Math.max(1, Number(request.query.page || 1) || 1);
    const pageSize = Math.max(1, Math.min(96, Number(request.query.pageSize || 24) || 24));
    const q = cleanText(request.query.q || "");
    const brand = cleanText(request.query.brand || "");
    const category = cleanText(request.query.category || "");
    const inStock = request.query.inStock === "true";
    const sort = ["price_asc", "price_desc", "name"].includes(request.query.sort) ? request.query.sort : "name";

    const result = await buildShopProductsFromDb({ q, brand, category, inStock, sort, page, pageSize });
    response.json({ ok: true, ...result, page, pageSize });
  } catch (error) {
    next(error);
  }
});

app.get("/api/shop/product/:offerId", shopCors, async (request, response, next) => {
  try {
    const offerId = cleanText(request.params.offerId || "");
    if (!offerId) return response.status(400).json({ error: "offerId required" });
    const product = await findShopProductByOfferId(offerId);
    if (!product) return response.status(404).json({ error: "Товар не найден" });
    response.json({ ok: true, ...product });
  } catch (error) {
    next(error);
  }
});

app.get("/api/shop/banners", shopCors, async (_request, response, next) => {
  try {
    const banners = await readShopBanners();
    response.json(banners.filter((b) => b.active).sort((a, b) => a.order - b.order));
  } catch (error) {
    next(error);
  }
});

app.get("/api/shop/categories", shopCors, async (_request, response, next) => {
  try {
    const cats = await readShopCategories();
    response.json(cats.sort((a, b) => a.order - b.order));
  } catch (error) {
    next(error);
  }
});

app.get("/api/shop/auto-categories", shopCors, async (_request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json([]);
    const allProds = await prisma.warehouseProduct.findMany({
      where: { archived: false, marketplace: { in: ["ozon", "yandex"] }, NOT: { status: "deleted" }, currentPrice: { gt: 0 }, links: { some: {} } },
      select: { offerId: true, name: true },
    });
    const seen = new Set();
    const counts = {};
    for (const p of allProds) {
      const key = (p.offerId || "").trim().toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      const cat = extractProductCategory(cleanText(p.name || ""));
      counts[cat.slug] = (counts[cat.slug] || 0) + 1;
    }
    const ORDER = ["edp", "edt", "parfum", "edc", "testers", "deo", "sets", "body", "home"];
    const result = ORDER
      .map((slug) => {
        const def = SHOP_CATEGORIES.find((c) => c.slug === slug);
        return { slug, label: def ? def.label : slug, count: counts[slug] || 0 };
      })
      .filter((c) => c.count > 0);
    Object.entries(counts)
      .filter(([slug]) => !ORDER.includes(slug) && (counts[slug] || 0) > 0)
      .sort(([, a], [, b]) => b - a)
      .forEach(([slug, count]) => {
        const def = SHOP_CATEGORIES.find((c) => c.slug === slug);
        const label = def ? def.label : slug === "parfumery" ? "Парфюмерия" : slug;
        result.push({ slug, label, count });
      });
    response.json(result);
  } catch (error) { next(error); }
});

app.get("/api/shop/new", shopCors, async (request, response, next) => {
  try {
    const days = Math.max(1, Math.min(90, Number(request.query.days || 14) || 14));
    const pageSize = Math.max(1, Math.min(96, Number(request.query.pageSize || 24) || 24));
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const result = await buildShopProductsFromDb({ q: "", brand: "", category: "", inStock: false, sort: "name", page: 1, pageSize, createdAfter: cutoff });
    response.json({ ok: true, ...result, days });
  } catch (error) { next(error); }
});

// ── Popular products (last 30 days by marketplace + site sales) ───────────────
app.get("/api/shop/popular", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, products: [] });

    const limit = Math.max(4, Math.min(20, Number(request.query.limit || 8) || 8));
    const since30d = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

    // Count from marketplace sales (FinanceOrder — Ozon/Yandex real orders)
    const financeOrders = await prisma.financeOrder.findMany({
      where: { soldAt: { gte: since30d }, offerId: { not: null }, status: { not: "cancelled" } },
      select: { offerId: true, quantity: true },
    });

    // Count from our own shop orders (ShopOrder.items is JSON)
    const shopOrders = await prisma.shopOrder.findMany({
      where: { createdAt: { gte: since30d }, status: { not: "cancelled" } },
      select: { items: true },
    });

    // Merge counts
    const scoreMap = new Map();
    const addScore = (offerId, qty) => {
      if (!offerId) return;
      const key = cleanText(offerId);
      if (!key) return;
      scoreMap.set(key, (scoreMap.get(key) || 0) + qty);
    };

    for (const fo of financeOrders) {
      addScore(fo.offerId, Number(fo.quantity || 1));
    }
    for (const so of shopOrders) {
      const items = Array.isArray(so.items) ? so.items : (so.items ? [so.items] : []);
      for (const item of items) {
        if (item && item.offerId) addScore(item.offerId, Number(item.quantity || 1));
      }
    }

    // Sort by score, take top N
    const topOfferIds = [...scoreMap.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit * 3) // fetch more to deduplicate
      .map(([id]) => id);

    if (!topOfferIds.length) {
      // Fallback: return products with highest currentPrice (proxy for premium / popular)
      const result = await buildShopProductsFromDb({ q: "", brand: "", category: "", inStock: true, sort: "name", page: 1, pageSize: limit });
      return response.json({ ok: true, products: result.products.slice(0, limit), source: "fallback" });
    }

    // Fetch matching warehouse products
    const shopSettings = await readShopSettings();
    const defaultMarkup = shopSettings.markup || 2.2;
    const shopMarkupRules = shopSettings.markupRules || [];
    let usdRate = Number(process.env.DEFAULT_USD_RATE || 95);
    try { const r = await getUsdRate(); usdRate = Number(r?.rate || r || 95); } catch (_) {}
    if (!usdRate || usdRate < 1) usdRate = Number(process.env.DEFAULT_USD_RATE || 95);

    const warehouseProducts = await prisma.warehouseProduct.findMany({
      where: {
        offerId: { in: topOfferIds },
        archived: false,
        marketplace: { in: ["ozon", "yandex"] },
        NOT: { status: "deleted" },
        currentPrice: { gt: 0 },
        links: { some: {} },
      },
      select: {
        id: true, offerId: true, name: true, brand: true, marketplace: true,
        images: true, raw: true, currentPrice: true, targetStock: true, status: true,
        marketplaceState: true,
        links: { take: 1, select: { supplierArticle: true } },
      },
      orderBy: [{ marketplace: "asc" }],
    });

    // De-duplicate by offerId
    const seen = new Set();
    const deduped = [];
    for (const p of warehouseProducts) {
      const key = cleanText(p.offerId).toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      deduped.push(p);
    }

    // PM price lookup
    const arts = deduped.flatMap((p) => p.links.map((l) => cleanText(l.supplierArticle))).filter(Boolean);
    const snaps = arts.length ? await prisma.priceMasterSnapshotItem.findMany({
      where: { article: { in: arts }, active: true },
      select: { article: true, price: true, currency: true },
    }) : [];
    const pmMap2 = new Map();
    for (const s of snaps) {
      const cur = pmMap2.get(s.article);
      if (!cur || Number(s.price) < Number(cur.price)) pmMap2.set(s.article, s);
    }

    // Ratings
    const offerIdsForRatings = deduped.map((p) => p.offerId).filter(Boolean);
    const ratingGroups = offerIdsForRatings.length ? await prisma.shopReview.groupBy({
      by: ["offerId"],
      where: { approved: true, offerId: { in: offerIdsForRatings } },
      _avg: { rating: true },
      _count: { rating: true },
    }) : [];
    const ratingMap = new Map();
    for (const g of ratingGroups) {
      if (g.offerId) ratingMap.set(g.offerId, { avg: g._avg.rating || 0, count: g._count.rating || 0 });
    }

    const products = deduped
      .map((p) => {
        const images = extractImages(p);
        const link = p.links[0];
        const snap = link ? pmMap2.get(cleanText(link.supplierArticle)) : null;
        const priceUsd = snap ? Number(snap.price || 0) : 0;
        const currentPriceNum = Number(p.currentPrice || 0);
        const markup = resolveShopMarkup(priceUsd, defaultMarkup, shopMarkupRules);
        const priceRub = priceUsd > 0
          ? Math.round(priceUsd * usdRate * markup)
          : currentPriceNum > 0 ? currentPriceNum : 0;
        const stockQty = p.targetStock ?? 0;
        const name = cleanText(p.name || "");
        const _cat = extractProductCategory(name);
        const ms = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
        const sr = ratingMap.get(p.offerId);
        const rating = Number(ms.ozonRating || 0) || Number(sr?.avg || 0) || 0;
        const reviewCount = Number(ms.ozonReviewCount || 0) || Number(sr?.count || 0) || 0;
        return {
          id: p.id, offerId: p.offerId,
          name: name || cleanText(p.offerId),
          brand: cleanText(p.brand || ""),
          description: "", images, priceRub,
          inStock: stockQty > 0 || (p.status !== "archived" && currentPriceNum > 0),
          stockQty: Math.max(0, stockQty),
          volume: extractVolume(p.name || ""),
          category: _cat.slug, categoryLabel: _cat.label, tags: [],
          rating: Math.round(rating * 10) / 10, reviewCount,
          salesScore: scoreMap.get(cleanText(p.offerId).toLowerCase()) || 0,
        };
      })
      .filter((p) => p.priceRub > 0 && p.name.length > 1)
      .sort((a, b) => b.salesScore - a.salesScore)
      .slice(0, limit);

    response.json({ ok: true, products, source: "sales" });
  } catch (error) { next(error); }
});

// ── Marketplace reviews for a product (Ozon, cached 6h) ─────────────────────
const _mpReviewCache = new Map(); // key → { reviews, avgRating, reviewCount, expiresAt }
const _MP_REVIEW_TTL = 6 * 60 * 60 * 1000;

app.get("/api/shop/marketplace-reviews", shopCors, async (request, response, next) => {
  try {
    const offerId = cleanText(request.query.offerId || "");
    if (!offerId) return response.json({ ok: true, reviews: [], avgRating: 0, reviewCount: 0 });

    const cacheKey = `ozon:${offerId}`;
    const cached = _mpReviewCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return response.json({ ok: true, reviews: cached.reviews, avgRating: cached.avgRating, reviewCount: cached.reviewCount });
    }

    const prisma = getPrisma();
    let productId = null;
    let wpId = null;
    let wpState = {};

    if (prisma) {
      const wp = await prisma.warehouseProduct.findFirst({
        where: { offerId: { equals: offerId, mode: "insensitive" }, marketplace: "ozon", archived: false },
        select: { id: true, productId: true, marketplaceState: true },
      });
      if (wp) {
        productId = wp.productId ? Number(wp.productId) : null;
        wpId = wp.id;
        wpState = wp.marketplaceState && typeof wp.marketplaceState === "object" ? wp.marketplaceState : {};
        // L2: DB-persisted reviews survive server restarts/deploys
        const cachedAt = wpState.ozonReviewsCachedAt ? new Date(wpState.ozonReviewsCachedAt).getTime() : 0;
        if (Array.isArray(wpState.ozonReviews) && wpState.ozonReviews.length > 0 && cachedAt > Date.now() - _MP_REVIEW_TTL) {
          const entry = { reviews: wpState.ozonReviews, avgRating: wpState.ozonRating || 0, reviewCount: wpState.ozonReviewCount || 0, expiresAt: cachedAt + _MP_REVIEW_TTL };
          _mpReviewCache.set(cacheKey, entry);
          return response.json({ ok: true, reviews: entry.reviews, avgRating: entry.avgRating, reviewCount: entry.reviewCount });
        }
      }
    }

    const accounts = getOzonAccounts().filter((a) => a.clientId && a.apiKey);
    if (!accounts.length || !productId) {
      _mpReviewCache.set(cacheKey, { reviews: [], avgRating: 0, reviewCount: 0, expiresAt: Date.now() + 30 * 60 * 1000 });
      return response.json({ ok: true, reviews: [], avgRating: 0, reviewCount: 0 });
    }

    // Fetch reviews — fetch up to 100 so we can filter client-side by offer_id.
    // Ozon's sku_ids filter may be unreliable (ignored on non-premium tiers),
    // so we always post-filter by review.offer_id === our offerId.
    const offerIdLower = offerId.toLowerCase();
    let rawReviews = [];
    for (const account of accounts) {
      try {
        const data = await ozonRequest("/v1/review/list", {
          limit: 100,
          sort_dir: "DESC",
          ...(productId ? { sku_ids: [productId] } : {}),
        }, account);
        const list = Array.isArray(data?.result) ? data.result : Array.isArray(data?.reviews) ? data.reviews : [];
        // Filter to only reviews that belong to this product (match offer_id or sku)
        const matched = list.filter((r) => {
          const rOffer = cleanText(r.offer_id || "").toLowerCase();
          const rSku = String(r.sku || "");
          return rOffer === offerIdLower || (productId && rSku === String(productId));
        });
        // If sku_ids filter worked perfectly, all reviews match — keep them all.
        // If nothing matched after filter, the sku_ids filter returned unrelated reviews — discard all.
        rawReviews = matched;
        break;
      } catch (_err) { /* try next account */ }
    }

    const reviews = rawReviews
      .filter((r) => r.text && r.text.trim().length > 2 && Number(r.rating || 5) >= 4)
      .map((r) => ({
        id: `ozon:${r.id || r.review_id || Math.random()}`,
        author: cleanText(r.author_name || "Покупатель Ozon") || "Покупатель Ozon",
        rating: Math.max(1, Math.min(5, Number(r.rating || 5))),
        text: cleanText(r.text || ""),
        advantages: cleanText(r.advantages || ""),
        disadvantages: cleanText(r.disadvantages || r.defects || ""),
        createdAt: r.published_at || r.created_at || new Date().toISOString(),
        source: "ozon",
        photos: Array.isArray(r.photos) ? r.photos.map((ph) => ph.url || ph).filter(Boolean) : [],
      }));

    const avgRating = reviews.length
      ? Math.round((reviews.reduce((s, r) => s + r.rating, 0) / reviews.length) * 10) / 10
      : 0;
    const reviewCount = rawReviews.length; // total, even if filtered

    _mpReviewCache.set(cacheKey, { reviews, avgRating, reviewCount, expiresAt: Date.now() + _MP_REVIEW_TTL });

    // Persist reviews + rating to DB so they survive restarts/deploys
    if (prisma && wpId) {
      try {
        await prisma.warehouseProduct.update({
          where: { id: wpId },
          data: {
            marketplaceState: {
              ...wpState,
              ozonRating: avgRating,
              ozonReviewCount: reviewCount,
              ozonRatingSyncedAt: new Date().toISOString(),
              ozonReviews: reviews,
              ozonReviewsCachedAt: new Date().toISOString(),
            },
          },
        });
      } catch (_) {}
    }

    response.json({ ok: true, reviews, avgRating, reviewCount });
  } catch (error) { next(error); }
});

app.get("/api/shop/brands", shopCors, async (_request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json([]);
    const rows = await prisma.warehouseProduct.findMany({
      where: { archived: false, marketplace: { in: ["ozon", "yandex"] }, NOT: { status: "deleted" }, currentPrice: { gt: 0 }, links: { some: {} }, brand: { not: null } },
      select: { offerId: true, brand: true },
    });
    const seen = new Set();
    const counts = {};
    for (const p of rows) {
      const key = (p.offerId || "").trim().toLowerCase();
      if (!key || seen.has(key) || !p.brand) continue;
      seen.add(key);
      const b = cleanText(p.brand).trim();
      if (b) counts[b] = (counts[b] || 0) + 1;
    }
    const brands = Object.entries(counts)
      .map(([name, count]) => ({ name, count }))
      .sort((a, b) => a.name.localeCompare(b.name, "ru"));
    response.json(brands);
  } catch (error) { next(error); }
});

app.get("/api/shop/settings", shopCors, async (_request, response, next) => {
  try {
    const settings = await readShopSettings();
    // Don't expose sensitive fields
    const { shopName, shopDescription, contactEmail, contactPhone, deliveryDays, freeDeliveryFrom } = settings;
    response.json({ shopName, shopDescription, contactEmail, contactPhone, deliveryDays, freeDeliveryFrom });
  } catch (error) {
    next(error);
  }
});

// Ozon PVZ — кэш полного списка точек (~93K, только координаты), TTL 24 ч
let _ozonPvzAllCache = null;
let _ozonPvzAllCacheAt = 0;
const _OZON_PVZ_LIST_TTL_MS = 24 * 60 * 60 * 1000;

async function _loadOzonPvzAll() {
  if (_ozonPvzAllCache && Date.now() - _ozonPvzAllCacheAt < _OZON_PVZ_LIST_TTL_MS) {
    return _ozonPvzAllCache;
  }
  const account = getOzonAccountByTarget("ozon");
  if (!account?.clientId || !account?.apiKey) return [];
  try {
    const res = await fetch("https://api-seller.ozon.ru/v1/delivery/point/list", {
      method: "POST",
      headers: { "Client-Id": account.clientId, "Api-Key": account.apiKey, "Content-Type": "application/json" },
      body: "{}",
      signal: AbortSignal.timeout(60000),
    });
    if (!res.ok) {
      logger.warn("ozon pvz list failed", { status: res.status });
      return _ozonPvzAllCache || [];
    }
    const data = await res.json();
    const points = data.points || [];
    _ozonPvzAllCache = points.map(p => ({ id: p.map_point_id, lat: p.coordinate?.lat ?? 0, lng: p.coordinate?.long ?? 0 }));
    _ozonPvzAllCacheAt = Date.now();
    logger.info("ozon pvz list loaded", { count: _ozonPvzAllCache.length });
    return _ozonPvzAllCache;
  } catch (err) {
    logger.warn("ozon pvz list error", { detail: err?.message });
    return _ozonPvzAllCache || [];
  }
}

function _pvzWorkingHoursToday(workingHours = []) {
  if (!workingHours.length) return null;
  const periods = workingHours[0]?.periods || [];
  if (!periods.length) return null;
  const p = periods[0];
  const fmt = (t) => `${String(t?.hours ?? 0).padStart(2, "0")}:${String(t?.minutes ?? 0).padStart(2, "0")}`;
  return `${fmt(p.min)}–${fmt(p.max)}`;
}

async function _fetchOzonPvzDetails(mapPointIds = []) {
  if (!mapPointIds.length) return [];
  const account = getOzonAccountByTarget("ozon");
  if (!account?.clientId || !account?.apiKey) return [];
  const results = [];
  for (let i = 0; i < mapPointIds.length; i += 100) {
    const batch = mapPointIds.slice(i, i + 100);
    try {
      const res = await fetch("https://api-seller.ozon.ru/v1/delivery/point/info", {
        method: "POST",
        headers: { "Client-Id": account.clientId, "Api-Key": account.apiKey, "Content-Type": "application/json" },
        body: JSON.stringify({ map_point_ids: batch }),
        signal: AbortSignal.timeout(20000),
      });
      if (!res.ok) continue;
      const data = await res.json();
      for (const point of (data.points || [])) {
        if (!point.enabled) continue;
        const dm = point.delivery_method || {};
        const addr = dm.address_details || {};
        const hoursToday = _pvzWorkingHoursToday(dm.working_hours);
        results.push({
          id: String(dm.map_point_id),
          name: cleanText(dm.name) || "Ozon ПВЗ",
          address: cleanText(dm.address) || [addr.city, addr.street, addr.house].filter(Boolean).join(", "),
          lat: dm.coordinates?.lat ?? 0,
          lng: dm.coordinates?.long ?? 0,
          schedule: hoursToday,
          type: cleanText(dm.delivery_type?.name) || "ПВЗ",
          city: cleanText(addr.city),
        });
      }
    } catch (error) {
      logger.warn("ozon pvz info batch failed", { detail: error?.message });
    }
  }
  return results;
}

// GET /api/shop/delivery/pvz?city=Москва  OR  ?lat=55.75&lng=37.62
// Ozon Seller API: /v1/delivery/point/list (cached 24h) + /v1/delivery/point/info
// City search: grid-sampling by bbox (равномерное покрытие города)
// Coords search: 80 ближайших к пользователю

function _gridSamplePvz(points, bboxS, bboxN, bboxW, bboxE, maxPoints = 80) {
  if (!points.length) return [];
  // Вычисляем размер сетки пропорционально bbox
  const aspect = Math.max(0.1, (bboxE - bboxW) / (bboxN - bboxS));
  const gridH = Math.max(1, Math.round(Math.sqrt(maxPoints / aspect)));
  const gridW = Math.max(1, Math.round(maxPoints / gridH));
  const cellH = (bboxN - bboxS) / gridH;
  const cellW = (bboxE - bboxW) / gridW;
  const used = new Set();
  const result = [];
  for (let row = 0; row < gridH && result.length < maxPoints; row++) {
    for (let col = 0; col < gridW && result.length < maxPoints; col++) {
      const clat = bboxS + (row + 0.5) * cellH;
      const clng = bboxW + (col + 0.5) * cellW;
      const cosLat = Math.cos((clat * Math.PI) / 180);
      let best = null, bestDist = Infinity;
      for (const p of points) {
        if (used.has(p.id)) continue;
        const d = Math.pow(p.lat - clat, 2) + Math.pow((p.lng - clng) * cosLat, 2);
        if (d < bestDist) { bestDist = d; best = p; }
      }
      if (best) { used.add(best.id); result.push(best); }
    }
  }
  return result;
}

app.get("/api/shop/delivery/pvz", shopCors, async (request, response, next) => {
  try {
    const city = String(request.query.city || "").trim().slice(0, 100);
    const latParam = parseFloat(String(request.query.lat || ""));
    const lngParam = parseFloat(String(request.query.lng || ""));
    const hasCoords = Number.isFinite(latParam) && Number.isFinite(lngParam);
    if (!city && !hasCoords) return response.status(400).json({ error: "city or lat/lng required" });

    const allPoints = await _loadOzonPvzAll();
    let nearby;

    if (hasCoords) {
      // Геолокация: 200 ближайших к пользователю в радиусе ~30 км
      const cLat = latParam, cLng = lngParam;
      const r = 0.27;
      const rLng = r / Math.max(0.3, Math.cos((cLat * Math.PI) / 180));
      const cosLat = Math.cos((cLat * Math.PI) / 180);
      nearby = allPoints
        .filter(p => p.lat && p.lng && Math.abs(p.lat - cLat) < r && Math.abs(p.lng - cLng) < rLng)
        .map(p => ({ ...p, dist: Math.pow(p.lat - cLat, 2) + Math.pow((p.lng - cLng) * cosLat, 2) }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 200);
      if (!nearby.length) return response.json({ pvz: [], city, source: "ozon", center: { lat: cLat, lng: cLng } });
    } else {
      // Поиск по городу: геокодируем → bbox → grid-sampling для равномерного охвата
      const geoRes = await fetch(
        `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(city + ", Россия")}&format=json&limit=1&featuretype=city`,
        { headers: { "User-Agent": "MagicVibesShop/1.0 (noreply@magicvibes.ru)" }, signal: AbortSignal.timeout(8000) }
      );
      if (!geoRes.ok) return response.json({ pvz: [], city, error: "geocode_failed" });
      const geoData = await geoRes.json();
      if (!geoData.length) return response.json({ pvz: [], city, error: "city_not_found" });
      const g = geoData[0];
      let bboxS, bboxN, bboxW, bboxE;
      if (Array.isArray(g.boundingbox) && g.boundingbox.length === 4) {
        [bboxS, bboxN, bboxW, bboxE] = g.boundingbox.map(Number);
      } else {
        const lat = parseFloat(g.lat), lng = parseFloat(g.lon);
        const r = 0.27, rLng = r / Math.max(0.3, Math.cos((lat * Math.PI) / 180));
        bboxS = lat - r; bboxN = lat + r; bboxW = lng - rLng; bboxE = lng + rLng;
      }
      const inBox = allPoints.filter(p => p.lat && p.lng && p.lat >= bboxS && p.lat <= bboxN && p.lng >= bboxW && p.lng <= bboxE);
      if (!inBox.length) return response.json({ pvz: [], city, error: "no_pvz_in_city" });
      // Равномерная сетка — 200 точек по всему городу
      nearby = _gridSamplePvz(inBox, bboxS, bboxN, bboxW, bboxE, 200);
    }

    const pvz = await _fetchOzonPvzDetails(nearby.map(p => p.id));
    response.json({ pvz, city, count: pvz.length, source: "ozon" });
  } catch (error) {
    next(error);
  }
});

// ── Ozon Pay Acquiring ────────────────────────────────────────────────────

const _ozonPayBaseUrl = "https://payapi.ozon.ru";
const _ozonPayShopBase = () => process.env.SHOP_BASE_URL || "https://magicvibes.ru";
const _ozonPayApiBase = () => process.env.SHOP_API_BASE_URL || "https://davidsklad.ru";

function _ozonPayRequestSign(...fields) {
  return require("crypto").createHash("sha256").update(fields.join("")).digest("hex");
}

function _ozonPayNotificationSign({ accessKey, orderID, transactionID, extOrderID, amount, currencyCode, notificationSecretKey }) {
  const fingerprint = `${accessKey}|${orderID || ""}|${transactionID != null ? String(transactionID) : ""}|${extOrderID || ""}|${amount}|${currencyCode}|${notificationSecretKey}`;
  return require("crypto").createHash("sha256").update(fingerprint).digest("hex");
}

async function _ozonPayCreateOrder({ orderId, totalRub, email, paymentMethod = "ozon_pay" }) {
  const accessKey = process.env.OZON_PAY_ACCESS_KEY;
  const secretKey = process.env.OZON_PAY_SECRET_KEY;
  if (!accessKey || !secretKey) return null;

  const extId = orderId;
  const paymentAlgorithm = paymentMethod === "sbp" ? "PAY_ALGO_SBP" : "PAY_ALGO_SMS";
  const currencyCode = "643";
  const value = String(Math.round(totalRub * 100));

  const requestSign = _ozonPayRequestSign(accessKey, "", extId, "", paymentAlgorithm, currencyCode, value, secretKey);

  const successUrl = `${_ozonPayShopBase()}/order-success?id=${encodeURIComponent(orderId)}`;
  const failUrl = `${_ozonPayShopBase()}/order-failed?id=${encodeURIComponent(orderId)}`;
  const notificationUrl = `${_ozonPayApiBase()}/api/shop/payment/notification`;

  const reqBody = {
    accessKey,
    amount: { currencyCode, value },
    extId,
    paymentAlgorithm,
    mode: "MODE_SHORTENED",
    enableFiscalization: false,
    successUrl,
    failUrl,
    notificationUrl,
    requestSign,
  };
  if (email) reqBody.receiptEmail = cleanText(email);

  try {
    const res = await fetch(`${_ozonPayBaseUrl}/v1/createOrder`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(reqBody),
      signal: AbortSignal.timeout(15000),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      logger.warn("ozon pay createOrder failed", {
        status: res.status, extId, detail: data?.message || JSON.stringify(data).slice(0, 200),
      });
      return null;
    }
    const payLink = data?.order?.payLink || null;
    if (payLink) logger.info("ozon pay order created", { extId });
    return payLink;
  } catch (error) {
    logger.warn("ozon pay createOrder error", { extId, detail: error?.message || String(error) });
    return null;
  }
}

app.post("/api/shop/orders", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const body = request.body || {};
    const items = Array.isArray(body.items) ? body.items : [];
    const delivery = body.delivery || {};

    if (!items.length) return response.status(400).json({ error: "Корзина пуста" });
    if (!delivery.firstName || !delivery.phone || !delivery.email) {
      return response.status(400).json({ error: "Заполните обязательные поля" });
    }

    const totalRub = items.reduce((s, i) => s + Number(i.priceRub || 0) * Number(i.quantity || 1), 0);
    const orderId = `MV-${Date.now().toString(36).toUpperCase()}`;

    // Resolve customer from Bearer token (optional — guest checkout also works)
    let customerId = null;
    const auth = request.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    const payload = verifyShopToken(token);
    if (payload?.customerId && prisma) {
      const cust = await prisma.shopCustomer.findUnique({ where: { id: payload.customerId }, select: { id: true } });
      if (cust) customerId = cust.id;
    }

    // Save order to DB
    if (prisma) {
      await prisma.shopOrder.create({
        data: {
          id: orderId,
          customerId,
          status: "pending",
          items: items,
          delivery: delivery,
          totalRub: Math.round(totalRub),
          comment: body.comment ? cleanText(body.comment) : null,
        },
      });
    }

    logger.info("shop order created", {
      orderId, totalRub, itemCount: items.length, customerId,
      city: cleanText(delivery.city || ""),
      phone: cleanText(delivery.phone || "").replace(/\d{4}$/, "****"),
    });

    const paymentMethod = ["ozon_pay", "sbp"].includes(body.paymentMethod) ? body.paymentMethod : "ozon_pay";
    const paymentUrl = await _ozonPayCreateOrder({ orderId, totalRub, email: cleanText(delivery.email || ""), paymentMethod });
    if (paymentUrl && prisma) {
      await prisma.shopOrder.update({ where: { id: orderId }, data: { status: "payment_pending" } }).catch(() => {});
    }

    response.json({ ok: true, id: orderId, status: paymentUrl ? "payment_pending" : "pending", totalRub, paymentUrl: paymentUrl || null });
  } catch (error) {
    next(error);
  }
});

app.post("/api/shop/payment/notification", async (request, response, next) => {
  try {
    const accessKey = process.env.OZON_PAY_ACCESS_KEY;
    const notificationSecretKey = process.env.OZON_PAY_NOTIFICATION_SECRET;
    const body = request.body || {};

    if (!accessKey || !notificationSecretKey) {
      logger.warn("ozon pay notification received but OZON_PAY_ACCESS_KEY/OZON_PAY_NOTIFICATION_SECRET not configured — rejecting");
      return response.status(503).json({ error: "Payment notifications not configured" });
    }
    if (!body.requestSign) {
      logger.warn("ozon pay notification missing requestSign", { extOrderID: body.extOrderID });
      return response.status(400).json({ error: "Missing signature" });
    }
    const expected = _ozonPayNotificationSign({
      accessKey,
      orderID: body.orderID || "",
      transactionID: body.transactionID,
      extOrderID: body.extOrderID || "",
      amount: String(body.amount || ""),
      currencyCode: String(body.currencyCode || ""),
      notificationSecretKey,
    });
    if (body.requestSign !== expected) {
      logger.warn("ozon pay notification bad signature", { extOrderID: body.extOrderID });
      return response.status(400).json({ error: "Bad signature" });
    }

    const { extOrderID, status, orderID } = body;
    logger.info("ozon pay notification", {
      extOrderID, orderID, status, amount: body.amount, method: body.paymentMethod,
    });

    if (extOrderID && status === "Completed") {
      const prisma = getPrisma();
      if (prisma) {
        const updated = await prisma.shopOrder.updateMany({
          where: { id: extOrderID, status: { not: "paid" } },
          data: { status: "paid" },
        }).catch((err) => {
          logger.warn("ozon pay order status update failed", { extOrderID, detail: err?.message });
          return null;
        });
        if (updated?.count > 0) logger.info("shop order paid via ozon pay", { extOrderID });
      }
    }

    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ── Auth routes ───────────────────────────────────────────────────────────

// ── Email OTP helpers ─────────────────────────────────────────────────────

let _shopOtpRedis = null;
function shopOtpRedis() {
  if (_shopOtpRedis) return _shopOtpRedis;
  if (!redisUrl) return null;
  try {
    const Redis = require("ioredis");
    _shopOtpRedis = new Redis(redisUrl, { maxRetriesPerRequest: 2, enableReadyCheck: false, lazyConnect: false });
    _shopOtpRedis.on("error", () => {});
    return _shopOtpRedis;
  } catch { return null; }
}

// Minimal SMTPS (port 465, implicit TLS) client — no external deps
function shopSendEmail({ to, subject, html }) {
  return new Promise((resolve, reject) => {
    const tls = require("tls");
    const host = process.env.SHOP_SMTP_HOST;
    const port = Number(process.env.SHOP_SMTP_PORT || 465);
    const user = process.env.SHOP_SMTP_USER;
    const pass = process.env.SHOP_SMTP_PASS;
    if (!host || !user || !pass) return reject(new Error("SMTP not configured"));

    const b64 = (s) => Buffer.from(s).toString("base64");
    const lines = [];
    let buf = "";
    let done = false;

    const sock = tls.connect({ host, port, rejectUnauthorized: false }, () => {
      // greeting handled in data handler
    });

    sock.setTimeout(15000);
    sock.on("timeout", () => { sock.destroy(new Error("SMTP timeout")); });

    function send(line) { sock.write(line + "\r\n"); }

    function onLine(line) {
      const code = parseInt(line.slice(0, 3), 10);
      const last = line[3] !== "-"; // multi-line if "-"
      if (!last) return;
      if (code === 220) { send("EHLO magicvibes.ru"); return; }
      if (code === 250) {
        if (!lines.includes("auth")) { lines.push("auth"); send("AUTH LOGIN"); return; }
        if (!lines.includes("user")) { lines.push("user"); send(b64(user)); return; }
        if (!lines.includes("rcpt")) { lines.push("rcpt"); send(`RCPT TO:<${to}>`); return; }
        if (!lines.includes("data")) { lines.push("data"); send("DATA"); return; }
        if (!lines.includes("quit")) { lines.push("quit"); send("QUIT"); return; }
        return;
      }
      if (code === 334) {
        if (!lines.includes("user")) { lines.push("user"); send(b64(user)); return; }
        send(b64(pass));
        return;
      }
      if (code === 235) {
        send(`MAIL FROM:<${user}>`);
        return;
      }
      if (code === 354) {
        const body = [
          `From: "Magic Vibes" <${user}>`,
          `To: ${to}`,
          `Subject: ${subject}`,
          `MIME-Version: 1.0`,
          `Content-Type: text/html; charset=utf-8`,
          `X-Mailer: Magic Vibes Shop`,
          `List-Unsubscribe: <mailto:${user}?subject=unsubscribe>`,
          `List-Unsubscribe-Post: List-Unsubscribe=One-Click`,
          ``,
          html,
          `.`,
        ].join("\r\n");
        sock.write(body + "\r\n");
        return;
      }
      if (code === 221) { sock.destroy(); if (!done) { done = true; resolve(); } return; }
      if (code >= 400) { sock.destroy(new Error(`SMTP error ${code}: ${line.slice(4)}`)); return; }
    }

    sock.on("data", (chunk) => {
      buf += chunk.toString();
      let idx;
      while ((idx = buf.indexOf("\r\n")) !== -1) {
        const line = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        onLine(line);
      }
    });
    sock.on("error", (err) => { if (!done) { done = true; reject(err); } });
    sock.on("close", () => { if (!done) { done = true; resolve(); } });
  });
}

function shopOtpEmailHtml(code) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f5f7;font-family:-apple-system,BlinkMacSystemFont,'Inter',sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f7;padding:40px 16px;">
<tr><td align="center">
<table width="480" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:20px;overflow:hidden;box-shadow:0 2px 16px rgba(0,0,0,0.08);">
<tr><td style="background:linear-gradient(135deg,#7c3aed,#9333ea);padding:32px 40px;text-align:center;">
  <div style="font-size:26px;font-weight:800;color:#fff;letter-spacing:-0.5px;">Magic Vibes</div>
  <div style="font-size:13px;color:rgba(255,255,255,0.75);margin-top:4px;">Парфюмерия</div>
</td></tr>
<tr><td style="padding:40px 40px 32px;">
  <div style="font-size:22px;font-weight:700;color:#1d1d1f;margin-bottom:8px;">Ваш код для входа</div>
  <div style="font-size:15px;color:#6e6e73;line-height:1.5;margin-bottom:28px;">Введите этот код на сайте. Код действителен 10 минут.</div>
  <div style="text-align:center;margin:28px 0;">
    <div style="display:inline-block;background:#f5f3ff;border-radius:16px;padding:24px 48px;border:2px solid #ede9fe;">
      <span style="font-size:42px;font-weight:800;letter-spacing:10px;color:#7c3aed;font-variant-numeric:tabular-nums;">${code}</span>
    </div>
  </div>
  <div style="font-size:13px;color:#aeaeb2;border-top:1px solid #f0f0f2;padding-top:24px;margin-top:8px;">Если вы не запрашивали этот код — просто проигнорируйте это письмо.</div>
</td></tr>
<tr><td style="background:#f9f9fb;padding:20px 40px;text-align:center;">
  <div style="font-size:12px;color:#aeaeb2;">© Magic Vibes · <a href="https://magicvibes.ru" style="color:#7c3aed;text-decoration:none;">magicvibes.ru</a></div>
</td></tr>
</table></td></tr></table></body></html>`;
}

app.post("/api/shop/auth/send-code", shopCors, async (request, response, next) => {
  try {
    const { email } = request.body || {};
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
      return response.status(400).json({ error: "Укажите корректный email" });
    }
    if (!process.env.SHOP_SMTP_HOST) return response.status(503).json({ error: "Email-сервис не настроен" });

    const redis = shopOtpRedis();
    if (!redis) return response.status(503).json({ error: "Сервис временно недоступен. Попробуйте позже." });

    const normalEmail = email.toLowerCase().trim();
    const emailKey = `shop:otp:${normalEmail}`;
    const rateLimitKey = `shop:otp:rl:${normalEmail}`;

    const rl = await redis.get(rateLimitKey);
    if (rl) return response.status(429).json({ error: "Подождите 60 секунд перед повторной отправкой", retryAfter: 60 });

    const code = String(Math.floor(100000 + Math.random() * 900000));
    await redis.set(emailKey, JSON.stringify({ code, attempts: 0 }), "EX", 600);
    await redis.set(rateLimitKey, "1", "EX", 60);

    await shopSendEmail({
      to: normalEmail,
      subject: `${code} — код для входа на Magic Vibes`,
      html: shopOtpEmailHtml(code),
    });

    logger.info("shop otp sent", { email: normalEmail.replace(/(.{2}).+(@.+)/, "$1***$2") });
    response.json({ ok: true });
  } catch (error) { next(error); }
});

app.post("/api/shop/auth/verify-code", shopCors, async (request, response, next) => {
  try {
    const { email, code } = request.body || {};
    if (!email || !code) return response.status(400).json({ error: "Email и код обязательны" });

    const redis = shopOtpRedis();
    const normalEmail = email.toLowerCase().trim();
    const emailKey = `shop:otp:${normalEmail}`;

    let otpData = null;
    if (redis) {
      const raw = await redis.get(emailKey);
      if (raw) try { otpData = JSON.parse(raw); } catch { otpData = null; }
    }

    if (!otpData) return response.status(400).json({ error: "Код истёк или не найден. Запросите новый." });

    if (otpData.attempts >= 5) {
      if (redis) await redis.del(emailKey);
      return response.status(400).json({ error: "Слишком много попыток. Запросите новый код." });
    }

    if (otpData.code !== String(code).trim()) {
      otpData.attempts += 1;
      if (redis) await redis.set(emailKey, JSON.stringify(otpData), "KEEPTTL");
      const left = 5 - otpData.attempts;
      return response.status(400).json({ error: `Неверный код. Осталось попыток: ${left}` });
    }

    if (redis) await redis.del(emailKey);

    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });

    let customer = await prisma.shopCustomer.findUnique({ where: { email: normalEmail } });
    if (!customer) {
      customer = await prisma.shopCustomer.create({
        data: { id: _shopCrypto.randomBytes(12).toString("hex"), email: normalEmail, password: "" },
      });
    }

    const token = signShopToken({ customerId: customer.id, email: customer.email });
    response.json({ ok: true, token, customer: { id: customer.id, email: customer.email, firstName: customer.firstName, lastName: customer.lastName, phone: customer.phone } });
  } catch (error) { next(error); }
});

app.post("/api/shop/auth/register", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const { email, password, firstName, lastName, phone } = request.body || {};
    if (!email || !password) return response.status(400).json({ error: "Email и пароль обязательны" });
    if (password.length < 6) return response.status(400).json({ error: "Пароль не менее 6 символов" });
    const existing = await prisma.shopCustomer.findUnique({ where: { email: email.toLowerCase().trim() } });
    if (existing) return response.status(409).json({ error: "Email уже зарегистрирован" });
    const hashed = await _shopHashPassword(password);
    const customer = await prisma.shopCustomer.create({
      data: {
        id: require("crypto").randomBytes(12).toString("hex"),
        email: email.toLowerCase().trim(),
        password: hashed,
        firstName: firstName ? cleanText(firstName) : null,
        lastName: lastName ? cleanText(lastName) : null,
        phone: phone ? cleanText(phone) : null,
      },
    });
    const token = signShopToken({ customerId: customer.id, email: customer.email });
    response.json({ ok: true, token, customer: { id: customer.id, email: customer.email, firstName: customer.firstName, lastName: customer.lastName } });
  } catch (error) { next(error); }
});

app.post("/api/shop/auth/login", shopCors, shopLoginLimiter, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const { email, password } = request.body || {};
    if (!email || !password) return response.status(400).json({ error: "Email и пароль обязательны" });
    const customer = await prisma.shopCustomer.findUnique({ where: { email: email.toLowerCase().trim() } });
    if (!customer) return response.status(401).json({ error: "Неверный email или пароль" });
    const valid = await _shopVerifyPassword(password, customer.password);
    if (!valid) return response.status(401).json({ error: "Неверный email или пароль" });
    const token = signShopToken({ customerId: customer.id, email: customer.email });
    response.json({ ok: true, token, customer: { id: customer.id, email: customer.email, firstName: customer.firstName, lastName: customer.lastName } });
  } catch (error) { next(error); }
});

app.get("/api/shop/auth/me", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const customer = await prisma.shopCustomer.findUnique({
      where: { id: request.shopCustomer.customerId },
      select: { id: true, email: true, firstName: true, lastName: true, phone: true, createdAt: true },
    });
    if (!customer) return response.status(404).json({ error: "Пользователь не найден" });
    response.json({ ok: true, customer });
  } catch (error) { next(error); }
});

app.patch("/api/shop/auth/profile", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const { firstName, lastName, phone } = request.body || {};
    const data = {};
    if (firstName !== undefined) data.firstName = firstName ? cleanText(firstName) : null;
    if (lastName !== undefined) data.lastName = lastName ? cleanText(lastName) : null;
    if (phone !== undefined) data.phone = phone ? cleanText(phone) : null;
    const customer = await prisma.shopCustomer.update({
      where: { id: request.shopCustomer.customerId },
      data,
      select: { id: true, email: true, firstName: true, lastName: true, phone: true },
    });
    response.json({ ok: true, customer });
  } catch (error) { next(error); }
});

app.get("/api/shop/auth/orders", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const orders = await prisma.shopOrder.findMany({
      where: { customerId: request.shopCustomer.customerId },
      orderBy: { createdAt: "desc" },
      take: 20,
    });
    response.json({ ok: true, orders });
  } catch (error) { next(error); }
});

// ── Yandex ID OAuth ──────────────────────────────────────────────────────

const _shopYandexOauthStates = new Map();
const _SHOP_YANDEX_STATE_TTL = 10 * 60 * 1000;

function _shopCleanYandexStates() {
  const now = Date.now();
  for (const [k, t] of _shopYandexOauthStates.entries()) {
    if (now - t > _SHOP_YANDEX_STATE_TTL) _shopYandexOauthStates.delete(k);
  }
}

app.get("/api/shop/auth/yandex/start", shopCors, (_request, response) => {
  const clientId = process.env.YANDEX_OAUTH_CLIENT_ID;
  if (!clientId) return response.status(503).json({ error: "Яндекс ID не настроен" });
  _shopCleanYandexStates();
  const state = _shopCrypto.randomBytes(20).toString("hex");
  _shopYandexOauthStates.set(state, Date.now());
  const url = new URL("https://oauth.yandex.ru/authorize");
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", clientId);
  url.searchParams.set("redirect_uri", process.env.YANDEX_OAUTH_REDIRECT_URI || "https://magicvibes.ru/");
  url.searchParams.set("state", state);
  url.searchParams.set("force_confirm", "no");
  response.json({ url: url.toString() });
});

app.post("/api/shop/auth/yandex/callback", shopCors, shopLoginLimiter, async (request, response, next) => {
  const clientId = process.env.YANDEX_OAUTH_CLIENT_ID;
  const clientSecret = process.env.YANDEX_OAUTH_CLIENT_SECRET;
  if (!clientId || !clientSecret) return response.status(503).json({ error: "Яндекс ID не настроен" });

  const { code, state } = request.body || {};
  if (!code || !state) return response.status(400).json({ error: "Отсутствует code или state" });

  _shopCleanYandexStates();
  if (!_shopYandexOauthStates.has(state)) {
    return response.status(400).json({ error: "Недействительный state. Попробуйте войти заново." });
  }
  _shopYandexOauthStates.delete(state);

  try {
    const tokenBody = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      client_id: clientId,
      client_secret: clientSecret,
      redirect_uri: process.env.YANDEX_OAUTH_REDIRECT_URI || "https://magicvibes.ru/",
    }).toString();

    const tokenRes = await fetch("https://oauth.yandex.ru/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: tokenBody,
    });
    const tokenData = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || "Ошибка получения токена Яндекс");
    }

    const profileRes = await fetch("https://login.yandex.ru/info?format=json", {
      headers: { Authorization: `OAuth ${tokenData.access_token}` },
    });
    const profile = await profileRes.json();
    if (!profileRes.ok || !profile.id) throw new Error("Не удалось получить профиль Яндекс");

    const email = profile.default_email || `yandex-${profile.id}@yandex.ru`;
    const normalEmail = email.toLowerCase().trim();

    let [firstName, ...lastParts] = (profile.real_name || profile.display_name || profile.login || "").split(" ");
    const lastName = lastParts.join(" ") || null;

    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });

    let customer = await prisma.shopCustomer.findUnique({ where: { email: normalEmail } });
    if (!customer) {
      customer = await prisma.shopCustomer.create({
        data: {
          id: _shopCrypto.randomBytes(12).toString("hex"),
          email: normalEmail,
          password: "",
          firstName: firstName || null,
          lastName: lastName || null,
        },
      });
    }

    const avatarUrl = (!profile.is_avatar_empty && profile.default_avatar_id)
      ? `https://avatars.yandex.net/get-yapic/${profile.default_avatar_id}/islands-200`
      : null;

    const token = signShopToken({ customerId: customer.id, email: customer.email });
    response.json({
      ok: true,
      token,
      customer: {
        id: customer.id,
        email: customer.email,
        firstName: customer.firstName,
        lastName: customer.lastName,
        phone: customer.phone,
        avatarUrl,
      },
    });
  } catch (error) {
    logger.warn("shop yandex oauth callback error", { detail: error?.message || String(error) });
    next(error);
  }
});

// ── Admin routes ──────────────────────────────────────────────────────────

// Banners
app.get("/api/shop/admin/banners", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await readShopBanners());
  } catch (error) { next(error); }
});

app.post("/api/shop/admin/banners", requireAdmin, async (request, response, next) => {
  try {
    const banners = await readShopBanners();
    const banner = {
      id: nanoid8(),
      imageUrl: cleanText(request.body.imageUrl || ""),
      title: cleanText(request.body.title || ""),
      subtitle: cleanText(request.body.subtitle || ""),
      linkUrl: cleanText(request.body.linkUrl || ""),
      linkText: cleanText(request.body.linkText || ""),
      active: request.body.active !== false,
      order: banners.length,
    };
    banners.push(banner);
    await writeShopData(SHOP_BANNERS_KEY, banners);
    response.json({ ok: true, banner });
  } catch (error) { next(error); }
});

app.put("/api/shop/admin/banners/:id", requireAdmin, async (request, response, next) => {
  try {
    const banners = await readShopBanners();
    const idx = banners.findIndex((b) => b.id === request.params.id);
    if (idx === -1) return response.status(404).json({ error: "Banner not found" });
    banners[idx] = { ...banners[idx], ...request.body, id: request.params.id };
    await writeShopData(SHOP_BANNERS_KEY, banners);
    response.json({ ok: true, banner: banners[idx] });
  } catch (error) { next(error); }
});

app.delete("/api/shop/admin/banners/:id", requireAdmin, async (request, response, next) => {
  try {
    const banners = await readShopBanners();
    const filtered = banners.filter((b) => b.id !== request.params.id);
    await writeShopData(SHOP_BANNERS_KEY, filtered);
    response.json({ ok: true });
  } catch (error) { next(error); }
});

// Categories
app.get("/api/shop/admin/categories", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await readShopCategories());
  } catch (error) { next(error); }
});

app.post("/api/shop/admin/categories", requireAdmin, async (request, response, next) => {
  try {
    const cats = await readShopCategories();
    const cat = {
      id: nanoid8(),
      name: cleanText(request.body.name || ""),
      slug: cleanText(request.body.slug || request.body.name || "").toLowerCase().replace(/\s+/g, "-"),
      imageUrl: cleanText(request.body.imageUrl || ""),
      order: cats.length,
      filterTag: cleanText(request.body.filterTag || ""),
    };
    cats.push(cat);
    await writeShopData(SHOP_CATEGORIES_KEY, cats);
    response.json({ ok: true, category: cat });
  } catch (error) { next(error); }
});

app.put("/api/shop/admin/categories/:id", requireAdmin, async (request, response, next) => {
  try {
    const cats = await readShopCategories();
    const idx = cats.findIndex((c) => c.id === request.params.id);
    if (idx === -1) return response.status(404).json({ error: "Category not found" });
    cats[idx] = { ...cats[idx], ...request.body, id: request.params.id };
    await writeShopData(SHOP_CATEGORIES_KEY, cats);
    response.json({ ok: true, category: cats[idx] });
  } catch (error) { next(error); }
});

app.delete("/api/shop/admin/categories/:id", requireAdmin, async (request, response, next) => {
  try {
    const cats = await readShopCategories();
    await writeShopData(SHOP_CATEGORIES_KEY, cats.filter((c) => c.id !== request.params.id));
    response.json({ ok: true });
  } catch (error) { next(error); }
});

// Settings
app.get("/api/shop/admin/settings", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await readShopSettings());
  } catch (error) { next(error); }
});

app.patch("/api/shop/admin/settings", requireAdmin, async (request, response, next) => {
  try {
    const current = await readShopSettings();
    const allowed = ["markup", "markupRules", "shopName", "shopDescription", "contactEmail", "contactPhone", "deliveryDays", "freeDeliveryFrom"];
    const updates = {};
    for (const k of allowed) {
      if (request.body[k] !== undefined) updates[k] = request.body[k];
    }
    if (updates.markup !== undefined) updates.markup = Math.max(0.5, Math.min(20, Number(updates.markup) || current.markup));
    if (updates.markupRules !== undefined) updates.markupRules = normalizeShopMarkupRules(updates.markupRules);
    const merged = { ...current, ...updates };
    const appSettings = await readAppSettings();
    await writeAppSettings({ ...appSettings, [SHOP_SETTINGS_KEY]: merged });
    response.json({ ok: true, settings: merged });
  } catch (error) { next(error); }
});

// Orders (admin)
app.get("/api/shop/admin/orders", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ orders: [], total: 0 });
    const page = Math.max(1, Number(request.query.page || 1));
    const pageSize = Math.min(50, Math.max(1, Number(request.query.pageSize || 20)));
    const status = request.query.status || undefined;
    const where = status ? { status } : {};
    const [orders, total] = await Promise.all([
      prisma.shopOrder.findMany({
        where,
        include: { customer: { select: { id: true, email: true, firstName: true, lastName: true } } },
        orderBy: { createdAt: "desc" },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
      prisma.shopOrder.count({ where }),
    ]);
    response.json({ ok: true, orders, total, page, pageSize });
  } catch (error) { next(error); }
});

app.patch("/api/shop/admin/orders/:id", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "DB unavailable" });
    const allowed = ["pending", "confirmed", "picking", "shipped", "delivered", "cancelled"];
    const { status } = request.body;
    if (!allowed.includes(status)) return response.status(400).json({ error: "Invalid status" });
    const order = await prisma.shopOrder.update({
      where: { id: request.params.id },
      data: { status },
    });
    response.json({ ok: true, order });
  } catch (error) { next(error); }
});

// Customers (admin)
app.get("/api/shop/admin/customers", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ customers: [], total: 0 });
    const page = Math.max(1, Number(request.query.page || 1));
    const pageSize = Math.min(50, Math.max(1, Number(request.query.pageSize || 20)));
    const [customers, total] = await Promise.all([
      prisma.shopCustomer.findMany({
        select: { id: true, email: true, firstName: true, lastName: true, phone: true, createdAt: true, _count: { select: { orders: true } } },
        orderBy: { createdAt: "desc" },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
      prisma.shopCustomer.count(),
    ]);
    response.json({ ok: true, customers, total, page, pageSize });
  } catch (error) { next(error); }
});

// Stats (admin)
app.get("/api/shop/admin/stats", requireAdmin, async (_request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, orders: 0, revenue: 0, customers: 0, todayOrders: 0 });
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const weekStart = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const [totalOrders, totalCustomers, todayOrders, weekOrders, revenueAgg, weekRevenueAgg] = await Promise.all([
      prisma.shopOrder.count(),
      prisma.shopCustomer.count(),
      prisma.shopOrder.count({ where: { createdAt: { gte: todayStart } } }),
      prisma.shopOrder.count({ where: { createdAt: { gte: weekStart } } }),
      prisma.shopOrder.aggregate({ _sum: { totalRub: true }, where: { status: { not: "cancelled" } } }),
      prisma.shopOrder.aggregate({ _sum: { totalRub: true }, where: { status: { not: "cancelled" }, createdAt: { gte: weekStart } } }),
    ]);
    response.json({
      ok: true,
      totalOrders,
      totalCustomers,
      todayOrders,
      weekOrders,
      totalRevenue: revenueAgg._sum.totalRub || 0,
      weekRevenue: weekRevenueAgg._sum.totalRub || 0,
    });
  } catch (error) { next(error); }
});

// ── Stock alerts (back-in-stock notifications) ────────────────────────────────
app.post("/api/shop/stock-alert", shopCors, async (request, response, next) => {
  try {
    const db = getPrisma();
    if (!db) return response.status(503).json({ error: "База данных недоступна" });
    const offerId = cleanText(request.body?.offerId || "");
    const email = cleanText(request.body?.email || "").toLowerCase();
    if (!offerId) return response.status(400).json({ error: "offerId обязателен" });
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return response.status(400).json({ error: "Некорректный email" });
    await db.stockAlert.upsert({
      where: { offerId_email: { offerId, email } },
      create: { offerId, email },
      update: { notifiedAt: null },
    });
    response.json({ ok: true });
  } catch (error) { next(error); }
});

app.get("/api/shop/admin/stock-alerts", requireAdmin, async (request, response, next) => {
  try {
    const db = getPrisma();
    if (!db) return response.json({ ok: true, alerts: [] });
    const offerId = cleanText(request.query.offerId || "");
    const alerts = await db.stockAlert.findMany({
      where: { ...(offerId ? { offerId } : {}), notifiedAt: null },
      orderBy: { createdAt: "desc" },
      take: 200,
    });
    response.json({ ok: true, alerts });
  } catch (error) { next(error); }
});
