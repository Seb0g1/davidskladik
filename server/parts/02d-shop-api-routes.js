// ─── Magic Vibes Shop API ──────────────────────────────────────────────────
const _shopCrypto = require("crypto");
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
    shopName: "Magic Vibes",
    shopDescription: "Оригинальная парфюмерия и косметика с доставкой по России",
    contactEmail: process.env.SHOP_CONTACT_EMAIL || "",
    contactPhone: process.env.SHOP_CONTACT_PHONE || "",
    deliveryDays: 3,
    freeDeliveryFrom: 3000,
  };
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

async function buildShopProductsFromDb({ q, brand, category, inStock, sort, page, pageSize }) {
  const prisma = getPrisma();
  if (!prisma) return { products: [], total: 0, brands: [] };

  const shopSettings = await readShopSettings();
  const markup = shopSettings.markup || 2.2;

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
        links: { take: 2, select: { supplierArticle: true } },
      },
      orderBy: sort === "price_asc" || sort === "price_desc" ? { currentPrice: sort === "price_asc" ? "asc" : "desc" } : { name: "asc" },
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

  const pmMap = new Map();
  if (articles.length) {
    const snaps = await prisma.priceMasterSnapshotItem.findMany({
      where: { article: { in: articles }, active: true },
      select: { article: true, price: true, currency: true },
    });
    for (const snap of snaps) {
      const cur = pmMap.get(snap.article);
      const price = Number(snap.price || 0);
      if (!cur || price < Number(cur.price)) pmMap.set(snap.article, snap);
    }
  }

  // Build shop products
  const products = deduped.map((p) => {
    const images = extractImages(p);
    const link = p.links[0];
    const snap = link ? pmMap.get(cleanText(link.supplierArticle)) : null;
    const priceUsd = snap ? Number(snap.price || 0) : 0;
    const currentPriceNum = Number(p.currentPrice || 0);
    const priceRub = priceUsd > 0
      ? Math.round(priceUsd * usdRate * markup)
      : currentPriceNum > 0 ? Math.round(currentPriceNum * markup / 100) : 0;

    const stockQty = p.targetStock ?? 0;
    const name = cleanText(p.name || "");
    const _cat = extractProductCategory(name);

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
      rating: 0,
      reviewCount: 0,
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
  { slug: "parfum", label: "Духи",              pattern: /духи|extrait|pure[\s-]parfum/i,                               keywords: ["духи", "extrait", "pure parfum"] },
  { slug: "edp",    label: "Парфюмерная вода",  pattern: /парфюм[\s-]?(ерная)?\s*вода|eau[\s-]de[\s-]parfum|\bedp\b/i,  keywords: ["парфюмерная вода", "eau de parfum"] },
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
  const markup = shopSettings.markup || 2.2;
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

  // Try to get description from marketplaceState
  let description = "";
  try {
    const state = p.marketplaceState;
    if (state && typeof state === "object") {
      description = cleanText(state.description || state.desc || state.productDescription || "");
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
  const priceRub = priceUsd > 0
    ? Math.round(priceUsd * usdRate * markup)
    : currentPriceNum > 0 ? Math.round(currentPriceNum * markup / 100) : 0;
  const _pCat = extractProductCategory(cleanText(p.name || ""));

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
    rating: 0,
    reviewCount: 0,
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
    const ORDER = ["edp", "edt", "parfum", "edc", "deo", "sets", "body", "home"];
    const result = ORDER
      .map((slug) => {
        const def = SHOP_CATEGORIES.find((c) => c.slug === slug);
        return { slug, label: def ? def.label : slug, count: counts[slug] || 0 };
      })
      .filter((c) => c.count > 0);
    Object.entries(counts)
      .filter(([slug]) => !ORDER.includes(slug) && (counts[slug] || 0) > 0)
      .sort(([, a], [, b]) => b - a)
      .forEach(([slug, count]) => result.push({ slug, label: slug, count }));
    response.json(result);
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

    response.json({ ok: true, id: orderId, status: "pending", totalRub, paymentUrl: null });
  } catch (error) {
    next(error);
  }
});

// ── Auth routes ───────────────────────────────────────────────────────────

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

app.post("/api/shop/auth/login", shopCors, async (request, response, next) => {
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
    const allowed = ["markup", "shopName", "shopDescription", "contactEmail", "contactPhone", "deliveryDays", "freeDeliveryFrom"];
    const updates = {};
    for (const k of allowed) {
      if (request.body[k] !== undefined) updates[k] = request.body[k];
    }
    if (updates.markup !== undefined) updates.markup = Math.max(0.5, Math.min(20, Number(updates.markup) || current.markup));
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
