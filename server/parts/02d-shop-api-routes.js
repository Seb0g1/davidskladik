// ─── Magic Vibes Shop API ──────────────────────────────────────────────────
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
  try { usdRate = await getUsdRate(); } catch (_) {}

  const skip = (page - 1) * pageSize;

  // Build where clause
  const where = {
    archived: false,
    marketplace: { in: ["ozon", "yandex"] },
    NOT: { status: "deleted" },
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

  // De-duplicate by offerId: prefer Ozon over Yandex
  const [rawProducts, total] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where,
      include: { links: { take: 1 } },
      orderBy: sort === "price_asc" || sort === "price_desc" ? { currentPrice: sort === "price_asc" ? "asc" : "desc" } : { name: "asc" },
      take: pageSize * 3, // over-fetch for de-dup
      skip: 0,
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
    const images = Array.isArray(p.images) ? p.images.filter(Boolean) : [];
    const link = p.links[0];
    const snap = link ? pmMap.get(cleanText(link.supplierArticle)) : null;
    const priceUsd = snap ? Number(snap.price || 0) : 0;
    const priceRub = priceUsd > 0
      ? Math.round(priceUsd * usdRate * markup)
      : p.currentPrice ? Math.round(p.currentPrice * markup / 100) : 0;

    const stockQty = p.targetStock ?? 0;

    return {
      id: p.id,
      offerId: p.offerId,
      name: cleanText(p.name || p.offerId),
      brand: cleanText(p.brand || ""),
      description: "",
      images,
      priceRub,
      inStock: stockQty > 0 || (p.status !== "archived" && p.currentPrice != null && p.currentPrice > 0),
      stockQty: Math.max(0, stockQty),
      volume: extractVolume(p.name || ""),
      category: categoryFromBrand(cleanText(p.brand || "")),
      tags: [],
      rating: 0,
      reviewCount: 0,
    };
  }).filter((p) => p.priceRub > 0 || !inStock);

  const filtered = inStock ? products.filter((p) => p.inStock) : products;

  // Extract unique brands for filter sidebar
  const brands = [...new Set(
    rawProducts.map((p) => cleanText(p.brand || "")).filter(Boolean)
  )].sort();

  return { products: filtered, total, brands };
}

function extractVolume(name = "") {
  const m = name.match(/(\d+\s*(?:мл|ml|г|g|oz)\b)/i);
  return m ? m[1] : undefined;
}

function categoryFromBrand(_brand) {
  return "parfumery";
}

async function findShopProductByOfferId(offerId) {
  const prisma = getPrisma();
  if (!prisma) return null;

  const shopSettings = await readShopSettings();
  const markup = shopSettings.markup || 2.2;
  let usdRate = Number(process.env.DEFAULT_USD_RATE || 95);
  try { usdRate = await getUsdRate(); } catch (_) {}

  const products = await prisma.warehouseProduct.findMany({
    where: { offerId: { equals: offerId, mode: "insensitive" }, archived: false },
    include: { links: { take: 3 } },
    take: 5,
  });

  if (!products.length) return null;

  // prefer Ozon
  const p = products.find((pr) => pr.marketplace === "ozon") || products[0];
  const images = Array.isArray(p.images) ? p.images.filter(Boolean) : [];

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

  const priceRub = priceUsd > 0
    ? Math.round(priceUsd * usdRate * markup)
    : p.currentPrice ? Math.round(p.currentPrice * markup / 100) : 0;

  return {
    id: p.id,
    offerId: p.offerId,
    name: cleanText(p.name || p.offerId),
    brand: cleanText(p.brand || ""),
    description,
    images,
    priceRub,
    inStock: (p.targetStock ?? 0) > 0 || (p.status !== "archived" && p.currentPrice != null),
    stockQty: Math.max(0, p.targetStock ?? 0),
    volume: extractVolume(p.name || ""),
    category: categoryFromBrand(cleanText(p.brand || "")),
    tags: [],
    rating: 0,
    reviewCount: 0,
  };
}

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
    const body = request.body || {};
    const items = Array.isArray(body.items) ? body.items : [];
    const delivery = body.delivery || {};

    if (!items.length) return response.status(400).json({ error: "Корзина пуста" });
    if (!delivery.firstName || !delivery.phone || !delivery.email) {
      return response.status(400).json({ error: "Заполните обязательные поля" });
    }

    const totalRub = items.reduce((s, i) => s + Number(i.priceRub || 0) * Number(i.quantity || 1), 0);
    const orderId = `MV-${Date.now().toString(36).toUpperCase()}`;

    // TODO: integrate Ozon Pay here — call Ozon Pay API to create payment session
    // const paymentUrl = await createOzonPaySession({ orderId, totalRub, items, delivery });
    // For now: return orderId and null paymentUrl
    const paymentUrl = process.env.OZON_PAY_ENABLED === "true"
      ? null  // placeholder — replace with real Ozon Pay call
      : null;

    logger.info("shop order created", {
      orderId,
      totalRub,
      itemCount: items.length,
      city: cleanText(delivery.city || ""),
      phone: cleanText(delivery.phone || "").replace(/\d{4}$/, "****"),
    });

    response.json({ ok: true, id: orderId, status: "pending", totalRub, paymentUrl });
  } catch (error) {
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
