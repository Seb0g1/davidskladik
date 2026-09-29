// ─── Magic Vibes Shop API ──────────────────────────────────────────────────
const _shopCrypto = require("crypto");
const { ipKeyGenerator: _shopIpKeyGenerator } = require("express-rate-limit");

const shopLoginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  standardHeaders: true,
  legacyHeaders: false,
  validate: { keyGeneratorIpFallback: false },
  keyGenerator: (req) => {
    const email = cleanText((req.body?.email || "")).toLowerCase().slice(0, 100);
    return `${_shopIpKeyGenerator(String(req.ip || ""))}:${email}`;
  },
  handler: (_req, res) => res.status(429).json({ error: "Слишком много попыток. Попробуйте через 15 минут." }),
});
// Per-IP limits for the public shop endpoints (bots, order / e-mail flooding, abuse of the paid
// carrier and geocoder APIs). Real client IPs come from nginx (PROXY protocol → X-Forwarded-For).
// The server itself (Next.js SSR, 127.0.0.1 / own public IP) is never limited.
const SHOP_LIMIT_EXEMPT = new Set(["127.0.0.1", "::1", "::ffff:127.0.0.1", ...String(process.env.SHOP_RATE_LIMIT_EXEMPT_IPS || "81.17.154.153").split(",").map((s) => s.trim()).filter(Boolean)]);
function shopRateLimit(windowMs, max, message = "Слишком много запросов. Подождите немного и попробуйте снова.") {
  return rateLimit({
    windowMs, max, standardHeaders: true, legacyHeaders: false,
    // the key does go through ipKeyGenerator (IPv6 /56); the library only greps the function text
    validate: { keyGeneratorIpFallback: false },
    skip: (req) => SHOP_LIMIT_EXEMPT.has(req.ip) || SHOP_LIMIT_EXEMPT.has(String(req.ip || "").replace(/^::ffff:/, "")),
    keyGenerator: (req) => _shopIpKeyGenerator(String(req.ip || "")),
    handler: (_req, res) => res.status(429).json({ error: message }),
  });
}
const _shopLimits = [
  [["/api/shop/orders"], shopRateLimit(10 * 60e3, 20, "Слишком много заказов подряд. Подождите 10 минут.")],
  [["/api/shop/auth/send-code"], shopRateLimit(10 * 60e3, 10, "Слишком много запросов кода. Подождите 10 минут.")],
  [["/api/shop/auth/verify-code", "/api/shop/auth/yandex/callback"], shopRateLimit(10 * 60e3, 40)],
  [["/api/shop/auth/register"], shopRateLimit(60 * 60e3, 10)],
  [["/api/shop/geo"], shopRateLimit(60e3, 120)],
  [["/api/shop/checkout", "/api/shop/delivery"], shopRateLimit(60e3, 150)],
  [["/api/shop/ai-search"], shopRateLimit(60e3, 30)],
  [["/api/shop/email-subscribe", "/api/shop/stock-alert", "/api/shop/unboxings", "/api/shop/upload-media",
    "/api/shop/promo/validate", "/api/shop/referral/validate", "/api/shop/push/subscribe"], shopRateLimit(10 * 60e3, 30)],
  [["/api/shop/support/chats"], shopRateLimit(10 * 60e3, 120)],
];
for (const [paths, limiter] of _shopLimits) {
  // only writes and the expensive lookups; catalog/product GETs stay unlimited here (nginx caps floods)
  app.use(paths, (req, res, next) => (req.method === "OPTIONS" || (req.method === "GET" && !/\/geo|\/checkout|\/delivery/.test(req.baseUrl)) ? next() : limiter(req, res, next)));
}

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
  if (typeof sig !== "string" || sig.length !== expected.length || !_shopCrypto.timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) return null;
  let payload;
  try { payload = JSON.parse(Buffer.from(b, "base64url").toString()); } catch { return null; }
  // a stolen token must not work forever: sessions last 180 days
  if (!payload || !(Number(payload.iat) > 0) || Date.now() - Number(payload.iat) > 180 * 864e5) return null;
  return payload;
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
const SHOP_HOLIDAY_KEY = "shopHolidayBanners";
const SHOP_PROMO_CODES_KEY = "shopPromoCodes";

// Canonical holiday presets — the source of truth for all holiday banners.
// Admin settings only store overrides (active/promoCode per key).
const HOLIDAY_PRESETS = [
  {
    key: "halloween",
    name: "Хэллоуин",
    emoji: "🎃",
    defaultTitle: "Тёмная магия ароматов",
    defaultSubtitle: "Мистические и восточные парфюмы для особой ночи",
    defaultPromoCode: "HALLOWEEN20",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Тёмная коллекция",
    windowStart: [10, 26],
    windowEnd: [11, 1],
  },
  {
    key: "black_friday",
    name: "Чёрная пятница",
    emoji: "🛒",
    defaultTitle: "Чёрная пятница",
    defaultSubtitle: "Самые большие скидки года — только несколько дней",
    defaultPromoCode: "BLACKFRIDAY",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Все скидки",
    windowStart: [11, 22],
    windowEnd: [11, 30],
  },
  {
    key: "new_year",
    name: "Новый год",
    emoji: "🎄",
    defaultTitle: "Новогодние ароматы",
    defaultSubtitle: "Подарите незабываемый парфюм к праздничному столу",
    defaultPromoCode: "NEWYEAR25",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Праздничная коллекция",
    windowStart: [12, 20],
    windowEnd: [1, 9],
  },
  {
    key: "valentine",
    name: "День влюблённых",
    emoji: "💝",
    defaultTitle: "День влюблённых",
    defaultSubtitle: "Романтичный аромат — лучший подарок для двоих",
    defaultPromoCode: "LOVE14",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Романтичная коллекция",
    windowStart: [2, 10],
    windowEnd: [2, 14],
  },
  {
    key: "defender_day",
    name: "23 февраля",
    emoji: "🛡️",
    defaultTitle: "Подарок защитнику",
    defaultSubtitle: "Брутальные и стойкие мужские ароматы к 23 февраля",
    defaultPromoCode: "MEN23",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Мужские ароматы",
    windowStart: [2, 18],
    windowEnd: [2, 23],
  },
  {
    key: "womens_day",
    name: "8 марта",
    emoji: "🌷",
    defaultTitle: "С 8 марта!",
    defaultSubtitle: "Цветочные и нежные ароматы для самых любимых",
    defaultPromoCode: "MARCH8",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Женские ароматы",
    windowStart: [3, 4],
    windowEnd: [3, 8],
  },
  {
    key: "may_day",
    name: "Майские праздники",
    emoji: "🌸",
    defaultTitle: "Весна и праздники",
    defaultSubtitle: "Свежие и зелёные ароматы — символ нового сезона",
    defaultPromoCode: "MAY10",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Весенняя коллекция",
    windowStart: [4, 28],
    windowEnd: [5, 9],
  },
  {
    key: "11_11",
    name: "11.11 Распродажа",
    emoji: "🔥",
    defaultTitle: "11.11 — Мегараспродажа",
    defaultSubtitle: "Один день, лучшие цены года на все ароматы",
    defaultPromoCode: "1111",
    defaultLinkUrl: "/catalog",
    defaultLinkText: "Смотреть все скидки",
    windowStart: [11, 9],
    windowEnd: [11, 11],
  },
];

function _isHolidayInWindow(preset) {
  const now = new Date();
  const [sm, sd] = preset.windowStart;
  const [em, ed] = preset.windowEnd;
  const month = now.getMonth() + 1;
  const day = now.getDate();
  // Handles cross-year windows (e.g. Dec 20 – Jan 9)
  if (sm <= em) {
    return (month > sm || (month === sm && day >= sd)) &&
           (month < em || (month === em && day <= ed));
  }
  return (month > sm || (month === sm && day >= sd)) ||
         (month < em || (month === em && day <= ed));
}

async function readHolidaySettings() {
  const appSettings = await readAppSettings();
  return appSettings[SHOP_HOLIDAY_KEY] || {};
}

async function buildHolidayBanners() {
  const overrides = await readHolidaySettings();
  const result = [];
  for (const preset of HOLIDAY_PRESETS) {
    const ov = overrides[preset.key] || {};
    const inWindow = _isHolidayInWindow(preset);
    // Show if manually enabled OR auto-active (in window and not explicitly disabled)
    const isActive = ov.active === true || (inWindow && ov.active !== false);
    if (!isActive) continue;
    result.push({
      id: `holiday-${preset.key}`,
      holidayKey: preset.key,
      imageUrl: "",
      title: ov.title || preset.defaultTitle,
      subtitle: ov.subtitle || preset.defaultSubtitle,
      promoCode: ov.promoCode || preset.defaultPromoCode,
      linkUrl: ov.linkUrl || preset.defaultLinkUrl,
      linkText: ov.linkText || preset.defaultLinkText,
      active: true,
      order: -1,
    });
  }
  return result;
}

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
    deliveryDays: 5,
    deliveryDaysMin: 1,
    deliveryPriceRub: 350,
    freeDeliveryFrom: 3000,
    vipTelegramLink: "",
  };
}

// Retail price for the shop. Currency comes from the product link (what the warehouse uses);
// PriceMaster snapshots of some suppliers (e.g. «Инна») say USD for rouble prices, which turned
// 34 470 ₽ into 6.7 M ₽. Guard: a non-RUB result above 3× the marketplace price is a currency
// mix-up → price it as roubles.
function shopRetailPrice({ snapPrice, snapCurrency, linkCurrency, currentPrice, markup, usdRate }) {
  const current = Number(currentPrice || 0);
  if (!(snapPrice > 0)) return current > 0 ? current : 0;
  const cur = String(linkCurrency || snapCurrency || "USD").toUpperCase();
  if (cur === "RUB") return Math.round(snapPrice * markup);
  const rub = Math.round(snapPrice * usdRate * markup);
  if (current > 0 && rub > current * 3) return Math.round(snapPrice * markup);
  return rub;
}

// PriceMaster rows belong to a partner: the same article code is a different product at another
// supplier (19-69 L'Air Barbès at «Инна» shares its code with an Elizabeth Arden tester at another
// partner → the shop sold it for 192 ₽). Match (partner, article) like the warehouse does
// (02a-price-master-link-lookup pairKey); article-only only for the few links without a partner.
const shopPmKey = (partnerId, article) => `${cleanText(partnerId || "")}|${cleanText(article || "")}`;
function shopPmIndex(snaps) {
  const map = new Map();
  for (const s of snaps || []) {
    for (const k of [shopPmKey(s.partnerId, s.article), shopPmKey("", s.article)]) {
      const cur = map.get(k);
      if (!cur || Number(s.price) < Number(cur.price)) map.set(k, s);
    }
  }
  return map;
}
function shopPmLookup(pmMap, link) {
  return pmMap.get(shopPmKey(link.partnerId ? link.partnerId : "", link.supplierArticle)) || null;
}

// Struck-through price = the real price of the same product on Ozon (or Yandex Market): shown only
// when it is ≥ 10 % above the site price and not a "blocking" price. Never sent as <oldprice> in the YML.
function shopMarketplaceOldPrice(priceRub, marketplacePrice) {
  const mp = Number(marketplacePrice || 0);
  if (!(priceRub > 0) || !(mp > 0) || mp >= 150000) return undefined;
  if (mp < priceRub * 1.1 || mp > priceRub * 3) return undefined;
  return Math.round(mp);
}

// Cheapest supplier offer across ALL links of a product. Using only links[0] fell back to the
// marketplace "blocking" price (e.g. 971 011 ₽) whenever the first supplier had no snapshot.
function shopPriceFromLinks(links, pmMap, { currentPrice, defaultMarkup, rules, usdRate }) {
  let best = 0;
  // Sanity band against the marketplace price (site prices are ~0.5–0.7 of it): a wrong link to another
  // product or a tiny tester price produced 169 ₽ Fidji and 24 031 ₽ Burberry Hero. Blocking marketplace
  // prices (> 150 000 ₽ = "not for sale") give no reference.
  const ref = Number(currentPrice || 0);
  const band = ref > 0 && ref < 150000;
  for (const link of links || []) {
    const snap = shopPmLookup(pmMap, link);
    const snapPrice = snap ? Number(snap.price || 0) : 0;
    if (!(snapPrice > 1)) continue; // 0 / 1 are placeholders in some price lists («Наш склад»)
    // rules are "from X USD": a rouble price list is looked up by its USD equivalent
    const cur = String(link.priceCurrency || snap.currency || "USD").toUpperCase();
    const markup = resolveShopMarkup(cur === "RUB" ? snapPrice / usdRate : snapPrice, defaultMarkup, rules);
    const rub = shopRetailPrice({ snapPrice, snapCurrency: snap.currency, linkCurrency: link.priceCurrency, currentPrice, markup, usdRate });
    if (band && (rub < ref * 0.35 || rub > ref * 2)) continue;
    if (rub > 0 && (!best || rub < best)) best = rub;
  }
  const current = Number(currentPrice || 0);
  return { priceRub: best || (current > 0 ? current : 0), fromSupplier: best > 0 };
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

const DEFAULT_PROMO_CODES = [
  { id: "builtin-vibes10",  code: "VIBES10",    discountPct: 10, active: true,  usageLimit: null, usageCount: 0, note: "Попап —10% на первый заказ",   builtin: true },
  { id: "builtin-quiz10",   code: "QUIZ10",     discountPct: 10, active: true,  usageLimit: null, usageCount: 0, note: "Квиз —10% подборка",            builtin: true },
  { id: "builtin-review5",  code: "REVIEW5",    discountPct: 5,  active: true,  usageLimit: null, usageCount: 0, note: "Отзыв —5% на следующий заказ",  builtin: true },
  { id: "builtin-unbox7",   code: "UNBOX7",     discountPct: 7,  active: true,  usageLimit: null, usageCount: 0, note: "Анбоксинг +7% к промокоду",     builtin: true },
];

async function readShopPromoCodes() {
  const appSettings = await readAppSettings();
  const stored = appSettings[SHOP_PROMO_CODES_KEY];
  if (!Array.isArray(stored) || stored.length === 0) return DEFAULT_PROMO_CODES;
  // Merge builtins with stored overrides
  const storedById = Object.fromEntries(stored.map(c => [c.id, c]));
  const merged = DEFAULT_PROMO_CODES.map(def => storedById[def.id] ? { ...def, ...storedById[def.id] } : def);
  const custom = stored.filter(c => !c.builtin);
  return [...merged, ...custom];
}

async function writeShopPromoCodes(codes) {
  await writeShopData(SHOP_PROMO_CODES_KEY, codes);
}

// Single-entry lock for promo code mutations: prevents lost usageCount increments
// when two orders with the same promo code arrive simultaneously.
let _shopPromoLockPromise = Promise.resolve();
async function withShopPromoLock(worker) {
  const run = _shopPromoLockPromise.then(() => worker());
  _shopPromoLockPromise = run.catch(() => {});
  return run;
}

async function validatePromoCode(code) {
  if (!code) return null;
  const upper = String(code).toUpperCase().trim();
  const codes = await readShopPromoCodes();
  const found = codes.find(c => c.code.toUpperCase() === upper && c.active);
  if (!found) return null;
  if (found.usageLimit !== null && found.usageCount >= found.usageLimit) return null;
  if (found.expiresAt && new Date(found.expiresAt) < new Date()) return null;
  return found;
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

// ── Shop "collections": menu words that are not in product names ────────────
// q=элитная / нишевая / арабская / цветочный … used to search names literally and
// always returned 0. Each collection maps to brand prefixes, name keywords and
// fragranceNotes accords.
const SHOP_ARABIC_BRANDS = [
  "Lattafa", "Rasasi", "Ajmal", "Armaf", "Afnan", "Al Haramain", "Al-Jazeera", "Alhambra", "Maison Alhambra",
  "Arabian Oud", "Arabian wind", "Ard Al Zaafaran", "Swiss Arabian", "Nabeel", "Fragrance World", "Zimaya",
  "Khadlaj", "Paris Corner", "Al Rehab", "Orientica", "Aj Arabia", "Alghabra", "Asdaaf", "Arabesque",
  "Amouroud", "Emir", "French Avenue", "Riiffs", "Nusuk", "Ibraheem Al Qurashi", "Abdul Samad Al Qurashi",
  "Al Wataniah", "Anfar", "Hamidi", "My Perfumes", "Otoori",
];
const SHOP_NICHE_BRANDS = [
  "Amouage", "Byredo", "Creed", "Kilian", "Initio", "Parfums de Marly", "Maison Francis Kurkdjian", "Xerjoff",
  "Nishane", "Memo", "Le Labo", "Diptyque", "Frederic Malle", "Serge Lutens", "Penhaligon", "Juliette Has a Gun",
  "Escentric Molecules", "Zarkoperfume", "Ex Nihilo", "Tiziana Terenzi", "Orto Parisi", "Nasomatto", "Mancera",
  "Montale", "Atelier Cologne", "Acqua di Parma", "Atelier des Ors", "Vilhelm", "Maison Margiela", "Replica",
  "Boadicea", "Clive Christian", "Roja", "Bdk", "BDK Parfums", "Goldfield", "Kajal", "Sospiro", "Mind Games",
  "3Mind Games", "Profumum", "Etat Libre", "Heeley", "Histoires de Parfums", "L'Artisan", "Carner",
  "Arte Profumi", "Arteolfatto", "Alyson Oldoini", "12 Parfumeurs", "Aedes de Venustas", "Affinessence",
  "Maison Crivelli", "Parle Moi de Parfum", "Masque Milano", "Fueguia", "Frapin", "Marc-Antoine Barrois",
  "Louis Vuitton", "Essential Parfums", "Ormonde Jayne", "Room 1015", "Stephane Humbert Lucas", "Nishane",
  "Unique'e Luxury", "Vertus", "Zoologist", "Jovoy", "Bond No. 9", "Bond No 9", "Electimuss", "Lorenzo Pazzaglia",
  "Filippo Sorcinelli", "Andy Tauer", "Mona di Orio", "Olfactive Studio", "Parfums MDCI", "Hermetica",
];
const SHOP_LUXURY_BRANDS = [
  "Chanel", "Dior", "Christian Dior", "Tom Ford", "Guerlain", "Yves Saint Laurent", "YSL", "Giorgio Armani", "Armani",
  "Hermes", "Hermès", "Givenchy", "Prada", "Gucci", "Versace", "Dolce", "Valentino", "Lancome", "Lancôme",
  "Cartier", "Bvlgari", "Bulgari", "Chloe", "Chloé", "Burberry", "Carolina Herrera", "Jo Malone", "Narciso Rodriguez",
  "Viktor", "Jean Paul Gaultier", "Paco Rabanne", "Rabanne", "Mugler", "Thierry Mugler", "Balenciaga",
  "Celine", "Louis Vuitton", "Van Cleef", "Boucheron", "Montblanc", "Kenzo", "Marc Jacobs", "Loewe",
  "Salvatore Ferragamo", "Bottega Veneta", "Chopard", "Elie Saab", "Escada", "Moschino", "Hugo Boss", "Boss",
  "Kilian", "Creed", "Parfums de Marly", "Maison Francis Kurkdjian", "Xerjoff", "Clive Christian", "Roja",
];
const SHOP_QUERY_COLLECTIONS = [
  { match: /^(элитн|люкс|премиум|luxury|premium)/i, brands: SHOP_LUXURY_BRANDS },
  { match: /^(нишев|ниша|niche)/i, brands: SHOP_NICHE_BRANDS },
  { match: /^(арабск|восточная ?\/ ?арабская|arab)/i, brands: SHOP_ARABIC_BRANDS, names: [" oud", "oud ", " уд ", "attar", "bakhoor", "бахур"] },
  { match: /^женск/i, names: ["женск", "for women", "pour femme", "for her"], gender: "female" },
  { match: /^мужск/i, names: ["мужск", "for men", "pour homme", "for him"], gender: "male" },
  { match: /^унисекс|^unisex/i, names: ["унисекс", "unisex"], gender: "unisex" },
  { match: /^детск/i, names: ["детск", "для детей", "kids", "children"] },
  { match: /^миниатюр/i, names: ["миниатюр", "mini ", " мини", "travel", "10 мл", "5 мл", "7.5 мл", "7,5 мл"] },
  { match: /^цветоч|^floral/i, accords: ["Floral", "White Floral", "Rose"],
    names: ["rose", "роза", "jasmin", "жасмин", "fleur", "flower", "bloom", "пион", "peony", "iris", "ирис", "tuberose", "тубероз", "lily", "лили", "magnolia", "gardenia", "violet", "фиалк", "orchid", "орхиде", "neroli", "нероли", "blossom", "petal", "garden"] },
  { match: /^древес|^woody/i, accords: ["Woody", "Woody Oriental"],
    names: ["wood", "дерев", "cedar", "кедр", "santal", "сандал", "vetiver", "ветивер", "oud", "уд ", "bois", "patchouli", "пачули", "forest", "лес", "birch", "берез", "cypress"] },
  { match: /^цитрус|^citrus/i, accords: ["Citrus", "Fresh Citrus"],
    names: ["citrus", "цитрус", "lemon", "лимон", "bergamot", "бергамот", "orange", "апельсин", "mandarin", "мандарин", "grapefruit", "грейпфрут", "lime", "лайм", "yuzu", "юдзу", "neroli", "нероли", "agrumi", "limone", "bigarade"] },
  { match: /^мускус|^musk/i, accords: ["Musky", "Musk"],
    names: ["musk", "мускус", "musc", "skin", "молекул", "molecule", "cashmere", "кашемир", "cotton", "clean"] },
  { match: /^восточн|^oriental|^amber/i, accords: ["Oriental", "Amber", "Spicy", "Warm Spicy", "Balsamic"],
    names: ["oud", "уд ", "amber", "амбр", "ambre", "oriental", "восточ", "spice", "прян", "incense", "ладан", "saffron", "шафран", "vanill", "ванил", "tobacco", "табак", "bakhoor", "бахур", "myrrh", "мирр"] },
  { match: /^свеж|^fresh/i, accords: ["Fresh", "Aquatic", "Marine", "Ozonic", "Green", "Fresh Spicy"],
    names: ["fresh", "свеж", "aqua", "аква", "acqua", "water", "eau fraiche", "marine", "морск", "ocean", "океан", "sea ", "breeze", "blue", "bleu", "sport", "cool", "ice", "green", "зелен", "mint", "мят"] },
  { match: /^фужер|^fougere/i, accords: ["Aromatic", "Fougere", "Lavender"],
    names: ["fougere", "фужер", "lavender", "лаванд", "lavande", "geranium", "герань", "sage", "шалфей", "rosemary", "розмарин", "barber", "tonka", "тонка", "moss", "мох"] },
  { match: /^(сладк|гурман|gourmand)/i, accords: ["Sweet", "Gourmand", "Vanilla"],
    names: ["vanill", "ванил", "caramel", "карамел", "sugar", "candy", "chocolate", "шоколад", "honey", "мёд", "gourmand", "praline", "пралине", "cake", "cookie", "cherry", "вишн", "coffee", "кофе"] },
  { match: /^шипр|^chypre/i, accords: ["Chypre", "Mossy"],
    names: ["chypre", "шипр", "moss", "мох", "oakmoss", "patchouli", "пачули", "labdanum", "лабданум", "leather", "кож"] },
];

function resolveShopQueryCollection(q) {
  const s = cleanText(q || "").toLowerCase();
  if (!s || s.length > 40) return null;
  return SHOP_QUERY_COLLECTIONS.find((c) => c.match.test(s)) || null;
}

// Rule-based reading of a free-text wish ("свежий морской для офиса, мужской")
// when the LLM is unavailable: stems → collection queries understood above.
const SHOP_AI_FALLBACK_RULES = [
  { re: /морск|океан|аква|свеж|лёгк|легк|летн|лето|спорт|офис|работ|днев|утр|чист/i, q: "свежий", label: "свежий" },
  { re: /цвет|роз|жасмин|пион|нежн|романт|весенн|весна|весной/i, q: "цветочный", label: "цветочный" },
  { re: /дерев|древес|кедр|сандал|ветивер|лес/i, q: "древесный", label: "древесный" },
  { re: /цитрус|лимон|апельсин|бергамот|грейпфрут|мандарин/i, q: "цитрусовый", label: "цитрусовый" },
  { re: /слад|ванил|гурман|карамел|шоколад|десерт|кофе|вишн|мёд|медов/i, q: "сладкий", label: "сладкий" },
  { re: /тёпл|тепл|прян|восточ|(^|\s)уд(\s|$)|амбр|ладан|вечер|ноч|соблазн|зим|осен|шлейф/i, q: "восточный", label: "тёплый восточный" },
  { re: /мускус|уют|кож[аи]|пудр/i, q: "мускусный", label: "мускусный" },
  { re: /лаванд|фужер|барбер/i, q: "фужерный", label: "фужерный" },
  { re: /шипр|мох|пачул/i, q: "шипровый", label: "шипровый" },
  { re: /араб/i, q: "арабская", label: "арабский" },
  { re: /ниш|редк|необычн/i, q: "нишевая", label: "нишевый" },
  { re: /элит|люкс|дорог|статус|премиум/i, q: "элитная", label: "элитный" },
];

function parseShopAiQueryFallback(query) {
  const text = cleanText(query || "").toLowerCase();
  const collections = [];
  const labels = [];
  for (const rule of SHOP_AI_FALLBACK_RULES) {
    if (rule.re.test(text) && !collections.includes(rule.q)) { collections.push(rule.q); labels.push(rule.label); }
  }
  let gender = "any";
  if (/мужч|мужск|(^|\s)муж|парн|для него|папе|пап[аы]|отц|брат|сын/i.test(text)) gender = "male";
  else if (/женщ|женск|девушк|для неё|для нее|мам[аеыу]|сестр|подруг|дочер|(^|\s)жен[аеуы](\s|,|$)/i.test(text)) gender = "female";
  else if (/унисекс/i.test(text)) gender = "unisex";
  const genderLabel = gender === "male" ? "мужской" : gender === "female" ? "женский" : "";
  const label = [labels.slice(0, 2).join(", "), genderLabel].filter(Boolean).join(" · ") || text.slice(0, 60);
  return { collections: collections.slice(0, 4), gender, label: label.charAt(0).toUpperCase() + label.slice(1) };
}

// Collections are perfume groupings: without this, "древесный" also found hair dye
// shades named "Сандаловое"/"Кедр" and creams "с ароматом розы".
const SHOP_PERFUME_NAME_WORDS = ["парфюм", "туалетн", "духи", "одеколон", "eau de", "parfum", "extrait", "edp", "edt", "аромат для", "отливант", "пробник"];
const SHOP_NON_PERFUME_WORDS = ["краск", "крем", "шампун", "бальзам", "маск", "гель", "лосьон", "мыло", "свеч", "оттен", "окрашив", "тонер", "сыворот"];

function buildShopPerfumeOnlyCondition() {
  return {
    OR: SHOP_PERFUME_NAME_WORDS.map((w) => ({ name: { contains: w, mode: "insensitive" } })),
    NOT: SHOP_NON_PERFUME_WORDS.map((w) => ({ name: { contains: w, mode: "insensitive" } })),
  };
}

// Prisma OR-conditions for a collection. Brands are prefix-matched because the
// brand field often contains "Brand + model" ("ALHAMBRA DARK AOUD").
function buildShopCollectionConditions(col) {
  const or = [];
  for (const b of col.brands || []) {
    or.push({ brand: { startsWith: b, mode: "insensitive" } });
    or.push({ name: { startsWith: b, mode: "insensitive" } });
  }
  for (const n of col.names || []) or.push({ name: { contains: n, mode: "insensitive" } });
  for (const a of col.accords || []) or.push({ fragranceNotes: { path: ["accords"], array_contains: a } });
  if (col.gender) or.push({ fragranceNotes: { path: ["gender"], equals: col.gender } });
  return or;
}

async function buildShopProductsFromDb({ q, brand, category, inStock, sort, page, pageSize, createdAfter, andQ, perfumeOnly }) {
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
  const _collection = q ? resolveShopQueryCollection(q) : null;
  if (q) {
    where.OR = [
      { name: { contains: q, mode: "insensitive" } },
      { brand: { contains: q, mode: "insensitive" } },
      { offerId: { contains: q, mode: "insensitive" } },
      ...(_collection ? buildShopCollectionConditions(_collection) : []),
    ];
    if (_collection) where.AND = [...(where.AND || []), buildShopPerfumeOnlyCondition()];
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
      where.AND = [...(where.AND || []), { OR: _catOr }];
    }
  }

  // perfumeOnly: gift configurator etc. — no hair dye / creams in "choose a fragrance"
  if (perfumeOnly) where.AND = [...(where.AND || []), buildShopPerfumeOnlyCondition()];

  // andQ narrows results to a second collection (AI search: "свежий" AND "мужская")
  const _andCollection = andQ ? resolveShopQueryCollection(andQ) : null;
  if (_andCollection) {
    where.AND = [...(where.AND || []), { OR: buildShopCollectionConditions(_andCollection) }];
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
        links: { take: 5, select: { supplierArticle: true, priceCurrency: true, partnerId: true } },
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
      select: { article: true, price: true, currency: true, partnerId: true },
    }) : [],
    dedupedOfferIds.length ? prisma.shopReview.groupBy({
      by: ["offerId"],
      where: { approved: true, offerId: { in: dedupedOfferIds } },
      _avg: { rating: true },
      _count: { rating: true },
    }) : [],
  ]);

  const pmMap = shopPmIndex(pmSnaps);

  const reviewMap = new Map();
  for (const g of reviewGroups) {
    if (g.offerId) reviewMap.set(g.offerId, { avg: g._avg.rating || 0, count: g._count.rating || 0 });
  }

  // Build shop products
  const products = deduped.map((p) => {
    const images = extractImages(p);
    const currentPriceNum = Number(p.currentPrice || 0);
    const { priceRub } = shopPriceFromLinks(p.links, pmMap, { currentPrice: currentPriceNum, defaultMarkup, rules: shopMarkupRules, usdRate });

    const stockQty = p.targetStock ?? 0;
    const name = normalizeProductName(cleanText(p.name || ""));
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
      oldPriceRub: shopMarketplaceOldPrice(priceRub, currentPriceNum),
      inStock: stockQty > 0 || (p.status !== "archived" && currentPriceNum > 0),
      stockQty: Math.max(0, stockQty),
      volume: extractVolume(name || p.name || ""),
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
  { slug: "edt",    label: "Туалетная вода",    pattern: /туалетн\S*\s*вода|eau[\s-]de[\s-]toilette|\bedt\b/i,           keywords: ["туалетная вода", "eau de toilette"] },
  { slug: "edc",    label: "Одеколон",          pattern: /одеколон|eau[\s-]de[\s-]cologne|\bedc\b/i,                    keywords: ["одеколон", "eau de cologne"] },
  { slug: "deo",    label: "Дезодоранты",       pattern: /дезодорант|антиперспирант|deodorant/i,                        keywords: ["дезодорант", "антиперспирант", "deodorant"] },
  { slug: "home",   label: "Ароматы для дома",  pattern: /свеч[аи]|аромасвеч|candle/i,                                 keywords: ["свеча", "свечи", "аромасвеча", "candle"] },
  { slug: "sets",   label: "Подарочные наборы", pattern: /набор|gift[\s-]set/i,                                         keywords: ["набор", "gift set"] },
  { slug: "body",   label: "Уход за телом",     pattern: /крем|лосьон|масло.{0,8}тел|гель.{0,8}душ|шампун/i,           keywords: ["крем", "лосьон", "масло для тела", "гель для душа", "шампунь"] },
];

function extractVolume(name = "") {
  // no \b after "мл": JS word boundaries don't see Cyrillic letters, so "50 мл" never matched
  const m = name.match(/(\d+(?:[.,]\d+)?\s*(?:мл|ml|г|g|oz))(?![a-zа-яё])/i);
  return m ? m[1] : undefined;
}

function extractProductCategory(name = "") {
  for (const cat of SHOP_CATEGORIES) {
    if (cat.pattern.test(name)) return { slug: cat.slug, label: cat.label };
  }
  return { slug: "parfumery", label: "Парфюмерия" };
}

// Normalize product display name: strip leading punctuation, ensure space before volume, unify ml→мл
function normalizeProductName(name) {
  if (!name) return name;
  let s = String(name);
  // Strip leading punctuation chars that sneak in from marketplace imports
  s = s.replace(/^[\s)\]([{\-/\\|,;:!?@#$%^&*~`'"«»‘’“”]+/, "");
  // Ensure space between word chars and volume: "Chronic100 мл" → "Chronic 100 мл"
  s = s.replace(/([a-zA-Zа-яёА-ЯЁ])(\d+\s*(?:мл|ml|г|g|oz)\b)/gi, "$1 $2");
  // Unify "100ml" / "100 ml" → "100 мл"
  s = s.replace(/(\d+)\s*ml\b/gi, "$1 мл");
  // Ensure space between digit and Cyrillic unit: "100мл" → "100 мл", "50г" → "50 г"
  s = s.replace(/(\d)(мл|г)(?=\s|$|[,.)\/])/gi, "$1 $2");
  // Collapse multiple spaces
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

async function findShopProductByOfferId(offerId, { fast = false } = {}) {
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
      links: { take: 5, select: { supplierArticle: true, priceCurrency: true, partnerId: true } },
    },
    take: 5,
  });

  if (!products.length) return null;

  // prefer Ozon
  // prefer Ozon, unless its listing was anonymised («парфюмерная вода» + a blank-bottle photo) — then Yandex
  const usable = products.filter((pr) => !shopNameMasked(pr.name) && !_shopPlaceholderImgs.has(shopFirstImage(pr)));
  const p = usable.find((pr) => pr.marketplace === "ozon") || usable[0] || products.find((pr) => pr.marketplace === "ozon") || products[0];
  // fast (YCP, 5 s budget): only already-known image hashes, no downloads
  const images = await stripMarketplaceOnlyImages(extractImages(p), { fetchMissing: !fast });

  let description = "";
  try {
    const state = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
    description = cleanText(state.description || state.desc || state.productDescription || "");
    // Ozon rows rarely carry a description; the Yandex Market row of the same offerId almost always does
    for (const row of [p, ...products.filter((x) => x !== p)]) {
      if (description) break;
      const raw = row.raw && typeof row.raw === "object" ? row.raw : {};
      description = cleanText(raw.ozon?.description || raw.yandex?.description || "");
    }
  } catch (_) {}
  let brandResolved = cleanText(p.brand || "");
  if (!brandResolved) {
    for (const row of products) {
      const v = cleanText(row.raw?.yandex?.vendor || "");
      if (v) { brandResolved = v; break; }
    }
  }

  const articles = p.links.map((l) => cleanText(l.supplierArticle)).filter(Boolean);
  const snapsSingle = articles.length ? await prisma.priceMasterSnapshotItem.findMany({
    where: { article: { in: articles }, active: true },
    select: { article: true, price: true, currency: true, partnerId: true },
  }) : [];
  const currentPriceNum = Number(p.currentPrice || 0);
  // same rule as the catalog and the feed, so the card, the cart and the order agree
  const { priceRub } = shopPriceFromLinks(p.links, shopPmIndex(snapsSingle), {
    currentPrice: currentPriceNum, defaultMarkup: defaultMarkupSingle, rules: shopMarkupRulesSingle, usdRate,
  });
  const normalizedName = normalizeProductName(cleanText(p.name || ""));
  const _pCat = extractProductCategory(normalizedName);

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
    name: normalizedName || cleanText(p.offerId),
    brand: brandResolved,
    description: shopCleanDescription(description),
    images,
    priceRub,
    oldPriceRub: shopMarketplaceOldPrice(priceRub, currentPriceNum),
    inStock: (p.targetStock ?? 0) > 0 || (p.status !== "archived" && currentPriceNum > 0),
    stockQty: Math.max(0, p.targetStock ?? 0),
    volume: extractVolume(normalizedName || p.name || ""),
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

// ── Catalog over the cached feed ─────────────────────────────────────────────
// The DB path filtered «в наличии» per page only (total never changed), sorted by the marketplace
// price instead of the shop price, listed brands of the current page only and took 10 s for
// «женская + парфюмерная вода». The feed (getShopFeedProducts, 1 h) already has shop prices,
// resolved brands, volumes and categories, so every filter here is exact and in-memory.
// Collections (женская, нишевая, цветочный…) need fragrance notes → one offerId query per
// collection, cached for an hour.
const _shopCollectionIds = new Map(); // key → { at, ids:Set }
async function shopCollectionOfferIds(key) {
  const col = resolveShopQueryCollection(key);
  if (!col) return null;
  const hit = _shopCollectionIds.get(key);
  if (hit && Date.now() - hit.at < SHOP_FEED_TTL) return hit.ids;
  const prisma = getPrisma();
  if (!prisma) return new Set();
  const rows = await prisma.warehouseProduct.findMany({
    where: { archived: false, OR: buildShopCollectionConditions(col), AND: [buildShopPerfumeOnlyCondition()] },
    select: { offerId: true },
  });
  const ids = new Set(rows.map((r) => cleanText(r.offerId).toLowerCase()));
  _shopCollectionIds.set(key, { at: Date.now(), ids });
  return ids;
}

const SHOP_PERFUME_RE = new RegExp(SHOP_PERFUME_NAME_WORDS.map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|"), "i");
const SHOP_NON_PERFUME_RE = new RegExp(SHOP_NON_PERFUME_WORDS.join("|"), "i");
const shopNorm = (x) => String(x || "").toLowerCase().replace(/ё/g, "е");
const shopMl = (v) => {
  const m = String(v || "").match(/(\d+(?:[.,]\d+)?)\s*(мл|ml)/i);
  return m ? Math.round(parseFloat(m[1].replace(",", ".")) * 10) / 10 : null;
};

async function buildShopCatalogFromFeed(opts) {
  const { q, brand, category, inStock, sort, page, pageSize, perfumeOnly, priceMin, priceMax, volume, tags } = opts;
  const feed = await getShopFeedProducts();

  // collection filters: q itself when it is a collection word, plus gender / line / group params
  const colKeys = [...new Set([...(resolveShopQueryCollection(q) ? [q.toLowerCase()] : []), ...tags.map((t) => t.toLowerCase())])]
    .filter((k) => resolveShopQueryCollection(k));
  const colSets = await Promise.all(colKeys.map(shopCollectionOfferIds));
  const textTerms = resolveShopQueryCollection(q) ? [] : shopNorm(q).split(/\s+/).filter((t) => t.length > 0).slice(0, 6);
  const brandN = shopNorm(brand);

  const base = feed.filter((p) => {
    if (colSets.length && !colSets.every((set) => set?.has(p.offerId.toLowerCase()))) return false;
    if (textTerms.length) {
      const hay = shopNorm(`${p.name} ${p.brand} ${p.offerId}`);
      if (!textTerms.every((t) => hay.includes(t))) return false;
    }
    if (category && category !== "parfumery" && p.category !== category) return false;
    // collections are perfume groupings (the DB query already asks for it; hair dye slipped through by notes)
    if ((perfumeOnly || colSets.length) && (!SHOP_PERFUME_RE.test(p.name) || SHOP_NON_PERFUME_RE.test(p.name))) return false;
    if (inStock && !p.inStock) return false;
    return true;
  });

  // facets are counted without their own filter (like marketplaces: brand list ignores the chosen brand)
  // brands come from the same resolution as /api/shop/brands → exact match; legacy long names
  // ("12 Parfumeurs Francais") still find their products by the name prefix
  const byBrand = (p) => !brandN || shopNorm(p.brand) === brandN || shopNorm(p.name).startsWith(brandN) || (brandN.startsWith(shopNorm(p.brand) + " ") && shopNorm(p.name).startsWith(shopNorm(p.brand)));
  const byPrice = (p) => (!priceMin || p.priceRub >= priceMin) && (!priceMax || p.priceRub <= priceMax);
  const byVolume = (p) => !volume || shopMl(p.volume) === volume;

  const brandCount = new Map();
  const volCount = new Map();
  let pMin = Infinity, pMax = 0;
  for (const p of base) {
    const okB = byBrand(p), okP = byPrice(p), okV = byVolume(p);
    if (okP && okV && p.brand) {
      const k = p.brand.toUpperCase();
      const cur = brandCount.get(k);
      brandCount.set(k, { name: cur?.name || p.brand, count: (cur?.count || 0) + 1 });
    }
    if (okB && okP) { const ml = shopMl(p.volume); if (ml) volCount.set(ml, (volCount.get(ml) || 0) + 1); }
    if (okB && okV) { if (p.priceRub < pMin) pMin = p.priceRub; if (p.priceRub > pMax) pMax = p.priceRub; }
  }
  const list = base.filter((p) => byBrand(p) && byPrice(p) && byVolume(p));

  const collator = new Intl.Collator("ru");
  if (sort === "price_asc") list.sort((a, b) => a.priceRub - b.priceRub);
  else if (sort === "price_desc") list.sort((a, b) => b.priceRub - a.priceRub);
  else if (sort === "new") list.sort((a, b) => String(b.lastmod || "").localeCompare(String(a.lastmod || "")));
  else if (sort === "name") list.sort((a, b) => collator.compare(a.name, b.name));
  else {
    // «Популярные» (default): reviews, rating, then perfume with photos before dyes / creams / photo-less rows
    const score = (p) => (SHOP_PERFUME_RE.test(p.name) && !SHOP_NON_PERFUME_RE.test(p.name) ? 2 : 0) + (p.images?.length ? 1 : 0) + (p.inStock ? 1 : 0);
    list.sort((a, b) => (b.reviewCount - a.reviewCount) || (b.rating - a.rating) || (score(b) - score(a)) || collator.compare(a.name, b.name));
  }

  const products = list.slice((page - 1) * pageSize, page * pageSize).map((p) => ({
    id: p.id || p.offerId, offerId: p.offerId, name: p.name, brand: p.brand, description: "",
    images: p.images, priceRub: p.priceRub, oldPriceRub: p.oldPriceRub, inStock: p.inStock, stockQty: p.stockQty || 0,
    volume: p.volume || undefined, category: p.category, categoryLabel: p.categoryLabel, tags: [],
    rating: p.rating || 0, reviewCount: p.reviewCount || 0,
  }));
  const brandsFacet = [...brandCount.values()].sort((a, b) => b.count - a.count || collator.compare(a.name, b.name)).slice(0, 300);
  return {
    products,
    total: list.length,
    brands: brandsFacet.map((b) => b.name),
    facets: {
      brands: brandsFacet,
      volumes: [...volCount.entries()].filter(([, n]) => n >= 2).sort((a, b) => b[1] - a[1]).slice(0, 14)
        .map(([ml, count]) => ({ ml, count })).sort((a, b) => a.ml - b.ml),
      price: { min: Number.isFinite(pMin) ? pMin : 0, max: pMax },
    },
  };
}

app.get("/api/shop/catalog", shopCors, async (request, response, next) => {
  try {
    const page = Math.max(1, Number(request.query.page || 1) || 1);
    const pageSize = Math.max(1, Math.min(96, Number(request.query.pageSize || 24) || 24));
    const q = cleanText(request.query.q || "");
    const brand = cleanText(request.query.brand || "");
    const category = cleanText(request.query.category || "");
    const inStock = request.query.inStock === "true";
    const perfumeOnly = request.query.perfumeOnly === "true";
    const sort = ["price_asc", "price_desc", "name", "new", "popular"].includes(request.query.sort) ? request.query.sort : "popular";
    const priceMin = Math.max(0, Number(request.query.priceMin) || 0);
    const priceMax = Math.max(0, Number(request.query.priceMax) || 0);
    const volume = Number(String(request.query.volume || "").replace(",", ".")) || 0;
    // collection filters on top of q: gender / line / group (женская, нишевая, цветочный…)
    const tags = ["gender", "line", "group"].map((k) => cleanText(request.query[k] || "")).filter(Boolean);

    // feed not built yet (cold start) → old DB path, and start the build
    if (!_shopFeedCache) {
      getShopFeedProducts().catch(() => {});
      const result = await buildShopProductsFromDb({ q: q || tags[0] || "", brand, category, inStock, sort: ["new", "popular"].includes(sort) ? "name" : sort, page, pageSize, perfumeOnly });
      return response.json({ ok: true, ...result, page, pageSize });
    }
    const result = await buildShopCatalogFromFeed({ q, brand, category, inStock, sort, page, pageSize, perfumeOnly, priceMin, priceMax, volume, tags });
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

// AI semantic search for fragrances
app.post("/api/shop/ai-search", shopCors, async (request, response, next) => {
  try {
    const query = cleanText(request.body?.query || "").slice(0, 400);
    if (!query) return response.status(400).json({ error: "query is required" });
    // own scent engine (02d-shop-scent-engine.js): notes/accords index + query parsing, no external LLM
    // (OpenAI answers 403 from the RU server, and the old path returned alphabetical keyword hits)
    const result = await shopScentSearch(query);
    logger.info("shop_ai_search", { q: query.slice(0, 120), n: result.products.length, label: result.label });
    response.json(result);
  } catch (error) { next(error); }
});

let _bannersCache = null;
let _bannersCacheAt = 0;
const _BANNERS_TTL = 10 * 60 * 1000; // 10 min

app.get("/api/shop/banners", shopCors, async (_request, response, next) => {
  try {
    if (_bannersCache && Date.now() - _bannersCacheAt < _BANNERS_TTL) {
      return response.json(_bannersCache);
    }
    const [banners, holidayBanners] = await Promise.all([readShopBanners(), buildHolidayBanners()]);
    const active = banners.filter((b) => b.active).sort((a, b) => a.order - b.order);
    _bannersCache = [...holidayBanners, ...active];
    _bannersCacheAt = Date.now();
    response.json(_bannersCache);
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

let _autoCatsCache = null;
let _autoCatsCacheAt = 0;
const _AUTO_CATS_TTL = 20 * 60 * 1000; // 20 min

app.get("/api/shop/auto-categories", shopCors, async (_request, response, next) => {
  try {
    if (_autoCatsCache && Date.now() - _autoCatsCacheAt < _AUTO_CATS_TTL) {
      return response.json(_autoCatsCache);
    }
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
    _autoCatsCache = result;
    _autoCatsCacheAt = Date.now();
    response.json(result);
  } catch (error) { next(error); }
});

const _newProductsCache = new Map(); // key `days:pageSize` → { data, at }
const _NEW_PRODUCTS_TTL = 10 * 60 * 1000; // 10 min

app.get("/api/shop/new", shopCors, async (request, response, next) => {
  try {
    const days = Math.max(1, Math.min(90, Number(request.query.days || 14) || 14));
    const pageSize = Math.max(1, Math.min(96, Number(request.query.pageSize || 24) || 24));
    const cacheKey = `${days}:${pageSize}`;
    const cached = _newProductsCache.get(cacheKey);
    if (cached && Date.now() - cached.at < _NEW_PRODUCTS_TTL) return response.json(cached.data);
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const result = await buildShopProductsFromDb({ q: "", brand: "", category: "", inStock: false, sort: "name", page: 1, pageSize, createdAfter: cutoff });
    const body = { ok: true, ...result, days };
    _newProductsCache.set(cacheKey, { data: body, at: Date.now() });
    response.json(body);
  } catch (error) { next(error); }
});

// ── Popular products (last 30 days by marketplace + site sales) ───────────────
const _popularCache = new Map(); // key `limit` → { data, at }
const _POPULAR_TTL = 5 * 60 * 1000; // 5 min

app.get("/api/shop/popular", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, products: [] });

    const limit = Math.max(4, Math.min(20, Number(request.query.limit || 8) || 8));
    const cached = _popularCache.get(limit);
    if (cached && Date.now() - cached.at < _POPULAR_TTL) return response.json(cached.data);
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
        links: { take: 5, select: { supplierArticle: true, priceCurrency: true, partnerId: true } },
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
      select: { article: true, price: true, currency: true, partnerId: true },
    }) : [];
    const pmMap2 = shopPmIndex(snaps);

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
        const currentPriceNum = Number(p.currentPrice || 0);
        const { priceRub } = shopPriceFromLinks(p.links, pmMap2, { currentPrice: currentPriceNum, defaultMarkup, rules: shopMarkupRules, usdRate });
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
          description: "", images, priceRub, oldPriceRub: shopMarketplaceOldPrice(priceRub, currentPriceNum),
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

    const result = { ok: true, products, source: "sales" };
    _popularCache.set(limit, { data: result, at: Date.now() });
    response.json(result);
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
        videoUrl: r.video_review?.url || r.video?.url || r.video_url || null,
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

// ─── Fragrance notes (public, shopCors) ──────────────────────────────────────
// Check DB first (LLM-generated or manual), then fall back to Fragrantica scraper.
app.get("/api/shop/product-notes", shopCors, async (request, response, next) => {
  try {
    const brand = cleanText(request.query.brand || "");
    const name = cleanText(request.query.name || "");
    const offerId = cleanText(request.query.offerId || "");
    if (!brand || !name) return response.json({ ok: true, data: null });

    // 1. Try DB (fast, always available)
    const prisma = getPrisma();
    if (prisma) {
      const where = offerId
        ? { offerId: { equals: offerId, mode: "insensitive" }, marketplace: "ozon", archived: false }
        : { brand: { equals: brand, mode: "insensitive" }, name: { contains: name.split(" ").slice(0, 3).join(" "), mode: "insensitive" }, archived: false };
      const wp = await prisma.warehouseProduct.findFirst({ where, select: { fragranceNotes: true } });
      if (wp?.fragranceNotes) {
        return response.json({ ok: true, data: wp.fragranceNotes, source: "db" });
      }
    }

    // 2. Try Fragrantica scraper (may be blocked by Cloudflare)
    const data = await lookupFragranticaData(brand, name);
    if (data) {
      // Persist to DB for future requests
      if (prisma) {
        // Only this product: the old brand-wide update stamped one perfume's notes on the whole brand.
        await prisma.warehouseProduct.updateMany({
          where: offerId
            ? { offerId: { equals: offerId, mode: "insensitive" }, archived: false }
            : { brand: { equals: brand, mode: "insensitive" }, name: { contains: name, mode: "insensitive" }, archived: false },
          data: { fragranceNotes: { ...data, source: "fragrantica" } },
        }).catch(() => {});
      }
    }

    response.json({ ok: true, data: data || null });
  } catch (error) { next(error); }
});

// ─── Product Q&A from Ozon ────────────────────────────────────────────────────
const _qaCache = new Map();
const _QA_TTL = 30 * 60 * 1000;

app.get("/api/shop/product-qa", shopCors, async (request, response, next) => {
  try {
    const offerId = cleanText(request.query.offerId || "");
    if (!offerId) return response.json({ ok: true, items: [] });

    const cacheKey = `qa:${offerId}`;
    const cached = _qaCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) return response.json({ ok: true, items: cached.items });

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
        const cachedAt = wpState.ozonQACachedAt ? new Date(wpState.ozonQACachedAt).getTime() : 0;
        if (Array.isArray(wpState.ozonQA) && wpState.ozonQA.length > 0 && cachedAt > Date.now() - _QA_TTL) {
          _qaCache.set(cacheKey, { items: wpState.ozonQA, expiresAt: cachedAt + _QA_TTL });
          return response.json({ ok: true, items: wpState.ozonQA });
        }
      }
    }

    const accounts = getOzonAccounts().filter((a) => a.clientId && a.apiKey);
    if (!accounts.length) {
      _qaCache.set(cacheKey, { items: [], expiresAt: Date.now() + _QA_TTL });
      return response.json({ ok: true, items: [] });
    }

    let rawQuestions = [];
    for (const account of accounts) {
      try {
        const data = await ozonRequest("/v1/question/list", {
          filter: {},
          ...(productId ? { sku: productId } : {}),
        }, account);
        const list = Array.isArray(data?.questions) ? data.questions
          : Array.isArray(data?.result?.questions) ? data.result.questions : [];
        const offerIdLower = offerId.toLowerCase();
        const matched = list.filter((q) => {
          const qOffer = cleanText(q.offer_id || "").toLowerCase();
          const qSku = String(q.sku || "");
          return qOffer === offerIdLower || (productId && (qSku === String(productId) || qSku === cleanText(q.sku)));
        });
        rawQuestions = matched.length ? matched : (productId ? list : []);
        break;
      } catch { /* try next */ }
    }

    const items = rawQuestions
      .filter((q) => q.text && (Number(q.answers_count || 0) > 0 || q.answer?.text || q.answers?.[0]?.text))
      .slice(0, 12)
      .map((q) => ({
        id: cleanText(String(q.id || q.question_id || Math.random())),
        question: cleanText(q.text || ""),
        answer: cleanText(q.answer?.text || q.answers?.[0]?.text || "Продавец ответил на этот вопрос"),
        createdAt: q.published_at || q.created_at || new Date().toISOString(),
      }))
      .filter((q) => q.question);

    _qaCache.set(cacheKey, { items, expiresAt: Date.now() + _QA_TTL });

    if (prisma && wpId) {
      prisma.warehouseProduct.update({
        where: { id: wpId },
        data: { marketplaceState: { ...wpState, ozonQA: items, ozonQACachedAt: new Date().toISOString() } },
      }).catch(() => {});
    }

    response.json({ ok: true, items });
  } catch (error) { next(error); }
});

// Sitemap-only endpoint: returns all published product slugs + dates without count limit
// ── Shop feed: every product the shop actually shows (same filter as the catalog),
// one entry per offerId (Ozon preferred), priced like the storefront. Feeds the
// sitemap (/api/shop/sitemap-products) and the Yandex YML feed (/api/shop/yml.xml).
const SHOP_FEED_TTL = 60 * 60 * 1000;
let _shopFeedCache = null;
let _shopFeedCacheAt = 0;
let _shopFeedBuilding = null;

const _SLUG_TRANSLIT = {
  "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y",
  "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f",
  "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
};
// must stay identical to shop-next/lib/slug.ts toProductSlug()
function shopProductSlug(name, offerId) {
  const nameSlug = String(name || "").toLowerCase().split("").map((c) => _SLUG_TRANSLIT[c] ?? c).join("")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").substring(0, 80).replace(/-+$/, "");
  return `${nameSlug}--${encodeURIComponent(offerId)}`;
}

function shopFeedDescription(p, name, category) {
  const ms = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
  const text = cleanText(ms.description || ms.desc || ms.productDescription || "");
  if (text.length >= 40) return text;
  // generated fallback: Yandex rejects offers without a description
  const vol = extractVolume(name);
  return [
    `${name}${p.brand ? ` от ${cleanText(p.brand)}` : ""} — ${category.label.toLowerCase()}${vol ? `, объём ${vol}` : ""}.`,
    "Оригинальная продукция с гарантией подлинности.",
    "Доставка по всей России через Ozon за 1–5 дней, оплата картой или СБП через Ozon Pay.",
  ].join(" ");
}

// Ozon listings that were "anonymised" after brand complaints: the name became just «парфюмерная вода»
// and the photo a blank bottle shared by dozens of products (ir.ozone.ru/…/7844840592.jpg on 86 items).
// The Yandex Market row of the same article keeps the real name and photos → the site uses it;
// with no usable row at all the product is left out of the site.
const SHOP_GENERIC_NAME_RE = /(парфюм\S*|туалетн\S*|вод[аы]|духи|одеколон|для|мужчин\S*|женщин\S*|унисекс|мужск\S*|женск\S*|набор\S*|пробник\S*|отливант\S*|тестер\S*|мини|eau|de|du|parfum|toilette|cologne|edp|edt|мл|ml|\d+(?:[.,]\d+)?)/gi;
function shopNameMasked(name) {
  const n = cleanText(name || "");
  const rest = n.replace(SHOP_GENERIC_NAME_RE, " ").replace(/[^a-zа-яё]+/gi, " ").trim();
  if (rest.length < 3) return true;
  // «помада», «жидкое мыло», «свеча ароматическая»: a bare lower-case Russian category, no brand/model/volume
  return /^[а-яё]/.test(n) && !/[a-z\d]/i.test(n) && n.split(/\s+/).length <= 3;
}
let _shopPlaceholderImgs = new Set(); // refreshed on every feed build
const shopFirstImage = (p) => extractImages(p)[0] || "";

async function buildShopFeedProducts() {
  const prisma = getPrisma();
  if (!prisma) return [];
  const shopSettings = await readShopSettings();
  const defaultMarkup = shopSettings.markup || 2.2;
  const markupRules = shopSettings.markupRules || [];
  let usdRate = Number(process.env.DEFAULT_USD_RATE || 95);
  try { const r = await getUsdRate(); usdRate = Number(r?.rate || r || 95); } catch (_) {}
  if (!usdRate || usdRate < 1) usdRate = Number(process.env.DEFAULT_USD_RATE || 95);

  const rows = await prisma.warehouseProduct.findMany({
    where: { archived: false, marketplace: { in: ["ozon", "yandex"] }, NOT: { status: "deleted" }, currentPrice: { gt: 0 }, links: { some: {} } },
    select: {
      id: true, offerId: true, name: true, brand: true, marketplace: true, images: true, currentPrice: true,
      targetStock: true, status: true, marketplaceState: true, updatedAt: true,
      links: { take: 5, select: { supplierArticle: true, priceCurrency: true, partnerId: true } },
    },
    orderBy: [{ marketplace: "asc" }, { updatedAt: "desc" }], // ozon < yandex → Ozon copy wins the dedupe
  });

  // one row per article: Ozon first, unless its name was anonymised — then the Yandex row
  const byKey = new Map();
  for (const p of rows) {
    const key = cleanText(p.offerId).toLowerCase();
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(p);
  }
  const unique = [];
  const altRows = new Map(); // key → the other rows, for the photo check below
  for (const [key, list] of byKey) {
    const good = list.filter((r) => !shopNameMasked(r.name));
    if (!good.length) continue; // only anonymised rows → not on the site
    unique.push(good[0]);
    altRows.set(key, good.slice(1));
  }

  // images column empty → the raw marketplace payload has them (fetched only for those rows)
  const needRaw = unique.filter((p) => !extractImages(p).length).map((p) => p.id);
  const rawById = new Map();
  for (let i = 0; i < needRaw.length; i += 1000) {
    const chunk = await prisma.warehouseProduct.findMany({ where: { id: { in: needRaw.slice(i, i + 1000) } }, select: { id: true, raw: true } });
    for (const r of chunk) rawById.set(r.id, r.raw);
  }

  // placeholder photos = the photos of the anonymised rows (a shared photo alone is normal: sample vials,
  // a brand line shot — a "≥ 5 articles" rule hid 314 good products)
  const maskedIds = rows.filter((r) => shopNameMasked(r.name)).map((r) => r.id);
  // …and only a photo shared by ≥ 3 anonymised articles: some anonymised rows kept their real photo
  const maskedUse = new Map();
  for (let i = 0; i < maskedIds.length; i += 1000) {
    for (const r of await prisma.warehouseProduct.findMany({ where: { id: { in: maskedIds.slice(i, i + 1000) } }, select: { offerId: true, images: true, raw: true } })) {
      const img = shopFirstImage(r);
      if (!img) continue;
      if (!maskedUse.has(img)) maskedUse.set(img, new Set());
      maskedUse.get(img).add(cleanText(r.offerId).toLowerCase());
    }
  }
  _shopPlaceholderImgs = new Set([...maskedUse].filter(([, ids]) => ids.size >= 3).map(([u]) => u));
  if (_shopPlaceholderImgs.size) {
    const altIds = [];
    for (const p of unique) {
      const img = shopFirstImage(rawById.has(p.id) ? { ...p, raw: rawById.get(p.id) } : p);
      if (_shopPlaceholderImgs.has(img)) altIds.push(...(altRows.get(cleanText(p.offerId).toLowerCase()) || []).map((r) => r.id));
    }
    let swapped = 0, dropped = 0;
    const altRaw = new Map();
    for (let i = 0; i < altIds.length; i += 1000) {
      for (const r of await prisma.warehouseProduct.findMany({ where: { id: { in: altIds.slice(i, i + 1000) } }, select: { id: true, raw: true } })) altRaw.set(r.id, r.raw);
    }
    for (let i = unique.length - 1; i >= 0; i--) {
      const p = unique[i];
      const img = shopFirstImage(rawById.has(p.id) ? { ...p, raw: rawById.get(p.id) } : p);
      if (!_shopPlaceholderImgs.has(img)) continue;
      const alt = (altRows.get(cleanText(p.offerId).toLowerCase()) || []).find((r) => {
        const im = shopFirstImage(altRaw.has(r.id) ? { ...r, raw: altRaw.get(r.id) } : r);
        return im && !_shopPlaceholderImgs.has(im);
      });
      if (alt) { if (altRaw.has(alt.id)) rawById.set(alt.id, altRaw.get(alt.id)); unique[i] = alt; swapped++; }
      else { unique.splice(i, 1); dropped++; }
    }
    logger.info("shop feed placeholder photos", { urls: [..._shopPlaceholderImgs].slice(0, 20), swapped, dropped });
  }

  // descriptions / vendors live mostly on the Yandex Market row of the same offerId
  const textByOffer = new Map();
  const offerIds = unique.map((p) => cleanText(p.offerId)).filter(Boolean);
  for (let i = 0; i < offerIds.length; i += 5000) {
    const chunk = offerIds.slice(i, i + 5000);
    const rows = await prisma.$queryRaw`
      SELECT offer_id,
             max(raw#>>'{ozon,description}') AS ozon_desc,
             max(raw#>>'{yandex,description}') AS ym_desc,
             max(raw#>>'{yandex,vendor}') AS ym_vendor
      FROM warehouse_products
      WHERE offer_id = ANY(${chunk}) AND archived = false
      GROUP BY offer_id`;
    for (const r of rows) {
      const desc = [r.ozon_desc, r.ym_desc].map((x) => shopCleanDescription(x || "")).find((x) => x.length >= 40) || "";
      textByOffer.set(cleanText(r.offer_id), { desc, vendor: cleanText(r.ym_vendor || "") });
    }
  }

  const articles = [...new Set(unique.flatMap((p) => p.links.map((l) => cleanText(l.supplierArticle))).filter(Boolean))];
  const allSnaps = [];
  for (let i = 0; i < articles.length; i += 5000) {
    allSnaps.push(...await prisma.priceMasterSnapshotItem.findMany({
      where: { article: { in: articles.slice(i, i + 5000) }, active: true },
      select: { article: true, price: true, currency: true, partnerId: true },
    }));
  }
  const pmMap = shopPmIndex(allSnaps);

  // Brand: the column is empty for most rows or holds "brand + model". Take the shortest known
  // brand (seen on >= 3 rows) that prefixes the name or the stored brand.
  const brandCount = new Map();
  for (const p of unique) {
    const b = cleanText(p.brand || "");
    if (b) brandCount.set(b.toUpperCase(), { name: b, n: (brandCount.get(b.toUpperCase())?.n || 0) + 1 });
  }
  const curated = [
    ...SHOP_LUXURY_BRANDS, ...SHOP_NICHE_BRANDS, ...SHOP_ARABIC_BRANDS,
    "Guess", "Michael Kors", "Pepe Jeans", "Lacoste", "Calvin Klein", "Davidoff", "Hugo Boss", "Antonio Banderas",
    "Jimmy Choo", "Tommy Hilfiger", "Nina Ricci", "Issey Miyake", "Azzaro", "Cacharel", "Lanvin", "Trussardi", "Zara",
    "Clinique", "Estee Lauder", "Shiseido", "Police", "Mexx", "Adidas", "Ariana Grande", "Britney Spears", "Juicy Couture",
    "Elizabeth Arden", "Lalique", "Rochas", "Diesel", "Dsquared2", "Emporio Armani", "Laura Biagiotti", "Roberto Cavalli",
    "Coach", "Dolce & Gabbana", "Dolce&Gabbana", "Sisley", "Clarins", "Lancome", "Byredo", "Maison Margiela", "Juliette Has A Gun",
    "Zadig & Voltaire", "Kenzo", "Givenchy", "Chloe", "Escada", "Moschino", "Versace", "Valentino", "Hermes", "Guerlain",
    "Jo Malone", "Frederic Malle", "Initio", "Nishane", "Memo", "Xerjoff", "Amouage", "Creed", "Kilian", "Mancera", "Montale",
    "Ex Nihilo", "Tiziana Terenzi", "Orto Parisi", "Nasomatto", "Atelier Cologne", "Acqua di Parma", "Aesop", "Diptyque", "Le Labo",
  ];
  for (const b of curated) if (!brandCount.has(b.toUpperCase())) brandCount.set(b.toUpperCase(), { name: b, n: 99 });
  const knownBrands = [...brandCount.entries()].filter(([, v]) => v.n >= 3).map(([up, v]) => ({ up, name: v.name }))
    .sort((a, b) => a.up.length - b.up.length);
  const resolveBrand = (name, stored) => {
    const N = name.toUpperCase(), S = stored.toUpperCase();
    const hit = knownBrands.find((b) => N.startsWith(b.up + " ") || N.startsWith(b.up + "-") || (S && (S === b.up || S.startsWith(b.up + " "))));
    if (hit) return hit.name;
    if (stored) return stored;
    return name.match(/^([^-–—(]{2,40}?)\s+[-–—]\s+/)?.[1]?.trim() || "";
  };

  const out = [];
  for (const p of unique) {
    const currentPriceNum = Number(p.currentPrice || 0);
    const { priceRub, fromSupplier } = shopPriceFromLinks(p.links, pmMap, { currentPrice: currentPriceNum, defaultMarkup, rules: markupRules, usdRate });
    // no supplier price + a huge marketplace price = a "do not sell" placeholder, keep it out of feeds
    if (!fromSupplier && priceRub > 150000) continue;
    const name = normalizeProductName(cleanText(p.name || ""));
    if (!(priceRub > 0) || name.length < 2) continue; // same rule as the catalog
    // feed: only hashes already known (product pages fill the cache) — no 15k downloads here
    const images = (await stripMarketplaceOnlyImages(extractImages(rawById.has(p.id) ? { ...p, raw: rawById.get(p.id) } : p), { fetchMissing: false }))
      .filter((u) => /^https:\/\//.test(u) && !_shopPlaceholderImgs.has(u)).slice(0, 10);
    const category = extractProductCategory(name);
    const stock = p.targetStock ?? 0;
    out.push({
      offerId: cleanText(p.offerId),
      slug: shopProductSlug(name, cleanText(p.offerId)),
      name,
      brand: resolveBrand(name, cleanText(p.brand || "") || textByOffer.get(cleanText(p.offerId))?.vendor || ""),
      priceRub,
      images,
      category: category.slug,
      categoryLabel: category.label,
      volume: extractVolume(name) || "",
      inStock: stock > 0 || (p.status !== "archived" && currentPriceNum > 0),
      description: textByOffer.get(cleanText(p.offerId))?.desc || shopFeedDescription(p, name, category),
      lastmod: p.updatedAt ? p.updatedAt.toISOString().slice(0, 10) : null,
      id: p.id,
      oldPriceRub: shopMarketplaceOldPrice(priceRub, currentPriceNum),
      stockQty: Math.max(0, stock),
      rating: Math.round((Number(p.marketplaceState?.ozonRating) || 0) * 10) / 10,
      reviewCount: Number(p.marketplaceState?.ozonReviewCount) || 0,
    });
  }
  // "brand + model" left in the brand column («Chanel Allure», «CHANEL BLEU DE») → the base brand that
  // prefixes it («Chanel»), so the brand list, the brand page and the catalog filter agree
  const brandN = new Map();
  for (const o of out) { const k = o.brand.toUpperCase(); if (k) brandN.set(k, (brandN.get(k) || 0) + 1); }
  const bases = [...brandN.entries()].filter(([, n]) => n >= 3).map(([k]) => k).sort((a, b) => a.length - b.length);
  const baseName = new Map();
  for (const o of out) { const k = o.brand.toUpperCase(); if (!baseName.has(k)) baseName.set(k, o.brand); }
  for (const o of out) {
    const k = o.brand.toUpperCase();
    const base = k && bases.find((b) => b !== k && k.startsWith(b + " "));
    if (base) o.brand = baseName.get(base) || o.brand;
  }
  // one name per house (after folding: «Christian Dior Sauvage» → «Christian Dior» → «Dior»)
  const ALIASES = {
    "CHRISTIAN DIOR": "Dior", "C.DIOR": "Dior", "YSL": "Yves Saint Laurent", "YVES SAINT-LAURENT": "Yves Saint Laurent",
    "D&G": "Dolce & Gabbana", "DOLCE&GABBANA": "Dolce & Gabbana", "DOLCE AND GABBANA": "Dolce & Gabbana",
    "LANCOME": "Lancôme", "GIORGIO ARMANI": "Giorgio Armani", "ARMANI": "Giorgio Armani", "EMPORIO ARMANI": "Emporio Armani",
    "HERMES": "Hermès", "CHLOE": "Chloé", "MAISON FRANCIS KURKDJIAN": "Maison Francis Kurkdjian", "MFK": "Maison Francis Kurkdjian",
    "FRANCIS KURKDJIAN": "Maison Francis Kurkdjian", "PARFUMS DE MARLY": "Parfums de Marly", "BY KILIAN": "Kilian", "KILIAN PARIS": "Kilian",
    "ESTEE LAUDER": "Estée Lauder", "JO MALONE LONDON": "Jo Malone", "CAROLINA HERRERA": "Carolina Herrera", "CK": "Calvin Klein",
  };
  // lines / typos that start with the house name
  const PREFIX_ALIASES = [["CHRISTIAN DIOR", "Dior"], ["DOLCE", "Dolce & Gabbana"], ["HERMESSENCE", "Hermès"], ["HERMES ", "Hermès"],
    ["YSL", "Yves Saint Laurent"], ["LANCME", "Lancôme"], ["LANCOME ", "Lancôme"], ["MONT BLANC", "Montblanc"], ["MONTBLANC ", "Montblanc"]];
  for (const o of out) {
    const k = o.brand.toUpperCase();
    const a = ALIASES[k] || PREFIX_ALIASES.find(([p]) => k === p.trim() || k.startsWith(p))?.[1];
    if (a) o.brand = a;
  }
  return out;
}

async function getShopFeedProducts() {
  if (_shopFeedCache && Date.now() - _shopFeedCacheAt < SHOP_FEED_TTL) return _shopFeedCache;
  if (!_shopFeedBuilding) {
    _shopFeedBuilding = buildShopFeedProducts()
      .then((list) => { _shopFeedCache = list; _shopFeedCacheAt = Date.now(); return list; })
      .finally(() => { _shopFeedBuilding = null; });
  }
  // serve the stale copy while a rebuild runs
  return _shopFeedCache || _shopFeedBuilding;
}

function xmlEsc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;")
    // control characters are illegal in XML 1.0
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
}

// Yandex YML categories: one root + the storefront categories
const SHOP_YML_CATEGORIES = [
  { id: 1, slug: "parfumery", name: "Парфюмерия" },
  ...SHOP_CATEGORIES.map((c, i) => ({ id: 10 + i, slug: c.slug, name: c.label, parentId: ["deo", "home", "body", "sets"].includes(c.slug) ? null : 1 })),
];

function shopYmlOfferId(offerId) {
  // YML offer id: latin letters and digits only, up to 20 chars
  const clean = offerId.replace(/[^A-Za-z0-9]/g, "");
  return clean.length && clean.length <= 20 ? clean : require("crypto").createHash("md5").update(offerId).digest("hex").slice(0, 20);
}

async function buildShopYml() {
  const site = process.env.SHOP_SITE_URL || "https://magicvibes.ru";
  const products = await getShopFeedProducts();
  const catId = new Map(SHOP_YML_CATEGORIES.map((c) => [c.slug, c.id]));
  const used = new Set();
  const offers = [];
  for (const p of products) {
    if (!p.images.length) continue; // Yandex requires a picture
    let id = shopYmlOfferId(p.offerId);
    while (used.has(id)) id = id.slice(0, 18) + Math.floor(Math.random() * 90 + 10);
    used.add(id);
    const params = [];
    if (p.volume) params.push(`<param name="Объём">${xmlEsc(p.volume)}</param>`);
    if (p.categoryLabel) params.push(`<param name="Тип">${xmlEsc(p.categoryLabel)}</param>`);
    offers.push(`<offer id="${id}" available="${p.inStock ? "true" : "false"}">
<url>${xmlEsc(`${site}/product/${p.slug}`)}</url>
<price>${p.priceRub}</price>
<currencyId>RUB</currencyId>
<categoryId>${catId.get(p.category) || 1}</categoryId>
${p.images.slice(0, 5).map((u) => `<picture>${xmlEsc(u)}</picture>`).join("\n")}
<name>${xmlEsc(p.name)}</name>
${p.brand ? `<vendor>${xmlEsc(p.brand)}</vendor>` : ""}
<vendorCode>${xmlEsc(p.offerId)}</vendorCode>
<description><![CDATA[${String(p.description).replace(/]]>/g, "]] >").slice(0, 3000)}]]></description>
<sales_notes>Оплата картой или СБП через Ozon Pay</sales_notes>
<delivery>true</delivery>
<pickup>true</pickup>
<manufacturer_warranty>true</manufacturer_warranty>
${params.join("\n")}
</offer>`.replace(/\n{2,}/g, "\n"));
  }
  const date = new Date().toISOString().slice(0, 16).replace("T", " ");
  return `<?xml version="1.0" encoding="UTF-8"?>
<yml_catalog date="${date}">
<shop>
<name>Magic Vibes</name>
<company>Magic Vibes</company>
<url>${site}</url>
<platform>Next.js</platform>
<email>noreply@magicvibes.ru</email>
<currencies><currency id="RUB" rate="1"/></currencies>
<categories>
${SHOP_YML_CATEGORIES.map((c) => `<category id="${c.id}"${c.parentId ? ` parentId="${c.parentId}"` : ""}>${xmlEsc(c.name)}</category>`).join("\n")}
</categories>
<delivery-options><option cost="0" days="1-5"/></delivery-options>
<pickup-options><option cost="0" days="1-5"/></pickup-options>
<offers>
${offers.join("\n")}
</offers>
</shop>
</yml_catalog>
`;
}

let _shopYmlCache = null;
let _shopYmlCacheAt = 0;

app.get("/api/shop/yml.xml", shopCors, async (_request, response, next) => {
  try {
    if (!_shopYmlCache || Date.now() - _shopYmlCacheAt > SHOP_FEED_TTL) {
      _shopYmlCache = await buildShopYml();
      _shopYmlCacheAt = Date.now();
    }
    response.set("Content-Type", "application/xml; charset=utf-8");
    response.set("Cache-Control", "public, max-age=1800");
    response.send(_shopYmlCache);
  } catch (error) { next(error); }
});

app.get("/api/shop/sitemap-products", shopCors, async (_request, response, next) => {
  try {
    const products = await getShopFeedProducts();
    response.json({
      products: products.map((p) => ({ offerId: p.offerId, slug: p.slug, name: p.name, image: p.images[0] || null, lastmod: p.lastmod })),
    });
  } catch (error) { next(error); }
});

// "Другие объёмы": same brand + name once the volume is stripped (EDP/EDT stay distinct — different scents)
const _shopVariantIndex = new WeakMap();
function shopVariantKey(p) {
  const name = String(p.name || "").toLowerCase()
    .replace(/\(?\s*\d+(?:[.,]\d+)?\s*(?:мл|ml|г|g|oz)(?![a-zа-яё])\s*\)?/gi, " ")
    .replace(/[^a-zа-яё0-9]+/gi, " ").replace(/\s+/g, " ").trim();
  return `${String(p.brand || "").toLowerCase()}|${name}`;
}
app.get("/api/shop/variants/:offerId", shopCors, async (request, response, next) => {
  try {
    const products = await getShopFeedProducts();
    let index = _shopVariantIndex.get(products);
    if (!index) {
      index = new Map();
      for (const p of products) {
        if (!p.volume) continue;
        const k = shopVariantKey(p);
        if (!index.has(k)) index.set(k, []);
        index.get(k).push(p);
      }
      _shopVariantIndex.set(products, index);
    }
    const offerId = cleanText(request.params.offerId || "").toLowerCase();
    const self = products.find((p) => p.offerId.toLowerCase() === offerId);
    const group = self?.volume ? index.get(shopVariantKey(self)) || [] : [];
    // one entry per volume — the cheapest in-stock offer wins
    const byVol = new Map();
    for (const p of group) {
      const v = p.volume.replace(",", ".").replace(/\s+/g, " ").toLowerCase();
      const cur = byVol.get(v);
      if (!cur || (p.inStock && !cur.inStock) || (p.inStock === cur.inStock && p.priceRub < cur.priceRub)) byVol.set(v, p);
    }
    if (self) byVol.set(self.volume.replace(",", ".").replace(/\s+/g, " ").toLowerCase(), self);
    const variants = [...byVol.values()]
      .sort((a, b) => parseFloat(a.volume.replace(",", ".")) - parseFloat(b.volume.replace(",", ".")))
      .map((p) => ({ offerId: p.offerId, slug: p.slug, volume: p.volume, priceRub: p.priceRub, inStock: p.inStock, current: p === self }));
    response.set("Cache-Control", "public, max-age=600");
    response.json({ variants: variants.length > 1 ? variants : [] });
  } catch (error) { next(error); }
});

app.get("/api/shop/brands", shopCors, async (_request, response, next) => {
  try {
    // Same brand resolution as the catalog (feed): the old list came from the raw brand column
    // («12 Parfumeurs Francais»), the catalog filters by the resolved brand («12 Parfumeurs») → 0 items.
    const feed = await getShopFeedProducts();
    const groups = new Map();
    for (const p of feed) {
      const b = cleanText(p.brand || "");
      if (!b) continue;
      const up = b.toUpperCase();
      const g = groups.get(up) || { count: 0, inStock: 0, minPrice: 0, spellings: new Map(), image: null };
      g.count += 1;
      if (p.inStock) g.inStock += 1;
      if (p.priceRub > 0 && (!g.minPrice || p.priceRub < g.minPrice)) g.minPrice = p.priceRub;
      if (!g.image && p.images?.[0]) g.image = p.images[0];
      g.spellings.set(b, (g.spellings.get(b) || 0) + 1);
      groups.set(up, g);
    }
    const brands = [...groups.values()]
      .map((g) => ({ name: [...g.spellings.entries()].sort((a, b) => b[1] - a[1])[0][0], count: g.count, inStock: g.inStock, minPrice: g.minPrice, image: g.image }))
      .sort((a, b) => a.name.localeCompare(b.name, "ru"));
    response.set("Cache-Control", "public, max-age=600");
    response.json(brands);
  } catch (error) { next(error); }
});

app.get("/api/shop/settings", shopCors, async (_request, response, next) => {
  try {
    const settings = await readShopSettings();
    // Don't expose sensitive fields
    const { shopName, shopDescription, contactEmail, contactPhone, deliveryDays, deliveryDaysMin, deliveryPriceRub, freeDeliveryFrom } = settings;
    // switchable storefront sections; off unless enabled in the admin
    const features = { giftBuilder: Boolean(settings.features?.giftBuilder), vipClub: Boolean(settings.features?.vipClub) };
    const deliveryMode = settings.deliveryRules?.mode === "ozon" ? "ozon" : "fixed";
    response.json({ shopName, shopDescription, contactEmail, contactPhone, deliveryDays, deliveryDaysMin, deliveryPriceRub, freeDeliveryFrom, features, deliveryMode });
  } catch (error) {
    next(error);
  }
});

// ── Аромат месяца ─────────────────────────────────────────────────────────
let _aromaMesyatsaCache = null;
let _aromaMesyatsaCacheAt = 0;
const _AROMA_MESYATSA_TTL = 5 * 60 * 1000; // 5 min

app.get("/api/shop/aroma-mesyatsa", shopCors, async (_request, response, next) => {
  try {
    if (_aromaMesyatsaCache && Date.now() - _aromaMesyatsaCacheAt < _AROMA_MESYATSA_TTL) {
      return response.json(_aromaMesyatsaCache);
    }
    const settings = await readShopSettings();
    const am = settings.aromaMesyatsa;
    if (!am || !am.offerId) {
      _aromaMesyatsaCache = { ok: true, product: null };
      _aromaMesyatsaCacheAt = Date.now();
      return response.json(_aromaMesyatsaCache);
    }
    const product = await findShopProductByOfferId(am.offerId);
    _aromaMesyatsaCache = { ok: true, product: product || null, note: cleanText(am.note || ""), validUntil: am.validUntil || null };
    _aromaMesyatsaCacheAt = Date.now();
    response.json(_aromaMesyatsaCache);
  } catch (error) { next(error); }
});

// ── Топ по городам ────────────────────────────────────────────────────────
let _cityTopsCache = null;
let _cityTopsCacheAt = 0;
const _CITY_TOPS_TTL = 4 * 60 * 60 * 1000; // 4 h

app.get("/api/shop/city-tops", shopCors, async (_request, response, next) => {
  try {
    if (_cityTopsCache && Date.now() - _cityTopsCacheAt < _CITY_TOPS_TTL) {
      return response.json(_cityTopsCache);
    }
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, entries: [] });

    const since = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000); // 60 days
    const orders = await prisma.shopOrder.findMany({
      where: { createdAt: { gte: since }, status: { in: ["delivered", "shipped", "completed", "paid", "confirmed"] } },
      select: { delivery: true, items: true },
    });

    // Aggregate: city → { productKey → { name, brand, count } }
    const cityMap = new Map();
    for (const order of orders) {
      const delivery = typeof order.delivery === "object" ? order.delivery : {};
      const city = cleanText((delivery.city || "")).split(" ")[0];
      if (!city || city.length < 2) continue;
      const items = Array.isArray(order.items) ? order.items : [];
      for (const item of items.slice(0, 3)) {
        const offerId = item.offerId || "";
        const name = cleanText(item.name || offerId).slice(0, 60);
        const brand = cleanText(item.brand || "").slice(0, 30);
        if (!offerId) continue;
        if (!cityMap.has(city)) cityMap.set(city, new Map());
        const products = cityMap.get(city);
        if (!products.has(offerId)) products.set(offerId, { name, brand, offerId, count: 0 });
        products.get(offerId).count++;
      }
    }

    // For each city pick the top product
    const entries = [];
    for (const [city, products] of cityMap.entries()) {
      if (products.size === 0) continue;
      const top = [...products.values()].sort((a, b) => b.count - a.count)[0];
      entries.push({ city, name: top.name, brand: top.brand, offerId: top.offerId });
    }

    // Sort by city popularity (descending), take top 8
    const CITY_ORDER = ["Москва","Санкт-Петербург","Краснодар","Екатеринбург","Казань","Новосибирск","Ростов-на-Дону","Нижний Новгород","Воронеж","Сочи"];
    entries.sort((a, b) => {
      const ia = CITY_ORDER.indexOf(a.city);
      const ib = CITY_ORDER.indexOf(b.city);
      if (ia === -1 && ib === -1) return 0;
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    });

    const result = { ok: true, entries: entries.slice(0, 8) };
    _cityTopsCache = result;
    _cityTopsCacheAt = Date.now();
    response.json(result);
  } catch (error) { next(error); }
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

  const headers = { "Client-Id": account.clientId, "Api-Key": account.apiKey, "Content-Type": "application/json" };

  // Step 1 — try the unfiltered list (works when seller has delivery methods configured)
  try {
    const res = await fetch("https://api-seller.ozon.ru/v1/delivery/point/list", {
      method: "POST", headers, body: "{}", signal: AbortSignal.timeout(60000),
    });
    if (res.ok) {
      const data = await res.json();
      const points = data.points || data.result || [];
      if (points.length > 0) {
        _ozonPvzAllCache = points.map(p => ({ id: p.map_point_id, lat: p.coordinate?.lat ?? 0, lng: p.coordinate?.long ?? 0 }));
        _ozonPvzAllCacheAt = Date.now();
        logger.info("ozon pvz list loaded (unfiltered)", { count: _ozonPvzAllCache.length });
        return _ozonPvzAllCache;
      }
      logger.warn("ozon pvz list empty — trying delivery-method fallback", { keys: Object.keys(data) });
    } else {
      logger.warn("ozon pvz list failed", { status: res.status });
    }
  } catch (err) {
    logger.warn("ozon pvz list error", { detail: err?.message });
  }

  // Step 2 — fetch delivery methods first, then load PVZ per method
  try {
    const dmRes = await fetch("https://api-seller.ozon.ru/v1/delivery-method/list", {
      method: "POST",
      headers,
      body: JSON.stringify({ filter: { is_enabled: true, warehouse_id: 0 }, limit: 50, offset: 0 }),
      signal: AbortSignal.timeout(15000),
    });
    if (!dmRes.ok) {
      logger.warn("ozon delivery-method list failed", { status: dmRes.status });
      return _ozonPvzAllCache || [];
    }
    const dmData = await dmRes.json();
    const methods = dmData.result || dmData.delivery_methods || [];
    logger.info("ozon delivery methods fetched", { count: methods.length });

    const allPoints = [];
    for (const method of methods.slice(0, 5)) {
      try {
        const pvzRes = await fetch("https://api-seller.ozon.ru/v1/delivery/point/list", {
          method: "POST",
          headers,
          body: JSON.stringify({ delivery_method_id: method.id }),
          signal: AbortSignal.timeout(60000),
        });
        if (!pvzRes.ok) continue;
        const pvzData = await pvzRes.json();
        const pts = pvzData.points || pvzData.result || [];
        logger.info("ozon pvz per method", { methodId: method.id, count: pts.length });
        allPoints.push(...pts);
      } catch (e) {
        logger.warn("ozon pvz per-method error", { methodId: method.id, detail: e?.message });
      }
    }

    const seen = new Set();
    _ozonPvzAllCache = allPoints
      .filter(p => p.map_point_id && !seen.has(p.map_point_id) && seen.add(p.map_point_id))
      .map(p => ({ id: p.map_point_id, lat: p.coordinate?.lat ?? 0, lng: p.coordinate?.long ?? 0 }));
    _ozonPvzAllCacheAt = Date.now();
    logger.info("ozon pvz list loaded (per-method)", { count: _ozonPvzAllCache.length });
    return _ozonPvzAllCache;
  } catch (err) {
    logger.warn("ozon pvz delivery-method fallback error", { detail: err?.message });
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
      if (!nearby.length) return response.json({ pvz: [], city, source: "ozon", center: { lat: cLat, lng: cLng }, _allLoaded: allPoints.length });
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
      if (!inBox.length) return response.json({ pvz: [], city, error: "no_pvz_in_city", _allLoaded: allPoints.length });
      // Равномерная сетка — 200 точек по всему городу, список — от центра к окраинам
      // (bbox Москвы включает Новую Москву до Калужской обл. — без сортировки список начинался с Жуковского)
      // Nominatim даёт центр Москвы по площади вместе с Новой Москвой (≈ Чертаново) — берём Кремль
      const isMoscow = /^москва$/i.test(city.replace(/^г\.?\s*/i, "")) || /^Москва,/.test(String(g.display_name || ""));
      const cLat = isMoscow ? 55.7520 : parseFloat(g.lat), cLng = isMoscow ? 37.6175 : parseFloat(g.lon);
      const cosC = Math.cos((cLat * Math.PI) / 180);
      const distC = (p) => Math.pow(p.lat - cLat, 2) + Math.pow((p.lng - cLng) * cosC, 2);
      if (Number.isFinite(cLat) && Number.isFinite(cLng)) {
        // 120 ближайших к центру (сетка там слишком редкая) + 80 по сетке для охвата окраин
        const core = [...inBox].sort((x, y) => distC(x) - distC(y)).slice(0, 120);
        const coreIds = new Set(core.map((p) => p.id));
        const spread = _gridSamplePvz(inBox.filter((p) => !coreIds.has(p.id)), bboxS, bboxN, bboxW, bboxE, 80);
        nearby = [...core, ...spread].sort((x, y) => distC(x) - distC(y));
      } else {
        nearby = _gridSamplePvz(inBox, bboxS, bboxN, bboxW, bboxE, 200);
      }
    }

    const order = new Map(nearby.map((p, i) => [String(p.id), i]));
    const pvz = (await _fetchOzonPvzDetails(nearby.map(p => p.id)))
      .sort((x, y) => (order.get(String(x.id)) ?? 1e9) - (order.get(String(y.id)) ?? 1e9));
    const extra = pvz.length === 0 ? { _debug: { allPoints: allPoints.length, nearbyRaw: nearby.length } } : {};
    response.json({ pvz, city, count: pvz.length, source: "ozon", ...extra });
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

// Cart → Acquiring items (MODE_FULL). The order discount is spread over the lines so that
// sum(price × qty) equals the charged amount exactly (Ozon rejects a mismatch).
function _ozonPayItems(items, totalRub) {
  const vat = process.env.OZON_PAY_VAT || "VAT_NONE"; // УСН → «без НДС»
  const baseKop = items.reduce((s, i) => s + Math.round(i.priceRub * 100) * i.quantity, 0);
  const totalKop = Math.round(totalRub * 100);
  const lines = items.map((i) => ({
    extId: i.offerId, // seller article — Ozon Delivery matches the product by it
    // Ozon rejects an empty name ("некорректное название товара")
    name: cleanText(`${i.brand && i.name && !i.name.toUpperCase().startsWith(i.brand.toUpperCase()) ? `${i.brand} ` : ""}${i.name || ""}`).slice(0, 128) || `Товар ${i.offerId}`,
    quantity: i.quantity,
    unitKop: baseKop > 0 ? Math.floor((Math.round(i.priceRub * 100) * totalKop) / baseKop) : 0,
  }));
  // rounding remainder: a qty-1 line absorbs it exactly, otherwise split it off as its own line
  const rest = totalKop - lines.reduce((s, l) => s + l.unitKop * l.quantity, 0);
  if (rest > 0) {
    const one = lines.find((l) => l.quantity === 1);
    if (one) one.unitKop += rest;
    else {
      const l = lines[0];
      l.quantity -= 1;
      lines.push({ ...l, quantity: 1, unitKop: l.unitKop + rest });
    }
  }
  return lines.filter((l) => l.quantity > 0).map((l) => ({
    extId: l.extId, name: l.name, quantity: l.quantity, type: "TYPE_PRODUCT", vat, needMark: false,
    price: { currencyCode: "643", value: String(l.unitKop) },
  }));
}

// Ozon Delivery via Acquiring: every article must be listed in our Ozon store
async function shopOzonDeliveryEligible(offerIds) {
  if (process.env.OZON_PAY_DELIVERY === "false" || !process.env.OZON_PAY_ACCESS_KEY) return false;
  const ids = [...new Set(offerIds.map((x) => cleanText(x || "")).filter(Boolean))];
  const prisma = getPrisma();
  if (!ids.length || !prisma) return false;
  const onOzon = await prisma.warehouseProduct.findMany({
    where: { marketplace: "ozon", archived: false, offerId: { in: ids } }, select: { offerId: true },
  }).catch(() => []);
  return new Set(onOzon.map((r) => r.offerId)).size === ids.length;
}

// ── Delivery price ─────────────────────────────────────────────────────────────
// settings.deliveryRules (davidsklad → Магазин → Настройки → «Доставка: правила»):
//   mode "fixed" — the old flat «Стоимость доставки» (deliveryPriceRub);
//   mode "ozon"  — cost-covering price from the Ozon Доставка tariffs (lib/ozon-logistics-tariffs.js):
//     обработка отправления + доставка до места выдачи + Σ товаров × (логистика по объёму и кластерам
//     + выдача в ПВЗ) + заложенные невыкупы (обратная логистика = тариф логистики + обработка возврата)
//     + комиссия Ozon Pay (с суммы доставки или всего заказа), затем наценка, округление вверх, мин/макс.
//   «Бесплатная доставка от» (freeDeliveryFrom) works in both modes.
const OZON_TARIFFS = require("./lib/ozon-logistics-tariffs");
const OZON_CITY_CLUSTERS = require("./lib/ozon-city-clusters");

const SHOP_DELIVERY_DEFAULTS = {
  mode: "fixed",
  senderCluster: "Москва, МО и Дальние регионы",
  handlingRub: 10,          // обработка отправления в ПВЗ/ППЗ
  lastMileRub: 25,          // доставка до места выдачи (не больше 25 ₽)
  issuePerItemRub: 30,      // выдача товара в партнёрском ПВЗ, за товар
  returnRatePct: 5,         // доля невыкупов и отмен
  returnProcessingRub: 15,  // обработка невыкупа, за товар
  acquiringPct: 2.2,        // Ozon Pay: 2,2% карта / Ozon Банк, 0,7% СБП — берём худший случай
  acquiringBase: "order",   // "order" — комиссия со всего заказа, "delivery" — только с доставки, "none"
  packMlFactor: 4,          // объём упаковки, л = мл флакона × factor / 1000 + packBaseL
  packBaseL: 0.15,
  defaultItemL: 0.6,        // товар без объёма в названии
  extraRub: 0,              // наценка к доставке, ₽
  extraPct: 0,              // наценка к доставке, %
  roundTo: 10,
  minRub: 0,
  maxRub: 0,                // 0 = без ограничения
  unknownCity: "max",       // город не распознан: "max" — самый дорогой кластер, "universal" — универсальный тариф
  cityClusters: [],         // [{ match: "Балашиха", cluster: "Москва, МО и Дальние регионы" }]
  cityRules: [],            // [{ match: "Москва" | cluster name, addRub?: number, fixedRub?: number }]
};

function shopDeliveryRules(settings) {
  const r = { ...SHOP_DELIVERY_DEFAULTS, ...(settings?.deliveryRules || {}) };
  for (const k of Object.keys(SHOP_DELIVERY_DEFAULTS)) {
    if (typeof SHOP_DELIVERY_DEFAULTS[k] === "number") r[k] = Math.max(0, Number(r[k]) || 0);
  }
  if (!Array.isArray(r.cityClusters)) r.cityClusters = [];
  if (!Array.isArray(r.cityRules)) r.cityRules = [];
  return r;
}

const _wordStart = (hay, pattern) => new RegExp(`(?:^|[^а-яёa-z0-9])(?:${pattern})`, "i").test(hay);
function shopCityCluster(city, rules) {
  const hay = cleanText(city || "").toLowerCase().replace(/ё/g, "е");
  if (!hay) return null;
  for (const c of rules.cityClusters) {
    const m = cleanText(c?.match || "").toLowerCase().replace(/ё/g, "е");
    if (m && hay.includes(m) && OZON_TARIFFS.clusters.includes(c.cluster)) return c.cluster;
  }
  for (const [pattern, cluster] of OZON_CITY_CLUSTERS) {
    if (_wordStart(hay, pattern.replace(/ё/g, "е"))) return cluster;
  }
  return null;
}

function shopItemLitres(item, rules) {
  const m = String(item.volume || item.name || "").match(/(\d+(?:[.,]\d+)?)\s*(мл|ml)(?![a-zа-яё])/i);
  const ml = m ? parseFloat(m[1].replace(",", ".")) : 0;
  return ml > 0 ? ml * rules.packMlFactor / 1000 + rules.packBaseL : rules.defaultItemL;
}

function _ozonBand(litres) {
  const i = OZON_TARIFFS.volumesL.findIndex((u) => u == null || litres <= u + 1e-9);
  return i < 0 ? OZON_TARIFFS.volumesL.length - 1 : i;
}

// logistics tariff for one item; destination null → per rules.unknownCity
function _ozonLogistics(litres, sender, dest, rules) {
  const band = _ozonBand(litres);
  const row = OZON_TARIFFS.matrix[sender] || OZON_TARIFFS.matrix[SHOP_DELIVERY_DEFAULTS.senderCluster] || {};
  if (dest && row[dest]?.[band] != null) return row[dest][band];
  if (!dest && rules.unknownCity === "max") {
    const all = Object.values(row).map((r) => r[band]).filter((x) => x != null);
    if (all.length) return Math.max(...all);
  }
  return OZON_TARIFFS.universal[band] ?? 0;
}

/**
 * items: [{ offerId, name, volume, quantity, priceRub }], goodsRub = goods after discounts.
 * Returns { priceRub, cluster, free, mode, breakdown }.
 */
function shopDeliveryQuote({ items = [], goodsRub = 0, city = "", settings }) {
  const rules = shopDeliveryRules(settings);
  const freeFrom = Math.max(0, Number(settings?.freeDeliveryFrom) || 0);
  const isFree = freeFrom > 0 && goodsRub >= freeFrom;
  if (rules.mode !== "ozon") {
    const price = Math.max(0, Math.round(Number(settings?.deliveryPriceRub) || 0));
    return { priceRub: isFree ? 0 : price, free: isFree, mode: "fixed", cluster: null, breakdown: null };
  }
  const cluster = shopCityCluster(city, rules);
  const units = items.reduce((n, i) => n + Math.max(1, Number(i.quantity) || 1), 0) || 1;
  let logistics = 0;
  for (const it of items.length ? items : [{ quantity: 1 }]) {
    logistics += _ozonLogistics(shopItemLitres(it, rules), rules.senderCluster, cluster, rules) * Math.max(1, Number(it.quantity) || 1);
  }
  const forward = rules.handlingRub + rules.lastMileRub + logistics + units * rules.issuePerItemRub;
  // a refused / cancelled parcel costs the way back (reverse logistics = the same tariff) + processing
  const returns = (rules.returnRatePct / 100) * (logistics + units * rules.returnProcessingRub + rules.lastMileRub);
  let cost = forward + returns;
  cost = cost * (1 + rules.extraPct / 100) + rules.extraRub;
  // Ozon Pay commission is taken from the whole payment: solve D so that D − a·(base) covers the cost
  const a = Math.min(0.2, rules.acquiringPct / 100);
  let price = rules.acquiringBase === "order" ? (cost + a * goodsRub) / (1 - a)
    : rules.acquiringBase === "delivery" ? cost / (1 - a) : cost;
  const step = rules.roundTo > 0 ? rules.roundTo : 1;
  price = Math.ceil(price / step) * step;
  if (rules.minRub > 0) price = Math.max(price, rules.minRub);
  if (rules.maxRub > 0) price = Math.min(price, rules.maxRub);
  // manual city / cluster rules: fixed price or a surcharge
  const hay = cleanText(city || "").toLowerCase().replace(/ё/g, "е");
  for (const cr of rules.cityRules) {
    const m = cleanText(cr?.match || "").toLowerCase().replace(/ё/g, "е");
    if (!m || !(hay.includes(m) || (cluster && cluster.toLowerCase() === m))) continue;
    if (Number(cr.fixedRub) >= 0 && cr.fixedRub !== "" && cr.fixedRub != null) price = Math.round(Number(cr.fixedRub));
    else if (Number(cr.addRub)) price += Math.round(Number(cr.addRub));
    break;
  }
  const r2 = (x) => Math.round(x * 100) / 100;
  return {
    priceRub: isFree ? 0 : Math.max(0, price),
    free: isFree,
    mode: "ozon",
    cluster,
    breakdown: { logistics: r2(logistics), handling: rules.handlingRub, lastMile: rules.lastMileRub, issue: r2(units * rules.issuePerItemRub), returns: r2(returns), acquiring: r2(price - cost), units, sender: rules.senderCluster, raw: Math.round(price) },
  };
}

// cheapest possible price for the cart (for «доставка от … ₽» before the city is known)
function shopDeliveryQuoteFrom({ items, goodsRub, settings }) {
  const rules = shopDeliveryRules(settings);
  if (rules.mode !== "ozon") return shopDeliveryQuote({ items, goodsRub, settings }).priceRub;
  let best = Infinity;
  for (const dest of OZON_TARIFFS.clusters) {
    if (!(OZON_TARIFFS.matrix[rules.senderCluster] || {})[dest]) continue;
    const q = shopDeliveryQuote({ items, goodsRub, settings: { ...settings, deliveryRules: { ...rules, cityClusters: [{ match: "__probe__", cluster: dest }] } }, city: "__probe__" });
    if (q.priceRub < best) best = q.priceRub;
  }
  return Number.isFinite(best) ? best : 0;
}

// checkout asks up front: with Ozon Delivery the address is picked on the Ozon Pay form (the API
// takes no address), so the site asks only for the city — it decides the Ozon cluster and the price.
// Body: { offerIds? , items?: [{ offerId, quantity }], city?, goodsRub? } (goodsRub = after the buyer's promo)
app.post("/api/shop/checkout/delivery-mode", shopCors, async (request, response, next) => {
  try {
    const body = request.body || {};
    const raw = Array.isArray(body.items) ? body.items : (Array.isArray(body.offerIds) ? body.offerIds.map((offerId) => ({ offerId, quantity: 1 })) : []);
    const items = [];
    for (const it of raw.slice(0, 50)) {
      const offerId = cleanText(it?.offerId || "");
      if (!offerId) continue;
      const p = await findShopProductByOfferId(offerId, { fast: true }).catch(() => null);
      items.push({ offerId, name: p?.name || "", volume: p?.volume || "", quantity: Math.min(10, Math.max(1, Number(it.quantity) || 1)), priceRub: Number(p?.priceRub) || 0 });
    }
    const settings = await readShopSettings();
    const goodsList = items.reduce((s2, i) => s2 + i.priceRub * i.quantity, 0);
    const goodsRub = Number(body.goodsRub) > 0 ? Math.min(goodsList || Infinity, Number(body.goodsRub)) : goodsList;
    const city = cleanText(body.city || "").slice(0, 120);
    const quote = shopDeliveryQuote({ items, goodsRub, city, settings });
    response.json({
      ozonDelivery: await shopOzonDeliveryEligible(items.map((i) => i.offerId)),
      deliveryRub: quote.priceRub,
      fromRub: city ? quote.priceRub : shopDeliveryQuoteFrom({ items, goodsRub, settings }),
      needCity: quote.mode === "ozon",
      cluster: quote.cluster,
      free: quote.free,
      // legacy fields (flat tariff) for old clients
      deliveryPriceRub: Math.max(0, Math.round(Number(settings.deliveryPriceRub) || 0)),
      freeDeliveryFrom: Math.max(0, Number(settings.freeDeliveryFrom) || 0),
    });
  } catch (error) { next(error); }
});

// admin preview of the rules: GET /api/shop/admin/delivery-preview?city=…&ml=100&qty=1&goods=5000
app.post("/api/shop/admin/delivery-preview", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const settings = { ...(await readShopSettings()), ...(body.settings || {}) };
    const items = (Array.isArray(body.items) && body.items.length ? body.items : [{ volume: "100 мл", quantity: 1 }])
      .slice(0, 20).map((i) => ({ volume: cleanText(i.volume || ""), quantity: Math.max(1, Number(i.quantity) || 1) }));
    const goodsRub = Math.max(0, Number(body.goodsRub) || 0);
    const cities = (Array.isArray(body.cities) && body.cities.length ? body.cities : ["Москва", "Санкт-Петербург", "Казань", "Екатеринбург", "Новосибирск", "Краснодар", "Владивосток", ""]).slice(0, 12);
    response.json({
      clusters: OZON_TARIFFS.clusters,
      senders: OZON_TARIFFS.senders,
      rows: cities.map((city) => ({ city: city || "город не указан", ...shopDeliveryQuote({ items, goodsRub, city: cleanText(city), settings }) })),
    });
  } catch (error) { next(error); }
});

// goodsRub = goods after discounts; deliveryRub = shop tariff for this order.
// Ozon Delivery: Ozon rejects extra lines, so the price goes to deliverySettings.fixedPrice and
// the order amount is the goods. Own delivery: «Доставка» is a separate service line in the receipt.
// Returns the Acquiring order ({ id, payLink, status, … }) or null. `extId` defaults to the shop order
// id; a repeat call with the same extId returns the order Ozon already has (used by «Оплатить» again).
async function _ozonPayCreateOrder({ orderId, extId = orderId, goodsRub, deliveryRub = 0, email, items = [], ozonDelivery = false }) {
  const accessKey = process.env.OZON_PAY_ACCESS_KEY;
  const secretKey = process.env.OZON_PAY_SECRET_KEY;
  if (!accessKey || !secretKey) return null;

  // only SMS/DMS exist; card / SBP / Ozon Card is chosen by the buyer on the Ozon Pay form
  const paymentAlgorithm = "PAY_ALGO_SMS";
  const currencyCode = "643";
  const vat = process.env.OZON_PAY_VAT || "VAT_NONE";
  const kop = (rub) => String(Math.round(rub * 100));

  const build = (withDelivery) => {
    const goods = _ozonPayItems(items, goodsRub);
    const lines = withDelivery || !(deliveryRub > 0) ? goods : [...goods, {
      extId: "delivery", name: "Доставка", quantity: 1, type: "TYPE_SERVICE", vat, needMark: false,
      price: { currencyCode, value: kop(deliveryRub) },
    }];
    const value = withDelivery ? kop(goodsRub) : kop(goodsRub + deliveryRub);
    const body = {
      accessKey,
      amount: { currencyCode, value },
      extId,
      paymentAlgorithm,
      mode: "MODE_FULL",
      items: lines,
      enableFiscalization: false,
      successUrl: `${_ozonPayShopBase()}/order-success?id=${encodeURIComponent(orderId)}`,
      failUrl: `${_ozonPayShopBase()}/order-failed?id=${encodeURIComponent(orderId)}`,
      notificationUrl: `${_ozonPayApiBase()}/api/shop/payment/notification`,
      requestSign: _ozonPayRequestSign(accessKey, "", extId, "", paymentAlgorithm, currencyCode, value, secretKey),
    };
    if (withDelivery) body.deliverySettings = { isEnabled: true, fixedPrice: { currencyCode, value: kop(deliveryRub) } };
    if (email) body.receiptEmail = cleanText(email);
    return body;
  };

  const post = (body) => fetch(`${_ozonPayBaseUrl}/v1/createOrder`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  try {
    let res = await post(build(ozonDelivery));
    let data = await res.json().catch(() => ({}));
    if (!res.ok && ozonDelivery && res.status === 400) {
      // Ozon refused the delivery part — the order is not created, the same extId is free
      logger.warn("ozon pay delivery refused, retrying without it", { extId, detail: JSON.stringify(data?.details || data).slice(0, 300) });
      res = await post(build(false));
      data = await res.json().catch(() => ({}));
    }
    if (!res.ok) {
      logger.warn("ozon pay createOrder failed", {
        status: res.status, extId, detail: JSON.stringify(data?.details || data?.message || data).slice(0, 300),
      });
      return null;
    }
    if (data?.order?.payLink) {
      logger.info("ozon pay order created", {
        extId, acquiringId: data.order.id, status: data.order.status, delivery: Boolean(data.order.deliverySettings?.isEnabled), deliveryRub, testMode: Boolean(data.order.isTestMode),
      });
    }
    return data?.order || null;
  } catch (error) {
    logger.warn("ozon pay createOrder error", { extId, detail: error?.message || String(error) });
    return null;
  }
}

app.post("/api/shop/orders", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const body = request.body || {};
    const clientItems = Array.isArray(body.items) ? body.items.slice(0, 50) : [];
    const delivery = body.delivery || {};

    if (!clientItems.length) return response.status(400).json({ error: "Корзина пуста" });
    if (!cleanText(delivery.firstName || "") || !delivery.phone || !delivery.email) {
      return response.status(400).json({ error: "Заполните обязательные поля" });
    }
    if (String(delivery.phone).replace(/\D/g, "").length < 10) return response.status(400).json({ error: "Укажите телефон полностью" });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanText(String(delivery.email)))) return response.status(400).json({ error: "Проверьте email" });
    // 152-ФЗ: separate consent to personal data; 38-ФЗ: optional ads opt-in — both stored with the order
    const consentsIn = body.consents || {};
    if (consentsIn.pd !== true) {
      return response.status(400).json({ error: "Отметьте согласие на обработку персональных данных" });
    }
    const consentAt = new Date().toISOString();
    const orderConsents = {
      version: cleanText(String(consentsIn.version || "")).slice(0, 20),
      pd: true, pdAt: consentAt,
      ads: consentsIn.ads === true, ...(consentsIn.ads === true ? { adsAt: consentAt } : {}),
      // request.ip = the address nginx saw (trust proxy); the first X-Forwarded-For entry is client-controlled
      ip: String(request.ip || "").replace(/^::ffff:/, "").slice(0, 64),
    };

    // Prices, names and photos come from the database, never from the browser
    // (the cart lives in localStorage and could be edited to any price).
    const items = [];
    for (const ci of clientItems) {
      const offerId = cleanText(ci?.offerId || "");
      if (!offerId) continue;
      const product = await findShopProductByOfferId(offerId);
      if (!product || !(product.priceRub > 0)) {
        return response.status(409).json({ error: `Товар «${cleanText(ci?.name || offerId)}» больше недоступен — удалите его из корзины` });
      }
      items.push({
        offerId: product.offerId,
        name: product.name,
        brand: product.brand || "",
        image: product.images?.[0] || null,
        volume: product.volume || null,
        quantity: Math.min(10, Math.max(1, Math.round(Number(ci.quantity) || 1))),
        priceRub: product.priceRub,
      });
    }
    if (!items.length) return response.status(400).json({ error: "Корзина пуста" });
    const baseTotal = items.reduce((s, i) => {
      const price = Number(i.priceRub);
      const qty = Math.max(1, Number(i.quantity) || 1);
      return s + (Number.isFinite(price) && price >= 0 ? price * qty : 0);
    }, 0);
    if (!Number.isFinite(baseTotal) || baseTotal <= 0) {
      return response.status(400).json({ error: "Некорректная сумма корзины" });
    }
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

    // Apply referral discount if valid code provided
    const refCode = cleanText(body.refCode || "").toUpperCase() || null;
    let refDiscountApplied = false;
    if (refCode && prisma) {
      const referrer = await prisma.shopCustomer.findUnique({ where: { referralCode: refCode }, select: { id: true, email: true, phone: true } });
      // no self-referral: neither from the same account nor as a guest with the owner's e-mail / phone
      const digits = (s) => String(s || "").replace(/\D/g, "").slice(-10);
      const self = referrer && (referrer.id === customerId
        || (referrer.email && referrer.email.toLowerCase() === cleanText(delivery.email || "").toLowerCase())
        || (digits(referrer.phone).length === 10 && digits(referrer.phone) === digits(delivery.phone)));
      if (referrer && !self) refDiscountApplied = true;
    }

    // Apply promo code discount
    const promoCodeInput = cleanText(body.promoCode || "").toUpperCase() || null;
    let promoDiscountApplied = false;
    let promoDiscountPct = 0;
    let appliedPromoCode = null;
    if (promoCodeInput && !refDiscountApplied) {
      const promo = await validatePromoCode(promoCodeInput);
      if (promo) {
        promoDiscountApplied = true;
        promoDiscountPct = promo.discountPct;
        appliedPromoCode = promo.code;
        // the usage is counted when the order is paid (payment notification), not here:
        // abandoned or scripted unpaid orders must not use up a limited promo code
      }
    }

    const discountFactor = refDiscountApplied ? (1 - _REFERRAL_DISCOUNT)
      : promoDiscountApplied ? (1 - promoDiscountPct / 100)
      : 1;
    const goodsRub = Math.round(baseTotal * discountFactor);
    // СДЭК / Яндекс / Достависта (02d-shop-delivery-carriers.js): цену выбранного варианта считаем заново на сервере
    let carrierOpt = null;
    if (["cdek", "yandex", "dostavista"].includes(delivery.carrier)) {
      const method = cleanText(delivery.method || "");
      const pvzId = cleanText(delivery.pvzId || "");
      // город и регион — из самого пункта (карта), для курьера — из адреса, найденного на карте
      let city = cleanText(delivery.city || ""), region = cleanText(delivery.region || "");
      if (pvzId && delivery.carrier === "cdek" && typeof cdekPointById === "function") {
        // the CDEK price depends on the city: take it from the real point, never from the browser
        const pt = await cdekPointById(pvzId);
        if (!pt) return response.status(400).json({ error: "Пункт выдачи СДЭК не найден — выберите его на карте ещё раз" });
        city = pt.city; region = pt.region;
      }
      if (pvzId && delivery.carrier === "yandex" && typeof _yaById !== "undefined" && _yaById.get(pvzId)) { const pt = _yaById.get(pvzId); city = pt.city || city; region = pt.region || region; }
      delivery.city = city; delivery.region = region;
      // same address string as the price quote (/checkout/quote): without a leading «<city>, »
      // (village addresses are «Хлопово, 10В»), otherwise the courier gets «Хлопово, Хлопово, 10В»
      if (!pvzId && city && delivery.address) {
        const esc = city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        delivery.address = cleanText(delivery.address).replace(new RegExp(`^${esc},\\s*`, "i"), "");
      }
      const r = await shopCarrierOptions({ items, goodsRub, city, region, address: cleanText(delivery.address || ""), pointId: pvzId, only: method });
      carrierOpt = r.options.find((o) => o.id === method) || null;
      if (!carrierOpt) return response.status(400).json({ error: "Этот способ доставки сейчас недоступен — выберите другой" });
      if (carrierOpt.needs === "point" && !pvzId) return response.status(400).json({ error: "Выберите пункт выдачи" });
      if (carrierOpt.needs === "address" && !cleanText(delivery.address || "")) return response.status(400).json({ error: "Укажите адрес доставки" });
      if (method === "dostavista_slot" && !(carrierOpt.intervals || []).some((iv) => iv.from === delivery.dvFrom && iv.to === delivery.dvTo)) {
        return response.status(400).json({ error: "Выберите время доставки курьером ещё раз — интервал уже недоступен" });
      }
    }
    const ozonDelivery = carrierOpt ? false : await shopOzonDeliveryEligible(items.map((i) => i.offerId));
    const deliveryQuote = carrierOpt ? { priceRub: carrierOpt.priceRub, cluster: null } : shopDeliveryQuote({ items, goodsRub, city: cleanText(delivery.city || ""), settings: await readShopSettings() });
    const deliveryRub = deliveryQuote.priceRub;
    const totalRub = goodsRub + deliveryRub;

    // Save order to DB
    if (prisma) {
      await prisma.shopOrder.create({
        data: {
          id: orderId,
          customerId,
          status: "pending",
          items: items,
          // whitelist: the delivery object comes from the browser
          delivery: {
            ...Object.fromEntries(
              ["type", "firstName", "lastName", "phone", "email", "city", "region", "address", "postalCode", "pvzId", "pvzName",
                "flat", "entrance", "floor", "intercom", "addrComment", "lat", "lng", "dvFrom", "dvTo"]
                .filter((k) => delivery[k] != null && delivery[k] !== "")
                .map((k) => [k, cleanText(String(delivery[k])).slice(0, 300)]),
            ),
            // Ozon Delivery: address and point are chosen on the Ozon Pay form, not here
            ...(ozonDelivery ? { type: "ozon_pay", provider: "ozon" } : {}),
            ...(carrierOpt ? {
              type: carrierOpt.kind === "courier" ? "courier" : "pickup", carrier: carrierOpt.carrier, provider: carrierOpt.carrier,
              method: carrierOpt.id, kind: carrierOpt.kind, carrierTitle: carrierOpt.title, carrierRawRub: carrierOpt.rawRub,
              ...(carrierOpt.tariffCode ? { tariffCode: carrierOpt.tariffCode, cityCode: carrierOpt.cityCode } : {}),
              daysMin: carrierOpt.daysMin, daysMax: carrierOpt.daysMax,
              ...(carrierOpt.vehicleTypeId ? { vehicleTypeId: carrierOpt.vehicleTypeId } : {}),
            } : {}),
            priceRub: deliveryRub,
            ...(deliveryQuote.cluster ? { cluster: deliveryQuote.cluster } : {}),
            consents: orderConsents,
          },
          totalRub,
          comment: body.comment ? cleanText(body.comment) : null,
          refCode: refDiscountApplied ? refCode : null,
          promoCode: promoDiscountApplied ? appliedPromoCode : null,
        },
      });
      // the referrer's points are awarded once the order is paid (shopOrderPaidRewards)
    }

    if (customerId && prisma) {
      prisma.shopCustomer.findUnique({ where: { id: customerId }, select: { firstName: true, lastName: true, phone: true } })
        .then((c) => {
          if (!c) return null;
          const data = {};
          if (!c.firstName && delivery.firstName) data.firstName = cleanText(delivery.firstName).slice(0, 60);
          if (!c.lastName && delivery.lastName) data.lastName = cleanText(delivery.lastName).slice(0, 60);
          if (!c.phone && delivery.phone) data.phone = cleanText(delivery.phone).slice(0, 30);
          return Object.keys(data).length ? prisma.shopCustomer.update({ where: { id: customerId }, data }) : null;
        })
        .catch(() => {});
    }

    logger.info("shop order created", {
      orderId, totalRub, itemCount: items.length, customerId,
      city: cleanText(delivery.city || ""),
      phone: cleanText(delivery.phone || "").replace(/\d{4}$/, "****"),
    });

    // «Заказ принят» e-mail is sent from /api/shop/payment/notification once Ozon Pay confirms
    // the payment — not here, where the buyer can still abandon or cancel the payment.

    const paymentUrl = (await _ozonPayCreateOrder({ orderId, goodsRub, deliveryRub, email: cleanText(delivery.email || ""), items, ozonDelivery }))?.payLink || null;
    if (paymentUrl && prisma) {
      await prisma.shopOrder.update({ where: { id: orderId }, data: { status: "payment_pending" } }).catch(() => {});
    } else if (!paymentUrl && prisma) {
      await prisma.shopOrder.update({ where: { id: orderId }, data: { status: "payment_failed" } }).catch(() => {});
      logger.warn("shop order payment link failed — marked payment_failed", { orderId, totalRub });
    }

    response.json({ ok: true, id: orderId, status: paymentUrl ? "payment_pending" : "pending", totalRub, deliveryRub, ozonDelivery, paymentUrl: paymentUrl || null, refDiscountApplied, promoDiscountApplied, promoDiscountPct: promoDiscountApplied ? promoDiscountPct : 0 });
  } catch (error) {
    next(error);
  }
});

// «Оплатить» from the cabinet: back to the Ozon Pay page of an unpaid order (the pickup point /
// courier address is chosen there). Ozon returns the same payment for the same extId; an expired or
// cancelled one gets a new attempt "<id>-R<n>" that the notification maps back to the order.
app.post("/api/shop/auth/orders/:id/pay", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const order = await prisma.shopOrder.findUnique({ where: { id: cleanText(request.params.id || "") } });
    if (!order || order.customerId !== request.shopCustomer.customerId) return response.status(404).json({ error: "Заказ не найден" });
    if (!["pending", "payment_pending", "payment_failed"].includes(order.status)) {
      return response.status(409).json({ error: order.status === "paid" ? "Заказ уже оплачен" : "Этот заказ нельзя оплатить" });
    }
    const d = order.delivery && typeof order.delivery === "object" ? order.delivery : {};
    const items = (Array.isArray(order.items) ? order.items : [])
      .map((i) => ({ offerId: cleanText(i.offerId || ""), name: cleanText(i.name || ""), brand: cleanText(i.brand || ""), quantity: Math.max(1, Number(i.quantity) || 1), priceRub: Number(i.priceRub) || 0 }))
      .filter((i) => i.offerId && i.priceRub > 0);
    if (!items.length) return response.status(409).json({ error: "В заказе нет товаров" });
    // Re-price from the catalogue: an unpaid order must not be paid at a stale price (e.g. the
    // 2026-09-28 cross-supplier price bug). Missing names of old orders are filled in on the way.
    const oldBase = items.reduce((sum, i) => sum + i.priceRub * i.quantity, 0);
    for (const it of items) {
      const p = await findShopProductByOfferId(it.offerId, { fast: true }).catch(() => null);
      if (!p || !(p.priceRub > 0)) return response.status(409).json({ error: `Товар «${it.name || it.offerId}» больше недоступен — оформите заказ заново` });
      if (!it.name) { it.name = cleanText(p.name || ""); it.brand = cleanText(p.brand || ""); }
      it.priceRub = p.priceRub;
    }
    const oldDelivery = Math.max(0, Number(d.priceRub) || 0);
    // keep the order's promo / referral discount as a ratio
    const factor = oldBase > 0 ? Math.min(1, (order.totalRub - oldDelivery) / oldBase) : 1;
    const goodsRub = Math.round(items.reduce((sum, i) => sum + i.priceRub * i.quantity, 0) * factor);
    // СДЭК / Яндекс / Достависта: the buyer already chose the point or address and its price — keep
    // that price and never switch the order to Ozon Delivery. The Ozon/flat rules only apply to the rest.
    const ownCarrier = ["cdek", "yandex", "dostavista"].includes(d.carrier);
    const deliveryRub = ownCarrier ? oldDelivery : shopDeliveryQuote({ items, goodsRub, city: cleanText(d.city || ""), settings: await readShopSettings() }).priceRub;
    const ozonDelivery = ownCarrier ? false : d.type === "ozon_pay" || d.provider === "ozon" ? await shopOzonDeliveryEligible(items.map((i) => i.offerId)) : false;
    let attempt = Math.max(1, Number(d.payAttempt) || 1);
    if (goodsRub + deliveryRub !== order.totalRub) {
      // amount changed → the existing Ozon payment (fixed amount) can't be reused: new attempt id
      attempt += 1;
      const priced = (Array.isArray(order.items) ? order.items : []).map((raw) => {
        const it = items.find((x) => x.offerId === cleanText(raw.offerId || ""));
        return it ? { ...raw, name: it.name, brand: it.brand, priceRub: it.priceRub } : raw;
      });
      await prisma.shopOrder.update({ where: { id: order.id }, data: { items: priced, totalRub: goodsRub + deliveryRub, delivery: { ...d, priceRub: deliveryRub, payAttempt: attempt } } });
      logger.info("shop order repriced before payment", { orderId: order.id, from: order.totalRub, to: goodsRub + deliveryRub });
      d.payAttempt = attempt;
      d.priceRub = deliveryRub; // later delivery updates below spread `d`
    }
    const args = { orderId: order.id, goodsRub, deliveryRub, email: cleanText(d.email || ""), items, ozonDelivery };

    let extId = attempt > 1 ? `${order.id}-R${attempt}` : order.id;
    let acq = await _ozonPayCreateOrder({ ...args, extId });
    if (acq && ["STATUS_PAID", "STATUS_AUTHORIZED"].includes(acq.status)) {
      // paid, the notification just hasn't arrived — the notification handler sends the e-mail
      const upd = await prisma.shopOrder.updateMany({ where: { id: order.id, status: { in: ["pending", "payment_pending", "payment_failed"] } }, data: { status: "paid" } });
      // this call made the transition, so the notification will find the order already paid
      if (upd.count > 0) await shopOrderOnPaid(order.id);
      return response.json({ ok: true, paid: true });
    }
    if (!acq?.payLink || !["STATUS_NEW", "STATUS_PAYMENT_PENDING"].includes(acq.status)) {
      extId = `${order.id}-R${attempt + 1}`;
      acq = await _ozonPayCreateOrder({ ...args, extId });
      if (acq?.payLink) {
        await prisma.shopOrder.update({ where: { id: order.id }, data: { delivery: { ...d, payAttempt: attempt + 1 } } }).catch(() => {});
      }
    }
    if (!acq?.payLink) return response.status(502).json({ error: "Не удалось получить ссылку на оплату — попробуйте позже" });
    if (order.status !== "payment_pending") {
      await prisma.shopOrder.update({ where: { id: order.id }, data: { status: "payment_pending" } }).catch(() => {});
    }
    logger.info("shop order payment reopened", { orderId: order.id, extId, acqStatus: acq.status });
    response.json({ ok: true, paymentUrl: acq.payLink });
  } catch (error) { next(error); }
});

// Everything that must happen once an order becomes "paid" — called only by whoever made the
// transition (payment notification or «Оплатить» finding the payment already done).
async function shopOrderOnPaid(orderId) {
  const prisma = getPrisma();
  if (!prisma) return;
  const order = await prisma.shopOrder.findUnique({ where: { id: orderId } }).catch(() => null);
  if (!order) return;
  const d = order.delivery && typeof order.delivery === "object" ? order.delivery : {};
  if (d.email && typeof sendOrderSequenceEmail === "function") {
    sendOrderSequenceEmail({
      orderId: order.id,
      email: cleanText(d.email),
      firstName: cleanText(d.firstName || ""),
      items: Array.isArray(order.items) ? order.items : [],
      totalRub: order.totalRub,
    }).catch((err) => logger.warn("order confirmation e-mail failed", { orderId, detail: err?.message }));
  }
  // promo usage is counted for paid orders only
  if (order.promoCode) {
    await withShopPromoLock(async () => {
      const codes = await readShopPromoCodes();
      const idx = codes.findIndex((c) => String(c.code || "").toUpperCase() === String(order.promoCode).toUpperCase());
      if (idx >= 0) {
        codes[idx] = { ...codes[idx], usageCount: (codes[idx].usageCount || 0) + 1 };
        await writeShopPromoCodes(codes);
      }
    }).catch((err) => logger.warn("promo usage count failed", { orderId, detail: err?.message }));
  }
  // referrer's points — for a paid order, once per order
  if (order.refCode) {
    try {
      const referrer = await prisma.shopCustomer.findUnique({ where: { referralCode: order.refCode }, select: { id: true } });
      const already = referrer ? await prisma.shopPointTransaction.findFirst({ where: { customerId: referrer.id, refId: order.id }, select: { id: true } }) : null;
      if (referrer && referrer.id !== order.customerId && !already) {
        await prisma.$transaction([
          prisma.shopPointTransaction.create({
            data: { customerId: referrer.id, points: _REFERRAL_POINTS_FOR_REFERRER, reason: `Реферальный заказ ${order.id}`, refId: order.id },
          }),
          prisma.shopCustomer.update({ where: { id: referrer.id }, data: { loyaltyPoints: { increment: _REFERRAL_POINTS_FOR_REFERRER } } }),
        ]);
      }
    } catch (err) {
      logger.warn("referral points award failed", { orderId, detail: err?.message || String(err) });
    }
  }
  // СДЭК / Яндекс / Достависта: отправка сразу после оплаты, если включено в настройках доставки
  if (["cdek", "yandex", "dostavista"].includes(d.carrier) && typeof shopCarrierCreateShipment === "function") {
    readShopSettings().then((st) => (st?.carriers?.autoCreateShipment ? shopCarrierCreateShipment(order.id) : null))
      .catch((err) => logger.warn("auto shipment failed", { orderId, detail: err?.message }));
  }
}

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

    const { status, orderID } = body;
    // a repeated payment attempt goes out as "<order id>-R<n>" (see /pay below)
    const extOrderID = cleanText(body.extOrderID || "").replace(/-R\d+$/, "");
    logger.info("ozon pay notification", {
      extOrderID, orderID, status, amount: body.amount, method: body.paymentMethod,
    });

    // Completed = captured; Authorized = funds held (two-stage payment) — both mean the buyer paid
    if (extOrderID && (status === "Completed" || status === "Authorized")) {
      const prisma = getPrisma();
      if (prisma) {
        const updated = await prisma.shopOrder.updateMany({
          where: { id: extOrderID, status: { notIn: ["paid", "shipped", "delivered", "cancelled"] } },
          data: { status: "paid" },
        }).catch((err) => {
          logger.warn("ozon pay order status update failed", { extOrderID, detail: err?.message });
          return null;
        });
        if (updated?.count > 0) {
          logger.info("shop order paid via ozon pay", { extOrderID, status });
          // amounts are signed by Ozon, but an old payment link of a since-repriced order could still be
          // paid — flag a short payment for the manager instead of trusting it silently
          const order = await prisma.shopOrder.findUnique({ where: { id: extOrderID } }).catch(() => null);
          const paidRub = Number(body.amount) / 100;
          if (order && Number.isFinite(paidRub) && paidRub > 0 && paidRub + 1 < Number(order.totalRub)) {
            logger.warn("ozon pay: paid less than the order total", { extOrderID, paidRub, totalRub: order.totalRub });
            const d0 = order.delivery && typeof order.delivery === "object" ? order.delivery : {};
            await prisma.shopOrder.update({ where: { id: order.id }, data: { delivery: { ...d0, paidRub, underpaid: true } } }).catch(() => {});
          }
          // first transition to "paid" only → e-mail, rewards and shipment happen exactly once
          await shopOrderOnPaid(extOrderID);
        }
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
// account "shop" → buyers' mail (noreply@magicvibes.ru, SHOP_SMTP_*);
// account "supplier" → cancellations to suppliers, must keep coming from the warehouse Gmail
// (SUPPLIER_SMTP_*; falls back to SHOP_SMTP_* only while the supplier box is not configured).
function shopSendEmail({ to, subject, html, marketing = false, account = "shop" }) {
  return new Promise((resolve, reject) => {
    const tls = require("tls");
    const env = process.env;
    const sup = account === "supplier" && env.SUPPLIER_SMTP_USER && env.SUPPLIER_SMTP_PASS;
    const host = sup ? (env.SUPPLIER_SMTP_HOST || "smtp.gmail.com") : env.SHOP_SMTP_HOST;
    const port = Number((sup ? env.SUPPLIER_SMTP_PORT : env.SHOP_SMTP_PORT) || 465);
    const user = sup ? env.SUPPLIER_SMTP_USER : env.SHOP_SMTP_USER;
    const pass = sup ? env.SUPPLIER_SMTP_PASS : env.SHOP_SMTP_PASS;
    const fromName = (sup ? env.SUPPLIER_SMTP_FROM_NAME : env.SHOP_SMTP_FROM_NAME) || "Magic Vibes";
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
          `From: =?UTF-8?B?${Buffer.from(fromName, "utf8").toString("base64")}?= <${user}>`,
          // Date + Message-ID: their absence is a spam signal for Gmail / Mail.ru / Yandex
          `Date: ${new Date().toUTCString().replace("GMT", "+0000")}`,
          `Message-ID: <${Date.now().toString(36)}.${_shopCrypto.randomBytes(8).toString("hex")}@${String(user).split("@")[1] || "magicvibes.ru"}>`,
          `To: ${to}`,
          `Subject: =?UTF-8?B?${Buffer.from(subject, "utf8").toString("base64")}?=`,
          `MIME-Version: 1.0`,
          `Content-Type: text/html; charset=utf-8`,
          `X-Mailer: Magic Vibes Shop`,
          ...(marketing ? [`List-Unsubscribe: <mailto:${user}?subject=unsubscribe>`] : []),
          `Content-Transfer-Encoding: base64`,
          ``,
          // base64 keeps long HTML lines within SMTP limits and makes dot-stuffing unnecessary
          Buffer.from(html, "utf8").toString("base64").replace(/.{1,76}/g, "$&\r\n").trimEnd(),
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

// ── Branded e-mail layout (magicvibes.ru design: paper, ink, lime, pink) ─────
// Table layout + inline styles only: Gmail / Mail.ru / Yandex strip <style> and web fonts,
// so display type falls back from Unbounded to Arial Black.
const MV_MAIL = {
  site: "https://magicvibes.ru",
  logo: "https://magicvibes.ru/brand/logo/logo-email.png",
  ink: "#121212", paper: "#f5f2ec", lime: "#d9f84a", pink: "#ff3d7f", violet: "#4b3cff", muted: "#6b6860",
  display: "'Unbounded','Arial Black','Helvetica Neue',Arial,sans-serif",
  body: "'Onest','Helvetica Neue',Arial,sans-serif",
};

function mvEsc(v) {
  return String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function mvEmailButton(label, href, tone = "ink") {
  const bg = tone === "lime" ? MV_MAIL.lime : MV_MAIL.ink;
  const fg = tone === "lime" ? MV_MAIL.ink : "#ffffff";
  return `<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:8px 0 4px;"><tr>
<td bgcolor="${bg}" style="border-radius:999px;background:${bg};">
<a href="${href}" target="_blank" style="display:inline-block;padding:16px 30px;font-family:${MV_MAIL.body};font-size:15px;font-weight:700;color:${fg};text-decoration:none;border-radius:999px;">${label}&nbsp;&nbsp;&#8599;</a>
</td></tr></table>`;
}

/** Big highlighted value (login code, promo code) on a lime tile. */
function mvEmailCodeTile(value, caption) {
  return `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:26px 0 22px;"><tr>
<td align="center" bgcolor="${MV_MAIL.lime}" style="background:${MV_MAIL.lime};border-radius:22px;padding:26px 16px 22px;">
<div style="font-family:${MV_MAIL.display};font-size:38px;line-height:1;font-weight:800;letter-spacing:8px;color:${MV_MAIL.ink};mso-line-height-rule:exactly;">${mvEsc(value)}</div>
${caption ? `<div style="font-family:${MV_MAIL.body};font-size:13px;color:rgba(18,18,18,0.62);margin-top:12px;">${caption}</div>` : ""}
</td></tr></table>`;
}

/**
 * Full e-mail. opts: { preheader, kicker, title, script, intro, content, cta: {label, href}, note, marketing }
 * `content` / `intro` / `note` are trusted HTML built by our templates (escape user data before passing).
 */
function mvEmailLayout(opts = {}) {
  const { preheader = "", kicker = "", title = "", script = "", intro = "", content = "", cta = null, note = "", marketing = false } = opts;
  const unsub = marketing
    ? `<br><a href="mailto:${mvEsc(process.env.SHOP_SMTP_USER || "")}?subject=unsubscribe" style="color:rgba(245,242,236,0.55);text-decoration:underline;">Отписаться от писем</a>`
    : "";
  return `<!DOCTYPE html>
<html lang="ru" xmlns="http://www.w3.org/1999/xhtml"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="x-apple-disable-message-reformatting">
<meta name="color-scheme" content="light only"><meta name="supported-color-schemes" content="light">
<title>Magic Vibes</title>
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@800&family=Onest:wght@400;600;700&family=Marck+Script&display=swap&subset=cyrillic" rel="stylesheet">
</head>
<body style="margin:0;padding:0;background:${MV_MAIL.paper};-webkit-text-size-adjust:100%;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;">${mvEsc(preheader)}&#8199;&#847;&#8199;&#847;&#8199;&#847;&#8199;&#847;</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="${MV_MAIL.paper}" style="background:${MV_MAIL.paper};">
<tr><td align="center" style="padding:28px 12px 36px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;">

<!-- logo -->
<tr><td style="padding:0 8px 18px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
<td align="left"><a href="${MV_MAIL.site}" target="_blank"><img src="${MV_MAIL.logo}" width="176" height="40" alt="MAGIC vibes" style="display:block;border:0;width:176px;height:auto;"></a></td>
<td align="right" style="font-family:${MV_MAIL.body};font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:${MV_MAIL.muted};">Оригинальная<br>парфюмерия</td>
</tr></table>
</td></tr>

<!-- card -->
<tr><td bgcolor="#ffffff" style="background:#ffffff;border-radius:28px;padding:36px 32px 34px;">
${kicker ? `<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr><td bgcolor="${MV_MAIL.ink}" style="background:${MV_MAIL.ink};border-radius:999px;padding:6px 12px;font-family:${MV_MAIL.body};font-size:11px;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;color:${MV_MAIL.paper};">&#10022;&nbsp;${kicker}</td></tr></table>` : ""}
${title ? `<h1 style="margin:18px 0 0;font-family:${MV_MAIL.display};font-size:30px;line-height:1.05;font-weight:800;letter-spacing:-0.5px;text-transform:uppercase;color:${MV_MAIL.ink};">${title}</h1>` : ""}
${script ? `<div style="margin:6px 0 0;font-family:'Marck Script','Segoe Script',Georgia,cursive;font-style:italic;font-size:22px;line-height:1.2;color:${MV_MAIL.pink};">${script}</div>` : ""}
${intro ? `<p style="margin:18px 0 0;font-family:${MV_MAIL.body};font-size:16px;line-height:1.6;color:#2b2a27;">${intro}</p>` : ""}
${content}
${cta ? mvEmailButton(cta.label, cta.href) : ""}
${note ? `<p style="margin:26px 0 0;padding-top:20px;border-top:1px solid #ece8e0;font-family:${MV_MAIL.body};font-size:13px;line-height:1.6;color:${MV_MAIL.muted};">${note}</p>` : ""}
</td></tr>

<!-- footer -->
<tr><td style="padding:14px 0 0;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
<td bgcolor="${MV_MAIL.ink}" style="background:${MV_MAIL.ink};border-radius:28px;padding:26px 32px;">
<div style="font-family:${MV_MAIL.display};font-size:17px;line-height:1.2;font-weight:800;text-transform:uppercase;color:${MV_MAIL.paper};">22&nbsp;000+ оригинальных ароматов</div>
<div style="font-family:'Marck Script','Segoe Script',Georgia,cursive;font-style:italic;font-size:17px;color:${MV_MAIL.lime};margin-top:4px;">с доставкой по всей России</div>
<div style="margin-top:16px;font-family:${MV_MAIL.body};font-size:13px;line-height:2;">
<a href="${MV_MAIL.site}/catalog" style="color:${MV_MAIL.paper};text-decoration:none;font-weight:600;">Каталог</a>&nbsp;&nbsp;<span style="color:${MV_MAIL.pink};">&#10022;</span>&nbsp;&nbsp;<a href="${MV_MAIL.site}/find" style="color:${MV_MAIL.paper};text-decoration:none;font-weight:600;">AI-подбор</a>&nbsp;&nbsp;<span style="color:${MV_MAIL.pink};">&#10022;</span>&nbsp;&nbsp;<a href="${MV_MAIL.site}/account" style="color:${MV_MAIL.paper};text-decoration:none;font-weight:600;">Кабинет</a>
</div>
<div style="margin-top:12px;font-family:${MV_MAIL.body};font-size:11px;line-height:1.6;color:rgba(245,242,236,0.55);">&copy; Magic Vibes &middot; <a href="${MV_MAIL.site}" style="color:rgba(245,242,236,0.75);text-decoration:none;">magicvibes.ru</a>${unsub}</div>
</td></tr></table>
</td></tr>

</table>
</td></tr></table>
</body></html>`;
}

function shopOtpEmailHtml(code) {
  return mvEmailLayout({
    preheader: `Код для входа: ${code}. Действует 10 минут.`,
    kicker: "Вход в кабинет",
    title: "Ваш код",
    script: "для входа на Magic Vibes",
    intro: "Введите его на странице входа. Код действует <b>10 минут</b>.",
    content: mvEmailCodeTile(code, "Никому не сообщайте этот код"),
    note: "Если вы не запрашивали вход — просто проигнорируйте письмо, аккаунт в безопасности.",
  });
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

    const code = String(_shopCrypto.randomInt(100000, 1000000));
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
      take: 50,
    });
    // Old orders stored only offerId/price from the browser → fill name/photo for the cabinet
    const missing = [...new Set(orders.flatMap((o) => (Array.isArray(o.items) ? o.items : [])
      .filter((i) => i && (!i.name || !i.image)).map((i) => cleanText(i.offerId))).filter(Boolean))].slice(0, 60);
    const info = new Map();
    await Promise.all(missing.map(async (id) => {
      const p = await findShopProductByOfferId(id).catch(() => null);
      if (p) info.set(id, { name: p.name, brand: p.brand, image: p.images?.[0] || null, slug: shopProductSlug(p.name, p.offerId) });
    }));
    const enriched = orders.map((o) => ({
      ...o,
      ...(o.delivery?.shipment?.id && typeof shopTrackUrl === "function" ? { trackUrl: shopTrackUrl(o.id).replace(/^https?:\/\/[^/]+/, "") } : {}),
      items: (Array.isArray(o.items) ? o.items : []).map((i) => {
        const extra = info.get(cleanText(i.offerId)) || {};
        return { ...extra, ...Object.fromEntries(Object.entries(i).filter(([, v]) => v != null && v !== "")), slug: i.slug || extra.slug || null };
      }),
    }));
    const paidLike = ["paid", "confirmed", "picking", "shipped", "delivered"];
    const stats = {
      orders: orders.length,
      paidOrders: orders.filter((o) => paidLike.includes(o.status)).length,
      spentRub: orders.filter((o) => paidLike.includes(o.status)).reduce((s, o) => s + (o.totalRub || 0), 0),
    };
    response.json({ ok: true, orders: enriched, stats });
  } catch (error) { next(error); }
});

// Saved delivery addresses = distinct pickup points / courier addresses from the customer's orders
// (the way Ozon / WB / Market remember them), newest first.
app.get("/api/shop/auth/addresses", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });
    const orders = await prisma.shopOrder.findMany({
      where: { customerId: request.shopCustomer.customerId },
      orderBy: { createdAt: "desc" },
      take: 100,
      select: { delivery: true, createdAt: true },
    });
    const seen = new Set();
    const addresses = [];
    let contact = null;
    for (const o of orders) {
      const d = o.delivery && typeof o.delivery === "object" ? o.delivery : {};
      if (!contact && d.phone) contact = { firstName: d.firstName || "", lastName: d.lastName || "", phone: d.phone || "", email: d.email || "" };
      const type = d.type === "courier" ? "courier" : "pickup";
      const address = cleanText(d.address || "");
      if (!address) continue;
      const key = type === "pickup" && d.pvzId ? `pvz:${d.pvzId}` : `${type}:${address.toLowerCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);
      addresses.push({
        id: key, type, pvzId: d.pvzId || null, name: cleanText(d.pvzName || ""), address,
        city: cleanText(d.city || ""), postalCode: cleanText(d.postalCode || ""), lastUsed: o.createdAt,
        ...(d.carrier ? { carrier: d.carrier, method: d.method || null, region: d.region || "", lat: Number(d.lat) || undefined, lng: Number(d.lng) || undefined,
          flat: d.flat || "", entrance: d.entrance || "", floor: d.floor || "", intercom: d.intercom || "", addrComment: d.addrComment || "" } : {}),
      });
      if (addresses.length >= 8) break;
    }
    response.json({ ok: true, addresses, contact });
  } catch (error) { next(error); }
});

// ── Referral program ─────────────────────────────────────────────────────

function _generateReferralCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 8; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

const _REFERRAL_DISCOUNT = 0.07;
const _REFERRAL_POINTS_FOR_REFERRER = 100;
const _SHOP_BASE_URL = process.env.SHOP_BASE_URL || "https://magicvibes.ru";

app.get("/api/shop/auth/referral", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "БД недоступна" });
    const customerId = request.shopCustomer.customerId;

    let customer = await prisma.shopCustomer.findUnique({
      where: { id: customerId },
      select: { referralCode: true },
    });
    if (!customer) return response.status(404).json({ error: "Не найден" });

    if (!customer.referralCode) {
      let code;
      for (let attempt = 0; attempt < 5; attempt++) {
        code = _generateReferralCode();
        const existing = await prisma.shopCustomer.findUnique({ where: { referralCode: code }, select: { id: true } });
        if (!existing) break;
      }
      customer = await prisma.shopCustomer.update({
        where: { id: customerId },
        data: { referralCode: code },
        select: { referralCode: true },
      });
    }

    const code = customer.referralCode;
    const ordersFromRef = await prisma.shopOrder.count({ where: { refCode: code } });
    response.json({ ok: true, code, link: `${_SHOP_BASE_URL}/?ref=${code}`, ordersFromRef, discountPct: 7 });
  } catch (error) { next(error); }
});

app.post("/api/shop/referral/validate", shopCors, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const code = cleanText(request.body?.code || "").toUpperCase();
    if (!code || code.length !== 8) return response.json({ ok: false, valid: false });
    if (!prisma) return response.json({ ok: false, valid: false });
    const owner = await prisma.shopCustomer.findUnique({ where: { referralCode: code }, select: { id: true } });
    response.json({ ok: true, valid: Boolean(owner), discountPct: owner ? 7 : 0 });
  } catch (error) { next(error); }
});

// POST /api/shop/promo/validate — public promo code validation
app.post("/api/shop/promo/validate", shopCors, async (request, response, next) => {
  try {
    const code = cleanText(request.body?.code || "").toUpperCase();
    if (!code) return response.json({ ok: true, valid: false });
    const found = await validatePromoCode(code);
    if (!found) return response.json({ ok: true, valid: false });
    response.json({ ok: true, valid: true, discountPct: found.discountPct, code: found.code, note: found.note || "" });
  } catch (error) { next(error); }
});

// ── VIP-клуб ─────────────────────────────────────────────────────────────

const _VIP_ORDER_THRESHOLD = 3;

app.get("/api/shop/auth/vip", shopCors, requireShopAuth, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.json({ ok: true, eligible: false, ordersCount: 0, vipLink: null });
    const settings = await readShopSettings();
    // switched off in the admin (Магазин → Настройки → Разделы сайта)
    if (!settings.features?.vipClub) return response.json({ ok: true, disabled: true, eligible: false, ordersCount: 0, vipLink: null });
    // the token carries customerId (".id" was undefined → Prisma counted every buyer's orders)
    const customerId = request.shopCustomer.customerId;
    const ordersCount = await prisma.shopOrder.count({
      where: { customerId, status: { in: ["completed", "delivered", "shipped"] } },
    });
    const vipLink = cleanText(settings.vipTelegramLink || "") || null;
    response.json({ ok: true, eligible: ordersCount >= _VIP_ORDER_THRESHOLD, ordersCount, vipLink });
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
      endDate: cleanText(request.body.endDate || ""),
      promoCode: cleanText(request.body.promoCode || "").toUpperCase() || null,
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
    const body = request.body || {};
    const allowedFields = {};
    if (body.imageUrl !== undefined) allowedFields.imageUrl = cleanText(body.imageUrl || "");
    if (body.title !== undefined) allowedFields.title = cleanText(body.title || "");
    if (body.subtitle !== undefined) allowedFields.subtitle = cleanText(body.subtitle || "");
    if (body.linkUrl !== undefined) allowedFields.linkUrl = cleanText(body.linkUrl || "");
    if (body.linkText !== undefined) allowedFields.linkText = cleanText(body.linkText || "");
    if (body.endDate !== undefined) allowedFields.endDate = cleanText(body.endDate || "");
    if (body.promoCode !== undefined) allowedFields.promoCode = cleanText(body.promoCode || "").toUpperCase() || null;
    if (body.active !== undefined) allowedFields.active = Boolean(body.active);
    if (body.order !== undefined) allowedFields.order = Number(body.order);
    banners[idx] = { ...banners[idx], ...allowedFields, id: request.params.id };
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

// Holiday banners
app.get("/api/shop/admin/holiday-banners", requireAdmin, async (_request, response, next) => {
  try {
    const overrides = await readHolidaySettings();
    const result = HOLIDAY_PRESETS.map((p) => {
      const ov = overrides[p.key] || {};
      const inWindow = _isHolidayInWindow(p);
      return {
        key: p.key,
        name: p.name,
        emoji: p.emoji,
        defaultPromoCode: p.defaultPromoCode,
        defaultTitle: p.defaultTitle,
        defaultSubtitle: p.defaultSubtitle,
        windowStart: p.windowStart,
        windowEnd: p.windowEnd,
        inWindow,
        active: ov.active === true || (inWindow && ov.active !== false),
        promoCode: ov.promoCode || p.defaultPromoCode,
        title: ov.title || p.defaultTitle,
        subtitle: ov.subtitle || p.defaultSubtitle,
        manualOverride: typeof ov.active === "boolean",
      };
    });
    response.json(result);
  } catch (error) { next(error); }
});

app.patch("/api/shop/admin/holiday-banners/:key", requireAdmin, async (request, response, next) => {
  try {
    const { key } = request.params;
    if (!HOLIDAY_PRESETS.find((p) => p.key === key))
      return response.status(404).json({ error: "Holiday preset not found" });
    const overrides = await readHolidaySettings();
    const existing = overrides[key] || {};
    const updated = { ...existing };
    if (typeof request.body.active === "boolean") updated.active = request.body.active;
    if (typeof request.body.promoCode === "string") updated.promoCode = cleanText(request.body.promoCode);
    if (typeof request.body.title === "string") updated.title = cleanText(request.body.title);
    if (typeof request.body.subtitle === "string") updated.subtitle = cleanText(request.body.subtitle);
    const appSettings = await readAppSettings();
    await writeAppSettings({ ...appSettings, [SHOP_HOLIDAY_KEY]: { ...overrides, [key]: updated } });
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
    const allowed = ["markup", "markupRules", "shopName", "shopDescription", "contactEmail", "contactPhone", "deliveryDays", "deliveryDaysMin", "deliveryPriceRub", "freeDeliveryFrom", "deliveryRules", "carriers", "features", "vipTelegramLink", "aromaMesyatsa", "contest"];
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
    logger.info("shop order status set by admin", { orderId: order.id, status, by: request.session?.user?.username || request.user?.username || null });
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
    if (request.body?.consents?.pd !== true) return response.status(400).json({ error: "Отметьте согласие на обработку персональных данных" });
    logger.info("shop_consent", { kind: "stock_alert", email, version: cleanText(String(request.body.consents.version || "")).slice(0, 20), ip: String(request.ip || "").replace(/^::ffff:/, "").slice(0, 64) });
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

// ─── Unboxings ────────────────────────────────────────────────────────────────
const SHOP_UNBOXINGS_KEY = "shop_unboxings";

async function readUnboxings() {
  const appSettings = await readAppSettings();
  const data = appSettings[SHOP_UNBOXINGS_KEY];
  return Array.isArray(data) ? data : [];
}

app.get("/api/shop/unboxings", shopCors, async (_req, res, next) => {
  try {
    const all = await readUnboxings();
    res.json({ ok: true, unboxings: all.filter((u) => u.approved) });
  } catch (e) { next(e); }
});

app.post("/api/shop/unboxings", shopCors, async (req, res, next) => {
  try {
    const all = await readUnboxings();
    // only http(s) media links (no javascript:/data: URLs rendered on the site after approval)
    const mediaUrl = cleanText(req.body?.mediaUrl || "").slice(0, 500);
    if (mediaUrl && !/^https:\/\/[^\s"'<>]+$/i.test(mediaUrl)) return res.status(400).json({ error: "Некорректная ссылка на фото или видео" });
    const entry = {
      id: nanoid8(),
      name: cleanText(req.body?.name || "Аноним").slice(0, 100),
      mediaUrl,
      text: cleanText(req.body?.text || "").slice(0, 1000),
      approved: false,
      createdAt: new Date().toISOString(),
    };
    all.unshift(entry);
    // the list lives in the shared app settings: a flood of anonymous posts must not grow it without
    // bound — keep every approved entry and the 100 newest ones waiting for moderation
    let pending = 0;
    const kept = all.filter((u) => u.approved || ++pending <= 100);
    await writeShopData(SHOP_UNBOXINGS_KEY, kept);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

app.get("/api/shop/admin/unboxings", requireAdmin, async (_req, res, next) => {
  try {
    res.json({ ok: true, unboxings: await readUnboxings() });
  } catch (e) { next(e); }
});

app.patch("/api/shop/admin/unboxings/:id", requireAdmin, async (req, res, next) => {
  try {
    const all = await readUnboxings();
    const idx = all.findIndex((u) => u.id === req.params.id);
    if (idx === -1) return res.status(404).json({ error: "Not found" });
    all[idx] = { ...all[idx], approved: req.body.approved !== false };
    await writeShopData(SHOP_UNBOXINGS_KEY, all);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

app.delete("/api/shop/admin/unboxings/:id", requireAdmin, async (req, res, next) => {
  try {
    const all = (await readUnboxings()).filter((u) => u.id !== req.params.id);
    await writeShopData(SHOP_UNBOXINGS_KEY, all);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Blog ─────────────────────────────────────────────────────────────────────

function _blogSlugify(title) {
  return title
    .toLowerCase()
    .replace(/[а-яёa-z0-9]+/gi, (m) => m)
    .replace(/[^a-z0-9а-яё]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || `post-${Date.now()}`;
}

// Public: list published posts
app.get("/api/shop/blog", shopCors, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.json({ ok: true, posts: [] });
    const tag = cleanText(req.query.tag || "");
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(20, Math.max(1, Number(req.query.pageSize) || 10));
    const where = { published: true, ...(tag ? { tags: { has: tag } } : {}) };
    const [posts, total] = await Promise.all([
      prisma.blogPost.findMany({
        where, orderBy: { publishedAt: "desc" }, skip: (page - 1) * pageSize, take: pageSize,
        select: { id: true, slug: true, title: true, excerpt: true, coverUrl: true, tags: true, publishedAt: true },
      }),
      prisma.blogPost.count({ where }),
    ]);
    res.json({ ok: true, posts, total, page, pageSize });
  } catch (e) { next(e); }
});

// Public: single post by slug
app.get("/api/shop/blog/:slug", shopCors, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "БД недоступна" });
    const slug = cleanText(req.params.slug);
    const post = await prisma.blogPost.findFirst({ where: { slug, published: true } });
    if (!post) return res.status(404).json({ error: "Статья не найдена" });
    res.json({ ok: true, post });
  } catch (e) { next(e); }
});

// Admin: list all posts (including drafts)
app.get("/api/shop/admin/blog", requireAdmin, async (_req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.json({ ok: true, posts: [] });
    const posts = await prisma.blogPost.findMany({
      orderBy: { createdAt: "desc" },
      select: { id: true, slug: true, title: true, excerpt: true, coverUrl: true, tags: true, published: true, publishedAt: true, createdAt: true },
    });
    res.json({ ok: true, posts });
  } catch (e) { next(e); }
});

// Admin: create post
app.post("/api/shop/admin/blog", requireAdmin, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "БД недоступна" });
    const title = cleanText(req.body?.title || "");
    if (!title) return res.status(400).json({ error: "Нужен title" });
    const slug = cleanText(req.body?.slug || "") || _blogSlugify(title);
    const published = Boolean(req.body?.published);
    const post = await prisma.blogPost.create({
      data: {
        slug,
        title,
        excerpt: cleanText(req.body?.excerpt || "") || null,
        content: cleanText(req.body?.content || ""),
        coverUrl: cleanText(req.body?.coverUrl || "") || null,
        tags: Array.isArray(req.body?.tags) ? req.body.tags.map(cleanText).filter(Boolean) : [],
        published,
        publishedAt: published ? new Date() : null,
      },
    });
    res.json({ ok: true, post });
  } catch (e) {
    if (e.code === "P2002") return res.status(409).json({ error: "Slug уже занят" });
    next(e);
  }
});

// Admin: update post
app.put("/api/shop/admin/blog/:id", requireAdmin, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "БД недоступна" });
    const existing = await prisma.blogPost.findUnique({ where: { id: req.params.id }, select: { published: true, publishedAt: true } });
    if (!existing) return res.status(404).json({ error: "Не найдено" });
    const published = Boolean(req.body?.published);
    const wasPublished = existing.published;
    const data = {};
    if (req.body.title !== undefined) data.title = cleanText(req.body.title);
    if (req.body.slug !== undefined) data.slug = cleanText(req.body.slug);
    if (req.body.excerpt !== undefined) data.excerpt = cleanText(req.body.excerpt) || null;
    if (req.body.content !== undefined) data.content = cleanText(req.body.content);
    if (req.body.coverUrl !== undefined) data.coverUrl = cleanText(req.body.coverUrl) || null;
    if (req.body.tags !== undefined) data.tags = Array.isArray(req.body.tags) ? req.body.tags.map(cleanText).filter(Boolean) : [];
    data.published = published;
    if (published && !wasPublished) data.publishedAt = new Date();
    const post = await prisma.blogPost.update({ where: { id: req.params.id }, data });
    res.json({ ok: true, post });
  } catch (e) {
    if (e.code === "P2002") return res.status(409).json({ error: "Slug уже занят" });
    next(e);
  }
});

// Admin: delete post
app.delete("/api/shop/admin/blog/:id", requireAdmin, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "БД недоступна" });
    await prisma.blogPost.delete({ where: { id: req.params.id } });
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ── Promo Codes (admin) ───────────────────────────────────────────────────────

// GET /api/shop/admin/promo-codes
app.get("/api/shop/admin/promo-codes", requireAdmin, async (_req, res, next) => {
  try {
    const codes = await readShopPromoCodes();
    res.json({ ok: true, codes });
  } catch (e) { next(e); }
});

// POST /api/shop/admin/promo-codes — create custom promo
app.post("/api/shop/admin/promo-codes", requireAdmin, async (req, res, next) => {
  try {
    const code = cleanText(req.body?.code || "").toUpperCase().replace(/\s+/g, "");
    const discountPct = Math.min(100, Math.max(1, Number(req.body?.discountPct || 10)));
    const note = cleanText(req.body?.note || "").slice(0, 200) || null;
    const usageLimit = req.body?.usageLimit ? Math.max(1, Number(req.body.usageLimit)) : null;
    const expiresAt = req.body?.expiresAt ? new Date(req.body.expiresAt).toISOString() : null;
    if (!code || code.length < 2 || code.length > 32) {
      return res.status(400).json({ error: "Код должен быть от 2 до 32 символов" });
    }
    const codes = await readShopPromoCodes();
    if (codes.some(c => c.code.toUpperCase() === code)) {
      return res.status(409).json({ error: "Такой промокод уже существует" });
    }
    const newCode = {
      id: `custom-${nanoid8()}`,
      code,
      discountPct,
      active: true,
      usageLimit,
      usageCount: 0,
      note,
      expiresAt,
      builtin: false,
      createdAt: new Date().toISOString(),
    };
    codes.push(newCode);
    await writeShopPromoCodes(codes);
    res.json({ ok: true, code: newCode });
  } catch (e) { next(e); }
});

// PATCH /api/shop/admin/promo-codes/:id — update (toggle, change discount, note, etc.)
app.patch("/api/shop/admin/promo-codes/:id", requireAdmin, async (req, res, next) => {
  try {
    const codes = await readShopPromoCodes();
    const idx = codes.findIndex(c => c.id === req.params.id);
    if (idx < 0) return res.status(404).json({ error: "Не найден" });
    const c = codes[idx];
    const updated = { ...c };
    if (typeof req.body.active === "boolean") updated.active = req.body.active;
    if (req.body.discountPct !== undefined) updated.discountPct = Math.min(100, Math.max(1, Number(req.body.discountPct)));
    if (req.body.note !== undefined) updated.note = cleanText(req.body.note).slice(0, 200) || null;
    if (req.body.usageLimit !== undefined) updated.usageLimit = req.body.usageLimit ? Math.max(1, Number(req.body.usageLimit)) : null;
    if (req.body.expiresAt !== undefined) updated.expiresAt = req.body.expiresAt ? new Date(req.body.expiresAt).toISOString() : null;
    if (req.body.code !== undefined && !c.builtin) {
      const newCode = cleanText(req.body.code).toUpperCase().replace(/\s+/g, "");
      if (newCode.length >= 2 && newCode.length <= 32) {
        if (!codes.some((x, i) => i !== idx && x.code.toUpperCase() === newCode)) {
          updated.code = newCode;
        }
      }
    }
    codes[idx] = updated;
    await writeShopPromoCodes(codes);
    res.json({ ok: true, code: updated });
  } catch (e) { next(e); }
});

// DELETE /api/shop/admin/promo-codes/:id — only custom codes
app.delete("/api/shop/admin/promo-codes/:id", requireAdmin, async (req, res, next) => {
  try {
    const codes = await readShopPromoCodes();
    const found = codes.find(c => c.id === req.params.id);
    if (!found) return res.status(404).json({ error: "Не найден" });
    if (found.builtin) return res.status(400).json({ error: "Встроенный промокод нельзя удалить, только отключить" });
    await writeShopPromoCodes(codes.filter(c => c.id !== req.params.id));
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// GET /api/shop/admin/email-subscribers
app.get("/api/shop/admin/email-subscribers-list", requireAdmin, async (req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "БД недоступна" });
    const page = Math.max(1, Number(req.query.page || 1));
    const pageSize = 50;
    const source = req.query.source ? String(req.query.source) : null;
    const where = source ? { source, unsubscribed: false } : { unsubscribed: false };
    const [subscribers, total] = await Promise.all([
      prisma.shopEmailSubscriber.findMany({
        where,
        orderBy: { createdAt: "desc" },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
      prisma.shopEmailSubscriber.count({ where }),
    ]);
    res.json({ ok: true, subscribers, total });
  } catch (e) { next(e); }
});

// ── Shop description backfill (worker only) ─────────────────────────────────
// Shop products whose Ozon row has no description and whose Yandex Market twin has none
// either: fetch it from Ozon /v1/product/info/description and keep it on the Ozon row
// (raw.ozon.description — the same place the Avito / Market exports read from).
const SHOP_DESC_BACKFILL_BATCH = Math.max(0, Number(process.env.SHOP_DESCRIPTION_BACKFILL_BATCH ?? 200) || 0);
let _shopDescBackfillRunning = false;

async function backfillShopDescriptionsFromOzon({ limit = SHOP_DESC_BACKFILL_BATCH, delayMs = 150 } = {}) {
  if (_shopDescBackfillRunning || !(limit > 0)) return { status: "skipped" };
  const prisma = getPrisma();
  if (!prisma) return { status: "no_db" };
  _shopDescBackfillRunning = true;
  let updated = 0, failed = 0;
  try {
    const rows = await prisma.$queryRaw`
      SELECT w.offer_id, w.target, w.product_id
      FROM warehouse_products w
      WHERE w.marketplace = 'ozon' AND w.archived = false AND w.current_price > 0
        AND coalesce(length(w.raw#>>'{ozon,description}'), 0) < 40
        AND coalesce((w.raw#>>'{ozon,_shopDescChecked}')::boolean, false) = false
        AND NOT EXISTS (
          SELECT 1 FROM warehouse_products y
          WHERE y.marketplace = 'yandex' AND lower(y.offer_id) = lower(w.offer_id)
            AND coalesce(length(y.raw#>>'{yandex,description}'), 0) >= 40)
      ORDER BY w.updated_at DESC
      LIMIT ${limit}`;
    for (const r of rows) {
      const offerId = cleanText(r.offer_id);
      const target = cleanText(r.target) || "ozon";
      const account = typeof getOzonAccountByTarget === "function" ? getOzonAccountByTarget(target) : null;
      if (!account || !offerId) continue;
      let description = "";
      try {
        const body = r.product_id ? { product_id: Number(r.product_id) || undefined, offer_id: offerId } : { offer_id: offerId };
        const data = await ozonRequest("/v1/product/info/description", body, account);
        description = cleanText(data?.result?.description || "");
      } catch (error) {
        failed += 1;
        if (failed >= 10) break; // API trouble: stop this run
      }
      // remember "checked" so products without any description on Ozon aren't re-queried every run
      const patch = description ? { description } : { _shopDescChecked: true };
      await prisma.$executeRaw`
        UPDATE warehouse_products
        SET raw = coalesce(raw, '{}'::jsonb) || jsonb_build_object('ozon', coalesce(raw->'ozon', '{}'::jsonb) || ${JSON.stringify(patch)}::jsonb)
        WHERE marketplace = 'ozon' AND target = ${target} AND lower(offer_id) = lower(${offerId})`.catch(() => {});
      if (description) updated += 1;
      if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    logger.info("shop description backfill", { checked: rows.length, updated, failed });
    return { status: "ok", checked: rows.length, updated, failed };
  } finally {
    _shopDescBackfillRunning = false;
  }
}

if (backgroundJobsEnabled && SHOP_DESC_BACKFILL_BATCH > 0) {
  setTimeout(() => { backfillShopDescriptionsFromOzon().catch((e) => logger.warn("shop description backfill failed", { detail: e?.message })); }, 90 * 1000);
  setInterval(() => { backfillShopDescriptionsFromOzon().catch((e) => logger.warn("shop description backfill failed", { detail: e?.message })); }, 30 * 60 * 1000).unref?.();
}

// ── Marketplace-only images (infographic cards, branding slides) ───────────────
// The warehouse appends up to 2 "extra cards" + shop stub slides to every marketplace card.
// Marketplaces re-host them (each product gets its own URL), so they are recognised by a
// perceptual hash of the picture and hidden on magicvibes.ru.
const _mpOnlyHashCache = new Map(); // image url → 64-bit dHash (BigInt) | null
let _mpOnlyRefs = null;
let _mpOnlyRefsAt = 0;

async function _dHashFromBuffer(buf) {
  const { data } = await sharp(buf, { failOn: "none" }).flatten({ background: "#ffffff" }).grayscale()
    .resize(9, 8, { fit: "fill" }).raw().toBuffer({ resolveWithObject: true });
  let h = 0n;
  for (let y = 0; y < 8; y++) for (let x = 0; x < 8; x++) h = (h << 1n) | (data[y * 9 + x] > data[y * 9 + x + 1] ? 1n : 0n);
  return h;
}

async function _imageHash(url) {
  if (_mpOnlyHashCache.has(url)) return _mpOnlyHashCache.get(url);
  let hash = null;
  try {
    const local = typeof localPublicFilePathFromUrl === "function" ? localPublicFilePathFromUrl(url, null) : null;
    let buf = null;
    if (local && require("fs").existsSync(local)) buf = require("fs").readFileSync(local);
    else if (/^https?:\/\//.test(url)) {
      const r = await fetch(url, { signal: AbortSignal.timeout(6000) });
      if (r.ok) buf = Buffer.from(await r.arrayBuffer());
    }
    if (buf) hash = await _dHashFromBuffer(buf);
  } catch (_) { hash = null; }
  if (_mpOnlyHashCache.size > 60000) _mpOnlyHashCache.clear();
  _mpOnlyHashCache.set(url, hash);
  return hash;
}

// dHashes of slides found repeated at the end of hundreds of Ozon galleries (checked by eye,
// 2026-09-27): «поделитесь впечатлениями», «индивидуальное послание», «добавьте в избранное»
// (Magic Stick), two AURA slides, «спасибо, что выбираете нас», «секреты нанесения».
// Product photos repeated across volumes (e.g. Mancera) and brand slides are NOT in this list.
const SHOP_MP_ONLY_HASHES = [
  "71e8ccb296d4e870", "11e8d0f071cc4002", "84222b396d698775", "363079ccd4fcd0d3",
  "9233a8e8687931b2", "8e0b5d59193a35c4", "f0f0d6b6b217a6b2",
].map((h) => BigInt("0x" + h));

async function _mpOnlyReferenceHashes() {
  if (_mpOnlyRefs && Date.now() - _mpOnlyRefsAt < 60 * 60 * 1000) return _mpOnlyRefs;
  const settings = await readAppSettings();
  const urls = new Set();
  for (const mp of ["ozon", "yandex"]) {
    const b = typeof brandingForMarketplace === "function" ? brandingForMarketplace(settings, mp) : {};
    for (const c of Array.isArray(b.extraCards) ? b.extraCards : []) if (c?.url) urls.add(cleanText(c.url));
  }
  for (const entry of Object.values(settings?.shopStubs || {})) {
    for (const u of Array.isArray(entry?.stubUrls) ? entry.stubUrls : []) if (u) urls.add(cleanText(u));
  }
  const hashes = [...SHOP_MP_ONLY_HASHES];
  for (const u of urls) { const h = await _imageHash(u); if (h != null) hashes.push(h); }
  _mpOnlyRefs = hashes;
  _mpOnlyRefsAt = Date.now();
  return hashes;
}

function _hamming(a, b) { let x = a ^ b, n = 0; while (x) { n += Number(x & 1n); x >>= 1n; } return n; }

/** Drops marketplace-only slides from the tail of a product gallery (never the first photo). */
async function stripMarketplaceOnlyImages(images, { fetchMissing = true } = {}) {
  if (!Array.isArray(images) || images.length < 2) return images || [];
  const refs = await _mpOnlyReferenceHashes().catch(() => []);
  if (!refs.length) return images;
  const tailStart = Math.max(1, images.length - 4);
  const drop = new Set();
  await Promise.all(images.slice(tailStart).map(async (url, i) => {
    const h = fetchMissing ? await _imageHash(url) : (_mpOnlyHashCache.get(url) ?? null);
    if (h != null && refs.some((r) => _hamming(r, h) <= 8)) drop.add(tailStart + i);
  }));
  return images.filter((_, i) => !drop.has(i));
}

// Marketplace descriptions come with HTML (<br/>, <p>, <li>, &nbsp; — sometimes HTML-escaped twice).
// Plain text with paragraph breaks for the site / feed.
function shopCleanDescription(raw) {
  const decode = (s) => s
    .replace(/&nbsp;/gi, " ").replace(/&quot;/gi, '"').replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<").replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCodePoint(parseInt(n, 16)))
    .replace(/&amp;/gi, "&");
  let s = decode(decode(String(raw || ""))); // escaped twice on some cards
  s = s
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*li[^>]*>/gi, "\n• ")
    .replace(/<\/\s*(p|div|li|ul|ol|h[1-6]|tr)\s*>/gi, "\n")
    .replace(/<[^>]*>/g, "")
    .replace(/\r/g, "");
  return s
    .split("\n").map((l) => l.replace(/[ \t\u00a0]+/g, " ").trim()).join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/\n+• /g, "\n• ")
    .trim();
}

// ── IndexNow: tell Yandex (and IndexNow partners) about new / changed product pages ─────
// Key file: magicvibes.ru/d85bbe1d86b5212d7862d0c51bdd5e30.txt (shop-next/public). Full list was submitted once on
// 2026-09-27; this worker job sends products updated in the last 2 days every 6 hours.
const SHOP_INDEXNOW_KEY = process.env.SHOP_INDEXNOW_KEY || "d85bbe1d86b5212d7862d0c51bdd5e30";
const SHOP_INDEXNOW_HOST = "magicvibes.ru";

async function shopIndexNowSubmit(urls) {
  const list = [...new Set(urls)].filter(Boolean);
  const results = [];
  for (const ep of ["https://yandex.com/indexnow", "https://api.indexnow.org/indexnow"]) {
    for (let i = 0; i < list.length; i += 10000) {
      try {
        const r = await fetch(ep, {
          method: "POST",
          headers: { "Content-Type": "application/json; charset=utf-8" },
          body: JSON.stringify({ host: SHOP_INDEXNOW_HOST, key: SHOP_INDEXNOW_KEY, keyLocation: `https://${SHOP_INDEXNOW_HOST}/${SHOP_INDEXNOW_KEY}.txt`, urlList: list.slice(i, i + 10000) }),
          signal: AbortSignal.timeout(30000),
        });
        results.push({ ep, status: r.status });
      } catch (e) {
        results.push({ ep, error: e?.message || String(e) });
      }
    }
  }
  return { count: list.length, results };
}

async function shopIndexNowRecent() {
  const products = await getShopFeedProducts();
  const since = new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString().slice(0, 10);
  const urls = products.filter((p) => p.lastmod && p.lastmod >= since).map((p) => `https://${SHOP_INDEXNOW_HOST}/product/${p.slug}`);
  if (!urls.length) return { count: 0 };
  const res = await shopIndexNowSubmit(urls);
  logger.info("shop indexnow submitted", { count: res.count, results: res.results });
  return res;
}

if (backgroundJobsEnabled) {
  setInterval(() => { shopIndexNowRecent().catch((e) => logger.warn("shop indexnow failed", { detail: e?.message })); }, 6 * 60 * 60 * 1000).unref?.();
}
