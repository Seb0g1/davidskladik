// «Добавить на Ozon» со страницы «Фрагрантика».
//
//   GET  /api/fragrantica/ozon/form              — предзаполненная форма (атрибуты категории + значения)
//   GET  /api/fragrantica/ozon/attribute-values  — поиск по справочнику атрибута
//   POST /api/fragrantica/ozon/images            — задача: фото флакона + «Пирамида аромата» через parfumdeclaration
//   GET  /api/fragrantica/ozon/images/:jobId
//   POST /api/fragrantica/ozon/export            — создать карточку (/v3/product/import) или поставить в очередь лимита
//   GET  /api/fragrantica/ozon/exports[/:id]     — история / статус (pending опрашивается в /v1/product/import/info)
//   POST /api/fragrantica/ozon/exports/:id/retry
//
// Дневной лимит создания (/v4/product/info/limit) исчерпан → status=queued_limit, worker отправит после
// сброса (00:00 UTC = 03:00 МСК). После imported Ozon генерирует штрихкод (/v1/barcode/generate).
// Фото в конце карточки («Секреты нанесения», «Спасибо») добавляет parfumdeclaration сам, ежечасно.

const fragranticaExportQueueEnabled = process.env.FRAGRANTICA_EXPORT_QUEUE_ENABLED !== "false";
const fragranticaExportQueueIntervalMs = 5 * 60_000;
const fragranticaDictCache = new Map();
const fragranticaImageJobs = new Map();

function fragranticaOzonAccounts() {
  return getOzonAccounts().map((account) => ({
    id: cleanText(account.id),
    name: cleanText(account.name) || `Ozon ${cleanText(account.clientId)}`,
    clientId: cleanText(account.clientId),
    style: fragranticaCardStyleForAccount(account),
  }));
}

// Стиль картинки нот: AURA для её кабинета, иначе Magic Stick. FRAGRANTICA_CARD_STYLES='{"<clientId>":"aura"}'.
function fragranticaCardStyleForAccount(account = {}) {
  try {
    const map = JSON.parse(process.env.FRAGRANTICA_CARD_STYLES || "{}");
    const style = map[cleanText(account.clientId)] || map[cleanText(account.id)];
    if (style) return style;
  } catch {
    // ignore bad JSON — fall back to the name check
  }
  return /aura/i.test(cleanText(account.name)) || cleanText(account.clientId) === "2533393" ? "aura" : "magicstick";
}

// Магазины для галочек в форме: кабинеты Ozon + бизнес Яндекс Маркета. Подписи и стили картинки нот
// по clientId / businessId (FRAGRANTICA_SHOP_LABELS / FRAGRANTICA_CARD_STYLES — JSON для других).
const FRAG_DEFAULT_SHOP_LABELS = { "1304220": "Magic Stick", "2533393": "AURA", "171782339": "Parfumerius" };
const FRAG_CARD_STYLES = ["magicstick", "aura", "parfumerius"];

function fragranticaEnvMap(name) {
  try {
    const value = JSON.parse(process.env[name] || "{}");
    return value && typeof value === "object" ? value : {};
  } catch {
    return {};
  }
}

function fragranticaShopLabel(key, fallback) {
  return fragranticaEnvMap("FRAGRANTICA_SHOP_LABELS")[key] || FRAG_DEFAULT_SHOP_LABELS[key] || fallback;
}

function fragranticaYandexShops() {
  return uniqueYandexShopsByBusiness();
}

function fragranticaTargets() {
  const ozon = getOzonAccounts().map((account) => ({
    key: `ozon:${cleanText(account.id)}`,
    kind: "ozon",
    id: cleanText(account.id),
    marketplace: "Ozon",
    label: fragranticaShopLabel(cleanText(account.clientId), cleanText(account.name) || `Ozon ${cleanText(account.clientId)}`),
    style: fragranticaCardStyleForAccount(account),
  }));
  const yandex = fragranticaYandexShops().map((shop) => {
    const business = cleanText(shop.businessId);
    const style = fragranticaEnvMap("FRAGRANTICA_CARD_STYLES")[business] || "parfumerius";
    return {
      key: `yandex:${cleanText(shop.id)}`,
      kind: "yandex",
      id: cleanText(shop.id),
      marketplace: "Яндекс Маркет",
      label: fragranticaShopLabel(business, cleanText(shop.name) || `Маркет ${business}`),
      style: FRAG_CARD_STYLES.includes(style) ? style : "parfumerius",
    };
  });
  return [...ozon, ...yandex];
}

function fragranticaResolveOzonAccount(accountId) {
  const accounts = getOzonAccounts();
  const target = cleanText(accountId);
  const account = (target && accounts.find((a) => cleanText(a.id) === target || cleanText(a.clientId) === target)) || accounts[0];
  if (!account) {
    const error = new Error("Кабинет Ozon не найден. Добавьте его в настройках.");
    error.statusCode = 400;
    throw error;
  }
  return account;
}

async function fragranticaDictValues(account, typeId, attributeId) {
  const key = `${account.id}:${typeId}:${attributeId}`;
  const cached = fragranticaDictCache.get(key);
  if (cached && Date.now() - cached.at < 6 * 3_600_000) return cached.values;
  const values = await ozonGetAttributeDictValues(account, FRAG_OZON_CATEGORY_ID, typeId, attributeId);
  fragranticaDictCache.set(key, { at: Date.now(), values });
  return values;
}

// Dictionary searches repeat for every volume of a perfume (brand, country, TN VED) — cached 6 h
const fragranticaDictSearchCache = new Map();

async function fragranticaSearchDict(account, typeId, attributeId, value, limit = 20) {
  const query = cleanText(value);
  if (query.length < 2) return [];
  const key = `${cleanText(account?.id)}:${Number(typeId)}:${Number(attributeId)}:${query.toLowerCase()}:${limit}`;
  const cached = fragranticaDictSearchCache.get(key);
  if (cached && Date.now() - cached.at < 6 * 3_600_000) return cached.values;
  const data = await ozonRequest("/v1/description-category/attribute/values/search", {
    description_category_id: FRAG_OZON_CATEGORY_ID,
    type_id: Number(typeId),
    attribute_id: Number(attributeId),
    value: query,
    limit,
  }, account);
  const values = (data.result || []).map((v) => ({ id: Number(v.id), value: String(v.value || ""), info: v.info || "" }));
  if (fragranticaDictSearchCache.size > 5000) fragranticaDictSearchCache.clear();
  fragranticaDictSearchCache.set(key, { at: Date.now(), values });
  return values;
}

function fragBrandKey(text) {
  return String(text || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^0-9a-zа-яё]+/gi, "");
}

async function fragranticaFindBrand(account, typeId, brand) {
  const candidates = await fragranticaSearchDict(account, typeId, FRAG_OZON_ATTR.brand, brand, 20).catch(() => []);
  const key = fragBrandKey(brand);
  const exact = candidates.find((c) => fragBrandKey(c.value) === key);
  return { match: exact || null, candidates: candidates.slice(0, 10) };
}

function fragranticaTnvedCode(typeId) {
  try {
    const file = require("fs").readFileSync(path.join(dataDir, "ozon-tnved-assignments.json"), "utf8");
    const assignments = JSON.parse(file).assignments || [];
    const hit = assignments.find((a) => Number(a.descCatId) === FRAG_OZON_CATEGORY_ID && Number(a.typeId) === Number(typeId))
      || assignments.find((a) => Number(a.descCatId) === FRAG_OZON_CATEGORY_ID && !Number(a.typeId));
    if (hit?.tnvedCode) return cleanText(hit.tnvedCode);
  } catch {
    // no assignments file — default code
  }
  return FRAG_OZON_DEFAULTS.tnvedCode;
}

async function fragranticaFindTnved(account, typeId, volume) {
  const code = fragranticaTnvedCode(typeId);
  const values = await fragranticaSearchDict(account, typeId, FRAG_OZON_ATTR.tnved, code, 10).catch(() => []);
  const small = Number(volume) > 0 && Number(volume) <= 3;
  return values.find((v) => v.value.startsWith(code) && /до 3 мл/i.test(v.value) === small) || values[0] || null;
}

// ─── Fragella: notes, accords and the bottle when Fragrantica gives us no page ───
// Fragrantica answers our server with Cloudflare 403 — no new perfume pages. Fragella (api.fragella.com, a paid
// fragrance API with a key, FRAGELLA_API_KEY) has the same facts: the note pyramid, accords, gender, year and a
// bottle photo. A perfume without a page is looked up there by brand + name (checked with the perfume parser),
// and the result is stored as its detail in the Fragrantica shape — the conveyor, pyramid cards and the AI
// description work unchanged. Note / accord names come in English and are translated once (fragella_ru cache).

const FRAGELLA_API = "https://api.fragella.com/api/v1";
const FRAGELLA_SHARE = { dominant: 100, prominent: 80, moderate: 60, subtle: 40 };
let fragellaChain = Promise.resolve();
let fragellaTablesReady = false;
const fragellaAccordColors = { at: 0, map: new Map() };

function fragellaKey() {
  return cleanText(process.env.FRAGELLA_API_KEY);
}

// The plan's monthly quota (free: 20 requests) runs out → Fragella rests a day and perfumes without a page wait
// for it (or for Fragrantica) instead of failing as «not found».
let fragellaRestUntil = 0;
function fragellaAvailable() {
  return Boolean(fragellaKey()) && Date.now() >= fragellaRestUntil;
}

// the plan allows 60 requests a minute — one at a time, 1.1 s apart, for every caller of the process
function fragellaRequest(pathname) {
  const run = fragellaChain.then(async () => {
    const response = await fetch(`${FRAGELLA_API}${pathname}`, { headers: { "x-api-key": fragellaKey() }, signal: AbortSignal.timeout(20_000) });
    await new Promise((resolve) => setTimeout(resolve, 1100));
    if (!response.ok) {
      const body = await response.text().catch(() => "");
      if (response.status === 429 && /quota/i.test(body)) {
        fragellaRestUntil = Date.now() + 24 * 3_600_000;
        logger.warn("fragella monthly quota exhausted", { detail: body.slice(0, 200) });
      }
      throw Object.assign(new Error(`Fragella ответила ${response.status}${/quota/i.test(body) ? " (месячный лимит тарифа исчерпан)" : ""}`), { statusCode: response.status });
    }
    return response.json();
  });
  fragellaChain = run.catch(() => {});
  return run;
}

async function requireFragellaTables() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("PostgreSQL недоступен.");
  if (!fragellaTablesReady) {
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS fragella_ru (kind TEXT NOT NULL, en TEXT NOT NULL, ru TEXT NOT NULL, PRIMARY KEY (kind, en))`);
    fragellaTablesReady = true;
  }
  return prisma;
}

/** English note / accord names → Russian, the way Russian Fragrantica writes them; cached for good. */
async function fragellaTranslate(kind, names = []) {
  const prisma = await requireFragellaTables();
  const list = [...new Set(names.map((name) => cleanText(name)).filter(Boolean))];
  const out = new Map();
  if (!list.length) return out;
  const known = await prisma.$queryRawUnsafe(`SELECT en, ru FROM fragella_ru WHERE kind = $1 AND en = ANY($2::text[])`, kind, list);
  for (const row of known) out.set(row.en, row.ru);
  const missing = list.filter((name) => !out.has(name));
  if (!missing.length) return out;
  const examples = kind === "note"
    ? "Bergamot → Бергамот, Calabrian bergamot → Калабрийский бергамот, Pink Pepper → Розовый перец, Ambroxan → Амброксан, Tonka Bean → Бобы тонка"
    : "fresh spicy → свежий пряный, amber → амбровый, woody → древесный, citrus → цитрусовый, aromatic → ароматический, white floral → белые цветы";
  const { data } = await createTextAiJson([
    { role: "system", content: `Ты переводишь названия ${kind === "note" ? "парфюмерных нот" : "аккордов аромата"} на русский язык так, как их пишет русская Фрагрантика. Примеры: ${examples}. Только перевод, ничего не добавляй.` },
    { role: "user", content: `Верни JSON-объект {"английское название": "русское название"} для каждого: ${JSON.stringify(missing)}` },
  ], { temperature: 0 });
  for (const name of missing) {
    let ru = cleanText(data?.[name]);
    if (!ru) continue;
    // Fragrantica writes «е» («теплый пряный») — «ё» would miss the accord colours and the note icons
    ru = ru.replace(/ё/g, "е").replace(/Ё/g, "Е");
    ru = kind === "note" ? ru.charAt(0).toUpperCase() + ru.slice(1) : ru.toLowerCase();
    out.set(name, ru);
    await prisma.$executeRawUnsafe(`INSERT INTO fragella_ru (kind, en, ru) VALUES ($1, $2, $3) ON CONFLICT (kind, en) DO UPDATE SET ru = EXCLUDED.ru`, kind, name, ru);
  }
  return out;
}

/** Accord colours as Fragrantica paints them (from the pages we have), by the Russian accord name. */
async function fragellaAccordPalette() {
  if (fragellaAccordColors.map.size && Date.now() - fragellaAccordColors.at < 6 * 3_600_000) return fragellaAccordColors.map;
  const prisma = getPrisma();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT DISTINCT ON (a->>'name') a->>'name' AS name, a->>'color' AS color, a->>'background' AS background
       FROM fragrantica_perfumes p, jsonb_array_elements(coalesce(p.detail->'accords', '[]'::jsonb)) a
      WHERE p.detail_at IS NOT NULL AND coalesce(p.detail->>'source', '') <> 'fragella'`,
  ).catch(() => []);
  fragellaAccordColors.map = new Map(rows.map((row) => [cleanText(row.name).toLowerCase(), { color: row.color || "#000000", background: row.background || "#d9d9d9" }]));
  fragellaAccordColors.at = Date.now();
  return fragellaAccordColors.map;
}

// Note icons we already render («Бергамот» looks the same on every card): Russian note name → icon, from the
// pages we have; Fragella's own picture only for notes we have never shown.
const fragellaNoteIconCache = { at: 0, map: new Map() };
async function fragellaKnownNoteIcons() {
  if (fragellaNoteIconCache.map.size && Date.now() - fragellaNoteIconCache.at < 6 * 3_600_000) return fragellaNoteIconCache.map;
  const rows = await getPrisma().$queryRawUnsafe(
    `SELECT DISTINCT ON (lower(n->>'name')) lower(n->>'name') AS name, n->>'icon' AS icon
       FROM fragrantica_perfumes p,
            jsonb_array_elements(coalesce(p.detail->'notes'->'top', '[]'::jsonb) || coalesce(p.detail->'notes'->'middle', '[]'::jsonb) || coalesce(p.detail->'notes'->'base', '[]'::jsonb)) n
      WHERE p.detail_at IS NOT NULL AND coalesce(p.detail->>'source', '') <> 'fragella' AND coalesce(n->>'icon', '') <> ''`,
  ).catch(() => []);
  fragellaNoteIconCache.map = new Map(rows.map((row) => [row.name, row.icon]));
  fragellaNoteIconCache.at = Date.now();
  return fragellaNoteIconCache.map;
}

/** The Fragella entry for our perfume: same brand, same name words, same gender — or null. */
async function fragellaFindPerfume(row) {
  const parser = require("./lib/perfume-match");
  const prisma = getPrisma();
  const brands = await supplierMatchBrands(prisma).catch(() => null);
  const results = await fragellaRequest(`/fragrances?search=${encodeURIComponent(`${row.brand} ${row.name}`)}&limit=10`);
  const genderWord = { male: "men", female: "women", unisex: "unisex" };
  const want = parser.parsePerfumeName(`${row.brand} ${row.name} ${genderWord[row.gender] || ""} 100 ml`, { brands });
  const brandWords = new Set(parser.tokensOf(`${row.brand}`));
  for (const item of Array.isArray(results) ? results : []) {
    // «Dior Sauvage» by «Christian Dior»: the brand words lead the name — keep only the perfume's own words
    const own = parser.tokensOf(item.Brand || "");
    // «Apres l'Ondee for women»: Fragella's «for women / for men» tail is not part of the name
    const words = parser.tokensOf(String(item.Name || "").replace(/\s+for\s+(women|men|him|her)(\s+and\s+(women|men))?\s*$/i, ""));
    while (words.length && (brandWords.has(words[0]) || own.includes(words[0]))) words.shift();
    const gender = genderWord[{ men: "male", women: "female", unisex: "unisex" }[cleanText(item.Gender).toLowerCase()]] || "";
    const candidate = parser.parsePerfumeName(`${row.brand} ${words.join(" ")} ${gender} 100 ml`, { brands });
    const result = parser.comparePerfumes(want, candidate);
    if (result.ok && !result.probable) return item;
  }
  return null;
}

/** Looks the perfume up in Fragella and stores it as its detail. → true when stored. */
async function fillPerfumeFromFragella(row) {
  if (!fragellaKey() || !row) return false;
  const item = await fragellaFindPerfume(row);
  if (!item) {
    logger.info("fragella perfume not found", { id: Number(row.id), brand: row.brand, name: row.name });
    return false;
  }
  const levels = { top: item.Notes?.Top || [], middle: item.Notes?.Middle || [], base: item.Notes?.Base || [] };
  // only a flat list («General Notes»): Fragrantica shows such perfumes with one tier
  if (!levels.top.length && !levels.middle.length && !levels.base.length) {
    levels.flat = (item["General Notes"] || []).map((name) => ({ name, imageUrl: "" }));
  }
  const noteNames = Object.values(levels).flat().map((note) => note.name);
  if (!noteNames.length) return false;
  const accordNames = (item["Main Accords"] || []).slice(0, 10);
  const [notesRu, accordsRu, palette, knownIcons] = await Promise.all([fragellaTranslate("note", noteNames), fragellaTranslate("accord", accordNames), fragellaAccordPalette(), fragellaKnownNoteIcons()]);
  if (noteNames.some((name) => !notesRu.has(cleanText(name)))) throw new Error("Перевод нот не удался.");
  const notes = Object.fromEntries(Object.entries(levels).map(([level, list]) => [level, list.map((note) => {
    const name = notesRu.get(cleanText(note.name));
    return { name, icon: knownIcons.get(name.toLowerCase()) || cleanText(note.imageUrl) };
  })]));
  const accords = accordNames.map((name) => {
    const ru = accordsRu.get(cleanText(name)) || cleanText(name);
    const colors = palette.get(ru.toLowerCase()) || { color: "#000000", background: "#d9d9d9" };
    const share = FRAGELLA_SHARE[cleanText(item["Main Accords Percentage"]?.[name]).toLowerCase()] || 50;
    return { name: ru, share, ...colors };
  });
  const gender = { men: "male", women: "female", unisex: "unisex" }[cleanText(item.Gender).toLowerCase()] || row.gender || "";
  const year = Number(item.Year) || Number(row.year) || null;
  const title = `${row.name} ${row.brand}`;
  const tierText = [["Верхние ноты", notes.top], ["средние ноты", notes.middle], ["базовые ноты", notes.base], ["ноты", notes.flat]]
    .filter(([, list]) => list && list.length).map(([label, list]) => `${label}: ${list.map((n) => n.name).join(", ")}`).join("; ");
  const forWhom = { male: "для мужчин", female: "для женщин", unisex: "для мужчин и женщин" }[gender] || "";
  const detail = {
    id: Number(row.id), url: row.url, name: row.name, year, brand: row.brand,
    image: cleanText(item["Image URL"]), notes, title, votes: null, family: "", gender,
    rating: Number(item.rating) || null, accords, brandSlug: row.brand_slug || "", perfumers: [],
    description: `${title} — это аромат${forWhom ? ` ${forWhom}` : ""}.${year ? ` ${row.name} выпущен в ${year} году.` : ""} ${tierText}.`,
    source: "fragella", fragellaId: cleanText(item._id),
  };
  // the bottle: Fragella's photo in place of the Fragrantica thumbnail (no Fragrantica «HD original» lookup)
  if (detail.image) {
    try {
      await downloadFragranticaMedia(detail.image, "images", `${Number(row.id)}.jpg`);
      await fragFs.promises.writeFile(fragranticaMediaPath("images", `${Number(row.id)}-hd.none`), "fragella");
    } catch (error) {
      logger.warn("fragella bottle image failed", { id: Number(row.id), detail: error?.message });
    }
  }
  await getPrisma().$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail = $2::jsonb, detail_at = now(), detail_error = NULL,
       gender = CASE WHEN coalesce(gender, '') = '' THEN $3 ELSE gender END, year = coalesce(year, $4), updated_at = now()
     WHERE id = $1`,
    Number(row.id), JSON.stringify(detail), gender, year,
  );
  logger.info("fragella perfume stored", { id: Number(row.id), brand: row.brand, name: row.name, fragellaId: detail.fragellaId, notes: noteNames.length });
  return true;
}

// ─── Parfumetrika: open Russian perfume base (parfumetrika.ru, robots «Allow: /») ───
// Notes per tier, accords with colours, gender and year — in Russian. One polite request at a time (3 s apart,
// an honest User-Agent). The perfume page is found by its address (brand-name slug) or, failing that, in the
// brand's list (pages of 20, remembered in parfumetrika_slugs). Bottle photos there are user uploads — not taken.

const PARFUMETRIKA_BASE = "https://parfumetrika.ru";
const PARFUMETRIKA_UA = "MagicVibesCatalogBot/1.0 (+https://davidsklad.ru; notes lookup, 1 request / 3 s)";
let parfumetrikaChain = Promise.resolve();
let parfumetrikaTablesReady = false;

function parfumetrikaRequest(pathname) {
  const run = parfumetrikaChain.then(async () => {
    const response = await fetch(`${PARFUMETRIKA_BASE}${pathname}`, { headers: { "User-Agent": PARFUMETRIKA_UA, "Accept-Language": "ru" }, signal: AbortSignal.timeout(25_000) });
    await new Promise((resolve) => setTimeout(resolve, 3000));
    if (response.status === 404) return null;
    if (!response.ok) throw Object.assign(new Error(`Parfumetrika ответила ${response.status}`), { statusCode: response.status });
    return response.text();
  });
  parfumetrikaChain = run.catch(() => {});
  return run;
}

async function requireParfumetrikaTables() {
  const prisma = getPrisma();
  if (!parfumetrikaTablesReady) {
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS parfumetrika_slugs (brand TEXT NOT NULL, slug TEXT NOT NULL, page INTEGER, PRIMARY KEY (brand, slug))`);
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS parfumetrika_brands (brand TEXT PRIMARY KEY, pages INTEGER, scanned_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
    parfumetrikaTablesReady = true;
  }
  return prisma;
}

function parfumetrikaSlug(text) {
  return String(text || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase()
    .replace(/&/g, " ").replace(/['’`]/g, "-").replace(/[^a-z0-9]+/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
}

/** Perfume slugs of a brand: from the cache, else its list pages (scanned once a month). */
async function parfumetrikaBrandSlugs(brand) {
  const prisma = await requireParfumetrikaTables();
  const key = cleanText(brand).toLowerCase();
  const [seen] = await prisma.$queryRawUnsafe(`SELECT pages FROM parfumetrika_brands WHERE brand = $1 AND scanned_at > now() - interval '30 days'`, key);
  if (!seen) {
    const brandPath = `/brands/${encodeURIComponent(cleanText(brand).replace(/\s+/g, "_"))}`;
    let pages = 0;
    for (let page = 1; page <= 60; page += 1) {
      const html = await parfumetrikaRequest(page === 1 ? brandPath : `${brandPath}?page=${page}`);
      if (!html) break;
      const slugs = [...new Set([...html.matchAll(/href="\/perfumes\/([^"#?]+)"/g)].map((m) => m[1]))];
      if (!slugs.length) break;
      pages = page;
      for (const slug of slugs) {
        await prisma.$executeRawUnsafe(`INSERT INTO parfumetrika_slugs (brand, slug, page) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, key, slug, page);
      }
      if (!new RegExp(`[?&]page=${page + 1}\\b`).test(html)) break;
    }
    await prisma.$executeRawUnsafe(
      `INSERT INTO parfumetrika_brands (brand, pages, scanned_at) VALUES ($1, $2, now()) ON CONFLICT (brand) DO UPDATE SET pages = EXCLUDED.pages, scanned_at = now()`, key, pages,
    );
  }
  return (await prisma.$queryRawUnsafe(`SELECT slug FROM parfumetrika_slugs WHERE brand = $1`, key)).map((r) => r.slug);
}

/**
 * The page's own facts from its schema.org Product block (the page also carries other perfumes' data — only this
 * block is about the perfume itself): name, brand, year, gender (from its description), tiers, accords; note icons
 * and accord colours from the markup.
 */
function parseParfumetrikaPage(html) {
  const decode = (text) => String(text || "").replace(/&amp;/g, "&").replace(/&#x27;|&#39;/g, "'").replace(/&quot;/g, '"');
  let product = null;
  for (const m of html.matchAll(/<script[^>]*application\/ld\+json[^>]*>([\s\S]*?)<\/script>/g)) {
    try {
      const data = JSON.parse(m[1]);
      if (data?.["@type"] === "Product") { product = data; break; }
    } catch {
      // another block
    }
  }
  if (!product) return null;
  const props = new Map((product.additionalProperty || []).map((p) => [cleanText(p.name).toLowerCase(), String(p.value || "").split(",").map((v) => cleanText(v)).filter(Boolean)]));
  const prop = (...names) => names.map((n) => props.get(n)).find((v) => v && v.length) || [];
  const icons = new Map([...html.matchAll(/src="([^"]+note_images[^"]+)"[^>]+alt="Нота ([^"]+)"/g)].map((m) => [decode(m[2]).toLowerCase(), m[1]]));
  const colors = new Map([...html.matchAll(/background-color:(#[0-9a-fA-F]{3,8});border-radius:12px;padding:4px 12px;font-size:14px;color:(#[0-9a-fA-F]{3,8})[^>]*>([^<]+)</g)]
    .map((m) => [decode(m[3]).trim().toLowerCase(), { background: m[1], color: m[2] }]));
  const description = cleanText(product.description);
  const gender = /женщин и мужчин|мужчин и женщин|унисекс/i.test(description) ? "unisex"
    : /мужск|для мужчин/i.test(description) ? "male" : /женск|для женщин/i.test(description) ? "female" : "";
  return {
    name: decode(product.name), brand: decode(product.brand?.name), year: Number(product.releaseDate) || null, gender,
    top: prop("топ ноты", "верхние ноты"), middle: prop("средние ноты", "ноты сердца"), base: prop("базовые ноты"), flat: prop("ноты"),
    accords: prop("аккорды"), icons, colors,
  };
}

/** Looks the perfume up on Parfumetrika and stores it as its detail. → true when stored. */
async function fillPerfumeFromParfumetrika(row) {
  const parser = require("./lib/perfume-match");
  const brands = await supplierMatchBrands(getPrisma()).catch(() => null);
  const guess = parfumetrikaSlug(`${row.brand} ${row.name}`);
  const genderWord = { male: "men", female: "women", unisex: "unisex" };
  const want = parser.parsePerfumeName(`${row.brand} ${row.name} ${genderWord[row.gender] || ""} 100 ml`, { brands });
  const fits = (page) => {
    if (!page || !(page.top.length + page.middle.length + page.base.length + page.flat.length)) return false;
    const candidate = parser.parsePerfumeName(`${page.brand || row.brand} ${page.name} ${genderWord[page.gender] || ""} 100 ml`, { brands });
    const result = parser.comparePerfumes(want, candidate);
    return result.ok && !result.probable;
  };
  let html = await parfumetrikaRequest(`/perfumes/${guess}`);
  let page = html ? parseParfumetrikaPage(html) : null;
  if (!fits(page)) {
    page = null;
    const slugs = await parfumetrikaBrandSlugs(row.brand);
    // «guerlain-l-homme-ideal-72724»: the same words plus a number
    const candidates = slugs.filter((slug) => slug === guess || new RegExp(`^${guess}-\\d+$`).test(slug));
    for (const slug of candidates.slice(0, 3)) {
      html = await parfumetrikaRequest(`/perfumes/${slug}`);
      const parsed = html ? parseParfumetrikaPage(html) : null;
      if (fits(parsed)) { page = parsed; break; }
    }
  }
  if (!page) {
    logger.info("parfumetrika perfume not found", { id: Number(row.id), brand: row.brand, name: row.name });
    return false;
  }
  const knownIcons = await fragellaKnownNoteIcons().catch(() => new Map());
  const palette = await fragellaAccordPalette().catch(() => new Map());
  const note = (name) => {
    const ru = cleanText(name).replace(/ё/g, "е");
    return { name: ru, icon: knownIcons.get(ru.toLowerCase()) || page.icons.get(ru.toLowerCase()) || "" };
  };
  const notes = { top: page.top.map(note), middle: page.middle.map(note), base: page.base.map(note) };
  if (!notes.top.length && !notes.middle.length && !notes.base.length) notes.flat = page.flat.map(note);
  const accords = page.accords.slice(0, 10).map((name, i) => {
    const ru = name.toLowerCase().replace(/ё/g, "е");
    return { name: ru, share: Math.max(40, 100 - i * 10), ...(palette.get(ru) || page.colors.get(ru) || { color: "#000000", background: "#d9d9d9" }) };
  });
  const gender = row.gender || page.gender || "";
  const year = Number(row.year) || page.year || null;
  const title = `${row.name} ${row.brand}`;
  const tierText = [["Верхние ноты", notes.top], ["средние ноты", notes.middle], ["базовые ноты", notes.base], ["ноты", notes.flat]]
    .filter(([, list]) => list && list.length).map(([label, list]) => `${label}: ${list.map((n) => n.name).join(", ")}`).join("; ");
  const forWhom = { male: "для мужчин", female: "для женщин", unisex: "для мужчин и женщин" }[gender] || "";
  const detail = {
    id: Number(row.id), url: row.url, name: row.name, year, brand: row.brand, image: "", notes, title, votes: null, family: "", gender,
    rating: null, accords, brandSlug: row.brand_slug || "", perfumers: [],
    description: `${title} — это аромат${forWhom ? ` ${forWhom}` : ""}.${year ? ` ${row.name} выпущен в ${year} году.` : ""} ${tierText}.`,
    source: "parfumetrika",
  };
  await getPrisma().$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail = $2::jsonb, detail_at = now(), detail_error = NULL,
       gender = CASE WHEN coalesce(gender, '') = '' THEN $3 ELSE gender END, year = coalesce(year, $4), updated_at = now()
     WHERE id = $1`,
    Number(row.id), JSON.stringify(detail), gender, year,
  );
  logger.info("parfumetrika perfume stored", { id: Number(row.id), brand: row.brand, name: row.name, notes: Object.values(notes).flat().length });
  return true;
}

// ─── Aromo (aromo.ru — our own site, all use allowed by its owner) ───────────────────────────────────────
// Russian pyramid (top / middle / bottom), families, gender, year. The page carries its data in window.__NUXT__
// (read in a vm sandbox). Without an API key only the brand page's first 40 perfumes (most popular) are listed;
// they are remembered in aromo_items. One request at a time, 3 s apart.

const AROMO_BASE = "https://aromo.ru";
let aromoChain = Promise.resolve();
let aromoTablesReady = false;
const AROMO_GROUP_ACCORD = {
  "цветочные": "цветочный", "древесные": "древесный", "пряные": "пряный", "восточные": "восточный", "фруктовые": "фруктовый",
  "цитрусовые": "цитрусовый", "гурманские": "гурманский", "фужерные": "фужерный", "шипровые": "шипровый", "кожаные": "кожаный",
  "зеленые": "зеленый", "зелёные": "зеленый", "водяные": "водяной", "акватические": "акватический", "мускусные": "мускусный",
  "пудровые": "пудровый", "свежие": "свежий", "сладкие": "сладкий", "травяные": "травяной", "смолистые": "смолистый", "табачные": "табачный",
};

function aromoRequest(pathname) {
  const run = aromoChain.then(async () => {
    const response = await fetch(`${AROMO_BASE}${pathname}`, { headers: { "User-Agent": PARFUMETRIKA_UA, "Accept-Language": "ru" }, signal: AbortSignal.timeout(30_000) });
    await new Promise((resolve) => setTimeout(resolve, 3000));
    if (response.status === 404) return null;
    if (!response.ok) throw Object.assign(new Error(`Aromo ответила ${response.status}`), { statusCode: response.status });
    return response.text();
  });
  aromoChain = run.catch(() => {});
  return run;
}

/** window.__NUXT__ of a page, evaluated in an empty sandbox (it is a data function, nothing else). */
function aromoNuxt(html) {
  const m = String(html || "").match(/<script>window\.__NUXT__=([\s\S]*?)<\/script>/);
  if (!m) return null;
  try {
    const sandbox = { window: {} };
    require("vm").runInNewContext(`window.__NUXT__=${m[1]}`, sandbox, { timeout: 2000 });
    return sandbox.window.__NUXT__ || null;
  } catch {
    return null;
  }
}

async function requireAromoTables() {
  const prisma = getPrisma();
  if (!aromoTablesReady) {
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS aromo_items (brand TEXT NOT NULL, code TEXT NOT NULL, name TEXT NOT NULL, concentration TEXT, PRIMARY KEY (brand, code))`);
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS aromo_brands (brand TEXT PRIMARY KEY, found BOOLEAN, scanned_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
    aromoTablesReady = true;
  }
  return prisma;
}

/** The brand's listed perfumes (first page — the most popular), remembered for a month. */
async function aromoBrandItems(brand) {
  const prisma = await requireAromoTables();
  const key = cleanText(brand).toLowerCase();
  const [seen] = await prisma.$queryRawUnsafe(`SELECT found FROM aromo_brands WHERE brand = $1 AND scanned_at > now() - interval '30 days'`, key);
  if (!seen) {
    const html = await aromoRequest(`/brands/${parfumetrikaSlug(brand)}/`);
    const items = aromoNuxt(html)?.data?.[0]?.catalogItems || [];
    for (const item of items) {
      if (!item?.code || !item?.name) continue;
      await prisma.$executeRawUnsafe(
        `INSERT INTO aromo_items (brand, code, name, concentration) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
        key, item.code, item.name, cleanText(item.concentration?.code),
      );
    }
    await prisma.$executeRawUnsafe(
      `INSERT INTO aromo_brands (brand, found, scanned_at) VALUES ($1, $2, now()) ON CONFLICT (brand) DO UPDATE SET found = EXCLUDED.found, scanned_at = now()`, key, items.length > 0,
    );
  }
  return prisma.$queryRawUnsafe(`SELECT code, name, concentration FROM aromo_items WHERE brand = $1`, key);
}

/** Looks the perfume up on Aromo and stores it as its detail. → true when stored. */
async function fillPerfumeFromAromo(row) {
  const parser = require("./lib/perfume-match");
  const brands = await supplierMatchBrands(getPrisma()).catch(() => null);
  const genderWord = { male: "men", female: "women", unisex: "unisex" };
  const want = parser.parsePerfumeName(`${row.brand} ${row.name} ${genderWord[row.gender] || ""} 100 ml`, { brands });
  const items = await aromoBrandItems(row.brand);
  // the same name; one concentration of it is enough (its pyramid is the perfume's)
  const matches = items.filter((item) => {
    const candidate = parser.parsePerfumeName(`${row.brand} ${item.name} 100 ml`, { brands });
    const result = parser.comparePerfumes({ ...want, gender: "" }, { ...candidate, gender: "" });
    return result.ok && !result.probable;
  });
  let perfume = null;
  for (const item of matches.slice(0, 2)) {
    const page = aromoNuxt(await aromoRequest(`/fragrance/${item.code}/`))?.data?.[0]?.perfume;
    if (!page?.olfactoryPyramid) continue;
    const gender = { female: "female", male: "male", unisex: "unisex" }[page.gender?.code] || "";
    if (row.gender && gender && row.gender !== gender && gender !== "unisex" && row.gender !== "unisex") continue;
    perfume = page;
    break;
  }
  if (!perfume) {
    logger.info("aromo perfume not found", { id: Number(row.id), brand: row.brand, name: row.name, listed: items.length });
    return false;
  }
  const knownIcons = await fragellaKnownNoteIcons().catch(() => new Map());
  const palette = await fragellaAccordPalette().catch(() => new Map());
  const note = (n) => {
    const name = cleanText(n?.name).replace(/ё/g, "е");
    return { name, icon: knownIcons.get(name.toLowerCase()) || cleanText(n?.picture?.url) };
  };
  const pyramid = perfume.olfactoryPyramid || {};
  const notes = { top: (pyramid.top || []).map(note), middle: (pyramid.middle || []).map(note), base: (pyramid.bottom || pyramid.base || []).map(note) };
  if (!notes.top.length && !notes.middle.length && !notes.base.length) return false;
  const accords = (perfume.groups || []).slice(0, 8).map((g, i) => {
    const name = AROMO_GROUP_ACCORD[cleanText(g.name).toLowerCase()] || cleanText(g.name).toLowerCase();
    return { name, share: Math.max(40, 100 - i * 15), ...(palette.get(name) || { color: "#000000", background: "#d9d9d9" }) };
  });
  const gender = row.gender || { female: "female", male: "male", unisex: "unisex" }[perfume.gender?.code] || "";
  const year = Number(row.year) || Number(perfume.produced?.from) || null;
  const title = `${row.name} ${row.brand}`;
  const tierText = [["Верхние ноты", notes.top], ["средние ноты", notes.middle], ["базовые ноты", notes.base]]
    .filter(([, list]) => list.length).map(([label, list]) => `${label}: ${list.map((n) => n.name).join(", ")}`).join("; ");
  const forWhom = { male: "для мужчин", female: "для женщин", unisex: "для мужчин и женщин" }[gender] || "";
  const detail = {
    id: Number(row.id), url: row.url, name: row.name, year, brand: row.brand, image: "", notes, title, votes: null, family: "", gender,
    rating: null, accords, brandSlug: row.brand_slug || "", perfumers: [],
    description: `${title} — это аромат${forWhom ? ` ${forWhom}` : ""}.${year ? ` ${row.name} выпущен в ${year} году.` : ""} ${tierText}.`,
    source: "aromo",
  };
  await getPrisma().$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail = $2::jsonb, detail_at = now(), detail_error = NULL,
       gender = CASE WHEN coalesce(gender, '') = '' THEN $3 ELSE gender END, year = coalesce(year, $4), updated_at = now()
     WHERE id = $1`,
    Number(row.id), JSON.stringify(detail), gender, year,
  );
  logger.info("aromo perfume stored", { id: Number(row.id), brand: row.brand, name: row.name });
  return true;
}

// ─── Parfumo: second open source after Parfumetrika (parfumo.com, perfume pages allowed in robots.txt) ───
// Its search is closed to robots, so perfume addresses come from its sitemaps (downloaded once a month into
// parfumo_urls). The page gives the pyramid (Top / Heart / Base), accords with colours, gender and year — in
// English: accords map through a fixed dictionary, notes through the cached translation (fragella_ru; the text
// AI translates a note once). One polite request at a time, 3 s apart. Bottle photos are not taken.

const PARFUMO_BASE = "https://www.parfumo.com";
let parfumoChain = Promise.resolve();
let parfumoTablesReady = false;
let parfumoIndexPromise = null;
const PARFUMO_ACCORDS_RU = {
  floral: "цветочный", oriental: "восточный", spicy: "пряный", woody: "древесный", fresh: "свежий", citrus: "цитрусовый",
  sweet: "сладкий", powdery: "пудровый", green: "зеленый", fruity: "фруктовый", gourmand: "гурманский", aquatic: "акватический",
  leathery: "кожаный", leather: "кожаный", smoky: "дымный", resinous: "смолистый", animal: "животный", animalic: "животный",
  chypre: "шипровый", fougere: "фужерный", "fougère": "фужерный", creamy: "кремовый", synthetic: "синтетический", earthy: "землистый",
  aromatic: "ароматический", herbal: "травяной", musky: "мускусный", balsamic: "бальзамический", tobacco: "табачный", boozy: "алкогольный",
};

function parfumoRequest(pathname) {
  const run = parfumoChain.then(async () => {
    const response = await fetch(pathname.startsWith("http") ? pathname : `${PARFUMO_BASE}${pathname}`, {
      headers: { "User-Agent": PARFUMETRIKA_UA, "Accept-Language": "en" }, signal: AbortSignal.timeout(60_000), redirect: "follow",
    });
    await new Promise((resolve) => setTimeout(resolve, 3000));
    if (response.status === 404) return null;
    if (!response.ok) throw Object.assign(new Error(`Parfumo ответила ${response.status}`), { statusCode: response.status });
    return pathname.endsWith(".gz") ? Buffer.from(await response.arrayBuffer()) : response.text();
  });
  parfumoChain = run.catch(() => {});
  return run;
}

async function requireParfumoTables() {
  const prisma = getPrisma();
  if (!parfumoTablesReady) {
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS parfumo_urls (path TEXT PRIMARY KEY, brand_key TEXT NOT NULL, name_key TEXT NOT NULL)`);
    await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS parfumo_urls_brand_idx ON parfumo_urls (brand_key)`);
    await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS parfumo_state (key TEXT PRIMARY KEY, value JSONB, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
    parfumoTablesReady = true;
  }
  return prisma;
}

function parfumoKey(text) {
  return String(text || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/&/g, " ").replace(/[^a-z0-9]+/g, "");
}

/** Perfume addresses from Parfumo's sitemaps, refreshed once a month (one download at a time). */
async function ensureParfumoIndex() {
  const prisma = await requireParfumoTables();
  const [state] = await prisma.$queryRawUnsafe(`SELECT updated_at FROM parfumo_state WHERE key = 'index' AND updated_at > now() - interval '30 days'`);
  if (state) return;
  if (!parfumoIndexPromise) {
    parfumoIndexPromise = (async () => {
      const zlib = require("zlib");
      const root = await parfumoRequest("/sitemap_en.xml");
      const files = [...String(root || "").matchAll(/<loc>([^<]+perfum[^<]+\.xml(?:\.gz)?)<\/loc>/g)].map((m) => m[1]);
      let total = 0;
      for (const file of files) {
        const body = await parfumoRequest(file);
        if (!body) continue;
        const xml = Buffer.isBuffer(body) ? zlib.gunzipSync(body).toString("utf8") : String(body);
        const rows = [];
        for (const m of xml.matchAll(/<loc>https:\/\/www\.parfumo\.com(\/Perfumes\/([^/<]+)\/([^/<]+))<\/loc>/g)) {
          rows.push([decodeURIComponent(m[1]), parfumoKey(decodeURIComponent(m[2]).replace(/_/g, " ")), parfumoKey(decodeURIComponent(m[3]).replace(/_/g, " "))]);
        }
        for (let i = 0; i < rows.length; i += 1000) {
          const chunk = rows.slice(i, i + 1000);
          await prisma.$executeRawUnsafe(
            `INSERT INTO parfumo_urls (path, brand_key, name_key)
             SELECT * FROM unnest($1::text[], $2::text[], $3::text[]) ON CONFLICT (path) DO NOTHING`,
            chunk.map((r) => r[0]), chunk.map((r) => r[1]), chunk.map((r) => r[2]),
          );
        }
        total += rows.length;
      }
      await prisma.$executeRawUnsafe(`INSERT INTO parfumo_state (key, value, updated_at) VALUES ('index', $1::jsonb, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, JSON.stringify({ files: files.length, urls: total }));
      logger.info("parfumo index loaded", { files: files.length, urls: total });
    })().finally(() => { parfumoIndexPromise = null; });
  }
  await parfumoIndexPromise;
}

/** Pyramid, accords (+ colours), gender, year, title from a Parfumo perfume page. */
function parseParfumoPage(html) {
  const decode = (text) => String(text || "").replace(/&amp;/g, "&").replace(/&#039;|&#39;|&apos;/g, "'").replace(/&quot;/g, '"').trim();
  const tier = (code) => [...html.matchAll(new RegExp(`data-nt="${code}"[^>]*>\\s*<span[^>]*>\\s*<img src="([^"]+)" alt="([^"]+)"`, "g"))]
    .map((m) => ({ name: decode(m[2]), icon: m[1].replace(/\?.*$/, "?width=160&aspect_ratio=1:1") }));
  const accords = [...html.matchAll(/<div class="s-circle[^"]*" style="background: (#[0-9a-fA-F]{3,8})"><\/div><div class="text-xs grey">([^<]+)<\/div>/g)]
    .map((m) => ({ name: decode(m[2]), background: m[1] }));
  const og = decode((html.match(/<meta property="og:description" content="([^"]*)"/) || [])[1]);
  const title = decode((html.match(/<title>([^<]*)<\/title>/) || [])[1]).replace(/\s*».*$/, "");
  const gender = /for women and men|unisex/i.test(og) ? "unisex" : /for men/i.test(og) ? "male" : /for women/i.test(og) ? "female" : "";
  const year = Number((og.match(/released in ((?:18|19|20)\d{2})/) || [])[1]) || null;
  return { top: tier("t"), middle: tier("m"), base: tier("b"), flat: tier("n"), accords, gender, year, title };
}

/** Looks the perfume up on Parfumo and stores it as its detail. → true when stored. */
async function fillPerfumeFromParfumo(row) {
  await ensureParfumoIndex();
  const prisma = getPrisma();
  const parser = require("./lib/perfume-match");
  const brands = await supplierMatchBrands(prisma).catch(() => null);
  const brandKey = parfumoKey(row.brand);
  const nameKey = parfumoKey(row.name);
  if (!brandKey || !nameKey) return false;
  // the same brand; the name exactly, else the name plus a concentration / flanker tail («Nahema_Eau_de_Parfum»)
  const urls = await prisma.$queryRawUnsafe(
    `SELECT path, name_key FROM parfumo_urls WHERE brand_key = $1 AND (name_key = $2 OR name_key LIKE $2 || '%')
      ORDER BY (name_key = $2) DESC, length(name_key) LIMIT 4`, brandKey, nameKey,
  );
  const genderWord = { male: "men", female: "women", unisex: "unisex" };
  const want = parser.parsePerfumeName(`${row.brand} ${row.name} ${genderWord[row.gender] || ""} 100 ml`, { brands });
  let page = null;
  for (const url of urls) {
    const html = await parfumoRequest(url.path);
    const parsed = html ? parseParfumoPage(html) : null;
    if (!parsed || !(parsed.top.length + parsed.middle.length + parsed.base.length + parsed.flat.length)) continue;
    // «Nahema by Guerlain (Parfum)»: the name before «by»; the concentration in brackets is not part of it
    const name = parsed.title.replace(/\s+by\s+.+$/i, "").replace(/\s*\([^)]*\)\s*$/, "");
    const candidate = parser.parsePerfumeName(`${row.brand} ${name} ${genderWord[parsed.gender] || ""} 100 ml`, { brands });
    const result = parser.comparePerfumes(want, candidate);
    if (result.ok && !result.probable) { page = parsed; break; }
  }
  if (!page) {
    logger.info("parfumo perfume not found", { id: Number(row.id), brand: row.brand, name: row.name, candidates: urls.length });
    return false;
  }
  const allNotes = [...page.top, ...page.middle, ...page.base, ...page.flat];
  const ru = await fragellaTranslate("note", allNotes.map((n) => n.name));
  if (allNotes.some((n) => !ru.has(cleanText(n.name)))) throw Object.assign(new Error("Перевод нот Parfumo не удался (текстовый AI занят, rate limit)"), { statusCode: 429 });
  const knownIcons = await fragellaKnownNoteIcons().catch(() => new Map());
  const palette = await fragellaAccordPalette().catch(() => new Map());
  const note = (n) => {
    const name = ru.get(cleanText(n.name));
    return { name, icon: knownIcons.get(name.toLowerCase()) || n.icon };
  };
  const notes = { top: page.top.map(note), middle: page.middle.map(note), base: page.base.map(note) };
  if (!notes.top.length && !notes.middle.length && !notes.base.length) notes.flat = page.flat.map(note);
  const accords = page.accords.slice(0, 10).map((a, i) => {
    const name = PARFUMO_ACCORDS_RU[a.name.toLowerCase()] || a.name.toLowerCase();
    return { name, share: Math.max(40, 100 - i * 10), ...(palette.get(name) || { color: "#000000", background: a.background }) };
  });
  const gender = row.gender || page.gender || "";
  const year = Number(row.year) || page.year || null;
  const title = `${row.name} ${row.brand}`;
  const tierText = [["Верхние ноты", notes.top], ["средние ноты", notes.middle], ["базовые ноты", notes.base], ["ноты", notes.flat]]
    .filter(([, list]) => list && list.length).map(([label, list]) => `${label}: ${list.map((n) => n.name).join(", ")}`).join("; ");
  const forWhom = { male: "для мужчин", female: "для женщин", unisex: "для мужчин и женщин" }[gender] || "";
  const detail = {
    id: Number(row.id), url: row.url, name: row.name, year, brand: row.brand, image: "", notes, title, votes: null, family: "", gender,
    rating: null, accords, brandSlug: row.brand_slug || "", perfumers: [],
    description: `${title} — это аромат${forWhom ? ` ${forWhom}` : ""}.${year ? ` ${row.name} выпущен в ${year} году.` : ""} ${tierText}.`,
    source: "parfumo",
  };
  await prisma.$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail = $2::jsonb, detail_at = now(), detail_error = NULL,
       gender = CASE WHEN coalesce(gender, '') = '' THEN $3 ELSE gender END, year = coalesce(year, $4), updated_at = now()
     WHERE id = $1`,
    Number(row.id), JSON.stringify(detail), gender, year,
  );
  logger.info("parfumo perfume stored", { id: Number(row.id), brand: row.brand, name: row.name, notes: allNotes.length });
  return true;
}

/**
 * Last resort — neither Fragrantica nor Fragella has the perfume: the text AI names its notes, but only when it
 * knows the perfume («unknown» otherwise). Such notes are marked source «ai»: the card stops at «Ноты подобрал
 * ИИ — проверьте» until a person confirms them (AI notes for 13k warehouse goods were often wrong).
 */
async function fillPerfumeFromAi(row) {
  const { data, completion } = await createTextAiJson([
    { role: "system", content: "Ты парфюмерный эксперт с энциклопедическим знанием ароматов. Даёшь официальную пирамиду нот аромата — ту, что публикует производитель и Фрагрантика. Честно оцени уверенность в поле confidence: high — знаешь этот аромат и его пирамиду, medium — знаешь аромат, но в нотах сомневаешься, low — аромат тебе незнаком." },
    { role: "user", content: `Аромат: ${row.brand} ${row.name}${row.year ? `, ${row.year} год` : ""}${row.gender ? `, ${({ male: "мужской", female: "женский", unisex: "унисекс" })[row.gender] || ""}` : ""}. Верни JSON: {"top": ["нота"], "middle": ["нота"], "base": ["нота"], "accords": ["аккорд"], "gender": "male|female|unisex", "year": 1979, "confidence": "high|medium|low"}. Ноты и аккорды — по-русски, как на русской Фрагрантике (Бергамот, Розовый перец, Ветивер; аккорды: древесный, цитрусовый, пудровый). confidence — насколько ты уверен, что это именно пирамида этого аромата.` },
  ], { temperature: 0 });
  const list = (value) => (Array.isArray(value) ? value : []).map((v) => cleanText(v).replace(/ё/g, "е")).filter(Boolean).slice(0, 12);
  const levels = { top: list(data?.top), middle: list(data?.middle), base: list(data?.base) };
  if (!data || data.unknown || data.confidence === "low" || !Object.values(levels).flat().length) {
    logger.info("ai perfume notes unknown", { id: Number(row.id), brand: row.brand, name: row.name, answer: cleanText(completion?.choices?.[0]?.message?.content).slice(0, 300) });
    return false;
  }
  const [knownIcons, palette] = await Promise.all([fragellaKnownNoteIcons(), fragellaAccordPalette()]);
  const notes = Object.fromEntries(Object.entries(levels).map(([level, names]) => [level, names.map((name) => {
    const ru = name.charAt(0).toUpperCase() + name.slice(1);
    return { name: ru, icon: knownIcons.get(ru.toLowerCase()) || "" };
  })]));
  const accords = list(data?.accords).slice(0, 8).map((name, i) => ({ name: name.toLowerCase(), share: Math.max(40, 100 - i * 10), ...(palette.get(name.toLowerCase()) || { color: "#000000", background: "#d9d9d9" }) }));
  const gender = ["male", "female", "unisex"].includes(data?.gender) ? data.gender : row.gender || "";
  const year = Number(row.year) || Number(data?.year) || null;
  const title = `${row.name} ${row.brand}`;
  const tierText = [["Верхние ноты", notes.top], ["средние ноты", notes.middle], ["базовые ноты", notes.base]]
    .filter(([, l]) => l.length).map(([label, l]) => `${label}: ${l.map((n) => n.name).join(", ")}`).join("; ");
  const detail = {
    id: Number(row.id), url: row.url, name: row.name, year, brand: row.brand, image: "", notes, title, votes: null,
    family: "", gender, rating: null, accords, brandSlug: row.brand_slug || "", perfumers: [], description: `${title}. ${tierText}.`, source: "ai",
  };
  await getPrisma().$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail = $2::jsonb, detail_at = now(), detail_error = NULL,
       gender = CASE WHEN coalesce(gender, '') = '' THEN $3 ELSE gender END, year = coalesce(year, $4), updated_at = now()
     WHERE id = $1`,
    Number(row.id), JSON.stringify(detail), gender, year,
  );
  logger.info("ai perfume notes stored", { id: Number(row.id), brand: row.brand, name: row.name });
  return true;
}

async function fragranticaPerfumeForExport(perfumeId) {
  let row = await readFragranticaPerfume(perfumeId);
  if (!row) {
    const error = new Error("Аромат не найден в каталоге.");
    error.statusCode = 404;
    throw error;
  }
  if (!row.detail_at) {
    // Sources in order: the Fragrantica page (unless it rests after a 403) → Fragella (while its quota lasts) →
    // the text AI (DeepSeek). A network hiccup on Fragrantica is retried later, never answered by the AI.
    let fetchError = null;
    const pagesPaused = fragranticaPagesPausedUntil() > 0;
    if (!pagesPaused) {
      try {
        await fetchAndStoreFragranticaPerfume(row.url);
      } catch (error) {
        fetchError = error;
      }
      row = await readFragranticaPerfume(perfumeId);
    }
    const fragranticaClosed = pagesPaused || /\b(403|429)\b|challenge|Cloudflare/i.test(String(fetchError?.message || ""));
    if (!row.detail_at && fragellaAvailable()) {
      const stored = await fillPerfumeFromFragella(row).catch((error) => {
        logger.warn("fragella fill failed", { id: Number(perfumeId), detail: error?.message || String(error) });
        return false;
      });
      if (stored) row = await readFragranticaPerfume(perfumeId);
    }
    // Parfumetrika (open Russian base), then Parfumo, before the AI: real notes, not a model's memory
    if (!row.detail_at && fragranticaClosed) {
      const stored = await fillPerfumeFromParfumetrika(row).catch((error) => {
        logger.warn("parfumetrika fill failed", { id: Number(perfumeId), detail: error?.message || String(error) });
        return false;
      });
      if (stored) row = await readFragranticaPerfume(perfumeId);
    }
    // Aromo (our own site): Russian pyramid, no translation needed
    if (!row.detail_at && fragranticaClosed) {
      const stored = await fillPerfumeFromAromo(row).catch((error) => {
        logger.warn("aromo fill failed", { id: Number(perfumeId), detail: error?.message || String(error) });
        return false;
      });
      if (stored) row = await readFragranticaPerfume(perfumeId);
    }
    if (!row.detail_at && fragranticaClosed) {
      let parfumoError = null;
      const stored = await fillPerfumeFromParfumo(row).catch((error) => {
        parfumoError = error;
        logger.warn("parfumo fill failed", { id: Number(perfumeId), detail: error?.message || String(error) });
        return false;
      });
      if (stored) row = await readFragranticaPerfume(perfumeId);
      // the note translation waits for the text AI («rate limit»): the draft asks again later
      else if (parfumoError && Number(parfumoError.statusCode) === 429) throw parfumoError;
    }
    if (!row.detail_at && fragranticaClosed && process.env.FRAGRANTICA_AI_NOTES !== "false") {
      let aiError = null;
      const stored = await fillPerfumeFromAi(row).catch((error) => {
        aiError = error;
        logger.warn("ai perfume notes failed", { id: Number(perfumeId), detail: error?.message || String(error) });
        return false;
      });
      if (stored) row = await readFragranticaPerfume(perfumeId);
      // «Слишком частые сообщения»: the draft waits and asks again
      else if (aiError) throw Object.assign(new Error(`Текстовый AI занят (rate limit): ${aiError.message}`), { statusCode: 429 });
      else throw Object.assign(new Error("Нет данных аромата: Фрагрантика закрыта, ИИ этот аромат не знает — выберите аромат вручную."), { statusCode: 404 });
    }
    if (!row.detail_at) {
      if (fetchError) throw fetchError;
      throw Object.assign(new Error("Нет данных аромата."), { statusCode: 404 });
    }
  }
  return { ...(row.detail || {}), id: Number(row.id), brand: row.brand, name: row.name, gender: row.gender, year: row.year, url: row.url };
}

function fragranticaAttributeForForm(attr) {
  return {
    id: Number(attr.id),
    name: attr.name,
    description: attr.description || "",
    type: attr.type,
    required: Boolean(attr.is_required),
    collection: Boolean(attr.is_collection),
    dictionaryId: Number(attr.dictionary_id || 0),
    maxValues: Number(attr.max_value_count || 0),
    group: attr.group_name || "",
  };
}

function fragranticaAbsoluteUrl(url) {
  const value = cleanText(url);
  return value.startsWith("/") ? `${fragranticaPublicBaseUrl()}${value}` : value;
}

// Предзаполненная форма карточки: и для формы на странице, и для конвейера (02d-fragrantica-drafts.js).
// Описание — общее для всех объёмов аромата (readFragranticaCardDescription), иначе текст Фрагрантики.
async function buildFragranticaFormData(query = {}) {
  const perfumeId = Number(query.perfumeId);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const account = fragranticaResolveOzonAccount(query.accountId);
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === query.typeKey) ? query.typeKey : fragOzonGuessTypeKey(perfume);
  const type = fragOzonTypeByKey(typeKey);
  const volume = fragFormatVolume(query.volume) || "";
  const tester = query.tester === "1" || query.tester === "true" || query.tester === true;
  const savedDescription = await readFragranticaCardDescription(perfumeId).catch(() => "");

  const brandInfo = await fragranticaBrandInfo(perfume.brandSlug).catch(() => ({ country: "", owner: "" }));
  const countryRu = fragranticaCountryRu(brandInfo.country);
  const [categoryAttrs, brand, genderDict, classificationDict, tnved, countryValues] = await Promise.all([
    ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId),
    fragranticaFindBrand(account, type.typeId, perfume.brand),
    fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.gender).catch(() => []),
    fragranticaDictValues(account, type.typeId, FRAG_OZON_ATTR.classification).catch(() => []),
    fragranticaFindTnved(account, type.typeId, volume),
    countryRu ? fragranticaSearchDict(account, type.typeId, FRAG_OZON_ATTR.country, countryRu, 10).catch(() => []) : Promise.resolve([]),
  ]);
  const country = countryValues.find((v) => v.value.toLowerCase() === countryRu.toLowerCase()) || null;
  const offerId = buildFragranticaOfferId({ perfumeId, volume, tester });
  const dimsTemplates = (await readFragranticaState("settings").catch(() => ({}))).dimsTemplates || [];
  const prefill = buildFragranticaOzonPrefill({
    dimsTemplates,
    perfume: savedDescription ? { ...perfume, description: savedDescription } : perfume,
    typeKey,
    volume,
    tester,
    offerId,
    lookups: {
      brand: brand.match,
      type: { id: type.typeId, value: type.label },
      gender: fragGenderValues(perfume.gender, genderDict),
      classification: matchFragranticaClassification(perfume, classificationDict),
      tnved,
      country,
      producer: perfume.brand,
    },
  });
  const values = Object.fromEntries(prefill.attributes.map((a) => [a.id, a.values]));
  const attributes = categoryAttrs
    .map(fragranticaAttributeForForm)
    .sort((a, b) => Number(b.required) - Number(a.required))
    .map((attr) => ({ ...attr, values: values[attr.id] || [] }));
  const prisma = await requireFragranticaTables();
  const exports = await prisma.$queryRawUnsafe(
    `SELECT id, marketplace, account_name AS "accountName", offer_id AS "offerId", volume_ml AS "volume", tester, status, product_id AS "productId", error, created_at AS "createdAt"
     FROM fragrantica_exports WHERE perfume_id = $1 ORDER BY id DESC`,
    perfumeId,
  );
  return {
    ok: true,
    perfume: { id: perfume.id, brand: perfume.brand, name: perfume.name, gender: perfume.gender, year: perfume.year, notes: perfume.notes, accords: perfume.accords },
    accounts: fragranticaOzonAccounts(),
    targets: fragranticaTargets(),
    account: { id: cleanText(account.id), name: cleanText(account.name), style: fragranticaCardStyleForAccount(account) },
    types: FRAG_OZON_TYPES,
    typeKey,
    // only a type the perfume name states — the form starts empty otherwise (no silent «парфюмерная вода»)
    typeKeyExplicit: fragExplicitTypeKey(perfume),
    typeId: type.typeId,
    volume,
    tester,
    offerId,
    name: prefill.name,
    vat: FRAG_OZON_DEFAULTS.vat,
    vatByTarget: Object.fromEntries(fragranticaTargets().map((t) => [t.key, t.kind === "ozon"
      ? fragranticaVatForClientId(cleanText(getOzonAccounts().find((a) => cleanText(a.id) === t.id)?.clientId), fragranticaEnvMap("FRAGRANTICA_VAT_BY_ACCOUNT"))
      : "0.05"])),
    country: brandInfo.country ? { source: brandInfo.country, ozon: country?.value || null } : null,
    dims: prefill.dims,
    attributes,
    brandMatched: Boolean(brand.match),
    brandCandidates: brand.candidates,
    sourceImage: fragranticaMediaUrl("images", `${perfumeId}.jpg`),
    exports: exports.map((e) => ({ ...e, id: Number(e.id), productId: e.productId ? Number(e.productId) : null, volume: e.volume === null ? null : Number(e.volume) })),
    descriptionShared: Boolean(savedDescription),
    categoryAttrs,
  };
}

app.get("/api/fragrantica/ozon/form", requireAdmin, async (request, response, next) => {
  try {
    const { categoryAttrs: _attrs, ...form } = await buildFragranticaFormData(request.query);
    response.json(form);
  } catch (error) {
    next(error);
  }
});

// «Написать описание ИИ» в форме: тот же промпт, что у склада, по фактам Фрагрантики.
// marketplace=yandex, если среди магазинов есть Маркет (у него правила строже — текст годится обоим).
async function generateFragranticaDescription(body = {}) {
  const perfume = await fragranticaPerfumeForExport(Number(body.perfumeId));
  const type = fragOzonTypeByKey(body.typeKey);
  const marketplace = body.marketplace === "ozon" ? "ozon" : "yandex";
  const tester = Boolean(body.tester);
  const genderLabel = { male: "мужской", female: "женский", unisex: "унисекс" }[perfume.gender] || undefined;
  const source = Object.fromEntries(Object.entries({
    marketplace,
    // No volume in the text: the same description goes to every volume of the perfume
    name: buildFragranticaOzonName({ perfume, typeKey: type.key, tester }),
    brand: perfume.brand,
    perfume: perfume.name,
    type: type.nameLabel,
    gender: genderLabel,
    tester: tester || undefined,
    ...fragranticaFactsFromDetail(perfume),
  }).filter(([, v]) => v !== undefined));
  const { data, completion } = await createTextAiJson(buildPerfumeCopyMessages(source, marketplace), { temperature: 0.7 });
  const description = normalizeParagraphText(data.description, 5000);
  if (!description) {
    const error = new Error("AI не вернул описание. Попробуйте ещё раз.");
    error.statusCode = 502;
    error.code = "text_ai_empty";
    throw error;
  }
  // every other volume of this perfume reuses this text
  await saveFragranticaCardDescription(perfume.id, description);
  return {
    ok: true,
    description,
    name: cleanText(data.name).slice(0, 200),
    bulletPoints: (Array.isArray(data.bulletPoints) ? data.bulletPoints : []).map((b) => cleanText(b)).filter(Boolean).slice(0, 8),
    seoKeywords: (Array.isArray(data.seoKeywords) ? data.seoKeywords : []).map((b) => cleanText(b)).filter(Boolean).slice(0, 12),
    model: cleanText(completion?.model),
  };
}

app.post("/api/fragrantica/ozon/describe", requireAdmin, async (request, response, next) => {
  try {
    response.json(await generateFragranticaDescription(request.body || {}));
  } catch (error) {
    if (error?.code === "text_ai_empty") return response.status(502).json({ error: error.message, code: error.code });
    next(error);
  }
});

// Настройки страницы «Фрагрантика»: шаблоны габаритов по объёму.
app.get("/api/fragrantica/settings", requireAdmin, async (_request, response, next) => {
  try {
    const settings = await readFragranticaState("settings");
    response.json({ ok: true, dimsTemplates: normalizeFragranticaDimsTemplates(settings.dimsTemplates) });
  } catch (error) {
    next(error);
  }
});

app.put("/api/fragrantica/settings", requireAdmin, async (request, response, next) => {
  try {
    const dimsTemplates = normalizeFragranticaDimsTemplates(request.body?.dimsTemplates);
    await writeFragranticaState("settings", { dimsTemplates });
    await appendAudit(request, "fragrantica.settings.update", { entityType: "fragrantica_settings", newValue: { dimsTemplates } });
    response.json({ ok: true, dimsTemplates });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/attribute-values", requireAdmin, async (request, response, next) => {
  try {
    const account = fragranticaResolveOzonAccount(request.query.accountId);
    const typeId = Number(request.query.typeId) || FRAG_OZON_TYPES[0].typeId;
    const attributeId = Number(request.query.attributeId);
    if (!attributeId) return response.status(400).json({ error: "attributeId required" });
    const q = cleanText(request.query.q);
    const items = q.length >= 2
      ? await fragranticaSearchDict(account, typeId, attributeId, q, 30)
      : (await fragranticaDictValues(account, typeId, attributeId)).slice(0, 100).map((v) => ({ id: Number(v.id), value: String(v.value || ""), info: v.info || "" }));
    response.json({ ok: true, items });
  } catch (error) {
    next(error);
  }
});

// ─── Фото через parfumdeclaration ──────────────────────────────────────────

// Фото рисуются здесь, на davidsklad (мощнее): тот же рендер parfumdeclaration, собранный в
// lib/perfume-render (scripts/build-perfume-render.cjs). Нет onnxruntime-node / ошибка — как раньше,
// через parfumdeclaration. FRAGRANTICA_RENDER=pd — всегда через parfumdeclaration.
let fragranticaLocalRenderer;
// Two perfumes render at once (8 cores; the upscale uses 2 threads each)
const fragranticaRenderPinned = Number(process.env.FRAGRANTICA_RENDER_PARALLEL) || 0;
const fragranticaLocalRenderLanes = Math.max(1, fragranticaRenderPinned || 6);
// lanes in use now: 6 at night, 3 by day (FRAGRANTICA_RENDER_PARALLEL pins it)
const fragranticaRenderLanesNow = () => Math.min(fragranticaLocalRenderLanes, fragranticaRenderPinned || (isNightWorkWindow() ? 6 : 2));
const fragranticaLocalRenderChains = Array.from({ length: fragranticaLocalRenderLanes }, () => Promise.resolve());
let fragranticaLocalRenderNext = 0;

function loadFragranticaLocalRenderer() {
  if (process.env.FRAGRANTICA_RENDER === "pd") return null;
  if (fragranticaLocalRenderer !== undefined) return fragranticaLocalRenderer;
  try {
    fragranticaLocalRenderer = require("./lib/perfume-render/index.cjs");
  } catch (error) {
    logger.warn("fragrantica local renderer unavailable, using parfumdeclaration", { detail: error?.message || String(error) });
    fragranticaLocalRenderer = null;
  }
  return fragranticaLocalRenderer;
}

async function fragranticaSourceBuffer(imageUrl) {
  const local = /\/uploads\/fragrantica\/(images|thumbs|cards)\/([^/?#]+)$/.exec(String(imageUrl || ""));
  if (local && fragFs.existsSync(fragranticaMediaPath(local[1], local[2]))) return fragFs.promises.readFile(fragranticaMediaPath(local[1], local[2]));
  const res = await fetch(imageUrl, { signal: AbortSignal.timeout(30_000) });
  if (!res.ok) throw new Error(`Фото не скачалось: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

// ─── Пул процессов рендера ─────────────────────────────────────────────────
// JS-часть рендера занимает поток целиком: в одном процессе «полосы» шли друг за другом. Каждая полоса —
// свой процесс (lib/perfume-render-worker.cjs), так ароматы рисуются на разных ядрах. Процесс упал или
// завис (5 мин) — пересоздаётся; не запустился — рисуем в этом процессе, как раньше.
const { fork: fragRenderFork } = require("child_process");
const FRAG_RENDER_TIMEOUT_MS = 5 * 60_000;
const fragranticaRenderProcs = [];
let fragranticaRenderJobId = 0;

function fragranticaRenderProc(lane) {
  const current = fragranticaRenderProcs[lane];
  if (current && current.child.connected) return current;
  const workerPath = path.join(path.dirname(path.dirname(require.resolve("./lib/perfume-render/index.cjs"))), "perfume-render-worker.cjs");
  // each render process stays on ~1–2 cores (libvips would take all 8): the site and the shop keep their CPU
  const child = fragRenderFork(workerPath, [], {
    serialization: "advanced",
    stdio: ["ignore", "inherit", "inherit", "ipc"],
    env: { ...process.env, VIPS_CONCURRENCY: process.env.FRAGRANTICA_RENDER_VIPS_THREADS || "1", UV_THREADPOOL_SIZE: "2" },
  });
  // background work: the site and the shop get the CPU first
  try { if (child.pid) require("os").setPriority(child.pid, 10); } catch { /* not allowed — fine */ }
  const proc = { child, pending: new Map() };
  child.on("message", (msg) => {
    const job = proc.pending.get(msg?.id);
    if (!job) return;
    proc.pending.delete(msg.id);
    clearTimeout(job.timer);
    if (msg.ok) job.resolve(msg.out);
    else job.reject(new Error(msg.error || "render failed"));
  });
  child.on("exit", (code) => {
    for (const job of proc.pending.values()) {
      clearTimeout(job.timer);
      job.reject(new Error(`процесс рендера завершился (${code})`));
    }
    proc.pending.clear();
    if (fragranticaRenderProcs[lane] === proc) fragranticaRenderProcs[lane] = null;
  });
  fragranticaRenderProcs[lane] = proc;
  return proc;
}

function renderFragranticaInProcess(lane, args) {
  const proc = fragranticaRenderProc(lane);
  const id = ++fragranticaRenderJobId;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      proc.pending.delete(id);
      reject(new Error("рендер завис — процесс перезапущен"));
      proc.child.kill("SIGKILL");
    }, FRAG_RENDER_TIMEOUT_MS);
    proc.pending.set(id, { resolve, reject, timer });
    proc.child.send({ id, source: args.source, styles: args.styles, perfume: args.perfume, extras: args.extras });
  });
}

/** Render on this server (a pool of render processes). Same answer shape as parfumdeclaration. */
async function renderFragranticaCardHere(payload) {
  const renderer = loadFragranticaLocalRenderer();
  if (!renderer) return null;
  const lane = fragranticaLocalRenderNext++ % fragranticaRenderLanesNow();
  const run = fragranticaLocalRenderChains[lane].then(async () => {
    const source = await fragranticaSourceBuffer(payload.imageUrl);
    const args = { source, styles: payload.styles, perfume: payload.perfume, extras: payload.extras !== false };
    let out;
    try {
      out = await renderFragranticaInProcess(lane, args);
    } catch (error) {
      logger.warn("fragrantica render process failed, rendering in the worker", { lane, detail: error?.message || String(error) });
      out = await renderer.renderPerfumeCard(args);
    }
    const b64 = (buf) => (buf ? Buffer.from(buf).toString("base64") : null);
    const map = (obj) => Object.fromEntries(Object.entries(obj || {}).map(([k, v]) => [k, b64(v)]));
    return {
      status: "done",
      main: b64(out.main),
      notesByStyle: map(out.notesByStyle),
      specsByStyle: map(out.specsByStyle),
      closeup: b64(out.closeup),
      tiersByStyle: Object.fromEntries(Object.entries(out.tiersByStyle || {}).map(([k, list]) => [k, list.map((t) => ({ tier: t.tier, image: b64(t.image) }))])),
      accordsByStyle: map(out.accordsByStyle),
      warnings: out.warnings || [],
      renderedBy: "davidsklad",
    };
  });
  fragranticaLocalRenderChains[lane] = run.catch(() => {});
  return run;
}

async function requestPerfumeCardPhotos(payload) {
  try {
    const here = await renderFragranticaCardHere(payload);
    if (here) return here;
  } catch (error) {
    logger.warn("fragrantica local render failed, using parfumdeclaration", { detail: error?.message || String(error) });
  }
  return requestPdPerfumeCard(payload);
}

async function requestPdPerfumeCard(payload) {
  if (!fragranticaPdToolsToken) throw new Error("PD_TOOLS_TOKEN не задан — обработка фото в parfumdeclaration недоступна.");
  const headers = { "Content-Type": "application/json", "x-tools-token": fragranticaPdToolsToken };
  const start = await fetch(`${fragranticaPdToolsUrl}/api/tools/perfume-card`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30_000),
  });
  const started = await start.json().catch(() => ({}));
  if (!start.ok || !started.jobId) throw new Error(started.error || `parfumdeclaration ответил ${start.status}`);
  const deadline = Date.now() + 6 * 60_000;
  while (Date.now() < deadline) {
    await sleep(2500);
    const poll = await fetch(`${fragranticaPdToolsUrl}/api/tools/perfume-card/${encodeURIComponent(started.jobId)}`, { headers, signal: AbortSignal.timeout(60_000) });
    const data = await poll.json().catch(() => ({}));
    if (!poll.ok) throw new Error(data.error || `parfumdeclaration ответил ${poll.status}`);
    if (data.status === "done") return data;
    if (data.status === "failed") throw new Error(data.error || "parfumdeclaration не смог обработать фото");
  }
  throw new Error("parfumdeclaration не успел обработать фото за 6 минут");
}

/** Absolute URLs of the extra card photos already made for a perfume: «Характеристики» (shop style), close-up. */
const FRAG_TIER_KEYS = ["top", "middle", "base", "flat"];

function fragranticaExtraPhotos(perfumeId, style) {
  const id = Number(perfumeId);
  // After the shop's «Пирамида аромата»: tiers with big icons, accords, «Характеристики», close-up (Ozon: 8+ photos)
  const files = [
    ...FRAG_TIER_KEYS.map((tier) => `${id}-tier-${tier}-${style}-v1.jpg`),
    `${id}-accords-${style}-v1.jpg`,
    `${id}-specs-${style}-v1.jpg`,
    `${id}-closeup-v1.jpg`,
  ];
  return files.filter((file) => fragFs.existsSync(fragranticaMediaPath("cards", file))).map((file) => fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", file)));
}

async function runFragranticaImageJob(job, options) {
  const result = await runFragranticaImageJobPhotos(job, options);
  // Видеообложка Ozon по стилю магазина: флакон → пирамида → характеристики → крупный план
  result.video = {};
  const local = (url) => (url && url.startsWith("/uploads/fragrantica/cards/") ? fragranticaMediaPath("cards", url.split("/").pop()) : "");
  // the covers go to the background video queue — the photos are ready now, the send waits for its video
  for (const style of options.styles || []) {
    const slides = [local(result.main), local(result.notes?.[style]), local(result.specs?.[style]), local(result.closeup)].filter(Boolean);
    const pending = queueFragranticaVideoCover(options.perfumeId, style, slides, { refresh: Boolean(options.refresh) });
    if (options.waitVideo) {
      const url = await pending.catch(() => null);
      if (url) result.video[style] = url;
    }
  }
  return result;
}

async function runFragranticaImageJobPhotos(job, { perfumeId, styles, refresh }) {
  const t0 = Date.now();
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  await ensureFragranticaPerfumeImage(perfumeId);
  const hdSource = await ensureFragranticaHdImage(perfumeId);
  const timing = { source: Date.now() - t0 };
  // v2: 1500×2000, JPEG q95 без прореживания цвета, исходник — оригинал Фрагрантики (см. ensureFragranticaHdImage)
  const mainFile = `${perfumeId}-main-v2.jpg`;
  const notesFile = (style) => `${perfumeId}-notes-${style}-v2.jpg`;
  const have = (file) => fragFs.existsSync(fragranticaMediaPath("cards", file));
  // Extra photos (up to 6 per card): close-up of the real photo + «Характеристики»; «.none» = no close-up possible
  const closeupFile = `${perfumeId}-closeup-v1.jpg`;
  const closeupNone = `${perfumeId}-closeup-v1.none`;
  const specsFile = (style) => `${perfumeId}-specs-${style}-v1.jpg`;
  // «Ноты крупно» + «Аккорды» per style; the marker says they were made (a perfume may have no tiers/accords)
  const extras2Marker = (style) => `${perfumeId}-extras2-${style}.done`;
  const result = { main: null, notes: {}, specs: {}, closeup: null, source: hdSource || fragranticaMediaUrl("images", `${perfumeId}.jpg`), warnings: [] };
  const cached = () => {
    result.main = fragranticaMediaUrl("cards", mainFile);
    for (const style of styles) result.notes[style] = fragranticaMediaUrl("cards", notesFile(style));
    for (const style of styles) if (have(specsFile(style))) result.specs[style] = fragranticaMediaUrl("cards", specsFile(style));
    if (have(closeupFile)) result.closeup = fragranticaMediaUrl("cards", closeupFile);
    return result;
  };
  if (!refresh && have(mainFile) && styles.every((style) => have(notesFile(style)) && have(specsFile(style)) && have(extras2Marker(style))) && (have(closeupFile) || have(closeupNone))) {
    return cached();
  }
  job.stage = "parfumdeclaration";
  try {
    const brandInfo = await fragranticaBrandInfo(perfume.brandSlug).catch(() => ({ country: "" }));
    timing.brand = Date.now() - t0;
    const pd = await requestPerfumeCardPhotos({
      imageUrl: fragranticaAbsoluteUrl(result.source),
      style: styles[0],
      styles,
      extras: true,
      perfume: {
        brand: perfume.brand,
        name: perfume.name,
        year: perfume.year || null,
        gender: perfume.gender || "",
        family: perfume.family || "",
        perfumers: Array.isArray(perfume.perfumers) ? perfume.perfumers.slice(0, 2) : [],
        country: fragranticaCountryRu(brandInfo.country) || "",
        notes: {
          top: (perfume.notes?.top || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          middle: (perfume.notes?.middle || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          base: (perfume.notes?.base || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
          flat: (perfume.notes?.flat || []).map((n) => ({ name: n.name, icon: fragranticaNoteIconLarge(n.icon) })),
        },
        accords: (perfume.accords || []).slice(0, 6),
      },
    });
    timing.render = Date.now() - t0;
    logger.info("fragrantica photo timing", { perfumeId, styles: styles.length, by: pd.renderedBy || "pd", ...timing });
    await fragFs.promises.mkdir(path.join(fragranticaMediaDir, "cards"), { recursive: true });
    if (pd.main) {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", mainFile), Buffer.from(pd.main, "base64"));
      result.main = fragranticaMediaUrl("cards", mainFile);
    }
    const byStyle = pd.notesByStyle || (pd.notes ? { [styles[0]]: pd.notes } : {});
    for (const style of styles) {
      if (!byStyle[style]) continue;
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", notesFile(style)), Buffer.from(byStyle[style], "base64"));
      result.notes[style] = fragranticaMediaUrl("cards", notesFile(style));
    }
    for (const style of styles) {
      for (const { tier, image } of pd.tiersByStyle?.[style] || []) {
        if (FRAG_TIER_KEYS.includes(tier)) await fragFs.promises.writeFile(fragranticaMediaPath("cards", `${perfumeId}-tier-${tier}-${style}-v1.jpg`), Buffer.from(image, "base64"));
      }
      if (pd.accordsByStyle?.[style]) await fragFs.promises.writeFile(fragranticaMediaPath("cards", `${perfumeId}-accords-${style}-v1.jpg`), Buffer.from(pd.accordsByStyle[style], "base64"));
      if (pd.tiersByStyle) await fragFs.promises.writeFile(fragranticaMediaPath("cards", extras2Marker(style)), "");
    }
    for (const style of styles) {
      const specs = pd.specsByStyle?.[style];
      if (!specs) continue;
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", specsFile(style)), Buffer.from(specs, "base64"));
      result.specs[style] = fragranticaMediaUrl("cards", specsFile(style));
    }
    if (pd.closeup) {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", closeupFile), Buffer.from(pd.closeup, "base64"));
      result.closeup = fragranticaMediaUrl("cards", closeupFile);
    } else {
      await fragFs.promises.writeFile(fragranticaMediaPath("cards", closeupNone), "");
    }
    result.warnings.push(...(pd.warnings || []));
  } catch (error) {
    logger.warn("fragrantica pd card failed", { perfumeId, detail: error?.message });
    result.warnings.push(`Фото не обработано: ${error?.message || error}. Можно создать карточку с исходным фото.`);
  }
  if (!result.main) result.main = result.source;
  return result;
}

app.post("/api/fragrantica/ozon/images", requireAdmin, async (request, response, next) => {
  try {
    const perfumeId = Number(request.body?.perfumeId);
    if (!perfumeId) return response.status(400).json({ error: "perfumeId required" });
    const requested = (Array.isArray(request.body?.styles) ? request.body.styles : []).filter((s) => FRAG_CARD_STYLES.includes(s));
    const styles = requested.length ? [...new Set(requested)] : [fragranticaCardStyleForAccount(fragranticaResolveOzonAccount(request.body?.accountId))];
    const jobId = `${perfumeId}-${styles.join("_")}-${Date.now().toString(36)}`;
    const job = { id: jobId, status: "running", stage: "download", result: null, error: null, at: Date.now() };
    fragranticaImageJobs.set(jobId, job);
    for (const [id, old] of fragranticaImageJobs) if (Date.now() - old.at > 3_600_000) fragranticaImageJobs.delete(id);
    // the manual form shows the video preview: it waits for the cover
    runFragranticaImageJob(job, { perfumeId, styles, refresh: Boolean(request.body?.refresh), waitVideo: true })
      .then((result) => Object.assign(job, { status: "done", result }))
      .catch((error) => Object.assign(job, { status: "failed", error: error?.message || String(error) }));
    response.json({ ok: true, jobId });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/images/:jobId", requireAdmin, (request, response) => {
  const job = fragranticaImageJobs.get(String(request.params.jobId));
  if (!job) return response.status(404).json({ error: "Задача не найдена (сервер перезапускался?) — запустите заново." });
  response.json({ ok: true, status: job.status, stage: job.stage, result: job.result, error: job.error });
});

// ─── Создание карточки и очередь ────────────────────────────────────────────

function fragranticaExportFromRow(row = {}) {
  return {
    id: Number(row.id),
    perfumeId: Number(row.perfume_id),
    marketplace: row.marketplace || "ozon",
    accountId: row.account_id,
    accountName: row.account_name,
    offerId: row.offer_id,
    volume: row.volume_ml === null || row.volume_ml === undefined ? null : Number(row.volume_ml),
    tester: Boolean(row.tester),
    status: row.status,
    productId: row.product_id ? Number(row.product_id) : null,
    result: row.result || null,
    links: Array.isArray(row.links) ? row.links : [],
    error: row.error || null,
    nextAttemptAt: row.next_attempt_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

async function readFragranticaExport(id) {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = $1`, Number(id));
  return rows[0] || null;
}

async function updateFragranticaExport(id, fields = {}) {
  const prisma = await requireFragranticaTables();
  const sets = [];
  const params = [Number(id)];
  for (const [column, value] of Object.entries(fields)) {
    params.push(value !== null && typeof value === "object" && !(value instanceof Date) ? JSON.stringify(value) : value);
    sets.push(`${column} = $${params.length}${["result", "item", "links"].includes(column) ? "::jsonb" : ""}`);
  }
  await prisma.$executeRawUnsafe(`UPDATE fragrantica_exports SET ${sets.join(", ")}, updated_at = now() WHERE id = $1`, ...params);
  return readFragranticaExport(id);
}

function fragranticaOzonErrorsText(errors = []) {
  return errors
    .map((e) => [e.attribute_name || e.field, e.description || e.message || e.code].filter(Boolean).join(": "))
    .filter(Boolean)
    .join("; ")
    .slice(0, 2000);
}

// Отправить карточку в Ozon или поставить в очередь лимита.
async function submitFragranticaExport(row) {
  const account = fragranticaResolveOzonAccount(row.account_id);
  const attempts = Number(row.attempts || 0) + 1;
  try {
    const quota = await ozonRequest("/v4/product/info/limit", {}, account).catch(() => null);
    const daily = quota?.daily_create;
    if (daily && Number(daily.limit) !== -1 && Number(daily.usage) >= Number(daily.limit)) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        attempts,
        error: `Дневной лимит создания карточек Ozon исчерпан (${daily.usage}/${daily.limit}) — отправим после сброса.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date(), daily.reset_at),
      });
    }
    const data = await ozonRequest("/v3/product/import", { items: [row.item] }, account);
    const taskId = data?.result?.task_id;
    if (!taskId) throw new Error("Ozon не вернул task_id");
    return updateFragranticaExport(row.id, { status: "pending", task_id: taskId, attempts, error: null, next_attempt_at: null });
  } catch (error) {
    const message = error?.ozon?.message || error?.message || String(error);
    if (isOzonLimitErrorText(message)) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        attempts,
        error: `Ozon: ${message} — отправим после сброса лимита.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date()),
      });
    }
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Ozon: ${message}`.slice(0, 2000) });
  }
}

// Карточка Маркета собирается теми же правилами, что перенос Ozon→Яндекс (категория по типу, бренд,
// параметры), из «синтетического» Ozon-товара с атрибутами формы и своими фото (Parfumerius).
function fragranticaSyntheticOzonProduct(row) {
  const item = row.item || {};
  const attr = (id) => cleanText((item.attributes || []).find((a) => Number(a.id) === id)?.values?.[0]?.value);
  return {
    id: `fragrantica-yandex-${row.offer_id}`,
    offerId: row.offer_id,
    marketplace: "ozon",
    // Not an Ozon cabinet on purpose: the Ozon→Market export would otherwise re-read the Ozon card with
    // the same offer id (enrichOzonProductsForYandexExport) and send its older dimensions/attributes.
    target: "fragrantica",
    name: item.yandexName || item.name,
    imageUrl: "",
    links: [],
    ozon: {
      name: item.yandexName || item.name,
      typeId: Number(item.type_id),
      attributes: item.attributes || [],
      description: attr(FRAG_OZON_ATTR.annotation),
      depth: item.depth,
      width: item.width,
      height: item.height,
      dimensionUnit: "mm",
      weight: item.weight,
      weightUnit: "g",
      images: [],
      primaryImage: "",
      barcodes: item.barcode ? [item.barcode] : [],
      barcode: item.barcode || "",
    },
    yandex: {
      pictures: item.yandexPictures || [],
      description: attr(FRAG_OZON_ATTR.annotation),
      price: Number(item.price) || undefined,
      extra: item.yandexExtra || {},
    },
  };
}

async function submitFragranticaYandexExport(row) {
  const attempts = Number(row.attempts || 0) + 1;
  const shop = fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(row.account_id));
  if (!shop) return updateFragranticaExport(row.id, { status: "failed", attempts, error: "Магазин Яндекс Маркета не найден в настройках." });
  try {
    const product = fragranticaSyntheticOzonProduct(row);
    const built = buildYandexOfferMapping(product);
    if (!built.ready) {
      const reason = built.categoryReview ? `категория: ${built.categoryReview}` : `не хватает: ${built.missing.join(", ")}`;
      return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${reason}` });
    }
    const improve = row.item?.improve === true;
    // improving an existing Market card: content only — its price, stock and warehouse record stay as they are
    const result = await exportOzonProductsToYandex([product], [shop], { reason: improve ? "fragrantica_improve" : "fragrantica_export", contentOnly: improve });
    if (result.sentOfferIds.has(cleanText(row.offer_id).toLowerCase())) {
      const vat = improve ? {} : await sendFragranticaYandexVatPrice(shop, row.offer_id, row.item?.price);
      return updateFragranticaExport(row.id, {
        status: "imported",
        attempts,
        error: null,
        result: row.result?.mediaRefresh
          // the Market send rewrites the warehouse card with an empty link list — links are always put back
          ? { ...(row.result || {}), market: "sent", ...vat, mediaRefresh: false, mediaRefreshedAt: new Date().toISOString(), links: Array.isArray(row.links) && row.links.length ? "pending" : "none" }
          : { ...(row.result || {}), market: "sent", ...vat, docs: "pending", links: Array.isArray(row.links) && row.links.length ? "pending" : "none" },
      });
    }
    const failure = (result.results || []).find((r) => !r.ok);
    const message = failure?.error || failure?.message || (failure?.errors || []).map((e) => e.message || e.type).join("; ") || "Маркет не принял карточку";
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${message}`.slice(0, 2000) });
  } catch (error) {
    return updateFragranticaExport(row.id, { status: "failed", attempts, error: `Маркет: ${error?.message || error}`.slice(0, 2000) });
  }
}

const fragranticaYandexParamsCache = new Map();
async function fragranticaYandexCategoryParams(shop, categoryId) {
  const key = `${shop.businessId}:${categoryId}`;
  const cached = fragranticaYandexParamsCache.get(key);
  if (cached && Date.now() - cached.at < 12 * 3_600_000) return cached.params;
  const data = await yandexRequest(shop, "POST", `/v2/category/${Number(categoryId)}/parameters`, {});
  const params = data?.result?.parameters || [];
  fragranticaYandexParamsCache.set(key, { at: Date.now(), params });
  return params;
}

// Код НДС Маркета (VatType): 10 — 5% УСН. FRAGRANTICA_YANDEX_VAT переопределяет.
const fragranticaYandexVat = Number(process.env.FRAGRANTICA_YANDEX_VAT || 10) || 10;

async function sendFragranticaYandexVatPrice(shop, offerId, price) {
  if (!shop?.campaignId || !(Number(price) > 0)) return { vat: "skipped" };
  try {
    await yandexRequest(shop, "POST", `/v2/campaigns/${shop.campaignId}/offer-prices/updates`, {
      offers: [{ offerId, price: { value: Math.round(Number(price)), currencyId: "RUR", vat: fragranticaYandexVat } }],
    });
    return { vat: String(fragranticaYandexVat) };
  } catch (error) {
    const message = cleanText(error?.message);
    // «Partner use only default price; LOCKED»: the cabinet sells at one basic price — its VAT comes from
    // the Market cabinet settings, not from the API
    if (/LOCKED|default price/i.test(message)) return { vat: "cabinet" };
    return { vat: "failed", vatError: message.slice(0, 300) };
  }
}

function submitFragranticaTargetExport(row) {
  return row.marketplace === "yandex" ? submitFragranticaYandexExport(row) : submitFragranticaExport(row);
}

async function generateFragranticaBarcode(row, account) {
  try {
    const data = await ozonRequest("/v1/barcode/generate", { product_ids: [String(row.product_id)] }, account);
    const errors = data?.errors || [];
    if (errors.length) throw new Error(errors.map((e) => e.error || e.code).join("; "));
    return { barcode: "generated" };
  } catch (error) {
    const message = error?.ozon?.message || error?.message || String(error);
    return { barcode: "pending", barcodeError: message };
  }
}

// pending → спросить Ozon статус импорта; imported → штрихкод.
// ─── Декларация (+ GTIN Маркета) через parfumdeclaration ─────────────────────
// Карточка создана → parfumdeclaration привязывает декларацию по бренду (на Маркете ещё GTIN бренда из
// справочника, если у карточки нет своего). Бесплатно, только магазины владельца. Пока у карточки Ozon нет
// SKU — повтор каждые 2 мин (до 60 попыток).
async function attachFragranticaDocs(row) {
  const tries = Number(row.result?.docsTries || 0) + 1;
  if (!fragranticaPdToolsToken) return { docs: "none", docsInfo: "PD_TOOLS_TOKEN не задан" };
  const prisma = await requireFragranticaTables();
  const [perfume] = await prisma.$queryRawUnsafe(`SELECT brand, name FROM fragrantica_perfumes WHERE id = $1`, Number(row.perfume_id));
  const body = {
    marketplace: row.marketplace === "yandex" ? "yandex" : "ozon",
    offerId: row.offer_id,
    brand: cleanText(perfume?.brand),
    name: cleanText(row.item?.name) || `${cleanText(perfume?.brand)} ${cleanText(perfume?.name)}`.trim(),
    barcode: cleanText(row.item?.barcode || (Array.isArray(row.item?.barcodes) ? row.item.barcodes[0] : "")),
  };
  if (body.marketplace === "ozon") {
    body.clientId = cleanText(fragranticaResolveOzonAccount(row.account_id)?.clientId);
  } else {
    body.businessId = cleanText(fragranticaYandexShops().find((s) => cleanText(s.id) === cleanText(row.account_id))?.businessId);
  }
  try {
    const response = await fetch(`${fragranticaPdToolsUrl}/api/tools/attach-docs`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-tools-token": fragranticaPdToolsToken },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60_000),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `parfumdeclaration ответил ${response.status}`);
    if (data.status === "queued") {
      return { docs: "done", docsTries: tries, docsInfo: `Декларация ${data.declaration}${data.gtin ? `, GTIN ${data.gtin}` : ""}` };
    }
    if (data.status === "already") return { docs: "done", docsTries: tries, docsInfo: data.message };
    if (data.status === "not_ready" && tries < 60) return { docs: "pending", docsTries: tries, docsInfo: data.message };
    return { docs: "none", docsTries: tries, docsInfo: data.message || data.status };
  } catch (error) {
    const message = error?.message || String(error);
    return tries < 60 ? { docs: "pending", docsTries: tries, docsInfo: message } : { docs: "none", docsTries: tries, docsInfo: message };
  }
}

async function refreshFragranticaExport(row) {
  if (!row) return row;
  if (row.status === "imported" && row.result?.docs === "pending" && (row.marketplace === "yandex" || row.product_id)) {
    const docs = await attachFragranticaDocs(row);
    const updated = await updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...docs } });
    if (docs.docs === "pending") return updated;
    return refreshFragranticaExport(updated);
  }
  if (row.marketplace === "yandex") {
    if (row.status !== "imported" || row.result?.links !== "pending") return row;
    let linkResult;
    try {
      linkResult = await applyFragranticaExportLinks(row);
    } catch (error) {
      linkResult = { links: "pending", linksError: error?.message || String(error) };
    }
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...linkResult } });
  }
  const account = fragranticaResolveOzonAccount(row.account_id);
  if (row.status === "pending" && row.task_id) {
    const data = await ozonRequest("/v1/product/import/info", { task_id: Number(row.task_id) }, account);
    const item = (data?.result?.items || []).find((i) => i.offer_id === row.offer_id) || data?.result?.items?.[0];
    if (!item) return row;
    const errors = (item.errors || []).filter((e) => cleanText(e.level).toLowerCase() !== "warning");
    // «исчерпали суточный лимит на обновление товаров» — the card was not updated: resend after 03:00 MSK
    const limitText = (item.errors || []).map((e) => cleanText(e.description || e.message || e.code)).find((t) => isOzonLimitErrorText(t));
    if (limitText) {
      return updateFragranticaExport(row.id, {
        status: "queued_limit",
        product_id: item.product_id || row.product_id || null,
        error: `Ozon: ${limitText.slice(0, 300)} — отправим ещё раз после сброса лимита.`,
        next_attempt_at: fragranticaNextOzonLimitReset(new Date()),
      });
    }
    if (item.status === "imported" || (item.product_id && !errors.length && item.status !== "failed")) {
      const updated = await updateFragranticaExport(row.id, {
        status: "imported",
        product_id: item.product_id || null,
        error: null,
        result: row.result?.mediaRefresh
          ? { ...(row.result || {}), warnings: fragranticaOzonErrorsText(item.errors || []) || null, mediaRefresh: false, mediaRefreshedAt: new Date().toISOString() }
          : {
            ...(row.result || {}),
            warnings: fragranticaOzonErrorsText(item.errors || []) || null,
            // an improved existing card keeps its barcode (no new one generated)
            barcode: row.item?.improve ? "kept" : "pending",
            docs: "pending",
            links: Array.isArray(row.links) && row.links.length ? "pending" : "none",
          },
      });
      return refreshFragranticaExport(updated);
    }
    if ((item.status === "failed" || errors.length) && !row.result?.mediaRetried && fragranticaMediaExtrasFailed(errors)) {
      // Ozon refused the Rich-контент or the video cover: resend once without them, the card itself is fine
      const plainItem = { ...row.item, attributes: (row.item?.attributes || []).filter((a) => Number(a.id) !== FRAG_RICH_ATTR) };
      delete plainItem.complex_attributes;
      logger.warn("fragrantica export: Ozon refused rich content / video cover, resending without them", { id: Number(row.id), errors: fragranticaOzonErrorsText(errors) });
      const resent = await updateFragranticaExport(row.id, { item: plainItem, result: { ...(row.result || {}), mediaRetried: true, mediaError: fragranticaOzonErrorsText(errors) } });
      return submitFragranticaExport(resent);
    }
    if (item.status === "failed" || errors.length) {
      return updateFragranticaExport(row.id, {
        status: "failed",
        product_id: item.product_id || null,
        error: fragranticaOzonErrorsText(item.errors || []) || "Ozon отклонил карточку",
        result: { ...(row.result || {}), errors: item.errors || [] },
      });
    }
    return row;
  }
  if (row.status === "imported" && row.product_id && row.result?.links === "pending") {
    let linkResult;
    try {
      linkResult = await applyFragranticaExportLinks(row);
    } catch (error) {
      linkResult = { links: "pending", linksError: error?.message || String(error) };
      logger.warn("fragrantica export links failed", { id: Number(row.id), detail: linkResult.linksError });
    }
    const updated = await updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...linkResult } });
    if (linkResult.links === "pending") return updated;
    return refreshFragranticaExport(updated);
  }
  if (row.status === "imported" && row.product_id && row.result?.barcode === "pending" && row.item?.barcode) {
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), barcode: "manufacturer" } });
  }
  if (row.status === "imported" && row.product_id && row.result?.barcode === "pending") {
    const barcode = await generateFragranticaBarcode(row, account);
    return updateFragranticaExport(row.id, { result: { ...(row.result || {}), ...barcode } });
  }
  return row;
}

// body.targets: [{ key: "ozon:<id>" | "yandex:<id>", notes: "<url of this shop's notes picture>" | null }]
// (без targets — один кабинет Ozon из accountId, как раньше). Общие: фото флакона images[0], атрибуты,
// цена Ozon (price/oldPrice) и цена Маркета (yandexPrice). Артикул один на все магазины.
// Создать/обновить карточки в выбранных магазинах. request нужен для журнала (session.username).
async function createFragranticaExports(body = {}, request = { session: {} }) {
  const perfumeId = Number(body.perfumeId);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const allTargets = fragranticaTargets();
  const wanted = Array.isArray(body.targets) && body.targets.length
    ? body.targets.map((t) => ({ ...allTargets.find((x) => x.key === cleanText(t.key)), notes: cleanText(t.notes) || null })).filter((t) => t.key)
    : [{ ...(allTargets.find((x) => x.kind === "ozon" && x.id === cleanText(fragranticaResolveOzonAccount(body.accountId).id)) || {}), notes: null }].filter((t) => t.key);
  if (!wanted.length) throw fragranticaHttpError(400, "Выберите хотя бы один магазин.", { code: "fragrantica_export_no_targets" });
  // Market: no hidden brand, nothing under 20 ml, no testers — such a target is dropped with a note
  const bodyVolume = body.volume || (Array.isArray(body.attributes) ? (body.attributes.find((a) => Number(a.id) === FRAG_OZON_ATTR.volume)?.values || [])[0]?.value : "");
  const marketCheck = fragranticaMarketBlockReasons(perfume, { volume: bodyVolume, typeKey: body.typeKey || fragOzonTypeById(body.typeId)?.key, tester: body.tester });
  const skippedTargets = marketCheck.length ? wanted.filter((t) => t.kind === "yandex").map((t) => ({ key: t.key, label: t.label, reasons: marketCheck })) : [];
  if (skippedTargets.length) {
    wanted.splice(0, wanted.length, ...wanted.filter((t) => t.kind !== "yandex"));
    if (!wanted.length) throw fragranticaHttpError(400, `Маркет: ${marketCheck.join("; ")}`, { code: "fragrantica_export_market_blocked", skippedTargets });
  }

  const type = fragOzonTypeById(body.typeId) || fragOzonTypeByKey(body.typeKey);
  const attrsAccount = fragranticaResolveOzonAccount(wanted.find((t) => t.kind === "ozon")?.id || body.accountId);
  const categoryAttrs = await ozonGetCategoryAttributes(attrsAccount, FRAG_OZON_CATEGORY_ID, type.typeId);
  const bottle = (Array.isArray(body.images) ? body.images : []).map(fragranticaAbsoluteUrl).filter(Boolean)[0] || "";
  const { item: baseItem, missing } = buildFragranticaOzonItem({ ...body, typeId: type.typeId, images: bottle ? [bottle] : [] }, categoryAttrs);
  if (missing.length) {
    throw fragranticaHttpError(400, `Заполните: ${missing.join(", ")}`, { code: "fragrantica_export_missing", missing });
  }

  // Один артикул на все магазины (FR<id>-<мл>[T]). Повторная отправка того же аромата не плодит
  // дубли: карточка с этим артикулом обновляется (Ozon /v3/product/import и Маркет обновляют по offer_id).
  const prisma = await requireFragranticaTables();
  const offerId = baseItem.offer_id;
  for (const attr of baseItem.attributes) {
    if (attr.id === FRAG_OZON_ATTR.sellerCode) attr.values = [{ value: offerId }];
  }

  // Маркет: характеристики категории, ТН ВЭД + ОКПД2, срок годности — по фактам Фрагрантики
  let yandexExtra = null;
  if (wanted.some((t) => t.kind === "yandex")) {
    const shop = fragranticaYandexShops().find((x) => wanted.some((t) => t.kind === "yandex" && t.id === cleanText(x.id)));
    const volume = fragFormatVolume((baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume)?.values || [])[0]?.value);
    yandexExtra = await buildFragranticaYandexExtra({ perfume, type, name: baseItem.name, volume, tester: Boolean(body.tester), shop });
    if (body.ownBottleOnly && yandexExtra) delete yandexExtra.videos;
  }
  const vatOverrides = fragranticaEnvMap("FRAGRANTICA_VAT_BY_ACCOUNT");

  const volumeAttr = baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.volume);
  const links = JSON.stringify((Array.isArray(body.links) ? body.links : []).slice(0, 10).map(fragranticaLinkDraft));
  const yandexPrice = Math.round(Number(body.yandexPrice) || Number(baseItem.price) || 0);
  const results = [];
  for (const target of wanted) {
    const notes = target.notes ? fragranticaAbsoluteUrl(target.notes) : "";
    const ozonAttributes = baseItem.attributes.map((a) => (a.id === FRAG_OZON_ATTR.annotation
      ? { ...a, values: a.values.map((v) => ({ ...v, value: formatDescriptionForMarketplace(v.value, "ozon") })) }
      : a));
    const ozonAccount = target.kind === "ozon" ? getOzonAccounts().find((a) => cleanText(a.id) === target.id) : null;
    // After the notes: «Характеристики» in this shop's style and the close-up (when they were made)
    const ownPhotos = (Array.isArray(body.customPhotos) ? body.customPhotos : []).map(fragranticaAbsoluteUrl).filter(Boolean);
    // tester / < 20 ml: «О аромате» and the close-up show Fragrantica's bottle — left out, as is the video cover
    const extras = body.onlyCustomPhotos ? [] : fragranticaExtraPhotos(perfumeId, target.style).filter((u) => !body.ownBottleOnly || !/-(specs|closeup)-/.test(u));
    const notesPhoto = body.onlyCustomPhotos ? "" : notes;
    // Ozon: Rich-контент из описания и фото магазина + видеообложка (если категория знает атрибут 11254)
    const ozonMedia = target.kind === "ozon" ? fragranticaOzonMediaExtras({ perfumeId, style: target.style, notes, baseItem, categoryAttrs, perfume, noBottle: body.ownBottleOnly === true }) : null;
    const improveFlag = body.improve === true ? { improve: true } : {};
    // improvement: every Ozon cabinet gets back its own current price (two cabinets may differ)
    const ownPrice = body.improve === true && target.kind === "ozon" ? body.pricesByTarget?.[target.key] : null;
    const priceFields = ownPrice && Number(ownPrice.price) > 0
      ? { price: String(Math.round(Number(ownPrice.price))), old_price: Number(ownPrice.oldPrice) > Number(ownPrice.price) ? String(Math.round(Number(ownPrice.oldPrice))) : "0" }
      : {};
    const item = target.kind === "ozon"
      ? {
        ...baseItem,
        ...improveFlag,
        ...priceFields,
        vat: fragranticaVatForClientId(cleanText(ozonAccount?.clientId), vatOverrides),
        attributes: [...ozonAttributes, ...ozonMedia.attributes],
        ...(ozonMedia.complex.length ? { complex_attributes: ozonMedia.complex } : {}),
        primary_image: bottle,
        images: [...new Set([...ownPhotos, notesPhoto, ...extras].filter(Boolean))].slice(0, 29),
      }
      : {
        ...baseItem,
        ...improveFlag,
        // Маркет: «Парфюмерная вода <бренд> <аромат> <для кого> <мл> мл» — так карточка получает больше баллов
        yandexName: buildFragranticaMarketName({ perfume, typeKey: type.key, volume: (volumeAttr?.values || [])[0]?.value, tester: Boolean(body.tester) }),
        price: String(yandexPrice), yandexPictures: [...new Set([bottle, ...ownPhotos, notesPhoto, ...extras].filter(Boolean))].slice(0, 30), yandexExtra,
      };
    // the same perfume+volume already sent to this shop → update that row (and that card), no duplicate
    const inserted = await prisma.$queryRawUnsafe(
      `INSERT INTO fragrantica_exports (perfume_id, marketplace, account_id, account_name, offer_id, volume_ml, tester, status, item, created_by, links)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'new', $8::jsonb, $9, $10::jsonb)
       ON CONFLICT (account_id, offer_id) DO UPDATE SET perfume_id = EXCLUDED.perfume_id, account_name = EXCLUDED.account_name,
         volume_ml = EXCLUDED.volume_ml, tester = EXCLUDED.tester, status = 'new', item = EXCLUDED.item, links = EXCLUDED.links,
         error = NULL, attempts = 0, next_attempt_at = NULL, task_id = NULL, updated_at = now()
       RETURNING *`,
      perfumeId,
      target.kind,
      target.id,
      target.label,
      offerId,
      Number(volumeAttr?.values?.[0]?.value) || null,
      Boolean(body.tester),
      JSON.stringify(item),
      cleanText(request.session?.username) || null,
      links,
    );
    results.push(await submitFragranticaTargetExport(inserted[0]));
  }
  await appendAudit(request, "fragrantica.export", {
    entityType: "fragrantica_export",
    entityId: results.map((r) => r.id).join(","),
    newValue: { perfumeId, brand: perfume.brand, name: perfume.name, offerId, shops: wanted.map((t) => t.label), statuses: results.map((r) => r.status) },
  });
  // the text the operator sent becomes the shared description of the perfume (other volumes reuse it)
  const sentDescription = String((baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value || "");
  if (sentDescription.trim()) await saveFragranticaCardDescription(perfumeId, sentDescription).catch(() => {});
  return { ok: true, offerId, exports: results.map(fragranticaExportFromRow), export: fragranticaExportFromRow(results[0]), skippedTargets };
}

async function buildFragranticaYandexExtra({ perfume, type, name, volume, tester, shop, style = "parfumerius" }) {
  const category = resolveYandexCategoryForOzonProduct({ typeId: type.typeId, name });
  const params = category.categoryId && shop ? await fragranticaYandexCategoryParams(shop, category.categoryId).catch(() => []) : [];
  const facts = fragranticaFactsFromDetail(perfume);
  const video = fragranticaVideoCoverUrl(perfume.id, style);
  return {
    commodityCodes: [{ code: fragranticaTnvedCode(type.typeId), type: "CUSTOMS_COMMODITY_CODE" }, { code: fragOkpd2ForType(type.key), type: "OKPD2_CODE" }],
    shelfLife: { timePeriod: Number(FRAG_OZON_DEFAULTS.shelfLifeDays), timeUnit: "DAY" },
    // Market rating: a video (+8 points) — the same cover as on Ozon, 1080×1440 MP4
    ...(video ? { videos: [video] } : {}),
    parameterValues: buildFragranticaYandexParameters(params, {
      typeLabel: type.nameLabel.toLowerCase(),
      gender: { male: "мужской", female: "женский", unisex: "унисекс" }[perfume.gender] || "",
      family: facts.family,
      accords: facts.accords,
      year: facts.year,
      topNotes: facts.topNotes,
      middleNotes: facts.middleNotes,
      baseNotes: facts.baseNotes,
      netWeight: fragOzonNetWeight(volume),
      tester,
      // filterable «Линейка» (the perfume itself) and «Особенности флакона» (spray bottle; oils have none)
      line: fragStripConcentration(perfume.name),
      bottleFeature: type.key === "oil" ? "" : "с распылителем",
    }).concat(
      // the variant group: «brand + perfume + вид» like every other Market card — EDT and EDP of one perfume are
      // different groups (one group made Market call them «Дубль варианта»)
      category.categoryId === YANDEX_CATEGORY_PERFUMERY
        ? [{ parameterId: YANDEX_PARAM_VARIANT_GROUP, value: `${fragNameWithBrand(perfume)} ${type.nameLabel.toLowerCase()}`.replace(/\s+/g, " ").trim().slice(0, 255) }]
        : [],
    ),
  };
}

function fragranticaOzonMediaExtras({ perfumeId, style, notes, baseItem, categoryAttrs = [], perfume = {}, noBottle = false }) {
  const attributes = [];
  const ownRich = baseItem.attributes.some((a) => Number(a.id) === FRAG_RICH_ATTR && (a.values || []).length);
  if (!ownRich && process.env.FRAGRANTICA_OZON_RICH !== "false" && categoryAttrs.some((a) => Number(a.id) === FRAG_RICH_ATTR)) {
    const description = (baseItem.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value || "";
    const file = (name) => (fragFs.existsSync(fragranticaMediaPath("cards", name)) ? fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", name)) : "");
    const rich = buildFragranticaRichContent({
      title: fragNameWithBrand(perfume),
      description,
      images: noBottle ? { notes } : { notes, specs: file(`${Number(perfumeId)}-specs-${style}-v1.jpg`), closeup: file(`${Number(perfumeId)}-closeup-v1.jpg`) },
    });
    if (rich) attributes.push({ id: FRAG_RICH_ATTR, complex_id: 0, values: [{ value: rich }] });
  }
  const complex = process.env.FRAGRANTICA_OZON_VIDEO_COVER === "false" || noBottle ? [] : buildFragranticaVideoCoverComplex(fragranticaVideoCoverUrl(perfumeId, style));
  return { attributes, complex };
}

/** Why this perfume/volume can't go to Market (the same hard rules as every Market send). */
function fragranticaMarketBlockReasons(perfume = {}, { volume, typeKey, tester } = {}) {
  const vol = fragFormatVolume(volume);
  const type = fragOzonTypeByKey(typeKey || fragOzonGuessTypeKey(perfume));
  return yandexHardBlockReasons({
    name: buildFragranticaMarketName({ perfume, typeKey: type.key, volume: vol, tester: Boolean(tester) }),
    brand: perfume.brand,
  });
}

function fragranticaHttpError(statusCode, message, detail = {}) {
  const error = new Error(message);
  error.statusCode = statusCode;
  error.detail = detail;
  return error;
}

app.post("/api/fragrantica/ozon/export", requireAdmin, async (request, response, next) => {
  try {
    response.json(await createFragranticaExports(request.body || {}, request));
  } catch (error) {
    if (error?.detail?.code) return response.status(error.statusCode || 400).json({ error: error.message, ...error.detail });
    next(error);
  }
});

// ─── Дозаливка медиа в уже созданные карточки ────────────────────────────────
// Ozon: фото «Ноты крупно» + «Аккорды», Rich-контент, видеообложка. Маркет: те же фото, видео, название
// 60–120 знаков, «Линейка» и «Особенности флакона». Карточка обновляется по тому же offer_id.

const fragranticaMediaRefreshState = { running: false, total: 0, done: 0, failed: 0, startedAt: null, finishedAt: null, last: null };

async function refreshFragranticaExportMedia(row) {
  const perfumeId = Number(row.perfume_id);
  const perfume = await fragranticaPerfumeForExport(perfumeId);
  const target = fragranticaTargets().find((t) => t.kind === row.marketplace && t.id === cleanText(row.account_id));
  if (!target) return { skipped: "магазин не найден" };
  await runFragranticaImageJob({}, { perfumeId, styles: [target.style], refresh: false, waitVideo: true });
  const media = (file) => (fragFs.existsSync(fragranticaMediaPath("cards", file)) ? fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", file)) : "");
  const notes = media(`${perfumeId}-notes-${target.style}-v2.jpg`);
  const extras = fragranticaExtraPhotos(perfumeId, target.style);
  const item = { ...(row.item || {}) };
  const type = fragOzonTypeById(item.type_id) || fragOzonTypeByKey(fragOzonGuessTypeKey(perfume));
  // A short annotation (Market docks 20 points below ~1000 chars): the perfume's shared AI text, written now if missing
  const annotation = (item.attributes || []).find((a) => Number(a.id) === FRAG_OZON_ATTR.annotation);
  const currentText = String(annotation?.values?.[0]?.value || "").replace(/<[^>]+>/g, "");
  if (currentText.length < 800) {
    let text = await readFragranticaCardDescription(perfumeId).catch(() => "");
    if (text.length < 800) text = (await generateFragranticaDescription({ perfumeId, typeKey: type.key, tester: Boolean(row.tester), marketplace: "yandex" }).catch(() => ({}))).description || text;
    if (text.length > currentText.length) {
      const value = row.marketplace === "ozon" ? formatDescriptionForMarketplace(text, "ozon") : text;
      item.attributes = [...(item.attributes || []).filter((a) => Number(a.id) !== FRAG_OZON_ATTR.annotation), { id: FRAG_OZON_ATTR.annotation, complex_id: 0, values: [{ value }] }];
    }
  }
  if (row.marketplace === "ozon") {
    const account = fragranticaResolveOzonAccount(row.account_id);
    const categoryAttrs = await ozonGetCategoryAttributes(account, FRAG_OZON_CATEGORY_ID, type.typeId).catch(() => []);
    const attributes = (item.attributes || []).filter((a) => Number(a.id) !== FRAG_RICH_ATTR);
    const extra = fragranticaOzonMediaExtras({ perfumeId, style: target.style, notes, baseItem: { attributes }, categoryAttrs, perfume });
    // parfumdeclaration appended its «фото в конце» after our photos (Ozon re-hosts them): keep that tail
    const info = getOzonOfferMapValue(await getOzonProductInfoMap([row.offer_id], account, { continueOnError: true }), row.offer_id) || {};
    const current = [...[].concat(info.primary_image || []), ...(Array.isArray(info.images) ? info.images : [])].filter(Boolean);
    const tail = current.slice(1 + (Array.isArray(row.item?.images) ? row.item.images.length : 0));
    item.images = [...new Set([notes, ...extras, ...tail].filter(Boolean))].slice(0, 29);
    item.attributes = [...attributes, ...extra.attributes];
    if (extra.complex.length) item.complex_attributes = extra.complex;
  } else {
    const shop = fragranticaYandexShops().find((x) => cleanText(x.id) === cleanText(row.account_id));
    const volume = row.volume_ml === null || row.volume_ml === undefined ? "" : fragFormatVolume(row.volume_ml);
    const bottle = (item.yandexPictures || [])[0] || "";
    // keep what parfumdeclaration appended after our pictures («фото в конце»)
    const mapping = shop ? (await getYandexOfferMappingsByOfferIds(shop, [row.offer_id]).catch(() => []))[0] : null;
    const current = Array.isArray(mapping?.offer?.pictures) ? mapping.offer.pictures.map(String) : [];
    const tail = current.slice((item.yandexPictures || []).length);
    item.yandexPictures = [...new Set([bottle, notes, ...extras, ...tail].filter(Boolean))].slice(0, 20);
    item.yandexExtra = { ...(item.yandexExtra || {}), ...(await buildFragranticaYandexExtra({ perfume, type, name: item.name, volume, tester: Boolean(row.tester), shop, style: target.style })) };
    item.yandexName = buildFragranticaMarketName({ perfume, typeKey: type.key, volume, tester: Boolean(row.tester) });
  }
  const updated = await updateFragranticaExport(row.id, { item, status: "new", error: null, attempts: 0, next_attempt_at: null, task_id: null, result: { ...(row.result || {}), mediaRefresh: true } });
  return submitFragranticaTargetExport(updated);
}

async function runFragranticaMediaRefresh(ids = []) {
  const prisma = await requireFragranticaTables();
  const rows = ids.length
    ? await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = ANY($1::bigint[]) AND status = 'imported' ORDER BY id`, ids)
    : await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE status = 'imported' ORDER BY id`);
  Object.assign(fragranticaMediaRefreshState, { running: true, total: rows.length, done: 0, failed: 0, startedAt: new Date().toISOString(), finishedAt: null, last: null });
  for (const row of rows) {
    try {
      const res = await refreshFragranticaExportMedia(row);
      fragranticaMediaRefreshState.last = `${row.offer_id} ${row.account_name}: ${res?.status || res?.skipped || "ok"}`;
      if (res?.status === "failed") fragranticaMediaRefreshState.failed += 1;
    } catch (error) {
      fragranticaMediaRefreshState.failed += 1;
      fragranticaMediaRefreshState.last = `${row.offer_id} ${row.account_name}: ${error?.message || error}`;
      logger.warn("fragrantica media refresh failed", { id: Number(row.id), detail: error?.message });
    }
    fragranticaMediaRefreshState.done += 1;
  }
  Object.assign(fragranticaMediaRefreshState, { running: false, finishedAt: new Date().toISOString() });
  logger.info("fragrantica media refresh done", { ...fragranticaMediaRefreshState });
}

app.post("/api/fragrantica/ozon/exports/refresh-media", requireAdmin, async (request, response, next) => {
  try {
    if (fragranticaMediaRefreshState.running) return response.json({ ok: true, started: false, state: fragranticaMediaRefreshState });
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter((id) => id > 0);
    void runFragranticaMediaRefresh(ids).catch((error) => logger.warn("fragrantica media refresh crashed", { detail: error?.message }));
    response.json({ ok: true, started: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/exports/refresh-media", requireAdmin, (_request, response) => {
  response.json({ ok: true, state: fragranticaMediaRefreshState });
});

app.get("/api/fragrantica/ozon/exports", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireFragranticaTables();
    const perfumeId = Number(request.query.perfumeId) || null;
    const rows = await prisma.$queryRawUnsafe(
      `SELECT * FROM fragrantica_exports ${perfumeId ? "WHERE perfume_id = $1" : ""} ORDER BY id DESC LIMIT 200`,
      ...(perfumeId ? [perfumeId] : []),
    );
    response.json({ ok: true, items: rows.map(fragranticaExportFromRow) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/fragrantica/ozon/exports/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    let row = await readFragranticaExport(request.params.id);
    if (!row) return response.status(404).json({ error: "Экспорт не найден." });
    row = await refreshFragranticaExport(row).catch((error) => {
      logger.warn("fragrantica export refresh failed", { id: Number(row.id), detail: error?.message });
      return row;
    });
    response.json({ ok: true, export: fragranticaExportFromRow(row) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/ozon/exports/:id/retry", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const row = await readFragranticaExport(request.params.id);
    if (!row) return response.status(404).json({ error: "Экспорт не найден." });
    if (!["failed", "queued_limit"].includes(row.status)) return response.status(409).json({ error: `Статус ${row.status} — повтор не нужен.` });
    const submitted = await submitFragranticaTargetExport(row);
    response.json({ ok: true, export: fragranticaExportFromRow(submitted) });
  } catch (error) {
    next(error);
  }
});

// Worker: очередь лимита (после 03:00 МСК) и досмотр pending / штрихкодов.
async function runFragranticaExportQueueTick() {
  const prisma = await requireFragranticaTables();
  const due = await prisma.$queryRawUnsafe(
    `SELECT * FROM fragrantica_exports
     WHERE (status = 'queued_limit' AND (next_attempt_at IS NULL OR next_attempt_at <= now()))
        OR (status = 'pending' AND marketplace = 'ozon' AND updated_at < now() - interval '1 minute')
        OR (status = 'imported' AND result->>'links' = 'pending' AND updated_at < now() - interval '2 minutes')
        OR (status = 'imported' AND marketplace = 'ozon' AND result->>'barcode' = 'pending' AND updated_at < now() - interval '30 minutes')
        OR (status = 'imported' AND result->>'docs' = 'pending' AND updated_at < now() - interval '2 minutes')
     ORDER BY id LIMIT 50`,
  );
  let sent = 0;
  for (const row of due) {
    try {
      if (row.status === "queued_limit") {
        const updated = await submitFragranticaTargetExport(row);
        if (updated.status === "queued_limit") break; // лимит всё ещё исчерпан — остальные ждут
        sent += 1;
      } else {
        await refreshFragranticaExport(row);
      }
    } catch (error) {
      logger.warn("fragrantica export queue item failed", { id: Number(row.id), detail: error?.message });
    }
  }
  if (due.length) logger.info("fragrantica export queue tick", { due: due.length, sent });
}

function scheduleFragranticaExportQueue(delayMs = fragranticaExportQueueIntervalMs) {
  if (!fragranticaExportQueueEnabled) return;
  setTimeout(async () => {
    try {
      await runFragranticaExportQueueTick();
    } catch (error) {
      logger.warn("fragrantica export queue tick failed", { detail: error?.message });
    } finally {
      scheduleFragranticaExportQueue(fragranticaExportQueueIntervalMs);
    }
  }, Math.max(10_000, Number(delayMs) || fragranticaExportQueueIntervalMs)).unref?.();
}
