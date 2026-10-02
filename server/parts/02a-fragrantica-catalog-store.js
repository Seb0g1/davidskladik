// Каталог «Фрагрантика»: таблицы PG (лениво, без prisma-миграции — как sweep_heartbeats),
// загрузка страниц fragrantica.ru и хранение фото на davidsklad.ru (public/uploads/fragrantica).
//
//   fragrantica_brands    — бренды из /designers-N/ (crawled_at — когда обошли страницу бренда)
//   fragrantica_perfumes  — ароматы со страниц брендов; detail — разобранная страница аромата
//   fragrantica_exports   — созданные из каталога карточки маркетплейсов (02d-fragrantica-ozon-export.js)
//   fragrantica_state     — состояние фонового обхода (пауза, блокировка) — общее для api и worker

const fragFs = require("fs");
const fragranticaMediaDir = path.join(publicDir, "uploads", "fragrantica");
// parfumdeclaration.ru: обработка фото / картинка нот и прокси страниц Фрагрантики (IP davidsklad
// получает от Cloudflare проверку «Just a moment», сервер parfumdeclaration — нет).
const fragranticaPdToolsUrl = cleanText(process.env.PD_TOOLS_URL || "https://parfumdeclaration.ru").replace(/\/+$/, "");
const fragranticaPdToolsToken = cleanText(process.env.PD_TOOLS_TOKEN || "");
// auto — напрямую, при проверке Cloudflare через parfumdeclaration (6 ч); direct | pd — принудительно.
const fragranticaFetchMode = cleanText(process.env.FRAGRANTICA_FETCH_VIA || "auto").toLowerCase();
let fragranticaDirectBlockedUntil = 0;
const fragranticaUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36";

let fragranticaTablesReady = false;
async function ensureFragranticaTables() {
  if (fragranticaTablesReady) return true;
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return false;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_brands (
      slug TEXT PRIMARY KEY,
      url TEXT NOT NULL,
      name TEXT NOT NULL,
      perfume_count INTEGER,
      crawled_at TIMESTAMPTZ,
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_perfumes (
      id INTEGER PRIMARY KEY,
      url TEXT NOT NULL,
      brand_slug TEXT NOT NULL DEFAULT '',
      brand TEXT NOT NULL DEFAULT '',
      name TEXT NOT NULL DEFAULT '',
      gender TEXT NOT NULL DEFAULT '',
      year INTEGER,
      search TEXT NOT NULL DEFAULT '',
      votes INTEGER,
      rating REAL,
      detail JSONB,
      detail_at TIMESTAMPTZ,
      detail_error TEXT,
      detail_wanted_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await prisma.$executeRawUnsafe(`ALTER TABLE fragrantica_brands ADD COLUMN IF NOT EXISTS country TEXT, ADD COLUMN IF NOT EXISTS owner TEXT`);
  await prisma.$executeRawUnsafe(`ALTER TABLE fragrantica_perfumes ADD COLUMN IF NOT EXISTS pm_rows INTEGER, ADD COLUMN IF NOT EXISTS pm_min_usd NUMERIC, ADD COLUMN IF NOT EXISTS pm_volumes JSONB, ADD COLUMN IF NOT EXISTS pm_checked_at TIMESTAMPTZ`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_perfumes_pm_idx ON fragrantica_perfumes (pm_rows DESC NULLS LAST) WHERE pm_rows > 0`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_perfumes_brand_idx ON fragrantica_perfumes (brand_slug)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_perfumes_year_idx ON fragrantica_perfumes (year DESC NULLS LAST)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_perfumes_votes_idx ON fragrantica_perfumes (votes DESC NULLS LAST)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_perfumes_pending_idx ON fragrantica_perfumes (detail_wanted_at) WHERE detail_at IS NULL`);
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_exports (
      id BIGSERIAL PRIMARY KEY,
      perfume_id INTEGER NOT NULL,
      marketplace TEXT NOT NULL DEFAULT 'ozon',
      account_id TEXT NOT NULL,
      account_name TEXT NOT NULL DEFAULT '',
      offer_id TEXT NOT NULL,
      volume_ml NUMERIC,
      tester BOOLEAN NOT NULL DEFAULT false,
      status TEXT NOT NULL,
      task_id BIGINT,
      product_id BIGINT,
      item JSONB NOT NULL,
      result JSONB,
      error TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at TIMESTAMPTZ,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await prisma.$executeRawUnsafe(`ALTER TABLE fragrantica_exports ADD COLUMN IF NOT EXISTS links JSONB`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_exports_perfume_idx ON fragrantica_exports (perfume_id)`);
  await prisma.$executeRawUnsafe(`CREATE UNIQUE INDEX IF NOT EXISTS fragrantica_exports_offer_idx ON fragrantica_exports (account_id, offer_id)`);
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_state (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  fragranticaTablesReady = true;
  return true;
}

async function requireFragranticaTables() {
  if (!(await ensureFragranticaTables())) {
    const error = new Error("Каталог Фрагрантики требует PostgreSQL.");
    error.statusCode = 503;
    throw error;
  }
  return getPrisma();
}

function fragranticaSearchText(...parts) {
  return parts.join(" ").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/\s+/g, " ").trim();
}

async function saveFragranticaBrandInfo(slug, info = {}) {
  const prisma = await requireFragranticaTables();
  await prisma.$executeRawUnsafe(
    `UPDATE fragrantica_brands SET country = COALESCE(NULLIF($2, ''), country), owner = COALESCE(NULLIF($3, ''), owner) WHERE slug = $1`,
    slug,
    cleanText(info.country),
    cleanText(info.owner),
  );
}

// Страна и владелец бренда (страница бренда Фрагрантики); нет в базе — загрузим один раз.
async function fragranticaBrandInfo(brandSlug) {
  const slug = cleanText(brandSlug);
  if (!slug) return { country: "", owner: "" };
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT url, country, owner FROM fragrantica_brands WHERE slug = $1`, slug);
  if (rows[0]?.country) return { country: rows[0].country, owner: rows[0].owner || "" };
  const url = rows[0]?.url || `https://www.fragrantica.ru/designers/${slug}.html`;
  const info = parseFragranticaBrandInfo(await fetchFragranticaHtml(url));
  if (rows[0]) await saveFragranticaBrandInfo(slug, info);
  return info;
}

async function fragranticaManagedOfferIds() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return new Set();
  const rows = await prisma.$queryRawUnsafe(`SELECT DISTINCT lower(offer_id) AS offer FROM fragrantica_exports`).catch(() => []);
  return new Set(rows.map((r) => r.offer));
}

// Факты Фрагрантики для описания товара склада: бренд совпадает, все слова названия аромата есть
// в названии товара (из нескольких — самый длинный, т. е. самый точный: «Sauvage Elixir» > «Sauvage»).
async function fragranticaFactsForProductName(brand, productName) {
  const prisma = getPrisma();
  if (!prisma || !cleanText(brand) || !cleanText(productName)) return null;
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id, name, detail FROM fragrantica_perfumes WHERE lower(brand) = lower($1) AND detail_at IS NOT NULL LIMIT 2000`,
    cleanText(brand),
  ).catch(() => []);
  const productTokens = new Set(fragranticaSearchText(productName).split(/[^0-9a-zа-я]+/).filter(Boolean));
  const best = rows
    .map((r) => ({ r, tokens: fragranticaSearchText(r.name).split(/[^0-9a-zа-я]+/).filter(Boolean) }))
    .filter((x) => x.tokens.length && x.tokens.every((t) => productTokens.has(t)))
    .sort((a, b) => b.tokens.length - a.tokens.length)[0];
  return best ? fragranticaFactsFromDetail(best.r.detail || {}) : null;
}

function fragranticaFactsFromDetail(detail = {}) {
  const names = (list) => (Array.isArray(list) ? list.map((n) => cleanText(n.name)).filter(Boolean) : []);
  const notes = detail.notes || {};
  const facts = {
    topNotes: names(notes.top),
    middleNotes: names(notes.middle),
    baseNotes: names(notes.base),
    notes: names(notes.flat),
    accords: (detail.accords || []).slice(0, 6).map((a) => cleanText(a.name)).filter(Boolean),
    family: cleanText(detail.family) || undefined,
    perfumers: (detail.perfumers || []).filter(Boolean),
    year: detail.year || undefined,
    fragranceDescription: cleanText(detail.description).slice(0, 1500) || undefined,
  };
  return Object.fromEntries(Object.entries(facts).filter(([, v]) => (Array.isArray(v) ? v.length : v !== undefined && v !== "")));
}

async function readFragranticaState(key) {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT value FROM fragrantica_state WHERE key = $1`, key);
  return rows[0]?.value || {};
}

async function writeFragranticaState(key, patch) {
  const prisma = await requireFragranticaTables();
  await prisma.$executeRawUnsafe(
    `INSERT INTO fragrantica_state (key, value, updated_at) VALUES ($1, $2::jsonb, now())
     ON CONFLICT (key) DO UPDATE SET value = fragrantica_state.value || EXCLUDED.value, updated_at = now()`,
    key,
    JSON.stringify(patch || {}),
  );
}

async function upsertFragranticaBrands(brands = []) {
  if (!brands.length) return 0;
  const prisma = await requireFragranticaTables();
  for (let i = 0; i < brands.length; i += 500) {
    const chunk = brands.slice(i, i + 500);
    await prisma.$executeRawUnsafe(
      `INSERT INTO fragrantica_brands (slug, url, name, perfume_count)
       SELECT b->>'slug', b->>'url', b->>'name', NULLIF(b->>'perfumeCount', '')::int FROM jsonb_array_elements($1::jsonb) b
       ON CONFLICT (slug) DO UPDATE SET url = EXCLUDED.url, name = EXCLUDED.name,
         perfume_count = COALESCE(EXCLUDED.perfume_count, fragrantica_brands.perfume_count)`,
      JSON.stringify(chunk),
    );
  }
  return brands.length;
}

async function upsertFragranticaListedPerfumes(perfumes = []) {
  if (!perfumes.length) return 0;
  const prisma = await requireFragranticaTables();
  const rows = perfumes.map((p) => ({ ...p, search: fragranticaSearchText(p.brand, p.name) }));
  for (let i = 0; i < rows.length; i += 500) {
    await prisma.$executeRawUnsafe(
      `INSERT INTO fragrantica_perfumes (id, url, brand_slug, brand, name, gender, year, search)
       SELECT (p->>'id')::int, p->>'url', COALESCE(p->>'brandSlug', ''), COALESCE(p->>'brand', ''), COALESCE(p->>'name', ''),
              COALESCE(p->>'gender', ''), NULLIF(p->>'year', '')::int, p->>'search'
       FROM jsonb_array_elements($1::jsonb) p
       ON CONFLICT (id) DO UPDATE SET url = EXCLUDED.url, brand_slug = EXCLUDED.brand_slug, brand = EXCLUDED.brand,
         name = EXCLUDED.name, gender = COALESCE(NULLIF(EXCLUDED.gender, ''), fragrantica_perfumes.gender),
         year = COALESCE(EXCLUDED.year, fragrantica_perfumes.year), search = EXCLUDED.search, updated_at = now()`,
      JSON.stringify(rows.slice(i, i + 500)),
    );
  }
  return rows.length;
}

async function saveFragranticaPerfumeDetail(detail) {
  const prisma = await requireFragranticaTables();
  await prisma.$executeRawUnsafe(
    `INSERT INTO fragrantica_perfumes (id, url, brand_slug, brand, name, gender, year, search, votes, rating, detail, detail_at, detail_error)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, now(), NULL)
     ON CONFLICT (id) DO UPDATE SET url = EXCLUDED.url, brand_slug = EXCLUDED.brand_slug, brand = EXCLUDED.brand,
       name = EXCLUDED.name, gender = COALESCE(NULLIF(EXCLUDED.gender, ''), fragrantica_perfumes.gender),
       year = COALESCE(EXCLUDED.year, fragrantica_perfumes.year), search = EXCLUDED.search, votes = EXCLUDED.votes,
       rating = EXCLUDED.rating, detail = EXCLUDED.detail, detail_at = now(), detail_error = NULL, updated_at = now()`,
    detail.id,
    detail.url,
    detail.brandSlug || "",
    detail.brand || "",
    detail.name || "",
    detail.gender || "",
    detail.year || null,
    fragranticaSearchText(detail.brand, detail.name),
    detail.votes || null,
    detail.rating || null,
    JSON.stringify(detail),
  );
}

async function markFragranticaDetailError(id, message) {
  const prisma = await requireFragranticaTables();
  await prisma.$executeRawUnsafe(
    `UPDATE fragrantica_perfumes SET detail_error = $2, detail_wanted_at = NULL, updated_at = now() WHERE id = $1`,
    id,
    String(message || "").slice(0, 500),
  );
}

// ─── HTTP ───────────────────────────────────────────────────────────────────

class FragranticaHttpError extends Error {
  constructor(status, url) {
    super(`Фрагрантика ответила ${status} (${url})`);
    this.status = status;
  }
}

async function fetchFragranticaHtmlViaPd(url) {
  if (!fragranticaPdToolsToken) throw new FragranticaHttpError(403, url);
  const response = await fetch(`${fragranticaPdToolsUrl}/api/tools/fragrantica/fetch`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-tools-token": fragranticaPdToolsToken },
    body: JSON.stringify({ url }),
    signal: AbortSignal.timeout(60_000),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `parfumdeclaration ответил ${response.status}`);
  if (Number(data.status) !== 200) throw new FragranticaHttpError(Number(data.status) || 502, url);
  const html = String(data.html || "");
  if (/<title>\s*Just a moment|cf-chl-|challenge-platform/i.test(html.slice(0, 5000))) throw new FragranticaHttpError(403, url);
  return html;
}

async function fetchFragranticaHtml(url) {
  const viaPd = fragranticaPdToolsToken && (fragranticaFetchMode === "pd" || (fragranticaFetchMode === "auto" && Date.now() < fragranticaDirectBlockedUntil));
  if (viaPd) return fetchFragranticaHtmlViaPd(url);
  try {
    return await fetchFragranticaHtmlDirect(url);
  } catch (error) {
    if (fragranticaFetchMode === "auto" && fragranticaPdToolsToken && error instanceof FragranticaHttpError && error.status === 403) {
      fragranticaDirectBlockedUntil = Date.now() + 6 * 3_600_000;
      logger.info("fragrantica direct fetch challenged, using parfumdeclaration proxy for 6 h");
      return fetchFragranticaHtmlViaPd(url);
    }
    throw error;
  }
}

async function fetchFragranticaHtmlDirect(url) {
  const response = await fetch(url, {
    headers: {
      "User-Agent": fragranticaUserAgent,
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "ru-RU,ru;q=0.9",
      Referer: "https://www.fragrantica.ru/",
    },
    redirect: "follow",
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) throw new FragranticaHttpError(response.status, url);
  const html = await response.text();
  if (/<title>\s*Just a moment|cf-chl-|challenge-platform/i.test(html.slice(0, 5000))) throw new FragranticaHttpError(403, url);
  return html;
}

function normalizeFragranticaPerfumeUrl(input) {
  const value = cleanText(input);
  const match = value.match(/^https?:\/\/(?:www\.)?fragrantica\.[a-z.]+\/(?:perfume|parfum|perfumes|perfumy|Parfum)\/([^/]+)\/([^/?#]+-(\d+)\.html)/i);
  if (!match) return null;
  return { id: Number(match[3]), url: `https://www.fragrantica.ru/perfume/${match[1]}/${match[2]}` };
}

// Загрузить и сохранить страницу аромата (по требованию из UI или обходом).
async function fetchAndStoreFragranticaPerfume(url) {
  const html = await fetchFragranticaHtml(url);
  const detail = parseFragranticaPerfumePage(html, { url });
  if (!detail.id || !detail.name) throw new Error("Не удалось разобрать страницу аромата.");
  await saveFragranticaPerfumeDetail(detail);
  return detail;
}

// ─── Фото на нашем сервере ──────────────────────────────────────────────────
//   thumbs/<id>.jpg  — превью для списка (270×270 с Фрагрантики), кешируется при первом показе
//   images/<id>.jpg  — фото флакона 750×1000 (исходник для обработки)
//   cards/<name>.jpg — обработанные картинки для карточек (фото флакона, «Пирамида аромата»)

function fragranticaMediaPath(kind, file) {
  const safe = String(file || "").replace(/[^a-zA-Z0-9._-]/g, "");
  return path.join(fragranticaMediaDir, kind, safe);
}

function fragranticaMediaUrl(kind, file) {
  return `/uploads/fragrantica/${kind}/${file}`;
}

function fragranticaPublicBaseUrl() {
  return cleanText(process.env.PUBLIC_BASE_URL || process.env.APP_PUBLIC_URL || "https://davidsklad.ru").replace(/\/+$/, "");
}

const fragranticaDownloads = new Map();
async function downloadFragranticaMedia(sourceUrl, kind, file) {
  const target = fragranticaMediaPath(kind, file);
  if (fragFs.existsSync(target)) return target;
  if (fragranticaDownloads.has(target)) return fragranticaDownloads.get(target);
  const job = (async () => {
    const response = await fetch(sourceUrl, { headers: { "User-Agent": fragranticaUserAgent, Referer: "https://www.fragrantica.ru/" }, signal: AbortSignal.timeout(30_000) });
    if (!response.ok) throw new FragranticaHttpError(response.status, sourceUrl);
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length < 200) throw new Error("Пустое изображение.");
    await fragFs.promises.mkdir(path.dirname(target), { recursive: true });
    const tmp = `${target}.${process.pid}.tmp`;
    await fragFs.promises.writeFile(tmp, buffer);
    await fragFs.promises.rename(tmp, target);
    return target;
  })().finally(() => fragranticaDownloads.delete(target));
  fragranticaDownloads.set(target, job);
  return job;
}

function fragranticaThumbSource(id) {
  return `https://fimgs.net/mdimg/perfume-thumbs/270x270.${Number(id)}.jpg`;
}

function fragranticaImageSource(id) {
  return `https://fimgs.net/mdimg/perfume-thumbs/375x500.${Number(id)}.2x.jpg`;
}

async function ensureFragranticaPerfumeImage(id) {
  return downloadFragranticaMedia(fragranticaImageSource(id), "images", `${Number(id)}.jpg`);
}

// ─── Чёткий исходник для карточки ───────────────────────────────────────────
// Превью 750×1000 сильно пережато. У Фрагрантики есть оригинал o.<id>.jpg (часто 1400–3400 px), но в нём
// бывает отражение под флаконом. Превью — тот же снимок, обрезанный по флакону, поэтому масштаб берём
// по ширине флакона, высоту — из превью (отражение отрезается), а совпадение проверяем сравнением
// уменьшенных копий. Не вышло (оригинал не больше, другой снимок) — null, работаем с превью.

function fragranticaInkBox(data, w, h, ch, limit = 238) {
  const rows = new Uint32Array(h);
  const cols = new Uint32Array(w);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const i = (y * w + x) * ch;
    if (Math.min(data[i], data[i + 1], data[i + 2]) < limit) { rows[y]++; cols[x]++; }
  }
  const minRow = Math.max(2, Math.round(w * 0.004));
  const minCol = Math.max(2, Math.round(h * 0.004));
  let top = 0; while (top < h && rows[top] < minRow) top++;
  let bottom = h - 1; while (bottom > top && rows[bottom] < minRow) bottom--;
  let left = 0; while (left < w && cols[left] < minCol) left++;
  let right = w - 1; while (right > left && cols[right] < minCol) right--;
  if (top >= bottom || left >= right) return null;
  return { left, top, width: right - left + 1, height: bottom - top + 1 };
}

async function buildFragranticaHdSource(thumbBuffer, originalBuffer) {
  const decode = async (input) => {
    const { data, info } = await sharp(input, { failOn: "none" }).rotate().removeAlpha().raw().toBuffer({ resolveWithObject: true });
    return { data, w: info.width, h: info.height, ch: info.channels };
  };
  const thumb = await decode(thumbBuffer);
  const original = await decode(originalBuffer);
  if (original.w * original.h > 40_000_000) return { reason: "оригинал слишком большой" };
  const tb = fragranticaInkBox(thumb.data, thumb.w, thumb.h, thumb.ch);
  const ob = fragranticaInkBox(original.data, original.w, original.h, original.ch);
  if (!tb || !ob) return { reason: "флакон не найден" };
  if (ob.width < tb.width * 1.15) return { reason: "оригинал не крупнее превью" };
  const height = Math.min(ob.height, Math.round(tb.height * (ob.width / tb.width)));
  const box = { left: ob.left, top: ob.top, width: ob.width, height };
  const small = (input, area) => sharp(input, { failOn: "none" }).rotate().extract(area).resize(32, 32, { fit: "fill" }).greyscale().raw().toBuffer();
  const [a, b] = await Promise.all([small(thumbBuffer, tb), small(originalBuffer, box)]);
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff += Math.abs(a[i] - b[i]);
  if (diff / a.length > 14) return { reason: "в оригинале другой снимок" };
  const pad = Math.round(Math.max(box.width, box.height) * 0.08);
  const buffer = await sharp(originalBuffer, { failOn: "none" }).rotate().removeAlpha().extract(box)
    .extend({ top: pad, bottom: pad, left: pad, right: pad, background: { r: 255, g: 255, b: 255 } })
    .jpeg({ quality: 97, chromaSubsampling: "4:4:4" })
    .toBuffer();
  return { buffer, scale: ob.width / tb.width };
}

// URL чёткого исходника images/<id>-hd.jpg или null (тогда — превью images/<id>.jpg).
async function ensureFragranticaHdImage(id) {
  const file = `${Number(id)}-hd.jpg`;
  const target = fragranticaMediaPath("images", file);
  const miss = fragranticaMediaPath("images", `${Number(id)}-hd.none`);
  if (fragFs.existsSync(target)) return fragranticaMediaUrl("images", file);
  if (fragFs.existsSync(miss)) return null;
  try {
    const thumbPath = await ensureFragranticaPerfumeImage(id);
    const originalPath = await downloadFragranticaMedia(`https://fimgs.net/mdimg/perfume/o.${Number(id)}.jpg`, "images", `${Number(id)}-o.jpg`);
    const result = await buildFragranticaHdSource(await fragFs.promises.readFile(thumbPath), await fragFs.promises.readFile(originalPath));
    if (!result.buffer) {
      await fragFs.promises.writeFile(miss, result.reason || "");
      return null;
    }
    await fragFs.promises.writeFile(target, result.buffer);
    return fragranticaMediaUrl("images", file);
  } catch (error) {
    logger.warn("fragrantica hd source failed", { id: Number(id), detail: error?.message });
    return null;
  }
}

// ─── Каталог: список и карточка ─────────────────────────────────────────────

function fragranticaPmFromRow(row = {}) {
  if (!row.pm_checked_at) return null;
  return {
    rows: Number(row.pm_rows || 0),
    minUsd: row.pm_min_usd === null || row.pm_min_usd === undefined ? null : Number(row.pm_min_usd),
    volumes: Array.isArray(row.pm_volumes) ? row.pm_volumes.map(Number) : [],
  };
}

function fragranticaRowToListItem(row = {}) {
  const detail = row.detail || null;
  return {
    id: Number(row.id),
    url: row.url,
    brand: row.brand,
    brandSlug: row.brand_slug,
    name: row.name,
    gender: row.gender || "",
    year: row.year ?? null,
    votes: row.votes ?? null,
    rating: row.rating ?? null,
    thumb: fragranticaMediaUrl("thumbs", `${Number(row.id)}.jpg`),
    hasDetail: Boolean(row.detail_at),
    accords: detail?.accords ? detail.accords.slice(0, 4).map((a) => ({ name: a.name, background: a.background, color: a.color })) : [],
    exported: Array.isArray(row.exported) ? row.exported : [],
    pm: fragranticaPmFromRow(row),
  };
}

const FRAGRANTICA_PENDING_SQL = "('new', 'pending', 'running', 'queued_limit')";

/** Per shop (account_id): how many perfumes are added / waiting / failed — for the control bar on the page. */
async function fragranticaShopCounts() {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT account_id AS "accountId",
            COUNT(DISTINCT perfume_id) FILTER (WHERE status NOT IN ('failed') AND status NOT IN ${FRAGRANTICA_PENDING_SQL})::int AS added,
            COUNT(DISTINCT perfume_id) FILTER (WHERE status IN ${FRAGRANTICA_PENDING_SQL})::int AS pending,
            COUNT(DISTINCT perfume_id) FILTER (WHERE status = 'failed')::int AS failed,
            COUNT(*) FILTER (WHERE status NOT IN ('failed'))::int AS offers
       FROM fragrantica_exports GROUP BY account_id`,
  );
  return new Map(rows.map((r) => [String(r.accountId), r]));
}

async function listFragranticaPerfumes(query = {}) {
  const prisma = await requireFragranticaTables();
  const where = [];
  const params = [];
  const add = (sql, value) => { params.push(value); where.push(sql.replace("?", `$${params.length}`)); };
  const q = fragranticaSearchText(cleanText(query.q));
  if (q) {
    for (const token of q.split(" ").filter(Boolean).slice(0, 6)) add("p.search LIKE ?", `%${token}%`);
  }
  if (cleanText(query.brand)) add("p.brand_slug = ?", cleanText(query.brand));
  if (["male", "female", "unisex"].includes(query.gender)) add("p.gender = ?", query.gender);
  if (Number(query.yearFrom)) add("p.year >= ?", Number(query.yearFrom));
  if (Number(query.yearTo)) add("p.year <= ?", Number(query.yearTo));
  // Shop filters: a failed export does not count as «added»
  const exported = cleanText(query.exported);
  const live = "e.status <> 'failed'";
  if (exported === "yes") where.push(`EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND ${live})`);
  if (exported === "no") where.push(`NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND ${live})`);
  if (exported === "ozon" || exported === "yandex") add(`EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND ${live} AND e.marketplace = ?)`, exported);
  if (exported === "failed") where.push("EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND e.status = 'failed')");
  if (exported === "pending") where.push(`EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND e.status IN ${FRAGRANTICA_PENDING_SQL})`);
  const shopIn = /^in:(.+)$/.exec(exported);
  const shopOut = /^out:(.+)$/.exec(exported);
  if (shopIn) add(`EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND ${live} AND e.account_id = ?)`, shopIn[1]);
  if (shopOut) add(`NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id AND ${live} AND e.account_id = ?)`, shopOut[1]);
  if (query.pm === "yes") where.push("p.pm_rows > 0");
  if (query.pm === "no") where.push("COALESCE(p.pm_rows, 0) = 0");
  const order = {
    popular: "p.votes DESC NULLS LAST, p.year DESC NULLS LAST, p.id DESC",
    new: "p.year DESC NULLS LAST, p.id DESC",
    name: "p.brand ASC, p.name ASC",
    pm: "p.pm_rows DESC NULLS LAST, p.votes DESC NULLS LAST, p.id DESC",
  }[query.sort] || "p.votes DESC NULLS LAST, p.year DESC NULLS LAST, p.id DESC";
  const limit = Math.min(120, Math.max(1, Number(query.limit) || 60));
  const page = Math.max(1, Number(query.page) || 1);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await prisma.$queryRawUnsafe(
    `SELECT p.id, p.url, p.brand, p.brand_slug, p.name, p.gender, p.year, p.votes, p.rating, p.detail_at,
            p.pm_rows, p.pm_min_usd, p.pm_volumes, p.pm_checked_at,
            jsonb_build_object('accords', p.detail->'accords') AS detail,
            (SELECT COALESCE(jsonb_agg(jsonb_build_object('offerId', e.offer_id, 'status', e.status, 'account', e.account_name, 'accountId', e.account_id, 'marketplace', e.marketplace, 'volume', e.volume_ml, 'tester', e.tester, 'error', left(e.error, 300)) ORDER BY e.id), '[]'::jsonb)
               FROM fragrantica_exports e WHERE e.perfume_id = p.id) AS exported
     FROM fragrantica_perfumes p ${whereSql}
     ORDER BY ${order} LIMIT ${limit + 1} OFFSET ${(page - 1) * limit}`,
    ...params,
  );
  const countRows = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::int AS total FROM fragrantica_perfumes p ${whereSql}`, ...params);
  return {
    items: rows.slice(0, limit).map(fragranticaRowToListItem),
    hasMore: rows.length > limit,
    total: countRows[0]?.total || 0,
    page,
  };
}

async function readFragranticaPerfume(id) {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_perfumes WHERE id = $1`, Number(id));
  return rows[0] || null;
}

async function listFragranticaBrands({ q = "", limit = 30 } = {}) {
  const prisma = await requireFragranticaTables();
  const search = `%${cleanText(q).toLowerCase()}%`;
  return prisma.$queryRawUnsafe(
    `SELECT b.slug, b.name, b.perfume_count AS "perfumeCount" FROM fragrantica_brands b
     WHERE lower(b.name) LIKE $1 ORDER BY b.perfume_count DESC NULLS LAST, b.name LIMIT ${Math.min(100, Math.max(1, Number(limit) || 30))}`,
    search,
  );
}

async function readFragranticaCatalogStats() {
  const prisma = await requireFragranticaTables();
  const rows = await prisma.$queryRawUnsafe(`
    SELECT
      (SELECT COUNT(*)::int FROM fragrantica_brands) AS brands,
      (SELECT COUNT(*)::int FROM fragrantica_brands WHERE crawled_at IS NOT NULL) AS brands_crawled,
      (SELECT COUNT(*)::int FROM fragrantica_perfumes) AS perfumes,
      (SELECT COUNT(*)::int FROM fragrantica_perfumes WHERE detail_at IS NOT NULL) AS details,
      (SELECT COUNT(*)::int FROM fragrantica_exports) AS exports,
      (SELECT COUNT(*)::int FROM fragrantica_perfumes WHERE pm_rows > 0) AS in_pm`);
  const row = rows[0] || {};
  return {
    brands: row.brands || 0,
    brandsCrawled: row.brands_crawled || 0,
    perfumes: row.perfumes || 0,
    details: row.details || 0,
    exports: row.exports || 0,
    inPm: row.in_pm || 0,
  };
}
