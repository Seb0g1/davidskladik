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

// ─── Каталог: список и карточка ─────────────────────────────────────────────

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
  };
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
  if (query.exported === "yes") where.push("EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id)");
  if (query.exported === "no") where.push("NOT EXISTS (SELECT 1 FROM fragrantica_exports e WHERE e.perfume_id = p.id)");
  const order = {
    popular: "p.votes DESC NULLS LAST, p.year DESC NULLS LAST, p.id DESC",
    new: "p.year DESC NULLS LAST, p.id DESC",
    name: "p.brand ASC, p.name ASC",
  }[query.sort] || "p.votes DESC NULLS LAST, p.year DESC NULLS LAST, p.id DESC";
  const limit = Math.min(120, Math.max(1, Number(query.limit) || 60));
  const page = Math.max(1, Number(query.page) || 1);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await prisma.$queryRawUnsafe(
    `SELECT p.id, p.url, p.brand, p.brand_slug, p.name, p.gender, p.year, p.votes, p.rating, p.detail_at,
            jsonb_build_object('accords', p.detail->'accords') AS detail,
            (SELECT COALESCE(jsonb_agg(jsonb_build_object('offerId', e.offer_id, 'status', e.status, 'account', e.account_name, 'volume', e.volume_ml) ORDER BY e.id), '[]'::jsonb)
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
      (SELECT COUNT(*)::int FROM fragrantica_exports) AS exports`);
  const row = rows[0] || {};
  return {
    brands: row.brands || 0,
    brandsCrawled: row.brands_crawled || 0,
    perfumes: row.perfumes || 0,
    details: row.details || 0,
    exports: row.exports || 0,
  };
}
