// Fragrantica.ru scraper — fetches fragrance notes/accords and caches for 30 days.
// Graceful: returns null on any network/parse failure, never throws to caller.

const _fragFs = require("fs");
const _fragPath = require("path");
const _fragCrypto = require("crypto");

const CACHE_FILE = _fragPath.join(__dirname, "../../data/fragrantica-cache.json");
const CACHE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const FETCH_TIMEOUT_MS = 8000;

const FRAGRANTICA_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  Referer: "https://www.fragrantica.ru/",
};

function cacheKey(brand, name) {
  return _fragCrypto.createHash("md5").update(`${brand}|${name}`).digest("hex");
}

function readCache() {
  try {
    const raw = _fragFs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function writeCache(cache) {
  try {
    _fragFs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), "utf8");
  } catch {
    // non-fatal
  }
}

function getFragranticaCache(brand, name) {
  const cache = readCache();
  const entry = cache[cacheKey(brand, name)];
  if (!entry) return null;
  if (Date.now() - new Date(entry.cachedAt).getTime() > CACHE_TTL_MS) return null;
  return entry;
}

function setFragranticaCache(brand, name, data) {
  const cache = readCache();
  cache[cacheKey(brand, name)] = { brand, name, cachedAt: new Date().toISOString(), ...data };
  writeCache(cache);
}

// Extract text content from HTML tag matches — strips inner HTML tags.
function extractTexts(html, pattern) {
  const results = [];
  let match;
  const re = new RegExp(pattern, "gi");
  while ((match = re.exec(html)) !== null) {
    const inner = match[1].replace(/<[^>]+>/g, "").trim();
    if (inner) results.push(inner);
  }
  return results;
}

function parseFragranticaPage(html) {
  // Notes: top/middle/base sections in Fragrantica HTML
  // <div class="cell text-left"> ... <b>Верхние ноты:</b> <span itemprop="description">...</span>
  // More reliable: <span itemprop="keywords"> note1, note2 </span> sections
  // Also: accord badges <div class="accord-box"> ... <div class="accord-bar"> style="..." <br>Name</div>

  const data = {};

  // --- Accords ---
  // <div class="accord-box"><div class="accord-bar" style="width: ...%; ..."><br>AccordName</div></div>
  const accords = [];
  const accordRe = /<div[^>]+class="accord-bar"[^>]*>.*?<br\s*\/?>([\s\S]*?)<\/div>/gi;
  let m;
  while ((m = accordRe.exec(html)) !== null) {
    const accord = m[1].replace(/<[^>]+>/g, "").trim();
    if (accord && !accords.includes(accord)) accords.push(accord);
  }
  data.accords = accords;

  // --- Notes by pyramid section ---
  // Fragrantica structure: sections labeled Верхние/Средние/Базовые ноты
  // Each note: <span itemprop="name">Note Name</span> inside the pyramid
  const pyramidSections = [
    { key: "topNotes", patterns: ["верхн", "top note", "pyramid-top"] },
    { key: "middleNotes", patterns: ["средн", "middle note", "heart note", "pyramid-middle"] },
    { key: "baseNotes", patterns: ["базов", "base note", "pyramid-base"] },
  ];

  // Find note sections by looking for labeled divs
  const noteSectionRe =
    /<div[^>]*>([\s\S]{0,200}?(?:верхние|средние|базовые|top notes|middle notes|base notes|heart notes)[^\S\r\n]*ноты?[\s\S]*?)<\/div>/gi;

  // Simpler approach: find all note spans with context
  // Fragrantica wraps pyramid in a known structure; extract all itemprop=name spans per section
  const pyramidBlockRe =
    /<b[^>]*>(верхние|средние|базовые|top|middle|base|heart)\s*ноты?:?<\/b>([\s\S]*?)(?=<b[^>]*>(?:верхние|средние|базовые|top|middle|base|heart)|<\/div>|$)/gi;

  while ((m = pyramidBlockRe.exec(html)) !== null) {
    const label = m[1].toLowerCase();
    const block = m[2];
    const noteNames = extractTexts(block, /<span[^>]*itemprop="name"[^>]*>([\s\S]*?)<\/span>/);
    if (label.startsWith("верхн") || label === "top") data.topNotes = noteNames;
    else if (label.startsWith("средн") || label === "middle" || label === "heart")
      data.middleNotes = noteNames;
    else if (label.startsWith("базов") || label === "base") data.baseNotes = noteNames;
  }

  // Fallback: grab all itemprop=name spans as combined notes if pyramid parsing yielded nothing
  if (!data.topNotes && !data.middleNotes && !data.baseNotes) {
    const allNotes = extractTexts(html, /<span[^>]*itemprop="name"[^>]*>([\s\S]*?)<\/span>/);
    if (allNotes.length) data.topNotes = allNotes;
  }

  data.topNotes = data.topNotes || [];
  data.middleNotes = data.middleNotes || [];
  data.baseNotes = data.baseNotes || [];

  // --- Gender ---
  // <meta itemprop="description" content="...для мужчин..." /> or page title hints
  const genderMatch = html.match(
    /для\s+(мужчин|женщин|мужчин и женщин|унисекс)|(?:for\s+)?(men|women|unisex)/i
  );
  if (genderMatch) {
    const g = (genderMatch[1] || genderMatch[2] || "").toLowerCase();
    if (g.includes("унисекс") || g.includes("unisex") || g.includes("мужчин и женщин"))
      data.gender = "unisex";
    else if (g.includes("женщин") || g.includes("women")) data.gender = "female";
    else if (g.includes("мужчин") || g.includes("men")) data.gender = "male";
  }
  data.gender = data.gender || "unisex";

  // --- Seasons / Occasions ---
  // Fragrantica voting charts; extract labels from vote bars
  const seasonRe =
    /<div[^>]*class="[^"]*vote-button[^"]*"[^>]*>.*?<span[^>]*>([\w\sа-яёА-ЯЁ]+?)<\/span>/gi;
  const seasons = [];
  while ((m = seasonRe.exec(html)) !== null) {
    const s = m[1].trim();
    if (s && s.length < 30) seasons.push(s);
  }
  data.seasons = [...new Set(seasons)];

  return data;
}

async function searchFragranticaPage(brand, fragranceName) {
  const query = encodeURIComponent(`${brand} ${fragranceName}`);
  const searchUrl = `https://www.fragrantica.ru/search/?query=${query}`;

  let html;
  try {
    const res = await fetch(searchUrl, {
      headers: FRAGRANTICA_HEADERS,
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!res.ok) return null;
    html = await res.text();
  } catch {
    return null;
  }

  // Find first perfume link: /perfume/Brand/Name-ID.html
  const linkRe = /href="(https?:\/\/www\.fragrantica\.ru\/perfume\/[^"]+\.html)"/i;
  const linkMatch = html.match(linkRe);
  if (!linkMatch) return null;

  const perfumeUrl = linkMatch[1];

  let perfumeHtml;
  try {
    const res2 = await fetch(perfumeUrl, {
      headers: FRAGRANTICA_HEADERS,
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!res2.ok) return null;
    perfumeHtml = await res2.text();
  } catch {
    return null;
  }

  const parsed = parseFragranticaPage(perfumeHtml);
  return { ...parsed, url: perfumeUrl };
}

async function lookupFragranticaData(brand, fragranceName) {
  const cached = getFragranticaCache(brand, fragranceName);
  if (cached) return cached;

  const result = await searchFragranticaPage(brand, fragranceName);
  if (!result) return null;

  setFragranticaCache(brand, fragranceName, result);
  return result;
}
