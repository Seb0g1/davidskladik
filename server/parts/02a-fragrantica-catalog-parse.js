// Парсеры страниц fragrantica.ru для каталога «Фрагрантика» (02f-fragrantica-crawler.js).
// Чистые функции без зависимостей — тесты грузят файл отдельно (test/fragrantica-parse.test.cjs).
//
//   /designers-N/            → бренды (страниц ~11, ~870 брендов на каждой)
//   /designers/<Brand>.html  → все ароматы бренда: id, название, пол, год, превью
//   /perfume/<Brand>/<Name>-<id>.html → карточка аромата: ноты с иконками, аккорды, описание, фото

const FRAGRANTICA_ORIGIN = "https://www.fragrantica.ru";

const FRAG_HTML_ENTITIES = { amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'", nbsp: " ", laquo: "«", raquo: "»", mdash: "—", ndash: "–", hellip: "…" };

function fragDecodeEntities(text) {
  return String(text || "").replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, code) => {
    if (code[0] === "#") {
      const num = code[1] === "x" || code[1] === "X" ? parseInt(code.slice(2), 16) : parseInt(code.slice(1), 10);
      return Number.isFinite(num) ? String.fromCodePoint(num) : match;
    }
    const named = FRAG_HTML_ENTITIES[code.toLowerCase()];
    return named === undefined ? match : named;
  });
}

function fragText(html) {
  return fragDecodeEntities(String(html || "").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

function fragAbsoluteUrl(href) {
  const value = String(href || "").trim();
  if (!value) return "";
  if (/^https?:\/\//i.test(value)) return value;
  return `${FRAGRANTICA_ORIGIN}${value.startsWith("/") ? "" : "/"}${value}`;
}

function fragPerfumeIdFromUrl(url) {
  const match = String(url || "").match(/-(\d+)\.html(?:[?#].*)?$/);
  return match ? Number(match[1]) : 0;
}

// «унисекс» / «женский» / «для мужчин» … → female | male | unisex | ""
function fragGenderFromText(text) {
  const value = String(text || "").toLowerCase();
  if (/унисекс|для мужчин и женщин|unisex/.test(value)) return "unisex";
  if (/женск|для женщин|women/.test(value)) return "female";
  if (/мужск|для мужчин|\bmen\b/.test(value)) return "male";
  return "";
}

function parseFragranticaDesignersIndex(html) {
  const source = String(html || "");
  const brands = [];
  const seen = new Set();
  const re = /<a href="(\/designers\/[^"]+\.html)"[^>]*>([\s\S]*?)<\/a>\s*(?:<span[^>]*>\s*([\d\s,.]+)\s*<\/span>)?/g;
  let match;
  while ((match = re.exec(source))) {
    const href = match[1];
    if (seen.has(href)) continue;
    const name = fragText(match[2]);
    if (!name) continue;
    seen.add(href);
    brands.push({
      slug: href.replace(/^\/designers\//, "").replace(/\.html$/, ""),
      url: fragAbsoluteUrl(href),
      name,
      perfumeCount: Number(String(match[3] || "").replace(/[^\d]/g, "")) || null,
    });
  }
  const pages = [...new Set([...source.matchAll(/\/designers-(\d+)\//g)].map((m) => Number(m[1])))].sort((a, b) => a - b);
  return { brands, pages };
}

// Список ароматов на странице бренда. title="Dior Bonne Étoile Baby Dior унисекс 2023"
function parseFragranticaBrandPage(html, { brandSlug = "" } = {}) {
  const source = String(html || "");
  const perfumes = [];
  const seen = new Set();
  const re = /<a href="(\/perfume\/([^/"]+)\/[^"]+-(\d+)\.html)"[^>]*\btitle="([^"]*)"[^>]*>([\s\S]*?)<\/a>/g;
  let match;
  while ((match = re.exec(source))) {
    const [, href, slug, idText, title, inner] = match;
    if (brandSlug && slug !== brandSlug) continue;
    const id = Number(idText);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const name = fragText((inner.match(/<h3[^>]*>([\s\S]*?)<\/h3>/) || [])[1]);
    const brand = fragText((inner.match(/<\/h3>\s*<p[^>]*>([\s\S]*?)<\/p>/) || [])[1]);
    const titleText = fragDecodeEntities(title);
    const yearMatch = titleText.match(/\b(1[89]\d\d|20\d\d)\s*$/);
    perfumes.push({
      id,
      url: fragAbsoluteUrl(href),
      brandSlug: slug,
      brand,
      name: name || titleText,
      gender: fragGenderFromText(titleText.slice(brand.length + name.length)),
      year: yearMatch ? Number(yearMatch[1]) : null,
    });
  }
  return perfumes;
}

function fragParseNoteLinks(html) {
  const notes = [];
  const re = /<a href="[^"]*\/notes\/[^"]*"[^>]*>[\s\S]*?<img[^>]*src="([^"]+)"[^>]*alt="([^"]*)"[\s\S]*?<\/a>/g;
  let match;
  while ((match = re.exec(String(html || "")))) {
    const name = fragDecodeEntities(match[2]).trim();
    if (name) notes.push({ name, icon: match[1] });
  }
  return notes;
}

function fragParseNotes(source) {
  const notes = { top: [], middle: [], base: [], flat: [] };
  const levels = [...source.matchAll(/<pyramid-level-new notes="(\w+)">/g)];
  levels.forEach((level, index) => {
    const end = index + 1 < levels.length ? levels[index + 1].index : source.indexOf("</pyramid-switch-new>", level.index);
    const chunk = source.slice(level.index, end > level.index ? end : level.index + 20000);
    const key = notes[level[1]] ? level[1] : "flat";
    for (const note of fragParseNoteLinks(chunk)) {
      if (!notes[key].some((item) => item.name === note.name)) notes[key].push(note);
    }
  });
  return notes;
}

function fragParseAccords(source) {
  const accords = [];
  const re = /style="color:\s*(#[0-9a-f]{3,8});\s*background:\s*(#[0-9a-f]{3,8});\s*opacity:\s*[\d.]+%;\s*width:\s*([\d.]+)%;\s*">\s*<span[^>]*>([\s\S]*?)<\/span>/gi;
  let match;
  while ((match = re.exec(source))) {
    const name = fragText(match[4]);
    if (!name || accords.some((item) => item.name === name)) continue;
    accords.push({ name, share: Math.round(Number(match[3]) || 0), color: match[1], background: match[2] });
  }
  return accords;
}

// Текст «Sauvage Dior — это аромат для мужчин…» из блока relative tw-rating-card,
// без хвоста «Читайте об этом аромате на других языках».
function fragParseDescription(source) {
  const start = source.indexOf("relative tw-rating-card");
  if (start < 0) return "";
  const chunk = source.slice(start, start + 30000);
  const open = chunk.indexOf(">");
  const stop = chunk.search(/id="lang-seo-links"|Читайте об этом аромате на других языках/);
  const body = chunk.slice(open + 1, stop > 0 ? stop : undefined);
  const paragraphs = [...body.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].map((m) => fragText(m[1])).filter(Boolean);
  const text = (paragraphs.length ? paragraphs.join("\n\n") : fragText(body)).replace(/\s*Читайте об этом аромате[\s\S]*$/, "").trim();
  return text;
}

function parseFragranticaPerfumePage(html, { url = "" } = {}) {
  const source = String(html || "");
  const canonical = (source.match(/<link rel="canonical" href="([^"]+)"/) || [])[1] || url;
  const id = fragPerfumeIdFromUrl(canonical) || fragPerfumeIdFromUrl(url);
  const h1 = (source.match(/<h1[^>]*itemprop="name"[^>]*>([\s\S]*?)<\/h1>/) || [])[1] || "";
  const genderLabel = fragText((h1.match(/<span[^>]*>([\s\S]*?)<\/span>/) || [])[1]);
  const fullTitle = fragText(h1.replace(/<span[^>]*>[\s\S]*?<\/span>/g, ""));
  const brandSlug = (canonical.match(/\/perfume\/([^/]+)\//) || [])[1] || "";
  const description = fragParseDescription(source);
  // «Sauvage Dior — это аромат…»: <b>Название</b> <b>Бренд</b>
  const bold = [...source.slice(source.indexOf("relative tw-rating-card")).matchAll(/<b>([\s\S]*?)<\/b>/g)].slice(0, 2).map((m) => fragText(m[1]));
  let name = bold[0] || "";
  let brand = bold[1] || "";
  if (!brand && fullTitle) {
    brand = fragDecodeEntities(brandSlug.replace(/-/g, " "));
  }
  if (!name) name = fullTitle.endsWith(brand) ? fullTitle.slice(0, fullTitle.length - brand.length).trim() : fullTitle;
  const yearMatch = description.match(/выпущен[а-я]*\s+в\s+(\d{4})/i);
  const familyMatch = description.match(/принадлежит к группе\s+([^.]+)\./i);
  const perfumerMatch = description.match(/Парфюмер[ыа]?:\s*([^.]+)\./i);
  const ratingValue = Number((source.match(/itemprop="ratingValue"[^>]*>([\d.]+)</) || [])[1]) || null;
  const ratingCount = Number((source.match(/itemprop="ratingCount" content="(\d+)"/) || [])[1]) || null;
  const image = (source.match(/https:\/\/fimgs\.net\/mdimg\/perfume-thumbs\/375x500\.\d+\.2x\.jpg/) || [])[0]
    || (source.match(/<img itemprop="image" src="([^"]+)"/) || [])[1]
    || "";
  return {
    id,
    url: canonical || url,
    brandSlug,
    brand,
    name,
    title: fullTitle,
    gender: fragGenderFromText(genderLabel) || fragGenderFromText(description.slice(0, 200)),
    year: yearMatch ? Number(yearMatch[1]) : null,
    family: familyMatch ? familyMatch[1].trim() : "",
    perfumers: perfumerMatch ? perfumerMatch[1].split(/,\s*|\s+и\s+/).map((s) => s.trim()).filter(Boolean) : [],
    notes: fragParseNotes(source),
    accords: fragParseAccords(source),
    description,
    rating: ratingValue,
    votes: ratingCount,
    image,
  };
}

// Иконка ноты: t.75.jpg (маленькая) → o.75.jpg (крупная) для картинки «Пирамида аромата».
function fragranticaNoteIconLarge(icon) {
  return String(icon || "").replace(/\/sastojci\/t\.(\d+)\./, "/sastojci/o.$1.");
}

// Карточка бренда: «Страна: France», «Владелец лицензии: LVMH» — страна идёт в «Страну-изготовителя».
function parseFragranticaBrandInfo(html) {
  const source = String(html || "");
  const country = fragText((source.match(/Страна:\s*<a[^>]*href="\/country\/[^"]*"[^>]*>([\s\S]*?)<\/a>/) || [])[1]);
  const countrySlug = (source.match(/Страна:\s*<a[^>]*href="\/country\/([^".]+)\.html"/) || [])[1] || "";
  const owner = fragText((source.match(/Владелец(?: лицензии)?:\s*(?:<a[^>]*>)?([^<]{2,80})/) || [])[1]);
  return { country: country || countrySlug.replace(/-/g, " "), owner };
}
