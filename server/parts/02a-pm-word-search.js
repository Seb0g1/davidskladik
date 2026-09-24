"use strict";

// GingerPM-style word search for PriceMaster names.
// Tokenizes by spaces + letter/digit + latin/cyrillic boundaries.
// Synonym groups: any synonym in a group is treated as matching.

const PM_SYNONYM_GROUPS = [
  // Мужской аромат
  ["homme", "man", "masculine", "men"],
  // Женский аромат
  ["femme", "feme", "femenine", "woman", "women", "lady"],
  // EDT / туалетная вода
  ["edt", "toilette", "туалетная"],
  // EDP / парфюмерная вода
  ["edp", "parfum", "parfume", "парфюмерная", "парфюмированная"],
  // Тестер
  ["tester", "test", "testep", "testor", "testr", "тестер"],
  // Объём мл
  ["ml", "мл"],
  // Бренды: кириллица ↔ латиница
  ["chanel", "шанель"],
  ["dior", "диор"],
  ["givenchy", "живанши", "живанши"],
  ["guerlain", "герлен"],
  ["hermes", "эрмес", "гермес"],
  ["lancome", "ланком"],
  ["yves", "ив"],
  ["saint", "сен"],
  ["laurent", "лоран"],
  ["versace", "версаче"],
  ["armani", "армани"],
  ["dolce", "дольче"],
  ["gabbana", "габбана"],
  ["prada", "прада"],
  ["gucci", "гуччи"],
  ["burberry", "барберри"],
  ["cartier", "картье"],
  ["bvlgari", "bulgari", "булгари"],
  ["montblanc", "монблан"],
  ["hugo", "хуго"],
  ["boss", "босс"],
  ["calvin", "кельвин"],
  ["klein", "кляйн"],
  ["diptyque", "диптик"],
  ["byredo", "байредо"],
  ["maison", "мезон"],
  ["margiela", "маржела"],
  ["narciso", "нарцисо"],
  ["rodriguez", "родригез"],
  ["viktor", "виктор"],
  ["rolf", "рольф"],
  ["flowerbomb", "флауэрбомб"],
  ["thierry", "тьерри"],
  ["mugler", "мюглер"],
  ["alien", "эйлиен"],
  ["angel", "энджел"],
  ["rabanne", "рабан"],
  ["invictus", "инвиктус"],
  ["olympea", "олимпеа"],
  ["paco", "пако"],
];

const PM_SYNONYM_MAP = new Map();
for (const group of PM_SYNONYM_GROUPS) {
  for (const t of group) PM_SYNONYM_MAP.set(t, group.filter((s) => s !== t));
}

// Filler words from marketplace-style titles ("Eau de Parfum", "парфюмерная вода 100 мл").
// PriceMaster names rarely contain them, so they only add relevance, never filter rows out.
const PM_SOFT_TOKENS = new Set([
  "eau", "de", "du", "des", "la", "le", "les", "di", "da", "del", "of", "the", "for", "by", "pour", "and", "et",
  "ml", "мл", "вода", "для", "и", "в", "с", "духи", "парфюм", "аромат",
  "мужской", "мужская", "мужские", "женский", "женская", "женские", "унисекс",
]);

// Canonical form for matching: lowercase, ё→е, Latin diacritics stripped (Hermès → hermes,
// Giò → gio), apostrophe variants (’ ` ´ ʼ ‘) unified to "'". Cyrillic й is preserved so the
// same normalized token still matches in MySQL LIKE.
function pmNormalizeSearchText(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/ß/g, "ss")
    .replace(/æ/g, "ae")
    .replace(/œ/g, "oe")
    .replace(/ø/g, "o")
    .normalize("NFD")
    .replace(/([a-z])[\u0300-\u036f]+/g, "$1")
    .normalize("NFC")
    .replace(/[’‘`´ʼ]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

// Split text into tokens at: spaces, symbols, letter↔digit, latin↔cyrillic.
function pmWordTokenize(text) {
  const lower = pmNormalizeSearchText(text)
    .replace(/[^a-zа-я0-9]+/g, " ")
    .replace(/([a-zа-я])(\d)/g, "$1 $2")
    .replace(/(\d)([a-zа-я])/g, "$1 $2")
    .replace(/([a-z])([а-я])/g, "$1 $2")
    .replace(/([а-я])([a-z])/g, "$1 $2");
  return [...new Set(lower.split(/\s+/).filter((t) => t.length >= 1))];
}

function pmWordExpand(token) {
  return [token, ...(PM_SYNONYM_MAP.get(token) || [])];
}

// Returns [[token, ...synonyms], ...] — one group per input token.
// Search is AND across groups, OR within each group.
// Pure-numeric tokens (e.g. "6" for 6 ml) or single letters pass through.
// Alphanumeric compounds (e.g. "no5", "h24") get an extra required group with _compound=true
// that matches with optional whitespace between the alpha and digit parts.
// Apostrophe words ("j'adore", "l'eau") stay one token that matches "J'adore", "Jadore",
// "J Adore" alike — splitting them produced a standalone "j" that almost never matched.
function pmQueryToTokenGroups(query) {
  const norm = pmNormalizeSearchText(query);
  const rawWords = norm
    .split(/[^a-zа-я0-9']+/)
    .map((w) => w.replace(/^'+|'+$/g, ""))
    .filter(Boolean);

  const groups = [];
  const seen = new Set();
  const compoundWords = [];
  for (const rawWord of rawWords) {
    const parts = rawWord.split("'").filter(Boolean);
    if (parts.length >= 2 && parts.every((p) => /^[a-zа-я]+$/.test(p))) {
      const token = parts.join("'");
      if (!seen.has(token)) {
        seen.add(token);
        groups.push([token]);
      }
      continue;
    }
    const word = parts.join("");
    for (const token of pmWordTokenize(word)) {
      if (seen.has(token)) continue;
      seen.add(token);
      groups.push(pmWordExpand(token));
    }
    if (word.length >= 2 && /[a-zа-я]/.test(word) && /\d/.test(word)) compoundWords.push(word);
  }

  const existingTokens = new Set(groups.flat());
  const compoundGroups = [...new Set(compoundWords)]
    // "100ml" / "50мл": the unit is soft, so a compound would wrongly demand "100ml" spelled together.
    .filter((w) => !existingTokens.has(w) && !(w.match(/[a-zа-я]+/g) || []).every((a) => PM_SOFT_TOKENS.has(a)))
    .map((w) => {
      const arr = [w];
      arr._compound = true;
      return arr;
    });

  return [...groups, ...compoundGroups];
}

function pmEscapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Match a single token against normalized (pmNormalizeSearchText) text.
function pmTokenMatchesText(lower, token) {
  if (/^\d+$/.test(token)) {
    // Both-boundary digit matching: "5" matches "5ml", "1.5ml" but NOT "50ml", "495ml", "15ml".
    // Left boundary is non-digit (so "1.5" still passes — "." is not \d).
    return new RegExp(`(?<!\\d)${pmEscapeRegExp(token)}(?!\\d)`).test(lower);
  }
  // Apostrophe word ("j'adore"): parts may be joined by an apostrophe, space, hyphen or nothing.
  if (token.includes("'")) {
    const flex = token.split("'").map(pmEscapeRegExp).join("[\\s'\\-._]*");
    return new RegExp(`(?<![a-zа-я])${flex}`).test(lower);
  }
  // Short pure-alpha tokens (≤3 chars like "ml", "no", "de"): require letter-word boundaries
  // to prevent "no" matching "noir", "ml" matching "mlm", etc.
  if (token.length <= 3 && /^[a-zа-я]+$/.test(token)) {
    return new RegExp(`(?<![a-zа-я])${pmEscapeRegExp(token)}(?![a-zа-я])`).test(lower);
  }
  // Alphanumeric compound ("no5", "h24"): match exact substring OR with optional separator
  // (space, hyphen, dot, underscore) between letter/digit parts — so "bod13" matches "bod-13".
  if (/[a-zа-я]/.test(token) && /\d/.test(token)) {
    if (lower.includes(token)) return true;
    const flex = token
      .replace(/([a-zа-я])(\d)/g, "$1[\\s\\-._ ]*$2")
      .replace(/(\d)([a-zа-я])/g, "$1[\\s\\-._ ]*$2");
    try { return new RegExp(flex).test(lower); } catch { return false; }
  }
  return lower.includes(token);
}

// Names are matched as written and, when they contain apostrophes, also with them removed —
// so a typed "jadore" still finds "J'adore".
function pmGroupMatches(lower, group) {
  if (group.some((token) => pmTokenMatchesText(lower, token))) return true;
  if (!lower.includes("'")) return false;
  const joined = lower.replace(/'/g, "");
  return group.some((token) => !token.includes("'") && pmTokenMatchesText(joined, token));
}

// True when text contains at least one term from each group.
function pmWordMatch(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = pmNormalizeSearchText(text);
  return tokenGroups.every((group) => pmGroupMatches(lower, group));
}

// Count how many token groups match (0..tokenGroups.length).
// Used for relevance scoring: higher = better match.
function pmWordMatchScore(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return 0;
  const lower = pmNormalizeSearchText(text);
  return tokenGroups.reduce((n, group) => n + (pmGroupMatches(lower, group) ? 1 : 0), 0);
}

// Filler group ("eau", "de", "мл"): never required, only adds relevance.
function pmTokenGroupIsSoft(group) {
  if (group._compound) return false;
  return group.every((t) => PM_SOFT_TOKENS.has(t));
}

// A token group is "optional" (not counted in the n-1 required minimum) if it is soft or
// every synonym in it is a pure number or ≤3 chars. Numbers ("50", "100") and short words
// would match thousands of unrelated products if they were the only criteria.
// Compound groups (e.g. ["no5"] with _compound flag) are always required.
function pmTokenGroupIsOptional(group) {
  if (group._compound) return false;
  if (pmTokenGroupIsSoft(group)) return true;
  return group.every((t) => /^\d+$/.test(t) || t.length <= 3);
}

// Splits groups by role:
//   required — long words; with 3+ of them one may be missing (n-1 tolerance)
//   strict   — numbers / short words the user typed; all must match (word boundaries)
//   soft     — filler; relevance only
// A query made only of filler words treats them as strict so it still filters.
function pmClassifyTokenGroups(tokenGroups) {
  const groups = tokenGroups || [];
  const nonSoft = groups.filter((g) => !pmTokenGroupIsSoft(g));
  const allSoft = groups.length > 0 && nonSoft.length === 0;
  const soft = allSoft ? [] : groups.filter((g) => pmTokenGroupIsSoft(g));
  const required = allSoft ? [] : nonSoft.filter((g) => !pmTokenGroupIsOptional(g));
  const strict = allSoft ? groups : nonSoft.filter((g) => pmTokenGroupIsOptional(g));
  const minRequired = required.length <= 2 ? required.length : required.length - 1;
  return { required, strict, soft, minRequired };
}

// Minimum number of REQUIRED token groups that must match for a row to be included.
// Optional groups (numbers, short units) are excluded from the minimum calculation.
function pmMinMatchCount(tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return 0;
  const { required, minRequired } = pmClassifyTokenGroups(tokenGroups);
  return required.length ? minRequired : 1;
}

// Returns true when text satisfies the search quality bar for the given token groups.
// Required groups must match up to (n-1) for 3+ of them; strict groups (numbers, short
// words) must all match — so "5 ml" with "Christian Dior" requires a standalone "5", not "50".
// Soft groups (filler words, units) are ignored.
function pmPassesSearchFilter(text, tokenGroups, { fuzzy = false } = {}) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = pmNormalizeSearchText(text);
  const { required, strict, minRequired } = pmClassifyTokenGroups(tokenGroups);
  if (!strict.every((group) => pmGroupMatches(lower, group))) return false;
  let score = 0;
  for (const group of required) {
    if (pmGroupMatches(lower, group)) { score++; continue; }
    if (!fuzzy || group._compound) continue;
    // Fuzzy: trigram similarity for long tokens (≥5 chars) — handles insertion/deletion typos.
    if (group.some((t) => t.length >= 5 && wordSimJs(t, lower) >= 0.4)) { score++; continue; }
    // Fuzzy: sorted-char for short tokens (4-7 chars) — handles transpositions like "doir"→"dior".
    if (group.some((t) => t.length >= 4 && sortedCharMatch(t, lower))) score++;
  }
  return score >= minRequired;
}

// True when every required group matches (no n-1 allowance used) — lets the UI keep partial
// matches below complete ones even when it re-sorts by price.
function pmIsFullMatch(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = pmNormalizeSearchText(text);
  const { required, strict } = pmClassifyTokenGroups(tokenGroups);
  return [...required, ...strict].every((group) => pmGroupMatches(lower, group));
}

// SQL LIKE needle for a token: apostrophe words use their longest part ("j'adore" → "adore")
// because the stored spelling may be "J'adore", "J`adore" or "Jadore".
function pmTokenSqlNeedle(token) {
  if (!token.includes("'")) return token;
  return token.split("'").sort((a, b) => b.length - a.length)[0] || token;
}

function pmSqlLike(value) {
  return `%${String(value).replace(/[\\%_]/g, "\\$&")}%`;
}

// Builds a MySQL WHERE fragment that is a superset of pmPassesSearchFilter (same n-1 rule,
// soft words not required), plus a relevance expression for ORDER BY so the LIMIT window
// is filled with the best matches first instead of just the newest rows.
// columns: { name, article, barcode? } — fully qualified column expressions.
function pmBuildMysqlSearchClause(tokenGroups, columns = {}) {
  const cols = [columns.name, columns.article, columns.barcode].filter(Boolean);
  // Same name without apostrophes (mirrors pmGroupMatches) so "jadore" reaches "J'adore" rows.
  if (columns.name) cols.push("REPLACE(REPLACE(REPLACE(" + columns.name + ", '''', ''), '`', ''), '\u2019', '')");
  const groupCond = (group, params) => {
    const needles = [...new Set(group.map(pmTokenSqlNeedle).filter(Boolean))];
    const conds = [];
    for (const needle of needles) {
      for (const col of cols) {
        conds.push(`${col} LIKE ?`);
        params.push(pmSqlLike(needle));
      }
    }
    return conds.length ? `(${conds.join(" OR ")})` : "1=1";
  };
  const { required, strict, soft, minRequired } = pmClassifyTokenGroups(tokenGroups);
  const whereParts = [];
  const params = [];
  // Compound groups are implied by their split parts (already strict groups) — the exact
  // spelling only affects ranking.
  for (const group of strict) whereParts.push(groupCond(group, params));
  const sqlRequired = required.filter((g) => !g._compound);
  const sqlMin = Math.min(sqlRequired.length, minRequired);
  if (sqlRequired.length && sqlMin === sqlRequired.length) {
    for (const group of sqlRequired) whereParts.push(groupCond(group, params));
  } else if (sqlRequired.length && sqlMin > 0) {
    const sum = sqlRequired.map((group) => `IF(${groupCond(group, params)}, 1, 0)`).join(" + ");
    whereParts.push(`(${sum}) >= ?`);
    params.push(sqlMin);
  }

  const scoreParams = [];
  const scoreTerms = [];
  for (const group of required) scoreTerms.push(`IF(${groupCond(group, scoreParams)}, 2, 0)`);
  for (const group of [...strict, ...soft]) scoreTerms.push(`IF(${groupCond(group, scoreParams)}, 1, 0)`);
  return {
    where: whereParts.length ? whereParts.join(" AND ") : "1=1",
    params,
    scoreSql: scoreTerms.length ? `(${scoreTerms.join(" + ")})` : "0",
    scoreParams,
  };
}

// Prisma `AND` clauses (array) equivalent of pmBuildMysqlSearchClause for pm_snapshot_items.
function pmBuildPrismaSearchWhere(tokenGroups, fields = ["article", "nativeName"]) {
  const groupClause = (group) => ({
    OR: [...new Set(group.map(pmTokenSqlNeedle).filter(Boolean))].flatMap((needle) =>
      fields.map((field) => ({ [field]: { contains: needle, mode: "insensitive" } }))),
  });
  const { required, strict, minRequired } = pmClassifyTokenGroups(tokenGroups);
  const and = strict.map(groupClause);
  const sqlRequired = required.filter((g) => !g._compound);
  const sqlMin = Math.min(sqlRequired.length, minRequired);
  if (sqlRequired.length && sqlMin === sqlRequired.length) {
    and.push(...sqlRequired.map(groupClause));
  } else if (sqlRequired.length && sqlMin > 0) {
    // n-1 of n: any one group may be missing.
    and.push({
      OR: sqlRequired.map((_, skip) => ({ AND: sqlRequired.filter((__, i) => i !== skip).map(groupClause) })),
    });
  }
  return and;
}

// Trigram similarity between two strings (mirrors Postgres pg_trgm similarity()).
function ngramSim(a, b) {
  function trigrams(s) {
    const src = " " + s; // 1-space prefix padding (Postgres convention)
    const set = new Set();
    for (let i = 0; i + 2 < src.length; i++) set.add(src.slice(i, i + 3));
    return set;
  }
  const ag = trigrams(a), bg = trigrams(b);
  if (!ag.size && !bg.size) return 1;
  if (!ag.size || !bg.size) return 0;
  let common = 0;
  for (const g of ag) if (bg.has(g)) common++;
  return (2 * common) / (ag.size + bg.size);
}

// word_similarity JS equivalent: max trigram similarity between token and any word in text.
function wordSimJs(token, lower) {
  const words = lower.split(/[\s\-.,/]+/).filter(Boolean);
  if (!words.length) return 0;
  return Math.max(...words.map((w) => ngramSim(token, w)));
}

// Sorted-character match: catches transpositions like "doir"→"dior", "givanchi"→"givenchy" (4-6 chars).
// Two tokens match if they have the same sorted characters — i.e. one is a transposition of the other.
// Only applied for tokens 4-7 chars (very short/long words would create too many false positives).
function sortedCharMatch(token, lower) {
  if (token.length < 4 || token.length > 7) return false;
  const sortedToken = token.split("").sort().join("");
  const words = lower.split(/[\s\-.,/]+/).filter((w) => Math.abs(w.length - token.length) <= 1);
  return words.some((w) => w.split("").sort().join("") === sortedToken);
}

// Like pmPassesSearchFilter but allows long required tokens (≥5 chars) to match
// via trigram similarity (≥0.4) when exact match fails.
// Used for fuzzy fallback results so typos in brand names still pass.
function pmPassesSearchFilterFuzzy(text, tokenGroups) {
  return pmPassesSearchFilter(text, tokenGroups, { fuzzy: true });
}

// Build sorted word list from PM row names (for autocomplete).
function buildPmWordIndex(rows = []) {
  const words = new Set();
  for (const row of rows) {
    const name = String(row.name || row.NativeName || row.nativeName || "");
    if (!name) continue;
    for (const token of pmWordTokenize(name)) {
      // Include single-digit numeric tokens (e.g. "5" from "5 ml") so users can chip them.
      if (token.length >= 2 || /^\d+$/.test(token)) words.add(token);
    }
  }
  for (const group of PM_SYNONYM_GROUPS) {
    for (const token of group) words.add(token);
  }
  return [...words].sort();
}

let _pmWordIndex = null;
let _pmWordIndexKey = null;

// Returns sorted word list, cached per snapshot version.
// readSnapshot is defined in 02a-snapshot-core.js (loaded after this file),
// but resolved at call time — not at load time.
async function getPmWordIndex() {
  const snapshot = await readSnapshot().catch(() => null);
  if (!snapshot) return [];
  const itemCount = Object.keys(snapshot.items || {}).length;
  const key = `${snapshot.syncId || ""}:${snapshot.createdAt || ""}:${itemCount}`;
  if (_pmWordIndexKey === key && _pmWordIndex) return _pmWordIndex;
  _pmWordIndex = buildPmWordIndex(Object.values(snapshot.items || {}));
  _pmWordIndexKey = key;
  return _pmWordIndex;
}
