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
  ["edp", "parfum", "parfume", "парфюмерная"],
  // Тестер
  ["tester", "test", "testep", "testor", "testr", "тестер"],
  // Объём мл
  ["ml", "мл"],
];

const PM_SYNONYM_MAP = new Map();
for (const group of PM_SYNONYM_GROUPS) {
  for (const t of group) PM_SYNONYM_MAP.set(t, group.filter((s) => s !== t));
}

// Split text into tokens at: spaces, symbols, letter↔digit, latin↔cyrillic.
function pmWordTokenize(text) {
  const lower = String(text || "")
    .toLowerCase()
    .replace(/ё/g, "е")
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
function pmQueryToTokenGroups(query) {
  return pmWordTokenize(query)
    .filter((t) => t.length >= 2 || /^\d+$/.test(t) || t.length === 1)
    .map(pmWordExpand);
}

// True when text contains at least one term from each group.
function pmWordMatch(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  return tokenGroups.every((group) => group.some((token) => lower.includes(token)));
}

// Count how many token groups match (0..tokenGroups.length).
// Used for relevance scoring: higher = better match.
function pmWordMatchScore(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return 0;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  return tokenGroups.reduce((n, group) => n + (group.some((token) => lower.includes(token)) ? 1 : 0), 0);
}

// A token group is "optional" if every synonym in it is either a pure number or ≤3 chars.
// Numbers ("50", "100") and short units ("ml", "мл") are extremely common and would match
// thousands of unrelated products if required — they contribute to scoring but not to minMatch.
function pmTokenGroupIsOptional(group) {
  return group.every((t) => /^\d+$/.test(t) || t.length <= 3);
}

// Minimum number of REQUIRED token groups that must match for a row to be included.
// Optional groups (numbers, short units) are excluded from the minimum calculation.
function pmMinMatchCount(tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return 0;
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g));
  if (required.length === 0) return 1;
  return required.length <= 2 ? required.length : required.length - 1;
}

// Returns true when text satisfies the search quality bar for the given token groups.
// Required groups (>3 chars, non-numeric) must all match up to (n-1) — same n-1 rule as before.
// When ALL groups are optional (short codes, numbers, units) we fall back to "any token matches".
function pmPassesSearchFilter(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g));
  if (required.length === 0) {
    // e.g. query "GTT81" or "50 ml" — all short/numeric; just need any token to match
    return tokenGroups.some((group) => group.some((t) => lower.includes(t)));
  }
  const minMatch = required.length <= 2 ? required.length : required.length - 1;
  const score = required.reduce((n, group) => n + (group.some((t) => lower.includes(t)) ? 1 : 0), 0);
  return score >= minMatch;
}

// Build sorted word list from PM row names (for autocomplete).
function buildPmWordIndex(rows = []) {
  const words = new Set();
  for (const row of rows) {
    const name = String(row.name || row.NativeName || row.nativeName || "");
    if (!name) continue;
    for (const token of pmWordTokenize(name)) {
      if (token.length >= 2) words.add(token);
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
