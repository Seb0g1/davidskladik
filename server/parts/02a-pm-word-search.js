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
function pmQueryToTokenGroups(query) {
  return pmWordTokenize(query)
    .filter((t) => t.length >= 2)
    .map(pmWordExpand);
}

// True when text contains at least one term from each group.
function pmWordMatch(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  return tokenGroups.every((group) => group.some((token) => lower.includes(token)));
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
