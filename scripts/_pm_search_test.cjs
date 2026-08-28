"use strict";
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

function pmWordTokenize(text) {
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е")
    .replace(/[^a-zа-я0-9]+/g, " ")
    .replace(/([a-zа-я])(\d)/g, "$1 $2").replace(/(\d)([a-zа-я])/g, "$1 $2")
    .replace(/([a-z])([а-я])/g, "$1 $2").replace(/([а-я])([a-z])/g, "$1 $2");
  return [...new Set(lower.split(/\s+/).filter((t) => t.length >= 1))];
}

const SYNONYM_GROUPS = [["ml","мл"],["edt","toilette","туалетная"],["edp","parfum","parfume","парфюмерная"],["homme","man","masculine","men"],["femme","feme","femenine","woman","women","lady"],["tester","test","testep","testor","testr","тестер"]];
const SYNONYM_MAP = new Map();
for (const g of SYNONYM_GROUPS) for (const t of g) SYNONYM_MAP.set(t, g.filter((s) => s !== t));

function pmTokenGroupIsOptional(g) {
  if (g._compound) return false;
  return g.every((t) => /^\d+$/.test(t) || t.length <= 3);
}

function pmTokenMatchesText(lower, token) {
  if (/^\d+$/.test(token)) {
    const esc = token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return new RegExp(`(?<!\\d)${esc}(?!\\d)`).test(lower);
  }
  if (token.length <= 3 && /^[a-zа-я]+$/.test(token)) {
    const esc = token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return new RegExp(`(?<![a-zа-я])${esc}(?![a-zа-я])`).test(lower);
  }
  if (/[a-zа-я]/.test(token) && /\d/.test(token)) {
    if (lower.includes(token)) return true;
    const flex = token.replace(/([a-zа-я])(\d)/g, "$1\\s*$2").replace(/(\d)([a-zа-я])/g, "$1\\s*$2");
    try { return new RegExp(flex).test(lower); } catch { return false; }
  }
  return lower.includes(token);
}

function pmPassesSearchFilter(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g));
  const optional = tokenGroups.filter((g) => pmTokenGroupIsOptional(g));
  if (required.length === 0) return tokenGroups.every((g) => g.some((t) => pmTokenMatchesText(lower, t)));
  const minMatch = required.length <= 2 ? required.length : required.length - 1;
  const score = required.reduce((n, g) => n + (g.some((t) => pmTokenMatchesText(lower, t)) ? 1 : 0), 0);
  if (score < minMatch) return false;
  return optional.every((g) => g.some((t) => pmTokenMatchesText(lower, t)));
}

function pmQueryToTokenGroups(q) {
  const lower = String(q || "").toLowerCase().replace(/ё/g, "е");
  const rawWords = lower.replace(/[^a-zа-я0-9]+/g, " ").split(/\s+/).filter(Boolean);
  const baseGroups = pmWordTokenize(q)
    .filter((t) => t.length >= 2 || /^\d+$/.test(t) || t.length === 1)
    .map((t) => [t, ...(SYNONYM_MAP.get(t) || [])]);
  const existingTokens = new Set(baseGroups.flat());
  const compoundGroups = rawWords
    .filter((w) => w.length >= 2 && /[a-zа-я]/.test(w) && /\d/.test(w) && !existingTokens.has(w))
    .map((w) => { const arr = [w]; arr._compound = true; return arr; });
  return [...baseGroups, ...compoundGroups];
}

function ngramSim(a, b) {
  function trigrams(s) { const src = " " + s; const set = new Set(); for (let i = 0; i + 2 < src.length; i++) set.add(src.slice(i, i + 3)); return set; }
  const ag = trigrams(a), bg = trigrams(b);
  if (!ag.size && !bg.size) return 1; if (!ag.size || !bg.size) return 0;
  let common = 0; for (const g of ag) if (bg.has(g)) common++;
  return (2 * common) / (ag.size + bg.size);
}
function wordSimJs(token, lower) {
  const words = lower.split(/[\s\-.,/]+/).filter(Boolean);
  return words.length ? Math.max(...words.map((w) => ngramSim(token, w))) : 0;
}
function pmPassesSearchFilterFuzzy(text, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return true;
  const lower = String(text || "").toLowerCase().replace(/ё/g, "е");
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g));
  const optional = tokenGroups.filter((g) => pmTokenGroupIsOptional(g));
  if (required.length === 0) return tokenGroups.every((g) => g.some((t) => pmTokenMatchesText(lower, t)));
  const minMatch = required.length <= 2 ? required.length : required.length - 1;
  let score = 0;
  for (const group of required) {
    if (group.some((t) => pmTokenMatchesText(lower, t))) { score++; continue; }
    if (!group._compound && group.some((t) => t.length >= 5 && wordSimJs(t, lower) >= 0.4)) score++;
  }
  if (score < minMatch) return false;
  return optional.every((g) => g.some((t) => pmTokenMatchesText(lower, t)));
}
function computeRelevance(name, article, tokenGroups) {
  if (!tokenGroups || !tokenGroups.length) return 0;
  const hay = [name, article].join(" ").toLowerCase().replace(/ё/g, "е");
  let score = 0;
  for (const group of tokenGroups) {
    const isOptional = pmTokenGroupIsOptional(group);
    const matches = group.some((t) => pmTokenMatchesText(hay, t));
    if (!matches) continue;
    score += isOptional ? 1 : 2;
    if (isOptional && group.some((t) => /^\d+$/.test(t))) {
      const exactMatch = group.some((t) => {
        if (!/^\d+$/.test(t)) return false;
        const esc = t.replace(/[-[\]/{}()*+?.\\^$|]/g, "\\$&");
        return new RegExp(`(?<!\\d)${esc}(?!\\d)`).test(hay);
      });
      if (exactMatch) score += 1;
    }
  }
  return score;
}

async function fuzzySearch(tokenGroups, limit) {
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g) && !g._compound);
  const long = required.filter((g) => g[0] && g[0].length >= 4);
  if (!long.length) return [];
  const params = [];
  const conditions = long.map((group) => {
    const conds = group.map((t) => { params.push(t); return `word_similarity($${params.length}, native_name) > 0.4`; });
    return `(${conds.join(" OR ")})`;
  });
  // Compound groups must also match via flexible regex
  for (const g of tokenGroups.filter(g => g._compound)) {
    const flex = g[0].replace(/([a-zа-я])(\d)/g, "$1\\s*$2").replace(/(\d)([a-zа-я])/g, "$1\\s*$2");
    params.push(flex);
    conditions.push(`native_name ~* $${params.length}`);
  }
  const orderTerms = long.map((group) => { params.push(group[0]); return `word_similarity($${params.length}, native_name)`; });
  params.push(Math.min(limit * 5, 500));
  const sql = `SELECT native_name, partner_name, price FROM pm_snapshot_items WHERE active = true AND price > 0 AND price IS NOT NULL AND ${conditions.join(" AND ")} ORDER BY (${orderTerms.join(" + ")}) DESC LIMIT $${params.length}`;
  try { return await prisma.$queryRawUnsafe(sql, ...params); } catch (e) { console.error("fuzzy error:", e.message); return []; }
}

async function testQuery(q) {
  const tokenGroups = pmQueryToTokenGroups(q);
  const required = tokenGroups.filter((g) => !pmTokenGroupIsOptional(g));
  const sqlGroups = required.filter((g) => !g._compound);
  const activeGroups = sqlGroups.length ? sqlGroups : tokenGroups.filter((g) => !g._compound);
  const orTerms = activeGroups.flatMap((g) => g.flatMap((syn) => [
    { nativeName: { contains: syn, mode: "insensitive" } },
    { article: { contains: syn, mode: "insensitive" } },
  ]));
  const rows = await prisma.priceMasterSnapshotItem.findMany({
    where: { AND: [{ active: true }, { price: { not: null, gt: 0 } }, ...(orTerms.length ? [{ OR: orTerms }] : [])] },
    orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
    take: 2000,
    select: { nativeName: true, partnerName: true, price: true },
  });
  let filtered = rows.filter((r) => {
    const hay = [r.nativeName || "", r.partnerName || ""].join(" ").toLowerCase();
    return pmPassesSearchFilter(hay, tokenGroups);
  });

  let fuzzyUsed = false;
  if (filtered.length < 3) {
    const fuzzyRows = await fuzzySearch(tokenGroups, 20);
    if (fuzzyRows.length) {
      const existingNames = new Set(filtered.map(r => r.nativeName));
      for (const fr of fuzzyRows) {
        if (existingNames.has(fr.native_name)) continue;
        const hay = (fr.native_name || "").toLowerCase();
        if (pmPassesSearchFilterFuzzy(hay, tokenGroups)) {
          filtered.push({ nativeName: fr.native_name, partnerName: fr.partner_name, price: fr.price });
          fuzzyUsed = true;
        }
      }
    }
  }

  filtered.sort((a, b) => {
    const sa = computeRelevance(a.nativeName || "", "", tokenGroups);
    const sb = computeRelevance(b.nativeName || "", "", tokenGroups);
    if (sb !== sa) return sb - sa;
    return (a.price || 0) - (b.price || 0);
  });
  const groups_str = tokenGroups.map(g => (g._compound ? "[C:" : "[") + g.join("|") + "]").join(", ");
  console.log("Q: [" + q + "] candidates:" + rows.length + " -> after-filter:" + filtered.length + (fuzzyUsed ? " [+fuzzy]" : ""));
  console.log("  groups:", groups_str);
  filtered.slice(0, 6).forEach((r) => {
    const rel = computeRelevance(r.nativeName || "", "", tokenGroups);
    console.log("  rel=" + rel, "-", r.nativeName, "|", r.partnerName, "| price:", r.price);
  });
}

async function main() {
  await testQuery("christian dior miss essence 5 ml");
  await testQuery("matsukita 50 ml");
  await testQuery("chanel no5 50 ml");
  await testQuery("doir sauvage 100");
  await testQuery("chanell no5 50 ml");
  await prisma.$disconnect();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
