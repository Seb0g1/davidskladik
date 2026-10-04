"use strict";

// Perfume name parser and matcher: our marketplace card titles («Kilian Angels' Share Парфюмерная вода 7.5 мл»,
// «Парфюмерная вода Giorgio Armani My Way Intense женская 90 мл») against PriceMaster rows
// («Paco Rabanne Invictus m edt100ml», «ISSEY MIYAKE L'EAU D' ISSEY SPORT men edt 100 ml», «… 100ml edp TESTER»).
//
// parsePerfumeName(text, { brands, brand }) → { brandKey, name, nameTokens, volume, type, concentration, gender,
//   tester, decant, sample, set, defect, clone, nonPerfume, bracketWords }
// comparePerfumes(card, row) → { ok, confidence: "exact" | "probable", issues, reason }
//
// Rules (from the owner): the perfume name is what is compared (not the brand words); EDP = парфюмерная вода,
// EDT = туалетная вода, EDC = одеколон, Extrait / Parfum = духи; men = man / men / m / homme / hom / masculine / мужской,
// women = woman / wom / femme / feminine / lady / w / l / женский, unisex = u / унисекс. Apostrophes, split / joined
// words («L'Eau D'Issey» = «L EAU D ISSEY» = «Leau Dissey») compare equal.

const APOSTROPHES = /['’‘`´ʼ′]/g;
const L = "0-9a-zа-я";
// JavaScript \b only knows Latin letters: «парфюмерная» has no \b around it. W(...) = the words as whole words.
const W = (alternatives) => new RegExp(`(?<![${L}])(?:${alternatives})(?![${L}])`);
const WG = (alternatives) => new RegExp(`(?<![${L}])(?:${alternatives})(?![${L}])`, "g");

function baseText(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[\u00c0-\u024f]/g, (c) => c.normalize("NFD").replace(/[\u0300-\u036f]/g, ""))
    .replace(/ё/g, "е")
    .replace(APOSTROPHES, "")
    .replace(/&/g, " and ")
    .replace(/№\s*/g, " no ")
    .replace(/(?<![a-zа-я])n[o°º]?\.?\s*(\d)/g, " no $1")
    .replace(/[«»"“”„]/g, " ");
}

/** Letters and digits only — «L'Eau D' Issey» and «leaudissey» give the same key. */
const squash = (text) => baseText(text).replace(/[^0-9a-zа-я]+/g, "");
const HOMOGLYPHS = { а: "a", в: "b", е: "e", к: "k", м: "m", н: "h", о: "o", р: "p", с: "c", т: "t", у: "y", х: "x" };
const fixMixed = (t) => (/[a-z]/.test(t) && /[а-я]/.test(t) ? t.replace(/[а-я]/g, (c) => HOMOGLYPHS[c] || c) : t);
const tokensOf = (text) => baseText(text).split(/[^0-9a-zа-я]+/).filter(Boolean).map(fixMixed);

// ─── Brands ──────────────────────────────────────────────────────────────────
// Canonical key for brands written differently by sellers
const BRAND_ALIASES = {
  christiandior: "dior", cdior: "dior", dior: "dior", diorparfums: "dior",
  giorgioarmani: "armani", armani: "armani", emporioarmani: "armani", armaniprive: "armani",
  yvessaintlaurent: "ysl", ysl: "ysl", saintlaurent: "ysl",
  dolceandgabbana: "dolcegabbana", dolcegabbana: "dolcegabbana", dandg: "dolcegabbana", dg: "dolcegabbana",
  calvinklein: "calvinklein", ck: "calvinklein",
  hugoboss: "hugoboss", boss: "hugoboss",
  pacorabanne: "rabanne", rabanne: "rabanne",
  jeanpaulgaultier: "gaultier", jpg: "gaultier", gaultier: "gaultier",
  maisonfranciskurkdjian: "mfk", mfk: "mfk", franciskurkdjian: "mfk",
  thierrymugler: "mugler", mugler: "mugler",
  bykilian: "kilian", kilian: "kilian", kilianparis: "kilian",
  abercrombieandfitch: "abercrombiefitch", abercrombiefitch: "abercrombiefitch", aandf: "abercrombiefitch", abercrombie: "abercrombiefitch",
  carolinaherrera: "carolinaherrera", herrera: "carolinaherrera",
  esteelauder: "esteelauder",
  vancleefandarpels: "vancleefarpels", vancleefarpels: "vancleefarpels", vancleef: "vancleefarpels",
  salvatoreferragamo: "ferragamo", ferragamo: "ferragamo",
  bvlgari: "bvlgari", bulgari: "bvlgari",
  jomalone: "jomalone", jomalonelondon: "jomalone",
  antoniobanderas: "banderas", abanderas: "banderas", banderas: "banderas",
  narcisorodriguez: "narcisorodriguez",
  thehouseofoud: "houseofoud", houseofoud: "houseofoud",
  parfumsdemarly: "parfumsdemarly",
  sterlingarmaf: "armaf", armaf: "armaf",
  elizabetharden: "elizabetharden",
  tomford: "tomford",
  initioparfumsprives: "initio", initio: "initio",
  lattafaperfumes: "lattafa", lattafa: "lattafa",
  laurabiagiotti: "laurabiagiotti", lbiagiotti: "laurabiagiotti",
  zadigandvoltaire: "zadigvoltaire", zadigvoltaire: "zadigvoltaire",
  viktorandrolf: "viktorrolf", viktorrolf: "viktorrolf",
  juliettehasagun: "juliettehasagun",
  memoizelondon: "memoize", memoize: "memoize",
  exnihilo: "exnihilo",
  maisonmargiela: "margiela", maisonmartinmargiela: "margiela", margiela: "margiela", montblanc: "montblanc", mont: "montblanc",
  rojaparfums: "roja", rojadove: "roja", roja: "roja", memoparis: "memo", memo: "memo", parfumsdenicolai: "nicolai", nicolai: "nicolai",
  ellakparfums: "ellak", ellak: "ellak", haute: "hfc", hautefragrancecompany: "hfc", bdkparfums: "bdk", goldfieldandbanks: "goldfieldbanks",
  stephanehumbertlucas: "shl777", shl777: "shl777",
};
const BRAND_KEY_TAILS = new Set(["parfums", "perfumes", "parfum", "perfume", "paris", "london", "australia", "milano", "italia", "perfumery", "fragrances", "fragrance", "cosmetics", "beauty", "new", "york"]);
const brandKey = (brand) => {
  const k = squash(brand);
  if (BRAND_ALIASES[k]) return BRAND_ALIASES[k];
  const toks = tokensOf(brand);
  while (toks.length > 1 && BRAND_KEY_TAILS.has(toks[toks.length - 1])) toks.pop();
  const short = toks.join("");
  return BRAND_ALIASES[short] || short || k;
};

// words that Fragrantica lists as one-word brands but sellers use in perfume names
const GENERIC_BRAND_WORDS = new Set([
  "the", "and", "for", "eau", "rose", "roses", "blue", "black", "white", "gold", "silver", "red", "pink", "green", "oud", "musk", "amber", "vanilla",
  "love", "night", "day", "sport", "intense", "extreme", "parfum", "parfums", "perfume", "perfumes", "edp", "edt", "man", "men", "woman", "women",
  "lady", "homme", "femme", "pour", "classic", "original", "collection", "edition", "limited", "new", "tester", "mini", "gift", "set", "body", "hair",
  "mist", "oil", "spray", "deo", "lotion", "cream", "soap", "candle", "home", "summer", "winter", "spring", "fresh", "sun", "sea", "ocean", "flower",
  "flowers", "fleur", "noir", "nero", "bleu", "blanc", "rouge", "absolu", "absolute", "elixir", "essence", "aqua", "acqua", "life", "light", "dark",
  "wood", "woods", "leather", "tobacco", "cherry", "peach", "coffee", "honey", "sugar", "candy", "ice", "fire", "star", "moon", "sky", "angel",
  "angels", "secret", "dream", "dreams", "magic", "royal", "king", "queen", "prince", "princess", "lord", "miss", "mr", "mister", "boy", "girl",
  "free", "pure", "nude", "glow", "shine", "bright", "crystal", "diamond", "diamonds", "pearl", "velvet", "silk", "cashmere", "orange", "lemon",
  "lime", "mint", "tea", "iris", "jasmine", "lily", "violet", "orchid", "tuberose", "patchouli", "vetiver", "cedar", "santal", "sandal", "ambre",
  "voyage", "paris", "london", "milano", "roma", "tokyo", "dubai", "arabia", "arabian", "oriental", "orient", "east", "west", "luxe", "maison",
  "prive", "private", "test", "sample", "vial", "unisex", "extrait", "cologne", "spice", "blend", "nuit", "jour", "bois", "cuir", "fleurs", "vert",
  "infinity", "legend", "hero", "icon", "idol", "joy", "good", "bad", "wild", "sweet", "sexy", "hot", "cool", "bloom", "garden", "paradise",
]);

/** Brand index: token sequences of every known brand (Fragrantica + aliases), longest first. */
function buildBrandIndex(names = []) {
  const byFirst = new Map();
  const vocab = new Map();
  byFirst.vocab = vocab;
  const add = (phrase, key, force = false) => {
    const toks = tokensOf(phrase);
    if (!toks.length) return;
    // one-letter / generic one-word «brands» would eat perfume names
    if (!force && toks.length === 1 && (toks[0].length < 3 || GENERIC_BRAND_WORDS.has(toks[0]) || /^\d+$/.test(toks[0]))) return;
    const k = key || brandKey(phrase);
    if (!vocab.has(k)) vocab.set(k, new Set());
    for (const t of toks) if (t.length > 1) vocab.get(k).add(t);
    const list = byFirst.get(toks[0]) || [];
    if (!list.some((e) => e.toks.join(" ") === toks.join(" "))) list.push({ toks, key: key || brandKey(phrase) });
    byFirst.set(toks[0], list);
  };
  for (const name of names) if (name) add(name);
  // seller spellings
  for (const [phrase, key] of [
    ["christian dior", "dior"], ["c dior", "dior"], ["dior", "dior"], ["giorgio armani", "armani"], ["armani", "armani"], ["emporio armani", "armani"],
    ["yves saint laurent", "ysl"], ["ysl", "ysl"], ["saint laurent", "ysl"], ["dolce and gabbana", "dolcegabbana"], ["d and g", "dolcegabbana"],
    ["dolce gabbana", "dolcegabbana"], ["calvin klein", "calvinklein"], ["ck", "calvinklein"], ["hugo boss", "hugoboss"], ["boss", "hugoboss"],
    ["paco rabanne", "rabanne"], ["rabanne", "rabanne"], ["p r paco rabanne", "rabanne"], ["jean paul gaultier", "gaultier"], ["jpg", "gaultier"],
    ["maison francis kurkdjian", "mfk"], ["mfk", "mfk"], ["francis kurkdjian", "mfk"], ["thierry mugler", "mugler"], ["mugler", "mugler"],
    ["by kilian", "kilian"], ["kilian", "kilian"], ["kilian paris", "kilian"], ["abercrombie and fitch", "abercrombiefitch"], ["a and f", "abercrombiefitch"],
    ["abercrombie fitch", "abercrombiefitch"], ["carolina herrera", "carolinaherrera"], ["van cleef and arpels", "vancleefarpels"],
    ["van cleef", "vancleefarpels"], ["salvatore ferragamo", "ferragamo"], ["ferragamo", "ferragamo"], ["bvlgari", "bvlgari"], ["bulgari", "bvlgari"],
    ["jo malone", "jomalone"], ["jo malone london", "jomalone"], ["antonio banderas", "banderas"], ["a banderas", "banderas"], ["banderas", "banderas"],
    ["the house of oud", "houseofoud"], ["house of oud", "houseofoud"], ["parfums de marly", "parfumsdemarly"], ["sterling armaf", "armaf"],
    ["armaf", "armaf"], ["tom ford", "tomford"], ["initio parfums prives", "initio"], ["initio", "initio"], ["lattafa perfumes", "lattafa"],
    ["lattafa", "lattafa"], ["laura biagiotti", "laurabiagiotti"], ["l biagiotti", "laurabiagiotti"], ["zadig and voltaire", "zadigvoltaire"],
    ["zadig voltaire", "zadigvoltaire"], ["viktor and rolf", "viktorrolf"], ["viktor rolf", "viktorrolf"], ["juliette has a gun", "juliettehasagun"],
    ["memoize london", "memoize"], ["memoize", "memoize"], ["ex nihilo", "exnihilo"], ["essential parfums", "essentialparfums"],
    ["maison margiela", "margiela"], ["maison martin margiela", "margiela"], ["martin margiela", "margiela"], ["margiela", "margiela"],
    ["montblanc", "montblanc"], ["mont blanc", "montblanc"], ["roja parfums", "roja"], ["roja dove", "roja"], ["roja", "roja"],
    ["memo paris", "memo"], ["memo", "memo"], ["parfums de nicolai", "nicolai"], ["nicolai", "nicolai"], ["ella k parfums", "ellak"], ["ella k", "ellak"],
    ["lancome", "lancome"], ["lacoste", "lacoste"], ["hfc", "hfc"], ["haute fragrance company", "hfc"], ["bdk", "bdk"], ["bdk parfums", "bdk"],
    ["goldfield and banks", "goldfieldbanks"], ["stephane humbert lucas", "shl777"], ["shl 777", "shl777"], ["pierre guillaume", "pierreguillaume"],
    ["stephanie de bruijn", "stephaniedebruijn"], ["hope", "hope"], ["juicy couture", "juicycouture"], ["max philip", "maxphilip"],
    ["giorgio armani prive", "armani"], ["armani prive", "armani"], ["emporio", "armani"],
    ["p r", "rabanne"], ["ck", "calvinklein"], ["dg", "dolcegabbana"], ["d and g", "dolcegabbana"], ["ysl", "ysl"], ["jpg", "gaultier"], ["mfk", "mfk"],
    ["hfc", "hfc"], ["bdk", "bdk"], ["shl", "shl777"], ["a and f", "abercrombiefitch"],
  ]) add(phrase, key, true);
  for (const list of byFirst.values()) list.sort((a, b) => b.toks.length - a.toks.length);
  return byFirst;
}

function isKnownBrand(name, index) {
  const toks = tokensOf(name);
  return Boolean(toks.length && (index.get(toks[0]) || []).some((e) => e.toks.join(" ") === toks.join(" ")));
}

function findSeq(tokens, seq) {
  if (!seq.length) return -1;
  outer: for (let i = 0; i + seq.length <= tokens.length; i += 1) {
    for (let j = 0; j < seq.length; j += 1) if (tokens[i + j] !== seq[j]) continue outer;
    return i;
  }
  return -1;
}

function findBrand(tokens, index, hint) {
  // the card's own brand column is often «brand + perfume» («Calvin Klein Euphoria») — trust it only when it is a known brand
  if (hint && index && !isKnownBrand(hint, index)) hint = "";
  if (hint && index) {
    const own = findBrand(tokens, index, "");
    if (own && own.key === brandKey(hint)) return own;
  }
  if (hint) {
    const hintToks = tokensOf(hint);
    const at = findSeq(tokens, hintToks);
    if (at >= 0) return { key: brandKey(hint), start: at, end: at + hintToks.length };
  }
  if (!index) return null;
  // the longest known brand, earliest in the title wins
  let best = null;
  for (let i = 0; i < tokens.length; i += 1) {
    for (const e of index.get(tokens[i]) || []) {
      if (findSeq(tokens.slice(i, i + e.toks.length), e.toks) !== 0) continue;
      const cand = { key: e.key, start: i, end: i + e.toks.length, len: e.toks.join("").length };
      if (!best || cand.start < best.start || (cand.start === best.start && cand.len > best.len)) best = cand;
    }
    if (best && i > best.end + 2) break;
  }
  return best;
}

// ─── Concentration, type, gender, flags ──────────────────────────────────────
const CONCENTRATION_PATTERNS = [
  ["parfum", W("extrait de parfum|extrait|exdp|perfume extract|parfum extract|extract de parfum|pure parfum|parfum pur|духи|perfume|parfume")],
  ["edp", W("eau de parfum|edp|e d p|парфюмерн[а-я]* вода|вода парфюмерн[а-я]*|парфюмированн[а-я]* вода|парфюмерн[а-я]*|парфюмированн[а-я]*")],
  ["edt", W("eau de toilette|edt|e d t|туалетн[а-я]* вода|вода туалетн[а-я]*|туалетн[а-я]*|туалтн[а-я]*|туал[а-я]* вода")],
  ["edc", W("eau de cologne|edc|cologne|одеколон|кельнск[а-я]* вода")],
  ["parfum", W("parfum|perfum")],
];
const TYPE_PATTERNS = [
  ["oil", W("perfume oil|parfum oil|oil|масло|масляные духи|маслян[а-я]*")],
  ["mist", W("body mist|hair mist|mist|мист|спрей для тела|дымка|hair perfume|для волос")],
  ["deo", W("deo|deodorant|дезодорант[а-я]*|антиперспирант[а-я]*")],
  ["lotion", W("after shave|aftershave|a sh|лосьон[а-я]*|после бритья|body lotion")],
];
const NON_PERFUME = W("шампун[а-я]*|shampoo|кондиционер[а-я]*|conditioner|маск[а-я]*|mask|крем[а-я]*|cream|гель|гели|gel|мыло|soap|свеч[а-я]*|candle|скраб|scrub|бальзам[а-я]*|balm|краск[а-я]*|сыворотк[а-я]*|serum|тоник|пудр[а-я]*|помад[а-я]*|тушь|консилер|диффузор[а-я]*|diffuser|аромадиффузор|молочко|milk|пена|foam|ароматизатор[а-я]*");
const TESTER = W("tester|testr|tstr|test|тестер|тест|tst");
const DECANT = W("отливант|распив|decant|atomizer|атомайзер|отлив|разлив");
const SAMPLE = W("sample|vial|пробник|пробирк[а-я]*|сэмпл");
const SET = /(?<![0-9a-zа-я])(набор|set|gift|подарочн[а-я]*|коллекц[а-я]*)(?![0-9a-zа-я])|\d\s*[xх×*]\s*\d|(ml|мл)\s*\+|\+\s*\d+\s*(ml|мл)/;
const DEFECT = /подмят|мят(ая|ый|ой)?\s*(короб|упак)|без\s*(короб|упак|крыш|слюд|целлофан|колпач)|уценк|брак|дефект|царап|поврежд|витрин|damaged|no\s*box|without\s*box|unbox|не\s*хватает|неполн|отпит|без\s*спрея|без\s*распыл/;
const DEFECT_WORDS = WG("подмят[а-я]*|мят[а-я]*|без|короб[а-я]*|упаков[а-я]*|крыш[а-я]*|слюд[а-я]*|целлофан[а-я]*|колпач[а-я]*|уценк[а-я]*|брак|дефект|царап[а-я]*|поврежд[а-я]*|витрин[а-я]*|damaged|box|without|unbox|не|хватает|неполн[а-я]*|отпит[а-я]*|спрея|распыл[а-я]*");

const MEN_WORDS = new Set(["man", "men", "m", "homme", "hom", "masculine", "masculin", "uomo", "him", "male", "муж", "мужской", "мужская", "мужские", "мужчин", "мужчины", "мужск"]);
const WOMEN_WORDS = new Set(["woman", "women", "wom", "femme", "feminine", "feminin", "lady", "ladies", "w", "l", "donna", "her", "female", "жен", "женский", "женская", "женские", "женщин", "женщины", "женск"]);
const UNISEX_WORDS = new Set(["unisex", "uni", "u", "унисекс", "unisexe", "уни"]);
const isMen = (t) => MEN_WORDS.has(t) || /^(мужчин|мужск)/.test(t);
const isWomen = (t) => WOMEN_WORDS.has(t) || /^(женщин|женск)/.test(t);
// gender words that can be the perfume's name («Dior Homme», «Rochas Femme», «Kenzo pour Homme»)
const NAME_GENDER_WORDS = new Set(["homme", "femme", "uomo", "donna", "him", "her", "man", "woman", "men", "women"]);

// a brand written again inside the perfume name: «YSL Y», «Emporio Armani Diamonds», «C.Dior Miss Dior» (miss dior stays: dior is its name)
const BRAND_NAME_WORDS = { ysl: ["ysl"], armani: ["armani", "giorgio"], dolcegabbana: ["dolce", "gabbana", "dg"], calvinklein: ["ck"] };
// words of the house that follow a brand: «Afnan Perfumes», «Hayari Parfums», «Memo Paris»
const BRAND_TAIL_WORDS = new Set(["perfumes", "parfums", "perfumery"]);
// abbreviations of a line inside the name
const NAME_SYNONYMS = { guerlain: { aa: ["aqua", "allegoria"] } };
// articles: compared only when the name is nothing else («Juicy Couture La La»)
const ARTICLES = new Set(["de", "di", "la", "le", "les", "el", "the", "of", "by", "a", "an", "du", "des", "il", "lo"]);

// words that describe the product, not the perfume
const NOISE = new Set([
  "вода", "для", "и", "в", "на", "с", "из", "мл", "ml", "spray", "vapo", "vaporisateur", "natural", "naturale", "nat", "new", "новый", "новинка",
  "новая", "original", "оригинал", "оригинальный", "франция", "france", "италия", "italy", "испания", "spain", "оаэ", "uae", "англия", "uk", "usa", "сша",
  "марк", "маркир", "маркировка", "маркированный", "честный", "знак", "шт", "штук", "pcs", "pc", "flacon", "флакон", "bottle",
  "edition", "версия", "version", "for", "pour", "eau", "aromat", "аромат", "марка", "маркой", "крышкой", "крышка", "уни", "lux", "refillabel",
  "spr", "old", "старый", "дизайн", "design", "стар", "диз", "new", "нов",
  "парфюм", "парфюмерия", "парфюмерный", "спрей", "мини", "mini", "travel", "дорожный", "оригинальная", "упаковка", "fl", "oz", "and", "also", "ед",
]);

const parseVolumes = (text) => {
  const out = [];
  for (const m of text.matchAll(/(\d+(?:[.,]\d+)?)\s*(ml|мл|гр|г|gr|g)(?![a-zа-я])/g)) {
    const v = Number(m[1].replace(",", "."));
    if (v > 0 && v < 5000) out.push(v);
  }
  return out;
};

// phrases that look like a concentration but name the perfume («Tresor La Nuit Le Parfum», «Not a Perfume»)
const NAME_PHRASES = [[/(?<![0-9a-zа-я])le parfum(?![0-9a-zа-я])/g, " leparfum "], [/(?<![0-9a-zа-я])not a perfume(?![0-9a-zа-я])/g, " notaperfume "]];
const spaced = (text) => NAME_PHRASES.reduce((t, [re, to]) => t.replace(re, to), text)
  .replace(/(?<![0-9a-zа-я])([ld])\s+(?!and(?![a-z]))(?=[aeiouyh][a-z])/g, "$1")
  .replace(/([а-я])(\d)/g, "$1 $2")
  .replace(/(\d)(ml|мл)(?![a-zа-я])/g, "$1 $2")
  .replace(/(edp|edt|edc)(\d)/g, "$1 $2")
  .replace(/(\d)(edp|edt|edc)/g, "$1 $2");

/** Parses a perfume title. brands — buildBrandIndex(); brand — the brand the card already knows (optional). */
function parsePerfumeName(text, { brands = null, brand = "" } = {}) {
  const raw = String(text || "");
  const lower = spaced(baseText(raw));
  // bracket contents: «( Megamare Orto Parisi )» is a clone reference, «(Silver)» a variant, «(L)» a gender
  const brackets = [...lower.matchAll(/\(([^)]*)\)|\[([^\]]*)\]/g)].map((m) => (m[1] || m[2] || "").trim()).filter(Boolean);
  const outside = lower.replace(/\([^)]*\)|\[[^\]]*\]/g, " ");
  let volumes = parseVolumes(lower);
  if (!volumes.length) {
    // «YSL Manifesto L'Eclat (L) 90 edt», «… 100 edp»
    const m = lower.match(/(?<![0-9a-zа-я.,])(\d{1,4}(?:[.,]\d)?)\s*(edp|edt|edc|parfum|extrait)(?![0-9a-zа-я])|(?<![0-9a-zа-я])(edp|edt|edc|parfum|extrait)\s*(\d{1,4}(?:[.,]\d)?)\s*$/);
    const v = m ? Number(String(m[1] || m[4]).replace(",", ".")) : 0;
    if (v >= 1 && v <= 1000) volumes = [v];
  }
  const tokens = tokensOf(outside
    .replace(/(?<![0-9a-zа-я])\d+\s*[xх×*]\s*\d+(?:[.,]\d+)?\s*(ml|мл)?(?![0-9a-zа-я])/g, " ")
    .replace(/(?<![0-9a-zа-я])\d+(?:[.,]\d+)?\s*(ml|мл|гр|г|gr|g)(?![0-9a-zа-я])/g, " "));
  let b = findBrand(tokens, brands, brand);
  // a known brand far inside the title is a word of the perfume name («Jul Et Mad Bergamote Twist»)
  const firstReal = tokens.findIndex((t) => /[a-z]/.test(t) && !NOISE.has(t) && !CONCENTRATION_PATTERNS.some(([, re]) => re.test(` ${t} `)));
  if (b && firstReal >= 0 && b.start > firstReal + 1) b = null;
  let brandGuessed = false;
  if (!b) {
    // a brand Fragrantica does not know (cosmetics, niche): the first real word of the title, at most a guess
    const i = tokens.findIndex((t) => /[a-z]/.test(t) && t.length >= 2 && !NOISE.has(t) && !CONCENTRATION_PATTERNS.some(([, re]) => re.test(` ${t} `)));
    if (i >= 0) { b = { key: `~${tokens[i]}`, start: i, end: i + 1 }; brandGuessed = true; }
  }
  // «C.Dior», «J.Del Pozo», «A.Banderas»: the initial before the brand
  if (b && !brandGuessed && b.start > 0 && tokens[b.start - 1].length === 1 && /[a-z]/.test(tokens[b.start - 1])) b = { ...b, start: b.start - 1 };
  // «Afnan Perfumes Turathi», «Hayari Parfums FeHom»: a word of the house right after the brand
  while (b && b.end < tokens.length && BRAND_TAIL_WORDS.has(tokens[b.end])) b = { ...b, end: b.end + 1 };
  const rest = b ? [...tokens.slice(0, b.start), "|", ...tokens.slice(b.end)] : tokens;
  const restText = ` ${rest.join(" ")} `;
  const bracketText = brackets.join(" ");

  let concentration = "";
  for (const [kind, re] of CONCENTRATION_PATTERNS) if (re.test(restText) || re.test(` ${bracketText} `)) { concentration = kind; break; }
  let type = "perfume";
  for (const [kind, re] of TYPE_PATTERNS) if (re.test(restText)) { type = kind; break; }
  const nonPerfume = NON_PERFUME.test(restText) && !W("edp|edt|духи|парфюмерн[а-я]*|туалетн[а-я]*").test(restText);

  const genders = new Set();
  for (const t of [...rest, ...tokensOf(bracketText)]) {
    if (UNISEX_WORDS.has(t)) genders.add("unisex");
    else if (isMen(t)) genders.add("men");
    else if (isWomen(t)) genders.add("women");
  }
  const gender = genders.size === 1 ? [...genders][0] : genders.size > 1 ? "unisex" : "";
  const flagsText = ` ${lower} `;
  const tester = TESTER.test(flagsText);
  const decant = DECANT.test(flagsText);
  const sample = SAMPLE.test(flagsText) || (volumes.length > 0 && Math.max(...volumes) <= 3);
  const isSet = SET.test(flagsText) || (volumes.length > 1 && new Set(volumes).size > 1);
  const defect = DEFECT.test(flagsText);

  // the perfume name: what is left without brand, concentration / type / gender words, volumes, flags and noise
  let nameText = restText;
  for (const [, re] of [...CONCENTRATION_PATTERNS, ...TYPE_PATTERNS]) nameText = nameText.replace(new RegExp(re.source, "g"), " ");
  for (const re of [TESTER, DECANT, SAMPLE]) nameText = nameText.replace(new RegExp(re.source, "g"), " ");
  if (defect) nameText = nameText.replace(DEFECT_WORDS, " ");
  nameText = nameText.replace(/(?<![0-9a-zа-я])\d+(?:[.,]\d+)?\s*(ml|мл|гр|г|gr|g)(?![0-9a-zа-я])/g, " ");
  nameText = nameText.replace(/(?<![0-9a-zа-я])\d+\s*[xх×*]\s*\d+(?:[.,]\d+)?(?![0-9a-zа-я])/g, " ");
  // the brand's own words repeated in the name («Mon Guerlain», «Emporio Armani Diamonds») go on both sides
  const brandWords = new Set([...(b ? tokens.slice(b.start, b.end) : []), ...(BRAND_NAME_WORDS[b?.key] || []),
    ...(b && !brandGuessed && brands?.vocab?.get(b.key) ? [...brands.vocab.get(b.key)].filter((t) => t.length >= 3) : [])]);
  const synonyms = NAME_SYNONYMS[b?.key] || {};
  const genderWords = [];
  let nameTokens = [];
  for (const t of tokensOf(nameText.replace(/\|/g, " ")).flatMap((x) => synonyms[x] || [x])) {
    if (brandWords.has(t)) continue;
    if (UNISEX_WORDS.has(t) || isMen(t) || isWomen(t)) { genderWords.push(t); continue; }
    if (NOISE.has(t)) continue;
    if (/^\d+([.,]\d+)?$/.test(t) && volumes.includes(Number(t.replace(",", ".")))) continue;
    nameTokens.push(t);
  }
  const withArticles = nameTokens;
  nameTokens = nameTokens.filter((t) => !ARTICLES.has(t));
  if (!nameTokens.length) nameTokens = withArticles;
  if (!nameTokens.length) nameTokens = genderWords.filter((t) => NAME_GENDER_WORDS.has(t));
  // «Devotion Pour Homme» ≠ «Devotion»: the gender words of the name, checked in comparePerfumes
  const nameGender = genderWords.filter((t) => NAME_GENDER_WORDS.has(t) && !["man", "men", "woman", "women"].includes(t));
  let clone = false;
  if (brands) {
    for (const x of brackets) {
      const xb = findBrand(tokensOf(x), brands);
      if (xb && (!b || xb.key !== b.key)) clone = true;
    }
  }
  const volume = volumes.length ? Math.max(...volumes) : null;
  const bracketWords = tokensOf(bracketText).filter((t) => !NOISE.has(t) && !/^\d/.test(t) && !isMen(t) && !isWomen(t) && !UNISEX_WORDS.has(t)
    && !TESTER.test(` ${t} `) && !DECANT.test(` ${t} `) && !SAMPLE.test(` ${t} `) && !CONCENTRATION_PATTERNS.some(([, re]) => re.test(` ${t} `))
    && !new RegExp(DEFECT_WORDS.source).test(` ${t} `) && !NOISE.has(t));
  return {
    raw, brandKey: b ? b.key : "", brandGuessed, brandWords: b ? tokens.slice(b.start, b.end) : [], name: nameTokens.join(" "), nameTokens, nameGender,
    volume, volumes, type, concentration, gender, tester, decant, sample, set: isSet, defect, clone, nonPerfume, bracketWords,
  };
}

// words a price list adds to the same perfume (line, packaging) — the name still means the same bottle
const EXTRA_OK = new Set([
  "maison", "collection", "luxe", "prive", "privee", "private", "blend", "exclusifs", "les", "tube", "box", "leather",
  "refillable", "refill", "limited", "edition", "vapo", "vaporisateur", "atomizer", "spray", "jar", "coffret", "boxed", "lux", "travel", "refillabel",
]);
/** «gentlemen» / «gentleman», «limiited» / «limited»: one letter apart in one word of ≥ 5 letters, the rest equal. */
function oneTypoApart(a, b) {
  let diffs = 0;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] === b[i]) continue;
    if (a[i].length < 5 || b[i].length < 5 || editDistance(a[i], b[i]) > 1) return false;
    // «lhomme» / «homme», «dor» / «or»: an elided article makes another name, not a typo
    if (/^[ld]/.test(a[i]) && a[i].slice(1) === b[i]) return false;
    if (/^[ld]/.test(b[i]) && b[i].slice(1) === a[i]) return false;
    diffs += 1;
  }
  return diffs === 1;
}
function editDistance(x, y) {
  if (Math.abs(x.length - y.length) > 1) return 2;
  const dp = Array.from({ length: x.length + 1 }, (_, i) => [i, ...new Array(y.length).fill(0)]);
  for (let j = 1; j <= y.length; j += 1) dp[0][j] = j;
  for (let i = 1; i <= x.length; i += 1) {
    for (let j = 1; j <= y.length; j += 1) dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + (x[i - 1] === y[j - 1] ? 0 : 1));
  }
  return dp[x.length][y.length];
}
const sameMultiset = (a, b) => a.length === b.length && [...a].sort().join(" ") === [...b].sort().join(" ");

/** Is the PriceMaster row the same product as our card? Both are parsePerfumeName() results. */
function comparePerfumes(card, row) {
  const issues = [];
  if (!card.brandKey || !row.brandKey) return { ok: false, reason: "brand" };
  let a = card;
  let z = row;
  if (card.brandKey !== row.brandKey) {
    // a brand Fragrantica does not know, guessed from the first word on one side: «Hayari» = «Hayari Parfums»
    const first = (p) => (p.brandGuessed ? p.brandKey.slice(1) : p.brandWords[0]);
    if (!(card.brandGuessed || row.brandGuessed) || first(card) !== first(row) || String(first(card)).length < 3) return { ok: false, reason: "brand" };
    // the guessed side keeps the other brand words in its name («Liquides Imaginaires Navis» → «Navis»)
    const strip = (p, words) => ({ ...p, nameTokens: p.nameTokens.filter((t) => !words.includes(t)) });
    if (card.brandGuessed) a = strip(card, row.brandWords);
    if (row.brandGuessed) z = strip(row, card.brandWords);
  }
  return compareSameBrand(a, z);
}

function compareSameBrand(card, row) {
  const issues = [];
  if (!card.volume || !row.volume || Math.abs(card.volume - row.volume) > 0.01) return { ok: false, reason: "volume" };
  if (row.set !== card.set) return { ok: false, reason: "set" };
  if (row.tester !== card.tester) return { ok: false, reason: "tester" };
  if (row.decant !== card.decant) return { ok: false, reason: "decant" };
  if (row.nonPerfume !== card.nonPerfume) return { ok: false, reason: "kind" };
  if (row.type !== card.type) return { ok: false, reason: "type" };
  if (!card.nameTokens.length && !row.nameTokens.length) {
    // «Bottega Veneta W edp 7,5ml» — the perfume named after the house
    issues.push("аромат назван как бренд — проверьте");
  } else if (!card.nameTokens.length || !row.nameTokens.length) return { ok: false, reason: "name" };
  const variants = (p) => (p.bracketWords.length ? [p.nameTokens, [...p.nameTokens, ...p.bracketWords]] : [p.nameTokens]);
  let nameSame = !card.nameTokens.length && !row.nameTokens.length;
  let nameLoose = false;
  let typo = false;
  // «№1932» = «1932»; a release year a price list adds («Toy Boy 2019») is not a name
  const clean = (list, other) => list.filter((t) => t !== "no" && !(/^(19|20)\d\d$/.test(t) && !other.includes(t)));
  for (const a0 of variants(card)) {
    for (const b0 of variants(row)) {
      const a = clean(a0, b0);
      const b = clean(b0, a0);
      if (!a.length || !b.length) continue;
      if (sameMultiset(a, b) || a.join("") === b.join("")) nameSame = true;
      else if (a.length === b.length && oneTypoApart([...a].sort(), [...b].sort())) typo = true;
      // the price list adds line / packaging words: «MAISON COLLECTION EDEN-ROC», «LUXE Spice Blend», «… tube»
      else if (a.every((t) => b.includes(t)) && b.filter((t) => !a.includes(t)).every((t) => EXTRA_OK.has(t))) nameLoose = true;
    }
  }
  if (!nameSame && !nameLoose && !typo) return { ok: false, reason: "name" };
  // shades / variants in brackets on both sides that differ: «(fair 4)» ≠ «(Deep 2)»
  if (card.bracketWords.length && row.bracketWords.length && !sameMultiset(card.bracketWords, row.bracketWords)
    && !card.bracketWords.every((t) => row.bracketWords.includes(t)) && !row.bracketWords.every((t) => card.bracketWords.includes(t))) {
    return { ok: false, reason: "name" };
  }
  if (!nameSame && !nameLoose && typo) issues.push("опечатка в названии");
  // «Devotion Pour Homme» vs «Devotion»: homme / femme / him / her on one side only, gender not confirmed → another perfume
  for (const [a, z] of [[card, row], [row, card]]) {
    if (!a.nameGender.length || a.nameGender.every((t) => z.nameGender.includes(t))) continue;
    if (z.gender && a.gender && z.gender !== a.gender) return { ok: false, reason: "name" };
    if (!z.gender) issues.push(`«${a.nameGender.join(" ")}» только с одной стороны`);
  }
  if (!nameSame) issues.push("в прайсе лишние слова линейки / упаковки");
  if (card.concentration && row.concentration && card.concentration !== row.concentration) {
    // «Парфюмерная вода» on a card whose perfume is a Parfum / Extrait (and back) — a seller's label, not another product for sure
    const pair = [card.concentration, row.concentration].sort().join("/");
    if (pair !== "edp/parfum") return { ok: false, reason: "concentration" };
    issues.push("концентрация: парфюмерная вода / духи");
  }
  if (card.gender && row.gender && card.gender !== row.gender) {
    if (card.gender !== "unisex" && row.gender !== "unisex") return { ok: false, reason: "gender" };
    issues.push("пол указан по-разному (унисекс)");
  }
  if (!row.concentration) issues.push("концентрация не указана");
  if (!card.concentration) issues.push("у карточки не указана концентрация");
  if (row.defect) issues.push("уценка / повреждение");
  if (card.brandGuessed || row.brandGuessed) issues.push("бренд определён по первому слову");
  return { ok: true, confidence: issues.length ? "probable" : "exact", issues };
}

/**
 * All rows that match one card. When the card does not say the gender (or the concentration) and the matching rows
 * come in several (men and women versions, EDT and EDP), no row is certain — they all become «probable».
 * rows: [{ row: parsed, ...anything }] → [{ ...item, result }] (only matches).
 */
function matchCardRows(card, rows) {
  const out = [];
  for (const item of rows) {
    const result = comparePerfumes(card, item.row);
    if (result.ok) out.push({ ...item, result });
  }
  if (out.length > 1) {
    const genders = new Set(out.map((x) => x.row.gender).filter((g) => g === "men" || g === "women"));
    const concs = new Set(out.map((x) => x.row.concentration).filter(Boolean));
    for (const x of out) {
      if (!card.gender && genders.size > 1) x.result = { ...x.result, confidence: "probable", issues: [...x.result.issues, "есть мужская и женская версии — у карточки пол не указан"] };
      if (!card.concentration && concs.size > 1) x.result = { ...x.result, confidence: "probable", issues: [...x.result.issues, "есть разные концентрации — у карточки не указана"] };
    }
  }
  return out;
}

module.exports = { parsePerfumeName, comparePerfumes, matchCardRows, buildBrandIndex, brandKey, squash, tokensOf };
