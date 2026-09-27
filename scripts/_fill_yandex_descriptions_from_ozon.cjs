// Fills empty descriptions on existing Market cards from their Ozon counterparts.
// Run on prod: node /tmp/filldesc.cjs            -> dry run (counts only, no Ozon/Market calls)
//              node /tmp/filldesc.cjs apply [N]  -> fetch from Ozon and send (N = max cards, default all)
process.env.SERVER_ROLE = "script";
process.chdir("/var/www/davidsklad/davidskladik");
require("/var/www/davidsklad/davidskladik/node_modules/dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const path = require("path"); const Module = require("module");
const root = "/var/www/davidsklad/davidskladik";
const { readServerSource } = require(root + "/server/source.js");
const m = new Module(path.join(root, "server.js")); m.filename = path.join(root, "server.js"); m.paths = Module._nodeModulePaths(root);
m._compile(readServerSource() + `\nmodule.exports.__x = { getPrisma, productFromPostgres, getOzonAccounts, uniqueYandexShopsByBusiness, listYandexCatalogCards, loadYandexVendorCanonicalMap, fillOzonDescriptionsForYandexExport: typeof fillOzonDescriptionsForYandexExport === "function" ? fillOzonDescriptionsForYandexExport : null, buildYandexOfferMapping, sendYandexOfferMappings, yandexCardMatchesOzonProduct, looksLikeArticle, cleanText, resolveYandexCategoryForOzonProduct, ozonRequest, getOzonAccountByTarget };`, m.filename);
const x = m.exports.__x;
const APPLY = process.argv[2] === "apply";
const MAX = Number(process.argv[3]) || Infinity;
// Same rule as isPlaceholderYandexDescription in 02a-ozon-yandex-offer-rules.js.
function placeholder(description, name, offerId) {
  const norm = (v) => String(v || "").replace(/\s+/g, " ").trim().toLowerCase();
  const text = norm(description);
  return text.length < 20 || text === norm(name) || text === norm(offerId);
}
(async () => {
  const targets = x.getOzonAccounts().filter((a) => a.importEnabled !== false).map((a) => a.id);
  const rows = await x.getPrisma().warehouseProduct.findMany({ where: { marketplace: "ozon", archived: false, target: { in: targets } } });
  const ozon = new Map();
  for (const r of rows) { const p = x.productFromPostgres(r); const k = String(p.offerId || "").trim().toLowerCase(); if (k && !ozon.has(k)) ozon.set(k, p); }
  const [shop] = x.uniqueYandexShopsByBusiness();
  const cards = await x.listYandexCatalogCards(shop);
  const stats = { cards: cards.size, emptyDesc: 0, noOzon: 0, mismatch: 0, candidates: 0, storedOzonDesc: 0 };
  const pre = [];
  for (const [key, card] of cards) {
    const offer = card.offer || {};
    if (!placeholder(offer.description, offer.name, offer.offerId)) continue;
    stats.emptyDesc++;
    const p = ozon.get(key); if (!p) { stats.noOzon++; continue; }
    // Never put another product's text on a card: names must describe the same product.
    const junkCard = x.looksLikeArticle(offer.name || "", offer.offerId);
    const ozonName = p.ozon?.name || p.name;
    const ruleCat = x.resolveYandexCategoryForOzonProduct({ typeId: 0, name: ozonName }).categoryId || x.resolveYandexCategoryForOzonProduct({ typeId: 0, name: offer.name }).categoryId || 0;
    if (!junkCard && !x.yandexCardMatchesOzonProduct(offer.name, ozonName, ruleCat)) { stats.mismatch++; continue; }
    if (!placeholder(p.ozon?.description, ozonName, p.offerId)) stats.storedOzonDesc++;
    pre.push({ p, offerId: offer.offerId });
  }
  stats.candidates = pre.length;
  console.log("stats", JSON.stringify(stats));
  if (!APPLY) {
    // Sample: how many of the candidates really have a description on Ozon.
    let real = 0; const got = [];
    for (const c of pre.slice(0, 40)) {
      const account = x.getOzonAccountByTarget(c.p.target || "ozon");
      try {
        const data = await x.ozonRequest("/v1/product/info/description", { offer_id: c.p.offerId }, account);
        const d = x.cleanText(data?.result?.description || "");
        if (!placeholder(d, c.p.ozon?.name || c.p.name, c.p.offerId)) { real++; if (got.length < 5) got.push({ offerId: c.offerId, d: d.slice(0, 120) }); }
      } catch (e) { console.log("ozon err", c.offerId, e.message); }
      await new Promise((r) => setTimeout(r, 100));
    }
    console.log("sample", Math.min(40, pre.length), "with real Ozon description", real);
    got.forEach((g) => console.log(" ", JSON.stringify(g)));
    process.exit(0);
  }
  if (!x.fillOzonDescriptionsForYandexExport) throw new Error("fillOzonDescriptionsForYandexExport not deployed");
  const batch = pre.slice(0, MAX);
  const filled = await x.fillOzonDescriptionsForYandexExport(batch.map((c) => c.p));
  const updates = [];
  filled.forEach((p, i) => {
    const description = x.buildYandexOfferMapping(p).offer.description;
    if (description) updates.push({ offerId: batch[i].offerId, description });
  });
  console.log("fetched", batch.length, "withDescription", updates.length);
  const results = await x.sendYandexOfferMappings(shop, updates);
  const ok = results.filter((r) => r.ok).length; const failed = results.filter((r) => !r.ok);
  console.log("SENT ok", ok, "failed", failed.length);
  failed.slice(0, 20).forEach((f) => console.log("  FAIL", f.offerId, f.error));
  process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
