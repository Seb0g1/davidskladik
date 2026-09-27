// Ozon ↔ Yandex Market catalog reconciliation: is everything that should be on Market there,
// and does everything on Market follow the transfer rules?
//
// Reads every Ozon product of the import accounts and every Market card (active AND archived —
// Market only returns archived cards with {"archived": true}) and reports:
//   missing      — Ozon products eligible by the rules but absent on Market (auto-import picks them up);
//   forbidden    — Market cards whose Ozon product is blocked by the rules (< 20 ml, tester,
//                  «Отливант», junk names…);
//   wrongCategory — cards whose Market category differs from the rule category;
//   badBarcode   — cards with a barcode that is not a valid GTIN (OZN…, 2/02 internal codes);
//   badVendor    — placeholder brands («Без бренда», «Нф-…»);
//   badName      — article instead of a name, or a name entirely in capitals;
//   manualReview — Ozon products whose category cannot be determined (sets, unknown types).
// Read-only: nothing is sent to Market.

let ozonYandexReconcileLast = null;
let ozonYandexReconcileRunning = false;

async function listYandexCatalogCards(shop) {
  const cards = new Map();
  for (const archived of [false, true]) {
    const items = await getYandexOfferMappings(shop, Number.POSITIVE_INFINITY, { archived });
    for (const item of items) {
      const offerId = cleanText(item?.offer?.offerId);
      if (!offerId) continue;
      cards.set(offerId.toLowerCase(), { offer: item.offer || {}, mapping: item.mapping || {}, archived });
    }
  }
  return cards;
}

async function runOzonYandexReconcile({ sampleSize = 50 } = {}) {
  if (ozonYandexReconcileRunning) return { status: "already_running", last: ozonYandexReconcileLast };
  const prisma = getPrisma();
  if (!prisma) return { status: "postgres_disabled" };
  const shops = uniqueYandexShopsByBusiness();
  if (!shops.length) return { status: "no_yandex_shops" };
  ozonYandexReconcileRunning = true;
  const startedAt = Date.now();
  try {
    await loadYandexVendorCanonicalMap();
    const importTargets = getOzonAccounts().filter((account) => account.importEnabled !== false).map((account) => account.id);
    const rows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "ozon", archived: false, target: { in: importTargets } },
      include: { links: true },
    });
    const ozonByOffer = new Map();
    for (const row of rows) {
      const product = productFromPostgres(row);
      const key = cleanText(product?.offerId).toLowerCase();
      if (key && !ozonByOffer.has(key)) ozonByOffer.set(key, product);
    }

    const report = {
      status: "ok",
      at: new Date().toISOString(),
      ozonProducts: ozonByOffer.size,
      marketCards: 0,
      marketArchived: 0,
      counts: {},
      samples: {},
    };
    const add = (bucket, item) => {
      report.counts[bucket] = (report.counts[bucket] || 0) + 1;
      report.samples[bucket] = report.samples[bucket] || [];
      if (report.samples[bucket].length < sampleSize) report.samples[bucket].push(item);
    };

    for (const shop of shops) {
      const cards = await listYandexCatalogCards(shop);
      report.marketCards += cards.size;
      report.marketArchived += Array.from(cards.values()).filter((card) => card.archived).length;

      let index = 0;
      for (const [offerKey, product] of ozonByOffer) {
        if (++index % 500 === 0) await new Promise((resolve) => setImmediate(resolve));
        const candidate = buildOzonYandexImportCandidate(product);
        const card = cards.get(offerKey);
        const offerId = product.offerId;
        if (candidate.blockReasons.length) {
          if (card) add("forbidden", { offerId, name: product.name, reasons: candidate.blockReasons, archived: card.archived });
          continue;
        }
        if (!card) {
          if (candidate.categoryReview) add("manualReview", { offerId, name: product.name, reason: candidate.categoryReview });
          else if (candidate.yandexReady) add("missing", { offerId, name: product.name });
          else add("notReady", { offerId, name: product.name, missing: candidate.missing });
          continue;
        }
        const ruleCategory = candidate.categoryId;
        const marketCategory = Number(card.mapping?.marketCategoryId || card.offer?.marketCategoryId || 0);
        if (ruleCategory && marketCategory && ruleCategory !== marketCategory) {
          add("wrongCategory", { offerId, name: product.name, market: marketCategory, rule: ruleCategory, marketName: cleanText(card.mapping?.marketCategoryName) });
        } else if (!ruleCategory) {
          add("manualReview", { offerId, name: product.name, reason: candidate.categoryReview, market: marketCategory });
        }
      }

      for (const card of cards.values()) {
        const offer = card.offer || {};
        const offerId = cleanText(offer.offerId);
        const badCodes = (Array.isArray(offer.barcodes) ? offer.barcodes : []).filter((code) => !isValidGtin(code));
        if (badCodes.length) add("badBarcode", { offerId, barcodes: badCodes });
        if (isPlaceholderVendor(offer.vendor || "")) add("badVendor", { offerId, vendor: offer.vendor || "" });
        if (looksLikeArticle(offer.name || "", offerId) || isAllCapsName(offer.name || "")) add("badName", { offerId, name: offer.name || "" });
        if (!ozonByOffer.has(offerId.toLowerCase())) report.counts.marketOnly = (report.counts.marketOnly || 0) + 1;
      }
    }
    report.elapsedMs = Date.now() - startedAt;
    ozonYandexReconcileLast = report;
    logger.info("ozon_yandex_reconcile_complete", { counts: report.counts, ozonProducts: report.ozonProducts, marketCards: report.marketCards, elapsedMs: report.elapsedMs });
    return report;
  } finally {
    ozonYandexReconcileRunning = false;
  }
}

app.post("/api/ozon-yandex-import/reconcile", requireAdmin, async (request, response, next) => {
  try {
    response.json(await runOzonYandexReconcile({ sampleSize: Math.min(500, Number(request.body?.sampleSize) || 50) }));
  } catch (error) {
    next(error);
  }
});

app.get("/api/ozon-yandex-import/reconcile/last", requireAdmin, async (_request, response) => {
  response.json(ozonYandexReconcileLast || { status: "never_run" });
});
