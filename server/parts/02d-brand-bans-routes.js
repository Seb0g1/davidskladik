const brandBansPath = path.join(dataDir, "brand-bans.json");

async function readBrandBans() {
  try {
    const text = await fs.readFile(brandBansPath, "utf8");
    const parsed = JSON.parse(text || "{}");
    return Array.isArray(parsed.bans) ? parsed.bans : [];
  } catch (error) {
    if (error.code !== "ENOENT" && !(error instanceof SyntaxError)) {
      logger.warn("read brand bans failed", { detail: error?.message || String(error) });
    }
    return [];
  }
}

async function writeBrandBans(bans) {
  await fs.mkdir(dataDir, { recursive: true });
  const tmp = `${brandBansPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(tmp, JSON.stringify({ bans }, null, 2), "utf8");
  await fs.rename(tmp, brandBansPath);
}

function banNormalizedKey(value) {
  return normalizedBrandIndexKey(cleanText(value));
}

// GET /api/brand-bans — list all banned brands
app.get("/api/brand-bans", requireAdmin, async (req, res, next) => {
  try {
    const bans = await readBrandBans();
    res.json({ bans });
  } catch (err) {
    next(err);
  }
});

// POST /api/brand-bans — add a brand ban
app.post("/api/brand-bans", requireAdmin, async (req, res, next) => {
  try {
    const displayBrand = cleanText(req.body?.brand || req.body?.displayBrand || "");
    if (!displayBrand) return res.status(400).json({ error: "brand is required" });
    const normalizedBrand = banNormalizedKey(displayBrand);
    if (!normalizedBrand) return res.status(400).json({ error: "invalid brand name" });

    const bans = await readBrandBans();
    const existing = bans.find((b) => banNormalizedKey(b.displayBrand) === normalizedBrand);
    if (existing) return res.json({ ok: true, ban: existing, skipped: true });

    const ban = {
      id: `ban-${Date.now()}`,
      displayBrand,
      normalizedBrand,
      note: cleanText(req.body?.note || ""),
      bannedAt: new Date().toISOString(),
    };
    bans.push(ban);
    await writeBrandBans(bans);
    res.json({ ok: true, ban });
  } catch (err) {
    next(err);
  }
});

// DELETE /api/brand-bans/:id — remove a brand ban
app.delete("/api/brand-bans/:id", requireAdmin, async (req, res, next) => {
  try {
    const id = cleanText(req.params.id);
    const bans = await readBrandBans();
    const next_ = bans.filter((b) => b.id !== id);
    if (next_.length === bans.length) return res.status(404).json({ error: "ban not found" });
    await writeBrandBans(next_);
    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

// PATCH /api/brand-bans/:id/offer-ids — add or remove specific offer IDs from a ban
app.patch("/api/brand-bans/:id/offer-ids", requireAdmin, async (req, res, next) => {
  try {
    const id = cleanText(req.params.id);
    const bans = await readBrandBans();
    const ban = bans.find((b) => b.id === id);
    if (!ban) return res.status(404).json({ error: "ban not found" });

    const rawOfferIds = Array.isArray(req.body?.offerIds) ? req.body.offerIds : [];
    const offerIds = [...new Set(rawOfferIds.map((o) => cleanText(String(o || ""))).filter(Boolean))];
    ban.bannedOfferIds = offerIds;
    await writeBrandBans(bans);
    res.json({ ok: true, ban });
  } catch (err) {
    next(err);
  }
});

// GET /api/brand-bans/brands-search?q=... — autocomplete brands from BrandIndex with marketplace presence
app.get("/api/brand-bans/brands-search", requireAdmin, async (req, res, next) => {
  try {
    const q = cleanText(req.query.q || "").trim();
    if (!q || q.length < 2) return res.json({ brands: [] });

    // Split into words and normalize each — supports "Chabaud Ma" matching both
    // "Chabaud De Maison" (Ozon) and "Chabaud Maison De Parfum" (YM)
    const normWords = q.split(/\s+/).map((w) => banNormalizedKey(w)).filter((w) => w.length >= 2);
    if (!normWords.length) return res.json({ brands: [] });

    const prisma = getPrisma();
    if (prisma && shouldUsePostgresStorage()) {
      const whereClause = normWords.length === 1
        ? { normalizedBrand: { contains: normWords[0] } }
        : { AND: normWords.map((word) => ({ normalizedBrand: { contains: word } })) };

      const rows = await prisma.brandIndexItem.findMany({
        where: whereClause,
        select: { normalizedBrand: true, displayBrand: true, marketplace: true },
        distinct: ["normalizedBrand", "marketplace"],
        orderBy: { displayBrand: "asc" },
        take: 120,
      });
      const map = new Map();
      for (const row of rows) {
        if (!map.has(row.normalizedBrand)) {
          map.set(row.normalizedBrand, { displayBrand: row.displayBrand, normalizedBrand: row.normalizedBrand, marketplaces: new Set() });
        }
        map.get(row.normalizedBrand).marketplaces.add(row.marketplace);
      }
      const brands = [...map.values()].slice(0, 25).map((b) => ({ ...b, marketplaces: [...b.marketplaces] }));
      return res.json({ brands });
    }

    // Fallback: scan warehouse
    const warehouse = await readWarehouse();
    const seen = new Map();
    for (const p of warehouse.products || []) {
      const brand = resolveWarehouseBrand(p);
      if (!brand) continue;
      const nb = banNormalizedKey(brand);
      if (!normWords.every((w) => nb.includes(w))) continue;
      if (!seen.has(nb)) seen.set(nb, { displayBrand: brand, normalizedBrand: nb, marketplaces: new Set() });
      seen.get(nb).marketplaces.add(p.marketplace || "ozon");
    }
    const brands = [...seen.values()].slice(0, 25).map((b) => ({ ...b, marketplaces: [...b.marketplaces] }));
    res.json({ brands });
  } catch (err) {
    next(err);
  }
});

// Resolve matching warehouse products for a brand ban using BrandIndex when available,
// falling back to resolveWarehouseBrand scan of the in-memory warehouse.
// Also includes products whose offer_id is explicitly listed in ban.bannedOfferIds.
async function resolveMatchingProductsForBan(ban) {
  const extraOfferIds = Array.isArray(ban.bannedOfferIds) ? ban.bannedOfferIds.filter(Boolean) : [];
  const prisma = getPrisma();
  if (prisma && shouldUsePostgresStorage()) {
    try {
      const indexItems = await prisma.brandIndexItem.findMany({
        where: { normalizedBrand: ban.normalizedBrand },
        select: { productId: true },
      });
      const byBrandIds = indexItems.length > 0 ? [...new Set(indexItems.map((item) => item.productId))] : [];
      const byBrandRows = byBrandIds.length > 0 ? await readWarehouseProductsFromPostgresByIds(byBrandIds) : [];

      // Include explicitly banned offer IDs (across all marketplaces)
      let extraRows = [];
      if (extraOfferIds.length) {
        const extraPgRows = await prisma.warehouseProduct.findMany({
          where: { offerId: { in: extraOfferIds }, archived: false },
          select: { id: true },
        });
        const extraPgIds = extraPgRows.map((r) => r.id);
        if (extraPgIds.length) {
          const fetched = await readWarehouseProductsFromPostgresByIds(extraPgIds);
          const byBrandSet = new Set(byBrandRows.map((p) => p.id));
          extraRows = fetched.filter((p) => !byBrandSet.has(p.id));
        }
      }

      if (byBrandRows.length > 0 || extraRows.length > 0) return [...byBrandRows, ...extraRows];
    } catch (err) {
      logger.warn("brand ban: brand index lookup failed, falling back to warehouse scan", { detail: err?.message || String(err) });
    }
  }
  const warehouse = await readWarehouse();
  const seen = new Set();
  return (warehouse.products || []).filter((p) => {
    if (seen.has(p.id)) return false;
    const brand = resolveWarehouseBrand(p);
    const matchesBrand = banNormalizedKey(brand) === ban.normalizedBrand;
    const matchesOfferId = extraOfferIds.length > 0 && extraOfferIds.includes(cleanText(p.offerId));
    if (matchesBrand || matchesOfferId) { seen.add(p.id); return true; }
    return false;
  });
}

// Zero WB stock for all cards whose brand matches the ban.
async function zeroWbStockForBan(ban) {
  const account = getWbAccountByTarget("wb");
  if (!account) return { wbCards: 0, wbZeroed: 0, wbError: null };
  const warehouseId = cleanText(account.campaignId);
  if (!warehouseId) return { wbCards: 0, wbZeroed: 0, wbError: "wb_warehouse_id_not_configured" };
  try {
    const cards = await wbCardsList(account);
    const matching = cards.filter((card) => {
      const nb = banNormalizedKey(cleanText(card.brand));
      return nb === ban.normalizedBrand || ozonBrandMatchesBan(nb, ban.normalizedBrand);
    });
    if (!matching.length) return { wbCards: 0, wbZeroed: 0, wbError: null };
    const stocks = matching.flatMap((card) =>
      (Array.isArray(card.sizes) ? card.sizes : [])
        .flatMap((size) => (Array.isArray(size.skus) ? size.skus : []))
        .map((sku) => ({ sku: cleanText(sku), amount: 0 }))
    ).filter((s) => s.sku);
    if (stocks.length) await wbUpdateStocks(account, warehouseId, stocks);
    return { wbCards: matching.length, wbZeroed: stocks.length, wbError: null };
  } catch (err) {
    return { wbCards: 0, wbZeroed: 0, wbError: err?.message || String(err) };
  }
}

// Word-overlap brand match: if all significant words (>=4 chars) of the shorter brand
// appear in the longer brand's word set — treat as same brand. Handles naming variants like
// "Chabaud De Maison" (Ozon) vs "Chabaud Maison De Parfum" (ЯМ).
function ozonBrandMatchesBan(ozonNorm, banNorm) {
  if (ozonNorm === banNorm) return true;
  const wordsA = ozonNorm.split(/\s+/).filter((w) => w.length >= 4);
  const wordsB = banNorm.split(/\s+/).filter((w) => w.length >= 4);
  if (!wordsA.length || !wordsB.length) return false;
  const shorter = wordsA.length <= wordsB.length ? wordsA : wordsB;
  const longerSet = new Set(wordsA.length <= wordsB.length ? wordsB : wordsA);
  return shorter.every((w) => longerSet.has(w));
}

// Scan Ozon API directly for products matching the ban brand (catches products with missing BrandIndex).
// Archives matching products and zeroes their FBS stocks by constructing minimal warehouse product objects.
async function archiveOzonProductsForBanViaApi(ban, alreadyFoundProductIds = new Set()) {
  const accounts = getOzonAccounts ? getOzonAccounts().filter((a) => a.clientId && a.apiKey) : [];
  if (!accounts.length) return { ozonExtra: 0, ozonError: null };
  // Build a keyword set from the ban brand for cheap name-based pre-filtering.
  // This avoids calling getOzonProductInfoMap on all 15k+ products (150+ API calls, most rate-limited).
  const banKeywords = ban.normalizedBrand.split(/\s+/).filter((w) => w.length >= 4);
  // Require at least 2 keywords to match (or all if fewer than 2) — "every" was too strict
  // because descriptive words like "parfum" rarely appear in product names.
  const requiredKeywordMatches = Math.min(2, banKeywords.length);
  let ozonExtra = 0;
  let ozonError = null;
  for (const account of accounts) {
    try {
      const allProducts = await getOzonProducts(Number.POSITIVE_INFINITY, account);
      // Pre-filter by product name: keep products whose name contains at least requiredKeywordMatches ban keywords.
      // /v3/product/list includes `name` without extra API calls.
      const candidates = allProducts.filter((p) => {
        const warehouseId = ozonWarehouseProductId ? ozonWarehouseProductId(account, cleanText(p.offer_id || p.offerId)) : null;
        if (warehouseId && alreadyFoundProductIds.has(warehouseId)) return false;
        const nameNorm = banNormalizedKey(cleanText(p.name || p.offer_id || ""));
        const matchCount = banKeywords.filter((kw) => nameNorm.includes(kw)).length;
        return matchCount >= requiredKeywordMatches;
      });
      if (!candidates.length) continue;
      // Confirm brand from info map — only for the small pre-filtered set.
      const candidateOfferIds = candidates.map((p) => cleanText(p.offer_id || p.offerId)).filter(Boolean);
      const infoMap = await getOzonProductInfoMap(candidateOfferIds, account, { continueOnError: true });
      const matching = [];
      for (const product of candidates) {
        const offerId = cleanText(product.offer_id || product.offerId);
        const info = infoMap.get(offerId) || infoMap.get(offerId.toLowerCase()) || {};
        const warehouseId = ozonWarehouseProductId ? ozonWarehouseProductId(account, offerId) : `ozon-${account.id}-${offerId}`;
        if (alreadyFoundProductIds.has(warehouseId)) continue;
        // Accept if info has brand match OR name-only match is confident enough (all keywords present).
        const brandRaw = cleanText(info.brand || info.vendor || extractBrandFromAttributes(info.attributes || info.complex_attributes?.[0]?.attributes || []));
        if (brandRaw) {
          const ozonNorm = banNormalizedKey(brandRaw);
          if (!ozonBrandMatchesBan(ozonNorm, ban.normalizedBrand)) continue;
        }
        // info might be empty if the detail call rate-limited — trust the name pre-filter in that case.
        const productId = cleanText(product.product_id || info.product_id || info.id || "");
        matching.push({
          id: warehouseId,
          marketplace: "ozon",
          target: account.id,
          offerId,
          productId,
          name: cleanText(info.name || product.name || offerId),
        });
      }
      if (!matching.length) continue;
      // Zero FBS stocks first — Ozon auto-unarchives when FBS stock > 0 is sent by stock sync
      await sendZeroStocksToMarketplace(matching).catch((e) => {
        if (!ozonError) ozonError = e?.message || String(e);
      });
      const productIds = matching.map((p) => Number(p.productId)).filter((id) => id > 0);
      if (productIds.length) {
        for (const chunk of chunkArray(productIds, 100)) {
          await ozonRequest("/v1/product/archive", { product_id: chunk }, account).catch((e) => {
            if (!ozonError) ozonError = e?.message || String(e);
          });
        }
      }
      // Write targetStock=0 so the periodic stock sync never re-sends FBS stock for these products.
      const now = new Date().toISOString();
      const patchItems = matching.map((m) => {
        const base = normalizeWarehouseProduct({ id: m.id, marketplace: "ozon", target: account.id, offerId: m.offerId });
        base.targetStock = 0;
        base.marketplaceState = { ...(base.marketplaceState || {}), stock: 0 };
        base.updatedAt = now;
        return base;
      });
      if (patchItems.length) {
        await writeWarehouseProductPatch(patchItems, { reason: "brand_ban_api_scan" }).catch((e) =>
          logger.warn("brand ban api scan: state patch failed", { detail: e?.message || String(e) }),
        );
      }
      ozonExtra += matching.length;
    } catch (err) {
      if (!ozonError) ozonError = err?.message || String(err);
    }
  }
  return { ozonExtra, ozonError };
}

// GET /api/brand-bans/:id/preview — count matching products without archiving
app.get("/api/brand-bans/:id/preview", requireAdmin, async (req, res, next) => {
  try {
    const id = cleanText(req.params.id);
    const bans = await readBrandBans();
    const ban = bans.find((b) => b.id === id);
    if (!ban) return res.status(404).json({ error: "ban not found" });

    const products = await resolveMatchingProductsForBan(ban);

    // Also count matching WB cards (WB isn't in the warehouse DB — uses separate API)
    const wbAccount = getWbAccountByTarget("wb");
    let wbCount = 0;
    let wbSuggestions = [];
    let wbCards = [];
    if (wbAccount && cleanText(wbAccount.campaignId)) {
      try {
        wbCards = await wbCardsList(wbAccount);
        wbCount = wbCards.filter((card) => {
          const nb = banNormalizedKey(cleanText(card.brand));
          return nb === ban.normalizedBrand || ozonBrandMatchesBan(nb, ban.normalizedBrand);
        }).length;
      } catch (_) {
        wbCount = -1;
      }
    }

    // When no Ozon/YM matches found, suggest brands from the BrandIndex that contain any word of the ban name
    let suggestions = [];
    if (products.length === 0) {
      const firstWord = ban.normalizedBrand.split(" ")[0];
      if (firstWord && firstWord.length >= 3) {
        const prisma = getPrisma();
        if (prisma && shouldUsePostgresStorage()) {
          try {
            const rows = await prisma.brandIndexItem.findMany({
              where: { normalizedBrand: { contains: firstWord } },
              select: { displayBrand: true, normalizedBrand: true },
              distinct: ["normalizedBrand"],
              take: 10,
            });
            suggestions = rows.map((r) => r.displayBrand).filter(Boolean);
          } catch (_) {}
        }
        // Also check WB brands if WB available
        if (wbCards.length > 0 && wbCount === 0) {
          const wbBrandMap = new Map();
          for (const card of wbCards) {
            const nb = banNormalizedKey(cleanText(card.brand));
            if (nb && nb.includes(firstWord) && !wbBrandMap.has(nb)) {
              wbBrandMap.set(nb, cleanText(card.brand));
            }
          }
          wbSuggestions = [...wbBrandMap.values()].slice(0, 5);
        }
      }
    }

    const summary = products.map((p) => ({
      id: p.id,
      name: cleanText(p.name),
      offerId: cleanText(p.offerId),
      marketplace: cleanText(p.marketplace),
      target: cleanText(p.target),
      brand: resolveWarehouseBrand(p),
    }));
    res.json({ ban, total: products.length, wbCount, suggestions, wbSuggestions, products: summary });
  } catch (err) {
    next(err);
  }
});

// POST /api/brand-bans/:id/apply — archive all matching products on all marketplaces + zero WB stock
app.post("/api/brand-bans/:id/apply", requireAdmin, async (req, res, next) => {
  try {
    const id = cleanText(req.params.id);
    const bans = await readBrandBans();
    const ban = bans.find((b) => b.id === id);
    if (!ban) return res.status(404).json({ error: "ban not found" });

    const [products, wbResult] = await Promise.all([
      resolveMatchingProductsForBan(ban),
      zeroWbStockForBan(ban),
    ]);

    let stockActions = [];
    let actions = [];
    if (products.length) {
      // Zero Ozon/YM stocks first — Ozon blocks archive when FBS stock > 0
      stockActions = await sendZeroStocksToMarketplace(products);
      actions = await archiveProductsOnMarketplaces(products);
    }

    // Also find Ozon warehouse products by the same offer IDs as already-found products.
    // Covers the case where Ozon brand wasn't indexed (broken attribute extraction) but
    // the Ozon variant of the same article exists in the warehouse under a different id.
    const alreadyFoundIds = new Set(products.map((p) => p.id));
    const allMatchedOfferIds = [...new Set(products.map((p) => cleanText(p.offerId)).filter(Boolean))];
    let ozonSiblingActions = [];
    let ozonSiblingStockActions = [];
    let ozonSiblings = [];
    if (allMatchedOfferIds.length) {
      try {
        const prisma = getPrisma();
        const ozonSiblingRows = prisma && shouldUsePostgresStorage()
          ? await readWarehouseProductsFromPostgresByIds(
              (await prisma.warehouseProduct.findMany({
                where: { marketplace: "ozon", offerId: { in: allMatchedOfferIds } },
                select: { id: true },
              })).map((r) => r.id),
            )
          : (await readWarehouse()).products.filter(
              (p) => p.marketplace === "ozon" && allMatchedOfferIds.includes(cleanText(p.offerId)),
            );
        ozonSiblings = ozonSiblingRows.filter((p) => !alreadyFoundIds.has(p.id));
        for (const p of ozonSiblings) alreadyFoundIds.add(p.id);
        if (ozonSiblings.length) {
          ozonSiblingStockActions = await sendZeroStocksToMarketplace(ozonSiblings);
          ozonSiblingActions = await archiveProductsOnMarketplaces(ozonSiblings);
        }
      } catch (e) {
        logger.warn("brand ban: ozon sibling lookup failed", { detail: e?.message || String(e) });
      }
    }

    // Also scan Ozon API directly for products whose brand wasn't indexed in BrandIndex
    // (happens when product was imported without extracting brand from attributes)
    const ozonApiResult = await archiveOzonProductsForBanViaApi(ban, alreadyFoundIds);

    // Write targetStock=0 for all known banned products so the next stock sync
    // does not re-send FBS stocks and undo the Ozon archive.
    const now = new Date().toISOString();
    const allBannedProducts = [...products, ...ozonSiblings];
    if (allBannedProducts.length) {
      const patchItems = allBannedProducts.map((p) => {
        const normalized = normalizeWarehouseProduct(p);
        normalized.targetStock = 0;
        normalized.marketplaceState = { ...(normalized.marketplaceState || {}), stock: 0 };
        normalized.updatedAt = now;
        return normalized;
      });
      await writeWarehouseProductPatch(patchItems, { reason: "brand_ban" }).catch((e) =>
        logger.warn("brand ban: state patch failed", { detail: e?.message || String(e) }),
      );
    }

    const archived = actions.filter((a) => a.ok).length;
    const ozonSiblingArchived = ozonSiblingActions.filter((a) => a.ok).length;
    const stockZeroed = stockActions.filter((a) => a.ok).length + ozonSiblingStockActions.filter((a) => a.ok).length;
    const failed = [...actions, ...ozonSiblingActions].filter((a) => !a.ok && !a.skipped);
    res.json({
      ok: failed.length === 0 && !wbResult.wbError && !ozonApiResult.ozonError,
      archived: archived + ozonSiblingArchived + ozonApiResult.ozonExtra,
      stockZeroed,
      total: products.length + ozonSiblings.length + ozonApiResult.ozonExtra,
      failed: failed.length,
      wbCards: wbResult.wbCards,
      wbZeroed: wbResult.wbZeroed,
      wbError: wbResult.wbError || undefined,
      ozonApiExtra: ozonApiResult.ozonExtra || undefined,
      ozonApiError: ozonApiResult.ozonError || undefined,
      ozonSiblings: ozonSiblingArchived || undefined,
      actions: [...actions, ...ozonSiblingActions],
    });
  } catch (err) {
    next(err);
  }
});
