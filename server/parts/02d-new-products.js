// Tester / decant detection — same heuristics as supplier search route.
function isTestOrDecant(name = "") {
  const n = String(name).toLowerCase();
  if (n.includes("отливант")) return true;
  if (n.includes("пробник")) return true;
  if (n.includes("decant")) return true;
  if (n.includes("тест") || n.includes("test") || n.includes("-tst")) return true;
  if (/(^|[^a-z0-9])tst([^a-z0-9]|$)/u.test(n)) return true;
  return false;
}

app.get("/api/new-products", requireStaff, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ ok: false, error: "Database not available" });

    const q = cleanText(request.query.q || "").toLowerCase();
    const supplierFilter = cleanText(request.query.supplier || "").toLowerCase();
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const sortBy = cleanText(request.query.sort || "date"); // date | price | name

    const ratePayload = await getUsdRate().catch(() => null);
    const usdRate = Number(ratePayload?.rate || process.env.DEFAULT_USD_RATE || 95) || 95;

    // Fetch all linked supplier articles in one query — article alone is the dedup key
    // because an article identifies a unique product across all suppliers.
    const [linkedLinks, warehouseNames] = await Promise.all([
      prisma.productLink.findMany({ select: { supplierArticle: true } }),
      prisma.warehouseProduct.findMany({ where: { archived: false }, select: { name: true } }),
    ]);
    const linkedSet = new Set(linkedLinks.map((l) => cleanText(l.supplierArticle || "").toLowerCase()));
    // Normalized warehouse names for name-prefix matching: PM often stores "Brand Fragrance"
    // while the warehouse product is "Brand Fragrance Парфюмерная вода 50 мл" — same product,
    // different article. If the PM name is a prefix of any warehouse name, skip it.
    const warehouseNormNames = warehouseNames
      .map((p) => cleanText(p.name || "").toLowerCase())
      .filter((n) => n.length >= 5);

    // Fetch all active PM snapshot items (newest first so DISTINCT ON keeps best row)
    const pmItems = await prisma.priceMasterSnapshotItem.findMany({
      where: { active: true },
      orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
      select: {
        article: true,
        partnerName: true,
        nativeName: true,
        price: true,
        currency: true,
        partnerId: true,
        docDate: true,
      },
    });

    // Deduplicate by article (keep newest occurrence), filter unlinked + exclude test/decant
    const seenArticles = new Set();
    const allCandidates = [];

    for (const item of pmItems) {
      const article = cleanText(item.article || "").toLowerCase();
      const name = cleanText(item.nativeName || "");
      const nameLower = name.toLowerCase();

      if (!article || !name) continue;
      if (seenArticles.has(article)) continue;
      seenArticles.add(article);

      if (linkedSet.has(article)) continue;
      if (isTestOrDecant(nameLower)) continue;
      // Skip PM items whose name is a case-insensitive prefix of any warehouse product name
      // (e.g. PM "Chanel Jersey" → warehouse "Chanel JERSEY Парфюмерная вода 1.5 мл")
      if (nameLower.length >= 8 && warehouseNormNames.some((wn) => wn.startsWith(nameLower) || nameLower.startsWith(wn))) continue;

      const priceNum = Number(item.price || 0) || 0;
      if (priceNum <= 0) continue;

      const currency = (cleanText(item.currency || "USD").toUpperCase() === "RUB") ? "RUB" : "USD";
      const priceRub = currency === "USD" ? Math.round(priceNum * usdRate) : Math.round(priceNum);
      const supplierName = cleanText(item.partnerName || "");

      allCandidates.push({
        article: cleanText(item.article),
        name,
        supplierName,
        partnerId: cleanText(item.partnerId || ""),
        price: priceNum,
        currency,
        priceRub,
        docDate: item.docDate ? item.docDate.toISOString().slice(0, 10) : null,
      });
    }

    // Server-side text/supplier filter
    let filtered = allCandidates;
    if (q) {
      filtered = filtered.filter((row) =>
        row.name.toLowerCase().includes(q) || row.article.toLowerCase().includes(q)
      );
    }
    if (supplierFilter) {
      filtered = filtered.filter((row) =>
        row.supplierName.toLowerCase().includes(supplierFilter)
      );
    }

    // Sort
    if (sortBy === "price") {
      filtered.sort((a, b) => a.priceRub - b.priceRub);
    } else if (sortBy === "price_desc") {
      filtered.sort((a, b) => b.priceRub - a.priceRub);
    } else if (sortBy === "name") {
      filtered.sort((a, b) => a.name.localeCompare(b.name, "ru", { sensitivity: "base" }));
    }
    // default: date desc (already ordered from Prisma query)

    // Stats by supplier
    const supplierMap = new Map();
    for (const row of filtered) {
      const key = row.partnerId || row.supplierName;
      if (!supplierMap.has(key)) supplierMap.set(key, { supplierName: row.supplierName, partnerId: row.partnerId, count: 0 });
      supplierMap.get(key).count++;
    }
    const bySupplier = Array.from(supplierMap.values()).sort((a, b) => b.count - a.count);

    // Price range stats
    const prices = filtered.map((r) => r.priceRub).filter((p) => p > 0);
    const priceMin = prices.length ? Math.min(...prices) : 0;
    const priceMax = prices.length ? Math.max(...prices) : 0;

    response.json({
      ok: true,
      total: filtered.length,
      shown: Math.min(filtered.length, limit),
      usdRate,
      priceMin,
      priceMax,
      bySupplier: bySupplier.slice(0, 30),
      rows: filtered.slice(0, limit),
    });
  } catch (error) {
    next(error);
  }
});
