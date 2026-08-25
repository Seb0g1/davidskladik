function priceMasterSnapshotRaw(row = {}) {
  const raw = row.raw;
  return raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
}

function priceMasterSnapshotPartner(row = {}) {
  const raw = priceMasterSnapshotRaw(row);
  const id = cleanText(row.partnerId || raw.partnerId || raw.PartnerID || "");
  const name = cleanText(row.partnerName || raw.partnerName || raw.PartnerName || raw.name || "");
  if (!id && !name) return null;
  return { id, partnerId: id, name, partnerName: name };
}

function priceMasterSnapshotOffer(row = {}, usdRate) {
  const raw = priceMasterSnapshotRaw(row);
  const currency = cleanText(row.currency || raw.priceCurrency || raw.currency || "USD").toUpperCase() === "RUB" ? "RUB" : "USD";
  const price = row.price ?? raw.price ?? raw.NativePrice ?? 0;
  const normalized = normalizePriceMasterPrice(price, usdRate, currency);
  const rowId = cleanText(row.rowId || raw.rowId || raw.RowID || row.id);
  const article = cleanText(row.article || raw.article || raw.NativeID || raw.offerId || "");
  const name = cleanText(row.nativeName || raw.name || raw.NativeName || "");
  const partner = priceMasterSnapshotPartner(row) || {};
  const docDate = row.docDate instanceof Date
    ? row.docDate.toISOString()
    : cleanText(row.docDate || raw.docDate || raw.DocDate || "");

  return {
    rowId,
    article,
    offerId: article,
    barcode: cleanText(raw.barcode || raw.BarCode || ""),
    name,
    productId: cleanText(raw.productId || raw.ProductID || ""),
    active: row.active !== false,
    isNew: Boolean(raw.isNew || raw.IsNew),
    ignored: Boolean(raw.ignored || raw.Ignored),
    docDate,
    partnerId: partner.partnerId || "",
    partnerName: partner.partnerName || "",
    priceCurrency: normalized.sourceCurrency,
    source: "postgres_snapshot",
    ...normalized,
  };
}

async function searchPriceMasterSnapshotPartners(query, limit = 25) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const q = cleanText(query);
  if (!q) return [];
  const take = Math.min(Math.max(Number(limit) * 4, 40), 320);
  try {
    const rows = await prisma.priceMasterSnapshotItem.findMany({
      where: {
        active: true,
        partnerName: { contains: q, mode: "insensitive" },
      },
      select: { partnerId: true, partnerName: true, raw: true },
      orderBy: [{ partnerName: "asc" }],
      take,
    });
    const unique = new Map();
    for (const row of rows) {
      const partner = priceMasterSnapshotPartner(row);
      if (!partner?.name) continue;
      const key = partner.partnerId || normalizeSupplierName(partner.name);
      if (!unique.has(key)) unique.set(key, partner);
      if (unique.size >= limit) break;
    }
    return Array.from(unique.values());
  } catch (error) {
    logger.warn("PriceMaster snapshot partner search failed, trying live", { detail: error?.message || String(error) });
    return null;
  }
}

async function searchPriceMasterSnapshotOffers({ search = "", partner = "", limit = 150, usdRate, tokenGroups = null } = {}) {
  if (!shouldUsePostgresStorage()) return null;
  const prisma = getPrisma();
  if (!prisma) return null;
  const q = cleanText(search);
  const partnerId = cleanText(partner);
  const take = cleanLimit(limit, 150, 500);

  const groups = tokenGroups && tokenGroups.length ? tokenGroups : (q ? pmQueryToTokenGroups(q) : null);
  const minMatchCount = groups ? pmMinMatchCount(groups) : 0;

  function buildQuery(groupSubset) {
    const and = [{ active: true }];
    if (groupSubset && groupSubset.length) {
      // OR across all tokens from all groups — JS post-filter enforces minimum match count
      const orTerms = groupSubset.flatMap((group) =>
        group.flatMap((synonym) => [
          { article: { contains: synonym, mode: "insensitive" } },
          { nativeName: { contains: synonym, mode: "insensitive" } },
        ]),
      );
      and.push({ OR: orTerms });
    } else if (q) {
      and.push({
        OR: [
          { article: { contains: q, mode: "insensitive" } },
          { nativeName: { contains: q, mode: "insensitive" } },
          { partnerName: { contains: q, mode: "insensitive" } },
        ],
      });
    }
    if (partnerId) and.push({ partnerId });
    return and;
  }

  try {
    let rows = await prisma.priceMasterSnapshotItem.findMany({
      where: { AND: buildQuery(groups) },
      orderBy: [{ docDate: "desc" }, { updatedAt: "desc" }],
      take: take * 3,
    });

    // Post-filter: run for any non-empty query so single numeric chips also enforce word-boundary.
    if (groups && groups.length >= 1) {
      rows = rows.filter((row) => {
        const hay = [cleanText(row.nativeName || ""), cleanText(row.article || "")].join(" ");
        return pmPassesSearchFilter(hay, groups);
      });
    }
    rows = rows.slice(0, take);

    return rows.map((row) => priceMasterSnapshotOffer(row, usdRate));
  } catch (error) {
    logger.warn("PriceMaster snapshot offer search failed, trying live", { detail: error?.message || String(error) });
    return null;
  }
}
function auditEntryProductIds(entry = {}) {
  const details = entry.details || {};
  const values = [
    entry.productId,
    details.productId,
    details.id,
    details.entityId,
    details.productIds,
  ];
  return values
    .flatMap((value) => (Array.isArray(value) ? value : [value]))
    .map((value) => cleanText(value || ""))
    .filter(Boolean);
}

function auditEntrySearchText(entry = {}) {
  const details = entry.details || {};
  const links = Array.isArray(details.links) ? details.links : [];
  return [
    entry.user,
    entry.action,
    entry.productId,
    details.productId,
    details.productIds,
    details.offerId,
    details.name,
    details.article,
    details.supplierName,
    details.linkId,
    ...links.flatMap((link) => [link.article, link.supplierName, link.partnerId, link.keyword]),
  ]
    .flatMap((value) => (Array.isArray(value) ? value : [value]))
    .map((value) => cleanText(value || "").toLowerCase())
    .filter(Boolean)
    .join(" ");
}

function auditEntryMatchesFilters(entry = {}, filters = {}) {
  const username = cleanText(filters.user || "").toLowerCase();
  if (username && cleanText(entry.user || "").toLowerCase() !== username) return false;
  const action = cleanText(filters.action || "");
  if (action && action !== "all" && cleanText(entry.action || "") !== action) return false;
  const query = cleanText(filters.q || "").toLowerCase();
  if (query && !auditEntrySearchText(entry).includes(query)) return false;
  const fromMs = filters.dateFrom ? new Date(filters.dateFrom).getTime() : 0;
  const toMs = filters.dateTo ? new Date(filters.dateTo).getTime() : 0;
  const atMs = new Date(entry.at || 0).getTime();
  if (Number.isFinite(fromMs) && fromMs > 0 && atMs < fromMs) return false;
  if (Number.isFinite(toMs) && toMs > 0 && atMs > toMs + 24 * 60 * 60 * 1000 - 1) return false;
  return true;
}

function auditPostgresWhereFromFilters(filters = {}) {
  const where = {};
  const username = cleanText(filters.user || "");
  if (username) {
    where.username = { equals: username, mode: "insensitive" };
  }
  const action = cleanText(filters.action || "");
  if (action && action !== "all") {
    where.action = action;
  }
  const createdAt = {};
  const dateFrom = toDateOrNull(filters.dateFrom);
  if (dateFrom) createdAt.gte = dateFrom;
  const dateTo = toDateOrNull(filters.dateTo);
  if (dateTo) {
    dateTo.setHours(23, 59, 59, 999);
    createdAt.lte = dateTo;
  }
  if (Object.keys(createdAt).length) where.createdAt = createdAt;
  return where;
}

async function readAuditFiltered(filters = {}, limit = 200) {
  const normalizedLimit = cleanLimit(limit, 200, 1000);
  const hasFilters = Object.values(filters).some((value) => cleanText(value || ""));
  const query = cleanText(filters.q || "");

  if (shouldUsePostgresStorage()) {
    try {
      const take = query ? Math.min(5000, Math.max(normalizedLimit * 10, 1000)) : normalizedLimit;
      const rows = await getPrisma().auditLog.findMany({
        where: auditPostgresWhereFromFilters(filters),
        take,
        orderBy: { createdAt: "desc" },
      });
      const entries = rows.map(auditRowToEntry);
      return query
        ? entries.filter((entry) => auditEntryMatchesFilters(entry, filters)).slice(0, normalizedLimit)
        : entries;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read filtered audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }

  const audit = await readAudit(hasFilters ? Math.max(normalizedLimit * 5, 1000) : normalizedLimit);
  return hasFilters
    ? audit.filter((entry) => auditEntryMatchesFilters(entry, filters)).slice(0, normalizedLimit)
    : audit;
}
function publicLinkAuditEntry(entry = {}) {
  const details = entry.details || {};
  const action = cleanText(entry.action || "");
  const productIds = auditEntryProductIds(entry);
  const links = Array.isArray(details.links)
    ? details.links.map((link) => ({
        article: cleanText(link.article || ""),
        supplierName: cleanText(link.supplierName || ""),
        partnerId: cleanText(link.partnerId || ""),
        priceCurrency: cleanText(link.priceCurrency || ""),
        keyword: cleanText(link.keyword || ""),
      })).filter((link) => link.article || link.supplierName || link.partnerId)
    : [];
  return {
    at: entry.at || null,
    user: entry.user || "system",
    action,
    productIds,
    offerId: details.offerId || "",
    name: details.name || "",
    article: details.article || links[0]?.article || "",
    supplierName: details.supplierName || links[0]?.supplierName || "",
    linkId: details.linkId || "",
    links,
    linksCount: links.length || null,
  };
}
