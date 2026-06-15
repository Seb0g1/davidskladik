// Products whose name carries the "дубль" (duplicate) marker are Ozon duplicate listings the
// seller creates; they clutter the catalog and get linked to the wrong supplier by mistake.
// They are hidden from the whole managed catalog (this WHERE is shared by the catalog page,
// summaries, counters, the reconciler and reprice) and from the fast sweeps. Data is kept in
// the table (not deleted) so it's reversible: set DUPLICATE_CATALOG_EXCLUDE_ENABLED=false.
const DUPLICATE_MARKER_NAME = cleanText(process.env.DUPLICATE_MARKER_NAME || "дубль").toLowerCase();
const duplicateCatalogExcludeEnabled = process.env.DUPLICATE_CATALOG_EXCLUDE_ENABLED !== "false" && Boolean(DUPLICATE_MARKER_NAME);

function isDuplicateMarkerName(name) {
  if (!duplicateCatalogExcludeEnabled) return false;
  return cleanText(name).toLowerCase().includes(DUPLICATE_MARKER_NAME);
}

// Prisma where fragment excluding duplicate-marker products (null names are kept).
function duplicateNameExclusionWhere() {
  if (!duplicateCatalogExcludeEnabled) return {};
  // Top-level NOT (the field-level `name: { not: { contains } }` form is invalid Prisma).
  // `name` is non-nullable (schema: WarehouseProduct.name String), so no null branch needed —
  // a `{ name: null }` branch would itself be invalid ("Argument `name` is missing").
  return { NOT: { name: { contains: DUPLICATE_MARKER_NAME, mode: "insensitive" } } };
}

// Raw-SQL fragment for the same exclusion, for the sweeps that query warehouse_products directly.
function duplicateNameSqlExclusion(alias = "p") {
  if (!duplicateCatalogExcludeEnabled) return "TRUE";
  const literal = DUPLICATE_MARKER_NAME.replace(/'/g, "''");
  return `(${alias}.name IS NULL OR ${alias}.name NOT ILIKE '%${literal}%')`;
}

function collectYandexWarehouseTargetAliases() {
  const aliases = new Set(["yandex"]);
  for (const shop of getYandexShops({ includeSyncDisabled: true })) {
    for (const value of [shop.id, shop.name, shop.businessId, shop.campaignId]) {
      const alias = cleanText(value);
      if (alias) aliases.add(alias);
    }
  }
  return Array.from(aliases);
}

function enabledWarehouseTargetWhere() {
  const or = [];
  const ozonAccounts = getOzonAccounts();
  if (ozonAccounts.length) {
    for (const account of ozonAccounts) {
      or.push({
        marketplace: "ozon",
        OR: [
          { target: account.id },
          { target: "ozon" },
        ],
      });
    }
  } else {
    or.push({ marketplace: "ozon" });
  }
  const yandexShops = getYandexShops({ includeSyncDisabled: true });
  if (yandexShops.length === 1) {
    or.push({ marketplace: "yandex" });
  } else if (yandexShops.length) {
    const yandexTargetFilters = [
      { target: "yandex" },
      { target: { startsWith: "yandex" } },
      ...collectYandexWarehouseTargetAliases().map((alias) => ({ target: alias })),
    ];
    or.push({
      marketplace: "yandex",
      OR: yandexTargetFilters,
    });
  } else {
    or.push({ marketplace: "yandex" });
  }
  const base = or.length ? { OR: or } : {};
  const exclusion = duplicateNameExclusionWhere();
  if (!Object.keys(exclusion).length) return base;
  return { AND: [base, exclusion] };
}

function warehousePagePostgresWhere(filters = {}) {
  const and = [enabledWarehouseTargetWhere()];
  const marketplace = cleanText(filters.marketplace || "all");
  if (marketplace !== "all" && ["ozon", "yandex"].includes(marketplace)) and.push({ marketplace });
  const linked = cleanText(filters.linked || "all");
  if (linked === "linked") and.push({ links: { some: {} } });
  if (linked === "ready") and.push({ links: { some: {} } });
  if (linked === "unlinked") and.push({ links: { none: {} } });
  if (linked === "changed") and.push({ links: { some: {} } });
  if (linked === "linked_archived") and.push({ links: { some: {} } }, { OR: [{ archived: true }, { status: "archived" }] });
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all") {
    if (stateCode === "archived") {
      and.push({ OR: [{ archived: true }, { status: "archived" }] });
    } else {
      and.push({ status: stateCode });
    }
  }
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter) {
    and.push({
      OR: [
        { brand: { contains: brandFilter, mode: "insensitive" } },
        { name: { contains: brandFilter, mode: "insensitive" } },
      ],
    });
  }
  const q = cleanText(filters.q || "");
  if (q) {
    if (isWarehouseArticleLikeQuery(q)) {
      and.push({
        OR: [
          { id: { equals: q, mode: "insensitive" } },
          { offerId: { equals: q, mode: "insensitive" } },
          { productId: { equals: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { equals: q, mode: "insensitive" } } } },
        ],
      });
    } else {
      and.push({
        OR: [
          { id: { contains: q, mode: "insensitive" } },
          { offerId: { contains: q, mode: "insensitive" } },
          { productId: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { brand: { contains: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { contains: q, mode: "insensitive" } } } },
          { links: { some: { supplierName: { contains: q, mode: "insensitive" } } } },
          { links: { some: { partnerId: { contains: q, mode: "insensitive" } } } },
          { links: { some: { keyword: { contains: q, mode: "insensitive" } } } },
        ],
      });
    }
  }
  return { AND: and.filter((item) => Object.keys(item || {}).length) };
}
